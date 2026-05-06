use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// CTF contract on Polygon for standard (non-negative-risk) markets.
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// NegRisk Adapter for negative-risk markets (BTC/ETH up/down markets use this).
const NEG_RISK_ADAPTER: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

/// pUSD (USDC.e) collateral token on Polygon used by Polymarket.
const COLLATERAL_TOKEN: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Zero bytes32 for parentCollectionId.
const PARENT_COLLECTION_ID: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

/// Delay after market end before attempting redemption (seconds).
const REDEEM_DELAY_SECS: u64 = 120;

/// A resolved market waiting for redemption.
#[derive(Debug, Clone)]
pub struct PendingRedemption {
    pub condition_id: String,
    pub slug: String,
    pub market_end_ts: DateTime<Utc>,
    pub redeem_after: DateTime<Utc>,
    pub attempts: u32,
}

/// Manages auto-redemption of winning tokens after market resolution.
pub struct Redeemer {
    pending: VecDeque<PendingRedemption>,
    http_client: Client,
    clob_url: String,
    private_key: String,
    dry_run: bool,
}

/// Response from the Polymarket CLOB /redeem endpoint.
#[derive(Debug, Deserialize)]
struct RedeemResponse {
    success: Option<bool>,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    error: Option<String>,
}

/// Command sent to the background redeemer task.
#[derive(Debug)]
pub enum RedeemCommand {
    Schedule {
        condition_id: String,
        slug: String,
        market_end_ts: DateTime<Utc>,
    },
}

/// Request body for the Polymarket Builder Relayer redeem endpoint.
#[derive(Debug, Serialize)]
struct RedeemRequest {
    condition_id: String,
}

impl Redeemer {
    pub fn new(clob_url: String, private_key: String, dry_run: bool) -> Self {
        Self {
            pending: VecDeque::new(),
            http_client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            clob_url,
            private_key,
            dry_run,
        }
    }

    /// Schedule a market for redemption after the standard delay.
    pub fn schedule_redemption(&mut self, condition_id: String, slug: String, market_end_ts: DateTime<Utc>) {
        let redeem_after = market_end_ts + chrono::Duration::seconds(REDEEM_DELAY_SECS as i64);

        // Don't add duplicates
        if self.pending.iter().any(|p| p.condition_id == condition_id) {
            debug!(slug, "Redemption already scheduled");
            return;
        }

        info!(
            slug,
            condition_id = condition_id.as_str(),
            redeem_after = %redeem_after,
            "Scheduled redemption"
        );

        self.pending.push_back(PendingRedemption {
            condition_id,
            slug,
            market_end_ts,
            redeem_after,
            attempts: 0,
        });
    }

    /// Check if any pending redemptions are ready and execute them.
    /// Returns the number of successful redemptions.
    pub async fn tick(&mut self) -> u32 {
        let now = Utc::now();
        let mut successes = 0u32;
        let mut retry_later: VecDeque<PendingRedemption> = VecDeque::new();

        while let Some(mut pending) = self.pending.pop_front() {
            if now < pending.redeem_after {
                retry_later.push_back(pending);
                continue;
            }

            match self.execute_redeem(&pending).await {
                Ok(true) => {
                    info!(
                        slug = pending.slug.as_str(),
                        condition_id = pending.condition_id.as_str(),
                        "Redemption successful — USDC collected"
                    );
                    successes += 1;
                }
                Ok(false) => {
                    // Market not yet resolved — retry later
                    pending.attempts += 1;
                    if pending.attempts < 10 {
                        pending.redeem_after = now + chrono::Duration::seconds(30);
                        warn!(
                            slug = pending.slug.as_str(),
                            attempt = pending.attempts,
                            "Market not yet resolved, will retry"
                        );
                        retry_later.push_back(pending);
                    } else {
                        error!(
                            slug = pending.slug.as_str(),
                            "Giving up on redemption after 10 attempts"
                        );
                    }
                }
                Err(e) => {
                    pending.attempts += 1;
                    if pending.attempts < 10 {
                        pending.redeem_after = now + chrono::Duration::seconds(30);
                        warn!(
                            slug = pending.slug.as_str(),
                            attempt = pending.attempts,
                            error = %e,
                            "Redemption failed, will retry"
                        );
                        retry_later.push_back(pending);
                    } else {
                        error!(
                            slug = pending.slug.as_str(),
                            error = %e,
                            "Giving up on redemption after 10 attempts"
                        );
                    }
                }
            }
        }

        self.pending = retry_later;
        successes
    }

    /// Number of pending redemptions.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Move the redeemer into its own background task so it never blocks the main loop.
    /// Returns a sender for scheduling new redemptions.
    pub fn spawn(mut self) -> mpsc::UnboundedSender<RedeemCommand> {
        let (tx, mut rx) = mpsc::unbounded_channel::<RedeemCommand>();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    Some(cmd) = rx.recv() => {
                        match cmd {
                            RedeemCommand::Schedule { condition_id, slug, market_end_ts } => {
                                self.schedule_redemption(condition_id, slug, market_end_ts);
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let redeemed = self.tick().await;
                        if redeemed > 0 {
                            info!(count = redeemed, "Redeemed positions");
                        }
                    }
                }
            }
        });
        tx
    }

    /// Execute the actual redemption call.
    async fn execute_redeem(&self, pending: &PendingRedemption) -> Result<bool> {
        if self.dry_run {
            info!(
                slug = pending.slug.as_str(),
                condition_id = pending.condition_id.as_str(),
                "DRY RUN — would redeem positions"
            );
            return Ok(true);
        }

        // Use the Polymarket CLOB API's /redeem endpoint (Builder Relayer pattern).
        // This is simpler than raw contract calls — the relayer handles the on-chain tx.
        let url = format!("{}/redeem", self.clob_url);

        let resp = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("POLY_ADDRESS", derive_poly_address(&self.private_key))
            .header("POLY_SIGNATURE", self.sign_redeem_request(&pending.condition_id))
            .json(&RedeemRequest {
                condition_id: pending.condition_id.clone(),
            })
            .send()
            .await
            .context("Redeem HTTP request failed")?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if status.is_success() {
            if let Ok(parsed) = serde_json::from_str::<RedeemResponse>(&body) {
                if parsed.success.unwrap_or(false) {
                    info!(
                        slug = pending.slug.as_str(),
                        tx_hash = parsed.transaction_hash.as_deref().unwrap_or("unknown"),
                        "Redeem tx submitted"
                    );
                    return Ok(true);
                }
                if let Some(err) = parsed.error {
                    if err.contains("not resolved") || err.contains("not yet") {
                        return Ok(false);
                    }
                    anyhow::bail!("Redeem API error: {}", err);
                }
            }
            Ok(true)
        } else if status.as_u16() == 400 || status.as_u16() == 422 {
            // Possibly not yet resolved
            if body.contains("not resolved") || body.contains("not yet") {
                Ok(false)
            } else {
                anyhow::bail!("Redeem API {} — {}", status, body);
            }
        } else {
            anyhow::bail!("Redeem API {} — {}", status, body);
        }
    }

    /// Sign the redemption request. In production, this uses L1 auth headers.
    /// For now, returns a placeholder — actual signing uses polymarket_client_sdk_v2.
    fn sign_redeem_request(&self, _condition_id: &str) -> String {
        // TODO: Implement proper EIP-712 signing with the private key
        // using polymarket_client_sdk_v2::auth module
        "placeholder_signature".to_string()
    }
}

/// Derive the Polygon address from a private key.
/// In production, use alloy/ethers to derive the actual address.
fn derive_poly_address(private_key: &str) -> String {
    // TODO: Implement proper address derivation
    // For now return a placeholder — the actual implementation uses:
    // let wallet = LocalWallet::from_str(private_key)?;
    // format!("{:?}", wallet.address())
    if private_key.starts_with("0x") && private_key.len() > 10 {
        format!("0x{}", &private_key[2..42])
    } else {
        "0x0000000000000000000000000000000000000000".to_string()
    }
}
