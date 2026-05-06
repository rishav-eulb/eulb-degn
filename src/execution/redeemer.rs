use std::str::FromStr;

use alloy::primitives::B256;
use alloy::providers::ProviderBuilder;
use alloy::signers::Signer as _;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use polymarket_client_sdk_v2::ctf::types::RedeemPositionsRequest;
use polymarket_client_sdk_v2::types::address;
use polymarket_client_sdk_v2::POLYGON;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

type PrivateKeySigner =
    polymarket_client_sdk_v2::auth::LocalSigner<k256::ecdsa::SigningKey>;

/// pUSD (USDC.e) collateral token on Polygon used by Polymarket.
const COLLATERAL_TOKEN: alloy::primitives::Address =
    address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

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

/// Command sent to the background redeemer task.
#[derive(Debug)]
pub enum RedeemCommand {
    Schedule {
        condition_id: String,
        slug: String,
        market_end_ts: DateTime<Utc>,
    },
}

/// Manages auto-redemption of winning tokens after market resolution,
/// using on-chain CTF contract calls via the Polymarket SDK.
pub struct Redeemer {
    pending: VecDeque<PendingRedemption>,
    rpc_url: String,
    private_key: String,
    dry_run: bool,
}

impl Redeemer {
    pub fn new(rpc_url: String, private_key: String, dry_run: bool) -> Self {
        Self {
            pending: VecDeque::new(),
            rpc_url,
            private_key,
            dry_run,
        }
    }

    pub fn schedule_redemption(
        &mut self,
        condition_id: String,
        slug: String,
        market_end_ts: DateTime<Utc>,
    ) {
        let redeem_after = market_end_ts + chrono::Duration::seconds(REDEEM_DELAY_SECS as i64);

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
                        "Redemption successful — collateral collected"
                    );
                    successes += 1;
                }
                Ok(false) => {
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
                            error = ?e,
                            "Redemption failed, will retry"
                        );
                        retry_later.push_back(pending);
                    } else {
                        error!(
                            slug = pending.slug.as_str(),
                            error = ?e,
                            "Giving up on redemption after 10 attempts"
                        );
                    }
                }
            }
        }

        self.pending = retry_later;
        successes
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

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

    async fn execute_redeem(&self, pending: &PendingRedemption) -> Result<bool> {
        if self.dry_run {
            info!(
                slug = pending.slug.as_str(),
                condition_id = pending.condition_id.as_str(),
                "DRY RUN — would redeem positions"
            );
            return Ok(true);
        }

        let condition_id = B256::from_str(&pending.condition_id)
            .context("Invalid condition_id hex")?;

        let signer: PrivateKeySigner = PrivateKeySigner::from_str(&self.private_key)
            .context("Invalid private key for redeemer")?
            .with_chain_id(Some(POLYGON));

        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect(&self.rpc_url)
            .await
            .context("Failed to connect to Polygon RPC")?;

        let ctf_client =
            polymarket_client_sdk_v2::ctf::Client::with_neg_risk(provider, POLYGON)
                .context("Failed to create CTF client")?;

        let request = RedeemPositionsRequest::for_binary_market(
            COLLATERAL_TOKEN,
            condition_id,
        );

        info!(
            slug = pending.slug.as_str(),
            condition_id = pending.condition_id.as_str(),
            "Submitting on-chain redeem tx (standard CTF)"
        );

        match ctf_client.redeem_positions(&request).await {
            Ok(resp) => {
                info!(
                    slug = pending.slug.as_str(),
                    tx_hash = %resp.transaction_hash,
                    block = resp.block_number,
                    "Redeem tx confirmed"
                );
                Ok(true)
            }
            Err(e) => {
                let err_str = format!("{e:?}");
                if err_str.contains("not resolved")
                    || err_str.contains("payout denominator is not set")
                {
                    return Ok(false);
                }
                warn!(
                    slug = pending.slug.as_str(),
                    error = ?e,
                    "Standard CTF redeem failed"
                );
                Err(anyhow::anyhow!("{e}"))
            }
        }
    }
}
