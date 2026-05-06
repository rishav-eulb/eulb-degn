use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, error, info, warn};

use polymarket_client_sdk_v2::auth::Signer;
use polymarket_client_sdk_v2::clob::types::{
    Amount, OrderStatusType, OrderType as SdkOrderType, Side, SignatureType,
};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Address, U256};
use polymarket_client_sdk_v2::POLYGON;

type PrivateKeySigner = polymarket_client_sdk_v2::auth::LocalSigner<k256::ecdsa::SigningKey>;
type AuthenticatedClient =
    ClobClient<polymarket_client_sdk_v2::auth::state::Authenticated<polymarket_client_sdk_v2::auth::Normal>>;

/// Outcome of an order submission.
#[derive(Debug, Clone)]
pub enum OrderResult {
    Filled { fill_price: f64, shares: f64 },
    PartialFill { fill_price: f64, filled_shares: f64, remaining_shares: f64 },
    Rejected { reason: String },
}

/// A request sent to the executor task.
#[derive(Debug)]
pub enum OrderRequest {
    Leg1Market {
        token_id: String,
        size_usd: f64,
        asset_key: String,
        /// Hint price from local WS book to skip the SDK's order book fetch.
        hint_price: Option<f64>,
    },
    Leg2Limit {
        token_id: String,
        price: f64,
        shares: f64,
        asset_key: String,
    },
    ForceClose {
        token_id: String,
        shares: f64,
        asset_key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRequestKind {
    Leg1,
    Leg2,
    ForceClose,
}

/// Builds and submits orders via the Polymarket CLOB SDK.
/// Caches the authenticated client across orders to avoid re-authentication overhead.
pub struct OrderExecutor {
    clob_url: String,
    funder_address: Option<String>,
    dry_run: bool,
    cached_client: Arc<Mutex<Option<AuthenticatedClient>>>,
    signer: PrivateKeySigner,
}

impl OrderExecutor {
    pub fn new(
        clob_url: String,
        private_key: String,
        funder_address: Option<String>,
        dry_run: bool,
    ) -> Result<Self> {
        let signer: PrivateKeySigner = PrivateKeySigner::from_str(&private_key)
            .context("Invalid private key")?
            .with_chain_id(Some(POLYGON));
        Ok(Self {
            clob_url,
            funder_address,
            dry_run,
            cached_client: Arc::new(Mutex::new(None)),
            signer,
        })
    }

    /// Spawn the background executor task. Returns a sender to submit order requests,
    /// and a receiver for completed order results.
    pub fn spawn(self) -> (mpsc::UnboundedSender<OrderRequest>, mpsc::UnboundedReceiver<(String, OrderRequestKind, Result<OrderResult>)>) {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<OrderRequest>();
        let (result_tx, result_rx) = mpsc::unbounded_channel::<(String, OrderRequestKind, Result<OrderResult>)>();

        tokio::spawn(async move {
            while let Some(request) = req_rx.recv().await {
                match request {
                    OrderRequest::Leg1Market { token_id, size_usd, asset_key, hint_price, .. } => {
                        let result = self.execute_leg1(&token_id, size_usd, hint_price).await;
                        let _ = result_tx.send((asset_key, OrderRequestKind::Leg1, result));
                    }
                    OrderRequest::Leg2Limit { token_id, price, shares, asset_key } => {
                        let result = self.execute_leg2(&token_id, price, shares).await;
                        let _ = result_tx.send((asset_key, OrderRequestKind::Leg2, result));
                    }
                    OrderRequest::ForceClose { token_id, shares, asset_key } => {
                        let result = self.execute_force_close(&token_id, shares).await;
                        let _ = result_tx.send((asset_key, OrderRequestKind::ForceClose, result));
                    }
                }
            }
        });

        (req_tx, result_rx)
    }

    async fn execute_leg1(&self, token_id: &str, size_usd: f64, hint_price: Option<f64>) -> Result<OrderResult> {
        info!(token_id, size_usd, ?hint_price, "Placing Leg1 FOK market order");

        if self.dry_run {
            debug!("DRY RUN — simulating Leg1 fill at 0.50");
            return Ok(OrderResult::Filled {
                fill_price: 0.50,
                shares: size_usd / 0.50,
            });
        }

        let amount = rust_decimal::Decimal::from_f64(size_usd).unwrap_or(dec!(25.0));
        let hint_dec = hint_price.and_then(rust_decimal::Decimal::from_f64);
        self.execute_market_order(token_id, Side::Buy, amount, hint_dec).await
    }

    async fn execute_leg2(&self, token_id: &str, price: f64, shares: f64) -> Result<OrderResult> {
        info!(token_id, price, shares, "Placing Leg2 limit order (maker)");

        if self.dry_run {
            debug!("DRY RUN — simulating Leg2 fill");
            return Ok(OrderResult::Filled { fill_price: price, shares });
        }

        let price_dec = rust_decimal::Decimal::from_f64(price).context("Invalid Leg2 price")?;
        let size_dec = rust_decimal::Decimal::from_f64(shares).context("Invalid Leg2 shares")?;
        self.execute_limit_order(token_id, Side::Buy, price_dec, size_dec).await
    }

    async fn execute_force_close(&self, token_id: &str, shares: f64) -> Result<OrderResult> {
        info!(token_id, shares, "Force-closing Leg1 at market");

        if self.dry_run {
            debug!("DRY RUN — simulating force close");
            return Ok(OrderResult::Filled { fill_price: 0.50, shares });
        }

        let size_dec = rust_decimal::Decimal::from_f64(shares)
            .context("Invalid close shares")?
            .trunc_with_scale(2); // SDK requires <= 2 decimal places for shares
        // Use aggressive minimum price hint to skip book fetch — force-close accepts any fill.
        let aggressive_price = Some(dec!(0.01));
        self.execute_market_order(token_id, Side::Sell, size_dec, aggressive_price).await
    }

    /// Get or create the cached authenticated client.
    /// On auth failure, invalidates the cache and retries once.
    async fn get_client(&self) -> Result<AuthenticatedClient> {
        {
            let guard = self.cached_client.lock().await;
            if let Some(ref client) = *guard {
                return Ok(client.clone());
            }
        }

        let client = self.authenticate_fresh().await?;
        let mut guard = self.cached_client.lock().await;
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Invalidate the cached client and re-authenticate.
    async fn refresh_client(&self) -> Result<AuthenticatedClient> {
        let client = self.authenticate_fresh().await?;
        let mut guard = self.cached_client.lock().await;
        *guard = Some(client.clone());
        info!("CLOB client re-authenticated (cache refreshed)");
        Ok(client)
    }

    fn authenticate_fresh(&self) -> impl std::future::Future<Output = Result<AuthenticatedClient>> + '_ {
        async {
            let config = ClobConfig::builder().use_server_time(false).build();
            let mut auth_builder = ClobClient::new(&self.clob_url, config)
                .context("Failed to create CLOB client")?
                .authentication_builder(&self.signer);

            if let Some(ref funder) = self.funder_address {
                let addr: Address = funder.parse().context("Invalid funder address")?;
                auth_builder = auth_builder
                    .funder(addr)
                    .signature_type(SignatureType::GnosisSafe);
                debug!(funder = %addr, "Using Gnosis Safe wallet as funder");
            }

            auth_builder
                .authenticate()
                .await
                .context("CLOB authentication failed")
        }
    }

    /// Execute a market order. When `hint_price` is provided, passes it to the SDK's
    /// market_order builder via `.price()` which skips the internal order book fetch (~1 RTT saved).
    /// For Buy orders, `amount` is USDC to spend. For Sell orders, `amount` is shares to sell.
    async fn execute_market_order(
        &self,
        token_id: &str,
        side: Side,
        amount: rust_decimal::Decimal,
        hint_price: Option<rust_decimal::Decimal>,
    ) -> Result<OrderResult> {
        let client = self.get_client().await?;
        let token = U256::from_str(token_id).context("Invalid token_id (not a valid U256)")?;

        let sdk_amount = match side {
            Side::Buy => Amount::usdc(amount).context("Invalid USDC amount")?,
            _ => Amount::shares(amount).context("Invalid shares amount")?,
        };

        let mut builder = client
            .market_order()
            .token_id(token)
            .amount(sdk_amount)
            .side(side)
            .order_type(SdkOrderType::FOK);

        if let Some(hp) = hint_price {
            builder = builder.price(hp);
            debug!(%hp, "Using hint price (skipping order book fetch)");
        }

        let order = builder.build().await.context("Failed to build market order")?;

        let signed_order = client
            .sign(&self.signer, order)
            .await
            .context("Failed to sign market order")?;

        info!(token_id, %amount, "Submitting market order to CLOB");

        let response = match client.post_order(signed_order).await {
            Ok(r) => r,
            Err(e) => {
                let err_str = format!("{e}");
                if err_str.contains("401") || err_str.contains("403") || err_str.contains("UNAUTHENTICATED") {
                    warn!("Auth error on post_order, refreshing client and retrying");
                    let client = self.refresh_client().await?;
                    let sdk_amount = match side {
                        Side::Buy => Amount::usdc(amount).context("Invalid USDC amount")?,
                        _ => Amount::shares(amount).context("Invalid shares amount")?,
                    };
                    let mut builder = client
                        .market_order()
                        .token_id(token)
                        .amount(sdk_amount)
                        .side(side)
                        .order_type(SdkOrderType::FOK);
                    if let Some(hp) = hint_price {
                        builder = builder.price(hp);
                    }
                    let order = builder.build().await.context("Failed to rebuild market order")?;
                    let signed_order = client
                        .sign(&self.signer, order)
                        .await
                        .context("Failed to re-sign market order")?;
                    client.post_order(signed_order).await.context("Retry post_order failed")?
                } else {
                    error!(error = %e, token_id, %amount, "CLOB post_order failed");
                    return Err(e).context("Failed to post market order");
                }
            }
        };

        info!(
            order_id = %response.order_id,
            success = response.success,
            status = ?response.status,
            "Market order response"
        );

        Self::to_order_result(response, amount)
    }

    async fn execute_limit_order(
        &self,
        token_id: &str,
        side: Side,
        price: rust_decimal::Decimal,
        size: rust_decimal::Decimal,
    ) -> Result<OrderResult> {
        let client = self.get_client().await?;
        let token = U256::from_str(token_id).context("Invalid token_id (not a valid U256)")?;

        let order = client
            .limit_order()
            .token_id(token)
            .size(size)
            .price(price)
            .side(side)
            .build()
            .await
            .context("Failed to build limit order")?;

        let signed_order = client
            .sign(&self.signer, order)
            .await
            .context("Failed to sign limit order")?;

        info!(token_id, %price, %size, "Submitting limit order to CLOB");

        let response = match client.post_order(signed_order).await {
            Ok(r) => r,
            Err(e) => {
                let err_str = format!("{e}");
                if err_str.contains("401") || err_str.contains("403") || err_str.contains("UNAUTHENTICATED") {
                    warn!("Auth error on post_order, refreshing client and retrying");
                    let client = self.refresh_client().await?;
                    let order = client
                        .limit_order()
                        .token_id(token)
                        .size(size)
                        .price(price)
                        .side(side)
                        .build()
                        .await
                        .context("Failed to rebuild limit order")?;
                    let signed_order = client
                        .sign(&self.signer, order)
                        .await
                        .context("Failed to re-sign limit order")?;
                    client.post_order(signed_order).await.context("Retry post_order failed")?
                } else {
                    error!(error = %e, token_id, %price, %size, "CLOB post_order failed");
                    return Err(e).context("Failed to post limit order");
                }
            }
        };

        info!(
            order_id = %response.order_id,
            success = response.success,
            status = ?response.status,
            "Limit order response"
        );

        Self::to_order_result(response, size)
    }

    fn to_order_result(
        response: polymarket_client_sdk_v2::clob::types::response::PostOrderResponse,
        requested_amount: rust_decimal::Decimal,
    ) -> Result<OrderResult> {
        if !response.success {
            let reason = response.error_msg.unwrap_or_else(|| format!("{:?}", response.status));
            warn!(reason = reason.as_str(), "Order rejected by CLOB");
            return Ok(OrderResult::Rejected { reason });
        }

        match response.status {
            OrderStatusType::Matched => {
                let making = response.making_amount.to_f64().unwrap_or(0.0);
                let taking = response.taking_amount.to_f64().unwrap_or(0.0);
                let fill_price = if taking > 0.0 { making / taking } else { 0.0 };
                info!(fill_price, shares = taking, "Order MATCHED");
                Ok(OrderResult::Filled { fill_price, shares: taking })
            }
            OrderStatusType::Delayed => {
                let shares = requested_amount.to_f64().unwrap_or(0.0);
                info!(shares, "Order DELAYED (accepted, pending match)");
                Ok(OrderResult::Filled { fill_price: 0.0, shares })
            }
            OrderStatusType::Live => {
                let shares = requested_amount.to_f64().unwrap_or(0.0);
                info!(shares, order_id = %response.order_id, "Order LIVE on book");
                Ok(OrderResult::Filled { fill_price: 0.0, shares })
            }
            OrderStatusType::Canceled | OrderStatusType::Unmatched => {
                let reason = format!("Order status: {:?}", response.status);
                warn!(reason = reason.as_str(), "Order not filled");
                Ok(OrderResult::Rejected { reason })
            }
            _ => {
                let reason = format!("Unexpected order status: {:?}", response.status);
                error!(reason = reason.as_str(), "Unknown order status");
                Ok(OrderResult::Rejected { reason })
            }
        }
    }
}
