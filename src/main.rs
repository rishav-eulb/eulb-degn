mod config;
mod execution;
mod feeds;
mod indicators;
mod market_discovery;
mod strategy;
mod utils;

use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::execution::{
    ActiveTrade, OrderExecutor, OrderRequest, OrderRequestKind, OrderResult, OrderTracker,
    PortfolioState, RedeemCommand, Redeemer, RiskManager, RiskStatus,
};
use crate::feeds::chainlink::FeedConfig;
use crate::feeds::{HlEvent, PolyEvent};
use crate::indicators::{BookImbalanceTracker, MomentumTracker, PriceAggregator};
use crate::market_discovery::MarketInfo;
use crate::strategy::{LegAction, LegManager, StrategyParams, evaluate_entry};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = config::Config::from_env()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    format!("polymarket_bot={}", cfg.log_level).parse().unwrap()
                }),
        )
        .compact()
        .init();

    info!("Polymarket Two-Leg Trading Bot starting...");
    info!(
        dry_run = cfg.dry_run,
        assets = ?cfg.assets,
        trade_size = cfg.trade_size_usd,
        "Configuration loaded"
    );

    run_bot(cfg).await
}

async fn run_bot(cfg: config::Config) -> Result<()> {
    // --- Channels ---
    let (hl_tx, mut hl_rx) = mpsc::unbounded_channel::<HlEvent>();
    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<PolyEvent>();

    // --- Conditionally spawn Chainlink feed ---
    let chainlink_enabled = !cfg.chainlink_api_key.is_empty()
        && !cfg.chainlink_api_secret.is_empty();

    if chainlink_enabled {
        let chainlink_feeds = vec![
            FeedConfig {
                asset: "BTC".to_string(),
                feed_id: cfg.chainlink_btc_feed_id.clone(),
            },
            FeedConfig {
                asset: "ETH".to_string(),
                feed_id: cfg.chainlink_eth_feed_id.clone(),
            },
        ];
        let (chainlink_tx, mut chainlink_rx) =
            mpsc::unbounded_channel::<feeds::chainlink::ChainlinkPrice>();
        let cl_url = cfg.chainlink_ws_url.clone();
        let cl_key = cfg.chainlink_api_key.clone();
        let cl_secret = cfg.chainlink_api_secret.clone();
        tokio::spawn(async move {
            if let Err(e) =
                feeds::run_chainlink_feed(&cl_url, &cl_key, &cl_secret, chainlink_feeds, chainlink_tx)
                    .await
            {
                error!(error = %e, "Chainlink feed fatal error");
            }
        });
        tokio::spawn(async move {
            while chainlink_rx.recv().await.is_some() {}
        });
        info!("Chainlink price feed enabled");
    } else {
        info!("Chainlink not configured — using Hyperliquid as primary price source");
    }

    // --- Spawn Hyperliquid feed (always active — primary price + microstructure) ---
    let hl_url = cfg.hyperliquid_ws_url.clone();
    let hl_assets = cfg.hyperliquid_assets.clone();
    tokio::spawn(async move {
        if let Err(e) = feeds::run_hyperliquid_feed(&hl_url, &hl_assets, hl_tx).await {
            error!(error = %e, "Hyperliquid feed fatal error");
        }
    });

    // --- Core state ---
    let mut price_agg = PriceAggregator::new(chainlink_enabled);
    let mut momentum_btc = MomentumTracker::new(5, 15);
    let mut momentum_eth = MomentumTracker::new(5, 15);
    let mut book_imbalance = BookImbalanceTracker::new(5);
    let tracker = OrderTracker::new();
    let mut risk_mgr = RiskManager::new(
        cfg.max_concurrent_positions,
        cfg.max_daily_loss_usd,
        cfg.cooldown_after_losses,
        cfg.cooldown_duration_secs,
    );

    // --- Order executor: cached auth + background task ---
    let executor = OrderExecutor::new(
        cfg.polymarket_clob_url.clone(),
        cfg.polymarket_private_key.clone(),
        cfg.polymarket_funder_address.clone(),
        cfg.dry_run,
    )?;
    if !cfg.dry_run {
        if let Err(e) = executor.warm().await {
            warn!(error = %e, "Failed to pre-warm CLOB client (will retry on first order)");
        }
    }
    let (order_tx, mut order_rx) = executor.spawn();

    let redeemer = Redeemer::new(
        cfg.polymarket_clob_url.clone(),
        cfg.polymarket_private_key.clone(),
        cfg.dry_run,
    );
    let redeem_tx = redeemer.spawn();

    // --- Active market state per asset ---
    let mut active_markets: std::collections::HashMap<String, ActiveMarketState> =
        std::collections::HashMap::new();

    // --- Market discovery loop ---
    let gamma_url = cfg.polymarket_gamma_url.clone();
    let interval_secs = cfg.market_interval_secs;
    let assets = cfg.assets.clone();
    let poly_ws_url = cfg.polymarket_ws_url.clone();

    let (market_tx, mut market_rx) = mpsc::unbounded_channel::<(String, MarketInfo)>();
    tokio::spawn(async move {
        loop {
            let secs_remaining = market_discovery::secs_remaining(interval_secs);
            let prefetch_advance = 30u64;

            let sleep_secs = secs_remaining.saturating_sub(prefetch_advance);
            if sleep_secs > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            }

            for asset in &assets {
                let slug = market_discovery::next_slug(asset, interval_secs);
                info!(asset, slug = %slug, "Pre-fetching next market");

                if let Some(market_info) =
                    market_discovery::prefetch_market(&gamma_url, &slug, 5).await
                {
                    let _ = market_tx.send((asset.clone(), market_info));
                } else {
                    warn!(asset, slug = %slug, "Failed to pre-fetch market");
                }
            }

            let remaining = market_discovery::secs_remaining(interval_secs);
            if remaining > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(remaining)).await;
            }
        }
    });

    // --- Portfolio state file ---
    let state_file = PathBuf::from("logs/state.json");
    let _trade_log_file = PathBuf::from("logs/trades.jsonl");
    tokio::fs::create_dir_all("logs").await?;
    let boot_time = Utc::now();
    let mut state_ticker = tokio::time::interval(std::time::Duration::from_secs(2));
    state_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // --- Main event loop ---
    info!("Entering main event loop");

    let mut token_prices: std::collections::HashMap<String, f64> =
        std::collections::HashMap::new();

    // Polymarket WS with incremental subscription channel
    let (poly_sub_tx, poly_sub_rx) = mpsc::unbounded_channel::<Vec<String>>();
    let poly_tx_clone = poly_tx.clone();
    let ws_url_clone = poly_ws_url.clone();
    tokio::spawn(async move {
        let _ = feeds::run_poly_clob_ws(&ws_url_clone, poly_tx_clone, poly_sub_rx).await;
    });

    loop {
        tokio::select! {
            // --- Order results from the background executor (non-blocking) ---
            Some((asset_key, kind, result)) = order_rx.recv() => {
                if let Some(state) = active_markets.get_mut(&asset_key) {
                    match kind {
                        OrderRequestKind::Leg1 => {
                            match result {
                                Ok(OrderResult::Filled { fill_price, shares }) => {
                                    info!(fill_price, shares, asset = asset_key.as_str(), "Leg1 filled");
                                    risk_mgr.on_position_opened(&asset_key);
                                    let action = state.leg_manager.on_leg1_fill(fill_price, shares);
                                    if let LegAction::PlaceLeg2 { token_id, price, shares } = &action {
                                        let _ = order_tx.send(OrderRequest::Leg2Limit {
                                            token_id: token_id.clone(),
                                            price: *price,
                                            shares: *shares,
                                            asset_key: asset_key.clone(),
                                        });
                                    }
                                }
                                Ok(OrderResult::Rejected { reason }) => {
                                    warn!(reason, asset = asset_key.as_str(), "Leg1 rejected");
                                    state.leg_manager.on_leg1_rejected();
                                }
                                Err(e) => {
                                    error!(error = %e, asset = asset_key.as_str(), "Leg1 order error");
                                    state.leg_manager.on_leg1_rejected();
                                }
                                _ => {}
                            }
                        }
                        OrderRequestKind::Leg2 => {
                            match result {
                                Ok(OrderResult::Filled { fill_price, .. }) => {
                                    info!(fill_price, asset = asset_key.as_str(), "Leg2 filled — position LOCKED");
                                    state.leg_manager.on_leg2_fill(fill_price);
                                }
                                Ok(OrderResult::Rejected { reason }) => {
                                    warn!(reason, asset = asset_key.as_str(), "Leg2 rejected");
                                    let action = state.leg_manager.on_leg2_rejected();
                                    if let LegAction::PlaceLeg2 { token_id, price, shares } = &action {
                                        let _ = order_tx.send(OrderRequest::Leg2Limit {
                                            token_id: token_id.clone(),
                                            price: *price,
                                            shares: *shares,
                                            asset_key: asset_key.clone(),
                                        });
                                    }
                                }
                                Err(e) => {
                                    error!(error = ?e, asset = asset_key.as_str(), "Leg2 order error (not retrying)");
                                }
                                _ => {}
                            }
                        }
                        OrderRequestKind::ForceClose => {
                            match result {
                                Ok(OrderResult::Filled { fill_price, .. }) => {
                                    info!(fill_price, asset = asset_key.as_str(), "Force-close executed");
                                    risk_mgr.on_position_closed(&asset_key);
                                }
                                Err(e) => {
                                    error!(error = %e, asset = asset_key.as_str(), "Force-close error");
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            // Periodic portfolio state dump (offloaded to background task)
            _ = state_ticker.tick() => {
                let now = Utc::now();
                let active_trades: Vec<ActiveTrade> = active_markets.iter().filter_map(|(asset, state)| {
                    if state.leg_manager.is_idle() {
                        return None;
                    }
                    let secs_since_entry = (now - state.open_ts).num_seconds().max(0) as u64;
                    let secs_to_expiry = interval_secs.saturating_sub(secs_since_entry);
                    Some(ActiveTrade {
                        asset: asset.clone(),
                        market_slug: state.market_info.slug.clone(),
                        direction: state.leg_manager.current_direction()
                            .map(|d| format!("{:?}", d))
                            .unwrap_or_else(|| "None".to_string()),
                        leg1_side: state.leg_manager.current_leg1_side()
                            .map(|s| format!("{:?}", s))
                            .unwrap_or_else(|| "None".to_string()),
                        leg1_price: state.leg_manager.leg1_fill_price().unwrap_or(0.0),
                        leg1_shares: state.leg_manager.leg1_fill_shares().unwrap_or(0.0),
                        leg2_status: state.leg_manager.leg2_status(),
                        leg2_price: state.leg_manager.leg2_fill_price(),
                        unrealised_pnl: state.leg_manager.unrealised_pnl(
                            token_prices.get(&state.market_info.yes_token_id).copied().unwrap_or(0.5),
                            token_prices.get(&state.market_info.no_token_id).copied().unwrap_or(0.5),
                        ),
                        secs_since_entry,
                        secs_to_expiry,
                    })
                }).collect();

                let unrealised_total: f64 = active_trades.iter().map(|t| t.unrealised_pnl).sum();

                let portfolio = PortfolioState {
                    timestamp: now,
                    uptime_secs: (now - boot_time).num_seconds().max(0) as u64,
                    total_realised_pnl: tracker.total_realised_pnl(),
                    unrealised_pnl: unrealised_total,
                    daily_pnl: tracker.daily_pnl(),
                    total_trades: tracker.total_trades(),
                    winning_trades: tracker.winning_trades(),
                    losing_trades: tracker.losing_trades(),
                    win_rate: tracker.win_rate(),
                    active_trades,
                    risk_status: RiskStatus {
                        daily_loss_limit: cfg.max_daily_loss_usd,
                        daily_loss_remaining: cfg.max_daily_loss_usd + tracker.daily_pnl(),
                        is_halted: risk_mgr.is_halted(),
                        cooldown_active: risk_mgr.cooldown_active(),
                        consecutive_losses: tracker.consecutive_losses(),
                        open_positions_btc: risk_mgr.open_positions("BTC"),
                        open_positions_eth: risk_mgr.open_positions("ETH"),
                    },
                };
                let sf = state_file.clone();
                tokio::spawn(async move {
                    execution::portfolio::write_state(&portfolio, &sf).await;
                });
            }

            // New market discovered
            Some(msg) = market_rx.recv() => {
                let (asset, market_info): (String, MarketInfo) = msg;
                info!(
                    asset = asset.as_str(),
                    slug = market_info.slug.as_str(),
                    yes_token = market_info.yes_token_id.as_str(),
                    no_token = market_info.no_token_id.as_str(),
                    "New market activated"
                );

                if let Some(prev_state) = active_markets.get(&asset) {
                    let market_end = prev_state.open_ts
                        + chrono::Duration::seconds(interval_secs as i64);
                    let _ = redeem_tx.send(RedeemCommand::Schedule {
                        condition_id: prev_state.market_info.condition_id.clone(),
                        slug: prev_state.market_info.slug.clone(),
                        market_end_ts: market_end,
                    });
                }

                price_agg.snapshot_open_price(&asset);

                let params = StrategyParams::from_config(&cfg, &asset);
                let leg_mgr = LegManager::new(
                    params.clone(),
                    market_info.yes_token_id.clone(),
                    market_info.no_token_id.clone(),
                    Utc::now(),
                    interval_secs,
                );

                active_markets.insert(asset, ActiveMarketState {
                    market_info: market_info.clone(),
                    leg_manager: leg_mgr,
                    params,
                    open_ts: Utc::now(),
                });

                let new_token_ids = vec![
                    market_info.yes_token_id.clone(),
                    market_info.no_token_id.clone(),
                ];
                let _ = poly_sub_tx.send(new_token_ids);
            }

            // Hyperliquid event (primary price source + microstructure)
            Some(event) = hl_rx.recv() => {
                match event {
                    HlEvent::Book(snapshot) => {
                        let mid = if !snapshot.bids.is_empty() && !snapshot.asks.is_empty() {
                            (snapshot.bids[0].price + snapshot.asks[0].price) / 2.0
                        } else {
                            0.0
                        };
                        if mid > 0.0 {
                            price_agg.update_hl_mid(&snapshot.asset, mid, snapshot.timestamp);

                            match snapshot.asset.as_str() {
                                "BTC" => momentum_btc.push(mid, snapshot.timestamp),
                                "ETH" => momentum_eth.push(mid, snapshot.timestamp),
                                _ => {}
                            }
                        }
                        book_imbalance.update(&snapshot);

                        let asset_key = &snapshot.asset;
                        if let Some(state) = active_markets.get_mut(asset_key) {
                            if state.leg_manager.is_idle() {
                                if let Some(price_state) = price_agg.get_state(asset_key) {
                                    if let Some(imb) = book_imbalance.get(asset_key) {
                                        let now = Utc::now();
                                        let secs_open = (now - state.open_ts).num_seconds().max(0) as u64;
                                        static TICK_CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                        let tick = TICK_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        if tick % 50 == 0 {
                                            let yes_p = token_prices
                                                .get(&state.market_info.yes_token_id)
                                                .copied()
                                                .unwrap_or(0.5);
                                            let no_p = token_prices
                                                .get(&state.market_info.no_token_id)
                                                .copied()
                                                .unwrap_or(0.5);
                                            info!(
                                                asset = asset_key.as_str(),
                                                bps_from_open = price_state.bps_from_open,
                                                imbalance = imb.imbalance,
                                                yes_price = yes_p,
                                                no_price = no_p,
                                                secs_since_open = secs_open,
                                                slug = state.market_info.slug.as_str(),
                                                "Signal eval tick"
                                            );
                                        }
                                        let yes_price = token_prices
                                            .get(&state.market_info.yes_token_id)
                                            .copied()
                                            .unwrap_or(0.5);
                                        let no_price = token_prices
                                            .get(&state.market_info.no_token_id)
                                            .copied()
                                            .unwrap_or(0.5);

                                        let secs_since_open =
                                            (now - state.open_ts).num_seconds().max(0) as u64;

                                        if let Some(signal) = evaluate_entry(
                                            &price_state,
                                            imb,
                                            yes_price,
                                            no_price,
                                            secs_since_open,
                                            &state.params,
                                        ) {
                                            if let Err(veto) = risk_mgr.check_entry(
                                                asset_key,
                                                tracker.daily_pnl(),
                                                tracker.consecutive_losses(),
                                            ) {
                                                warn!(asset = asset_key.as_str(), ?veto, "Entry vetoed by risk manager");
                                            } else {
                                                let action = state.leg_manager.on_signal(signal);
                                                if let LegAction::PlaceLeg1 { token_id, size_usd, .. } = &action {
                                                    let hint_price = token_prices.get(token_id).copied();
                                                    let _ = order_tx.send(OrderRequest::Leg1Market {
                                                        token_id: token_id.clone(),
                                                        size_usd: *size_usd,
                                                        asset_key: asset_key.clone(),
                                                        hint_price,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    HlEvent::Trade(_trade) => {}
                }
            }

            // Polymarket event
            Some(event) = poly_rx.recv() => {
                match event {
                    PolyEvent::Book(book) => {
                        if let Some(best_ask) = book.asks.first() {
                            token_prices.insert(book.token_id.clone(), best_ask.price);
                        }

                        let now = Utc::now();
                        for (asset_key, state) in active_markets.iter_mut() {
                            let opposite_token_id = match state.leg_manager.state {
                                strategy::LegState::Leg2Pending { ref signal, .. } => {
                                    match signal.leg1_side {
                                        strategy::Leg1Side::Yes => &state.market_info.no_token_id,
                                        strategy::Leg1Side::No => &state.market_info.yes_token_id,
                                    }
                                }
                                _ => continue,
                            };

                            if book.token_id == *opposite_token_id {
                                if let Some(best_ask) = book.asks.first() {
                                    let action = state.leg_manager.on_tick(best_ask.price, now);
                                    if let LegAction::ForceCloseLeg1 { token_id, shares } = &action {
                                        let _ = order_tx.send(OrderRequest::ForceClose {
                                            token_id: token_id.clone(),
                                            shares: *shares,
                                            asset_key: asset_key.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    PolyEvent::Price(price_update) => {
                        token_prices.insert(price_update.token_id, price_update.price);
                    }
                }
            }
        }
    }
}

struct ActiveMarketState {
    market_info: MarketInfo,
    leg_manager: LegManager,
    params: StrategyParams,
    open_ts: chrono::DateTime<Utc>,
}
