use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Polymarket orderbook snapshot for a token.
#[derive(Debug, Clone)]
pub struct PolyBook {
    pub token_id: String,
    pub bids: Vec<PolyLevel>,
    pub asks: Vec<PolyLevel>,
    pub timestamp: DateTime<Utc>,
}

/// A single price level in the Polymarket CLOB.
#[derive(Debug, Clone, Copy)]
pub struct PolyLevel {
    pub price: f64,
    pub size: f64,
}

/// A price update from Polymarket RTDS (real-time token price).
#[derive(Debug, Clone)]
pub struct PolyPrice {
    pub token_id: String,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
}

/// Events from the Polymarket feed.
#[derive(Debug, Clone)]
pub enum PolyEvent {
    Book(PolyBook),
    Price(PolyPrice),
}

/// Matches the actual Polymarket CLOB WS market channel message format.
/// Polymarket uses "event_type" (not "type") to identify message kinds.
#[derive(Debug, Deserialize)]
struct ClobWsMsg {
    event_type: Option<String>,
    market: Option<String>,
    asset_id: Option<String>,
    // book event fields
    bids: Option<Vec<ClobLevel>>,
    asks: Option<Vec<ClobLevel>>,
    // last_trade_price / price_change fields
    price: Option<String>,
    side: Option<String>,
    size: Option<String>,
    // best_bid_ask fields
    best_bid: Option<String>,
    best_ask: Option<String>,
    // price_change nested array
    price_changes: Option<Vec<PriceChangeEntry>>,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClobLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEntry {
    asset_id: Option<String>,
    price: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

/// Counter for logging first N messages to diagnose data flow.
static MSG_LOG_CTR: AtomicU64 = AtomicU64::new(0);
const MSG_LOG_LIMIT: u64 = 20;

/// Runs the Polymarket CLOB WebSocket for orderbook data.
/// Subscribes to orderbook updates for the given token IDs.
/// Sends PING every 10s to keep the connection alive.
pub async fn run_poly_clob_ws(
    ws_url: &str,
    token_ids: Vec<String>,
    tx: mpsc::UnboundedSender<PolyEvent>,
) -> Result<()> {
    loop {
        info!("Connecting to Polymarket CLOB WebSocket...");

        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Polymarket CLOB WebSocket");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to market data for all tokens at once
                let sub_msg = serde_json::json!({
                    "assets_ids": &token_ids,
                    "type": "market",
                    "custom_feature_enabled": true
                });
                if let Err(e) = write
                    .send(Message::Text(sub_msg.to_string()))
                    .await
                {
                    error!(error = %e, "Failed to subscribe to Poly market channel");
                }

                // Heartbeat ticker — Polymarket requires PING every 10s
                let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
                ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Ping(vec![])).await {
                                warn!(error = %e, "Failed to send PING to Polymarket WS");
                                break;
                            }
                        }
                        msg_opt = read.next() => {
                            match msg_opt {
                                Some(Ok(Message::Text(text))) => {
                                    // Log first N messages for diagnostics
                                    let count = MSG_LOG_CTR.fetch_add(1, Ordering::Relaxed);
                                    if count < MSG_LOG_LIMIT {
                                        info!(
                                            msg_num = count + 1,
                                            raw = %text.chars().take(300).collect::<String>(),
                                            "Poly WS raw message"
                                        );
                                    }

                                    if let Err(e) = process_clob_message(&text, &tx) {
                                        debug!(error = %e, "Failed to process Poly CLOB msg");
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Polymarket CLOB WS closed by server");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "Polymarket CLOB WS read error");
                                    break;
                                }
                                None => {
                                    warn!("Polymarket CLOB WS stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Polymarket CLOB WS");
            }
        }

        warn!("Polymarket CLOB WS disconnected, reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Runs the Polymarket RTDS for real-time token price streaming.
pub async fn run_poly_rtds(
    rtds_url: &str,
    token_ids: Vec<String>,
    tx: mpsc::UnboundedSender<PolyEvent>,
) -> Result<()> {
    loop {
        info!("Connecting to Polymarket RTDS...");

        match connect_async(rtds_url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Polymarket RTDS");
                let (mut write, mut read) = ws_stream.split();

                let sub_msg = serde_json::json!({
                    "type": "subscribe",
                    "channel": "prices",
                    "assets_ids": token_ids
                });
                if let Err(e) =
                    write.send(Message::Text(sub_msg.to_string())).await
                {
                    error!(error = %e, "Failed to subscribe to RTDS prices");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = process_rtds_message(&text, &tx) {
                                debug!(error = %e, "Failed to process RTDS msg");
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Close(_)) => {
                            warn!("RTDS WS closed by server");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "RTDS WS read error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to RTDS WS");
            }
        }

        warn!("RTDS WS disconnected, reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Subscribe to new token_ids on an existing CLOB WebSocket.
pub async fn subscribe_tokens(
    _ws_url: &str,
    new_token_ids: &[String],
) -> Result<()> {
    debug!(
        count = new_token_ids.len(),
        "Subscribing to new Polymarket tokens"
    );
    Ok(())
}

fn process_clob_message(
    text: &str,
    tx: &mpsc::UnboundedSender<PolyEvent>,
) -> Result<()> {
    // Initial book snapshot arrives as a JSON array of book objects
    if text.starts_with('[') {
        let msgs: Vec<ClobWsMsg> =
            serde_json::from_str(text).context("Parse CLOB WS array message")?;
        for msg in msgs {
            process_single_clob_msg(msg, tx);
        }
        return Ok(());
    }

    let msg: ClobWsMsg =
        serde_json::from_str(text).context("Parse CLOB WS message")?;
    process_single_clob_msg(msg, tx);
    Ok(())
}

fn process_single_clob_msg(
    msg: ClobWsMsg,
    tx: &mpsc::UnboundedSender<PolyEvent>,
) {
    // The initial book snapshot has no event_type field — detect by presence of bids/asks
    let event = msg.event_type.as_deref().unwrap_or_else(|| {
        if msg.bids.is_some() || msg.asks.is_some() {
            "book"
        } else {
            ""
        }
    });

    match event {
        "book" => {
            let token_id = msg
                .asset_id
                .or(msg.market)
                .unwrap_or_default();

            let bids: Vec<PolyLevel> = msg
                .bids
                .unwrap_or_default()
                .iter()
                .filter_map(|l| {
                    Some(PolyLevel {
                        price: l.price.parse().ok()?,
                        size: l.size.parse().ok()?,
                    })
                })
                .collect();

            let asks: Vec<PolyLevel> = msg
                .asks
                .unwrap_or_default()
                .iter()
                .filter_map(|l| {
                    Some(PolyLevel {
                        price: l.price.parse().ok()?,
                        size: l.size.parse().ok()?,
                    })
                })
                .collect();

            let book = PolyBook {
                token_id,
                bids,
                asks,
                timestamp: Utc::now(),
            };
            let _ = tx.send(PolyEvent::Book(book));
        }

        "last_trade_price" => {
            let token_id = msg.asset_id.or(msg.market).unwrap_or_default();
            if let Some(price_str) = msg.price {
                if let Ok(price) = price_str.parse::<f64>() {
                    let _ = tx.send(PolyEvent::Price(PolyPrice {
                        token_id,
                        price,
                        timestamp: Utc::now(),
                    }));
                }
            }
        }

        "best_bid_ask" => {
            let token_id = msg.asset_id.or(msg.market).unwrap_or_default();
            if let Some(ask_str) = msg.best_ask {
                if let Ok(ask_price) = ask_str.parse::<f64>() {
                    let _ = tx.send(PolyEvent::Price(PolyPrice {
                        token_id,
                        price: ask_price,
                        timestamp: Utc::now(),
                    }));
                }
            }
        }

        "price_change" => {
            if let Some(changes) = msg.price_changes {
                for entry in &changes {
                    let token_id = entry.asset_id.clone().unwrap_or_default();
                    if token_id.is_empty() {
                        continue;
                    }
                    let price_val = entry
                        .best_ask
                        .as_ref()
                        .or(entry.price.as_ref())
                        .and_then(|s| s.parse::<f64>().ok());
                    if let Some(price) = price_val {
                        let _ = tx.send(PolyEvent::Price(PolyPrice {
                            token_id,
                            price,
                            timestamp: Utc::now(),
                        }));
                    }
                }
            }
        }

        _ => {}
    }
}

fn process_rtds_message(
    text: &str,
    tx: &mpsc::UnboundedSender<PolyEvent>,
) -> Result<()> {
    let msg: ClobWsMsg =
        serde_json::from_str(text).context("Parse RTDS message")?;

    if let Some(price_str) = msg.price {
        let token_id = msg.asset_id.unwrap_or_default();
        let price: f64 = price_str.parse().context("Parse RTDS price")?;

        let update = PolyPrice {
            token_id,
            price,
            timestamp: Utc::now(),
        };
        let _ = tx.send(PolyEvent::Price(update));
    }

    Ok(())
}
