use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// A level-2 orderbook snapshot from Hyperliquid.
#[derive(Debug, Clone)]
pub struct HlBookSnapshot {
    pub asset: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: DateTime<Utc>,
}

/// A single price level (price, size).
#[derive(Debug, Clone, Copy)]
pub struct PriceLevel {
    pub price: f64,
    pub size: f64,
}

/// A trade event from Hyperliquid.
#[derive(Debug, Clone)]
pub struct HlTrade {
    pub asset: String,
    pub price: f64,
    pub size: f64,
    pub side: TradeSide,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Events emitted by the Hyperliquid feed.
#[derive(Debug, Clone)]
pub enum HlEvent {
    Book(HlBookSnapshot),
    Trade(HlTrade),
}

#[derive(Debug, Deserialize)]
struct WsResponse {
    channel: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct L2BookData {
    coin: String,
    levels: Vec<Vec<LevelEntry>>,
    time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LevelEntry {
    px: String,
    sz: String,
    n: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TradesData {
    #[serde(default)]
    trades: Vec<TradeEntry>,
}

#[derive(Debug, Deserialize)]
struct TradeEntry {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
}

/// Spawns a Hyperliquid WebSocket that streams L2 book snapshots and trades.
pub async fn run_hyperliquid_feed(
    ws_url: &str,
    assets: &[String],
    tx: mpsc::UnboundedSender<HlEvent>,
) -> Result<()> {
    loop {
        info!("Connecting to Hyperliquid WebSocket...");

        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Hyperliquid WebSocket");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to L2 book for each asset
                for asset in assets {
                    let sub_msg = serde_json::json!({
                        "method": "subscribe",
                        "subscription": {
                            "type": "l2Book",
                            "coin": asset
                        }
                    });
                    if let Err(e) = write
                        .send(Message::Text(sub_msg.to_string()))
                        .await
                    {
                        error!(asset, error = %e, "Failed to subscribe to HL L2 book");
                    }

                    // Subscribe to trades
                    let trade_sub = serde_json::json!({
                        "method": "subscribe",
                        "subscription": {
                            "type": "trades",
                            "coin": asset
                        }
                    });
                    if let Err(e) = write
                        .send(Message::Text(trade_sub.to_string()))
                        .await
                    {
                        error!(asset, error = %e, "Failed to subscribe to HL trades");
                    }
                }

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = process_hl_message(&text, &tx) {
                                debug!(error = %e, "Failed to process HL message");
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Hyperliquid WS closed by server");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Hyperliquid WS read error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Hyperliquid WS");
            }
        }

        warn!("Hyperliquid WS disconnected, reconnecting in 3s...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

fn process_hl_message(
    text: &str,
    tx: &mpsc::UnboundedSender<HlEvent>,
) -> Result<()> {
    let resp: WsResponse =
        serde_json::from_str(text).context("Parse HL WS message")?;

    match resp.channel.as_deref() {
        Some("l2Book") => {
            if let Some(data) = resp.data {
                let book: L2BookData =
                    serde_json::from_value(data).context("Parse L2 book")?;
                let snapshot = parse_book_snapshot(book);
                let _ = tx.send(HlEvent::Book(snapshot));
            }
        }
        Some("trades") => {
            if let Some(data) = resp.data {
                let trades: Vec<TradeEntry> =
                    serde_json::from_value(data).context("Parse trades")?;
                for entry in trades {
                    let trade = parse_trade(entry);
                    let _ = tx.send(HlEvent::Trade(trade));
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn parse_book_snapshot(book: L2BookData) -> HlBookSnapshot {
    let ts = book
        .time
        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
        .unwrap_or_else(Utc::now);

    let bids = book
        .levels
        .first()
        .map(|levels| {
            levels
                .iter()
                .filter_map(|l| {
                    Some(PriceLevel {
                        price: l.px.parse().ok()?,
                        size: l.sz.parse().ok()?,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let asks = book
        .levels
        .get(1)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|l| {
                    Some(PriceLevel {
                        price: l.px.parse().ok()?,
                        size: l.sz.parse().ok()?,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    HlBookSnapshot {
        asset: book.coin,
        bids,
        asks,
        timestamp: ts,
    }
}

fn parse_trade(entry: TradeEntry) -> HlTrade {
    let ts = DateTime::from_timestamp_millis(entry.time as i64)
        .unwrap_or_else(Utc::now);

    HlTrade {
        asset: entry.coin,
        price: entry.px.parse().unwrap_or(0.0),
        size: entry.sz.parse().unwrap_or(0.0),
        side: if entry.side == "B" {
            TradeSide::Buy
        } else {
            TradeSide::Sell
        },
        timestamp: ts,
    }
}
