use anyhow::Result;
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

/// Single-pass tagged deserialization -- avoids intermediate serde_json::Value allocation.
#[derive(Debug, Deserialize)]
#[serde(tag = "channel")]
enum TypedWsResponse {
    #[serde(rename = "l2Book")]
    L2Book { data: L2BookData },
    #[serde(rename = "trades")]
    Trades { data: Vec<TradeEntry> },
}

#[derive(Debug, Deserialize)]
struct L2BookData {
    coin: String,
    levels: Vec<Vec<LevelEntry>>,
    time: Option<u64>,
}

/// Price level with string-to-f64 deserialization done inline via serde helper.
#[derive(Debug, Deserialize)]
struct LevelEntry {
    #[serde(deserialize_with = "deser_f64_from_str")]
    px: f64,
    #[serde(deserialize_with = "deser_f64_from_str")]
    sz: f64,
    #[allow(dead_code)]
    n: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TradeEntry {
    coin: String,
    #[serde(deserialize_with = "deser_f64_from_str")]
    px: f64,
    #[serde(deserialize_with = "deser_f64_from_str")]
    sz: f64,
    side: String,
    time: u64,
}

fn deser_f64_from_str<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = <&str>::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
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

                // Batch all subscriptions and flush once
                for asset in assets {
                    let book_sub = serde_json::json!({
                        "method": "subscribe",
                        "subscription": { "type": "l2Book", "coin": asset }
                    });
                    let trade_sub = serde_json::json!({
                        "method": "subscribe",
                        "subscription": { "type": "trades", "coin": asset }
                    });
                    if let Err(e) = write.feed(Message::Text(book_sub.to_string())).await {
                        error!(asset, error = %e, "Failed to buffer HL L2 book subscription");
                    }
                    if let Err(e) = write.feed(Message::Text(trade_sub.to_string())).await {
                        error!(asset, error = %e, "Failed to buffer HL trades subscription");
                    }
                }
                if let Err(e) = write.flush().await {
                    error!(error = %e, "Failed to flush HL subscription batch");
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
    match serde_json::from_str::<TypedWsResponse>(text) {
        Ok(TypedWsResponse::L2Book { data }) => {
            let snapshot = parse_book_snapshot(data);
            let _ = tx.send(HlEvent::Book(snapshot));
        }
        Ok(TypedWsResponse::Trades { data }) => {
            for entry in data {
                let trade = parse_trade(entry);
                let _ = tx.send(HlEvent::Trade(trade));
            }
        }
        Err(_) => {
            // Non-data messages (subscriptions, pongs, etc.) -- ignore silently
        }
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
                .map(|l| PriceLevel { price: l.px, size: l.sz })
                .collect()
        })
        .unwrap_or_default();

    let asks = book
        .levels
        .get(1)
        .map(|levels| {
            levels
                .iter()
                .map(|l| PriceLevel { price: l.px, size: l.sz })
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
        price: entry.px,
        size: entry.sz,
        side: if entry.side == "B" {
            TradeSide::Buy
        } else {
            TradeSide::Sell
        },
        timestamp: ts,
    }
}
