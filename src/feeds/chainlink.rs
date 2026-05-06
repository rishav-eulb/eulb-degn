use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// A price update from Chainlink Data Streams.
#[derive(Debug, Clone)]
pub struct ChainlinkPrice {
    pub asset: String,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct StreamReport {
    #[serde(rename = "feedID")]
    feed_id: String,
    #[serde(rename = "benchmarkPrice")]
    benchmark_price: String,
    #[serde(rename = "observationsTimestamp")]
    observations_timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct WsMessage {
    report: Option<StreamReport>,
}

/// Configuration for a single Chainlink feed subscription.
#[derive(Debug, Clone)]
pub struct FeedConfig {
    pub asset: String,
    pub feed_id: String,
}

/// Spawns a Chainlink WebSocket connection that streams BTC/USD and ETH/USD prices.
/// Sends `ChainlinkPrice` updates through the provided channel.
pub async fn run_chainlink_feed(
    ws_url: &str,
    api_key: &str,
    api_secret: &str,
    feeds: Vec<FeedConfig>,
    tx: mpsc::UnboundedSender<ChainlinkPrice>,
) -> Result<()> {
    let auth_url = format!(
        "{}?api_key={}&api_secret={}",
        ws_url, api_key, api_secret
    );

    loop {
        info!("Connecting to Chainlink Data Streams WebSocket...");

        match connect_async(&auth_url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Chainlink Data Streams");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to feeds
                let feed_ids: Vec<&str> =
                    feeds.iter().map(|f| f.feed_id.as_str()).collect();
                let subscribe_msg = serde_json::json!({
                    "type": "subscribe",
                    "feedIDs": feed_ids
                });
                if let Err(e) = write
                    .send(Message::Text(subscribe_msg.to_string()))
                    .await
                {
                    error!(error = %e, "Failed to subscribe to Chainlink feeds");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Err(e) =
                                process_message(&text, &feeds, &tx)
                            {
                                debug!(error = %e, "Failed to process Chainlink message");
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Chainlink WS closed by server");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Chainlink WS read error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Chainlink WS");
            }
        }

        warn!("Chainlink WS disconnected, reconnecting in 3s...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

fn process_message(
    text: &str,
    feeds: &[FeedConfig],
    tx: &mpsc::UnboundedSender<ChainlinkPrice>,
) -> Result<()> {
    let msg: WsMessage =
        serde_json::from_str(text).context("Parse Chainlink WS message")?;

    if let Some(report) = msg.report {
        let asset = feeds
            .iter()
            .find(|f| f.feed_id == report.feed_id)
            .map(|f| f.asset.clone())
            .unwrap_or_else(|| "UNKNOWN".to_string());

        let price: f64 = report
            .benchmark_price
            .parse()
            .context("Parse benchmark price")?;

        let timestamp = DateTime::from_timestamp(report.observations_timestamp, 0)
            .unwrap_or_else(Utc::now);

        let update = ChainlinkPrice {
            asset,
            price,
            timestamp,
        };

        debug!(
            asset = %update.asset,
            price = update.price,
            "Chainlink price update"
        );

        let _ = tx.send(update);
    }

    Ok(())
}
