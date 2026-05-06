use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::Path;
use tokio::fs;
use tracing::error;

/// Live portfolio state written to disk as JSON for monitoring.
#[derive(Debug, Clone, Serialize)]
pub struct PortfolioState {
    pub timestamp: DateTime<Utc>,
    pub uptime_secs: u64,
    pub total_realised_pnl: f64,
    pub unrealised_pnl: f64,
    pub daily_pnl: f64,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub win_rate: f64,
    pub active_trades: Vec<ActiveTrade>,
    pub risk_status: RiskStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveTrade {
    pub asset: String,
    pub market_slug: String,
    pub direction: String,
    pub leg1_side: String,
    pub leg1_price: f64,
    pub leg1_shares: f64,
    pub leg2_status: String,
    pub leg2_price: Option<f64>,
    pub unrealised_pnl: f64,
    pub secs_since_entry: u64,
    pub secs_to_expiry: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskStatus {
    pub daily_loss_limit: f64,
    pub daily_loss_remaining: f64,
    pub is_halted: bool,
    pub cooldown_active: bool,
    pub consecutive_losses: u32,
    pub open_positions_btc: usize,
    pub open_positions_eth: usize,
}

/// Writes the portfolio state to a JSON file.
/// Call this periodically (e.g. every second or on every state change).
pub async fn write_state(state: &PortfolioState, path: &Path) {
    match serde_json::to_string_pretty(state) {
        Ok(json) => {
            if let Err(e) = fs::write(path, json).await {
                error!(error = %e, "Failed to write portfolio state file");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to serialize portfolio state");
        }
    }
}

/// Appends a trade log entry to a JSONL (JSON Lines) trade history file.
pub async fn append_trade_log(entry: &TradeLogEntry, path: &Path) {
    match serde_json::to_string(entry) {
        Ok(line) => {
            use tokio::io::AsyncWriteExt;
            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await
            {
                Ok(mut file) => {
                    let _ = file.write_all(line.as_bytes()).await;
                    let _ = file.write_all(b"\n").await;
                }
                Err(e) => {
                    error!(error = %e, "Failed to open trade log file");
                }
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to serialize trade log entry");
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeLogEntry {
    pub timestamp: DateTime<Utc>,
    pub asset: String,
    pub market_slug: String,
    pub direction: String,
    pub leg1_price: f64,
    pub leg2_price: Option<f64>,
    pub shares: f64,
    pub pnl: f64,
    pub outcome: String,
    pub duration_secs: u64,
}
