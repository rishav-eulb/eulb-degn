use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use tracing::info;

/// Tracks filled orders and computes running PnL.
#[derive(Debug)]
pub struct OrderTracker {
    trades: VecDeque<TradeRecord>,
    daily_pnl: f64,
    total_pnl: f64,
    total_trades: u64,
    winning_trades: u64,
    losing_trades: u64,
    consecutive_losses: u32,
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub asset: String,
    pub leg1_price: f64,
    pub leg2_price: Option<f64>,
    pub shares: f64,
    pub pnl: f64,
    pub outcome: TradeOutcome,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeOutcome {
    Locked,
    ForceClose,
    Expired,
}

impl OrderTracker {
    pub fn new() -> Self {
        Self {
            trades: VecDeque::new(),
            daily_pnl: 0.0,
            total_pnl: 0.0,
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            consecutive_losses: 0,
        }
    }

    /// Record a completed trade (locked or force-closed).
    pub fn record_trade(&mut self, record: TradeRecord) {
        info!(
            asset = %record.asset,
            pnl = record.pnl,
            outcome = ?record.outcome,
            total_pnl = self.total_pnl + record.pnl,
            "Trade completed"
        );

        self.daily_pnl += record.pnl;
        self.total_pnl += record.pnl;
        self.total_trades += 1;

        if record.pnl > 0.0 {
            self.winning_trades += 1;
            self.consecutive_losses = 0;
        } else {
            self.losing_trades += 1;
            self.consecutive_losses += 1;
        }

        self.trades.push_back(record);

        // Keep last 1000 trades in memory
        while self.trades.len() > 1000 {
            self.trades.pop_front();
        }
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    pub fn total_pnl(&self) -> f64 {
        self.total_pnl
    }

    pub fn consecutive_losses(&self) -> u32 {
        self.consecutive_losses
    }

    pub fn win_rate(&self) -> f64 {
        if self.total_trades == 0 {
            return 0.0;
        }
        self.winning_trades as f64 / self.total_trades as f64
    }

    pub fn total_trades(&self) -> u64 {
        self.total_trades
    }

    pub fn winning_trades(&self) -> u64 {
        self.winning_trades
    }

    pub fn losing_trades(&self) -> u64 {
        self.losing_trades
    }

    pub fn total_realised_pnl(&self) -> f64 {
        self.total_pnl
    }

    /// Reset daily PnL (call at midnight UTC).
    pub fn reset_daily(&mut self) {
        self.daily_pnl = 0.0;
    }
}

impl Default for OrderTracker {
    fn default() -> Self {
        Self::new()
    }
}
