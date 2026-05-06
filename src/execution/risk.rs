use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{info, warn};

/// Risk manager enforcing position limits, daily loss caps, and cooldowns.
#[derive(Debug)]
pub struct RiskManager {
    /// Max concurrent open positions per asset.
    max_concurrent_positions: usize,
    /// Max daily loss before halting trading.
    max_daily_loss_usd: f64,
    /// Number of consecutive losses before cooldown triggers.
    cooldown_trigger_losses: u32,
    /// Cooldown duration in seconds.
    cooldown_duration_secs: u64,
    /// Current open positions per asset.
    open_positions: HashMap<String, usize>,
    /// Cooldown expiry time (None = no cooldown active).
    cooldown_until: Option<DateTime<Utc>>,
    /// Whether trading is halted for the day.
    daily_halt: bool,
}

/// Reasons for rejecting a trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskVeto {
    DailyLossLimitReached,
    MaxPositionsReached { asset: String, current: usize },
    CooldownActive { expires_in_secs: u64 },
    TradingHalted,
}

impl RiskManager {
    pub fn new(
        max_concurrent_positions: usize,
        max_daily_loss_usd: f64,
        cooldown_trigger_losses: u32,
        cooldown_duration_secs: u64,
    ) -> Self {
        Self {
            max_concurrent_positions,
            max_daily_loss_usd,
            cooldown_duration_secs,
            cooldown_trigger_losses,
            open_positions: HashMap::new(),
            cooldown_until: None,
            daily_halt: false,
        }
    }

    /// Check if a new trade is allowed for the given asset.
    /// Returns Ok(()) if allowed, Err(RiskVeto) if rejected.
    pub fn check_entry(
        &self,
        asset: &str,
        daily_pnl: f64,
        consecutive_losses: u32,
    ) -> Result<(), RiskVeto> {
        // Daily halt
        if self.daily_halt {
            return Err(RiskVeto::TradingHalted);
        }

        // Daily loss limit
        if daily_pnl <= -self.max_daily_loss_usd {
            return Err(RiskVeto::DailyLossLimitReached);
        }

        // Position limit
        let current = self.open_positions.get(asset).copied().unwrap_or(0);
        if current >= self.max_concurrent_positions {
            return Err(RiskVeto::MaxPositionsReached {
                asset: asset.to_string(),
                current,
            });
        }

        // Cooldown
        if let Some(until) = self.cooldown_until {
            let now = Utc::now();
            if now < until {
                let expires_in = (until - now).num_seconds().max(0) as u64;
                return Err(RiskVeto::CooldownActive {
                    expires_in_secs: expires_in,
                });
            }
        }

        // Check if cooldown should trigger (based on consecutive losses)
        if consecutive_losses >= self.cooldown_trigger_losses {
            return Err(RiskVeto::CooldownActive {
                expires_in_secs: self.cooldown_duration_secs,
            });
        }

        Ok(())
    }

    /// Called when a new position is opened.
    pub fn on_position_opened(&mut self, asset: &str) {
        let count = self.open_positions.entry(asset.to_string()).or_insert(0);
        *count += 1;
    }

    /// Called when a position is closed (locked, force-closed, or expired).
    pub fn on_position_closed(&mut self, asset: &str) {
        if let Some(count) = self.open_positions.get_mut(asset) {
            *count = count.saturating_sub(1);
        }
    }

    /// Activate cooldown (called after consecutive loss threshold hit).
    pub fn activate_cooldown(&mut self) {
        let until = Utc::now()
            + chrono::Duration::seconds(self.cooldown_duration_secs as i64);
        self.cooldown_until = Some(until);
        warn!(
            duration_secs = self.cooldown_duration_secs,
            "Cooldown activated"
        );
    }

    /// Check and trigger daily halt if needed.
    pub fn check_daily_halt(&mut self, daily_pnl: f64) {
        if daily_pnl <= -self.max_daily_loss_usd && !self.daily_halt {
            self.daily_halt = true;
            info!(
                daily_pnl,
                limit = -self.max_daily_loss_usd,
                "DAILY LOSS LIMIT REACHED — halting trading"
            );
        }
    }

    /// Reset for a new trading day.
    pub fn reset_daily(&mut self) {
        self.daily_halt = false;
        self.cooldown_until = None;
        info!("Risk manager reset for new day");
    }

    pub fn is_halted(&self) -> bool {
        self.daily_halt
    }

    pub fn open_position_count(&self, asset: &str) -> usize {
        self.open_positions.get(asset).copied().unwrap_or(0)
    }

    pub fn open_positions(&self, asset: &str) -> usize {
        self.open_positions.get(asset).copied().unwrap_or(0)
    }

    pub fn cooldown_active(&self) -> bool {
        self.cooldown_until
            .map(|until| Utc::now() < until)
            .unwrap_or(false)
    }
}
