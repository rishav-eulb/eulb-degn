use crate::strategy::params::StrategyParams;
use crate::strategy::signal::{EntrySignal, Leg1Side};
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

/// States of the two-leg state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum LegState {
    /// No active position, waiting for a signal.
    Idle,
    /// Leg1 order has been placed, awaiting fill confirmation.
    Leg1Pending {
        signal: EntrySignal,
        order_time: DateTime<Utc>,
    },
    /// Leg1 is filled, now scanning for a profitable Leg2 lock.
    Scanning {
        signal: EntrySignal,
        leg1_fill_price: f64,
        leg1_shares: f64,
        fill_time: DateTime<Utc>,
        first_profitable_time: Option<DateTime<Utc>>,
        best_leg2_price: Option<f64>,
    },
    /// Leg2 order has been placed.
    Leg2Pending {
        signal: EntrySignal,
        leg1_fill_price: f64,
        leg1_shares: f64,
        leg2_target_price: f64,
        order_time: DateTime<Utc>,
    },
    /// Both legs filled — position is locked for guaranteed payout.
    Locked {
        signal: EntrySignal,
        leg1_fill_price: f64,
        leg2_fill_price: f64,
        shares: f64,
        expected_pnl: f64,
    },
    /// Force-closing Leg1 (market about to expire, couldn't lock).
    ForceClose {
        signal: EntrySignal,
        leg1_fill_price: f64,
        shares: f64,
    },
}

/// Actions the leg manager requests the execution layer to perform.
#[derive(Debug, Clone)]
pub enum LegAction {
    /// Place a Leg1 market order (FOK).
    PlaceLeg1 {
        token_id: String,
        side: Leg1Side,
        size_usd: f64,
    },
    /// Place a Leg2 limit order (maker).
    PlaceLeg2 {
        token_id: String,
        price: f64,
        shares: f64,
    },
    /// Force-sell Leg1 at market (approaching expiry).
    ForceCloseLeg1 {
        token_id: String,
        shares: f64,
    },
    /// No action needed this tick.
    None,
}

/// Manages the two-leg state machine for a single market position.
#[derive(Debug)]
pub struct LegManager {
    pub state: LegState,
    params: StrategyParams,
    /// YES token ID for the active market.
    yes_token_id: String,
    /// NO token ID for the active market.
    no_token_id: String,
    /// Market open timestamp.
    market_open_ts: DateTime<Utc>,
    /// Market interval in seconds.
    market_interval_secs: u64,
}

impl LegManager {
    pub fn new(
        params: StrategyParams,
        yes_token_id: String,
        no_token_id: String,
        market_open_ts: DateTime<Utc>,
        market_interval_secs: u64,
    ) -> Self {
        Self {
            state: LegState::Idle,
            params,
            yes_token_id,
            no_token_id,
            market_open_ts,
            market_interval_secs,
        }
    }

    /// Called when an entry signal fires.
    pub fn on_signal(&mut self, signal: EntrySignal) -> LegAction {
        if self.state != LegState::Idle {
            return LegAction::None;
        }

        let token_id = match signal.leg1_side {
            Leg1Side::Yes => self.yes_token_id.clone(),
            Leg1Side::No => self.no_token_id.clone(),
        };

        info!(
            asset = %signal.asset,
            direction = ?signal.direction,
            bps = signal.bps_move,
            imbalance = signal.book_imbalance,
            "Entry signal fired — placing Leg1"
        );

        self.state = LegState::Leg1Pending {
            signal: signal.clone(),
            order_time: Utc::now(),
        };

        LegAction::PlaceLeg1 {
            token_id,
            side: signal.leg1_side,
            size_usd: self.params.trade_size_usd,
        }
    }

    /// Called when Leg1 order is confirmed filled.
    /// Immediately computes the Leg2 limit price and emits PlaceLeg2.
    pub fn on_leg1_fill(&mut self, fill_price: f64, shares: f64) -> LegAction {
        if let LegState::Leg1Pending { signal, .. } = &self.state {
            // Compute max price for opposite token that still yields profit.
            // As a maker (0% fee), profit = 1.0 - leg1_price - leg2_price
            // We want: profit >= min_profit_per_share
            // So: leg2_price <= 1.0 - leg1_price - min_profit_per_share
            let max_leg2_price = 1.0 - fill_price - self.params.min_profit_per_share;

            let opposite_token_id = match signal.leg1_side {
                Leg1Side::Yes => self.no_token_id.clone(),
                Leg1Side::No => self.yes_token_id.clone(),
            };

            info!(
                fill_price,
                shares,
                max_leg2_price,
                profit_target = self.params.min_profit_per_share,
                "Leg1 filled — placing Leg2 limit at discount"
            );

            if max_leg2_price < 0.01 {
                // Leg1 price too high — no valid Leg2 price exists, go to scanning for force-close
                warn!(fill_price, "Leg1 fill too expensive for profitable Leg2 — scanning for exit");
                self.state = LegState::Scanning {
                    signal: signal.clone(),
                    leg1_fill_price: fill_price,
                    leg1_shares: shares,
                    fill_time: Utc::now(),
                    first_profitable_time: None,
                    best_leg2_price: None,
                };
                return LegAction::None;
            }

            // Round down to tick (0.01 increments)
            let target_price = (max_leg2_price * 100.0).floor() / 100.0;

            self.state = LegState::Leg2Pending {
                signal: signal.clone(),
                leg1_fill_price: fill_price,
                leg1_shares: shares,
                leg2_target_price: target_price,
                order_time: Utc::now(),
            };

            return LegAction::PlaceLeg2 {
                token_id: opposite_token_id,
                price: target_price,
                shares,
            };
        }
        LegAction::None
    }

    /// Called when Leg1 order is rejected or times out.
    pub fn on_leg1_rejected(&mut self) {
        warn!("Leg1 order rejected/timed out — returning to Idle");
        self.state = LegState::Idle;
    }

    /// Called every tick with the current opposite token ask price.
    /// Handles force-close near expiry for both Scanning and Leg2Pending states.
    /// Scanning state also allows late Leg2 placement if price improves enough.
    pub fn on_tick(&mut self, opposite_ask: f64, now: DateTime<Utc>) -> LegAction {
        let secs_to_expiry = self.secs_to_expiry(now);

        match &self.state {
            LegState::Scanning {
                signal,
                leg1_fill_price,
                leg1_shares,
                ..
            } => {
                // Force close if approaching expiry
                if secs_to_expiry <= self.params.force_close_secs {
                    let token_id = match signal.leg1_side {
                        Leg1Side::Yes => self.yes_token_id.clone(),
                        Leg1Side::No => self.no_token_id.clone(),
                    };
                    let shares = *leg1_shares;
                    self.state = LegState::ForceClose {
                        signal: signal.clone(),
                        leg1_fill_price: *leg1_fill_price,
                        shares,
                    };
                    warn!(secs_to_expiry, "Force-closing Leg1 — market expiring");
                    return LegAction::ForceCloseLeg1 { token_id, shares };
                }

                // Scanning is a fallback state (when on_leg1_fill couldn't place Leg2).
                // Try to place Leg2 if a profitable price becomes available.
                let max_leg2_price = 1.0 - *leg1_fill_price - self.params.min_profit_per_share;
                if opposite_ask <= max_leg2_price && max_leg2_price >= 0.01 {
                    let target_price = (max_leg2_price * 100.0).floor() / 100.0;
                    let opposite_token_id = match signal.leg1_side {
                        Leg1Side::Yes => self.no_token_id.clone(),
                        Leg1Side::No => self.yes_token_id.clone(),
                    };
                    let shares = *leg1_shares;

                    info!(
                        target_price,
                        opposite_ask,
                        "Placing Leg2 limit from Scanning (price now viable)"
                    );

                    self.state = LegState::Leg2Pending {
                        signal: signal.clone(),
                        leg1_fill_price: *leg1_fill_price,
                        leg1_shares: shares,
                        leg2_target_price: target_price,
                        order_time: now,
                    };

                    return LegAction::PlaceLeg2 {
                        token_id: opposite_token_id,
                        price: target_price,
                        shares,
                    };
                }

                LegAction::None
            }

            LegState::Leg2Pending { .. } => {
                // Force close if approaching expiry and Leg2 hasn't filled
                if secs_to_expiry <= self.params.force_close_secs {
                    if let LegState::Leg2Pending {
                        signal,
                        leg1_fill_price,
                        leg1_shares,
                        ..
                    } = &self.state
                    {
                        let token_id = match signal.leg1_side {
                            Leg1Side::Yes => self.yes_token_id.clone(),
                            Leg1Side::No => self.no_token_id.clone(),
                        };
                        let shares = *leg1_shares;
                        self.state = LegState::ForceClose {
                            signal: signal.clone(),
                            leg1_fill_price: *leg1_fill_price,
                            shares,
                        };
                        warn!("Leg2 unfilled near expiry — force-closing");
                        return LegAction::ForceCloseLeg1 { token_id, shares };
                    }
                }
                LegAction::None
            }

            _ => LegAction::None,
        }
    }

    /// Called when Leg2 order is filled.
    pub fn on_leg2_fill(&mut self, fill_price: f64) {
        if let LegState::Leg2Pending {
            signal,
            leg1_fill_price,
            leg1_shares,
            ..
        } = &self.state
        {
            let leg1_cost = *leg1_fill_price + self.params.slippage_cents;
            let leg2_cost = fill_price;
            let fee = leg1_cost * self.params.taker_fee_rate
                + leg2_cost * self.params.maker_fee_rate;
            let total_cost = leg1_cost + leg2_cost + fee;
            let pnl_per_share = 1.0 - total_cost;
            let expected_pnl = pnl_per_share * *leg1_shares;

            info!(
                leg1_price = leg1_fill_price,
                leg2_price = fill_price,
                expected_pnl,
                "LOCKED — both legs filled"
            );

            self.state = LegState::Locked {
                signal: signal.clone(),
                leg1_fill_price: *leg1_fill_price,
                leg2_fill_price: fill_price,
                shares: *leg1_shares,
                expected_pnl,
            };
        }
    }

    /// Called when Leg2 order is not filled (retry logic).
    pub fn on_leg2_rejected(&mut self) {
        if let LegState::Leg2Pending {
            signal,
            leg1_fill_price,
            leg1_shares,
            ..
        } = &self.state
        {
            debug!("Leg2 not filled — returning to Scanning");
            self.state = LegState::Scanning {
                signal: signal.clone(),
                leg1_fill_price: *leg1_fill_price,
                leg1_shares: *leg1_shares,
                fill_time: Utc::now(),
                first_profitable_time: None,
                best_leg2_price: None,
            };
        }
    }

    /// Reset to idle (for new market cycle).
    pub fn reset(
        &mut self,
        yes_token_id: String,
        no_token_id: String,
        market_open_ts: DateTime<Utc>,
    ) {
        self.state = LegState::Idle;
        self.yes_token_id = yes_token_id;
        self.no_token_id = no_token_id;
        self.market_open_ts = market_open_ts;
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.state, LegState::Idle)
    }

    pub fn is_locked(&self) -> bool {
        matches!(self.state, LegState::Locked { .. })
    }

    pub fn current_direction(&self) -> Option<&crate::strategy::signal::Direction> {
        match &self.state {
            LegState::Leg1Pending { signal, .. }
            | LegState::Scanning { signal, .. }
            | LegState::Leg2Pending { signal, .. }
            | LegState::Locked { signal, .. }
            | LegState::ForceClose { signal, .. } => Some(&signal.direction),
            LegState::Idle => None,
        }
    }

    pub fn current_leg1_side(&self) -> Option<&Leg1Side> {
        match &self.state {
            LegState::Leg1Pending { signal, .. }
            | LegState::Scanning { signal, .. }
            | LegState::Leg2Pending { signal, .. }
            | LegState::Locked { signal, .. }
            | LegState::ForceClose { signal, .. } => Some(&signal.leg1_side),
            LegState::Idle => None,
        }
    }

    pub fn leg1_fill_price(&self) -> Option<f64> {
        match &self.state {
            LegState::Scanning { leg1_fill_price, .. }
            | LegState::Leg2Pending { leg1_fill_price, .. }
            | LegState::Locked { leg1_fill_price, .. }
            | LegState::ForceClose { leg1_fill_price, .. } => Some(*leg1_fill_price),
            _ => None,
        }
    }

    pub fn leg1_fill_shares(&self) -> Option<f64> {
        match &self.state {
            LegState::Scanning { leg1_shares, .. }
            | LegState::Leg2Pending { leg1_shares, .. } => Some(*leg1_shares),
            LegState::Locked { shares, .. }
            | LegState::ForceClose { shares, .. } => Some(*shares),
            _ => None,
        }
    }

    pub fn leg2_status(&self) -> String {
        match &self.state {
            LegState::Idle => "N/A".to_string(),
            LegState::Leg1Pending { .. } => "Leg1 pending".to_string(),
            LegState::Scanning { .. } => "Scanning for Leg2".to_string(),
            LegState::Leg2Pending { leg2_target_price, .. } => {
                format!("Leg2 pending @ {:.4}", leg2_target_price)
            }
            LegState::Locked { leg2_fill_price, .. } => {
                format!("LOCKED @ {:.4}", leg2_fill_price)
            }
            LegState::ForceClose { .. } => "Force-closing".to_string(),
        }
    }

    pub fn leg2_fill_price(&self) -> Option<f64> {
        match &self.state {
            LegState::Locked { leg2_fill_price, .. } => Some(*leg2_fill_price),
            _ => None,
        }
    }

    /// Estimate unrealised PnL based on current token prices.
    pub fn unrealised_pnl(&self, yes_price: f64, no_price: f64) -> f64 {
        match &self.state {
            LegState::Scanning { signal, leg1_fill_price, leg1_shares, .. }
            | LegState::Leg2Pending { signal, leg1_fill_price, leg1_shares: leg1_shares, .. } => {
                let current_value = match signal.leg1_side {
                    Leg1Side::Yes => yes_price,
                    Leg1Side::No => no_price,
                };
                (current_value - leg1_fill_price) * leg1_shares
            }
            LegState::Locked { expected_pnl, .. } => *expected_pnl,
            _ => 0.0,
        }
    }

    fn secs_to_expiry(&self, now: DateTime<Utc>) -> u64 {
        let expiry = self.market_open_ts
            + chrono::Duration::seconds(self.market_interval_secs as i64);
        let remaining = (expiry - now).num_seconds();
        remaining.max(0) as u64
    }
}
