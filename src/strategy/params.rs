/// Strategy parameters extracted from config for easy passing to strategy logic.
#[derive(Debug, Clone)]
pub struct StrategyParams {
    /// Minimum BTC/ETH BPS move from market open to trigger entry.
    pub entry_bps_threshold: f64,
    /// Minimum book imbalance (absolute) aligned with direction.
    pub book_imb_threshold: f64,
    /// Minimum acceptable entry token price (e.g. 0.55).
    pub min_entry_price: f64,
    /// Maximum acceptable entry token price (e.g. 0.92).
    pub max_entry_price: f64,
    /// Minimum seconds to wait after Leg1 fill before placing Leg2 limit.
    pub leg2_min_wait_secs: u64,
    /// How long to keep Leg2 limit order alive before repricing (seconds).
    pub leg2_wait_secs: u64,
    /// Seconds before market expiry to force-close unlocked positions.
    pub force_close_secs: u64,
    /// Earliest time (seconds after market open) to consider entry.
    pub min_entry_time_secs: u64,
    /// Latest time (seconds after market open) to consider entry.
    pub max_entry_time_secs: u64,
    /// Trade size in USD.
    pub trade_size_usd: f64,
    /// Estimated slippage per leg (in cents).
    pub slippage_cents: f64,
    /// Fee rate on gross winnings (2% for taker).
    pub taker_fee_rate: f64,
    /// Fee rate for maker orders (0%).
    pub maker_fee_rate: f64,
    /// Minimum profit target per share for Leg2 (e.g. 0.01 = 1 cent/share).
    pub min_profit_per_share: f64,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            entry_bps_threshold: 5.0,
            book_imb_threshold: 0.1,
            min_entry_price: 0.55,
            max_entry_price: 0.92,
            leg2_min_wait_secs: 3,
            leg2_wait_secs: 15,
            force_close_secs: 10,
            min_entry_time_secs: 60,
            max_entry_time_secs: 240,
            trade_size_usd: 25.0,
            slippage_cents: 0.01,
            taker_fee_rate: 0.02,
            maker_fee_rate: 0.0,
            min_profit_per_share: 0.01,
        }
    }
}

impl StrategyParams {
    pub fn from_config(cfg: &crate::config::Config, asset: &str) -> Self {
        Self {
            entry_bps_threshold: cfg.entry_bps_for_asset(asset),
            book_imb_threshold: cfg.book_imb_thresh,
            min_entry_price: cfg.min_entry_price,
            max_entry_price: cfg.max_entry_price,
            leg2_min_wait_secs: cfg.leg2_min_wait_secs,
            leg2_wait_secs: cfg.leg2_wait_secs,
            force_close_secs: cfg.force_close_secs,
            min_entry_time_secs: 60,
            max_entry_time_secs: 240,
            trade_size_usd: cfg.trade_size_usd,
            slippage_cents: 0.01,
            taker_fee_rate: 0.02,
            maker_fee_rate: 0.0,
            min_profit_per_share: 0.05,
        }
    }
}
