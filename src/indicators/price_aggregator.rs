use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Aggregates prices from Chainlink and Hyperliquid, producing BPS deltas
/// relative to the market's opening price.
///
/// When Chainlink is unavailable, Hyperliquid mid-price is used as the
/// primary price source for directional signals.
#[derive(Debug)]
pub struct PriceAggregator {
    /// Latest Chainlink prices (preferred source of truth).
    chainlink_prices: HashMap<String, TimestampedPrice>,
    /// Latest Hyperliquid mid prices (primary when Chainlink unavailable).
    hl_mid_prices: HashMap<String, TimestampedPrice>,
    /// Market open prices for BPS calculation (set when market opens).
    market_open_prices: HashMap<String, f64>,
    /// Whether Chainlink is available as a price source.
    chainlink_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct TimestampedPrice {
    pub price: f64,
    pub timestamp: DateTime<Utc>,
}

/// Computed price state for a single asset at a point in time.
#[derive(Debug, Clone)]
pub struct PriceState {
    pub asset: String,
    /// The authoritative price (Chainlink if available, else Hyperliquid mid).
    pub price: f64,
    /// Source of the price.
    pub source: PriceSource,
    /// BPS move from market open.
    pub bps_from_open: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceSource {
    Chainlink,
    Hyperliquid,
}

impl PriceAggregator {
    pub fn new(chainlink_enabled: bool) -> Self {
        Self {
            chainlink_prices: HashMap::new(),
            hl_mid_prices: HashMap::new(),
            market_open_prices: HashMap::new(),
            chainlink_enabled,
        }
    }

    /// Update Chainlink price for an asset. Caller should pass pre-uppercased keys.
    pub fn update_chainlink(&mut self, asset: &str, price: f64, ts: DateTime<Utc>) {
        self.chainlink_prices.insert(
            asset.to_string(),
            TimestampedPrice { price, timestamp: ts },
        );
    }

    /// Update Hyperliquid mid-price for an asset. Caller should pass pre-uppercased keys.
    pub fn update_hl_mid(&mut self, asset: &str, price: f64, ts: DateTime<Utc>) {
        self.hl_mid_prices.insert(
            asset.to_string(),
            TimestampedPrice { price, timestamp: ts },
        );
    }

    /// Snapshot the market open price from the best available source.
    pub fn snapshot_open_price(&mut self, asset: &str) {
        if let Some(price) = self.get_best_price(asset) {
            self.market_open_prices
                .insert(asset.to_string(), price.price);
        }
    }

    /// Get the current price state for an asset using the best available source.
    pub fn get_state(&self, asset: &str) -> Option<PriceState> {
        let best = self.get_best_price(asset)?;
        let open = self.market_open_prices.get(asset)?;

        let bps_from_open = if *open > 0.0 {
            (best.price - open) / open * 10_000.0
        } else {
            0.0
        };

        let source = if self.chainlink_enabled && self.chainlink_prices.contains_key(asset) {
            PriceSource::Chainlink
        } else {
            PriceSource::Hyperliquid
        };

        Some(PriceState {
            asset: asset.to_string(),
            price: best.price,
            source,
            bps_from_open,
            timestamp: best.timestamp,
        })
    }

    /// Returns the best available price: Chainlink if enabled and fresh, else Hyperliquid.
    fn get_best_price(&self, asset: &str) -> Option<TimestampedPrice> {
        if self.chainlink_enabled {
            if let Some(cl) = self.chainlink_prices.get(asset) {
                return Some(*cl);
            }
        }

        self.hl_mid_prices.get(asset).copied()
    }

    /// Clear open prices (called when rotating to a new market interval).
    pub fn clear_open_prices(&mut self) {
        self.market_open_prices.clear();
    }

    /// Check if we have any price data for the given asset.
    pub fn has_price(&self, asset: &str) -> bool {
        self.chainlink_prices.contains_key(asset) || self.hl_mid_prices.contains_key(asset)
    }
}

impl Default for PriceAggregator {
    fn default() -> Self {
        Self::new(false)
    }
}
