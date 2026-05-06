use crate::feeds::hyperliquid::HlBookSnapshot;

/// Tracks order book imbalance from Hyperliquid LOB snapshots.
/// Imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
/// Range: [-1.0, +1.0] where +1.0 = all bids, -1.0 = all asks.
#[derive(Debug)]
pub struct BookImbalanceTracker {
    /// Number of top levels to consider for imbalance calculation.
    depth_levels: usize,
    /// Latest computed imbalance per asset.
    latest_imbalance: std::collections::HashMap<String, ImbalanceState>,
}

/// Current book imbalance state for an asset.
#[derive(Debug, Clone, Copy)]
pub struct ImbalanceState {
    /// Overall imbalance [-1, +1].
    pub imbalance: f64,
    /// Total bid depth (in USD).
    pub bid_depth: f64,
    /// Total ask depth (in USD).
    pub ask_depth: f64,
    /// Top-of-book bid/ask ratio.
    pub tob_ratio: f64,
    /// Best bid price.
    pub best_bid: f64,
    /// Best ask price.
    pub best_ask: f64,
    /// Mid price.
    pub mid_price: f64,
    /// Spread in basis points.
    pub spread_bps: f64,
}

impl BookImbalanceTracker {
    pub fn new(depth_levels: usize) -> Self {
        Self {
            depth_levels,
            latest_imbalance: std::collections::HashMap::new(),
        }
    }

    /// Process a new LOB snapshot and update the imbalance state.
    pub fn update(&mut self, snapshot: &HlBookSnapshot) {
        let bid_depth: f64 = snapshot
            .bids
            .iter()
            .take(self.depth_levels)
            .map(|l| l.price * l.size)
            .sum();

        let ask_depth: f64 = snapshot
            .asks
            .iter()
            .take(self.depth_levels)
            .map(|l| l.price * l.size)
            .sum();

        let total = bid_depth + ask_depth;
        let imbalance = if total > 0.0 {
            (bid_depth - ask_depth) / total
        } else {
            0.0
        };

        let best_bid = snapshot
            .bids
            .first()
            .map(|l| l.price)
            .unwrap_or(0.0);
        let best_ask = snapshot
            .asks
            .first()
            .map(|l| l.price)
            .unwrap_or(0.0);

        let mid_price = if best_bid > 0.0 && best_ask > 0.0 {
            (best_bid + best_ask) / 2.0
        } else {
            0.0
        };

        let spread_bps = if mid_price > 0.0 {
            (best_ask - best_bid) / mid_price * 10_000.0
        } else {
            0.0
        };

        // Top-of-book ratio: bid_size / (bid_size + ask_size) at level 0
        let tob_bid = snapshot.bids.first().map(|l| l.size).unwrap_or(0.0);
        let tob_ask = snapshot.asks.first().map(|l| l.size).unwrap_or(0.0);
        let tob_total = tob_bid + tob_ask;
        let tob_ratio = if tob_total > 0.0 {
            tob_bid / tob_total
        } else {
            0.5
        };

        let state = ImbalanceState {
            imbalance,
            bid_depth,
            ask_depth,
            tob_ratio,
            best_bid,
            best_ask,
            mid_price,
            spread_bps,
        };

        self.latest_imbalance
            .insert(snapshot.asset.clone(), state);
    }

    /// Get the latest imbalance state for an asset.
    pub fn get(&self, asset: &str) -> Option<&ImbalanceState> {
        self.latest_imbalance.get(asset)
    }

    /// Returns true if the imbalance is directionally aligned with the given sign.
    /// sign > 0 means bullish (price going up), sign < 0 means bearish.
    pub fn is_aligned(&self, asset: &str, bps_direction: f64, threshold: f64) -> bool {
        if let Some(state) = self.get(asset) {
            if bps_direction > 0.0 {
                state.imbalance > threshold
            } else if bps_direction < 0.0 {
                state.imbalance < -threshold
            } else {
                false
            }
        } else {
            false
        }
    }
}
