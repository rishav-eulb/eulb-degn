use crate::utils::ring_buffer::RingBuffer;
use chrono::{DateTime, Utc};

/// Tracks rolling momentum over configurable windows (e.g. 5s, 15s).
/// Momentum = (latest_price - price_N_seconds_ago) / price_N_seconds_ago * 10000 bps
#[derive(Debug)]
pub struct MomentumTracker {
    /// Ring buffer of (timestamp, price) tuples.
    samples: RingBuffer<(DateTime<Utc>, f64)>,
    /// Short window in seconds (e.g. 5s).
    short_window_secs: i64,
    /// Long window in seconds (e.g. 15s).
    long_window_secs: i64,
}

/// Momentum readings at both windows.
#[derive(Debug, Clone, Copy)]
pub struct MomentumState {
    /// BPS change over the short window.
    pub short_bps: f64,
    /// BPS change over the long window.
    pub long_bps: f64,
    /// Acceleration: short_bps - long_bps (positive = accelerating).
    pub acceleration: f64,
    pub latest_price: f64,
}

impl MomentumTracker {
    pub fn new(short_window_secs: i64, long_window_secs: i64) -> Self {
        // Store enough samples to cover the long window at 1-sample-per-second
        let capacity = (long_window_secs as usize + 5).max(32);
        Self {
            samples: RingBuffer::new(capacity),
            short_window_secs,
            long_window_secs,
        }
    }

    /// Push a new price sample.
    pub fn push(&mut self, price: f64, ts: DateTime<Utc>) {
        self.samples.push((ts, price));
    }

    /// Compute current momentum state.
    pub fn compute(&self) -> Option<MomentumState> {
        let latest = self.samples.latest()?;
        let now = latest.0;
        let current_price = latest.1;

        let short_price = self.price_at_offset(now, self.short_window_secs);
        let long_price = self.price_at_offset(now, self.long_window_secs);

        let short_bps = short_price
            .map(|p| (current_price - p) / p * 10_000.0)
            .unwrap_or(0.0);

        let long_bps = long_price
            .map(|p| (current_price - p) / p * 10_000.0)
            .unwrap_or(0.0);

        Some(MomentumState {
            short_bps,
            long_bps,
            acceleration: short_bps - long_bps,
            latest_price: current_price,
        })
    }

    /// Find the price closest to `now - offset_secs` in our buffer.
    fn price_at_offset(&self, now: DateTime<Utc>, offset_secs: i64) -> Option<f64> {
        let target = now - chrono::Duration::seconds(offset_secs);
        let mut best: Option<(i64, f64)> = None;

        for (ts, price) in self.samples.iter() {
            let diff = (*ts - target).num_milliseconds().abs();
            match best {
                Some((best_diff, _)) if diff < best_diff => {
                    best = Some((diff, *price));
                }
                None => {
                    best = Some((diff, *price));
                }
                _ => {}
            }
        }

        // Only accept if within 2 seconds of target
        best.and_then(|(diff, price)| {
            if diff <= 2000 {
                Some(price)
            } else {
                None
            }
        })
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }
}
