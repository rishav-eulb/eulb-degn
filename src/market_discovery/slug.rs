use chrono::Utc;

/// Generates a Polymarket 5-minute market slug for a given asset and timestamp.
/// Format: `{asset}-updown-5m-{aligned_timestamp}`
/// The timestamp is aligned to the nearest 5-minute boundary (floor).
pub fn current_slug(asset: &str, interval_secs: u64) -> String {
    let now = Utc::now().timestamp() as u64;
    let aligned = (now / interval_secs) * interval_secs;
    format_slug(asset, aligned)
}

/// Generates the NEXT market slug (the one that hasn't opened yet).
/// Used for pre-fetching token_ids before market opens.
pub fn next_slug(asset: &str, interval_secs: u64) -> String {
    let now = Utc::now().timestamp() as u64;
    let aligned = (now / interval_secs) * interval_secs + interval_secs;
    format_slug(asset, aligned)
}

/// Generates a slug from asset and Unix timestamp.
pub fn format_slug(asset: &str, timestamp: u64) -> String {
    format!("{}-updown-5m-{}", asset.to_lowercase(), timestamp)
}

/// Returns seconds remaining in the current market interval.
pub fn secs_remaining(interval_secs: u64) -> u64 {
    let now = Utc::now().timestamp() as u64;
    let next_boundary = ((now / interval_secs) + 1) * interval_secs;
    next_boundary - now
}

/// Returns the Unix timestamp when the current market opened.
pub fn current_market_open_ts(interval_secs: u64) -> u64 {
    let now = Utc::now().timestamp() as u64;
    (now / interval_secs) * interval_secs
}

/// Returns the Unix timestamp when the next market opens.
pub fn next_market_open_ts(interval_secs: u64) -> u64 {
    current_market_open_ts(interval_secs) + interval_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_slug() {
        assert_eq!(format_slug("BTC", 1700000000), "btc-updown-5m-1700000000");
        assert_eq!(format_slug("ETH", 1700000300), "eth-updown-5m-1700000300");
    }

    #[test]
    fn test_secs_remaining() {
        let remaining = secs_remaining(300);
        assert!(remaining > 0 && remaining <= 300);
    }
}
