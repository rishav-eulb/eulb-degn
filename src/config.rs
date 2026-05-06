use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    // Polymarket
    pub polymarket_private_key: String,
    pub polymarket_funder_address: Option<String>,
    pub polymarket_clob_url: String,
    pub polymarket_gamma_url: String,
    pub polymarket_ws_url: String,
    pub polymarket_rtds_url: String,

    // Chainlink
    pub chainlink_ws_url: String,
    pub chainlink_api_key: String,
    pub chainlink_api_secret: String,
    pub chainlink_btc_feed_id: String,
    pub chainlink_eth_feed_id: String,

    // Hyperliquid
    pub hyperliquid_ws_url: String,
    pub hyperliquid_assets: Vec<String>,

    // Strategy
    pub trade_size_usd: f64,
    pub btc_entry_bps: f64,
    pub eth_entry_bps: f64,
    pub book_imb_thresh: f64,
    pub min_entry_price: f64,
    pub max_entry_price: f64,
    pub leg2_wait_secs: u64,
    pub leg2_min_wait_secs: u64,
    pub force_close_secs: u64,

    // Risk
    pub max_concurrent_positions: usize,
    pub max_daily_loss_usd: f64,
    pub cooldown_after_losses: u32,
    pub cooldown_duration_secs: u64,

    // General
    pub log_level: String,
    pub dry_run: bool,
    pub assets: Vec<String>,
    pub market_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Self {
            // Polymarket
            polymarket_private_key: required("POLYMARKET_PRIVATE_KEY")?,
            polymarket_funder_address: env::var("POLYMARKET_FUNDER_ADDRESS").ok().filter(|s| !s.is_empty()),
            polymarket_clob_url: optional(
                "POLYMARKET_CLOB_URL",
                "https://clob.polymarket.com",
            ),
            polymarket_gamma_url: optional(
                "POLYMARKET_GAMMA_URL",
                "https://gamma-api.polymarket.com",
            ),
            polymarket_ws_url: optional(
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws",
            ),
            polymarket_rtds_url: optional(
                "POLYMARKET_RTDS_URL",
                "wss://ws-live-data.polymarket.com",
            ),

            // Chainlink (optional — falls back to Hyperliquid mid price)
            chainlink_ws_url: optional(
                "CHAINLINK_WS_URL",
                "wss://ws.testnet-dataengine.chain.link",
            ),
            chainlink_api_key: env::var("CHAINLINK_API_KEY").unwrap_or_default(),
            chainlink_api_secret: env::var("CHAINLINK_API_SECRET").unwrap_or_default(),
            chainlink_btc_feed_id: env::var("CHAINLINK_BTC_FEED_ID").unwrap_or_default(),
            chainlink_eth_feed_id: env::var("CHAINLINK_ETH_FEED_ID").unwrap_or_default(),

            // Hyperliquid
            hyperliquid_ws_url: optional(
                "HYPERLIQUID_WS_URL",
                "wss://api.hyperliquid.xyz/ws",
            ),
            hyperliquid_assets: parse_csv("HYPERLIQUID_ASSETS", "BTC,ETH"),

            // Strategy
            trade_size_usd: parse_f64("TRADE_SIZE_USD", 25.0)?,
            btc_entry_bps: parse_f64("BTC_ENTRY_BPS", 5.0)?,
            eth_entry_bps: parse_f64("ETH_ENTRY_BPS", 5.0)?,
            book_imb_thresh: parse_f64("BOOK_IMB_THRESH", 0.1)?,
            min_entry_price: parse_f64("MIN_ENTRY_PRICE", 0.55)?,
            max_entry_price: parse_f64("MAX_ENTRY_PRICE", 0.92)?,
            leg2_wait_secs: parse_u64("LEG2_WAIT_SECS", 15)?,
            leg2_min_wait_secs: parse_u64("LEG2_MIN_WAIT_SECS", 3)?,
            force_close_secs: parse_u64("FORCE_CLOSE_SECS", 10)?,

            // Risk
            max_concurrent_positions: parse_u64("MAX_CONCURRENT_POSITIONS", 3)? as usize,
            max_daily_loss_usd: parse_f64("MAX_DAILY_LOSS_USD", 200.0)?,
            cooldown_after_losses: parse_u64("COOLDOWN_AFTER_LOSSES", 3)? as u32,
            cooldown_duration_secs: parse_u64("COOLDOWN_DURATION_SECS", 300)?,

            // General
            log_level: optional("LOG_LEVEL", "info"),
            dry_run: env::var("DRY_RUN")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(true),
            assets: parse_csv("ASSETS", "BTC,ETH"),
            market_interval_secs: parse_u64("MARKET_INTERVAL_SECS", 300)?,
        })
    }

    pub fn entry_bps_for_asset(&self, asset: &str) -> f64 {
        match asset.to_uppercase().as_str() {
            "ETH" => self.eth_entry_bps,
            _ => self.btc_entry_bps,
        }
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("Missing required env var: {key}"))
}

fn optional(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_f64(key: &str, default: f64) -> Result<f64> {
    match env::var(key) {
        Ok(val) => val
            .parse::<f64>()
            .with_context(|| format!("Invalid f64 for {key}: {val}")),
        Err(_) => Ok(default),
    }
}

fn parse_u64(key: &str, default: u64) -> Result<u64> {
    match env::var(key) {
        Ok(val) => val
            .parse::<u64>()
            .with_context(|| format!("Invalid u64 for {key}: {val}")),
        Err(_) => Ok(default),
    }
}

fn parse_csv(key: &str, default: &str) -> Vec<String> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}
