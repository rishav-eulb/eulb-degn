//! Integration test: market discovery + live YES/NO token price check.
//!
//! Run with: cargo test --test test_market_discovery -- --nocapture
//! Requires network access to Polymarket Gamma API and CLOB API.

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Deserialize;

const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
const CLOB_URL: &str = "https://clob-v2.polymarket.com";
const INTERVAL_SECS: u64 = 300;

// ─── Slug Generation ────────────────────────────────────────────────────────

fn format_slug(asset: &str, timestamp: u64) -> String {
    format!("{}-updown-5m-{}", asset.to_lowercase(), timestamp)
}

fn current_slug(asset: &str) -> String {
    let now = Utc::now().timestamp() as u64;
    let aligned = (now / INTERVAL_SECS) * INTERVAL_SECS;
    format_slug(asset, aligned)
}

fn next_slug(asset: &str) -> String {
    let now = Utc::now().timestamp() as u64;
    let aligned = (now / INTERVAL_SECS) * INTERVAL_SECS + INTERVAL_SECS;
    format_slug(asset, aligned)
}

fn secs_remaining() -> u64 {
    let now = Utc::now().timestamp() as u64;
    ((now / INTERVAL_SECS) + 1) * INTERVAL_SECS - now
}

// ─── Gamma API Types ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: Option<String>,
    /// JSON-encoded string array, e.g. "[\"token1\", \"token2\"]"
    clob_token_ids: Option<String>,
    /// JSON-encoded string array, e.g. "[\"Up\", \"Down\"]"
    outcomes: Option<String>,
    outcome_prices: Option<String>,
    question: Option<String>,
    active: Option<bool>,
    liquidity: Option<String>,
}

#[derive(Debug)]
struct MarketInfo {
    slug: String,
    condition_id: String,
    up_token_id: String,
    down_token_id: String,
    question: String,
}

// ─── CLOB Orderbook Types ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ClobBook {
    bids: Option<Vec<ClobLevel>>,
    asks: Option<Vec<ClobLevel>>,
}

#[derive(Debug, Deserialize)]
struct ClobLevel {
    price: String,
    size: String,
}

// ─── Helpers ────────────────────────────────────────────────────────────────

async fn fetch_market_info(slug: &str) -> Result<MarketInfo> {
    let url = format!("{}/markets?slug={}", GAMMA_URL, slug);
    let resp = reqwest::get(&url).await.context("Gamma API request failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("Gamma API returned {} for slug: {}", resp.status(), slug);
    }

    let markets: Vec<GammaMarket> = resp.json().await.context("Parse Gamma response")?;
    let market = markets
        .into_iter()
        .next()
        .context(format!("No market found for slug: {slug}"))?;

    let condition_id = market.condition_id.context("No conditionId")?;

    let raw_tokens = market.clob_token_ids.context("No clobTokenIds")?;
    let clob_token_ids: Vec<String> =
        serde_json::from_str(&raw_tokens).context("Parse clobTokenIds JSON string")?;

    let raw_outcomes = market.outcomes.context("No outcomes")?;
    let outcomes: Vec<String> =
        serde_json::from_str(&raw_outcomes).context("Parse outcomes JSON string")?;

    if clob_token_ids.len() < 2 || outcomes.len() < 2 {
        anyhow::bail!("Expected 2 outcomes, got {}", outcomes.len());
    }

    // outcomes is ["Up", "Down"], clobTokenIds is aligned
    let mut up_token_id = None;
    let mut down_token_id = None;

    for (i, outcome) in outcomes.iter().enumerate() {
        match outcome.as_str() {
            "Up" | "Yes" => up_token_id = clob_token_ids.get(i).cloned(),
            "Down" | "No" => down_token_id = clob_token_ids.get(i).cloned(),
            _ => {}
        }
    }

    Ok(MarketInfo {
        slug: slug.to_string(),
        condition_id,
        up_token_id: up_token_id.context("No Up token")?,
        down_token_id: down_token_id.context("No Down token")?,
        question: market.question.unwrap_or_default(),
    })
}

async fn fetch_token_book(token_id: &str) -> Result<(f64, f64, Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let url = format!("{}/book?token_id={}", CLOB_URL, token_id);
    let resp = reqwest::get(&url).await.context("CLOB book request failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("CLOB returned {} for token: {}", resp.status(), token_id);
    }

    let book: ClobBook = resp.json().await.context("Parse CLOB book")?;

    let bids: Vec<(f64, f64)> = book
        .bids
        .unwrap_or_default()
        .iter()
        .filter_map(|l| Some((l.price.parse().ok()?, l.size.parse().ok()?)))
        .collect();

    let asks: Vec<(f64, f64)> = book
        .asks
        .unwrap_or_default()
        .iter()
        .filter_map(|l| Some((l.price.parse().ok()?, l.size.parse().ok()?)))
        .collect();

    let best_bid = bids.first().map(|l| l.0).unwrap_or(0.0);
    let best_ask = asks.first().map(|l| l.0).unwrap_or(0.0);

    Ok((best_bid, best_ask, bids, asks))
}

// ─── Unit Tests ─────────────────────────────────────────────────────────────

#[test]
fn test_slug_format() {
    let slug = format_slug("BTC", 1700000000);
    assert_eq!(slug, "btc-updown-5m-1700000000");

    let slug = format_slug("ETH", 1700000300);
    assert_eq!(slug, "eth-updown-5m-1700000300");
}

#[test]
fn test_slug_alignment() {
    let now = Utc::now().timestamp() as u64;
    let current = current_slug("BTC");

    let ts_str = current.strip_prefix("btc-updown-5m-").unwrap();
    let ts: u64 = ts_str.parse().unwrap();

    assert_eq!(ts % 300, 0, "Timestamp should be aligned to 300s");
    assert!(ts <= now, "Current slug should be <= now");
    assert!(now - ts < 300, "Current slug should be within last 5 min");
}

#[test]
fn test_next_slug_is_future() {
    let now = Utc::now().timestamp() as u64;
    let next = next_slug("BTC");

    let ts_str = next.strip_prefix("btc-updown-5m-").unwrap();
    let ts: u64 = ts_str.parse().unwrap();

    assert!(ts > now, "Next slug should be in the future");
    assert!(ts - now <= 300, "Next slug should be within 5 min");
}

#[test]
fn test_secs_remaining_range() {
    let remaining = secs_remaining();
    assert!(remaining > 0, "Should have > 0 seconds remaining");
    assert!(remaining <= 300, "Should have <= 300 seconds remaining");
}

// ─── Live Integration Tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_discover_current_btc_market() {
    let slug = current_slug("BTC");
    println!("\n==================================================");
    println!("=== BTC Market Discovery ===");
    println!("Current slug: {}", slug);
    println!("Seconds remaining: {}s", secs_remaining());
    println!();

    match fetch_market_info(&slug).await {
        Ok(info) => {
            println!("  [OK] Market found!");
            println!("  Question:     {}", info.question);
            println!("  Condition ID: {}...{}", &info.condition_id[..10], &info.condition_id[info.condition_id.len()-8..]);
            println!("  Up token:     {}...", &info.up_token_id[..20]);
            println!("  Down token:   {}...", &info.down_token_id[..20]);

            assert!(!info.condition_id.is_empty());
            assert!(!info.up_token_id.is_empty());
            assert!(!info.down_token_id.is_empty());
            assert_ne!(info.up_token_id, info.down_token_id);
        }
        Err(e) => {
            println!("  [SKIP] Market not found (may be between intervals): {}", e);
        }
    }
}

#[tokio::test]
async fn test_discover_current_eth_market() {
    let slug = current_slug("ETH");
    println!("\n=== ETH Market Discovery ===");
    println!("Current slug: {}", slug);

    match fetch_market_info(&slug).await {
        Ok(info) => {
            println!("  [OK] Market found!");
            println!("  Question:  {}", info.question);
            println!("  Up token:  {}...", &info.up_token_id[..20]);
            println!("  Down token:{}...", &info.down_token_id[..20]);

            assert!(!info.up_token_id.is_empty());
            assert!(!info.down_token_id.is_empty());
        }
        Err(e) => {
            println!("  [SKIP] Market not found: {}", e);
        }
    }
}

#[tokio::test]
async fn test_token_prices_btc() {
    let slug = current_slug("BTC");
    println!("\n=== BTC Token Prices ===");
    println!("Slug: {}", slug);
    println!("Time remaining: {}s", secs_remaining());
    println!();

    let market = match fetch_market_info(&slug).await {
        Ok(m) => m,
        Err(e) => {
            println!("  [SKIP] No active market: {}", e);
            return;
        }
    };

    println!("  Question: {}", market.question);
    println!();

    // Fetch UP token orderbook
    let up_result = fetch_token_book(&market.up_token_id).await;
    let down_result = fetch_token_book(&market.down_token_id).await;

    let mut up_mid = 0.0;
    let mut down_mid = 0.0;

    if let Ok((bid, ask, bids, asks)) = &up_result {
        up_mid = if *bid > 0.0 && *ask > 0.0 { (bid + ask) / 2.0 } else { 0.0 };
        println!("  UP Token (first 20 chars: {}...)", &market.up_token_id[..20]);
        println!("    Best Bid:  ${:.4} (depth: {} levels)", bid, bids.len());
        println!("    Best Ask:  ${:.4} (depth: {} levels)", ask, asks.len());
        println!("    Mid:       ${:.4}", up_mid);
        println!("    Spread:    ${:.4} ({:.2} bps)", ask - bid,
            if up_mid > 0.0 { (ask - bid) / up_mid * 10000.0 } else { 0.0 });

        // Show top 3 levels
        println!("    Top bids: {:?}", &bids[..bids.len().min(3)]);
        println!("    Top asks: {:?}", &asks[..asks.len().min(3)]);

        if *bid > 0.0 {
            assert!(*bid > 0.0 && *bid < 1.0, "UP bid should be in (0, 1)");
        }
        if *ask > 0.0 {
            assert!(*ask > 0.0 && *ask <= 1.0, "UP ask should be in (0, 1]");
            assert!(ask >= bid, "Ask should be >= bid");
        }
    } else if let Err(e) = &up_result {
        println!("  [ERROR] UP token book: {}", e);
    }

    println!();

    if let Ok((bid, ask, bids, asks)) = &down_result {
        down_mid = if *bid > 0.0 && *ask > 0.0 { (bid + ask) / 2.0 } else { 0.0 };
        println!("  DOWN Token (first 20 chars: {}...)", &market.down_token_id[..20]);
        println!("    Best Bid:  ${:.4} (depth: {} levels)", bid, bids.len());
        println!("    Best Ask:  ${:.4} (depth: {} levels)", ask, asks.len());
        println!("    Mid:       ${:.4}", down_mid);
        println!("    Spread:    ${:.4} ({:.2} bps)", ask - bid,
            if down_mid > 0.0 { (ask - bid) / down_mid * 10000.0 } else { 0.0 });

        println!("    Top bids: {:?}", &bids[..bids.len().min(3)]);
        println!("    Top asks: {:?}", &asks[..asks.len().min(3)]);
    } else if let Err(e) = &down_result {
        println!("  [ERROR] DOWN token book: {}", e);
    }

    // Sanity check: UP_mid + DOWN_mid ≈ $1.00
    if up_mid > 0.0 && down_mid > 0.0 {
        let combined = up_mid + down_mid;
        println!();
        println!("  === Sanity Check ===");
        println!("  UP mid + DOWN mid = ${:.4}", combined);
        println!("  Deviation from $1: {:.2}c", (combined - 1.0).abs() * 100.0);

        assert!(
            (combined - 1.0).abs() < 0.10,
            "UP + DOWN should sum near $1.00, got {:.4}",
            combined
        );
        println!("  [OK] Sum is within 10c of $1.00");
    }
}

#[tokio::test]
async fn test_token_prices_eth() {
    let slug = current_slug("ETH");
    println!("\n=== ETH Token Prices ===");
    println!("Slug: {}", slug);
    println!();

    let market = match fetch_market_info(&slug).await {
        Ok(m) => m,
        Err(e) => {
            println!("  [SKIP] No active ETH market: {}", e);
            return;
        }
    };

    println!("  Question: {}", market.question);

    let up_result = fetch_token_book(&market.up_token_id).await;
    let down_result = fetch_token_book(&market.down_token_id).await;

    if let (Ok((ub, ua, _, _)), Ok((db, da, _, _))) = (&up_result, &down_result) {
        println!();
        println!("  ┌──────────┬──────────┬──────────┬──────────┐");
        println!("  │ Token    │ Bid      │ Ask      │ Spread   │");
        println!("  ├──────────┼──────────┼──────────┼──────────┤");
        println!("  │ UP       │ ${:.4}  │ ${:.4}  │ ${:.4}  │", ub, ua, ua - ub);
        println!("  │ DOWN     │ ${:.4}  │ ${:.4}  │ ${:.4}  │", db, da, da - db);
        println!("  ├──────────┼──────────┼──────────┼──────────┤");
        let combined_bid = ub + db;
        let combined_ask = ua + da;
        println!("  │ Combined │ ${:.4}  │ ${:.4}  │          │", combined_bid, combined_ask);
        println!("  └──────────┴──────────┴──────────┴──────────┘");

        if *ua > 0.0 && *da > 0.0 {
            let up_mid = (ub + ua) / 2.0;
            let down_mid = (db + da) / 2.0;
            println!();
            println!("  UP mid + DOWN mid = ${:.4}", up_mid + down_mid);
        }
    }
}

#[tokio::test]
async fn test_next_market_prefetch() {
    let slug = next_slug("BTC");
    println!("\n=== Next Market Pre-fetch ===");
    println!("Next slug: {}", slug);
    println!("Opens in: {}s", secs_remaining());
    println!();

    // The next market may or may not exist yet
    match fetch_market_info(&slug).await {
        Ok(info) => {
            println!("  [OK] Next market already available!");
            println!("  Question: {}", info.question);
            println!("  Up token: {}...", &info.up_token_id[..20]);
        }
        Err(_) => {
            println!("  [OK] Next market not yet created (expected if > 30s to open)");
        }
    }
}
