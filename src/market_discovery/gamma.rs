use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tracing::{debug, warn};

/// Represents a discovered Polymarket binary market with its token IDs.
#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub slug: String,
    pub condition_id: String,
    /// Token ID for the "Up" outcome (equivalent to YES).
    pub yes_token_id: String,
    /// Token ID for the "Down" outcome (equivalent to NO).
    pub no_token_id: String,
    pub question: String,
}

/// Raw Gamma API market response. Note: `clobTokenIds` and `outcomes` are
/// returned as JSON-encoded strings (e.g. `"[\"Up\",\"Down\"]"`), not arrays.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: Option<String>,
    clob_token_ids: Option<String>,
    outcomes: Option<String>,
    question: Option<String>,
}

/// Parse a JSON-encoded string array like `"[\"a\",\"b\"]"` into Vec<String>.
fn parse_json_string_array(raw: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).context("Parse JSON string array")
}

/// Fetches market info (condition_id, Up/Down token_ids) from the Gamma API for a slug.
/// The Gamma API returns `clobTokenIds` and `outcomes` as JSON-encoded strings.
pub async fn fetch_market_info(gamma_url: &str, slug: &str) -> Result<MarketInfo> {
    let url = format!("{}/markets?slug={}", gamma_url, slug);
    debug!(slug, "Fetching market info from Gamma API");

    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("Gamma API request failed for slug: {slug}"))?;

    if !resp.status().is_success() {
        bail!(
            "Gamma API returned {} for slug: {}",
            resp.status(),
            slug
        );
    }

    let markets: Vec<GammaMarket> = resp.json().await.with_context(|| {
        format!("Failed to parse Gamma API response for slug: {slug}")
    })?;

    let market = markets
        .into_iter()
        .next()
        .with_context(|| format!("No market found for slug: {slug}"))?;

    let condition_id = market
        .condition_id
        .with_context(|| format!("No conditionId for slug: {slug}"))?;

    let raw_token_ids = market
        .clob_token_ids
        .with_context(|| format!("No clobTokenIds for slug: {slug}"))?;
    let clob_token_ids = parse_json_string_array(&raw_token_ids)
        .with_context(|| format!("Failed to parse clobTokenIds for slug: {slug}"))?;

    let raw_outcomes = market
        .outcomes
        .with_context(|| format!("No outcomes for slug: {slug}"))?;
    let outcomes = parse_json_string_array(&raw_outcomes)
        .with_context(|| format!("Failed to parse outcomes for slug: {slug}"))?;

    if clob_token_ids.len() < 2 || outcomes.len() < 2 {
        bail!("Expected 2 outcomes and 2 token IDs for slug: {slug}");
    }

    // Match token IDs to outcomes. Outcomes are typically ["Up", "Down"].
    let mut yes_token_id = None;
    let mut no_token_id = None;

    for (i, outcome) in outcomes.iter().enumerate() {
        match outcome.as_str() {
            "Up" | "Yes" => yes_token_id = clob_token_ids.get(i).cloned(),
            "Down" | "No" => no_token_id = clob_token_ids.get(i).cloned(),
            other => {
                warn!(slug, outcome = other, "Unknown outcome value");
            }
        }
    }

    let yes_token_id =
        yes_token_id.with_context(|| format!("No Up/Yes token for slug: {slug}"))?;
    let no_token_id =
        no_token_id.with_context(|| format!("No Down/No token for slug: {slug}"))?;

    let question = market.question.unwrap_or_default();

    Ok(MarketInfo {
        slug: slug.to_string(),
        condition_id,
        yes_token_id,
        no_token_id,
        question,
    })
}

/// Pre-fetches the next market, retrying up to `max_retries` times with backoff.
/// Returns None if market is not yet available (normal before it's created).
pub async fn prefetch_market(
    gamma_url: &str,
    slug: &str,
    max_retries: u32,
) -> Option<MarketInfo> {
    for attempt in 0..max_retries {
        match fetch_market_info(gamma_url, slug).await {
            Ok(info) => return Some(info),
            Err(e) => {
                if attempt < max_retries - 1 {
                    let delay = std::time::Duration::from_secs(2u64.pow(attempt));
                    debug!(slug, attempt, ?delay, "Market not yet available, retrying");
                    tokio::time::sleep(delay).await;
                } else {
                    warn!(slug, error = %e, "Failed to prefetch market after all retries");
                }
            }
        }
    }
    None
}
