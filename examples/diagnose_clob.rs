use std::env;
use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk_v2::clob::types::{Amount, OrderType as SdkOrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Address, U256};
use polymarket_client_sdk_v2::POLYGON;
use rust_decimal_macros::dec;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let private_key = env::var("POLYMARKET_PRIVATE_KEY").expect("Need POLYMARKET_PRIVATE_KEY");
    let funder_env = env::var("POLYMARKET_FUNDER_ADDRESS").ok();
    let clob_url = env::var("POLYMARKET_CLOB_URL").unwrap_or_else(|_| "https://clob.polymarket.com".into());

    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let eoa = signer.address();

    println!("\n======= CLOB Diagnostic =======\n");
    println!("  EOA:          {eoa}");
    println!("  CLOB URL:     {clob_url}");

    if let Some(ref f) = funder_env {
        println!("  ENV Funder:   {f}");
    }

    // --- Test 1: Authenticate with explicit funder ---
    println!("\n━━━ Test 1: Auth with explicit funder ━━━\n");
    if let Some(ref f) = funder_env {
        let funder_addr: Address = f.parse()?;
        let config = ClobConfig::builder().use_server_time(true).build();
        match ClobClient::new(&clob_url, config)?
            .authentication_builder(&signer)
            .funder(funder_addr)
            .signature_type(SignatureType::GnosisSafe)
            .authenticate()
            .await
        {
            Ok(client) => {
                println!("  ✓ Auth succeeded (explicit funder + server time)");
                run_diagnostics(&client, &signer).await;
            }
            Err(e) => println!("  ✗ Auth failed: {e:#}"),
        }
    }

    // --- Test 2: Authenticate with auto-derived funder ---
    println!("\n━━━ Test 2: Auth with auto-derived funder ━━━\n");
    {
        let config = ClobConfig::builder().use_server_time(true).build();
        match ClobClient::new(&clob_url, config)?
            .authentication_builder(&signer)
            .signature_type(SignatureType::GnosisSafe)
            .authenticate()
            .await
        {
            Ok(client) => {
                println!("  ✓ Auth succeeded (auto-derived funder + server time)");
                run_diagnostics(&client, &signer).await;
            }
            Err(e) => println!("  ✗ Auth failed: {e:#}"),
        }
    }

    // --- Test 3: Auth with local time (what the bot currently uses) ---
    println!("\n━━━ Test 3: Auth with local time (bot's current config) ━━━\n");
    if let Some(ref f) = funder_env {
        let funder_addr: Address = f.parse()?;
        let config = ClobConfig::builder().use_server_time(false).build();
        match ClobClient::new(&clob_url, config)?
            .authentication_builder(&signer)
            .funder(funder_addr)
            .signature_type(SignatureType::GnosisSafe)
            .authenticate()
            .await
        {
            Ok(client) => {
                println!("  ✓ Auth succeeded (explicit funder + local time)");
                run_diagnostics(&client, &signer).await;
            }
            Err(e) => println!("  ✗ Auth failed: {e:#}"),
        }
    }

    println!("\n━━━ Done ━━━\n");
    Ok(())
}

type AuthClient = ClobClient<polymarket_client_sdk_v2::auth::state::Authenticated<polymarket_client_sdk_v2::auth::Normal>>;

async fn run_diagnostics(client: &AuthClient, signer: &LocalSigner<k256::ecdsa::SigningKey>) {
    // Check API keys
    match client.api_keys().await {
        Ok(keys) => println!("  API keys: {keys:?}"),
        Err(e) => println!("  ⚠ api_keys() failed: {e}"),
    }

    // Check balance/allowance
    println!("\n  Checking balance/allowance...");
    match client.balance_allowance(Default::default()).await {
        Ok(ba) => println!("  Balance/Allowance: {ba:?}"),
        Err(e) => println!("  ⚠ balance_allowance() failed: {e}"),
    }

    println!("\n  Refreshing CLOB allowance cache...");
    match client.update_balance_allowance(Default::default()).await {
        Ok(r) => println!("  ✓ update_balance_allowance: {r:?}"),
        Err(e) => println!("  ⚠ update_balance_allowance failed: {e}"),
    }

    // Try a small test order on a known liquid BTC updown market
    println!("\n  Attempting a test market order ($1 FOK BUY)...");

    // Use a BTC updown token — fetch tick size + neg_risk via the SDK
    let test_token_str = "59363949496345081575141410189982188543750471847508297920374995282671535375280";
    let test_token = match U256::from_str(test_token_str) {
        Ok(t) => t,
        Err(e) => {
            println!("  ✗ Bad token_id: {e}");
            return;
        }
    };

    println!("  Token: {test_token_str}");

    match client.tick_size(test_token).await {
        Ok(ts) => println!("  Tick size: {ts:?}"),
        Err(e) => println!("  ⚠ tick_size() failed: {e}"),
    }
    match client.neg_risk(test_token).await {
        Ok(nr) => println!("  Neg risk: {nr:?}"),
        Err(e) => println!("  ⚠ neg_risk() failed: {e}"),
    }

    let order_result = client
        .market_order()
        .token_id(test_token)
        .amount(Amount::usdc(dec!(1)).unwrap())
        .side(Side::Buy)
        .order_type(SdkOrderType::FOK)
        .price(dec!(0.55))
        .build()
        .await;

    match order_result {
        Ok(order) => {
            println!("  ✓ Order built successfully");
            match client.sign(signer, order).await {
                Ok(signed) => {
                    println!("  ✓ Order signed successfully");
                    match client.post_order(signed).await {
                        Ok(resp) => {
                            println!("  ✓ post_order response:");
                            println!("    success: {}", resp.success);
                            println!("    status: {:?}", resp.status);
                            println!("    order_id: {}", resp.order_id);
                            println!("    error_msg: {:?}", resp.error_msg);
                            println!("    making: {}", resp.making_amount);
                            println!("    taking: {}", resp.taking_amount);
                        }
                        Err(e) => {
                            println!("  ✗ post_order FAILED:");
                            println!("    Error: {e}");
                            println!("    Debug: {e:?}");
                            println!("    Chain: {e:#}");
                        }
                    }
                }
                Err(e) => println!("  ✗ sign() failed: {e:#}"),
            }
        }
        Err(e) => println!("  ✗ Order build failed: {e:#}"),
    }
}
