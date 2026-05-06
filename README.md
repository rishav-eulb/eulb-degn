# Polymarket Two-Leg Trading Bot

A low-latency Rust trading bot for Polymarket 5-minute BTC/ETH binary markets using the Two-Leg Momentum strategy.

## Strategy

1. **Detect directional move** — BTC or ETH moves >= 5 bps from market open (via Hyperliquid LOB)
2. **Buy confirmed-direction token** — Buy the UP/DOWN token trading between $0.55–$0.92 (Leg 1, taker FOK)
3. **Lock the opposite leg** — Scan for profitable lock where `leg1_cost + leg2_cost + fees < $1.00` (Leg 2, maker limit)
4. **Guaranteed payout** — Both tokens resolve to $1.00 combined at market expiry

Backtested result: **$357/day** combined BTC+ETH over 10 days with 100% positive days.

## Architecture

```
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Hyperliquid WS     │────▶│  Price Aggregator │────▶│  Signal Engine  │
│  (BTC/ETH LOB+Trades)│    │  + Momentum       │     │  (BPS + Imb)    │
└─────────────────────┘     │  + Book Imbalance │     └────────┬────────┘
                            └──────────────────┘              │
┌─────────────────────┐                                       ▼
│  Gamma API          │────▶  Market Discovery    ──▶  ┌──────────────┐
│  (slug → token_ids) │       (5m rotation)            │  Leg Manager │
└─────────────────────┘                                │  State Machine│
                                                       └──────┬───────┘
┌─────────────────────┐                                       │
│  Polymarket CLOB WS │◀──── Token orderbook ──────────────────┘
│  (book for active   │                                       │
│   market tokens)    │                                       ▼
└─────────────────────┘                              ┌─────────────────┐
                                                     │  Order Executor │
                                                     │  (rs-clob-v2)   │
                                                     └─────────────────┘
```

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your Polymarket private key

# 2. Build
cargo build --release

# 3. Run (dry-run mode by default)
cargo run --release
```

## Configuration

All config is via `.env` (see `.env.example`):

| Variable | Required | Description |
|----------|----------|-------------|
| `POLYMARKET_PRIVATE_KEY` | Yes | Your wallet private key for signing orders |
| `DRY_RUN` | — | `true` (default) = simulate, `false` = live trading |
| `TRADE_SIZE_USD` | — | Per-trade size (default $25) |
| `BTC_ENTRY_BPS` | — | Min BPS move for BTC entry (default 5) |
| `ETH_ENTRY_BPS` | — | Min BPS move for ETH entry (default 5) |
| `BOOK_IMB_THRESH` | — | Min book imbalance alignment (default 0.1) |
| `ASSETS` | — | Comma-separated assets to trade (default `BTC,ETH`) |

### Price Feed

By default, the bot uses **Hyperliquid perpetual mid-price** as the primary price source for directional signals. Chainlink Data Streams can be enabled optionally by setting `CHAINLINK_API_KEY` and `CHAINLINK_API_SECRET`.

### Risk Controls

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_CONCURRENT_POSITIONS` | 3 | Max open positions per asset |
| `MAX_DAILY_LOSS_USD` | 200 | Stop trading after this daily loss |
| `COOLDOWN_AFTER_LOSSES` | 3 | Consecutive losses before cooldown |
| `COOLDOWN_DURATION_SECS` | 300 | 5-minute cooldown period |

## Module Layout

```
src/
├── main.rs                 # Orchestrator (tokio tasks + channels)
├── config.rs               # .env loading
├── market_discovery/
│   ├── slug.rs             # 5-min slug generation + rotation
│   └── gamma.rs            # Gamma API: condition_id, token_ids
├── feeds/
│   ├── chainlink.rs        # Chainlink WS (optional)
│   ├── hyperliquid.rs      # Hyperliquid WS: LOB + trades
│   └── polymarket.rs       # Polymarket CLOB WS: token orderbook
├── indicators/
│   ├── price_aggregator.rs # BPS from open, source selection
│   ├── momentum.rs         # 5s/15s rolling momentum
│   └── book_imbalance.rs   # HL book imbalance tracker
├── strategy/
│   ├── signal.rs           # Entry signal evaluation
│   ├── leg_manager.rs      # Two-leg state machine
│   └── params.rs           # Strategy parameters
├── execution/
│   ├── order_builder.rs    # Order construction via polymarket SDK
│   ├── risk.rs             # Position limits, daily loss cap
│   └── tracker.rs          # Fill tracking, PnL
└── utils/
    └── ring_buffer.rs      # Fixed-size rolling buffer
```

## Two-Leg State Machine

```
Idle → Leg1Pending → Scanning → Leg2Pending → Locked ($1 payout)
                  ↘                         ↗
                   → ForceClose (if near expiry)
```

- **Leg 1**: FOK market order on confirmed directional token
- **Scanning**: Wait 3s min, then track best opposite-token ask for 15s
- **Leg 2**: Limit order (maker, 0% fee) at best observed price
- **Force Close**: Sell Leg 1 at market if < 10s to expiry

## Testing

```bash
# Run integration tests (hits live Polymarket APIs)
cargo test --test test_market_discovery -- --nocapture
```

## Dependencies

- `polymarket_client_sdk_v2` — Polymarket CLOB + Gamma + WebSocket
- `chainlink-data-streams-sdk` — Optional price feed
- `tokio` — Async runtime
- `tokio-tungstenite` — WebSocket (Hyperliquid)
- `rust_decimal` — Precise decimal math
- `tracing` — Structured JSON logging
