use crate::indicators::book_imbalance::ImbalanceState;
use crate::indicators::price_aggregator::PriceState;
use crate::strategy::params::StrategyParams;

/// Represents the direction of a trade signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// An entry signal generated when conditions are met.
#[derive(Debug, Clone, PartialEq)]
pub struct EntrySignal {
    pub asset: String,
    pub direction: Direction,
    pub bps_move: f64,
    pub book_imbalance: f64,
    /// Which token to buy for Leg1 (YES if Up, NO if Down).
    pub leg1_side: Leg1Side,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg1Side {
    Yes,
    No,
}

/// Evaluates whether entry conditions are met for a given asset.
/// Returns Some(EntrySignal) if all conditions pass, None otherwise.
pub fn evaluate_entry(
    price_state: &PriceState,
    imbalance: &ImbalanceState,
    token_yes_price: f64,
    token_no_price: f64,
    secs_since_open: u64,
    params: &StrategyParams,
) -> Option<EntrySignal> {
    // Time window check
    if secs_since_open < params.min_entry_time_secs
        || secs_since_open > params.max_entry_time_secs
    {
        return None;
    }

    let bps_move = price_state.bps_from_open;
    let abs_bps = bps_move.abs();

    // BPS threshold check
    if abs_bps < params.entry_bps_threshold {
        return None;
    }

    // Determine direction
    let direction = if bps_move > 0.0 {
        Direction::Up
    } else {
        Direction::Down
    };

    // Book imbalance alignment check
    let imb_aligned = match direction {
        Direction::Up => imbalance.imbalance > params.book_imb_threshold,
        Direction::Down => imbalance.imbalance < -params.book_imb_threshold,
    };
    if !imb_aligned {
        return None;
    }

    // Determine which token to buy and check price range
    let (leg1_side, entry_price) = match direction {
        Direction::Up => (Leg1Side::Yes, token_yes_price),
        Direction::Down => (Leg1Side::No, token_no_price),
    };

    // Token price range check
    if entry_price < params.min_entry_price || entry_price > params.max_entry_price {
        return None;
    }

    Some(EntrySignal {
        asset: price_state.asset.clone(),
        direction,
        bps_move,
        book_imbalance: imbalance.imbalance,
        leg1_side,
    })
}
