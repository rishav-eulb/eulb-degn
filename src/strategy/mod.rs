pub mod leg_manager;
pub mod params;
pub mod signal;

pub use leg_manager::{LegAction, LegManager, LegState};
pub use params::StrategyParams;
pub use signal::{Leg1Side, evaluate_entry};
