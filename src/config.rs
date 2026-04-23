//! Application-wide numeric constants.
//!
//! Centralising magic numbers here makes tuning straightforward and keeps the
//! constants discoverable in one place.

pub const DEFAULT_COLUMN_WIDTH: u16 = 15;
pub const MIN_COLUMN_WIDTH: u16 = 6;
pub const MAX_COLUMN_WIDTH: u16 = 40;
pub const PAGE_SCROLL_AMOUNT: u16 = 20;
pub const Y_AXIS_PADDING: f64 = 0.05;
pub const Y_AXIS_TICKS: usize = 5;
pub const CHART_BORDER_WIDTH: u16 = 1;
pub const MAX_UNIQUE: usize = 500;
