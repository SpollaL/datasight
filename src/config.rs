//! Application-wide numeric constants.
//!
//! Centralising magic numbers here makes tuning straightforward and keeps the
//! constants discoverable in one place.

pub const DEFAULT_COLUMN_WIDTH: u16 = 15;
pub const MIN_COLUMN_WIDTH: u16 = 6;
/// How much `-` / `+` change the selected column's width per press.
pub const COLUMN_WIDTH_STEP: u16 = 4;
pub const PAGE_SCROLL_AMOUNT: u16 = 20;
pub const Y_AXIS_PADDING: f64 = 0.05;
pub const Y_AXIS_TICKS: usize = 5;
pub const CHART_BORDER_WIDTH: u16 = 1;
pub const MAX_UNIQUE: usize = 500;

/// Cap on the byte slice the text viewer will load from a single file.
/// Files larger than this are read up to this point and shown with a
/// "truncated, X MB total" banner — nothing beyond the cap is read.
pub const MAX_TEXT_BYTES: usize = 10 * 1024 * 1024;
