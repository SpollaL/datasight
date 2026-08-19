//! Clipboard copy for the selected cell or row.
//!
//! Text extraction is kept separate from the transport: [`cell_text`] and [`row_text`]
//! are pure functions over a [`DataFrame`], and [`copy`] is the only part that touches
//! the terminal.
//!
//! The transport is OSC 52, so the *terminal* performs the copy. That works over SSH and
//! in a headless WSL install, where a native clipboard library cannot — but it also means
//! a terminal that does not implement OSC 52 ignores the sequence silently, and the app
//! has no way to detect it.

use crossterm::clipboard::CopyToClipboard;
use crossterm::execute;
use polars::prelude::*;

/// Text of a single cell exactly as [`crate::ui`] renders it. Nulls become the empty
/// string rather than the `∅` display glyph, which would be wrong once pasted.
///
/// Returns `None` if `row` or `col` is out of range.
pub fn cell_text(df: &DataFrame, row: usize, col: usize) -> Option<String> {
    // as_materialized_series, not as_series: the latter returns None for scalar-backed
    // columns, which any 1-row frame uses.
    let s = df.get_columns().get(col)?.as_materialized_series();
    let str_s = s.cast(&DataType::String).ok()?;
    let ca = str_s.str().ok()?;
    (row < ca.len()).then(|| ca.get(row).unwrap_or_default().to_string())
}

/// Every column of `row`, tab-separated so the result pastes straight into a spreadsheet.
///
/// Returns `None` if `row` is out of range.
pub fn row_text(df: &DataFrame, row: usize) -> Option<String> {
    if row >= df.height() {
        return None;
    }
    let cells: Vec<String> = (0..df.width())
        .map(|col| cell_text(df, row, col).unwrap_or_default())
        .collect();
    Some(cells.join("\t"))
}

/// Hand `text` to the terminal's clipboard via OSC 52.
///
/// Writes straight to stdout rather than through ratatui's backend: OSC 52 is
/// out-of-band, so it neither moves the cursor nor disturbs the rendered frame.
pub fn copy(text: &str) -> std::io::Result<()> {
    execute!(std::io::stdout(), CopyToClipboard::to_clipboard_from(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame() -> DataFrame {
        df![
            "name" => ["alice", "bob", "carol"],
            "qty"  => [1i64, 2, 3],
            "rate" => [1.5f64, 2.25, 3.0],
        ]
        .unwrap()
    }

    #[test]
    fn test_cell_text_reads_each_dtype() {
        let df = frame();
        assert_eq!(cell_text(&df, 0, 0).as_deref(), Some("alice"));
        assert_eq!(cell_text(&df, 1, 1).as_deref(), Some("2"));
        assert_eq!(cell_text(&df, 2, 2).as_deref(), Some("3.0"));
    }

    #[test]
    fn test_cell_text_null_is_empty_not_glyph() {
        let df = df![
            "name" => [Some("alice"), None],
            "qty"  => [Some(1i64), None],
        ]
        .unwrap();
        assert_eq!(cell_text(&df, 1, 0).as_deref(), Some(""));
        assert_eq!(cell_text(&df, 1, 1).as_deref(), Some(""));
    }

    #[test]
    fn test_cell_text_on_single_row_frame() {
        // polars backs length-1 columns with a Scalar; as_series() would return None
        // here and yield an empty copy for every 1-row file.
        let df = df!["name" => ["solitary"], "qty" => [7i64]].unwrap();
        assert_eq!(cell_text(&df, 0, 0).as_deref(), Some("solitary"));
        assert_eq!(cell_text(&df, 0, 1).as_deref(), Some("7"));
    }

    #[test]
    fn test_cell_text_formats_temporal_as_displayed() {
        let df = df!["ts" => ["2025-05-23 10:52:37", "2025-05-23 11:46:14"]]
            .unwrap()
            .lazy()
            .select([col("ts").str().to_datetime(
                None,
                None,
                StrptimeOptions {
                    format: Some("%Y-%m-%d %H:%M:%S".into()),
                    ..Default::default()
                },
                lit("raise"),
            )])
            .collect()
            .unwrap();
        assert_eq!(
            cell_text(&df, 0, 0).as_deref(),
            Some("2025-05-23 10:52:37.000000")
        );
    }

    #[test]
    fn test_cell_text_out_of_range() {
        let df = frame();
        assert_eq!(cell_text(&df, 3, 0), None);
        assert_eq!(cell_text(&df, 0, 3), None);
    }

    #[test]
    fn test_row_text_is_tab_separated_across_all_columns() {
        assert_eq!(row_text(&frame(), 1).as_deref(), Some("bob\t2\t2.25"));
    }

    #[test]
    fn test_row_text_keeps_position_of_null() {
        let df = df![
            "a" => [Some("x")],
            "b" => [None::<i64>],
            "c" => [Some("z")],
        ]
        .unwrap();
        assert_eq!(row_text(&df, 0).as_deref(), Some("x\t\tz"));
    }

    #[test]
    fn test_row_text_out_of_range() {
        assert_eq!(row_text(&frame(), 3), None);
    }
}
