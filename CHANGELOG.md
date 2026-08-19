# Changelog

All notable changes to this project will be documented in this file.

## [0.9.0] - 2026-08-19

### Added
- `y` copies the selected cell to the clipboard; `Y` copies the current row, tab-separated so it pastes straight into a spreadsheet (#24). Copying uses the OSC 52 escape sequence, so it also works over SSH — see the README for terminal support.

## [0.8.0] - 2026-08-19

### Added
- `-` / `+` narrow and widen the selected column by 4 cells.

### Changed
- `_` now toggles: autofit the selected column, press again to restore the default width.

### Fixed
- Autofit is bounded by the table pane instead of a fixed 40-character cap, so long values fit fully when there is room for them (#23).
- Columns backed by a single scalar value rendered every cell as `∅`. Any 1-row file was affected, including straight from the CSV reader.
- Key release events are now ignored, so Windows terminals no longer handle every keystroke twice.
- Enabled the `dtype-duration` and `dtype-time` polars features, so files with duration or time columns load instead of erroring.

## [0.7.0] - 2026-05-18

### Added
- `datasight browse` now lists every entry. Plain text and non-array JSON open in a new text viewer with scroll, search (`/`, `n`/`N`), line numbers, word wrap (`w`), and pretty-printed JSON. Binary files render dimmed and surface a status message on Enter.
- `T → Theme` hint added to the Normal and Filter mode shortcut bars so the theme picker is discoverable in the standard viewer.

### Changed
- Expanded crates.io package description to list all supported formats, features, and cloud backends. Keywords swapped to `csv` / `parquet` / `tui` / `dataframe` / `cli`; added `development-tools` category.

## [0.6.1] - 2026-05-08

### Fixed
- Sync `Cargo.lock` to package version so `cargo install --git` from the v0.6.0 tag no longer fails on the lockfile mismatch.

## [0.6.0] - 2026-05-06

### Added
- `datasight browse` subcommand — file browser TUI with split-pane navigation and live preview. Supports local filesystem, Azure Blob Storage (behind `--features azure`), and S3 (behind `--features aws`).
- Base16 theme system — 9 built-in color schemes. Press `t` in normal mode to cycle themes, use `~/.config/datasight/state.toml` for persistence.

### Fixed
- CSV schema inference now correctly promoted to Decimal dtype for columns with decimal values.
- Full CSV schema inference fixed to avoid treating free-text columns as dates.

## [0.5.0] - 2026-04-23

### Added
- Numeric Y-axis scale on line/bar/histogram plots with K/M suffix formatting.
- Plot a column against its row index — press `i` in pick-Y mode.
- Searchable column inspector: press `/` in unique values mode to filter the list by substring; press `/` in column inspector (`i`) to search columns by name.

## [0.4.1] - 2026-04-22

### Fixed
- Group-by view no longer collapses when cycling the sort direction or clearing sorts; filters are now keyed by column name so they survive the pre-/post-aggregation schema switch.
- `update_filter` rebuilds the full pipeline (raw filter → group-by → aggregate filter → sort) so filters can target either raw or aggregated columns without conflict.

### Added
- Automatic date detection for non-ISO formats (`MM/DD/YYYY`, `MM-DD-YYYY`, `DD-Mon-YYYY`, `DD Mon YYYY`). String columns that parse cleanly are promoted to `Date` so chronological sort works.
- Ambiguity guard: slash-date columns in which every row has day ≤ 12 are kept as strings rather than silently coerced to the wrong calendar convention.

### Changed
- Disabled Polars' built-in CSV date auto-detect in favour of the post-load helper so the ambiguity guard applies consistently, with byte-level pre-filter and 32-row sampling so free-text columns skip the date check in microseconds.

## [0.4.0] - 2026-04-20

### Added
- Multi-column Y-axis plot: press `p`, toggle any number of Y columns with `Space` in the new pick-Y mode, then pick an X column to render a Line or Bar chart for side-by-side series comparison.
- `qa.sh` — automated TUI smoke-test suite covering every mode, keybinding, and file format (required before tagging a release).

### Changed
- Histogram plot type is disabled when multiple Y columns are selected.

## [0.3.0] - 2026-04-13

### Added
- Hierarchical multi-column sort — `s` on a column cycles Ascending → Descending → off; pressing `s` on additional columns appends them as secondary priorities.
- Header glyphs (`①▲` / `②▼`) show sort priority and a sapphire `Sort: name▲ → age▼` summary appears in the status bar when sorts are active.
- `S` clears every active sort at once (mirrors `F` for filters).

### Changed
- Column stats popup moved from `S` to `e` to free `S` for clear-sorts.

## [0.2.0] - 2026-04-09

### Added
- TSV support and custom delimiter flag (`-d`)
- Stdin/pipe support with automatic format detection (CSV, JSON, NDJSON)
- JSON (`[{...}]`) and NDJSON/JSON Lines (`.ndjson`, `.jsonl`) file formats
- Context-aware Zellij-style shortcut bar

### Fixed
- Surface silent errors and prevent duplicate filter stacking
- Treat unknown extensions as CSV when `-d` delimiter is provided

### Changed
- CI matrix expanded to macOS and Windows
- Expanded test coverage from 29 to 66 tests

## [0.1.0] - 2026-03-23

### Added
- Vim-style navigation (`hjkl`, `g`/`G`, `PageUp`/`PageDown`)
- Search within a column (`/`, `n`/`N`)
- Multi-column filtering with comparison operators — `> 30`, `= Engineering`, `!= 0` (`f`, `F`)
- Unique values popup — searchable overlay showing distinct values sorted by frequency; press `Enter` to apply as a filter (`u`)
- Sort by any column (`s`)
- Group-by with per-column aggregations (`b`, `a`, `B`)
- Column plot — line, bar, or histogram chart (`p`, `t`)
- Column Inspector — schema and stats for every column (`i`)
- Column stats popup (`S`)
- In-app help popup with scrolling (`?`)
- Autofit column width (`_`, `=`)
- CSV and Parquet file support via Polars
- Catppuccin Mocha theme with zebra-striped rows
- Viewport-windowed rendering for large files
