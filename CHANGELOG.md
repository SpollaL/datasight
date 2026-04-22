# Changelog

All notable changes to this project will be documented in this file.

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
