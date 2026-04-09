# Changelog

All notable changes to this project will be documented in this file.

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
