# Changelog

All notable changes to this project will be documented in this file.

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
