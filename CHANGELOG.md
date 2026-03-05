# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-03-05

### Added
- **Column Inspector** (`i`) — full-screen table showing every column's type, count, null count, unique count, min, max, mean, and median
- **Histogram plot** — third plot type cycled with `t` (line → bar → histogram); bins computed via Sturges' rule

### Fixed
- Categorical X-axis labels in plot mode no longer consume more than 1/3 of the screen for long string values

### Performance
- Table rendering is now O(viewport height) instead of O(total rows) — large files no longer freeze the UI

## [0.1.0] - 2026-03-02

### Added
- Initial release
- Vim-style navigation (`hjkl`, `g`/`G`, `PageUp`/`PageDown`)
- Search within a column (`/`, `n`/`N`)
- Multi-column filtering (`f`, `F`)
- Sort by any column (`s`)
- Group-by with per-column aggregations (`b`, `a`, `B`)
- Column plot — line or bar chart with rotated labels for categorical X axes (`p`, `t`)
- Column stats popup (`S`)
- In-app help popup (`?`)
- Autofit column width (`_`, `=`)
- CSV and Parquet file support
- Catppuccin Mocha theme
