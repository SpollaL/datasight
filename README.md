# iron-sight

A terminal CSV/Parquet viewer with vim-style navigation, built with Rust and ratatui. Themed with [Catppuccin Mocha](https://github.com/catppuccin/catppuccin).

[![CI](https://github.com/SpollaL/iron-sight/actions/workflows/ci.yml/badge.svg)](https://github.com/SpollaL/iron-sight/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

![demo](.github/assets/demo.gif)

## Features

- Vim-style navigation (`hjkl`, `g`/`G`, `PageUp`/`PageDown`)
- Search within a column (`/`, `n`/`N`)
- Multi-column filtering (`f`, `F`)
- Sort by any column (`s`)
- Group-by with per-column aggregations (`b`, `a`, `B`)
- Column plot — line, bar, or histogram chart (`p`, `t`)
- Column Inspector — schema and stats for every column at a glance (`i`)
- Column stats popup (`S`)
- In-app help popup (`?`)
- Catppuccin Mocha color theme with zebra-striped rows and mode-aware status bar
- Supports CSV and Parquet files
- Viewport-windowed rendering — stays fast on large files

## Install

### Pre-built binaries (recommended)

Download the latest binary for your platform from the [GitHub Releases](https://github.com/SpollaL/iron-sight/releases) page.

### Build from source

Requires Rust 1.75 or higher.

```
cargo install --git https://github.com/SpollaL/iron-sight
```

Or clone and run locally:

```
cargo run -- <path-to-file.csv>
cargo run -- <path-to-file.parquet>
```

## Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `j` / `Down` | Move down |
| `k` / `Up` | Move up |
| `h` / `Left` | Move left |
| `l` / `Right` | Move right |
| `g` / `Home` | Jump to first row |
| `G` / `End` | Jump to last row |
| `PageDown` | Scroll down 20 rows |
| `PageUp` | Scroll up 20 rows |

### Search

| Key | Action |
|-----|--------|
| `/` | Enter search mode (searches in current column) |
| `Enter` | Confirm search and jump to first match |
| `n` | Next match |
| `N` | Previous match |
| `Esc` | Exit search and clear results |

### Filter

| Key | Action |
|-----|--------|
| `f` | Enter filter mode (filters rows by current column) |
| `Enter` | Confirm filter and return to normal mode |
| `F` | Clear all filters |
| `Esc` | Discard input |

### Sort

| Key | Action |
|-----|--------|
| `s` | Sort by current column (toggles asc/desc) |

### Group By

| Key | Action |
|-----|--------|
| `b` | Toggle group-by key for current column |
| `a` | Cycle aggregation for current column (Σ μ # ↓ ↑) |
| `B` | Execute group-by / clear and return to full view |

### Plot

| Key | Context | Action |
|-----|---------|--------|
| `p` | Normal | Mark current column as Y and enter pick-X mode |
| `h` / `←` / `l` / `→` | Pick-X | Navigate to the X column |
| `Enter` | Pick-X | Confirm X column and show chart |
| `Esc` | Pick-X | Cancel and return to normal mode |
| `t` | Plot | Cycle chart type (line → bar → histogram) |
| `Esc` / `p` | Plot | Close chart and return to normal mode |
| `q` | Plot | Quit |

Numeric X columns are plotted directly. String or date X columns use row indices as data points and render the actual values as rotated (vertical) labels below the chart — all labels are shown when they fit, otherwise they are sampled evenly.

For histogram, the Y column is binned automatically — no X column selection needed.

### Column Inspector

| Key | Action |
|-----|--------|
| `i` | Open Column Inspector (type, count, nulls, unique, min, max, mean, median) |
| `j` / `k` | Navigate rows |
| `Enter` | Jump to the selected column and return to data view |
| `Esc` / `i` | Close and return to data view |

### Column Stats

| Key | Action |
|-----|--------|
| `S` | Toggle stats popup for current column (count, min, max, mean, median) |

### Other

| Key | Action |
|-----|--------|
| `i` | Open Column Inspector |
| `_` | Autofit current column width |
| `=` | Autofit all columns |
| `S` | Toggle column stats popup |
| `?` | Toggle help popup |
| `q` | Quit |
