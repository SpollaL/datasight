# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

After cloning, activate the pre-commit hooks:

```bash
git config core.hooksPath .githooks
```

## Commands

```bash
cargo build                          # debug build
cargo run -- <file.csv|file.parquet> # run with a file
cargo test                           # run tests
cargo clippy -- -D warnings          # lint (CI enforces warnings as errors)
cargo fmt                            # format
cargo fmt --check                    # check formatting (CI)
cargo build --profile dist           # release/distribution build (thin LTO)
vhs demo.tape                        # regenerate .github/assets/demo.gif (requires vhs + ttyd)
```

## Pre-release QA

Before bumping the version or tagging a release, run the full TUI QA suite:

```bash
cargo build && bash qa.sh
```

`qa.sh` creates a tmux session named `qa` (or reuses one if it exists) and drives the binary
through all 7 modes, every keybinding, all file formats, edge cases (0-row views, wide.csv),
and the null fixture (`tests/fixtures/orders_nulls.csv`).

A release should only be tagged after `qa.sh` exits 0 **and** `cargo test` passes.

## Architecture

Source files under `src/`:

- **`main.rs`** — CLI parsing via `clap`, file loading (CSV/TSV/Parquet/JSON/NDJSON via `polars`), wires `App` into `ratatui::run`.
- **`app.rs`** — All application state and data-manipulation logic. `App` holds two DataFrames: `df` (the original, never mutated after load) and `view` (the current filtered/sorted/grouped result). State is decomposed into focused sub-structs: `SearchState`, `FilterState`, `SortState`, `GroupByState`, `PlotState`, `UniqueValuesState`, `ColumnsViewState`, `ViewportState`. The `Mode` enum drives which keybindings are active.
- **`app_tests.rs`** — Unit tests for `app.rs`, loaded via `#[path]` so they share `app`'s private scope (`FilterQuery`, `parse_operator`, etc.) without requiring visibility changes.
- **`config.rs`** — Application-wide numeric constants (`DEFAULT_COLUMN_WIDTH`, `PAGE_SCROLL_AMOUNT`, etc.).
- **`events.rs`** — The main event loop (`run_app`). Reads crossterm key events and dispatches to `App` methods or small helper functions based on `app.mode`.
- **`ui.rs`** — All ratatui rendering. Uses Catppuccin Mocha (`PALETTE.mocha`) for colors via a thin `c()` helper. `count_visible_from()` handles horizontal viewport windowing; `ViewportState` tracks `row`/`col` offsets so large files stay fast.

### State sub-structs

| Struct | Fields |
|---|---|
| `SearchState` | `query`, `results`, `cursor` |
| `FilterState` | `filters`, `query`, `error`, `col` |
| `SortState` | `column`, `direction`, `error` |
| `GroupByState` | `keys`, `aggs`, `active`, `saved_headers`, `saved_column_widths` |
| `PlotState` | `y_cols`, `x_col`, `plot_type` |
| `UniqueValuesState` | `col`, `values`, `filtered`, `query`, `state`, `truncated` |
| `ColumnsViewState` | `profile`, `state` |
| `ViewportState` | `row`, `col` |

### Mode state machine

`Mode` variants (defined in `app.rs`): `Normal`, `Search`, `Filter`, `PlotPickY`, `PlotPickX`, `Plot`, `ColumnsView`, `UniqueValues`. The event loop in `events.rs` matches on `app.mode` first; `ui.rs` branches on mode to render the appropriate full-screen view or popup overlay.

### Data flow

1. `main.rs` loads the file into a `polars::DataFrame`.
2. `App::new` stores it as `df` and sets `view = df.clone()`.
3. User actions (filter, sort, group-by) call methods on `App` that recompute `view` from `df`.
4. `ui()` renders only the visible window of `view` using `ViewportState.row`/`col` offsets.
