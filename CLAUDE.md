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

## Architecture

The entire application is four files under `src/`:

- **`main.rs`** — CLI parsing via `clap`, file loading (CSV/Parquet via `polars`), wires `App` into `ratatui::run`.
- **`app.rs`** — All application state (`App` struct) and data-manipulation logic: filtering, sorting, group-by, search, column profiling, unique values, autofit. The `Mode` enum drives which keybindings are active. Two DataFrames are kept: `df` (the original, never mutated after load) and `view` (the current filtered/sorted/grouped result).
- **`events.rs`** — The main event loop (`run_app`). Reads crossterm key events and dispatches to `App` methods or inline state changes based on `app.mode`.
- **`ui.rs`** — All ratatui rendering. Uses Catppuccin Mocha (`PALETTE.mocha`) for colors via a thin `c()` helper. Implements viewport windowing: `view_offset` (vertical) and `col_offset` (horizontal) track which slice of the DataFrame is currently visible so that large files stay fast.

### Mode state machine

`Mode` variants (defined in `app.rs`): `Normal`, `Search`, `Filter`, `PlotPickX`, `Plot`, `ColumnsView`, `UniqueValues`. The event loop in `events.rs` matches on `app.mode` first; `ui.rs` branches on mode to render the appropriate full-screen view or popup overlay.

### Data flow

1. `main.rs` loads the file into a `polars::DataFrame`.
2. `App::new` stores it as `df` and sets `view = df.clone()`.
3. User actions (filter, sort, group-by) call methods on `App` that recompute `view` from `df` (or from the existing `view` for chained filters).
4. `ui()` renders only the visible window of `view` using `view_offset`/`col_offset`.
