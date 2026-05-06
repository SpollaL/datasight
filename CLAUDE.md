# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

After cloning, activate the pre-commit hooks:

```bash
git config core.hooksPath .githooks
```

## Commands

```bash
cargo build                                             # debug build
cargo run -- <file.csv|file.parquet>                    # run with a file
cargo run -- browse <path|az://container|s3://bucket>   # file browser TUI
cargo test                                              # run tests
cargo clippy -- -D warnings                             # lint (CI enforces warnings as errors)
cargo fmt                                               # format
cargo fmt --check                                       # check formatting (CI)
cargo build --profile dist                              # release/distribution build (thin LTO)
cargo build --features azure                            # include Azure backend
cargo build --features aws                              # include S3 backend
cargo install --path . --features azure                 # install with Azure support
vhs demo.tape                                           # regenerate .github/assets/demo.gif (requires vhs + ttyd)
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

When implementing a new feature that changes keybindings, adds modes, or modifies existing TUI interactions, update `qa.sh` to cover the new behaviour before opening the PR.

## Architecture

Source files under `src/`:

- **`main.rs`** — CLI parsing via `clap`, file loading (CSV/TSV/Parquet/JSON/NDJSON via `polars`), wires `App` into `ratatui::run`.
- **`app.rs`** — All application state and data-manipulation logic. `App` holds two DataFrames: `df` (the original, never mutated after load) and `view` (the current filtered/sorted/grouped result). State is decomposed into focused sub-structs: `SearchState`, `FilterState`, `SortState`, `GroupByState`, `PlotState`, `UniqueValuesState`, `ColumnsViewState`, `ViewportState`. The `Mode` enum drives which keybindings are active.
- **`app_tests.rs`** — Unit tests for `app.rs`, loaded via `#[path]` so they share `app`'s private scope (`FilterQuery`, `parse_operator`, etc.) without requiring visibility changes.
- **`config.rs`** — Application-wide numeric constants (`DEFAULT_COLUMN_WIDTH`, `PAGE_SCROLL_AMOUNT`, etc.).
- **`events.rs`** — The main event loop (`run_app`). Reads crossterm key events and dispatches to `App` methods or small helper functions based on `app.mode`.
- **`ui.rs`** — All ratatui rendering. Resolves colors from the active `&'static Theme` (`app.theme`) — no hardcoded palette references. `count_visible_from()` handles horizontal viewport windowing; `ViewportState` tracks `row`/`col` offsets so large files stay fast.
- **`theme.rs`** — Base16 theme system. Owns 9 built-in `Base16Scheme` constants, the semantic `Theme` struct (slot-based: `bg`, `bg_alt`, `accent`, `error`, `series[6]`, etc.), state file I/O at `~/.config/datasight/state.toml`, and the `resolve_theme(cli, env, state)` precedence function (CLI > env > state file > `mocha`).
- **`theme_picker.rs`** — `ThemePicker` state (cursor + original theme name) and `render_picker()` popup helper, used by both `App` (plain mode) and `BrowserApp` (browse mode).

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

`Mode` variants (defined in `app.rs`): `Normal`, `Search`, `Filter`, `PlotPickY`, `PlotPickX`, `Plot`, `ColumnsView`, `UniqueValues`, `ThemePicker`. The event loop in `events.rs` matches on `app.mode` first; `ui.rs` branches on mode to render the appropriate full-screen view or popup overlay. In browse mode (`BrowserApp`), the picker is gated by an `Option<ThemePicker>` field instead of a mode variant, and `BrowserApp` propagates its `&'static Theme` to the viewer's `App` on file load and on every picker key event so live preview stays in sync across both panes.

### Data flow

1. `main.rs` loads the file into a `polars::DataFrame`.
2. `App::new` stores it as `df` and sets `view = df.clone()`.
3. User actions (filter, sort, group-by) call methods on `App` that recompute `view` from `df`.
4. `ui()` renders only the visible window of `view` using `ViewportState.row`/`col` offsets.

### Browse subcommand

`src/browser/` contains the file browser feature (`datasight browse`):

- **`mod.rs`** — `FileBrowser` trait, `Entry`, `BrowserError`, `build_backend`, `load_file_for_browser`
- **`app.rs`** — `BrowserApp` state, `Focus` enum (`Browser`/`Viewer`), navigation methods
- **`events.rs`** — `run_browser_app` event loop; `Tab` toggles focus, `ctrl-e` toggles sidebar, `Esc` ascends
- **`ui.rs`** — `browser_ui` split-pane renderer, `browser_shortcut_bar` (context-aware 1-row hint bar)
- **`local.rs`** — `LocalBackend` (always compiled)
- **`azure.rs`** — `AzureBackend` behind `--features azure` (object_store + tokio)
- **`s3.rs`** — `S3Backend` behind `--features aws`

**Gotchas:**
- `ui()` in `src/ui.rs` takes `area: Rect` — needed so the viewer renders in a sub-pane
- `dispatch_viewer_key` in `src/events.rs` is `pub(crate)` — reused by browser event loop
- `e` is taken by the viewer's stats toggle; browser sidebar uses `ctrl-e`
- Azure: `object_store::MicrosoftAzureBuilder::from_env()` ignores `AZURE_STORAGE_CONNECTION_STRING`; `azure.rs` parses it manually. HTTP `BlobEndpoint` values (Azurite) require `with_allow_http(true)`.

### Browse keybindings

| Key | Action |
|---|---|
| `j` / `k` | Navigate list |
| `. / Enter` | Open file or enter directory |
| `Esc` | Go up to parent |
| `Tab` | Toggle focus browser ↔ viewer |
| `ctrl-e` | Toggle browser sidebar |
| `q` | Quit (only when no viewer loaded) |
