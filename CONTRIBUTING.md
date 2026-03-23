# Contributing to iron-sight

Thanks for your interest in contributing!

## Development setup

Requires Rust 1.75 or higher. Install via [rustup](https://rustup.rs/).

```
git clone https://github.com/SpollaL/iron-sight
cd iron-sight
cargo build
cargo run -- <path-to-file.csv>
```

## Before submitting a PR

All three CI checks must pass:

```
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

Run `cargo fmt` (no `--check`) to auto-format before committing.

## Project structure

| File | Responsibility |
|------|----------------|
| `src/app.rs` | `App` struct, all state and data logic |
| `src/events.rs` | Event loop and key dispatch per mode |
| `src/ui.rs` | ratatui rendering: table, status bar, popups, plot |
| `src/main.rs` | Entry point, CLI argument parsing |

## Adding a new mode or feature

1. Add a variant to the `Mode` enum in `app.rs` if needed
2. Handle the new mode's key events in `events.rs`
3. Add rendering in `ui.rs`
4. Add unit tests in the relevant file

## Tests

Tests live at the bottom of each source file. Run them with `cargo test`.

Use the `make_app()` helper in `app.rs` tests to avoid boilerplate. Add test cases for edge cases (empty dataframes, zero-row results, etc.).

## Reporting bugs

Open an issue on GitHub with:
- The iron-sight version (`iron-sight --version` or from the release page)
- Your OS and terminal
- A minimal CSV/Parquet file that reproduces the issue, if applicable
- Steps to reproduce
