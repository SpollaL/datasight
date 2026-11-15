# Contributing to datasight

Thanks for your interest in contributing!

## Development setup

Requires Rust 1.75 or higher. Install via [rustup](https://rustup.rs/).

```
git clone https://github.com/SpollaL/datasight
cd datasight
cargo build
cargo run -- <path-to-file.csv>
```

## Documentation

The user manual lives in `manual/src/` as Markdown. Preview it with
[mdBook](https://rust-lang.github.io/mdBook/): `mdbook serve manual`. A change to keys
or behaviour should update its chapter and `cheat-sheet.md`.

## Before submitting a PR

CI must pass:

```
cargo test
cargo clippy -- -D warnings
cargo fmt --check
cargo build && bash qa.sh     # TUI QA suite, needs tmux
```

Run `cargo fmt` (no `--check`) to auto-format before committing.

The PR title must be a [Conventional Commit](https://www.conventionalcommits.org/)
(`feat: …`, `fix(browse): …`). Merging to main releases automatically: `feat` and `fix`
titles become the changelog entries and pick the version bump. `git config core.hooksPath
.githooks` checks your commit messages the same way locally.

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
- The datasight version (`datasight --version` or from the release page)
- Your OS and terminal
- A minimal CSV/Parquet file that reproduces the issue, if applicable
- Steps to reproduce
