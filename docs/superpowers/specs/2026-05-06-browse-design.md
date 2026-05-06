# datasight browse — Design Spec

**Date:** 2026-05-06
**Status:** Approved

---

## Overview

Add a `browse` subcommand to datasight that launches a split-pane TUI: a file browser on the left and the existing datasight viewer on the right. The browser is filesystem-agnostic — it works against local paths, Azure Blob Storage (`az://`), and AWS S3 (`s3://`). The user navigates to a supported file and opens it; the viewer renders it inline. The browser pane can be toggled in and out like a neovim file explorer.

---

## Invocation

```bash
# Local — defaults to current working directory
datasight browse
datasight browse /path/to/dir

# Azure — credentials from env
AZURE_STORAGE_CONNECTION_STRING="..." datasight browse az://my-container/data/

# AWS — credentials from standard AWS env vars
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... datasight browse s3://my-bucket/prefix/
```

`browse` is a clap subcommand. The path argument is optional and defaults to `"."` (current working directory) when omitted. The URI scheme determines which backend is constructed.

Supported schemes:

| Prefix | Backend | Credentials |
|--------|---------|-------------|
| (none / local path) | `LocalBackend` | none |
| `az://` | `AzureBackend` | `AZURE_STORAGE_CONNECTION_STRING` or `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` |
| `s3://` | `S3Backend` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` |

---

## Module Layout

```
src/
  browser/
    mod.rs       ← FileBrowser trait, Entry type, BrowserError, scheme detection
    local.rs     ← LocalBackend
    azure.rs     ← AzureBackend  (Cargo feature: "azure")
    s3.rs        ← S3Backend     (Cargo feature: "aws")
    app.rs       ← BrowserApp state struct
    events.rs    ← BrowserApp event loop
    ui.rs        ← split-pane rendering
  main.rs        ← adds `browse` subcommand, constructs backend, launches BrowserApp
  app.rs         ← unchanged
  events.rs      ← unchanged
  ui.rs          ← unchanged
```

Azure and S3 backends are gated behind optional Cargo features (`--features azure`, `--features aws`) so local-only builds carry no cloud SDK weight. Both use the `object_store` crate for a unified async listing API.

---

## Core Types

```rust
// browser/mod.rs

pub trait FileBrowser {
    fn list(&self, prefix: &str) -> Result<Vec<Entry>, BrowserError>;
}

pub struct Entry {
    pub name: String,   // display name (last path segment)
    pub path: String,   // full URI, e.g. "az://container/data/sales.csv"
    pub is_dir: bool,
}

pub enum BrowserError {
    Auth(String),
    Network(String),
    NotFound(String),
    Other(String),
}
```

---

## BrowserApp State

```rust
// browser/app.rs

pub struct BrowserApp {
    pub backend: Box<dyn FileBrowser>,
    pub entries: Vec<Entry>,      // current listing
    pub cursor: usize,            // selected row in browser pane
    pub cwd: String,              // current prefix / path
    pub viewer: Option<App>,      // loaded datasight viewer; None until a file is opened
    pub browser_visible: bool,    // toggle state
    pub focus: Focus,             // Browser | Viewer
    pub status: Option<String>,   // one-line error or info message
}

pub enum Focus { Browser, Viewer }
```

`BrowserApp::new(backend, root_path)` immediately calls `backend.list(root_path)` to populate `entries`. An error at this point is shown as `status` — the browser still opens.

---

## Data Flow

1. `main.rs` parses the subcommand, detects scheme, constructs the backend.
2. Backend construction errors (missing credentials, bad URI) → `eprintln!` + `exit(1)` before the TUI starts.
3. `BrowserApp::new` populates the initial listing.
4. The event loop in `browser/events.rs` runs; keyboard events are dispatched based on `focus`.
5. When the user opens a file: `load_dataframe(entry.path)` (cloud paths via `object_store`) → `App::new(df, path)` → stored in `browser_app.viewer`; focus switches to `Viewer`.
6. Directory navigation: `backend.list(new_prefix)` replaces `entries`; `cwd` is updated.

---

## Keyboard Bindings

| Key | Context | Action |
|-----|---------|--------|
| `j` / `k` | Browser focused | Move cursor down / up |
| `.` or `Enter` | Browser, cursor on dir | Descend into directory |
| `h` | Browser focused | Go up to parent prefix (no-op at root) |
| `Enter` | Browser, cursor on file | Load file → right pane; focus → Viewer |
| `ctrl-e` | Anywhere | Toggle browser pane visibility |
| `ctrl-h` | Anywhere | Focus browser pane |
| `ctrl-l` | Anywhere | Focus viewer pane |
| `q` | Browser focused, no viewer | Quit |
| (all existing keys) | Viewer focused | Handled unchanged by existing `App` logic |

`ctrl-h`, `ctrl-l`, and `ctrl-e` are currently unused in the existing event loop. Plain `h`/`l` remain column-navigation keys in the viewer.

---

## Split-Pane Rendering

- Browser pane: 30% width, viewer: 70% (ratatui `Layout::horizontal` with `Constraint::Percentage`).
- When browser is toggled off, viewer expands to 100%.
- A thin vertical border separates the two panes.
- Browser pane title: current path, truncated from the left if too long (e.g. `…/data/2024/`).
- Directories rendered in Catppuccin Mocha blue, files in normal text color, selected row uses existing selection highlight style.
- Right pane when no file is loaded: blank area with centered hint `"Navigate to a file and press Enter to open it"`.
- One-line status bar at the bottom of the browser pane for errors and info messages.

---

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| Missing credentials at startup | `eprintln!` + `exit(1)` before TUI opens |
| Listing error (network, auth, timeout) | Status bar message in browser pane; pane stays navigable |
| File load error (corrupt, wrong content) | Status bar message; right pane reverts to hint placeholder |

---

## Testing

- **Unit:** `LocalBackend::list()` against `tests/fixtures/` — only supported extensions returned, `is_dir` correct, hidden files excluded.
- **Unit:** Scheme detection (`az://`, `s3://`, bare path → local).
- **Integration (cloud):** `AzureBackend` and `S3Backend` tests guarded by env var presence, skipped in CI unless configured. No mocking — consistent with repo philosophy.
- **QA script:** `qa.sh` gets a new section launching `datasight browse tests/fixtures/` in tmux, navigating to a file, opening it, toggling the browser pane, and verifying the viewer renders.

---

## Cloud File Loading

When the user opens a cloud file, `object_store` downloads it as raw bytes, then the bytes are fed into the existing in-memory parsers:

- CSV / TSV / JSON / NDJSON → reuse `parse_buf(bytes, delimiter)` from `main.rs`
- Parquet → `ParquetReader::new(std::io::Cursor::new(bytes)).finish()`

This avoids temp files and reuses all existing format-detection and date-parsing logic. The download is synchronous (blocking on a Tokio runtime created for the call) to keep the TUI event loop simple.

---

## Cargo Changes

```toml
[features]
azure = ["dep:object_store", "object_store/azure"]
aws   = ["dep:object_store", "object_store/aws"]

[dependencies]
object_store = { version = "0.11", optional = true }
```

`object_store` is only compiled when `azure` or `aws` feature is enabled. Each feature activates the corresponding sub-feature on `object_store`.
