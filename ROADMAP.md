# Roadmap

This document captures what's needed to make datasight professional — simple but powerful, reliable, stable, and bug-free. Informed by a survey of VisiData, csvlens, tabiew, tidy-viewer, and qsv.

---

## Reliability / Bug-risk

### P0 — Crash on bad file input
`main.rs` calls `eprintln! + exit(1)` on any load failure (misnamed file, malformed CSV, unsupported encoding). The TUI never starts and the user gets a raw error dump. Every serious TUI tool recovers gracefully: start the app, display the error as a message, let the user quit cleanly.

### P1 — Unwraps in plot rendering
`ui.rs` has `.unwrap()` on `.f64()` casts in the plot path, after pre-checks. The pre-checks are likely sound, but if a Decimal or mixed-type column edge case slips through, the result is a panic rather than a "cannot plot this column" message in the UI.

### P1 — Terminal resize not covered by QA
`qa.sh` does not test terminal resize. Ratatui handles `Event::Resize` via crossterm, but the app must listen for it and call `terminal.clear()` before the next draw. If not wired, resizing produces garbage. Worth verifying and adding a resize step to `qa.sh`.

### P1 — Null values are invisible
Actual `null` and an empty string cell look identical. This is a data quality signal that's currently invisible. The fix: render `∅` or `NA` in a muted/distinct color for null cells. tidy-viewer's approach here is the reference.

---

## Features — Tier 1 (high impact, keeps the tool simple)

### Copy cell / row to clipboard
The single most-reached-for action after finding something in the data. csvlens has it. Suggested keys: `y` for current cell, `Y` for the full row. Use the `arboard` crate (cross-platform, no system dependency on Linux if Wayland/X11 is available). Scope: Normal mode only, no mode changes needed.

### Export current view to file
When a user has filtered 200 rows out of 1M, they cannot save what they see. This breaks every downstream workflow. Implementation: `Ctrl+S` opens a filename prompt at the bottom of the screen; on confirm, write `app.view` to that path as CSV using `polars::DataFrame::write_csv`. The exported file should reflect the current filtered/sorted/grouped state, not the original data.

### Regex filter
Substring match is useful; regex is the power escape hatch that makes filtering feel unlimited. Polars already has regex support. Suggested syntax: if the filter query is wrapped in `/…/` (e.g. `/foo|bar/`), treat it as a regex; otherwise fall back to current substring behavior. Numeric operators are unaffected.

### Filter by current cell value
VisiData's `,` key — instantly filter to rows where the current column equals the current cell value. One keystroke replaces an entire type-and-confirm cycle. This is the most-used interactive gesture in any data tool. Implementation: read `app.view[app.selected_col][app.viewport.row]`, escape as an exact-match filter (`= value`), and commit it.

---

## Features — Tier 2 (significant, more state management)

### Column hide / unhide
Wide datasets (50+ columns) are unusable without the ability to hide irrelevant columns. Suggested UX: press `H` in the column inspector (`i` mode) to hide the highlighted column; a separate key (e.g. `Shift+H`) to reveal all hidden columns. Hidden columns should be excluded from rendering, autofit, and export.

### Freeze first column
When scrolling right in a wide dataset, the row identifier (first column) disappears. csvlens has this. Suggested key: `Ctrl+F` to toggle pinning the leftmost column so it always renders regardless of horizontal scroll offset.

### Row/col position always in status bar
The status bar should always show `row 42/1000  col 3/15` — even during filter/search input. Currently unclear whether this drops out during input modes. Active filter count (`2 filters`), active sort, and groupby indicator should also be persistent.

### Column context in filter prompt
When pressing `f` to filter a specific column, there is no persistent visual indicator of *which column* is being filtered while typing. A `[col_name]:` prefix in the filter input line would prevent a common mistake where the user filters the wrong column.

---

## Features — Tier 3 (defer until Tier 1/2 are done)

These are deliberately out of scope to keep the tool simple:

- **Undo stack** — VisiData's undo is opt-in and incomplete. Only add if a correct full implementation is feasible; a partial undo is worse than none because it creates false confidence.
- **Multiple file tabs** — changes the mental model significantly; a different tool category.
- **Computed / derived columns** — spreadsheet territory; crossing this line changes the tool's identity.
- **Config file** — add only if default column widths or keybindings become a real user pain point.
- **Python / SQL expression filter** — impressive in VisiData but adds a language runtime dependency and contradicts the "simple tool" goal.
- **Command log / replay** — high implementation complexity; changes the tool's scope.

---

## UX / Discoverability

### Mode-filtered help overlay
The `?` help popup exists and is scrollable, which is good. But if it always dumps all 30+ keybindings regardless of the active mode, users in `PlotPickY` mode scroll past 20 irrelevant entries. Filtering the help list to only bindings active in the current mode would be a meaningful improvement. VisiData's `Ctrl+H` is the reference implementation.

### Status bar information density
Always-visible state that should be in the status bar at all times:

```
datasight  orders.csv  |  row 42/1000  col 3/15  |  2 filters  |  sort: revenue ▲  |  FILTER
```

Currently, entering a mode may clobber state indicators. The mode name should be in addition to, not instead of, position and filter context.

---

## Testing gaps

| Gap | Risk |
|---|---|
| `qa.sh` cannot run in CI (requires tmux) | Manual pre-release gate is easy to skip under time pressure |
| No test for malformed UTF-8 in CSV | Common in real-world data; currently an untested crash path |
| No test for empty Parquet file | Polars may panic or return an unusual schema |
| No test for terminal resize | Could produce garbled output with no existing coverage |
| No test for clipboard / export once added | New features need integration coverage at the point of introduction |
| No test for regex filter once added | Edge cases (empty pattern, invalid regex) need explicit coverage |

The structural problem: `qa.sh` cannot run in CI. Consider using ratatui's `TestBackend` for the highest-risk UI paths (filter rendering, plot rendering on edge-case data, resize). This does not need to replace `qa.sh` — just automate the cases most likely to regress silently.

---

## What VisiData does well that datasight already beats

- **Performance on large files** — VisiData is pure Python and lags noticeably at ~20K rows. Polars gives datasight a genuine advantage here that should be protected: avoid any design that forces full-DataFrame copies on every keystroke.
- **Null visualization** — tidy-viewer and VisiData both handle this better; fixing it (see P1 above) closes the gap.
- **Schema inference** — scanning the full file for schema inference (not just first 100 rows) is already implemented and is better behavior than most tools.

## What VisiData does that datasight should deliberately not copy

- Python expression filters — adds a language runtime, breaks the "simple" constraint
- Sheet-of-sheets model — powerful but steep; the current modal model is more learnable
- Cell editing — crosses into spreadsheet territory and changes the tool's identity

---

## Implementation order

1. Null display — one-day fix, immediate data quality improvement
2. Fix unwraps in plot path — reliability, low-risk change
3. Copy cell / row to clipboard — highest user-facing impact
4. Regex filter — polars already has it; syntax design is the main work
5. Filter by current cell value — single keybinding, calls existing filter machinery
6. Export view to CSV — one `write_csv` call + filename prompt
7. Column hide / unhide — more state to manage; follows column inspector patterns
8. Mode-filtered help overlay — polish, meaningful for discoverability
9. Status bar polish — always-visible filter / sort / position indicator
10. Freeze first column — viewport change, medium complexity
