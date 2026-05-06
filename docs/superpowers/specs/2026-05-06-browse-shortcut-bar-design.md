# Browse-Mode Shortcut Bar — Design Spec

**Date:** 2026-05-06
**Status:** Approved

---

## Overview

Add a persistent 1-row shortcut bar at the very bottom of the `datasight browse` TUI, spanning the full terminal width. The bar shows context-aware keybinding hints — similar to the Zellij status bar and the existing viewer shortcut bar. The standard `datasight <file>` viewer is unaffected.

---

## Layout Change

`browser_ui` currently fills the full frame area with a horizontal split:

```
┌─────────────────────────────────────────────────┐
│  browser pane 30%  │  viewer pane 70%           │
└─────────────────────────────────────────────────┘
```

After this change, a 1-row bar is reserved at the bottom:

```
┌─────────────────────────────────────────────────┐
│  browser pane 30%  │  viewer pane 70%           │  ← Min(1)
├─────────────────────────────────────────────────┤
│  j/k Navigate   Enter Open   h Up   ctrl-e Hide │  ← Length(1)
└─────────────────────────────────────────────────┘
```

The top zone (`content_area`) is split horizontally as before. The bottom zone (`bar_area`) receives the shortcut bar.

When the browser pane is hidden, the layout becomes:

```
┌─────────────────────────────────────────────────┐
│  viewer (full width)                            │  ← Min(1)
├─────────────────────────────────────────────────┤
│  ctrl-e  Show browser                           │  ← Length(1)
└─────────────────────────────────────────────────┘
```

---

## Bar Content

Three states based on `app.browser_visible` and `app.focus`:

| Condition | Keys shown |
|---|---|
| `!browser_visible` | `ctrl-e Show browser` |
| `browser_visible && focus == Viewer` | `ctrl-h Browser` `ctrl-e Hide` |
| `browser_visible && focus == Browser && viewer.is_none()` | `j/k Navigate` `Enter Open` `h Up` `ctrl-e Hide` `ctrl-l Viewer` `q Quit` |
| `browser_visible && focus == Browser && viewer.is_some()` | `j/k Navigate` `Enter Open` `h Up` `ctrl-e Hide` `ctrl-l Viewer` |

`q Quit` is omitted when a viewer is loaded because the viewer's own shortcut bar (in the right pane) handles quit and all other viewer-mode keys.

---

## Styling

Matches the existing viewer `shortcut_bar()` exactly:

- Key pill: `bg(blue) fg(base) BOLD` — blue background, dark text, bold
- Label: `bg(mantle) fg(subtext0)` — muted text on dark background  
- Gap between entries: `bg(mantle)` — 2 spaces
- Bar background: `bg(mantle)` — fills remainder of the row

All colors are Catppuccin Mocha palette tokens via the `c()` helper.

---

## Implementation

**Single file changed:** `src/browser/ui.rs`

Two modifications:

1. **`browser_ui`**: add a 2-zone vertical layout (`content_area` + `bar_area`), pass `content_area` to pane rendering, render `browser_shortcut_bar` at `bar_area`.

2. **New private function** `browser_shortcut_bar<'a>(app: &BrowserApp, m: &FlavorColors) -> Line<'a>`: returns a `Line` of styled spans encoding the three-state keybinding list above. Pattern is identical to `shortcut_bar()` in `src/ui.rs`.

No changes to `src/ui.rs`, `src/events.rs`, `src/app.rs`, `src/main.rs`, or any file outside `src/browser/ui.rs`.

---

## Testing

Unit tests in `src/browser/ui.rs` (within the existing `#[cfg(test)] mod tests` block):

- `test_shortcut_bar_browser_hidden`: `browser_visible = false` → spans contain `"ctrl-e"` and `"Show browser"`.
- `test_shortcut_bar_viewer_focused`: `focus = Focus::Viewer` → spans contain `"ctrl-h"` and `"Browser"`.
- `test_shortcut_bar_browser_focused_no_viewer`: default state → spans contain `"j / k"`, `"Navigate"`, `"q"`, `"Quit"`.
- `test_shortcut_bar_browser_focused_with_viewer`: viewer loaded → spans contain `"j / k"` but NOT `"Quit"`.

Tests inspect span text content directly (no render backend needed).
