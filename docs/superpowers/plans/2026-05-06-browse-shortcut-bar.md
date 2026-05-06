# Browse-Mode Shortcut Bar — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a full-width 1-row keybinding hint bar at the bottom of the `datasight browse` TUI, spanning both panes, that updates based on which pane is focused.

**Architecture:** Single function `browser_shortcut_bar` added to `src/browser/ui.rs`; `browser_ui` gets a 2-zone vertical layout so the bar always occupies the last row. The existing viewer `ui()` and all other files are untouched.

**Tech Stack:** ratatui 0.30, catppuccin 2 (Mocha palette), polars 0.46 (for test DataFrame construction)

---

## File Map

**Modified:**
- `src/browser/ui.rs` — add `browser_shortcut_bar`, update `browser_ui` layout

---

### Task 1: Add browser_shortcut_bar and update browser_ui layout

**Files:**
- Modify: `src/browser/ui.rs`

- [ ] **Step 1: Write failing tests**

Add the following test module at the end of `src/browser/ui.rs`, replacing the existing `mod tests` block (keep the existing 4 truncate tests, append 4 new ones):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{BrowserError, Entry, FileBrowser};

    // ── existing truncate tests (keep as-is) ─────────────────────────────────

    #[test]
    fn test_truncate_path_left_short_path() {
        assert_eq!(truncate_path_left("/data/file.csv", 50), "/data/file.csv");
    }

    #[test]
    fn test_truncate_path_left_long_path() {
        let result = truncate_path_left("/home/user/very/long/path/data.csv", 15);
        assert!(result.starts_with('…'));
        assert!(result.chars().count() <= 15);
    }

    #[test]
    fn test_truncate_path_left_exact_fit() {
        assert_eq!(truncate_path_left("abc", 3), "abc");
    }

    #[test]
    fn test_truncate_path_left_multibyte() {
        let result = truncate_path_left("/héllo.csv", 8);
        assert!(result.starts_with('…'));
        assert!(result.chars().count() <= 8);
    }

    // ── shortcut bar tests ────────────────────────────────────────────────────

    struct StubBackend;
    impl FileBrowser for StubBackend {
        fn list(&self, _: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(vec![])
        }
    }

    fn make_app() -> crate::browser::app::BrowserApp {
        crate::browser::app::BrowserApp::new(Box::new(StubBackend), "/test".to_string())
    }

    fn bar_text(app: &crate::browser::app::BrowserApp) -> String {
        let m = &catppuccin::PALETTE.mocha.colors;
        let line = browser_shortcut_bar(app, m);
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn test_shortcut_bar_browser_hidden() {
        let mut app = make_app();
        app.browser_visible = false;
        let text = bar_text(&app);
        assert!(text.contains("ctrl-e"), "expected ctrl-e in: {}", text);
        assert!(text.contains("Show browser"), "expected 'Show browser' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_viewer_focused() {
        let mut app = make_app();
        app.focus = crate::browser::app::Focus::Viewer;
        let text = bar_text(&app);
        assert!(text.contains("ctrl-h"), "expected ctrl-h in: {}", text);
        assert!(text.contains("Browser"), "expected 'Browser' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_browser_focused_no_viewer_shows_quit() {
        let app = make_app(); // default: browser focused, no viewer
        let text = bar_text(&app);
        assert!(text.contains("j / k"), "expected 'j / k' in: {}", text);
        assert!(text.contains("Navigate"), "expected 'Navigate' in: {}", text);
        assert!(text.contains("q"), "expected 'q' in: {}", text);
        assert!(text.contains("Quit"), "expected 'Quit' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_browser_focused_with_viewer_no_quit() {
        use polars::prelude::*;
        let df = df!("col" => &[1i64]).unwrap();
        let viewer = crate::app::App::new(df, "test.csv".to_string());
        let mut app = make_app();
        app.viewer = Some(viewer);
        let text = bar_text(&app);
        assert!(text.contains("j / k"), "expected 'j / k' in: {}", text);
        assert!(!text.contains("Quit"), "expected no 'Quit' when viewer loaded, got: {}", text);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cargo test browser::ui::tests 2>&1 | tail -15
```

Expected: compilation error — `browser_shortcut_bar` not yet defined.

- [ ] **Step 3: Add browser_shortcut_bar function**

Add the following private function to `src/browser/ui.rs`, immediately before the `#[cfg(test)]` block:

```rust
fn browser_shortcut_bar<'a>(app: &BrowserApp, m: &catppuccin::FlavorColors) -> Line<'a> {
    type Shortcuts = &'static [(&'static str, &'static str)];

    let keys: Shortcuts = if !app.browser_visible {
        &[("ctrl-e", "Show browser")]
    } else if app.focus == Focus::Viewer {
        &[("ctrl-h", "Browser"), ("ctrl-e", "Hide")]
    } else if app.viewer.is_none() {
        &[
            ("j / k", "Navigate"),
            ("Enter", "Open"),
            ("h", "Up"),
            ("ctrl-e", "Hide"),
            ("ctrl-l", "Viewer"),
            ("q", "Quit"),
        ]
    } else {
        &[
            ("j / k", "Navigate"),
            ("Enter", "Open"),
            ("h", "Up"),
            ("ctrl-e", "Hide"),
            ("ctrl-l", "Viewer"),
        ]
    };

    let key_style = Style::default()
        .bg(c(m.blue))
        .fg(c(m.base))
        .add_modifier(Modifier::BOLD);
    let label_style = Style::default().bg(c(m.mantle)).fg(c(m.subtext0));
    let gap_style = Style::default().bg(c(m.mantle));

    let mut spans = Vec::new();
    for (key, action) in keys {
        spans.push(Span::styled(format!(" {} ", key), key_style));
        spans.push(Span::styled(format!(" {} ", action), label_style));
        spans.push(Span::styled("  ", gap_style));
    }

    Line::from(spans).style(Style::default().bg(c(m.mantle)))
}
```

Note: `Span` must be imported. Check the import block at the top of `src/browser/ui.rs`. The current imports are:

```rust
use ratatui::text::Line;
```

Add `Span` to that import:

```rust
use ratatui::text::{Line, Span};
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test browser::ui::tests 2>&1 | tail -15
```

Expected: all 8 tests pass (4 truncate + 4 shortcut bar).

- [ ] **Step 5: Update browser_ui to use the bar**

Replace the current `browser_ui` function body in `src/browser/ui.rs`:

Current:
```rust
pub fn browser_ui(frame: &mut Frame, app: &mut BrowserApp) {
    let m = &PALETTE.mocha.colors;

    if app.browser_visible {
        let [browser_area, viewer_area] =
            Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
                .areas(frame.area());
        render_browser_pane(frame, app, browser_area, m);
        render_viewer_pane(frame, app, viewer_area, m);
    } else {
        render_viewer_pane(frame, app, frame.area(), m);
    }
}
```

Replace with:
```rust
pub fn browser_ui(frame: &mut Frame, app: &mut BrowserApp) {
    let m = &PALETTE.mocha.colors;

    let [content_area, bar_area] =
        Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(frame.area());

    if app.browser_visible {
        let [browser_area, viewer_area] =
            Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
                .areas(content_area);
        render_browser_pane(frame, app, browser_area, m);
        render_viewer_pane(frame, app, viewer_area, m);
    } else {
        render_viewer_pane(frame, app, content_area, m);
    }

    frame.render_widget(Paragraph::new(browser_shortcut_bar(app, m)), bar_area);
}
```

- [ ] **Step 6: Build and run full test suite**

```bash
cargo build 2>&1 | grep "^error" | head -10
cargo test 2>&1 | tail -5
```

Expected: clean build, 189 tests passing (185 + 4 new shortcut bar tests).

- [ ] **Step 7: Format check**

```bash
cargo fmt --check 2>&1 | head -5
```

Expected: clean. If not, run `cargo fmt` and re-check.

- [ ] **Step 8: Commit**

```bash
git add src/browser/ui.rs
git commit -m "feat(browser): full-width shortcut bar at bottom of browse TUI"
```

---

## Done

```bash
cargo test                  # 189 tests pass
cargo clippy -- -D warnings # no warnings
cargo fmt --check           # clean
cargo build                 # clean
```
