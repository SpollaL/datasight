use crate::browser::app::{BrowserApp, Focus, Viewer};
use crate::text_viewer::render_text_viewer;
use crate::theme::Theme;
use crate::ui::ui;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use ratatui::Frame;

pub fn browser_ui(frame: &mut Frame, app: &mut BrowserApp) {
    let theme: &Theme = app.theme;

    let [content_area, bar_area] =
        Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(frame.area());

    if app.browser_visible {
        let [browser_area, viewer_area] =
            Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
                .areas(content_area);
        render_browser_pane(frame, app, browser_area, theme);
        render_viewer_pane(frame, app, viewer_area, theme);
    } else {
        render_viewer_pane(frame, app, content_area, theme);
    }

    // Whichever prompt owns the keyboard owns the bar, so the hints on screen are
    // the keys that actually do something.
    let bar = match (&app.download, &app.find) {
        (Some(prompt), _) => crate::browser::download::prompt_line(prompt, theme),
        (None, Some(prompt)) => {
            crate::browser::find::prompt_line(prompt, app.matches.len(), app.entries.len(), theme)
        }
        (None, None) => browser_shortcut_bar(app, theme),
    };
    frame.render_widget(Paragraph::new(bar), bar_area);

    if let Some(ref picker) = app.picker {
        crate::theme_picker::render_picker(frame, frame.area(), picker, app.theme);
    }
}

fn render_browser_pane(frame: &mut Frame, app: &BrowserApp, area: Rect, theme: &Theme) {
    let is_focused = app.focus == Focus::Browser;
    let border_style = if is_focused {
        Style::default().fg(theme.accent)
    } else {
        Style::default().fg(theme.border_idle)
    };

    let title = truncate_path_left(&app.cwd, area.width.saturating_sub(4) as usize);
    let block = Block::default()
        .title(Line::from(title).alignment(Alignment::Left))
        .borders(Borders::ALL)
        .border_style(border_style)
        .style(Style::default().bg(theme.bg_alt));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Reserve one line for the status bar when there's a message.
    let (list_area, status_area) = if app.status.is_some() {
        let [l, s] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(inner);
        (l, Some(s))
    } else {
        (inner, None)
    };

    if let (Some(msg), Some(sa)) = (&app.status, status_area) {
        // '✓' marks a confirmation; every other message reports something refused.
        let fg = if msg.contains('✓') {
            theme.success
        } else {
            theme.error
        };
        let status = Paragraph::new(msg.as_str()).style(Style::default().fg(fg));
        frame.render_widget(status, sa);
    }

    // An active query with nothing to show would otherwise render as a blank
    // pane — say why it is empty.
    if app.matches.is_empty() && app.find.is_some() {
        let hint = Paragraph::new("No matches")
            .alignment(Alignment::Center)
            .style(Style::default().fg(theme.fg_muted));
        frame.render_widget(hint, list_area);
        return;
    }

    let items: Vec<ListItem> = app
        .matches
        .iter()
        .map(|m| {
            let entry = &app.entries[m.index];
            let style = match entry.kind {
                crate::browser::EntryKind::Dir => Style::default().fg(theme.accent),
                crate::browser::EntryKind::Data => Style::default().fg(theme.fg),
                crate::browser::EntryKind::Text => Style::default().fg(theme.fg),
                crate::browser::EntryKind::Binary => Style::default().fg(theme.fg_dim),
            };
            ListItem::new(highlight(&entry.name, &m.positions, theme)).style(style)
        })
        .collect();

    let mut list_state = ListState::default();
    list_state.select(Some(app.cursor));

    let list = List::new(items).highlight_style(
        Style::default()
            .bg(theme.bg_sel)
            .add_modifier(Modifier::BOLD),
    );

    frame.render_stateful_widget(list, list_area, &mut list_state);
}

/// Split `name` so the chars the query matched stand out from the rest.
///
/// `positions` are char indices, so the name is walked by chars: slicing by byte
/// offset would cut a multi-byte name mid-character. Spans carry no color of
/// their own except on a hit, leaving the list item's kind color to show through.
fn highlight<'a>(name: &str, positions: &[usize], theme: &Theme) -> Line<'a> {
    if positions.is_empty() {
        return Line::from(name.to_string());
    }
    let hit = Style::default().fg(theme.warn).add_modifier(Modifier::BOLD);

    let mut spans = Vec::new();
    let mut run = String::new();
    let mut run_is_hit = false;
    for (i, c) in name.chars().enumerate() {
        // Query length is a handful of chars, so a scan beats a set.
        let is_hit = positions.contains(&i);
        if is_hit != run_is_hit && !run.is_empty() {
            let style = if run_is_hit { hit } else { Style::default() };
            spans.push(Span::styled(std::mem::take(&mut run), style));
        }
        run_is_hit = is_hit;
        run.push(c);
    }
    let style = if run_is_hit { hit } else { Style::default() };
    spans.push(Span::styled(run, style));

    Line::from(spans)
}

fn render_viewer_pane(frame: &mut Frame, app: &mut BrowserApp, area: Rect, theme: &Theme) {
    match app.viewer {
        Some(Viewer::DataFrame(ref mut a)) => ui(frame, a, area),
        Some(Viewer::Text(ref mut t)) => render_text_viewer(frame, t, area, theme),
        None => {
            let hint = Paragraph::new("Navigate to a file and press Enter to open it")
                .alignment(Alignment::Center)
                .style(Style::default().fg(theme.fg_muted))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(theme.border_idle)),
                );
            frame.render_widget(hint, area);
        }
    }
}

fn browser_shortcut_bar<'a>(app: &BrowserApp, theme: &Theme) -> Line<'a> {
    let keys: Vec<(&str, &str)> = if !app.browser_visible {
        vec![("ctrl-e", "Show browser")]
    } else if app.focus == Focus::Viewer {
        vec![("tab", "Browser"), ("ctrl-e", "Hide"), ("q", "Quit")]
    } else {
        let mut keys = vec![
            ("j / k", "Navigate"),
            (". / Enter", "Open"),
            ("Esc", "Up"),
            ("/", "Find"),
            ("ctrl-e", "Hide"),
            ("tab", "Viewer"),
        ];
        // Downloading is offered only where it means something: a local listing is
        // already on disk.
        if crate::browser::is_remote(&app.cwd) {
            keys.push(("d", "Download"));
        }
        keys.push(("q", "Quit"));
        keys
    };

    let key_style = Style::default()
        .bg(theme.accent)
        .fg(theme.bg)
        .add_modifier(Modifier::BOLD);
    let label_style = Style::default().bg(theme.bg_alt).fg(theme.fg_dim);
    let gap_style = Style::default().bg(theme.bg_alt);

    let mut spans = Vec::new();
    for (key, action) in keys {
        spans.push(Span::styled(format!(" {} ", key), key_style));
        spans.push(Span::styled(format!(" {} ", action), label_style));
        spans.push(Span::styled("  ", gap_style));
    }

    Line::from(spans).style(Style::default().bg(theme.bg_alt))
}

/// Truncate a path from the left so it fits within `max_chars`, prefixing with `…`.
fn truncate_path_left(path: &str, max_chars: usize) -> String {
    let char_count = path.chars().count();
    if char_count <= max_chars {
        return path.to_string();
    }
    if max_chars < 2 {
        return "…".to_string();
    }
    let keep = max_chars - 1;
    let start_char = char_count - keep;
    let start_byte = path
        .char_indices()
        .nth(start_char)
        .map(|(i, _)| i)
        .unwrap_or(path.len());
    format!("…{}", &path[start_byte..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{BrowserError, Entry, FileBrowser};

    // ── truncate tests ────────────────────────────────────────────────────────

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
        crate::browser::app::BrowserApp::new(
            Box::new(StubBackend),
            "/test".to_string(),
            crate::theme::default_theme(),
        )
    }

    fn bar_text(app: &crate::browser::app::BrowserApp) -> String {
        let line = browser_shortcut_bar(app, crate::theme::default_theme());
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn test_shortcut_bar_browser_hidden() {
        let mut app = make_app();
        app.browser_visible = false;
        let text = bar_text(&app);
        assert!(text.contains("ctrl-e"), "expected ctrl-e in: {}", text);
        assert!(
            text.contains("Show browser"),
            "expected 'Show browser' in: {}",
            text
        );
    }

    #[test]
    fn test_shortcut_bar_viewer_focused() {
        let mut app = make_app();
        app.focus = crate::browser::app::Focus::Viewer;
        let text = bar_text(&app);
        assert!(text.contains("tab"), "expected tab in: {}", text);
        assert!(text.contains("Browser"), "expected 'Browser' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_browser_focused_no_viewer_shows_quit() {
        let app = make_app();
        let text = bar_text(&app);
        assert!(text.contains("j / k"), "expected 'j / k' in: {}", text);
        assert!(
            text.contains("Navigate"),
            "expected 'Navigate' in: {}",
            text
        );
        assert!(text.contains("q"), "expected 'q' in: {}", text);
        assert!(text.contains("Quit"), "expected 'Quit' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_offers_quit_with_a_viewer_loaded() {
        use polars::prelude::*;
        let df = df!("col" => &[1i64]).unwrap();
        let viewer =
            crate::app::App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        let mut app = make_app();
        app.viewer = Some(Viewer::DataFrame(Box::new(viewer)));
        let text = bar_text(&app);
        assert!(text.contains("j / k"), "expected 'j / k' in: {}", text);
        // `q` used to be swallowed once a file was open, leaving the browser pane
        // with no way out at all.
        assert!(
            text.contains("Quit"),
            "expected 'Quit' with a viewer loaded, got: {}",
            text
        );
    }

    #[test]
    fn test_shortcut_bar_offers_find() {
        let text = bar_text(&make_app());
        assert!(text.contains("Find"), "expected 'Find' in: {}", text);
    }

    #[test]
    fn test_shortcut_bar_viewer_focused_offers_quit() {
        let mut app = make_app();
        app.focus = crate::browser::app::Focus::Viewer;
        let text = bar_text(&app);
        assert!(text.contains("Quit"), "expected 'Quit' in: {}", text);
    }

    // ── highlight ─────────────────────────────────────────────────────────────

    fn highlighted(name: &str, positions: &[usize]) -> Vec<String> {
        highlight(name, positions, crate::theme::default_theme())
            .spans
            .iter()
            .map(|s| s.content.to_string())
            .collect()
    }

    #[test]
    fn test_highlight_without_positions_is_one_span() {
        assert_eq!(highlighted("orders.csv", &[]), vec!["orders.csv"]);
    }

    #[test]
    fn test_highlight_splits_runs_around_the_matches() {
        // "or" then "d" plain, "e" hit, rest plain.
        assert_eq!(
            highlighted("orders.csv", &[0, 1, 4]),
            vec!["or", "de", "r", "s.csv"]
        );
    }

    #[test]
    fn test_highlight_reassembles_the_original_name() {
        // Char-indexed positions on a multi-byte name: the spans must still join
        // back into exactly what the backend listed.
        let joined = highlighted("héllo.csv", &[1, 2]).concat();
        assert_eq!(joined, "héllo.csv");
    }
}
