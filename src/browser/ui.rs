use crate::browser::app::{BrowserApp, Focus};
use crate::ui::ui;
use catppuccin::PALETTE;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use ratatui::Frame;

fn c(color: catppuccin::Color) -> Color {
    Color::Rgb(color.rgb.r, color.rgb.g, color.rgb.b)
}

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

fn render_browser_pane(
    frame: &mut Frame,
    app: &BrowserApp,
    area: Rect,
    m: &catppuccin::FlavorColors,
) {
    let is_focused = app.focus == Focus::Browser;
    let border_style = if is_focused {
        Style::default().fg(c(m.blue))
    } else {
        Style::default().fg(c(m.overlay0))
    };

    let title = truncate_path_left(&app.cwd, area.width.saturating_sub(4) as usize);
    let block = Block::default()
        .title(Line::from(title).alignment(Alignment::Left))
        .borders(Borders::ALL)
        .border_style(border_style)
        .style(Style::default().bg(c(m.mantle)));

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
        let status = Paragraph::new(msg.as_str()).style(Style::default().fg(c(m.red)));
        frame.render_widget(status, sa);
    }

    let items: Vec<ListItem> = app
        .entries
        .iter()
        .map(|entry| {
            let style = if entry.is_dir {
                Style::default().fg(c(m.blue))
            } else {
                Style::default().fg(c(m.text))
            };
            ListItem::new(entry.name.clone()).style(style)
        })
        .collect();

    let mut list_state = ListState::default();
    list_state.select(Some(app.cursor));

    let list = List::new(items).highlight_style(
        Style::default()
            .bg(c(m.surface1))
            .add_modifier(Modifier::BOLD),
    );

    frame.render_stateful_widget(list, list_area, &mut list_state);
}

fn render_viewer_pane(
    frame: &mut Frame,
    app: &mut BrowserApp,
    area: Rect,
    m: &catppuccin::FlavorColors,
) {
    if let Some(ref mut viewer) = app.viewer {
        ui(frame, viewer, area);
    } else {
        let hint = Paragraph::new("Navigate to a file and press Enter to open it")
            .alignment(Alignment::Center)
            .style(Style::default().fg(c(m.overlay1)))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(c(m.overlay0))),
            );
        frame.render_widget(hint, area);
    }
}

/// Truncate a path from the left so it fits within `max_chars`, prefixing with `…`.
fn truncate_path_left(path: &str, max_chars: usize) -> String {
    if path.len() <= max_chars {
        return path.to_string();
    }
    if max_chars < 2 {
        return "…".to_string();
    }
    let keep = max_chars - 1; // 1 char for `…`
    let start = path.len() - keep;
    format!("…{}", &path[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
