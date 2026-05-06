use crate::theme::{default_theme, list_themes, theme_by_name, Theme};

pub struct ThemePicker {
    pub cursor: usize,
    pub original: &'static str,
}

impl ThemePicker {
    pub fn open(current: &'static Theme) -> Self {
        let cursor = list_themes()
            .iter()
            .position(|t| t.name == current.name)
            .unwrap_or(0);
        Self {
            cursor,
            original: current.name,
        }
    }

    pub fn move_up(&mut self) -> &'static Theme {
        let n = list_themes().len();
        self.cursor = if self.cursor == 0 {
            n - 1
        } else {
            self.cursor - 1
        };
        self.current()
    }

    pub fn move_down(&mut self) -> &'static Theme {
        let n = list_themes().len();
        self.cursor = (self.cursor + 1) % n;
        self.current()
    }

    pub fn current(&self) -> &'static Theme {
        &list_themes()[self.cursor]
    }

    pub fn original_theme(&self) -> &'static Theme {
        theme_by_name(self.original).unwrap_or_else(default_theme)
    }
}

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};
use ratatui::Frame;

pub fn render_picker(frame: &mut Frame, area: Rect, picker: &ThemePicker, theme: &Theme) {
    let popup_w = 36u16.min(area.width.saturating_sub(2));
    let popup_h = 13u16.min(area.height.saturating_sub(2));
    let x = area.x + (area.width.saturating_sub(popup_w)) / 2;
    let y = area.y + (area.height.saturating_sub(popup_h)) / 2;
    let popup = Rect {
        x,
        y,
        width: popup_w,
        height: popup_h,
    };

    frame.render_widget(Clear, popup);

    let block = Block::default()
        .title(Line::from(" Theme ").style(Style::default().fg(theme.accent)))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.accent))
        .style(Style::default().bg(theme.bg_alt).fg(theme.fg));

    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let [list_area, footer_area] =
        Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);

    let items: Vec<ListItem> = list_themes()
        .iter()
        .map(|t| {
            ListItem::new(Line::from(vec![
                Span::styled(format!(" {:14}", t.name), Style::default().fg(theme.fg)),
                Span::styled(
                    format!("  {}", t.display_name),
                    Style::default().fg(theme.fg_dim),
                ),
            ]))
        })
        .collect();

    let mut state = ListState::default();
    state.select(Some(picker.cursor));

    let list = List::new(items).highlight_style(
        Style::default()
            .bg(theme.bg_sel)
            .add_modifier(Modifier::BOLD),
    );

    frame.render_stateful_widget(list, list_area, &mut state);

    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" j/k ", Style::default().bg(theme.accent).fg(theme.bg)),
        Span::styled(" navigate  ", Style::default().fg(theme.fg_dim)),
        Span::styled(" Enter ", Style::default().bg(theme.accent).fg(theme.bg)),
        Span::styled(" keep  ", Style::default().fg(theme.fg_dim)),
        Span::styled(" Esc ", Style::default().bg(theme.accent).fg(theme.bg)),
        Span::styled(" cancel", Style::default().fg(theme.fg_dim)),
    ]))
    .alignment(Alignment::Left)
    .style(Style::default().bg(theme.bg_alt));
    frame.render_widget(footer, footer_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_starts_cursor_at_current_theme() {
        let nord = theme_by_name("nord").unwrap();
        let picker = ThemePicker::open(nord);
        assert_eq!(picker.current().name, "nord");
        assert_eq!(picker.original, "nord");
    }

    #[test]
    fn move_down_advances_and_wraps() {
        let mut picker = ThemePicker::open(default_theme());
        let n = list_themes().len();
        for _ in 0..n - 1 {
            picker.move_down();
        }
        let _ = picker.move_down();
        assert_eq!(picker.cursor, 0);
    }

    #[test]
    fn move_up_retreats_and_wraps() {
        let mut picker = ThemePicker::open(default_theme());
        let last = picker.move_up();
        assert_eq!(picker.cursor, list_themes().len() - 1);
        assert_eq!(last.name, list_themes().last().unwrap().name);
    }

    #[test]
    fn original_theme_returns_starting_theme() {
        let dracula = theme_by_name("dracula").unwrap();
        let mut picker = ThemePicker::open(dracula);
        picker.move_down();
        picker.move_down();
        assert_eq!(picker.original_theme().name, "dracula");
    }
}
