//! Terminal rendering for all modes and overlays.
//!
//! The top-level entry point is [`ui`], which routes to a full-screen renderer
//! ([`render_plot`], [`render_columns_view`]) or builds the main table view with
//! overlays ([`render_stats_popup`], [`render_help_popup`],
//! [`render_unique_values_popup`]).
//!
//! Colors come from the active [`Theme`] resolved at startup; each renderer
//! receives `theme: &Theme` and reads semantic slots (`theme.bg`, `theme.accent`,
//! `theme.series[N]`, …). Viewport windowing is handled by [`count_visible_from`],
//! which computes how many columns fit a given terminal width starting from a
//! column offset.

use crate::app::{AggFunc, App, ColumnProfile, Mode, PlotType, SortDirection};
use crate::config;
use crate::theme::Theme;
use polars::prelude::{DataType, Series};
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Axis, Block, BorderType, Borders, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table,
};
use ratatui::Frame;

const NULL_GLYPH: &str = "∅";

// None → muted ∅ glyph so a real null is distinguishable from an empty-string cell.
fn format_cell<'a>(value: Option<&str>, theme: &Theme) -> Cell<'a> {
    match value {
        None => Cell::from(NULL_GLYPH).style(Style::default().fg(theme.fg_muted)),
        Some(s) => Cell::from(s.to_string()),
    }
}

pub fn ui(frame: &mut Frame, app: &mut App, area: Rect) {
    let theme = app.theme;

    if matches!(app.mode, Mode::Plot) {
        render_plot(frame, app, theme, area);
        return;
    }

    if matches!(app.mode, Mode::ColumnsView) {
        render_columns_view(frame, app, theme, area);
        return;
    }

    let chunks = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);

    // 2 borders + 1 header row + 1 header bottom-margin = 4 rows of overhead.
    let page_h = (chunks[0].height.saturating_sub(4)) as usize;
    let total_rows = app.view.height();
    let selected = app.state.selected().unwrap_or(0);

    // Scroll the viewport to keep `selected` visible.
    if selected < app.viewport.row {
        app.viewport.row = selected;
    } else if page_h > 0 && selected >= app.viewport.row + page_h {
        app.viewport.row = selected.saturating_sub(page_h - 1);
    }
    // Don't let the offset run past the last page.
    app.viewport.row = app
        .viewport
        .row
        .min(total_rows.saturating_sub(page_h.max(1)));

    let slice_len = page_h.min(total_rows.saturating_sub(app.viewport.row));
    let visible_view = app.view.slice(app.viewport.row as i64, slice_len);

    // Horizontal windowing: only pass columns that fit the terminal width to ratatui.
    // 2 border chars; column spacing of 1 between every pair of adjacent columns.
    let available_w = chunks[0].width.saturating_sub(2) as usize;
    let total_cols = app.headers.len();
    let selected_col = app.state.selected_column().unwrap_or(0);

    // Scroll col_offset to keep selected_col visible.
    if selected_col < app.viewport.col {
        app.viewport.col = selected_col;
    } else {
        let vis = count_visible_from(&app.column_widths, app.viewport.col, available_w);
        if selected_col >= app.viewport.col + vis {
            app.viewport.col = selected_col.saturating_sub(vis.saturating_sub(1));
        }
    }
    app.viewport.col = app.viewport.col.min(total_cols.saturating_sub(1));

    let vis_count = count_visible_from(&app.column_widths, app.viewport.col, available_w);
    let vis_cols: Vec<usize> = (app.viewport.col..total_cols).take(vis_count).collect();

    let header_cells = Row::new(vis_cols.iter().map(|&i| {
        Cell::from(app.header_label(i))
            .style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD))
    }))
    .style(Style::default().bg(theme.bg_alt));

    // Pre-cast only the visible columns to String series.
    let all_columns = visible_view.get_columns();
    let str_columns: Vec<Option<Series>> = vis_cols
        .iter()
        .map(|&i| {
            all_columns
                .get(i)
                .map(|col| col.as_materialized_series())
                .and_then(|s| s.cast(&DataType::String).ok())
        })
        .collect();

    let rows: Vec<Row> = (0..slice_len)
        .map(|i| {
            let abs_row = app.viewport.row + i;
            let bg = if abs_row % 2 == 0 {
                theme.bg
            } else {
                theme.bg_alt
            };
            Row::new(
                str_columns
                    .iter()
                    .map(|s| {
                        let opt = s
                            .as_ref()
                            .and_then(|series| series.str().ok())
                            .and_then(|ca| ca.get(i));
                        format_cell(opt, theme)
                    })
                    .collect::<Vec<Cell>>(),
            )
            .style(Style::default().bg(bg).fg(theme.fg))
        })
        .collect();

    let widths: Vec<Constraint> = vis_cols
        .iter()
        .map(|&i| Constraint::Length(app.column_widths[i]))
        .collect();

    let table = Table::new(rows, widths)
        .header(header_cells.bottom_margin(1))
        .block(
            Block::default()
                .title(format!(" {} ", app.file_path))
                .title_style(
                    Style::default()
                        .fg(theme.accent)
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.border_idle))
                .style(Style::default().bg(theme.bg)),
        )
        .row_highlight_style(Style::default().bg(theme.bg_alt))
        .column_highlight_style(Style::default().bg(theme.bg_sel))
        .cell_highlight_style(
            Style::default()
                .bg(theme.accent)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        );

    let (bar_text, bar_style) = get_bar(app, theme);
    let bar = Paragraph::new(bar_text).style(bar_style);

    // Render with a temporary state. Column index is relative to the visible window.
    let mut render_state = ratatui::widgets::TableState::default();
    render_state.select(Some(selected.saturating_sub(app.viewport.row)));
    render_state.select_column(Some(selected_col.saturating_sub(app.viewport.col)));
    frame.render_stateful_widget(table, chunks[0], &mut render_state);
    frame.render_widget(bar, chunks[1]);
    frame.render_widget(Paragraph::new(shortcut_bar(app, theme)), chunks[2]);

    if app.show_stats {
        render_stats_popup(frame, app, theme);
    }

    if app.show_help {
        render_help_popup(frame, app, theme);
    }

    if matches!(app.mode, Mode::UniqueValues) {
        render_unique_values_popup(frame, app, theme);
    }

    if app.mode == Mode::ThemePicker {
        if let Some(ref picker) = app.picker {
            crate::theme_picker::render_picker(frame, area, picker, app.theme);
        }
    }
}

/// Returns the number of columns that fit within `available_w` terminal cells
/// starting from column index `start`. Columns are separated by 1 spacing cell
/// except before the first column. Always returns at least 1.
fn count_visible_from(column_widths: &[u16], start: usize, available_w: usize) -> usize {
    let mut used = 0usize;
    let mut count = 0usize;
    for i in start..column_widths.len() {
        let w = column_widths.get(i).copied().unwrap_or(15) as usize;
        let needed = if count == 0 { w } else { w + 1 }; // +1 column spacing
        if used + needed > available_w && count > 0 {
            break;
        }
        used += needed;
        count += 1;
    }
    count.max(1)
}

fn render_stats_popup(frame: &mut Frame, app: &mut App, theme: &Theme) {
    let col = app
        .state
        .selected_column()
        .unwrap_or(0)
        .min(app.headers.len().saturating_sub(1));
    let stats = app.get_or_compute_stats(col);
    let area = centered_rect(40, 40, frame.area());
    frame.render_widget(Clear, area);
    let content = format!(
        "\n Count:  {}\n Sum:    {}\n Min:    {}\n Max:    {}\n Mean:   {}\n Median: {}",
        stats.count,
        stats.sum.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
        stats.min,
        stats.max,
        stats
            .mean
            .map_or("N/A".to_string(), |v| format!("{:.2}", v)),
        stats
            .median
            .map_or("N/A".to_string(), |v| format!("{:.2}", v)),
    );
    let popup = Paragraph::new(content)
        .block(
            Block::default()
                .title(" Column Stats ")
                .title_style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.info)),
        )
        .style(Style::default().bg(theme.bg_alt).fg(theme.fg));
    frame.render_widget(popup, area);
}

fn render_help_popup(frame: &mut Frame, app: &mut App, theme: &Theme) {
    let area = centered_rect(55, 80, frame.area());
    frame.render_widget(Clear, area);
    let text = help_text(theme);
    let total_lines = text.lines.len() as u16;
    let visible_lines = area.height.saturating_sub(2); // subtract top+bottom borders
    app.help_scroll = app
        .help_scroll
        .min(total_lines.saturating_sub(visible_lines));
    let popup = Paragraph::new(text)
        .block(
            Block::default()
                .title(" Help — j/k to scroll · ? or Esc to close ")
                .title_style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.info)),
        )
        .style(Style::default().bg(theme.bg_alt).fg(theme.fg))
        .scroll((app.help_scroll, 0));
    frame.render_widget(popup, area);
}

fn shortcut_bar<'a>(app: &App, theme: &Theme) -> Line<'a> {
    // (primary, secondary) — primary keys are highlighted in blue, secondary in grey.
    // Secondary = always-valid base shortcuts not already shown in primary.
    type Shortcuts = &'static [(&'static str, &'static str)];
    let (primary, secondary): (Shortcuts, Shortcuts) = match app.mode {
        Mode::Normal if app.groupby.active => (
            &[("B", "Clear group-by"), ("s", "Sort"), ("p", "Plot")],
            &[
                ("/", "Search"),
                ("f", "Filter"),
                ("b", "Group-by"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("T", "Theme"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        ),
        Mode::Normal if !app.groupby.keys.is_empty() => (
            &[("b", "Toggle key"), ("a", "Cycle agg"), ("B", "Execute")],
            &[
                ("/", "Search"),
                ("f", "Filter"),
                ("s", "Sort"),
                ("p", "Plot"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("T", "Theme"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        ),
        Mode::Normal if !app.search.results.is_empty() => (
            &[
                ("n", "Next match"),
                ("N", "Prev match"),
                ("/", "New search"),
            ],
            &[
                ("f", "Filter"),
                ("s", "Sort"),
                ("b", "Group-by"),
                ("p", "Plot"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        ),
        Mode::Normal if !app.filter.filters.is_empty() => (
            &[("F", "Clear filters"), ("f", "Add filter")],
            &[
                ("/", "Search"),
                ("s", "Sort"),
                ("S", "Clear sorts"),
                ("b", "Group-by"),
                ("p", "Plot"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        ),
        Mode::Normal if !app.sort.sorts.is_empty() => (
            &[("S", "Clear sorts"), ("s", "Add/cycle sort")],
            &[
                ("/", "Search"),
                ("f", "Filter"),
                ("b", "Group-by"),
                ("p", "Plot"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        ),
        Mode::Normal => (
            &[
                ("/", "Search"),
                ("f", "Filter"),
                ("s", "Sort"),
                ("b", "Group-by"),
                ("p", "Plot"),
                ("i", "Inspector"),
                ("u", "Unique"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
            &[],
        ),
        Mode::Search => (
            &[
                ("Enter", "Jump"),
                ("n / N", "Next / Prev"),
                ("Esc", "Cancel"),
            ],
            &[],
        ),
        Mode::Filter => (&[("Enter", "Confirm"), ("Esc", "Cancel")], &[]),
        Mode::PlotPickY => (
            &[
                ("← →", "Navigate"),
                ("Space", "Toggle Y"),
                ("2", "Toggle P2"),
                ("Enter", "Pick X axis"),
                ("i", "Plot with index"),
                ("Esc", "Cancel"),
            ],
            &[],
        ),
        Mode::PlotPickX => (
            &[("← →", "Navigate"), ("Enter", "Confirm"), ("Esc", "Back")],
            &[],
        ),
        // render_plot() returns early in ui() and renders its own status bar.
        Mode::Plot => unreachable!("shortcut_bar is not called in Plot mode"),
        Mode::ThemePicker => (
            &[("j / k", "Navigate"), ("Enter", "Keep"), ("Esc", "Cancel")],
            &[],
        ),
        Mode::ColumnsView => {
            if app.columns_view.searching {
                (
                    &[
                        ("type", "Search"),
                        ("↑ ↓", "Navigate"),
                        ("Enter", "Jump to column"),
                        ("Esc", "Exit search"),
                    ][..],
                    &[][..],
                )
            } else {
                (
                    &[
                        ("/", "Search"),
                        ("j / k", "Navigate"),
                        ("Enter", "Jump to column"),
                        ("Esc / i", "Close"),
                    ][..],
                    &[][..],
                )
            }
        }
        Mode::UniqueValues => {
            if app.unique_values.searching {
                (
                    &[
                        ("type", "Search"),
                        ("↑ ↓", "Navigate"),
                        ("Enter", "Apply filter"),
                        ("Esc", "Exit search"),
                    ][..],
                    &[][..],
                )
            } else {
                (
                    &[
                        ("/", "Search"),
                        ("j / k", "Navigate"),
                        ("Enter", "Apply filter"),
                        ("Esc", "Close"),
                    ][..],
                    &[][..],
                )
            }
        }
    };

    let primary_key = Style::default()
        .bg(theme.accent)
        .fg(theme.bg)
        .add_modifier(Modifier::BOLD);
    let secondary_key = Style::default()
        .bg(theme.border_idle)
        .fg(theme.bg)
        .add_modifier(Modifier::BOLD);
    let label = Style::default().bg(theme.bg_alt).fg(theme.fg_dim);
    let gap = Style::default().bg(theme.bg_alt);
    let sep = Style::default().bg(theme.bg_alt).fg(theme.border_idle);

    let mut spans = Vec::new();

    for (key, action) in primary {
        spans.push(Span::styled(format!(" {} ", key), primary_key));
        spans.push(Span::styled(format!(" {} ", action), label));
        spans.push(Span::styled("  ", gap));
    }

    if !primary.is_empty() && !secondary.is_empty() {
        spans.push(Span::styled(" │ ", sep));
    }

    for (key, action) in secondary {
        spans.push(Span::styled(format!(" {} ", key), secondary_key));
        spans.push(Span::styled(format!(" {} ", action), label));
        spans.push(Span::styled("  ", gap));
    }

    Line::from(spans).style(Style::default().bg(theme.bg_alt))
}

fn get_bar(app: &App, theme: &Theme) -> (String, Style) {
    match app.mode {
        Mode::PlotPickY => {
            let p1 = if app.plot.y_cols.is_empty() {
                "none".to_string()
            } else {
                app.plot
                    .y_cols
                    .iter()
                    .map(|&i| app.headers[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let p2 = if app.plot.y2_cols.is_empty() {
                "none".to_string()
            } else {
                app.plot
                    .y2_cols
                    .iter()
                    .map(|&i| app.headers[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (
                format!(
                    " P1: [{}]  P2: [{}]  —  Space p1 · 2 p2 · ←/→ navigate · i index · Enter pick X · Esc cancel ",
                    p1, p2
                ),
                Style::default()
                    .bg(theme.info)
                    .fg(theme.bg)
                    .add_modifier(Modifier::BOLD),
            )
        }
        Mode::PlotPickX => {
            let y_names = app
                .plot
                .y_cols
                .iter()
                .map(|&i| app.headers[i].as_str())
                .collect::<Vec<_>>()
                .join(", ");
            (
                format!(
                    " Y: [{}]  —  navigate to X column and press Enter  (Esc to go back) ",
                    y_names
                ),
                Style::default()
                    .bg(theme.info)
                    .fg(theme.bg)
                    .add_modifier(Modifier::BOLD),
            )
        }
        // render_plot() returns early in ui() and renders its own status bar.
        Mode::Plot => unreachable!("get_bar is not called in Plot mode"),
        Mode::ThemePicker => (
            format!(
                " Theme: {}  |  j/k navigate  |  Enter keep  |  Esc cancel ",
                app.theme.name
            ),
            Style::default()
                .bg(theme.accent)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::UniqueValues => (
            {
                let col = app
                    .headers
                    .get(app.unique_values.col)
                    .map_or("", |s| s.as_str());
                if app.unique_values.searching {
                    format!(
                        " Unique values: {}  |  type to search  |  ↑/↓ navigate  |  Enter filter  |  Esc exit search ",
                        col
                    )
                } else {
                    format!(
                        " Unique values: {}  |  / search  |  j/k navigate  |  Enter filter  |  Esc close ",
                        col
                    )
                }
            },
            Style::default()
                .bg(theme.info)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::ColumnsView => (
            if app.columns_view.searching {
                " Column Inspector  |  type to search  |  ↑/↓ navigate  |  Enter jump to column  |  Esc exit search ".to_string()
            } else {
                " Column Inspector  |  / search  |  j/k navigate  |  Enter jump to column  |  Esc close ".to_string()
            },
            Style::default()
                .bg(theme.success)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Search => (
            format!(" /{}_ ", app.search.query),
            Style::default()
                .bg(theme.warn)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Filter => {
            if let Some(ref err) = app.filter.error {
                (
                    format!(" f {}_ — {} ", app.filter.query, err),
                    Style::default()
                        .bg(theme.error)
                        .fg(theme.bg)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                (
                    format!(" f {}_ (>,<,>=,<=,!=,= for numbers) ", app.filter.query),
                    Style::default()
                        .bg(theme.info)
                        .fg(theme.bg)
                        .add_modifier(Modifier::BOLD),
                )
            }
        }
        Mode::Normal => {
            let (text, fg) = if app.groupby.active {
                let key_names = app
                    .groupby
                    .saved_headers
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| app.groupby.keys.contains(i))
                    .map(|(_, h)| h.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut agg_entries: Vec<(usize, &AggFunc)> =
                    app.groupby.aggs.iter().map(|(i, f)| (*i, f)).collect();
                agg_entries.sort_by_key(|(i, _)| *i);
                let agg_summary = agg_entries
                    .iter()
                    .map(|(i, func)| {
                        let sym = match func {
                            AggFunc::Sum => "Σ",
                            AggFunc::Mean => "μ",
                            AggFunc::Count => "#",
                            AggFunc::Min => "↓",
                            AggFunc::Max => "↑",
                        };
                        format!("{}[{}]", app.groupby.saved_headers[*i], sym)
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                (
                    format!(
                        " ◆ GROUPED  By: {} | Agg: {} | {} rows ",
                        key_names,
                        agg_summary,
                        app.view.height()
                    ),
                    theme.warn,
                )
            } else if !app.groupby.keys.is_empty() {
                let key_names = app
                    .groupby
                    .keys
                    .iter()
                    .map(|&i| app.headers[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    format!(" GroupBy: {} | press B to execute ", key_names),
                    theme.warn,
                )
            } else if !app.search.results.is_empty() {
                (
                    format!(
                        " [{}/{}]  {} ",
                        app.search.cursor + 1,
                        app.search.results.len(),
                        app.search.query
                    ),
                    theme.info,
                )
            } else if !app.filter.filters.is_empty() {
                let filter_summary = app
                    .filter
                    .filters
                    .iter()
                    .map(|(col, q)| format!("[{}: {}]", col, q))
                    .collect::<Vec<_>>()
                    .join(" ");
                (
                    format!(
                        " {} | Row {}/{} | Col {}/{} | {} ",
                        filter_summary,
                        app.state
                            .selected()
                            .map_or(0, |i| i.saturating_add(1).min(app.view.height())),
                        app.view.height(),
                        app.state
                            .selected_column()
                            .map_or(0, |i| i.saturating_add(1).min(app.headers.len())),
                        app.headers.len(),
                        app.file_path
                    ),
                    theme.info,
                )
            } else if !app.sort.sorts.is_empty() {
                if let Some(ref err) = app.sort.error {
                    (format!(" Sort error: {} ", err), theme.error)
                } else {
                    let sort_summary = app
                        .sort
                        .sorts
                        .iter()
                        .map(|(col, dir)| {
                            let name = app.headers.get(*col).map_or("?", |h| h.as_str());
                            let arrow = if matches!(dir, SortDirection::Descending) {
                                "▼"
                            } else {
                                "▲"
                            };
                            format!("{}{}", name, arrow)
                        })
                        .collect::<Vec<_>>()
                        .join(" → ");
                    (
                        format!(
                            " Sort: {} | Row {}/{} | Col {}/{} | {} ",
                            sort_summary,
                            app.state
                                .selected()
                                .map_or(0, |i| i.saturating_add(1).min(app.view.height())),
                            app.view.height(),
                            app.state
                                .selected_column()
                                .map_or(0, |i| i.saturating_add(1).min(app.headers.len())),
                            app.headers.len(),
                            app.file_path
                        ),
                        theme.info,
                    )
                }
            } else if let Some(ref err) = app.sort.error {
                (format!(" Sort error: {} ", err), theme.error)
            } else {
                (
                    format!(
                        " Row {}/{} | Col {}/{} | {}  ? help ",
                        app.state
                            .selected()
                            .map_or(0, |i| i.saturating_add(1).min(app.view.height())),
                        app.view.height(),
                        app.state
                            .selected_column()
                            .map_or(0, |i| i.saturating_add(1).min(app.headers.len())),
                        app.headers.len(),
                        app.file_path
                    ),
                    theme.fg_dim,
                )
            };
            (text, Style::default().bg(theme.bg_alt).fg(fg))
        }
    }
}

fn help_text(theme: &Theme) -> Text<'static> {
    let section = |title: &'static str| {
        Line::from(vec![
            Span::raw(" "),
            Span::styled(
                title,
                Style::default().fg(theme.info).add_modifier(Modifier::BOLD),
            ),
        ])
    };
    let key = |k: &'static str, desc: &'static str| {
        Line::from(vec![
            Span::styled(format!("  {:<14}", k), Style::default().fg(theme.accent)),
            Span::styled(desc, Style::default().fg(theme.fg)),
        ])
    };
    Text::from(vec![
        Line::raw(""),
        section("Navigation"),
        key("j / ↓", "Move down"),
        key("k / ↑", "Move up"),
        key("h / ←", "Move left"),
        key("l / →", "Move right"),
        key("g / Home", "First row"),
        key("G / End", "Last row"),
        key("PageDown", "Scroll down 20 rows"),
        key("PageUp", "Scroll up 20 rows"),
        Line::raw(""),
        section("Search"),
        key("/", "Enter search mode"),
        key("Enter", "Jump to first match"),
        key("n / N", "Next / previous match"),
        key("Esc", "Exit search"),
        Line::raw(""),
        section("Filter"),
        key("f", "Enter filter mode (current column)"),
        key("Enter", "Apply filter"),
        key("F", "Clear all filters"),
        key("Esc", "Discard input"),
        key("", "  >, <, >=, <=, !=, = for numeric columns"),
        Line::raw(""),
        section("Sort"),
        key("s", "Add/cycle sort on column  (▲ → ▼ → off)"),
        key("S", "Clear all sorts"),
        Line::raw(""),
        section("Group By"),
        key("b", "Toggle group-by key [K]"),
        key("a", "Cycle aggregation  [Σ μ # ↓ ↑]"),
        key("B", "Execute / clear group-by"),
        Line::raw(""),
        section("Plot"),
        key("p", "Mark column as Y, enter pick-Y mode"),
        key("←/→ h/l", "Navigate columns (pick-Y / pick-X)"),
        key("Space", "Toggle Y column (pick-Y)"),
        key("2", "Toggle column into panel 2 (dual-panel mode)"),
        key("Enter", "pick-Y: advance to pick-X  |  pick-X: show chart"),
        key("i", "Plot against row index (skip pick-X)"),
        key(
            "t",
            "Cycle chart type (line → bar → histogram; line ↔ bar for multi-Y)",
        ),
        key("Esc / p", "Close chart"),
        Line::raw(""),
        section("Theme"),
        key("T", "Open theme picker"),
        key("j / k", "Navigate themes (live preview)"),
        key("Enter", "Keep selected theme (saved to disk)"),
        key("Esc", "Cancel — restore previous theme"),
        Line::raw(""),
        section("Other"),
        key("u", "Unique values popup (searchable, Enter to filter)"),
        key("i", "Column Inspector (schema + stats)"),
        key("_", "Autofit column width"),
        key("=", "Autofit all columns"),
        key("e", "Toggle column stats popup"),
        key("?", "Toggle this help"),
        key("q", "Quit"),
        Line::raw(""),
    ])
}

fn render_unique_values_popup(frame: &mut Frame, app: &mut App, theme: &Theme) {
    let area = centered_rect(52, 70, frame.area());
    frame.render_widget(Clear, area);

    let col_name = app
        .headers
        .get(app.unique_values.col)
        .map_or("", |s| s.as_str());
    let truncated_note = if app.unique_values.truncated {
        " [top 500]"
    } else {
        ""
    };
    let title = format!(
        " Unique: {} ({} shown{}) ",
        col_name,
        app.unique_values.filtered.len(),
        truncated_note
    );

    let outer = Block::default()
        .title(title)
        .title_style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.info))
        .style(Style::default().bg(theme.bg));

    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let zones = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    // Search field: show cursor when actively searching, otherwise hint
    let (search_text, search_style) = if app.unique_values.searching {
        (
            format!(" Search: {}_ ", app.unique_values.query),
            Style::default().bg(theme.bg_alt).fg(theme.fg),
        )
    } else if app.unique_values.query.is_empty() {
        (
            " press / to search ".to_string(),
            Style::default().bg(theme.bg_alt).fg(theme.fg_dim),
        )
    } else {
        (
            format!(" Search: {} (press / to edit) ", app.unique_values.query),
            Style::default().bg(theme.bg_alt).fg(theme.fg_dim),
        )
    };
    frame.render_widget(Paragraph::new(search_text).style(search_style), zones[0]);

    // Values table
    let header = Row::new([
        Cell::from("Value").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Count").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
    ])
    .style(Style::default().bg(theme.bg_alt))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .unique_values
        .filtered
        .iter()
        .enumerate()
        .map(|(i, (val, count))| {
            let bg = if i % 2 == 0 { theme.bg } else { theme.bg_alt };
            Row::new([
                Cell::from(val.clone()).style(Style::default().fg(theme.fg)),
                Cell::from(count.to_string()).style(Style::default().fg(theme.fg_dim)),
            ])
            .style(Style::default().bg(bg))
        })
        .collect();

    let table = Table::new(rows, [Constraint::Min(10), Constraint::Length(8)])
        .header(header)
        .row_highlight_style(
            Style::default()
                .bg(theme.info)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        );

    frame.render_stateful_widget(table, zones[1], &mut app.unique_values.state);
}

fn render_columns_view(frame: &mut Frame, app: &mut App, theme: &Theme, full_area: Rect) {
    frame.render_widget(Clear, full_area);

    let chunks = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(full_area);

    let (search_text, search_style) = if app.columns_view.searching {
        (
            format!(" Search: {}_ ", app.columns_view.query),
            Style::default().bg(theme.bg_alt).fg(theme.fg),
        )
    } else if app.columns_view.query.is_empty() {
        (
            " press / to search ".to_string(),
            Style::default().bg(theme.bg_alt).fg(theme.fg_dim),
        )
    } else {
        (
            format!(" Search: {} (press / to edit) ", app.columns_view.query),
            Style::default().bg(theme.bg_alt).fg(theme.fg_dim),
        )
    };
    frame.render_widget(Paragraph::new(search_text).style(search_style), chunks[0]);

    let (bar_text, bar_style) = get_bar(app, theme);
    frame.render_widget(Paragraph::new(bar_text).style(bar_style), chunks[2]);

    let header = Row::new([
        Cell::from("Column").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Type").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Count").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Nulls").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Unique").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Sum").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Min").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Max").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Mean").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
        Cell::from("Median").style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD)),
    ])
    .style(Style::default().bg(theme.bg_alt))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .columns_view
        .filtered
        .iter()
        .enumerate()
        .filter_map(|(i, &idx)| {
            app.columns_view
                .profile
                .get(idx)
                .map(|p| profile_row(p, i, theme))
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(12),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(9),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Length(10),
    ];

    let title = format!(
        " Column Inspector — {} ({}/{} matched) ",
        app.file_path,
        app.columns_view.filtered.len(),
        app.columns_view.profile.len()
    );

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(title)
                .title_style(
                    Style::default()
                        .fg(theme.success)
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.border_idle))
                .style(Style::default().bg(theme.bg)),
        )
        .row_highlight_style(
            Style::default()
                .bg(theme.success)
                .fg(theme.bg)
                .add_modifier(Modifier::BOLD),
        );

    frame.render_stateful_widget(table, chunks[1], &mut app.columns_view.state);
}

fn profile_row<'a>(p: &'a ColumnProfile, idx: usize, theme: &Theme) -> Row<'a> {
    let bg = if idx % 2 == 0 { theme.bg } else { theme.bg_alt };
    let null_style = if p.null_count > 0 {
        Style::default().fg(theme.error)
    } else {
        Style::default().fg(theme.fg)
    };
    Row::new([
        Cell::from(p.name.clone()).style(Style::default().fg(theme.fg)),
        Cell::from(p.dtype.clone()).style(Style::default().fg(theme.fg_dim)),
        Cell::from(p.count.to_string()).style(Style::default().fg(theme.fg)),
        Cell::from(p.null_count.to_string()).style(null_style),
        Cell::from(p.unique.to_string()).style(Style::default().fg(theme.fg)),
        Cell::from(p.sum.map_or("—".to_string(), |v| format!("{:.2}", v)))
            .style(Style::default().fg(theme.accent)),
        Cell::from(p.min.clone()).style(Style::default().fg(theme.fg_dim)),
        Cell::from(p.max.clone()).style(Style::default().fg(theme.fg_dim)),
        Cell::from(p.mean.map_or("—".to_string(), |v| format!("{:.2}", v)))
            .style(Style::default().fg(theme.accent)),
        Cell::from(p.median.map_or("—".to_string(), |v| format!("{:.2}", v)))
            .style(Style::default().fg(theme.accent)),
    ])
    .style(Style::default().bg(bg))
}

fn series_color(idx: usize, theme: &Theme) -> Color {
    theme.series[idx % theme.series.len()]
}

fn downsample(data: Vec<(f64, f64)>, max_points: usize) -> Vec<(f64, f64)> {
    if data.len() <= max_points {
        return data;
    }
    let step = data.len() as f64 / max_points as f64;
    (0..max_points)
        .map(|i| data[(i as f64 * step) as usize])
        .collect()
}

fn compute_histogram(app: &App, y_idx: usize) -> Result<Vec<(f64, f64)>, String> {
    let col = app
        .view
        .column(&app.headers[y_idx])
        .map_err(|e| format!("Column error: {}", e))?;
    let y_f64 = series_to_f64(col).ok_or_else(|| {
        format!(
            "'{}' is not a numeric column (int or float required)",
            app.headers[y_idx]
        )
    })?;
    let values: Vec<f64> = y_f64
        .f64()
        .map(|ca| ca.into_iter().flatten().collect())
        .unwrap_or_default();
    if values.is_empty() {
        return Err(format!(
            "'{}' contains no non-null numeric values",
            app.headers[y_idx]
        ));
    }
    let n = values.len();
    // Sturges' rule, clamped to a sensible range.
    let n_bins = ((n as f64).log2().ceil() as usize + 1).clamp(5, 50);
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if (max - min).abs() < f64::EPSILON {
        return Ok(vec![(min, n as f64)]);
    }
    let bin_w = (max - min) / n_bins as f64;
    let mut counts = vec![0u64; n_bins];
    for v in &values {
        let bin = ((v - min) / bin_w) as usize;
        counts[bin.min(n_bins - 1)] += 1;
    }
    Ok(counts
        .iter()
        .enumerate()
        .map(|(i, &c)| (min + (i as f64 + 0.5) * bin_w, c as f64))
        .collect())
}

fn render_histogram(frame: &mut Frame, app: &App, theme: &Theme, y_idx: usize, full_area: Rect) {
    let zones = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(full_area);
    let chart_area = zones[0];
    let bar_area = zones[1];

    let bar_text =
        " Histogram chart  |  t cycle line/bar/histogram  |  Esc / p to close ".to_string();
    frame.render_widget(
        Paragraph::new(bar_text).style(Style::default().bg(theme.bg_alt).fg(theme.fg_dim)),
        bar_area,
    );

    let data = match compute_histogram(app, y_idx) {
        Ok(d) => d,
        Err(msg) => {
            let paragraph = Paragraph::new(format!(" {} ", msg))
                .block(
                    Block::default()
                        .title(" Plot Error ")
                        .title_style(
                            Style::default()
                                .fg(theme.error)
                                .add_modifier(Modifier::BOLD),
                        )
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(Style::default().fg(theme.error)),
                )
                .style(Style::default().bg(theme.bg).fg(theme.fg));
            frame.render_widget(paragraph, chart_area);
            return;
        }
    };

    let x_min = data.first().map(|p| p.0).unwrap_or(0.0);
    let x_max = data.last().map(|p| p.0).unwrap_or(1.0);
    let y_max = data.iter().map(|p| p.1).fold(0.0f64, f64::max);
    let y_pad = y_max * config::Y_AXIS_PADDING;

    // Three evenly-spaced X labels showing the data range.
    let x_mid = (x_min + x_max) / 2.0;
    let x_labels = vec![
        ratatui::text::Span::raw(format!("{:.2}", x_min)),
        ratatui::text::Span::raw(format!("{:.2}", x_mid)),
        ratatui::text::Span::raw(format!("{:.2}", x_max)),
    ];

    let dataset = Dataset::default()
        .name(app.headers[y_idx].as_str())
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Bar)
        .style(Style::default().fg(theme.info))
        .data(&data);

    let chart = Chart::new(vec![dataset])
        .block(
            Block::default()
                .title(format!(" Distribution of {} ", app.headers[y_idx]))
                .title_style(Style::default().fg(theme.info).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.border_idle))
                .style(Style::default().bg(theme.bg)),
        )
        .x_axis(
            Axis::default()
                .title(app.headers[y_idx].as_str())
                .style(Style::default().fg(theme.fg_dim))
                .labels(x_labels)
                .bounds([x_min, x_max]),
        )
        .y_axis(
            Axis::default()
                .title("Count")
                .style(Style::default().fg(theme.fg_dim))
                .labels(numeric_axis_labels(
                    0.0,
                    y_max + y_pad,
                    config::Y_AXIS_TICKS,
                ))
                .bounds([0.0, y_max + y_pad]),
        );

    frame.render_widget(chart, chart_area);
}

fn compute_x_bounds(app: &App, y_cols_all: &[usize], max_points: usize) -> [f64; 2] {
    let x_min = y_cols_all
        .iter()
        .flat_map(|&y_idx| {
            let (raw, _) = match app.plot.x_col {
                Some(x_idx) => extract_plot_data(app, x_idx, y_idx),
                None => (extract_plot_data_indexed(app, y_idx), false),
            };
            downsample(raw, max_points).into_iter().map(|p| p.0)
        })
        .fold(f64::INFINITY, f64::min);
    let x_max = y_cols_all
        .iter()
        .flat_map(|&y_idx| {
            let (raw, _) = match app.plot.x_col {
                Some(x_idx) => extract_plot_data(app, x_idx, y_idx),
                None => (extract_plot_data_indexed(app, y_idx), false),
            };
            downsample(raw, max_points).into_iter().map(|p| p.0)
        })
        .fold(f64::NEG_INFINITY, f64::max);
    if x_min == f64::INFINITY || x_max == f64::NEG_INFINITY {
        [0.0, 1.0]
    } else {
        [x_min, x_max]
    }
}

fn render_plot(frame: &mut Frame, app: &App, theme: &Theme, full_area: Rect) {
    frame.render_widget(Clear, full_area);

    if app.plot.y_cols.is_empty() {
        return;
    }

    let dual = !app.plot.y2_cols.is_empty();

    // Histogram only in single-panel, single-column mode.
    if matches!(app.plot.plot_type, PlotType::Histogram) && !dual {
        render_histogram(frame, app, theme, app.plot.y_cols[0], full_area);
        return;
    }

    let max_points = (full_area.width as usize * 2).max(200);

    // Rotated categorical X labels are only shown in single-panel mode.
    let (x_labels, label_height) = if !dual {
        let all_series: Vec<(Vec<(f64, f64)>, bool)> = app
            .plot
            .y_cols
            .iter()
            .map(|&y_idx| {
                let (raw, cat) = match app.plot.x_col {
                    Some(x_idx) => extract_plot_data(app, x_idx, y_idx),
                    None => (extract_plot_data_indexed(app, y_idx), false),
                };
                (downsample(raw, max_points), cat)
            })
            .collect();
        let x_is_categorical = all_series.iter().any(|(_, cat)| *cat);
        let first_len = all_series
            .iter()
            .find(|(d, _)| !d.is_empty())
            .map(|(d, _)| d.len())
            .unwrap_or(0);
        let labels = match (x_is_categorical, app.plot.x_col) {
            (true, Some(x_idx)) => collect_all_x_labels(app, x_idx, first_len),
            _ => vec![],
        };
        let max_label_len = labels.iter().map(|s| s.chars().count()).max().unwrap_or(0);
        let lh = (max_label_len as u16).min(full_area.height / 3);
        (labels, lh)
    } else {
        (vec![], 0)
    };

    let zones = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(label_height),
            Constraint::Length(1),
        ])
        .split(full_area);
    let chart_area = zones[0];
    let label_area = zones[1];
    let bar_area = zones[2];

    let cycle_hint = if app.plot.y_cols.len() > 1 || dual {
        "t cycle line/bar"
    } else {
        "t cycle line/bar/histogram"
    };
    let type_label = if dual && matches!(app.plot.plot_type, PlotType::Histogram) {
        "Bar"
    } else {
        app.plot_type_label()
    };
    frame.render_widget(
        Paragraph::new(format!(
            " {} chart  |  {}  |  Esc / p to close ",
            type_label, cycle_hint
        ))
        .style(Style::default().bg(theme.bg_alt).fg(theme.fg_dim)),
        bar_area,
    );

    if dual {
        let all_y_cols: Vec<usize> = app
            .plot
            .y_cols
            .iter()
            .chain(app.plot.y2_cols.iter())
            .copied()
            .collect();
        let x_bounds = compute_x_bounds(app, &all_y_cols, max_points);
        let panels = Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(chart_area);
        render_plot_panel(
            frame,
            app,
            theme,
            &app.plot.y_cols,
            panels[0],
            max_points,
            x_bounds,
        );
        render_plot_panel(
            frame,
            app,
            theme,
            &app.plot.y2_cols,
            panels[1],
            max_points,
            x_bounds,
        );
    } else {
        let x_bounds = compute_x_bounds(app, &app.plot.y_cols, max_points);
        render_plot_panel(
            frame,
            app,
            theme,
            &app.plot.y_cols,
            chart_area,
            max_points,
            x_bounds,
        );
        if !x_labels.is_empty() && label_area.height > 0 {
            // Compute y_label_width for rotated-label alignment (single-panel only).
            let all_series: Vec<(Vec<(f64, f64)>, bool)> = app
                .plot
                .y_cols
                .iter()
                .map(|&y_idx| {
                    let (raw, _cat) = match app.plot.x_col {
                        Some(x_idx) => extract_plot_data(app, x_idx, y_idx),
                        None => (extract_plot_data_indexed(app, y_idx), false),
                    };
                    (downsample(raw, max_points), false)
                })
                .collect();
            let nonempty: Vec<(usize, &Vec<(f64, f64)>)> = all_series
                .iter()
                .enumerate()
                .filter(|(_, (d, _))| !d.is_empty())
                .map(|(i, (d, _))| (i, d))
                .collect();
            let first_len = nonempty.first().map(|(_, d)| d.len()).unwrap_or(0);
            let y_min = nonempty
                .iter()
                .flat_map(|(_, d)| d.iter().map(|p| p.1))
                .fold(f64::INFINITY, f64::min);
            let y_max = nonempty
                .iter()
                .flat_map(|(_, d)| d.iter().map(|p| p.1))
                .fold(f64::NEG_INFINITY, f64::max);
            let y_pad = (y_max - y_min).abs() * config::Y_AXIS_PADDING;
            let y_bounds = [y_min - y_pad, y_max + y_pad];
            let y_labels = numeric_axis_labels(y_bounds[0], y_bounds[1], config::Y_AXIS_TICKS);
            let y_label_width = max_label_width(&y_labels);
            render_vertical_x_labels(
                frame,
                &x_labels,
                first_len,
                chart_area,
                label_area,
                y_label_width,
                theme.fg_dim,
            );
        }
    }
}

fn render_plot_panel(
    frame: &mut Frame,
    app: &App,
    theme: &Theme,
    y_cols: &[usize],
    area: Rect,
    max_points: usize,
    x_bounds: [f64; 2],
) {
    let all_series: Vec<(Vec<(f64, f64)>, bool)> = y_cols
        .iter()
        .map(|&y_idx| {
            let (raw, cat) = match app.plot.x_col {
                Some(x_idx) => extract_plot_data(app, x_idx, y_idx),
                None => (extract_plot_data_indexed(app, y_idx), false),
            };
            (downsample(raw, max_points), cat)
        })
        .collect();

    let nonempty: Vec<(usize, &Vec<(f64, f64)>)> = all_series
        .iter()
        .enumerate()
        .filter(|(_, (d, _))| !d.is_empty())
        .map(|(i, (d, _))| (i, d))
        .collect();

    if nonempty.is_empty() {
        let msg = Paragraph::new(" No data to plot. Y columns must be numeric (int or float). ")
            .block(
                Block::default()
                    .title(" Plot Error ")
                    .title_style(
                        Style::default()
                            .fg(theme.error)
                            .add_modifier(Modifier::BOLD),
                    )
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(Style::default().fg(theme.error)),
            )
            .style(Style::default().bg(theme.bg).fg(theme.fg));
        frame.render_widget(msg, area);
        return;
    }

    let y_min = nonempty
        .iter()
        .flat_map(|(_, d)| d.iter().map(|p| p.1))
        .fold(f64::INFINITY, f64::min);
    let y_max = nonempty
        .iter()
        .flat_map(|(_, d)| d.iter().map(|p| p.1))
        .fold(f64::NEG_INFINITY, f64::max);

    let y_pad = (y_max - y_min).abs() * config::Y_AXIS_PADDING;
    let y_bounds = [y_min - y_pad, y_max + y_pad];

    let graph_type = match app.plot.plot_type {
        PlotType::Line => GraphType::Line,
        _ => GraphType::Bar,
    };

    let datasets: Vec<Dataset<'_>> = nonempty
        .iter()
        .map(|(series_idx, data)| {
            Dataset::default()
                .marker(symbols::Marker::Braille)
                .graph_type(graph_type)
                .style(Style::default().fg(series_color(*series_idx, theme)))
                .data(data)
        })
        .collect();

    let title_y = y_cols
        .iter()
        .map(|&i| app.headers[i].as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let y_labels = numeric_axis_labels(y_bounds[0], y_bounds[1], config::Y_AXIS_TICKS);

    let x_header: &str = app
        .plot
        .x_col
        .map(|i| app.headers[i].as_str())
        .unwrap_or("row index");

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(format!(" {} vs {} ", title_y, x_header))
                .title_style(
                    Style::default()
                        .fg(series_color(0, theme))
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme.border_idle))
                .style(Style::default().bg(theme.bg)),
        )
        .x_axis(
            Axis::default()
                .title(x_header)
                .style(Style::default().fg(theme.fg_dim))
                .bounds(x_bounds),
        )
        .y_axis(
            Axis::default()
                .title(if y_cols.len() == 1 {
                    app.headers[y_cols[0]].as_str()
                } else {
                    "Value"
                })
                .style(Style::default().fg(theme.fg_dim))
                .labels(y_labels)
                .bounds(y_bounds),
        );

    frame.render_widget(chart, area);

    if y_cols.len() > 1 {
        render_plot_legend(frame, app, theme, y_cols, area);
    }
}

fn render_plot_legend(
    frame: &mut Frame,
    app: &App,
    theme: &Theme,
    y_cols: &[usize],
    chart_area: Rect,
) {
    let legend_inner_w = y_cols
        .iter()
        .map(|&i| app.headers[i].chars().count() + 3)
        .max()
        .unwrap_or(4) as u16;
    let legend_w = legend_inner_w + 2;
    let legend_h = y_cols.len() as u16 + 2;

    let legend_x = chart_area
        .x
        .saturating_add(chart_area.width)
        .saturating_sub(legend_w)
        .saturating_sub(1);
    let legend_y = chart_area.y + 1;

    if legend_w > chart_area.width || legend_h > chart_area.height {
        return;
    }

    let legend_area = Rect {
        x: legend_x,
        y: legend_y,
        width: legend_w,
        height: legend_h,
    };

    let lines: Vec<Line<'_>> = y_cols
        .iter()
        .enumerate()
        .map(|(i, &y_idx)| {
            Line::from(vec![
                Span::styled("● ", Style::default().fg(series_color(i, theme))),
                Span::styled(app.headers[y_idx].as_str(), Style::default().fg(theme.fg)),
            ])
        })
        .collect();

    let legend = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(theme.border_idle))
            .style(Style::default().bg(theme.bg)),
    );
    frame.render_widget(legend, legend_area);
}

/// Format a numeric axis tick. `range` is the span of the axis; it drives
/// decimal precision so labels stay readable across very different magnitudes.
/// Values >= 1e6 use an "M" suffix, >= 1e3 use "k"; otherwise decimals scale
/// with the range so small spans get more precision.
fn format_axis_tick(value: f64, range: f64) -> String {
    if !value.is_finite() {
        return String::from("–");
    }
    let abs = value.abs();
    if abs >= 1e6 {
        return format!("{:.1}M", value / 1e6);
    }
    if abs >= 1e3 {
        return format!("{:.1}k", value / 1e3);
    }
    if range >= 100.0 {
        format!("{:.0}", value)
    } else if range >= 1.0 {
        format!("{:.2}", value)
    } else {
        format!("{:.3}", value)
    }
}

/// Build `ticks` evenly spaced axis labels across `[min, max]`. Ratatui's
/// `Axis::labels` distributes them along the axis, so the first lands at `min`
/// and the last at `max`.
fn numeric_axis_labels(min: f64, max: f64, ticks: usize) -> Vec<Span<'static>> {
    let ticks = ticks.max(2);
    let range = (max - min).abs();
    (0..ticks)
        .map(|i| {
            let t = i as f64 / (ticks - 1) as f64;
            let v = min + (max - min) * t;
            Span::raw(format_axis_tick(v, range))
        })
        .collect()
}

/// Widest label width in cells — used to mirror ratatui's internal Y-label
/// gutter so overlays (like rotated X labels) can align with the plot area.
fn max_label_width(labels: &[Span<'_>]) -> u16 {
    labels
        .iter()
        .map(|s| s.content.chars().count() as u16)
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
pub fn extract_plot_data_pub(app: &App, x_idx: usize, y_idx: usize) -> (Vec<(f64, f64)>, bool) {
    extract_plot_data(app, x_idx, y_idx)
}

#[cfg(test)]
pub fn compute_histogram_pub(app: &App, y_idx: usize) -> Result<Vec<(f64, f64)>, String> {
    compute_histogram(app, y_idx)
}

fn series_to_f64(col: &polars::prelude::Column) -> Option<polars::prelude::Series> {
    let s = col.as_materialized_series();
    if s.dtype().is_primitive_numeric() || matches!(s.dtype(), DataType::Decimal(_, _)) {
        s.cast(&DataType::Float64).ok()
    } else {
        None
    }
}

/// Collect all string representations of an X column (for categorical axes).
fn collect_all_x_labels(app: &App, x_idx: usize, n_points: usize) -> Vec<String> {
    if n_points == 0 {
        return vec![];
    }
    let col = match app.view.column(&app.headers[x_idx]) {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    let s = col.as_materialized_series();
    let str_series = match s.cast(&DataType::String) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let str_ca = match str_series.str() {
        Ok(ca) => ca,
        Err(_) => return vec![],
    };
    (0..n_points)
        .map(|i| str_ca.get(i).unwrap_or("").to_string())
        .collect()
}

/// Render x-axis labels rotated 90° into `label_area` (one char per row).
/// Samples down to `plot_width` labels if there are more than that many columns.
///
/// `y_label_gutter` is the width ratatui reserves on the left of the inner
/// chart area for the Y-axis labels. We mirror its layout here so each rotated
/// label column lines up with its data point.
fn render_vertical_x_labels(
    frame: &mut Frame,
    labels: &[String],
    n_data_points: usize,
    chart_area: Rect,
    label_area: Rect,
    y_label_gutter: u16,
    color: Color,
) {
    if labels.is_empty() || n_data_points == 0 || label_area.height == 0 {
        return;
    }

    // Ratatui's chart reserves: 1 border + y_label_gutter + 1 axis column on the
    // left (the +1 axis only when y-axis labels are present, which they always
    // are since we added the numeric scale).
    let left_reserved =
        config::CHART_BORDER_WIDTH + y_label_gutter + if y_label_gutter > 0 { 1 } else { 0 };
    let plot_x = chart_area.x + left_reserved;
    let plot_w = chart_area
        .width
        .saturating_sub(left_reserved + config::CHART_BORDER_WIDTH);
    if plot_w == 0 {
        return;
    }

    // Show all labels if they fit (one column each); otherwise sample evenly.
    let n_slots = plot_w as usize;
    let display: Vec<&str> = if labels.len() <= n_slots {
        labels.iter().map(|s| s.as_str()).collect()
    } else {
        let n = n_slots;
        (0..n)
            .map(|i| {
                let idx = if n <= 1 {
                    0
                } else {
                    i * (labels.len() - 1) / (n - 1)
                };
                labels[idx].as_str()
            })
            .collect()
    };

    let n = display.len();
    if n == 0 {
        return;
    }

    let style = Style::default().fg(color);
    let buf = frame.buffer_mut();

    for (i, label) in display.iter().enumerate() {
        let col_x = if n == 1 {
            plot_x
        } else {
            plot_x + (i as u16) * (plot_w - 1) / (n as u16 - 1)
        };
        if col_x >= chart_area.x + chart_area.width {
            continue;
        }
        for (row, ch) in label.chars().enumerate() {
            let cell_y = label_area.y + row as u16;
            if cell_y >= label_area.y + label_area.height {
                break;
            }
            if let Some(cell) = buf.cell_mut(Position::new(col_x, cell_y)) {
                cell.set_char(ch);
                cell.set_style(style);
            }
        }
    }
}

/// Extract (row_index, y_value) pairs for plotting against the implicit row
/// index. Null Y values are skipped, so gaps in the data appear as gaps in the
/// plot rather than being drawn as 0. Returns an empty vec if the column is
/// non-numeric.
fn extract_plot_data_indexed(app: &App, y_idx: usize) -> Vec<(f64, f64)> {
    let Some(ys) = app
        .view
        .column(&app.headers[y_idx])
        .ok()
        .and_then(series_to_f64)
    else {
        return vec![];
    };
    let yca = ys.f64().unwrap();
    yca.into_iter()
        .enumerate()
        .filter_map(|(i, y)| Some((i as f64, y?)))
        .collect()
}

fn extract_plot_data(app: &App, x_idx: usize, y_idx: usize) -> (Vec<(f64, f64)>, bool) {
    let x_series = app
        .view
        .column(&app.headers[x_idx])
        .ok()
        .and_then(series_to_f64);
    let y_series = app
        .view
        .column(&app.headers[y_idx])
        .ok()
        .and_then(series_to_f64);

    match (x_series, y_series) {
        (Some(xs), Some(ys)) => {
            let xca = xs.f64().unwrap();
            let yca = ys.f64().unwrap();
            let points = xca
                .into_iter()
                .zip(yca)
                .filter_map(|(x, y)| Some((x?, y?)))
                .collect();
            (points, false)
        }
        (None, Some(ys)) => {
            let yca = ys.f64().unwrap();
            let points: Vec<(f64, f64)> = yca
                .into_iter()
                .enumerate()
                .filter_map(|(i, y)| Some((i as f64, y?)))
                .collect();
            (points, true)
        }
        _ => (vec![], false),
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area);

    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical[1])[1]
}

#[cfg(test)]
mod histogram_tests {
    use super::*;
    use crate::app::App;
    use polars::prelude::*;

    fn make_numeric_app() -> App {
        let df = df! {
            "val" => [1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        }
        .unwrap();
        App::new(df, "test.csv".to_string(), crate::theme::default_theme())
    }

    #[test]
    fn test_compute_histogram_numeric_returns_ok() {
        let app = make_numeric_app();
        let result = compute_histogram_pub(&app, 0);
        assert!(result.is_ok());
        let data = result.unwrap();
        assert!(!data.is_empty());
        // All counts must be non-negative
        assert!(data.iter().all(|(_, count)| *count >= 0.0));
        // Bin centres must be within the data range
        assert!(data.iter().all(|(x, _)| *x >= 1.0 && *x <= 10.0));
    }

    #[test]
    fn test_compute_histogram_non_numeric_returns_err() {
        let df = df! {
            "name" => ["alice", "bob", "charlie"],
        }
        .unwrap();
        let app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        let result = compute_histogram_pub(&app, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("numeric"));
    }

    #[test]
    fn test_compute_histogram_single_unique_value() {
        // All values identical — bin_w would be ~0, special-cased to one bar
        let df = df! {
            "val" => [5.0f64, 5.0, 5.0],
        }
        .unwrap();
        let app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        let result = compute_histogram_pub(&app, 0);
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], (5.0, 3.0));
    }

    #[test]
    fn test_compute_histogram_total_count_equals_row_count() {
        let app = make_numeric_app();
        let data = compute_histogram_pub(&app, 0).unwrap();
        let total: f64 = data.iter().map(|(_, c)| c).sum();
        assert_eq!(total as usize, 10);
    }

    #[test]
    fn test_compute_histogram_decimal_returns_ok() {
        let s = Series::new("price".into(), &[1.5f64, 2.5, 3.5])
            .cast(&DataType::Decimal(Some(10), Some(2)))
            .unwrap();
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let app = App::new(
            df,
            "test.parquet".to_string(),
            crate::theme::default_theme(),
        );
        let result = compute_histogram_pub(&app, 0);
        assert!(
            result.is_ok(),
            "Decimal column should be plottable: {:?}",
            result
        );
    }
}

#[cfg(test)]
mod axis_label_tests {
    use super::*;

    #[test]
    fn test_format_axis_tick_small_range() {
        assert_eq!(format_axis_tick(0.5, 1.0), "0.50");
        assert_eq!(format_axis_tick(0.001, 0.01), "0.001");
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_format_axis_tick_integer_range() {
        assert_eq!(format_axis_tick(42.0, 500.0), "42");
        assert_eq!(format_axis_tick(3.14, 50.0), "3.14");
    }

    #[test]
    fn test_format_axis_tick_k_and_m_suffix() {
        assert_eq!(format_axis_tick(1_500.0, 10_000.0), "1.5k");
        assert_eq!(format_axis_tick(2_500_000.0, 5_000_000.0), "2.5M");
        assert_eq!(format_axis_tick(-1_500.0, 10_000.0), "-1.5k");
    }

    #[test]
    fn test_format_axis_tick_non_finite() {
        assert_eq!(format_axis_tick(f64::NAN, 1.0), "–");
        assert_eq!(format_axis_tick(f64::INFINITY, 1.0), "–");
    }

    #[test]
    fn test_numeric_axis_labels_endpoints_and_count() {
        let labels = numeric_axis_labels(0.0, 100.0, 5);
        assert_eq!(labels.len(), 5);
        assert_eq!(labels[0].content, "0");
        assert_eq!(labels[4].content, "100");
        // Midpoint must be 50 (range 100 → integer format).
        assert_eq!(labels[2].content, "50");
    }

    #[test]
    fn test_numeric_axis_labels_minimum_tick_count() {
        // Passing 0 or 1 should clamp to 2 (min + max only).
        let labels = numeric_axis_labels(0.0, 10.0, 1);
        assert_eq!(labels.len(), 2);
    }
}

#[cfg(test)]
mod indexed_plot_tests {
    use super::*;
    use crate::app::App;
    use polars::prelude::*;

    #[test]
    fn test_extract_plot_data_indexed_numeric() {
        let df = df! {
            "val" => [10.0f64, 20.0, 30.0],
        }
        .unwrap();
        let app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        let data = extract_plot_data_indexed(&app, 0);
        assert_eq!(data, vec![(0.0, 10.0), (1.0, 20.0), (2.0, 30.0)]);
    }

    #[test]
    fn test_extract_plot_data_indexed_skips_nulls() {
        // Nulls must be dropped but row indices must reflect the ORIGINAL
        // position so gaps are visually preserved.
        let s = Series::new("val".into(), &[Some(10.0f64), None, Some(30.0)]);
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        let data = extract_plot_data_indexed(&app, 0);
        assert_eq!(data, vec![(0.0, 10.0), (2.0, 30.0)]);
    }

    #[test]
    fn test_extract_plot_data_indexed_non_numeric_returns_empty() {
        let df = df! {
            "name" => ["alice", "bob"],
        }
        .unwrap();
        let app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());
        assert!(extract_plot_data_indexed(&app, 0).is_empty());
    }
}

#[cfg(test)]
mod count_visible_tests {
    use super::*;

    #[test]
    fn test_all_columns_fit() {
        // Three 10-wide columns, 32 available: 10 + 11 + 11 = 32 — all fit.
        let widths = vec![10u16, 10, 10];
        assert_eq!(count_visible_from(&widths, 0, 32), 3);
    }

    #[test]
    fn test_only_first_fits() {
        // First column (20) fits; second would need 21 more (20+1 spacing).
        let widths = vec![20u16, 20, 20];
        assert_eq!(count_visible_from(&widths, 0, 20), 1);
    }

    #[test]
    fn test_offset_skips_leading_columns() {
        // Start at index 1; widths[1]=5, widths[2]=5 → 5+6=11 fit in 12.
        let widths = vec![100u16, 5, 5];
        assert_eq!(count_visible_from(&widths, 1, 12), 2);
    }

    #[test]
    fn test_returns_at_least_one_even_when_column_wider_than_available() {
        let widths = vec![100u16];
        assert_eq!(count_visible_from(&widths, 0, 5), 1);
    }

    #[test]
    fn test_empty_widths_returns_one() {
        let widths: Vec<u16> = vec![];
        // No columns to show; count.max(1) should still return 1.
        assert_eq!(count_visible_from(&widths, 0, 80), 1);
    }

    #[test]
    fn test_start_beyond_end_returns_one() {
        let widths = vec![10u16, 10];
        // start=5 is past the end of widths; loop never runs → count=0 → max(1).
        assert_eq!(count_visible_from(&widths, 5, 80), 1);
    }

    #[test]
    fn test_exactly_two_fit() {
        // widths[0]=10, widths[1]=10 → need 10+11=21. available=21 → 2 fit; widths[2] needs 11 more → 32 > 21.
        let widths = vec![10u16, 10, 10];
        assert_eq!(count_visible_from(&widths, 0, 21), 2);
    }
}

#[cfg(test)]
mod null_render_tests {
    use super::*;
    use crate::app::App;
    use polars::prelude::*;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    #[test]
    fn test_null_cell_renders_glyph_with_muted_fg() {
        // One column, three rows: a value, an empty string, a real null.
        let s = Series::new("col".into(), &[Some("alice"), Some(""), None]);
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let mut app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());

        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| ui(frame, &mut app, frame.area()))
            .unwrap();

        let buffer = terminal.backend().buffer();
        let expected_fg = crate::theme::default_theme().fg_muted;

        let area = buffer.area;
        let mut null_cells = 0;
        for y in 0..area.height {
            for x in 0..area.width {
                let cell = buffer.cell(Position::new(x, y)).unwrap();
                if cell.symbol() == NULL_GLYPH {
                    null_cells += 1;
                    assert_eq!(
                        cell.fg, expected_fg,
                        "null glyph at ({x},{y}) should use the muted foreground"
                    );
                }
            }
        }
        // Exactly one null in the data → exactly one ∅ glyph in the buffer.
        assert_eq!(null_cells, 1, "expected one ∅ in the rendered buffer");
    }

    /// Regression: polars backs any length-1 column with a `Scalar` rather than a
    /// `Series`, and `Column::as_series()` is a downcast that returns `None` for it.
    /// The renderer used to treat that `None` as "no data" and paint the whole row
    /// with the null glyph, so a single-row CSV displayed as ∅ in every cell.
    #[test]
    fn test_single_row_frame_renders_values_not_null_glyphs() {
        let a = Series::new("a".into(), &["hello"]);
        let b = Series::new("b".into(), &[1i64]);
        let df = DataFrame::new(vec![a.into(), b.into()]).unwrap();
        let mut app = App::new(df, "one.csv".to_string(), crate::theme::default_theme());

        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| ui(frame, &mut app, frame.area()))
            .unwrap();

        let buffer = terminal.backend().buffer();
        let text: String = (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer.cell(Position::new(x, y)).unwrap().symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(text.contains("hello"), "single-row value missing:\n{text}");
        assert!(text.contains('1'), "single-row value missing:\n{text}");
        assert!(
            !text.contains(NULL_GLYPH),
            "no nulls in the data, so no ∅ should render:\n{text}"
        );
    }

    #[test]
    fn test_empty_string_does_not_render_null_glyph() {
        // Empty strings must stay blank — only real nulls get the glyph.
        let s = Series::new("col".into(), &[Some("alice"), Some(""), Some("bob")]);
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let mut app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());

        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| ui(frame, &mut app, frame.area()))
            .unwrap();

        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        for y in 0..area.height {
            for x in 0..area.width {
                let cell = buffer.cell(Position::new(x, y)).unwrap();
                assert_ne!(
                    cell.symbol(),
                    NULL_GLYPH,
                    "no ∅ glyph should appear when every value is a real string"
                );
            }
        }
    }

    #[test]
    fn test_numeric_null_renders_glyph() {
        // Numeric nulls go through the same cast-to-String path; they must also
        // render as ∅ rather than a blank cell.
        let s = Series::new("val".into(), &[Some(1i64), None, Some(3)]);
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let mut app = App::new(df, "test.csv".to_string(), crate::theme::default_theme());

        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| ui(frame, &mut app, frame.area()))
            .unwrap();

        let buffer = terminal.backend().buffer();
        let null_count = (0..buffer.area.height)
            .flat_map(|y| (0..buffer.area.width).map(move |x| (x, y)))
            .filter(|&(x, y)| {
                buffer
                    .cell(Position::new(x, y))
                    .is_some_and(|c| c.symbol() == NULL_GLYPH)
            })
            .count();
        assert_eq!(null_count, 1, "one numeric null should render as one ∅");
    }
}
