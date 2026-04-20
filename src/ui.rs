//! Terminal rendering for all modes and overlays.
//!
//! The top-level entry point is [`ui`], which routes to a full-screen renderer
//! ([`render_plot`], [`render_columns_view`]) or builds the main table view with
//! overlays ([`render_stats_popup`], [`render_help_popup`],
//! [`render_unique_values_popup`]).
//!
//! All colors come from Catppuccin Mocha (`PALETTE.mocha`) via the thin [`c`]
//! helper. Viewport windowing is handled by [`count_visible_from`], which
//! computes how many columns fit a given terminal width starting from a column
//! offset.

use crate::app::{AggFunc, App, ColumnProfile, Mode, PlotType, SortDirection};
use crate::config;
use catppuccin::PALETTE;
use polars::prelude::{DataType, Series};
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Axis, Block, BorderType, Borders, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table,
};
use ratatui::Frame;

fn c(color: catppuccin::Color) -> Color {
    Color::Rgb(color.rgb.r, color.rgb.g, color.rgb.b)
}

pub fn ui(frame: &mut Frame, app: &mut App) {
    let m = &PALETTE.mocha.colors;

    if matches!(app.mode, Mode::Plot) {
        render_plot(frame, app, m);
        return;
    }

    if matches!(app.mode, Mode::ColumnsView) {
        render_columns_view(frame, app, m);
        return;
    }

    let chunks = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(frame.area());

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
        Cell::from(app.header_label(i)).style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        )
    }))
    .style(Style::default().bg(c(m.surface0)));

    // Pre-cast only the visible columns to String series.
    let all_columns = visible_view.get_columns();
    let str_columns: Vec<Option<Series>> = vis_cols
        .iter()
        .map(|&i| {
            all_columns
                .get(i)
                .and_then(|col| col.as_series())
                .and_then(|s| s.cast(&DataType::String).ok())
        })
        .collect();

    let rows: Vec<Row> = (0..slice_len)
        .map(|i| {
            let abs_row = app.viewport.row + i;
            let bg = if abs_row % 2 == 0 {
                c(m.base)
            } else {
                c(m.mantle)
            };
            Row::new(
                str_columns
                    .iter()
                    .map(|s| {
                        Cell::from(
                            s.as_ref()
                                .and_then(|series| series.str().ok())
                                .and_then(|ca| ca.get(i))
                                .unwrap_or("")
                                .to_string(),
                        )
                    })
                    .collect::<Vec<Cell>>(),
            )
            .style(Style::default().bg(bg).fg(c(m.text)))
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
                .title_style(Style::default().fg(c(m.blue)).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.overlay0)))
                .style(Style::default().bg(c(m.base))),
        )
        .row_highlight_style(Style::default().bg(c(m.surface0)))
        .column_highlight_style(Style::default().bg(c(m.surface1)))
        .cell_highlight_style(
            Style::default()
                .bg(c(m.blue))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        );

    let (bar_text, bar_style) = get_bar(app, m);
    let bar = Paragraph::new(bar_text).style(bar_style);

    // Render with a temporary state. Column index is relative to the visible window.
    let mut render_state = ratatui::widgets::TableState::default();
    render_state.select(Some(selected.saturating_sub(app.viewport.row)));
    render_state.select_column(Some(selected_col.saturating_sub(app.viewport.col)));
    frame.render_stateful_widget(table, chunks[0], &mut render_state);
    frame.render_widget(bar, chunks[1]);
    frame.render_widget(Paragraph::new(shortcut_bar(app, m)), chunks[2]);

    if app.show_stats {
        render_stats_popup(frame, app, m);
    }

    if app.show_help {
        render_help_popup(frame, app, m);
    }

    if matches!(app.mode, Mode::UniqueValues) {
        render_unique_values_popup(frame, app, m);
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

fn render_stats_popup(frame: &mut Frame, app: &mut App, m: &catppuccin::FlavorColors) {
    let col = app
        .state
        .selected_column()
        .unwrap_or(0)
        .min(app.headers.len().saturating_sub(1));
    let stats = app.get_or_compute_stats(col);
    let area = centered_rect(40, 40, frame.area());
    frame.render_widget(Clear, area);
    let content = format!(
        "\n Count:  {}\n Min:    {}\n Max:    {}\n Mean:   {}\n Median: {}",
        stats.count,
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
                .title_style(Style::default().fg(c(m.mauve)).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.mauve))),
        )
        .style(Style::default().bg(c(m.surface0)).fg(c(m.text)));
    frame.render_widget(popup, area);
}

fn render_help_popup(frame: &mut Frame, app: &mut App, m: &catppuccin::FlavorColors) {
    let area = centered_rect(55, 80, frame.area());
    frame.render_widget(Clear, area);
    let text = help_text(m);
    let total_lines = text.lines.len() as u16;
    let visible_lines = area.height.saturating_sub(2); // subtract top+bottom borders
    app.help_scroll = app
        .help_scroll
        .min(total_lines.saturating_sub(visible_lines));
    let popup = Paragraph::new(text)
        .block(
            Block::default()
                .title(" Help — j/k to scroll · ? or Esc to close ")
                .title_style(
                    Style::default()
                        .fg(c(m.lavender))
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.lavender))),
        )
        .style(Style::default().bg(c(m.surface0)).fg(c(m.text)))
        .scroll((app.help_scroll, 0));
    frame.render_widget(popup, area);
}

fn shortcut_bar<'a>(app: &App, m: &catppuccin::FlavorColors) -> Line<'a> {
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
                ("Enter", "Pick X"),
                ("Esc", "Cancel"),
            ],
            &[],
        ),
        Mode::PlotPickX => (
            &[("← →", "Navigate"), ("Enter", "Confirm"), ("Esc", "Back")],
            &[],
        ),
        Mode::Plot => (
            &[("t", "Cycle type"), ("Esc / p", "Close"), ("q", "Quit")],
            &[],
        ),
        Mode::ColumnsView => (
            &[
                ("j / k", "Navigate"),
                ("Enter", "Jump to column"),
                ("Esc / i", "Close"),
            ],
            &[],
        ),
        Mode::UniqueValues => (
            &[
                ("type", "Search"),
                ("j / k", "Navigate"),
                ("Enter", "Apply filter"),
                ("Esc", "Close"),
            ],
            &[],
        ),
    };

    let primary_key = Style::default()
        .bg(c(m.blue))
        .fg(c(m.base))
        .add_modifier(Modifier::BOLD);
    let secondary_key = Style::default()
        .bg(c(m.overlay0))
        .fg(c(m.base))
        .add_modifier(Modifier::BOLD);
    let label = Style::default().bg(c(m.mantle)).fg(c(m.subtext0));
    let gap = Style::default().bg(c(m.mantle));
    let sep = Style::default().bg(c(m.mantle)).fg(c(m.overlay0));

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

    Line::from(spans).style(Style::default().bg(c(m.mantle)))
}

fn get_bar(app: &App, m: &catppuccin::FlavorColors) -> (String, Style) {
    match app.mode {
        Mode::PlotPickY => {
            let y_names = if app.plot.y_cols.is_empty() {
                "none".to_string()
            } else {
                app.plot
                    .y_cols
                    .iter()
                    .map(|&i| app.headers[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (
                format!(
                    " Y: [{}]  —  Space toggle · ←/→ navigate · Enter pick X · Esc cancel ",
                    y_names
                ),
                Style::default()
                    .bg(c(m.mauve))
                    .fg(c(m.base))
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
                    .bg(c(m.mauve))
                    .fg(c(m.base))
                    .add_modifier(Modifier::BOLD),
            )
        }
        Mode::Plot => {
            let cycle_hint = if app.plot.y_cols.len() > 1 {
                "t cycle line/bar"
            } else {
                "t cycle line/bar/histogram"
            };
            (
                format!(
                    " {} chart  |  {}  |  Esc / p to close ",
                    app.plot_type_label(),
                    cycle_hint
                ),
                Style::default().bg(c(m.surface0)).fg(c(m.subtext1)),
            )
        }
        Mode::UniqueValues => (
            format!(
                " Unique values: {}  |  type to search  |  Enter filter  |  Esc close ",
                app.headers
                    .get(app.unique_values.col)
                    .map_or("", |s| s.as_str())
            ),
            Style::default()
                .bg(c(m.teal))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        ),
        Mode::ColumnsView => (
            " Column Inspector  |  j/k navigate  |  Enter jump to column  |  Esc / i close "
                .to_string(),
            Style::default()
                .bg(c(m.green))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Search => (
            format!(" /{}_ ", app.search.query),
            Style::default()
                .bg(c(m.yellow))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Filter => {
            if let Some(ref err) = app.filter.error {
                (
                    format!(" f {}_ — {} ", app.filter.query, err),
                    Style::default()
                        .bg(c(m.red))
                        .fg(c(m.base))
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                (
                    format!(" f {}_ (>,<,>=,<=,!=,= for numbers) ", app.filter.query),
                    Style::default()
                        .bg(c(m.sapphire))
                        .fg(c(m.base))
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
                    c(m.yellow),
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
                    c(m.peach),
                )
            } else if !app.search.results.is_empty() {
                (
                    format!(
                        " [{}/{}]  {} ",
                        app.search.cursor + 1,
                        app.search.results.len(),
                        app.search.query
                    ),
                    c(m.sky),
                )
            } else if !app.filter.filters.is_empty() {
                let filter_summary = app
                    .filter
                    .filters
                    .iter()
                    .map(|(col, q)| {
                        format!("[{}: {}]", app.headers.get(*col).map_or("?", |h| h), q)
                    })
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
                    c(m.teal),
                )
            } else if !app.sort.sorts.is_empty() {
                if let Some(ref err) = app.sort.error {
                    (format!(" Sort error: {} ", err), c(m.red))
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
                        c(m.sapphire),
                    )
                }
            } else if let Some(ref err) = app.sort.error {
                (format!(" Sort error: {} ", err), c(m.red))
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
                    c(m.subtext1),
                )
            };
            (text, Style::default().bg(c(m.surface0)).fg(fg))
        }
    }
}

fn help_text(m: &catppuccin::FlavorColors) -> Text<'static> {
    let section = |title: &'static str| {
        Line::from(vec![
            Span::raw(" "),
            Span::styled(
                title,
                Style::default()
                    .fg(c(m.lavender))
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    };
    let key = |k: &'static str, desc: &'static str| {
        Line::from(vec![
            Span::styled(format!("  {:<14}", k), Style::default().fg(c(m.blue))),
            Span::styled(desc, Style::default().fg(c(m.text))),
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
        key("p", "Mark column as Y, enter pick-X mode"),
        key("←/→ h/l", "Navigate to X column (pick-X mode)"),
        key("Enter", "Confirm X column, show chart"),
        key("t", "Toggle line / bar chart"),
        key("Esc / p", "Close chart"),
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

fn render_unique_values_popup(frame: &mut Frame, app: &mut App, m: &catppuccin::FlavorColors) {
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
        .title_style(Style::default().fg(c(m.teal)).add_modifier(Modifier::BOLD))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(c(m.teal)))
        .style(Style::default().bg(c(m.base)));

    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let zones = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    // Search field
    let search_text = format!(" Search: {}_ ", app.unique_values.query);
    frame.render_widget(
        Paragraph::new(search_text).style(Style::default().bg(c(m.surface0)).fg(c(m.text))),
        zones[0],
    );

    // Values table
    let header = Row::new([
        Cell::from("Value").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Count").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
    ])
    .style(Style::default().bg(c(m.surface0)))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .unique_values
        .filtered
        .iter()
        .enumerate()
        .map(|(i, (val, count))| {
            let bg = if i % 2 == 0 { c(m.base) } else { c(m.mantle) };
            Row::new([
                Cell::from(val.clone()).style(Style::default().fg(c(m.text))),
                Cell::from(count.to_string()).style(Style::default().fg(c(m.subtext1))),
            ])
            .style(Style::default().bg(bg))
        })
        .collect();

    let table = Table::new(rows, [Constraint::Min(10), Constraint::Length(8)])
        .header(header)
        .row_highlight_style(
            Style::default()
                .bg(c(m.teal))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        );

    frame.render_stateful_widget(table, zones[1], &mut app.unique_values.state);
}

fn render_columns_view(frame: &mut Frame, app: &mut App, m: &catppuccin::FlavorColors) {
    let full_area = frame.area();
    frame.render_widget(Clear, full_area);

    let chunks = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(full_area);

    let (bar_text, bar_style) = get_bar(app, m);
    frame.render_widget(Paragraph::new(bar_text).style(bar_style), chunks[1]);

    let header = Row::new([
        Cell::from("Column").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Type").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Count").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Nulls").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Unique").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Min").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Max").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Mean").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Median").style(
            Style::default()
                .fg(c(m.lavender))
                .add_modifier(Modifier::BOLD),
        ),
    ])
    .style(Style::default().bg(c(m.surface0)))
    .bottom_margin(1);

    let rows: Vec<Row> = app
        .columns_view
        .profile
        .iter()
        .enumerate()
        .map(|(i, p)| profile_row(p, i, m))
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(12),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(9),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Length(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(format!(" Column Inspector — {} ", app.file_path))
                .title_style(Style::default().fg(c(m.green)).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.overlay0)))
                .style(Style::default().bg(c(m.base))),
        )
        .row_highlight_style(
            Style::default()
                .bg(c(m.green))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        );

    frame.render_stateful_widget(table, chunks[0], &mut app.columns_view.state);
}

fn profile_row<'a>(p: &'a ColumnProfile, idx: usize, m: &catppuccin::FlavorColors) -> Row<'a> {
    let bg = if idx % 2 == 0 { c(m.base) } else { c(m.mantle) };
    let null_style = if p.null_count > 0 {
        Style::default().fg(c(m.red))
    } else {
        Style::default().fg(c(m.text))
    };
    Row::new([
        Cell::from(p.name.clone()).style(Style::default().fg(c(m.text))),
        Cell::from(p.dtype.clone()).style(Style::default().fg(c(m.subtext1))),
        Cell::from(p.count.to_string()).style(Style::default().fg(c(m.text))),
        Cell::from(p.null_count.to_string()).style(null_style),
        Cell::from(p.unique.to_string()).style(Style::default().fg(c(m.text))),
        Cell::from(p.min.clone()).style(Style::default().fg(c(m.subtext1))),
        Cell::from(p.max.clone()).style(Style::default().fg(c(m.subtext1))),
        Cell::from(p.mean.map_or("—".to_string(), |v| format!("{:.2}", v)))
            .style(Style::default().fg(c(m.blue))),
        Cell::from(p.median.map_or("—".to_string(), |v| format!("{:.2}", v)))
            .style(Style::default().fg(c(m.blue))),
    ])
    .style(Style::default().bg(bg))
}

fn series_color(idx: usize, m: &catppuccin::FlavorColors) -> Color {
    match idx % 8 {
        0 => c(m.blue),
        1 => c(m.green),
        2 => c(m.red),
        3 => c(m.yellow),
        4 => c(m.mauve),
        5 => c(m.peach),
        6 => c(m.teal),
        _ => c(m.lavender),
    }
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

fn render_histogram(
    frame: &mut Frame,
    app: &App,
    m: &catppuccin::FlavorColors,
    y_idx: usize,
    full_area: Rect,
) {
    let zones = Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(full_area);
    let chart_area = zones[0];
    let bar_area = zones[1];

    let bar_text =
        " Histogram chart  |  t cycle line/bar/histogram  |  Esc / p to close ".to_string();
    frame.render_widget(
        Paragraph::new(bar_text).style(Style::default().bg(c(m.surface0)).fg(c(m.subtext1))),
        bar_area,
    );

    let data = match compute_histogram(app, y_idx) {
        Ok(d) => d,
        Err(msg) => {
            let paragraph = Paragraph::new(format!(" {} ", msg))
                .block(
                    Block::default()
                        .title(" Plot Error ")
                        .title_style(Style::default().fg(c(m.red)).add_modifier(Modifier::BOLD))
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(Style::default().fg(c(m.red))),
                )
                .style(Style::default().bg(c(m.base)).fg(c(m.text)));
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
        .style(Style::default().fg(c(m.mauve)))
        .data(&data);

    let chart = Chart::new(vec![dataset])
        .block(
            Block::default()
                .title(format!(" Distribution of {} ", app.headers[y_idx]))
                .title_style(Style::default().fg(c(m.mauve)).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.overlay0)))
                .style(Style::default().bg(c(m.base))),
        )
        .x_axis(
            Axis::default()
                .title(app.headers[y_idx].as_str())
                .style(Style::default().fg(c(m.subtext1)))
                .labels(x_labels)
                .bounds([x_min, x_max]),
        )
        .y_axis(
            Axis::default()
                .title("Count")
                .style(Style::default().fg(c(m.subtext1)))
                .bounds([0.0, y_max + y_pad]),
        );

    frame.render_widget(chart, chart_area);
}

fn render_plot(frame: &mut Frame, app: &App, m: &catppuccin::FlavorColors) {
    let full_area = frame.area();
    frame.render_widget(Clear, full_area);

    let x_idx = match app.plot.x_col {
        Some(x) => x,
        None => return,
    };
    if app.plot.y_cols.is_empty() {
        return;
    }

    // Histogram: single-column only; use first Y col.
    if matches!(app.plot.plot_type, PlotType::Histogram) {
        render_histogram(frame, app, m, app.plot.y_cols[0], full_area);
        return;
    }

    let max_points = (full_area.width as usize * 2).max(200);

    // Extract and downsample data for every Y column.
    let all_series: Vec<(Vec<(f64, f64)>, bool)> = app
        .plot
        .y_cols
        .iter()
        .map(|&y_idx| {
            let (raw, cat) = extract_plot_data(app, x_idx, y_idx);
            (downsample(raw, max_points), cat)
        })
        .collect();

    let x_is_categorical = all_series.iter().any(|(_, cat)| *cat);

    // Collect x labels from the first series (all share the same X column).
    let first_len = all_series.first().map(|(d, _)| d.len()).unwrap_or(0);
    let x_labels = if x_is_categorical {
        collect_all_x_labels(app, x_idx, first_len)
    } else {
        vec![]
    };
    let max_label_len = x_labels
        .iter()
        .map(|s| s.chars().count())
        .max()
        .unwrap_or(0);
    let label_height = (max_label_len as u16).min(full_area.height / 3);

    // Three-zone layout: chart | rotated-label strip | status bar
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

    let cycle_hint = if app.plot.y_cols.len() > 1 {
        "t cycle line/bar"
    } else {
        "t cycle line/bar/histogram"
    };
    frame.render_widget(
        Paragraph::new(format!(
            " {} chart  |  {}  |  Esc / p to close ",
            app.plot_type_label(),
            cycle_hint
        ))
        .style(Style::default().bg(c(m.surface0)).fg(c(m.subtext1))),
        bar_area,
    );

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
                    .title_style(Style::default().fg(c(m.red)).add_modifier(Modifier::BOLD))
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(Style::default().fg(c(m.red))),
            )
            .style(Style::default().bg(c(m.base)).fg(c(m.text)));
        frame.render_widget(msg, chart_area);
        return;
    }

    let x_min = nonempty
        .iter()
        .flat_map(|(_, d)| d.iter().map(|p| p.0))
        .fold(f64::INFINITY, f64::min);
    let x_max = nonempty
        .iter()
        .flat_map(|(_, d)| d.iter().map(|p| p.0))
        .fold(f64::NEG_INFINITY, f64::max);
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
                .style(Style::default().fg(series_color(*series_idx, m)))
                .data(data)
        })
        .collect();

    let title_y = app
        .plot
        .y_cols
        .iter()
        .map(|&i| app.headers[i].as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(format!(" {} vs {} ", title_y, app.headers[x_idx]))
                .title_style(
                    Style::default()
                        .fg(series_color(0, m))
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.overlay0)))
                .style(Style::default().bg(c(m.base))),
        )
        .x_axis(
            Axis::default()
                .title(app.headers[x_idx].as_str())
                .style(Style::default().fg(c(m.subtext1)))
                .bounds([x_min, x_max]),
        )
        .y_axis(
            Axis::default()
                .title("Value")
                .style(Style::default().fg(c(m.subtext1)))
                .bounds(y_bounds),
        );

    frame.render_widget(chart, chart_area);

    // Legend for multi-series plots.
    if app.plot.y_cols.len() > 1 {
        render_plot_legend(frame, app, m, chart_area);
    }

    if !x_labels.is_empty() && label_area.height > 0 {
        render_vertical_x_labels(
            frame,
            &x_labels,
            first_len,
            chart_area,
            label_area,
            c(m.subtext1),
        );
    }
}

fn render_plot_legend(
    frame: &mut Frame,
    app: &App,
    m: &catppuccin::FlavorColors,
    chart_area: Rect,
) {
    let legend_inner_w = app
        .plot
        .y_cols
        .iter()
        .map(|&i| app.headers[i].chars().count() + 3) // "● " prefix + padding
        .max()
        .unwrap_or(4) as u16;
    let legend_w = legend_inner_w + 2; // borders
    let legend_h = app.plot.y_cols.len() as u16 + 2;

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

    let lines: Vec<Line<'_>> = app
        .plot
        .y_cols
        .iter()
        .enumerate()
        .map(|(i, &y_idx)| {
            Line::from(vec![
                Span::styled("● ", Style::default().fg(series_color(i, m))),
                Span::styled(app.headers[y_idx].as_str(), Style::default().fg(c(m.text))),
            ])
        })
        .collect();

    let legend = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(c(m.overlay0)))
            .style(Style::default().bg(c(m.base))),
    );
    frame.render_widget(legend, legend_area);
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
    let s = col.as_series()?;
    if s.dtype().is_primitive_numeric() {
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
    let s = match col.as_series() {
        Some(s) => s,
        None => return vec![],
    };
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
fn render_vertical_x_labels(
    frame: &mut Frame,
    labels: &[String],
    n_data_points: usize,
    chart_area: Rect,
    label_area: Rect,
    color: Color,
) {
    if labels.is_empty() || n_data_points == 0 || label_area.height == 0 {
        return;
    }

    // The plot's x range covers the inner chart width minus the left border.
    // No explicit y-axis labels → inner area starts right after the left border.
    let plot_x = chart_area.x + 1;
    let plot_w = chart_area
        .width
        .saturating_sub(config::CHART_BORDER_WIDTH * 2);
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
        App::new(df, "test.csv".to_string())
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
        let app = App::new(df, "test.csv".to_string());
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
        let app = App::new(df, "test.csv".to_string());
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
