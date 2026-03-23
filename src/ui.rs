use crate::app::{AggFunc, App, ColumnProfile, Mode, PlotType};
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

const Y_AXIS_PADDING: f64 = 0.05;
const CHART_BORDER_WIDTH: u16 = 1;

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
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(frame.area());

    // 2 borders + 1 header row + 1 header bottom-margin = 4 rows of overhead.
    let page_h = (chunks[0].height.saturating_sub(4)) as usize;
    let total_rows = app.view.height();
    let selected = app.state.selected().unwrap_or(0);

    // Scroll the viewport to keep `selected` visible.
    if selected < app.view_offset {
        app.view_offset = selected;
    } else if page_h > 0 && selected >= app.view_offset + page_h {
        app.view_offset = selected.saturating_sub(page_h - 1);
    }
    // Don't let the offset run past the last page.
    app.view_offset = app
        .view_offset
        .min(total_rows.saturating_sub(page_h.max(1)));

    let slice_len = page_h.min(total_rows.saturating_sub(app.view_offset));
    let visible_view = app.view.slice(app.view_offset as i64, slice_len);

    // Horizontal windowing: only pass columns that fit the terminal width to ratatui.
    // 2 border chars; column spacing of 1 between every pair of adjacent columns.
    let available_w = chunks[0].width.saturating_sub(2) as usize;
    let total_cols = app.headers.len();
    let selected_col = app.state.selected_column().unwrap_or(0);

    // Count how many columns fit starting from a given offset.
    let count_from = |start: usize| -> usize {
        let mut used = 0usize;
        let mut count = 0usize;
        for i in start..total_cols {
            let w = app.column_widths.get(i).copied().unwrap_or(15) as usize;
            let needed = if count == 0 { w } else { w + 1 }; // +1 column spacing
            if used + needed > available_w && count > 0 {
                break;
            }
            used += needed;
            count += 1;
        }
        count.max(1)
    };

    // Scroll col_offset to keep selected_col visible.
    if selected_col < app.col_offset {
        app.col_offset = selected_col;
    } else {
        let vis = count_from(app.col_offset);
        if selected_col >= app.col_offset + vis {
            app.col_offset = selected_col.saturating_sub(vis.saturating_sub(1));
        }
    }
    app.col_offset = app.col_offset.min(total_cols.saturating_sub(1));

    let vis_count = count_from(app.col_offset);
    let vis_cols: Vec<usize> = (app.col_offset..total_cols).take(vis_count).collect();

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
            let abs_row = app.view_offset + i;
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
    render_state.select(Some(selected.saturating_sub(app.view_offset)));
    render_state.select_column(Some(selected_col.saturating_sub(app.col_offset)));
    frame.render_stateful_widget(table, chunks[0], &mut render_state);
    frame.render_widget(bar, chunks[1]);

    if app.show_stats {
        let col = app.state.selected_column().unwrap_or(0);
        let stats = app.compute_stats(col);
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

    if app.show_help {
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

    if matches!(app.mode, Mode::UniqueValues) {
        render_unique_values_popup(frame, app, m);
    }
}

fn get_bar(app: &App, m: &catppuccin::FlavorColors) -> (String, Style) {
    match app.mode {
        Mode::PlotPickX => {
            let y_name = app
                .plot_y_col
                .map(|i| app.headers[i].as_str())
                .unwrap_or("?");
            (
                format!(
                    " Y: {}  —  navigate to X column and press Enter  (Esc to cancel) ",
                    y_name
                ),
                Style::default()
                    .bg(c(m.mauve))
                    .fg(c(m.base))
                    .add_modifier(Modifier::BOLD),
            )
        }
        Mode::Plot => (
            format!(
                " {} chart  |  t cycle line/bar/histogram  |  Esc / p to close ",
                app.plot_type_label()
            ),
            Style::default().bg(c(m.surface0)).fg(c(m.subtext1)),
        ),
        Mode::UniqueValues => (
            format!(
                " Unique values: {}  |  type to search  |  Enter filter  |  Esc close ",
                app.headers
                    .get(app.unique_values_col)
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
            format!(" /{}_ ", app.search_query),
            Style::default()
                .bg(c(m.yellow))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Filter => (
            format!(" f {}_ (>,<,>=,<=,!=,= for numbers) ", app.filter_input),
            Style::default()
                .bg(c(m.sapphire))
                .fg(c(m.base))
                .add_modifier(Modifier::BOLD),
        ),
        Mode::Normal => {
            let (text, fg) = if app.groupby_active {
                let key_names = app
                    .saved_headers
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| app.groupby_keys.contains(i))
                    .map(|(_, h)| h.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut agg_entries: Vec<(usize, &AggFunc)> =
                    app.groupby_aggs.iter().map(|(i, f)| (*i, f)).collect();
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
                        format!("{}[{}]", app.saved_headers[*i], sym)
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
            } else if !app.groupby_keys.is_empty() {
                let key_names = app
                    .groupby_keys
                    .iter()
                    .map(|&i| app.headers[i].as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    format!(" GroupBy: {} | press B to execute ", key_names),
                    c(m.peach),
                )
            } else if !app.search_results.is_empty() {
                (
                    format!(
                        " [{}/{}]  {} ",
                        app.search_cursor + 1,
                        app.search_results.len(),
                        app.search_query
                    ),
                    c(m.sky),
                )
            } else if !app.filters.is_empty() {
                let filter_summary = app
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
                        app.state.selected().map_or(0, |i| i + 1),
                        app.view.height(),
                        app.state.selected_column().map_or(0, |i| i + 1),
                        app.headers.len(),
                        app.file_path
                    ),
                    c(m.teal),
                )
            } else {
                (
                    format!(
                        " Row {}/{} | Col {}/{} | {}  ? help ",
                        app.state.selected().map_or(0, |i| i + 1),
                        app.view.height(),
                        app.state.selected_column().map_or(0, |i| i + 1),
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
        key("s", "Sort by column (toggles asc / desc)"),
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
        key("S", "Toggle column stats popup"),
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
        .get(app.unique_values_col)
        .map_or("", |s| s.as_str());
    let title = format!(
        " Unique: {} ({} shown) ",
        col_name,
        app.unique_values_filtered.len()
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
    let search_text = format!(" Search: {}_ ", app.unique_values_query);
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
        .unique_values_filtered
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

    frame.render_stateful_widget(table, zones[1], &mut app.unique_values_state);
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
        .columns_profile
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

    frame.render_stateful_widget(table, chunks[0], &mut app.columns_view_state);
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

fn downsample(data: Vec<(f64, f64)>, max_points: usize) -> Vec<(f64, f64)> {
    if data.len() <= max_points {
        return data;
    }
    let step = data.len() as f64 / max_points as f64;
    (0..max_points)
        .map(|i| data[(i as f64 * step) as usize])
        .collect()
}

fn compute_histogram(app: &App, y_idx: usize) -> Vec<(f64, f64)> {
    let col = match app.view.column(&app.headers[y_idx]) {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    let y_f64 = match series_to_f64(col) {
        Some(s) => s,
        None => return vec![],
    };
    let values: Vec<f64> = y_f64
        .f64()
        .map(|ca| ca.into_iter().flatten().collect())
        .unwrap_or_default();
    if values.is_empty() {
        return vec![];
    }
    let n = values.len();
    // Sturges' rule, clamped to a sensible range.
    let n_bins = ((n as f64).log2().ceil() as usize + 1).clamp(5, 50);
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if (max - min).abs() < f64::EPSILON {
        return vec![(min, n as f64)];
    }
    let bin_w = (max - min) / n_bins as f64;
    let mut counts = vec![0u64; n_bins];
    for v in &values {
        let bin = ((v - min) / bin_w) as usize;
        counts[bin.min(n_bins - 1)] += 1;
    }
    counts
        .iter()
        .enumerate()
        .map(|(i, &c)| (min + (i as f64 + 0.5) * bin_w, c as f64))
        .collect()
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

    let data = compute_histogram(app, y_idx);
    if data.is_empty() {
        let msg = Paragraph::new(" No data to plot. Column must be numeric (int or float). ")
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

    let x_min = data.first().map(|p| p.0).unwrap_or(0.0);
    let x_max = data.last().map(|p| p.0).unwrap_or(1.0);
    let y_max = data.iter().map(|p| p.1).fold(0.0f64, f64::max);
    let y_pad = y_max * Y_AXIS_PADDING;

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

    let (x_idx, y_idx) = match (app.plot_x_col, app.plot_y_col) {
        (Some(x), Some(y)) => (x, y),
        _ => return,
    };

    if matches!(app.plot_type, PlotType::Histogram) {
        render_histogram(frame, app, m, y_idx, full_area);
        return;
    }

    let (raw_data, x_is_categorical) = extract_plot_data(app, x_idx, y_idx);
    // Downsample to ~2× chart width — more points than that are invisible at terminal resolution.
    let max_points = (full_area.width as usize * 2).max(200);
    let data = downsample(raw_data, max_points);

    // Collect all x labels now so we know the max length for layout.
    let x_labels = if x_is_categorical {
        collect_all_x_labels(app, x_idx, data.len())
    } else {
        vec![]
    };
    let max_label_len = x_labels
        .iter()
        .map(|s| s.chars().count())
        .max()
        .unwrap_or(0);
    // Cap so labels never consume more than 1/3 of the screen.
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

    let bar_text = format!(
        " {} chart  |  t cycle line/bar/histogram  |  Esc / p to close ",
        app.plot_type_label()
    );
    frame.render_widget(
        Paragraph::new(bar_text).style(Style::default().bg(c(m.surface0)).fg(c(m.subtext1))),
        bar_area,
    );

    if data.is_empty() {
        let msg = Paragraph::new(" No data to plot. Y column must be numeric (int or float). ")
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

    let x_min = data.iter().map(|p| p.0).fold(f64::INFINITY, f64::min);
    let x_max = data.iter().map(|p| p.0).fold(f64::NEG_INFINITY, f64::max);
    let y_min = data.iter().map(|p| p.1).fold(f64::INFINITY, f64::min);
    let y_max = data.iter().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);

    let y_pad = (y_max - y_min).abs() * Y_AXIS_PADDING;
    let y_bounds = [y_min - y_pad, y_max + y_pad];

    let dataset = Dataset::default()
        .name(app.headers[y_idx].as_str())
        .marker(symbols::Marker::Braille)
        .graph_type(match app.plot_type {
            PlotType::Line => GraphType::Line,
            _ => GraphType::Bar,
        })
        .style(Style::default().fg(c(m.blue)))
        .data(&data);

    let chart = Chart::new(vec![dataset])
        .block(
            Block::default()
                .title(format!(
                    " {} vs {} ",
                    app.headers[y_idx], app.headers[x_idx]
                ))
                .title_style(Style::default().fg(c(m.blue)).add_modifier(Modifier::BOLD))
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(c(m.overlay0)))
                .style(Style::default().bg(c(m.base))),
        )
        // Categorical X: don't pass labels to Chart — we render them vertically below.
        .x_axis(
            Axis::default()
                .title(app.headers[x_idx].as_str())
                .style(Style::default().fg(c(m.subtext1)))
                .bounds([x_min, x_max]),
        )
        .y_axis(
            Axis::default()
                .title(app.headers[y_idx].as_str())
                .style(Style::default().fg(c(m.subtext1)))
                .bounds(y_bounds),
        );

    frame.render_widget(chart, chart_area);

    if !x_labels.is_empty() && label_area.height > 0 {
        render_vertical_x_labels(
            frame,
            &x_labels,
            data.len(),
            chart_area,
            label_area,
            c(m.subtext1),
        );
    }
}

#[cfg(test)]
pub fn extract_plot_data_pub(app: &App, x_idx: usize, y_idx: usize) -> (Vec<(f64, f64)>, bool) {
    extract_plot_data(app, x_idx, y_idx)
}

fn is_numeric_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn series_to_f64(col: &polars::prelude::Column) -> Option<polars::prelude::Series> {
    let s = col.as_series()?;
    if is_numeric_dtype(s.dtype()) {
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
    let plot_w = chart_area.width.saturating_sub(CHART_BORDER_WIDTH * 2);
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
