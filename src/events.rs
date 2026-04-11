use crate::app::{App, Mode, PlotType};
use crate::ui::ui;
use crossterm::event;

const PAGE_SCROLL_AMOUNT: u16 = 20;

pub fn run_app(
    terminal: &mut ratatui::DefaultTerminal,
    mut app: App,
) -> Result<(), Box<dyn std::error::Error>> {
    while !app.should_quit {
        terminal.draw(|frame| ui(frame, &mut app))?;

        if let event::Event::Key(key) = event::read()? {
            match app.mode {
                Mode::Normal if app.show_help => match key.code {
                    event::KeyCode::Char('j') | event::KeyCode::Down => {
                        app.help_scroll = app.help_scroll.saturating_add(1)
                    }
                    event::KeyCode::Char('k') | event::KeyCode::Up => {
                        app.help_scroll = app.help_scroll.saturating_sub(1)
                    }
                    event::KeyCode::PageDown => {
                        app.help_scroll = app.help_scroll.saturating_add(PAGE_SCROLL_AMOUNT)
                    }
                    event::KeyCode::PageUp => {
                        app.help_scroll = app.help_scroll.saturating_sub(PAGE_SCROLL_AMOUNT)
                    }
                    event::KeyCode::Char('?') | event::KeyCode::Esc => {
                        app.show_help = false;
                        app.help_scroll = 0;
                    }
                    _ => {}
                },
                Mode::Normal => match key.code {
                    event::KeyCode::Char('q') => app.should_quit = true,
                    event::KeyCode::Down => app.select_next_row(),
                    event::KeyCode::Up => app.select_previous_row(),
                    event::KeyCode::Left => app.select_previous_column(),
                    event::KeyCode::Right => app.select_next_column(),
                    event::KeyCode::Char('j') => app.select_next_row(),
                    event::KeyCode::Char('k') => app.select_previous_row(),
                    event::KeyCode::Char('h') => app.select_previous_column(),
                    event::KeyCode::Char('l') => app.select_next_column(),
                    event::KeyCode::Char('g') => app.select_first_row(),
                    event::KeyCode::Char('G') => app.select_last_row(),
                    event::KeyCode::PageDown => app.scroll_down_rows(PAGE_SCROLL_AMOUNT),
                    event::KeyCode::PageUp => app.scroll_up_rows(PAGE_SCROLL_AMOUNT),
                    event::KeyCode::Home => app.select_first_row(),
                    event::KeyCode::End => app.select_last_row(),
                    event::KeyCode::Char('_') => autofit_column(&mut app),
                    event::KeyCode::Char('/') => enter_search_mode(&mut app),
                    event::KeyCode::Char('n') => go_to_next_search_result(&mut app),
                    event::KeyCode::Char('N') => go_to_previous_search_result(&mut app),
                    event::KeyCode::Char('f') => enter_filter_mode(&mut app),
                    event::KeyCode::Char('F') => clear_filters(&mut app),
                    event::KeyCode::Char('s') => app.sort_by_column(),
                    event::KeyCode::Char('S') => app.show_stats = !app.show_stats,
                    event::KeyCode::Char('b') => app.toggle_groupby_key(),
                    event::KeyCode::Char('a') => app.cycle_groupby_agg(),
                    event::KeyCode::Char('B') => {
                        if app.groupby_active {
                            app.clear_groupby();
                        } else {
                            app.apply_groupby();
                        }
                    }
                    event::KeyCode::Char('?') => app.show_help = !app.show_help,
                    event::KeyCode::Esc => app.show_help = false,
                    event::KeyCode::Char('=') => app.autofit_all_columns(),
                    event::KeyCode::Char('p') if !app.df.is_empty() => {
                        app.plot_y_col = app.state.selected_column();
                        app.mode = Mode::PlotPickX;
                    }
                    event::KeyCode::Char('i') if !app.df.is_empty() => {
                        app.build_columns_profile();
                        app.mode = Mode::ColumnsView;
                    }
                    event::KeyCode::Char('u') if !app.df.is_empty() => {
                        app.build_unique_values();
                        app.mode = Mode::UniqueValues;
                    }
                    _ => {}
                },
                Mode::Search => match key.code {
                    event::KeyCode::Backspace => pop_char_from_search_query(&mut app),
                    event::KeyCode::Enter => to_first_search_query_result(&mut app),
                    event::KeyCode::Char(c) => push_char_to_search_query(&mut app, c),
                    event::KeyCode::Esc => from_search_to_normal_mode(&mut app),
                    _ => {}
                },
                Mode::PlotPickX => match key.code {
                    event::KeyCode::Left | event::KeyCode::Char('h') => {
                        app.select_previous_column()
                    }
                    event::KeyCode::Right | event::KeyCode::Char('l') => app.select_next_column(),
                    event::KeyCode::Enter => {
                        app.plot_x_col = app.state.selected_column();
                        app.mode = Mode::Plot;
                    }
                    event::KeyCode::Esc => {
                        app.plot_y_col = None;
                        app.mode = Mode::Normal;
                    }
                    _ => {}
                },
                Mode::Plot => match key.code {
                    event::KeyCode::Char('t') => {
                        app.plot_type = match app.plot_type {
                            PlotType::Line => PlotType::Bar,
                            PlotType::Bar => PlotType::Histogram,
                            PlotType::Histogram => PlotType::Line,
                        };
                    }
                    event::KeyCode::Esc | event::KeyCode::Char('p') => app.mode = Mode::Normal,
                    event::KeyCode::Char('q') => app.should_quit = true,
                    _ => {}
                },
                Mode::UniqueValues => match key.code {
                    event::KeyCode::Esc => app.mode = Mode::Normal,
                    event::KeyCode::Down | event::KeyCode::Char('j') => {
                        app.unique_values_state.select_next()
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        app.unique_values_state.select_previous()
                    }
                    event::KeyCode::Backspace => {
                        app.unique_values_query.pop();
                        app.filter_unique_values();
                    }
                    event::KeyCode::Enter => {
                        if let Some(idx) = app.unique_values_state.selected() {
                            if let Some((value, _)) = app.unique_values_filtered.get(idx) {
                                let filter = format!("= {}", value);
                                let col = app.unique_values_col;
                                let already_exists =
                                    app.filters.iter().any(|(c, q)| *c == col && q == &filter);
                                if !already_exists {
                                    app.filters.push((col, filter));
                                    app.update_filter();
                                }
                            }
                        }
                        app.mode = Mode::Normal;
                    }
                    event::KeyCode::Char(c) => {
                        app.unique_values_query.push(c);
                        app.filter_unique_values();
                    }
                    _ => {}
                },
                Mode::ColumnsView => match key.code {
                    event::KeyCode::Down | event::KeyCode::Char('j') => {
                        app.columns_view_state.select_next()
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        app.columns_view_state.select_previous()
                    }
                    event::KeyCode::Char('g') | event::KeyCode::Home => {
                        app.columns_view_state.select_first()
                    }
                    event::KeyCode::Char('G') | event::KeyCode::End => {
                        app.columns_view_state.select_last()
                    }
                    event::KeyCode::Enter => {
                        let col = app.columns_view_state.selected().unwrap_or(0);
                        app.state.select_column(Some(col));
                        app.mode = Mode::Normal;
                    }
                    event::KeyCode::Esc | event::KeyCode::Char('i') => {
                        app.mode = Mode::Normal;
                    }
                    event::KeyCode::Char('q') => app.should_quit = true,
                    _ => {}
                },
                Mode::Filter => match key.code {
                    event::KeyCode::Backspace => pop_char_from_filter_query(&mut app),
                    event::KeyCode::Enter => to_normal_mode_with_filter(&mut app),
                    event::KeyCode::Char(c) => push_char_to_filter_query(&mut app, c),
                    event::KeyCode::Esc => from_filter_to_normal_mode(&mut app),
                    _ => {}
                },
            }
        }
    }
    Ok(())
}

fn autofit_column(app: &mut App) {
    app.autofit_selected_column();
}

fn enter_search_mode(app: &mut App) {
    app.mode = Mode::Search;
    app.search_results = Vec::new();
    app.search_query = String::new();
}

fn enter_filter_mode(app: &mut App) {
    app.mode = Mode::Filter;
    app.filter_input = String::new();
    app.filter_col = app.state.selected_column();
}

fn push_char_to_search_query(app: &mut App, c: char) {
    app.search_query.push(c);
    app.update_search();
}

fn push_char_to_filter_query(app: &mut App, c: char) {
    app.filter_input.push(c);
    app.update_filter();
}

fn pop_char_from_search_query(app: &mut App) {
    app.search_query.pop();
    app.update_search();
}

fn pop_char_from_filter_query(app: &mut App) {
    app.filter_input.pop();
    app.update_filter();
}

fn to_first_search_query_result(app: &mut App) {
    if app.search_results.is_empty() {
        return;
    }
    app.state
        .select(Some(app.search_results[app.search_cursor]));
    app.mode = Mode::Normal;
}

fn to_normal_mode_with_filter(app: &mut App) {
    if app.filter_error.is_some() {
        return;
    }
    app.mode = Mode::Normal;
    if !app.filter_input.is_empty() {
        let col = app.filter_col.unwrap_or(0);
        let already_exists = app
            .filters
            .iter()
            .any(|(c, q)| *c == col && q == &app.filter_input);
        if !already_exists {
            app.filters.push((col, app.filter_input.clone()));
            app.update_filter();
        }
        app.filter_input = String::new();
    }
}

fn from_search_to_normal_mode(app: &mut App) {
    app.mode = Mode::Normal;
    app.search_results = Vec::new();
    app.search_query = String::new();
    app.search_cursor = 0;
}

fn from_filter_to_normal_mode(app: &mut App) {
    app.mode = Mode::Normal;
    app.filter_input = String::new();
}

fn clear_filters(app: &mut App) {
    app.filter_input = String::new();
    app.filters = Vec::new();
    app.update_filter();
}

fn go_to_next_search_result(app: &mut App) {
    if app.search_results.is_empty() {
        return;
    }
    app.search_cursor = if app.search_cursor < app.search_results.len() - 1 {
        app.search_cursor + 1
    } else {
        0
    };
    app.state
        .select(Some(app.search_results[app.search_cursor]));
}

fn go_to_previous_search_result(app: &mut App) {
    if app.search_results.is_empty() {
        return;
    }
    app.search_cursor = if app.search_cursor > 0 {
        app.search_cursor - 1
    } else {
        app.search_results.len() - 1
    };
    app.state
        .select(Some(app.search_results[app.search_cursor]));
}
