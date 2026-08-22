//! Main event loop and key-dispatch logic.
//!
//! [`run_app`] drives the terminal: it draws a frame via [`crate::ui::ui`] then
//! blocks on the next crossterm key event. The outer `match` branches on
//! [`crate::app::Mode`]; each arm delegates to small, focused helper functions
//! (`enter_search_mode`, `go_to_next_search_result`, etc.) or mutates [`crate::app::App`]
//! state directly for one-liner transitions.

use crate::app::{App, Mode, PlotType};
use crate::config;
use crate::ui::ui;
use crossterm::event;

pub fn run_app(
    terminal: &mut ratatui::DefaultTerminal,
    mut app: App,
) -> Result<(), Box<dyn std::error::Error>> {
    while !app.should_quit {
        terminal.draw(|frame| {
            let area = frame.area();
            ui(frame, &mut app, area);
        })?;
        if let event::Event::Key(key) = event::read()? {
            // Windows terminals report both Press and Release for every key;
            // Unix reports only Press. Without this guard each keystroke would
            // be handled twice on Windows, so toggles cancel themselves out.
            if key.kind != event::KeyEventKind::Press {
                continue;
            }
            dispatch_viewer_key(&mut app, &key);
        }
    }
    Ok(())
}

/// Dispatch a single key event to the viewer App based on its current mode.
/// All existing mode-specific key handling lives here so both run_app and
/// the browser event loop can reuse it.
pub(crate) fn dispatch_viewer_key(app: &mut App, key: &event::KeyEvent) {
    // Transient messages live for exactly one keystroke.
    app.status = None;

    match app.mode {
        Mode::Normal if app.show_help => match key.code {
            event::KeyCode::Char('j') | event::KeyCode::Down => {
                app.help_scroll = app.help_scroll.saturating_add(1)
            }
            event::KeyCode::Char('k') | event::KeyCode::Up => {
                app.help_scroll = app.help_scroll.saturating_sub(1)
            }
            event::KeyCode::PageDown => {
                app.help_scroll = app.help_scroll.saturating_add(config::PAGE_SCROLL_AMOUNT)
            }
            event::KeyCode::PageUp => {
                app.help_scroll = app.help_scroll.saturating_sub(config::PAGE_SCROLL_AMOUNT)
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
            event::KeyCode::PageDown => app.scroll_down_rows(config::PAGE_SCROLL_AMOUNT),
            event::KeyCode::PageUp => app.scroll_up_rows(config::PAGE_SCROLL_AMOUNT),
            event::KeyCode::Home => app.select_first_row(),
            event::KeyCode::End => app.select_last_row(),
            event::KeyCode::Char('y') => app.copy_cell(),
            event::KeyCode::Char('Y') => app.copy_row(),
            event::KeyCode::Char('_') => autofit_column(app),
            event::KeyCode::Char('-') => app.shrink_selected_column(),
            event::KeyCode::Char('+') => app.grow_selected_column(),
            event::KeyCode::Char('/') => enter_search_mode(app),
            event::KeyCode::Char('n') => go_to_next_search_result(app),
            event::KeyCode::Char('N') => go_to_previous_search_result(app),
            event::KeyCode::Char('f') => enter_filter_mode(app),
            event::KeyCode::Char('F') => clear_filters(app),
            event::KeyCode::Char('s') => app.sort_by_column(),
            event::KeyCode::Char('S') => app.clear_sorts(),
            event::KeyCode::Char('e') => app.show_stats = !app.show_stats,
            event::KeyCode::Char('b') => app.toggle_groupby_key(),
            event::KeyCode::Char('a') => app.cycle_groupby_agg(),
            event::KeyCode::Char('B') => {
                if app.groupby.active {
                    app.clear_groupby();
                } else {
                    app.apply_groupby();
                }
            }
            event::KeyCode::Char('?') => app.show_help = !app.show_help,
            event::KeyCode::Esc => app.show_help = false,
            event::KeyCode::Char('=') => app.autofit_all_columns(),
            event::KeyCode::Char('p') if !app.df.is_empty() => {
                app.plot.y_cols.clear();
                app.plot.y2_cols.clear();
                if let Some(col) = app.state.selected_column() {
                    app.plot.y_cols.push(col);
                }
                app.mode = Mode::PlotPickY;
            }
            event::KeyCode::Char('i') if !app.df.is_empty() => {
                app.build_columns_profile();
                app.mode = Mode::ColumnsView;
            }
            event::KeyCode::Char('u') if !app.df.is_empty() => {
                app.build_unique_values();
                app.mode = Mode::UniqueValues;
            }
            event::KeyCode::Char('w') => enter_export_mode(app),
            event::KeyCode::Char('T') => {
                app.picker = Some(crate::theme_picker::ThemePicker::open(app.theme));
                app.mode = Mode::ThemePicker;
            }
            _ => {}
        },
        Mode::ThemePicker => {
            if let Some(picker) = app.picker.as_mut() {
                match key.code {
                    event::KeyCode::Char('j') | event::KeyCode::Down => {
                        app.theme = picker.move_down();
                    }
                    event::KeyCode::Char('k') | event::KeyCode::Up => {
                        app.theme = picker.move_up();
                    }
                    event::KeyCode::Enter => {
                        if let Some(path) = crate::theme::state_path() {
                            if let Err(e) =
                                crate::theme::write_state_theme_at(&path, app.theme.name)
                            {
                                eprintln!("warning: could not save theme to {:?}: {}", path, e);
                            }
                        }
                        app.picker = None;
                        app.mode = Mode::Normal;
                    }
                    event::KeyCode::Esc => {
                        app.theme = picker.original_theme();
                        app.picker = None;
                        app.mode = Mode::Normal;
                    }
                    _ => {}
                }
            }
        }
        Mode::Search => match key.code {
            event::KeyCode::Backspace => pop_char_from_search_query(app),
            event::KeyCode::Enter => to_first_search_query_result(app),
            event::KeyCode::Char(c) => push_char_to_search_query(app, c),
            event::KeyCode::Esc => from_search_to_normal_mode(app),
            _ => {}
        },
        Mode::PlotPickY => match key.code {
            event::KeyCode::Left | event::KeyCode::Char('h') => app.select_previous_column(),
            event::KeyCode::Right | event::KeyCode::Char('l') => app.select_next_column(),
            event::KeyCode::Char(' ') => {
                if let Some(col) = app.state.selected_column() {
                    // mutual exclusion: remove from y2 if present
                    if let Some(pos) = app.plot.y2_cols.iter().position(|&c| c == col) {
                        app.plot.y2_cols.remove(pos);
                    }
                    if let Some(pos) = app.plot.y_cols.iter().position(|&c| c == col) {
                        app.plot.y_cols.remove(pos);
                    } else {
                        app.plot.y_cols.push(col);
                    }
                }
            }
            event::KeyCode::Char('2') => {
                if let Some(col) = app.state.selected_column() {
                    // refuse if this would leave y_cols empty
                    let already_in_y1 = app.plot.y_cols.contains(&col);
                    let would_drain_y1 = already_in_y1 && app.plot.y_cols.len() == 1;
                    if !would_drain_y1 {
                        if let Some(pos) = app.plot.y_cols.iter().position(|&c| c == col) {
                            app.plot.y_cols.remove(pos);
                        }
                        if let Some(pos) = app.plot.y2_cols.iter().position(|&c| c == col) {
                            app.plot.y2_cols.remove(pos);
                        } else {
                            app.plot.y2_cols.push(col);
                        }
                    }
                }
            }
            event::KeyCode::Enter if !app.plot.y_cols.is_empty() => {
                app.mode = Mode::PlotPickX;
            }
            event::KeyCode::Char('i') if !app.plot.y_cols.is_empty() => {
                app.plot.x_col = None;
                app.mode = Mode::Plot;
            }
            event::KeyCode::Esc => {
                app.plot.y_cols.clear();
                app.plot.y2_cols.clear();
                app.mode = Mode::Normal;
            }
            _ => {}
        },
        Mode::PlotPickX => match key.code {
            event::KeyCode::Left | event::KeyCode::Char('h') => app.select_previous_column(),
            event::KeyCode::Right | event::KeyCode::Char('l') => app.select_next_column(),
            event::KeyCode::Enter => {
                app.plot.x_col = app.state.selected_column();
                app.mode = Mode::Plot;
            }
            event::KeyCode::Esc => {
                app.mode = Mode::PlotPickY;
            }
            _ => {}
        },
        Mode::Plot => match key.code {
            event::KeyCode::Char('t') => {
                app.plot.plot_type = if app.plot.y_cols.len() > 1 || !app.plot.y2_cols.is_empty() {
                    match app.plot.plot_type {
                        PlotType::Line => PlotType::Bar,
                        _ => PlotType::Line,
                    }
                } else {
                    match app.plot.plot_type {
                        PlotType::Line => PlotType::Bar,
                        PlotType::Bar => PlotType::Histogram,
                        PlotType::Histogram => PlotType::Line,
                    }
                };
            }
            event::KeyCode::Esc | event::KeyCode::Char('p') => app.mode = Mode::Normal,
            event::KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        Mode::UniqueValues => {
            if app.unique_values.searching {
                match key.code {
                    event::KeyCode::Esc => app.unique_values.searching = false,
                    event::KeyCode::Enter => apply_unique_value_filter(app),
                    event::KeyCode::Down => app.unique_values.state.select_next(),
                    event::KeyCode::Up => app.unique_values.state.select_previous(),
                    event::KeyCode::Home => app.unique_values.state.select_first(),
                    event::KeyCode::End => app.unique_values.state.select_last(),
                    event::KeyCode::Backspace => {
                        app.unique_values.query.pop();
                        app.filter_unique_values();
                    }
                    event::KeyCode::Char(c) => {
                        app.unique_values.query.push(c);
                        app.filter_unique_values();
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    event::KeyCode::Esc => app.mode = Mode::Normal,
                    event::KeyCode::Enter => apply_unique_value_filter(app),
                    event::KeyCode::Down | event::KeyCode::Char('j') => {
                        app.unique_values.state.select_next()
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        app.unique_values.state.select_previous()
                    }
                    event::KeyCode::Char('g') | event::KeyCode::Home => {
                        app.unique_values.state.select_first()
                    }
                    event::KeyCode::Char('G') | event::KeyCode::End => {
                        app.unique_values.state.select_last()
                    }
                    event::KeyCode::Char('/') => {
                        app.unique_values.query.clear();
                        app.unique_values.searching = true;
                        app.filter_unique_values();
                    }
                    event::KeyCode::Char('q') => app.should_quit = true,
                    _ => {}
                }
            }
        }
        Mode::ColumnsView => {
            if app.columns_view.searching {
                match key.code {
                    event::KeyCode::Esc => app.columns_view.searching = false,
                    event::KeyCode::Enter => jump_to_selected_column(app),
                    event::KeyCode::Down => app.columns_view.state.select_next(),
                    event::KeyCode::Up => app.columns_view.state.select_previous(),
                    event::KeyCode::Home => app.columns_view.state.select_first(),
                    event::KeyCode::End => app.columns_view.state.select_last(),
                    event::KeyCode::Backspace => {
                        app.columns_view.query.pop();
                        app.filter_columns_profile();
                    }
                    event::KeyCode::Char(c) => {
                        app.columns_view.query.push(c);
                        app.filter_columns_profile();
                    }
                    _ => {}
                }
            } else {
                match key.code {
                    event::KeyCode::Esc | event::KeyCode::Char('i') => app.mode = Mode::Normal,
                    event::KeyCode::Enter => jump_to_selected_column(app),
                    event::KeyCode::Down | event::KeyCode::Char('j') => {
                        app.columns_view.state.select_next()
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        app.columns_view.state.select_previous()
                    }
                    event::KeyCode::Char('g') | event::KeyCode::Home => {
                        app.columns_view.state.select_first()
                    }
                    event::KeyCode::Char('G') | event::KeyCode::End => {
                        app.columns_view.state.select_last()
                    }
                    event::KeyCode::Char('/') => {
                        app.columns_view.query.clear();
                        app.columns_view.searching = true;
                        app.filter_columns_profile();
                    }
                    event::KeyCode::Char('q') => app.should_quit = true,
                    _ => {}
                }
            }
        }
        Mode::Filter => match key.code {
            event::KeyCode::Backspace => pop_char_from_filter_query(app),
            event::KeyCode::Enter => to_normal_mode_with_filter(app),
            event::KeyCode::Char(c) => push_char_to_filter_query(app, c),
            event::KeyCode::Esc => from_filter_to_normal_mode(app),
            _ => {}
        },
        Mode::Export => match key.code {
            event::KeyCode::Backspace => {
                app.export_path.pop();
            }
            // A blank path has nothing to resolve, so Enter stays in the prompt.
            event::KeyCode::Enter if !app.export_path.trim().is_empty() => {
                let path = std::mem::take(&mut app.export_path);
                app.export_view(&path);
                app.mode = Mode::Normal;
            }
            event::KeyCode::Char(c) => app.export_path.push(c),
            event::KeyCode::Esc => {
                app.export_path.clear();
                app.mode = Mode::Normal;
            }
            _ => {}
        },
    }
}

fn enter_export_mode(app: &mut App) {
    app.mode = Mode::Export;
    app.export_path = crate::export::default_filename(&app.file_path);
}

fn autofit_column(app: &mut App) {
    app.autofit_selected_column();
}

fn enter_search_mode(app: &mut App) {
    app.mode = Mode::Search;
    app.search.results = Vec::new();
    app.search.query = String::new();
}

fn enter_filter_mode(app: &mut App) {
    app.mode = Mode::Filter;
    app.filter.query = String::new();
    app.filter.col = app.state.selected_column();
}

fn push_char_to_search_query(app: &mut App, c: char) {
    app.search.query.push(c);
    app.update_search();
}

fn push_char_to_filter_query(app: &mut App, c: char) {
    app.filter.query.push(c);
    app.update_filter();
}

fn pop_char_from_search_query(app: &mut App) {
    app.search.query.pop();
    app.update_search();
}

fn pop_char_from_filter_query(app: &mut App) {
    app.filter.query.pop();
    app.update_filter();
}

fn to_first_search_query_result(app: &mut App) {
    if app.search.results.is_empty() {
        return;
    }
    app.state
        .select(Some(app.search.results[app.search.cursor]));
    app.mode = Mode::Normal;
}

fn to_normal_mode_with_filter(app: &mut App) {
    if app.filter.error.is_some() {
        return;
    }
    app.mode = Mode::Normal;
    if !app.filter.query.is_empty() {
        let col = app.filter.col.unwrap_or(0);
        if let Some(col_name) = app.headers.get(col).cloned() {
            let already_exists = app
                .filter
                .filters
                .iter()
                .any(|(c, q)| c == &col_name && q == &app.filter.query);
            if !already_exists {
                app.filter
                    .filters
                    .push((col_name, app.filter.query.clone()));
                app.update_filter();
            }
        }
        app.filter.query = String::new();
    }
}

fn from_search_to_normal_mode(app: &mut App) {
    app.mode = Mode::Normal;
    app.search.results = Vec::new();
    app.search.query = String::new();
    app.search.cursor = 0;
}

fn from_filter_to_normal_mode(app: &mut App) {
    app.mode = Mode::Normal;
    app.filter.query = String::new();
}

fn clear_filters(app: &mut App) {
    app.filter.query = String::new();
    app.filter.filters = Vec::new();
    app.update_filter();
}

fn go_to_next_search_result(app: &mut App) {
    if app.search.results.is_empty() {
        return;
    }
    app.search.cursor = if app.search.cursor < app.search.results.len() - 1 {
        app.search.cursor + 1
    } else {
        0
    };
    app.state
        .select(Some(app.search.results[app.search.cursor]));
}

fn go_to_previous_search_result(app: &mut App) {
    if app.search.results.is_empty() {
        return;
    }
    app.search.cursor = if app.search.cursor > 0 {
        app.search.cursor - 1
    } else {
        app.search.results.len() - 1
    };
    app.state
        .select(Some(app.search.results[app.search.cursor]));
}

fn jump_to_selected_column(app: &mut App) {
    if let Some(sel) = app.columns_view.state.selected() {
        if let Some(&col) = app.columns_view.filtered.get(sel) {
            app.state.select_column(Some(col));
        }
    }
    app.mode = Mode::Normal;
}

fn apply_unique_value_filter(app: &mut App) {
    if let Some(idx) = app.unique_values.state.selected() {
        if let Some((value, _)) = app.unique_values.filtered.get(idx) {
            let filter = format!("= {}", value);
            let col = app.unique_values.col;
            if let Some(col_name) = app.headers.get(col).cloned() {
                let already_exists = app
                    .filter
                    .filters
                    .iter()
                    .any(|(c, q)| c == &col_name && q == &filter);
                if !already_exists {
                    app.filter.filters.push((col_name, filter));
                    app.update_filter();
                }
            }
        }
    }
    app.mode = Mode::Normal;
}
