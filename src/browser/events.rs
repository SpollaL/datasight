use crate::app::App;
use crate::browser::app::{BrowserApp, Focus};
use crate::browser::load_file_for_browser;
use crate::browser::ui::browser_ui;
use crate::events::dispatch_viewer_key;
use crossterm::event::{self, KeyModifiers};

pub fn run_browser_app(
    terminal: &mut ratatui::DefaultTerminal,
    mut app: BrowserApp,
) -> Result<(), Box<dyn std::error::Error>> {
    while !app.should_quit {
        terminal.draw(|frame| browser_ui(frame, &mut app))?;

        if let event::Event::Key(key) = event::read()? {
            let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

            // Theme picker takes precedence over all other browse-mode keys.
            if app.picker.is_some() {
                if let Some(picker) = app.picker.as_mut() {
                    match key.code {
                        event::KeyCode::Char('j') | event::KeyCode::Down => {
                            let next = picker.move_down();
                            app.theme = next;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.theme = next;
                            }
                        }
                        event::KeyCode::Char('k') | event::KeyCode::Up => {
                            let prev = picker.move_up();
                            app.theme = prev;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.theme = prev;
                            }
                        }
                        event::KeyCode::Enter => {
                            if let Some(path) = crate::theme::state_path() {
                                if let Err(e) =
                                    crate::theme::write_state_theme_at(&path, app.theme.name)
                                {
                                    eprintln!(
                                        "warning: could not save theme to {:?}: {}",
                                        path, e
                                    );
                                }
                            }
                            app.picker = None;
                        }
                        event::KeyCode::Esc => {
                            let original = picker.original_theme();
                            app.theme = original;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.theme = original;
                            }
                            app.picker = None;
                        }
                        _ => {}
                    }
                }
                continue;
            }

            // T (uppercase) opens the picker.
            if key.code == event::KeyCode::Char('T') {
                app.picker = Some(crate::theme_picker::ThemePicker::open(app.theme));
                continue;
            }

            // ctrl-e: toggle browser sidebar visibility.
            if ctrl && key.code == event::KeyCode::Char('e') {
                app.browser_visible = !app.browser_visible;
                if !app.browser_visible && app.focus == Focus::Browser && app.viewer.is_some() {
                    app.focus = Focus::Viewer;
                }
                continue;
            }

            // Tab: toggle focus between browser and viewer.
            if key.code == event::KeyCode::Tab {
                match app.focus {
                    Focus::Browser if app.viewer.is_some() => app.focus = Focus::Viewer,
                    Focus::Viewer => {
                        app.browser_visible = true;
                        app.focus = Focus::Browser;
                    }
                    _ => {}
                }
                continue;
            }

            match app.focus {
                Focus::Browser => handle_browser_key(&mut app, &key),
                Focus::Viewer => {
                    if let Some(ref mut viewer) = app.viewer {
                        dispatch_viewer_key(viewer, &key);
                        if viewer.should_quit {
                            app.should_quit = true;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn handle_browser_key(app: &mut BrowserApp, key: &event::KeyEvent) {
    match key.code {
        event::KeyCode::Char('j') | event::KeyCode::Down => app.navigate_down(),
        event::KeyCode::Char('k') | event::KeyCode::Up => app.navigate_up(),
        event::KeyCode::Esc => app.ascend(),
        event::KeyCode::Char('.') | event::KeyCode::Enter => open_or_descend(app),
        event::KeyCode::Char('q') if app.viewer.is_none() => app.should_quit = true,
        _ => {}
    }
}

fn open_or_descend(app: &mut BrowserApp) {
    let entry = match app.entries.get(app.cursor) {
        Some(e) => e.clone(),
        None => return,
    };

    if entry.is_dir {
        app.descend();
    } else {
        match load_file_for_browser(&entry.path, app.backend.as_ref()) {
            Ok((df, title)) => {
                app.viewer = Some(App::new(df, title, app.theme));
                app.focus = Focus::Viewer;
                app.status = None;
            }
            Err(e) => {
                app.viewer = None;
                app.status = Some(format!("Error loading file: {}", e));
            }
        }
    }
}
