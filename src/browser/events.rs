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

            // Global ctrl bindings — intercepted before focus dispatch.
            if ctrl {
                match key.code {
                    event::KeyCode::Char('e') => {
                        app.browser_visible = !app.browser_visible;
                        // If browser was just hidden and focus was there, shift to viewer.
                        if !app.browser_visible
                            && app.focus == Focus::Browser
                            && app.viewer.is_some()
                        {
                            app.focus = Focus::Viewer;
                        }
                        continue;
                    }
                    event::KeyCode::Char('h') => {
                        app.browser_visible = true;
                        app.focus = Focus::Browser;
                        continue;
                    }
                    event::KeyCode::Char('l') => {
                        if app.viewer.is_some() {
                            app.focus = Focus::Viewer;
                        }
                        continue;
                    }
                    _ => {}
                }
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
        event::KeyCode::Char('h') => app.ascend(),
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
                app.viewer = Some(App::new(df, title));
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
