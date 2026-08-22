use crate::app::App;
use crate::browser::app::{BrowserApp, Focus, Viewer};
use crate::browser::ui::browser_ui;
use crate::browser::{load_file_for_browser, EntryKind, FileBrowser};
use crate::config::MAX_TEXT_BYTES;
use crate::events::dispatch_viewer_key;
use crate::text_viewer::{dispatch_text_viewer_key, load_text, TextApp, TextLoadError};
use crossterm::event::{self, KeyModifiers};

pub fn run_browser_app(
    terminal: &mut ratatui::DefaultTerminal,
    mut app: BrowserApp,
) -> Result<(), Box<dyn std::error::Error>> {
    while !app.should_quit {
        terminal.draw(|frame| browser_ui(frame, &mut app))?;

        if let event::Event::Key(key) = event::read()? {
            // Windows terminals report both Press and Release for every key;
            // Unix reports only Press. Without this guard each keystroke would
            // be handled twice on Windows, so toggles cancel themselves out.
            if key.kind != event::KeyEventKind::Press {
                continue;
            }
            let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

            // A pending download owns the keyboard: the destination is free text, so
            // it has to be consumed before any single-key binding sees it.
            if app.download.is_some() {
                handle_download_key(&mut app, &key);
                continue;
            }

            // Theme picker takes precedence over all other browse-mode keys.
            if app.picker.is_some() {
                if let Some(picker) = app.picker.as_mut() {
                    match key.code {
                        event::KeyCode::Char('j') | event::KeyCode::Down => {
                            let next = picker.move_down();
                            app.theme = next;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.set_theme(next);
                            }
                        }
                        event::KeyCode::Char('k') | event::KeyCode::Up => {
                            let prev = picker.move_up();
                            app.theme = prev;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.set_theme(prev);
                            }
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
                        }
                        event::KeyCode::Esc => {
                            let original = picker.original_theme();
                            app.theme = original;
                            if let Some(ref mut viewer) = app.viewer {
                                viewer.set_theme(original);
                            }
                            app.picker = None;
                        }
                        _ => {}
                    }
                }
                continue;
            }

            // T (uppercase) opens the picker — but not when the viewer is accepting text input
            // (Search, Filter, ColumnsView/UniqueValues in search sub-mode).
            let viewer_typing = app.focus == Focus::Viewer
                && app.viewer.as_ref().map(|v| v.is_typing()).unwrap_or(false);
            if key.code == event::KeyCode::Char('T') && !viewer_typing {
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
                        match viewer {
                            Viewer::DataFrame(a) => dispatch_viewer_key(a, &key),
                            Viewer::Text(t) => dispatch_text_viewer_key(t, &key),
                        }
                        if viewer.should_quit() {
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
        event::KeyCode::Char('d') => app.begin_download(),
        event::KeyCode::Char('q') if app.viewer.is_none() => app.should_quit = true,
        _ => {}
    }
}

fn handle_download_key(app: &mut BrowserApp, key: &event::KeyEvent) {
    match key.code {
        event::KeyCode::Enter => app.confirm_download(),
        event::KeyCode::Esc => app.download = None,
        event::KeyCode::Backspace => {
            if let Some(prompt) = app.download.as_mut() {
                prompt.input.pop();
            }
        }
        // Chords are not text: without this guard ctrl-e would append an 'e' to the
        // path instead of toggling the sidebar, and ctrl-c a 'c' instead of aborting.
        event::KeyCode::Char(c)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            if let Some(prompt) = app.download.as_mut() {
                prompt.input.push(c);
            }
        }
        _ => {}
    }
}

fn open_or_descend(app: &mut BrowserApp) {
    let entry = match app.entries.get(app.cursor) {
        Some(e) => e.clone(),
        None => return,
    };

    match entry.kind {
        EntryKind::Dir => app.descend(),
        EntryKind::Binary => {
            app.status = Some(format!("Cannot preview {}: binary file", entry.name));
        }
        EntryKind::Data => {
            let ext = entry.name.rsplit('.').next().unwrap_or("").to_lowercase();

            // Polars happily loads object-rooted JSON as a 1-row all-null
            // DataFrame instead of erroring, so the Err fall-through alone
            // can't distinguish tabular from non-tabular .json. Peek the
            // root byte: only arrays go to the DataFrame viewer; objects
            // and scalars route directly to the text viewer with pretty
            // printing. .ndjson/.jsonl skip this check — each line is its
            // own document.
            if ext == "json" && peek_json_root(&entry.path, app.backend.as_ref()) != Some(b'[') {
                open_as_text(app, &entry.path, &entry.name);
                return;
            }

            match load_file_for_browser(&entry.path, app.backend.as_ref()) {
                Ok((df, title)) => {
                    app.viewer = Some(Viewer::DataFrame(Box::new(App::new(df, title, app.theme))));
                    app.focus = Focus::Viewer;
                    app.status = None;
                }
                Err(e) => {
                    // Polars couldn't parse a JSON array (malformed). Fall
                    // through to text viewer rather than blocking the user.
                    if ext == "json" {
                        open_as_text(app, &entry.path, &entry.name);
                    } else {
                        app.status = Some(format!("Error loading file: {}", e));
                    }
                }
            }
        }
        EntryKind::Text => open_as_text(app, &entry.path, &entry.name),
    }
}

/// Returns the first non-whitespace byte of the file at `path`, or `None`
/// if the file can't be read or is whitespace-only. For local paths this
/// reads at most 4 KiB; for cloud paths it goes through the backend's
/// `download_bytes` (which currently fetches the whole object).
fn peek_json_root(path: &str, backend: &dyn FileBrowser) -> Option<u8> {
    let bytes = if path.starts_with("az://") || path.starts_with("s3://") {
        backend.download_bytes(path).ok()?
    } else {
        use std::io::Read;
        let mut file = std::fs::File::open(path).ok()?;
        let mut buf = [0u8; 4096];
        let n = file.read(&mut buf).ok()?;
        buf[..n].to_vec()
    };
    bytes.iter().copied().find(|b| !b.is_ascii_whitespace())
}

fn open_as_text(app: &mut BrowserApp, path: &str, name: &str) {
    match load_text(path, app.backend.as_ref(), MAX_TEXT_BYTES) {
        Ok(load) => {
            app.viewer = Some(Viewer::Text(TextApp::new(
                load,
                path.to_string(),
                app.theme,
            )));
            app.focus = Focus::Viewer;
            app.status = None;
        }
        Err(TextLoadError::Binary) => {
            app.status = Some(format!("Cannot preview {}: not a text file", name));
        }
        Err(TextLoadError::Io(e)) => {
            app.status = Some(format!("Error loading file: {}", e));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::download::DownloadPrompt;
    use crate::browser::{BrowserError, Entry};

    struct StubBackend;

    impl FileBrowser for StubBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(vec![])
        }
    }

    fn app_with_prompt() -> BrowserApp {
        let mut app = BrowserApp::new(
            Box::new(StubBackend),
            "az://c/data/".to_string(),
            crate::theme::default_theme(),
        );
        app.download = Some(DownloadPrompt::open("az://c/data/sales.csv"));
        app
    }

    fn press(app: &mut BrowserApp, code: event::KeyCode, modifiers: KeyModifiers) {
        handle_download_key(app, &event::KeyEvent::new(code, modifiers));
    }

    #[test]
    fn download_prompt_edits_the_destination_with_plain_keys() {
        let mut app = app_with_prompt();
        press(&mut app, event::KeyCode::Backspace, KeyModifiers::NONE);
        press(&mut app, event::KeyCode::Char('X'), KeyModifiers::SHIFT);
        assert_eq!(app.download.expect("prompt still open").input, "sales.csX");
    }

    #[test]
    fn download_prompt_ignores_control_and_alt_chords() {
        let mut app = app_with_prompt();
        for modifiers in [KeyModifiers::CONTROL, KeyModifiers::ALT] {
            for c in ['e', 'c', 'q'] {
                press(&mut app, event::KeyCode::Char(c), modifiers);
            }
        }
        // A chord is a command, not text — ctrl-e must not land an 'e' in the path.
        assert_eq!(app.download.expect("prompt still open").input, "sales.csv");
    }
}
