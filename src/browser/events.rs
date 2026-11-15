use crate::browser::app::{BrowserApp, Focus, Viewer};
use crate::browser::loading::Done;
use crate::browser::ui::browser_ui;
use crate::browser::{load_file_for_browser, EntryKind, FileBrowser, Progress};
use crate::config::MAX_TEXT_BYTES;
use crate::events::dispatch_viewer_key;
use crate::text_viewer::{
    dispatch_text_viewer_key, load_text, parse_text, read_bytes, TextLoad, TextLoadError,
};
use crossterm::event::{self, KeyModifiers};
use polars::prelude::{JsonFormat, JsonReader, SerReader};
use std::io::Cursor;

pub fn run_browser_app(
    terminal: &mut ratatui::DefaultTerminal,
    mut app: BrowserApp,
    rx: std::sync::mpsc::Receiver<crate::ctl::Request>,
) -> Result<(), Box<dyn std::error::Error>> {
    while !app.should_quit {
        // Also here, not just on an idle tick: a held key never lets `poll` time out.
        app.finish_loading()?;
        terminal.draw(|frame| browser_ui(frame, &mut app))?;
        // Answer `datasight ctl` while waiting for a key. Every `continue` below comes
        // back through here, so an open prompt cannot starve a request.
        while !event::poll(crate::config::CTL_POLL)? {
            // While a file loads, redraw every tick so its bar moves, and hold ctl
            // requests until the new table is in — as when the load blocked here.
            if app.loading.is_some() {
                app.finish_loading()?;
                terminal.draw(|frame| browser_ui(frame, &mut app))?;
                continue;
            }
            let table = match app.viewer.as_mut() {
                Some(Viewer::DataFrame(a)) => Some(&mut **a),
                _ => None,
            };
            if crate::ctl::serve(&rx, table) {
                terminal.draw(|frame| browser_ui(frame, &mut app))?;
            }
        }

        if let event::Event::Key(key) = event::read()? {
            // Windows terminals report both Press and Release for every key;
            // Unix reports only Press. Without this guard each keystroke would
            // be handled twice on Windows, so toggles cancel themselves out.
            if key.kind != event::KeyEventKind::Press {
                continue;
            }
            let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

            // A load in progress owns the keyboard: nothing may change the view under it.
            if app.loading.is_some() {
                handle_loading_key(&mut app, &key);
                continue;
            }

            // A pending download owns the keyboard: the destination is free text, so
            // it has to be consumed before any single-key binding sees it.
            if app.download.is_some() {
                handle_download_key(&mut app, &key);
                continue;
            }

            // So does an open find prompt, and for the same reason: the query is
            // free text, so `q`, `d` and `T` have to reach it as characters
            // rather than as the browse bindings they would otherwise be.
            if app.find.is_some() {
                handle_find_key(&mut app, &key);
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

/// Keys while a job runs. Esc cancels it (dropping it stops the download); q quits,
/// except that a save's first q only warns — quitting throws the download away.
fn handle_loading_key(app: &mut BrowserApp, key: &event::KeyEvent) {
    match key.code {
        event::KeyCode::Esc => app.loading = None,
        event::KeyCode::Char('q') => match app.loading.as_mut() {
            Some(loading) if loading.saves && !loading.quit_armed => loading.quit_armed = true,
            _ => app.should_quit = true,
        },
        _ => {}
    }
}

fn handle_browser_key(app: &mut BrowserApp, key: &event::KeyEvent) {
    match key.code {
        event::KeyCode::Char('j') | event::KeyCode::Down => app.navigate_down(),
        event::KeyCode::Char('k') | event::KeyCode::Up => app.navigate_up(),
        event::KeyCode::Esc => app.ascend(),
        event::KeyCode::Char('.') | event::KeyCode::Enter => open_or_descend(app),
        event::KeyCode::Char('d') => app.begin_download(),
        event::KeyCode::Char('/') => app.open_find(),
        event::KeyCode::Char('q') => app.should_quit = true,
        _ => {}
    }
}

/// Keys for the find prompt. `j`/`k` are query text here, so the list is walked
/// with the arrows or the readline chords instead.
fn handle_find_key(app: &mut BrowserApp, key: &event::KeyEvent) {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        event::KeyCode::Enter => {
            // Restore the full listing first: it leaves the cursor on the entry
            // that was highlighted, so opening it is the ordinary unfiltered path.
            app.close_find();
            open_or_descend(app);
        }
        event::KeyCode::Esc => app.close_find(),
        event::KeyCode::Backspace => app.find_backspace(),
        event::KeyCode::Down => app.navigate_down(),
        event::KeyCode::Up => app.navigate_up(),
        event::KeyCode::Char('n') if ctrl => app.navigate_down(),
        event::KeyCode::Char('p') if ctrl => app.navigate_up(),
        event::KeyCode::Char('u') if ctrl => app.find_clear(),
        // Chords are commands, not text: without this guard ctrl-e would type an
        // 'e' into the query instead of being ignored.
        event::KeyCode::Char(c)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            app.find_push(c)
        }
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
    let entry = match app.selected_entry() {
        Some(e) => e.clone(),
        None => return,
    };

    match entry.kind {
        EntryKind::Dir => app.descend(),
        EntryKind::Binary => {
            app.status = Some(format!("Cannot preview {}: binary file", entry.name));
        }
        EntryKind::Data => {
            app.start_loading(entry.name.clone(), READING, move |backend, progress| {
                open_data(&entry.path, &entry.name, backend, progress)
            });
        }
        EntryKind::Text => {
            app.start_loading(entry.name.clone(), READING, move |backend, progress| {
                let load = load_text(&entry.path, backend, progress, MAX_TEXT_BYTES);
                text_done(load, &entry.path, &entry.name)
            });
        }
    }
}

const READING: &str = "Reading into memory…";

/// Worker side of opening a data file: a table, or text for JSON that isn't one.
fn open_data(path: &str, name: &str, backend: &dyn FileBrowser, progress: &Progress) -> Done {
    if name.to_lowercase().ends_with(".json") {
        return open_json(path, name, backend, progress);
    }
    match load_file_for_browser(path, backend, progress) {
        Ok((df, title)) => Done::Table(df, title),
        Err(e) => Done::Status(format!("Error loading file: {}", e)),
    }
}

/// `.json` is read once — for a cloud object that is the whole download — and its
/// root decides the viewer: an array opens as a table, anything else as
/// pretty-printed text. Polars loads an object root as a 1-row all-null frame
/// instead of erroring, so the parse can't make that call; an array it can't parse
/// (malformed) falls back to text rather than blocking the user. `.ndjson`/`.jsonl`
/// never come here — each line is its own document.
fn open_json(path: &str, name: &str, backend: &dyn FileBrowser, progress: &Progress) -> Done {
    let bytes = match read_bytes(path, backend, progress) {
        Ok(bytes) => bytes,
        Err(e) => return Done::Status(format!("Error loading file: {}", e)),
    };
    if bytes.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'[') {
        // `parse_buf` would do, but it takes the buffer, and the text fallback
        // still needs it.
        let table = JsonReader::new(Cursor::new(&bytes))
            .with_json_format(JsonFormat::Json)
            .finish();
        if let Ok(df) = table {
            return Done::Table(crate::try_parse_date_columns(df), path.to_string());
        }
    }
    text_done(parse_text(&bytes, "json", MAX_TEXT_BYTES), path, name)
}

/// What opening `path` in the text viewer hands back.
fn text_done(load: Result<TextLoad, TextLoadError>, path: &str, name: &str) -> Done {
    match load {
        Ok(load) => Done::Text(load, path.to_string()),
        Err(TextLoadError::Binary) => {
            Done::Status(format!("Cannot preview {}: not a text file", name))
        }
        Err(TextLoadError::Io(e)) => Done::Status(format!("Error loading file: {}", e)),
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
    fn opening_a_data_file_loads_in_the_background_then_shows_it() {
        let app = open_fixture("orders.csv");
        assert!(matches!(app.viewer, Some(Viewer::DataFrame(_))));
    }

    #[test]
    fn opening_a_text_file_loads_in_the_background_then_shows_it() {
        let app = open_fixture("sample.txt");
        assert!(matches!(app.viewer, Some(Viewer::Text(_))));
    }

    /// Serves one body for every download, counting them.
    struct JsonBackend {
        body: &'static [u8],
        downloads: std::sync::atomic::AtomicUsize,
    }

    impl FileBrowser for JsonBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(vec![])
        }
        fn download_bytes(&self, _path: &str, _: &Progress) -> Result<Vec<u8>, BrowserError> {
            self.downloads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self.body.to_vec())
        }
    }

    #[test]
    fn a_remote_json_file_downloads_once_whichever_viewer_it_opens_in() {
        let cases: [(&'static [u8], bool); 3] = [
            (br#"[{"a": 1}]"#, true),
            (br#"{"a": 1}"#, false),
            (br#"[{"a": 1"#, false), // malformed array: text, not an error
        ];
        for (body, table) in cases {
            let backend = JsonBackend {
                body,
                downloads: Default::default(),
            };
            let done = open_data("az://c/x.json", "x.json", &backend, &Progress::default());
            let opened = match done {
                Done::Table(..) => table,
                Done::Text(..) => !table,
                Done::Status(s) => panic!("{}", s),
            };
            assert!(opened, "wrong viewer for {}", String::from_utf8_lossy(body));
            let downloads = backend.downloads.into_inner();
            assert_eq!(downloads, 1, "{}", String::from_utf8_lossy(body));
        }
    }

    fn loading_key(app: &mut BrowserApp, code: event::KeyCode) {
        handle_loading_key(app, &event::KeyEvent::new(code, KeyModifiers::NONE));
    }

    #[test]
    fn q_during_a_download_only_warns_until_pressed_again() {
        // StubBackend fails every download before anything is written, and the job
        // is never polled, so `loading` stays up for the whole test.
        let mut app = app_with_prompt();
        app.confirm_download();
        loading_key(&mut app, event::KeyCode::Char('q'));
        assert!(
            !app.should_quit,
            "a stray q must not throw the download away"
        );
        assert!(app.loading.as_ref().is_some_and(|l| l.quit_armed));
        loading_key(&mut app, event::KeyCode::Char('q'));
        assert!(app.should_quit);
    }

    #[test]
    fn q_during_an_open_quits_at_once() {
        let mut app = listing();
        open_or_descend(&mut app);
        assert!(app.loading.is_some());
        loading_key(&mut app, event::KeyCode::Char('q'));
        assert!(app.should_quit, "an open dies with the app anyway");
    }

    /// Opens `name` from the fixtures listing and waits for its background load.
    fn open_fixture(name: &str) -> BrowserApp {
        let mut app = BrowserApp::new(
            Box::new(crate::browser::local::LocalBackend),
            "tests/fixtures".to_string(),
            crate::theme::default_theme(),
        );
        app.cursor = app
            .matches
            .iter()
            .position(|m| app.entries[m.index].name == name)
            .expect("fixture listed");
        open_or_descend(&mut app);
        assert!(app.loading.is_some() && app.viewer.is_none());
        app.wait_for_loading();
        assert_eq!(app.focus, Focus::Viewer);
        app
    }

    // ── fuzzy find ────────────────────────────────────────────────────────────

    struct ListBackend {
        entries: Vec<Entry>,
    }

    impl FileBrowser for ListBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(self.entries.clone())
        }
    }

    fn entry(name: &str) -> Entry {
        Entry {
            kind: crate::browser::classify(name),
            name: name.to_string(),
            path: format!("/test/{}", name),
        }
    }

    fn app_with_listing(entries: Vec<Entry>) -> BrowserApp {
        BrowserApp::new(
            Box::new(ListBackend { entries }),
            "/test".to_string(),
            crate::theme::default_theme(),
        )
    }

    fn listing() -> BrowserApp {
        app_with_listing(vec![
            entry("orders.csv"),
            entry("sales.parquet"),
            entry("notes.txt"),
        ])
    }

    fn browser_key(app: &mut BrowserApp, code: event::KeyCode) {
        handle_browser_key(app, &event::KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn find_key(app: &mut BrowserApp, code: event::KeyCode, modifiers: KeyModifiers) {
        handle_find_key(app, &event::KeyEvent::new(code, modifiers));
    }

    fn type_into_find(app: &mut BrowserApp, query: &str) {
        for c in query.chars() {
            find_key(app, event::KeyCode::Char(c), KeyModifiers::NONE);
        }
    }

    #[test]
    fn slash_opens_the_find_prompt() {
        let mut app = listing();
        browser_key(&mut app, event::KeyCode::Char('/'));
        assert!(app.find.is_some());
    }

    #[test]
    fn find_prompt_narrows_the_listing_as_it_is_typed() {
        let mut app = listing();
        app.open_find();
        type_into_find(&mut app, "sal");
        assert_eq!(app.find.as_ref().unwrap().query, "sal");
        assert_eq!(app.selected_entry().unwrap().name, "sales.parquet");
    }

    #[test]
    fn find_prompt_swallows_the_browse_bindings_as_text() {
        // `q`, `d` and `T` are all live keys in the browser pane. Inside the
        // prompt they are characters, or a query could never contain them.
        let mut app = listing();
        app.open_find();
        type_into_find(&mut app, "qdT");
        assert_eq!(app.find.as_ref().unwrap().query, "qdT");
        assert!(!app.should_quit, "q must not quit while typing a query");
        assert!(
            app.download.is_none(),
            "d must not open the download prompt"
        );
    }

    #[test]
    fn find_prompt_ignores_control_and_alt_chords() {
        let mut app = listing();
        app.open_find();
        for modifiers in [KeyModifiers::CONTROL, KeyModifiers::ALT] {
            for c in ['e', 'c', 'q'] {
                find_key(&mut app, event::KeyCode::Char(c), modifiers);
            }
        }
        assert_eq!(
            app.find.as_ref().unwrap().query,
            "",
            "a chord is a command, not query text"
        );
    }

    #[test]
    fn ctrl_u_clears_the_query() {
        let mut app = listing();
        app.open_find();
        type_into_find(&mut app, "sal");
        find_key(&mut app, event::KeyCode::Char('u'), KeyModifiers::CONTROL);
        assert_eq!(app.find.as_ref().unwrap().query, "");
        assert_eq!(app.matches.len(), 3);
    }

    #[test]
    fn arrows_and_readline_chords_walk_the_matches() {
        let mut app = listing();
        app.open_find();
        // 's' matches all three names; j/k would be query text, so movement has to
        // come from somewhere else.
        type_into_find(&mut app, "s");
        find_key(&mut app, event::KeyCode::Down, KeyModifiers::NONE);
        assert_eq!(app.cursor, 1);
        find_key(&mut app, event::KeyCode::Char('n'), KeyModifiers::CONTROL);
        assert_eq!(app.cursor, 2);
        find_key(&mut app, event::KeyCode::Char('p'), KeyModifiers::CONTROL);
        assert_eq!(app.cursor, 1);
        find_key(&mut app, event::KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn esc_closes_the_prompt_and_restores_the_listing() {
        let mut app = listing();
        app.open_find();
        type_into_find(&mut app, "sal");
        find_key(&mut app, event::KeyCode::Esc, KeyModifiers::NONE);
        assert!(app.find.is_none());
        assert_eq!(app.matches.len(), 3);
    }

    #[test]
    fn enter_opens_the_highlighted_match() {
        let mut app = app_with_listing(vec![
            entry("orders.csv"),
            Entry {
                name: "reports".to_string(),
                path: "/test/reports".to_string(),
                kind: crate::browser::EntryKind::Dir,
            },
        ]);
        app.open_find();
        type_into_find(&mut app, "rep");
        find_key(&mut app, event::KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(app.cwd, "/test/reports", "Enter acts on the filtered entry");
        assert!(app.find.is_none());
    }

    // ── quit ──────────────────────────────────────────────────────────────────

    #[test]
    fn q_quits_the_browser_pane() {
        let mut app = listing();
        browser_key(&mut app, event::KeyCode::Char('q'));
        assert!(app.should_quit);
    }

    #[test]
    fn q_quits_with_a_file_already_open() {
        // The reported bug: `q` was guarded on `viewer.is_none()`, so opening a
        // file left the browser pane with no way out.
        use polars::prelude::*;
        let mut app = listing();
        app.viewer = Some(Viewer::DataFrame(Box::new(crate::app::App::new(
            df!("col" => &[1i64]).unwrap(),
            "test.csv".to_string(),
            crate::theme::default_theme(),
        ))));
        browser_key(&mut app, event::KeyCode::Char('q'));
        assert!(app.should_quit);
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
