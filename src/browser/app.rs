use crate::app::App;
use crate::browser::download::{self, DownloadPrompt};
use crate::browser::find::{self, FindPrompt, Match};
use crate::browser::{is_remote, Entry, FileBrowser};
use crate::text_viewer::TextApp;
use crate::theme::Theme;
use crate::theme_picker::ThemePicker;

/// One of the two viewer types loaded into the right-hand pane. The
/// dataframe variant wraps the existing tabular viewer; the text variant
/// drives `TextApp` for plain-text and pretty-printed JSON files.
pub enum Viewer {
    DataFrame(Box<App>),
    Text(TextApp),
}

impl Viewer {
    pub fn set_theme(&mut self, theme: &'static Theme) {
        match self {
            Viewer::DataFrame(a) => a.theme = theme,
            Viewer::Text(t) => t.theme = theme,
        }
    }

    pub fn is_typing(&self) -> bool {
        match self {
            Viewer::DataFrame(a) => a.is_typing(),
            Viewer::Text(t) => t.is_typing(),
        }
    }

    pub fn should_quit(&self) -> bool {
        match self {
            Viewer::DataFrame(a) => a.should_quit,
            Viewer::Text(t) => t.should_quit,
        }
    }
}

pub struct BrowserApp {
    pub backend: Box<dyn FileBrowser>,
    pub entries: Vec<Entry>,
    /// The entries currently on screen, in display order — every entry when no
    /// query is active, the ranked survivors when one is. `cursor` indexes this,
    /// not `entries`, so use [`BrowserApp::selected_entry`] to resolve it.
    pub matches: Vec<Match>,
    pub cursor: usize,
    pub cwd: String,
    pub viewer: Option<Viewer>,
    pub browser_visible: bool,
    pub focus: Focus,
    pub status: Option<String>,
    pub should_quit: bool,
    pub theme: &'static Theme,
    pub picker: Option<ThemePicker>,
    /// Pending download, gated by an `Option` like `picker`. `Some` means the
    /// destination prompt owns the keyboard.
    pub download: Option<DownloadPrompt>,
    /// Live fuzzy filter, gated the same way. `Some` means the query owns the
    /// keyboard and `matches` is narrowed to what it selects.
    pub find: Option<FindPrompt>,
}

#[derive(Debug, PartialEq)]
pub enum Focus {
    Browser,
    Viewer,
}

impl BrowserApp {
    pub fn new(backend: Box<dyn FileBrowser>, root_path: String, theme: &'static Theme) -> Self {
        let (entries, status) = match backend.list(&root_path) {
            Ok(e) => (e, None),
            Err(e) => (Vec::new(), Some(e.to_string())),
        };
        Self {
            matches: find::rank(&entries, ""),
            backend,
            entries,
            cursor: 0,
            cwd: root_path,
            viewer: None,
            browser_visible: true,
            focus: Focus::Browser,
            status,
            should_quit: false,
            theme,
            picker: None,
            download: None,
            find: None,
        }
    }

    /// The entry under the cursor, resolved through the active filter.
    pub fn selected_entry(&self) -> Option<&Entry> {
        self.entries.get(self.selected_index()?)
    }

    /// Where the cursor points in `entries` — the index a filter maps it back to.
    fn selected_index(&self) -> Option<usize> {
        self.matches.get(self.cursor).map(|m| m.index)
    }

    pub fn navigate_down(&mut self) {
        if self.cursor + 1 < self.matches.len() {
            self.cursor += 1;
        }
    }

    pub fn navigate_up(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    /// Descend into the directory at the current cursor (no-op if it's a file).
    pub fn descend(&mut self) {
        if let Some(entry) = self.selected_entry() {
            if entry.is_dir() {
                let path = entry.path.clone();
                self.refresh_listing(path);
            }
        }
    }

    /// Go up to the parent directory/prefix. No-op at the root.
    pub fn ascend(&mut self) {
        let parent = parent_path(&self.cwd);
        if parent != self.cwd {
            self.refresh_listing(parent);
        }
    }

    /// Open the download prompt for the entry under the cursor.
    ///
    /// Only remote files can be downloaded: a local entry is already on disk, and
    /// copying a whole prefix recursively is a different feature.
    pub fn begin_download(&mut self) {
        // Resolved to an index first: borrowing `entries` directly leaves `status`
        // free to be written in the same breath.
        let Some(index) = self.selected_index() else {
            return;
        };
        let entry = &self.entries[index];
        if entry.is_dir() {
            self.status = Some("Download works on files, not directories".to_string());
        } else if !is_remote(&entry.path) {
            self.status = Some(format!("{} is already a local file", entry.name));
        } else {
            self.download = Some(DownloadPrompt::open(&entry.path));
            self.status = None;
        }
    }

    /// Fetch the pending download to the typed destination and report the outcome.
    /// A blank destination has nothing to resolve, so the prompt stays open.
    pub fn confirm_download(&mut self) {
        let Some(prompt) = self.download.as_ref() else {
            return;
        };
        let Some(dest) = download::resolve_dest(&prompt.input, &prompt.source) else {
            return;
        };
        let result = download::download_to(self.backend.as_ref(), &prompt.source, &dest);
        self.download = None;
        // The prompt showed the full destination before Enter, so the confirmation
        // names the file only — it has to fit the narrow browser pane.
        let name = dest
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| dest.display().to_string());
        self.status = Some(match result {
            Ok(bytes) => format!("✓ Saved {} ({})", name, download::human_size(bytes)),
            Err(e) => format!("✗ Download failed: {}", e),
        });
    }

    /// Open the find prompt. The listing is unchanged until something is typed.
    pub fn open_find(&mut self) {
        self.find = Some(FindPrompt::open());
        self.status = None;
    }

    /// Append to the query and re-rank.
    pub fn find_push(&mut self, c: char) {
        if let Some(prompt) = self.find.as_mut() {
            prompt.query.push(c);
            self.apply_find();
        }
    }

    /// Delete the last query char. Backspace on an empty query closes the prompt:
    /// there is nothing left to delete, so the keystroke means "out".
    pub fn find_backspace(&mut self) {
        let Some(prompt) = self.find.as_mut() else {
            return;
        };
        if prompt.query.pop().is_some() {
            self.apply_find();
        } else {
            self.close_find();
        }
    }

    /// Clear the query but stay in the prompt.
    pub fn find_clear(&mut self) {
        if let Some(prompt) = self.find.as_mut() {
            prompt.query.clear();
            self.apply_find();
        }
    }

    /// Close the prompt and restore the full listing, leaving the cursor on
    /// whatever was highlighted. Both `Enter` and `Esc` end here — the entry you
    /// found stays under the cursor either way, and only the caller decides
    /// whether to open it.
    pub fn close_find(&mut self) {
        self.find = None;
        let selected = self.selected_index();
        self.matches = find::rank(&self.entries, "");
        self.cursor = selected.unwrap_or(0);
    }

    /// Re-rank the listing against the current query.
    fn apply_find(&mut self) {
        let query = self.find.as_ref().map_or("", |p| p.query.as_str());
        self.matches = find::rank(&self.entries, query);
        // Back to the top on every edit: the best match for what was just typed
        // is the one worth selecting, and the old cursor points into a list that
        // no longer exists.
        self.cursor = 0;
    }

    fn refresh_listing(&mut self, path: String) {
        match self.backend.list(&path) {
            Ok(entries) => {
                self.entries = entries;
                self.cwd = path;
                self.cursor = 0;
                self.status = None;
                // A query belongs to the listing it was typed against.
                self.find = None;
                self.matches = find::rank(&self.entries, "");
            }
            Err(e) => {
                self.status = Some(e.to_string());
            }
        }
    }
}

/// Compute the parent path. Returns the same path when already at the root.
fn parent_path(path: &str) -> String {
    for scheme in &["az://", "s3://"] {
        if let Some(rest) = path.strip_prefix(scheme) {
            let trimmed = rest.trim_end_matches('/');
            if let Some(pos) = trimmed.rfind('/') {
                return format!("{}{}/", scheme, &trimmed[..pos]);
            }
            return path.to_string(); // at container/bucket root
        }
    }
    // Local path
    std::path::Path::new(path)
        .parent()
        .and_then(|p| {
            let s = p.to_string_lossy();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{BrowserError, Entry, EntryKind, FileBrowser};

    struct StubBackend {
        entries: Vec<Entry>,
    }

    impl FileBrowser for StubBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(self.entries.clone())
        }
    }

    struct ErrorBackend;

    impl FileBrowser for ErrorBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Err(BrowserError::NotFound("not found".to_string()))
        }
    }

    fn make_app(entries: Vec<Entry>) -> BrowserApp {
        BrowserApp::new(
            Box::new(StubBackend { entries }),
            "/test/root".to_string(),
            crate::theme::default_theme(),
        )
    }

    fn file_entry(name: &str) -> Entry {
        Entry {
            kind: crate::browser::classify(name),
            name: name.to_string(),
            path: format!("/test/{}", name),
        }
    }

    fn dir_entry(name: &str) -> Entry {
        Entry {
            name: name.to_string(),
            path: format!("/test/{}", name),
            kind: EntryKind::Dir,
        }
    }

    #[test]
    fn test_new_populates_entries() {
        let app = make_app(vec![file_entry("a.csv"), file_entry("b.csv")]);
        assert_eq!(app.entries.len(), 2);
    }

    #[test]
    fn test_new_cursor_at_zero() {
        let app = make_app(vec![file_entry("a.csv")]);
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn test_navigate_down_increments() {
        let mut app = make_app(vec![file_entry("a.csv"), file_entry("b.csv")]);
        app.navigate_down();
        assert_eq!(app.cursor, 1);
    }

    #[test]
    fn test_navigate_down_clamps_at_end() {
        let mut app = make_app(vec![file_entry("a.csv")]);
        app.navigate_down();
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn test_navigate_up_decrements() {
        let mut app = make_app(vec![file_entry("a.csv"), file_entry("b.csv")]);
        app.cursor = 1;
        app.navigate_up();
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn test_navigate_up_clamps_at_zero() {
        let mut app = make_app(vec![file_entry("a.csv")]);
        app.navigate_up();
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn test_parent_path_local_nested() {
        assert_eq!(parent_path("/home/user/data"), "/home/user");
    }

    #[test]
    fn test_parent_path_local_root_no_op() {
        assert_eq!(parent_path("/"), "/");
    }

    #[test]
    fn test_parent_path_az_nested() {
        assert_eq!(parent_path("az://container/a/b/"), "az://container/a/");
    }

    #[test]
    fn test_parent_path_az_one_level() {
        assert_eq!(parent_path("az://container/a/"), "az://container/");
    }

    #[test]
    fn test_parent_path_az_root_no_op() {
        assert_eq!(parent_path("az://container/"), "az://container/");
    }

    #[test]
    fn test_descend_into_dir_changes_cwd() {
        let mut app = make_app(vec![dir_entry("subdir")]);
        app.descend();
        assert_eq!(app.cwd, "/test/subdir");
    }

    #[test]
    fn test_descend_on_file_does_nothing() {
        let mut app = make_app(vec![file_entry("data.csv")]);
        let old_cwd = app.cwd.clone();
        app.descend();
        assert_eq!(app.cwd, old_cwd);
    }

    #[test]
    fn test_ascend_moves_to_parent() {
        let mut app = BrowserApp::new(
            Box::new(StubBackend { entries: vec![] }),
            "/test/root/child".to_string(),
            crate::theme::default_theme(),
        );
        app.ascend();
        assert_eq!(app.cwd, "/test/root");
    }

    #[test]
    fn test_ascend_no_op_at_local_root() {
        let mut app = BrowserApp::new(
            Box::new(StubBackend { entries: vec![] }),
            "/".to_string(),
            crate::theme::default_theme(),
        );
        app.ascend();
        assert_eq!(app.cwd, "/");
    }

    #[test]
    fn test_new_sets_status_on_list_error() {
        let app = BrowserApp::new(
            Box::new(ErrorBackend),
            "/nonexistent".to_string(),
            crate::theme::default_theme(),
        );
        assert!(app.status.is_some(), "status should be set on list error");
        assert!(app.entries.is_empty(), "entries should be empty on error");
    }

    // ── download ──────────────────────────────────────────────────────────────

    /// Serves object bytes so the full prompt → fetch → write path can be exercised
    /// without a cloud account.
    struct RemoteBackend {
        entries: Vec<Entry>,
        bytes: Vec<u8>,
    }

    impl FileBrowser for RemoteBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(self.entries.clone())
        }
        fn download_bytes(&self, _path: &str) -> Result<Vec<u8>, BrowserError> {
            Ok(self.bytes.clone())
        }
    }

    fn remote_entry(name: &str) -> Entry {
        Entry {
            kind: crate::browser::classify(name),
            name: name.to_string(),
            path: format!("az://c/data/{}", name),
        }
    }

    fn make_remote_app(bytes: &[u8]) -> BrowserApp {
        BrowserApp::new(
            Box::new(RemoteBackend {
                entries: vec![remote_entry("sales.csv")],
                bytes: bytes.to_vec(),
            }),
            "az://c/data/".to_string(),
            crate::theme::default_theme(),
        )
    }

    #[test]
    fn test_begin_download_opens_prompt_for_remote_file() {
        let mut app = make_remote_app(b"a,b\n");
        app.begin_download();
        let prompt = app.download.expect("prompt should open for a remote file");
        assert_eq!(prompt.source, "az://c/data/sales.csv");
        assert_eq!(prompt.input, "sales.csv");
    }

    #[test]
    fn test_begin_download_refuses_local_file() {
        let mut app = make_app(vec![file_entry("a.csv")]);
        app.begin_download();
        assert!(app.download.is_none(), "no prompt for a local file");
        assert!(app.status.unwrap().contains("already a local file"));
    }

    #[test]
    fn test_begin_download_refuses_directory() {
        let mut app = BrowserApp::new(
            Box::new(StubBackend {
                entries: vec![Entry {
                    name: "sub".to_string(),
                    path: "az://c/data/sub/".to_string(),
                    kind: EntryKind::Dir,
                }],
            }),
            "az://c/data/".to_string(),
            crate::theme::default_theme(),
        );
        app.begin_download();
        assert!(app.download.is_none(), "no prompt for a directory");
        assert!(app.status.unwrap().contains("not directories"));
    }

    #[test]
    fn test_confirm_download_writes_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.csv");
        let mut app = make_remote_app(b"a,b\n1,2\n");
        app.begin_download();
        app.download.as_mut().unwrap().input = dest.to_string_lossy().to_string();
        app.confirm_download();
        assert!(app.download.is_none(), "prompt should close after Enter");
        assert_eq!(std::fs::read(&dest).unwrap(), b"a,b\n1,2\n");
        let status = app.status.unwrap();
        assert!(status.contains('✓'), "unexpected status: {}", status);
        assert!(status.contains("8 B"), "unexpected status: {}", status);
    }

    #[test]
    fn test_confirm_download_keeps_prompt_open_on_blank_input() {
        let mut app = make_remote_app(b"a,b\n");
        app.begin_download();
        app.download.as_mut().unwrap().input = "  ".to_string();
        app.confirm_download();
        assert!(
            app.download.is_some(),
            "a blank destination has nothing to write"
        );
        assert!(app.status.is_none());
    }

    // ── fuzzy find ────────────────────────────────────────────────────────────

    fn find_app() -> BrowserApp {
        make_app(vec![
            file_entry("orders.csv"),
            file_entry("sales.parquet"),
            file_entry("notes.txt"),
        ])
    }

    /// Type a whole query, one keystroke at a time — the way the event loop does.
    fn type_query(app: &mut BrowserApp, query: &str) {
        app.open_find();
        for c in query.chars() {
            app.find_push(c);
        }
    }

    #[test]
    fn test_new_shows_every_entry() {
        let app = find_app();
        assert_eq!(
            app.matches.len(),
            3,
            "an unfiltered listing shows all of it"
        );
    }

    #[test]
    fn test_opening_the_prompt_does_not_filter_anything_yet() {
        let mut app = find_app();
        app.open_find();
        assert_eq!(app.matches.len(), 3);
    }

    #[test]
    fn test_query_narrows_the_listing() {
        let mut app = find_app();
        type_query(&mut app, "sal");
        assert_eq!(app.matches.len(), 1);
        assert_eq!(app.selected_entry().unwrap().name, "sales.parquet");
    }

    #[test]
    fn test_cursor_returns_to_the_best_match_on_every_keystroke() {
        let mut app = find_app();
        app.open_find();
        app.find_push('s');
        app.navigate_down();
        assert_eq!(app.cursor, 1);
        app.find_push('a');
        assert_eq!(app.cursor, 0, "a new query means a new best match");
    }

    #[test]
    fn test_navigation_is_bounded_by_the_matches_not_the_listing() {
        let mut app = find_app();
        type_query(&mut app, "sal");
        app.navigate_down();
        assert_eq!(app.cursor, 0, "one match, nowhere to go");
        assert_eq!(app.selected_entry().unwrap().name, "sales.parquet");
    }

    #[test]
    fn test_closing_the_prompt_restores_the_listing_under_the_same_entry() {
        let mut app = find_app();
        type_query(&mut app, "sal");
        app.close_find();
        assert!(app.find.is_none());
        assert_eq!(app.matches.len(), 3, "the full listing is back");
        assert_eq!(
            app.selected_entry().unwrap().name,
            "sales.parquet",
            "the entry that was found stays under the cursor"
        );
    }

    #[test]
    fn test_backspace_reopens_what_it_narrowed() {
        let mut app = make_app(vec![file_entry("sales.parquet"), file_entry("sample.txt")]);
        type_query(&mut app, "sam");
        assert_eq!(app.matches.len(), 1, "only sample has an m");
        app.find_backspace();
        assert_eq!(app.matches.len(), 2, "sa is back to matching both");
    }

    #[test]
    fn test_backspace_on_an_empty_query_closes_the_prompt() {
        let mut app = find_app();
        app.open_find();
        app.find_backspace();
        assert!(app.find.is_none(), "nothing left to delete means 'out'");
        assert_eq!(app.matches.len(), 3);
    }

    #[test]
    fn test_clearing_the_query_keeps_the_prompt_open() {
        let mut app = find_app();
        type_query(&mut app, "sal");
        app.find_clear();
        assert!(
            app.find.is_some(),
            "ctrl-u clears the query, not the prompt"
        );
        assert_eq!(app.matches.len(), 3);
    }

    #[test]
    fn test_a_query_matching_nothing_leaves_no_selection() {
        let mut app = find_app();
        type_query(&mut app, "zzz");
        assert!(app.matches.is_empty());
        assert!(
            app.selected_entry().is_none(),
            "nothing shown means nothing selected"
        );
    }

    #[test]
    fn test_descending_clears_the_filter() {
        let mut app = make_app(vec![dir_entry("subdir")]);
        type_query(&mut app, "sub");
        app.descend();
        assert!(app.find.is_none(), "a query belongs to one listing");
        assert_eq!(app.cwd, "/test/subdir");
    }

    #[test]
    fn test_download_targets_the_filtered_selection() {
        let mut app = BrowserApp::new(
            Box::new(StubBackend {
                entries: vec![remote_entry("orders.csv"), remote_entry("sales.csv")],
            }),
            "az://c/data/".to_string(),
            crate::theme::default_theme(),
        );
        type_query(&mut app, "sal");
        app.begin_download();
        assert_eq!(
            app.download.expect("prompt should open").source,
            "az://c/data/sales.csv",
            "the download follows the filter, not the raw listing index"
        );
    }

    #[test]
    fn test_parent_path_s3_nested() {
        assert_eq!(parent_path("s3://bucket/a/b/"), "s3://bucket/a/");
    }

    #[test]
    fn test_parent_path_s3_one_level() {
        assert_eq!(parent_path("s3://bucket/a/"), "s3://bucket/");
    }

    #[test]
    fn test_parent_path_s3_root_no_op() {
        assert_eq!(parent_path("s3://bucket/"), "s3://bucket/");
    }
}
