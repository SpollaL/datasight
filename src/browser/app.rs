use crate::app::App;
use crate::browser::download::{self, DownloadPrompt};
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
        }
    }

    pub fn navigate_down(&mut self) {
        if self.cursor + 1 < self.entries.len() {
            self.cursor += 1;
        }
    }

    pub fn navigate_up(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    /// Descend into the directory at the current cursor (no-op if it's a file).
    pub fn descend(&mut self) {
        if let Some(entry) = self.entries.get(self.cursor) {
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
        let Some(entry) = self.entries.get(self.cursor) else {
            return;
        };
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

    fn refresh_listing(&mut self, path: String) {
        match self.backend.list(&path) {
            Ok(entries) => {
                self.entries = entries;
                self.cwd = path;
                self.cursor = 0;
                self.status = None;
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
