use crate::app::App;
use crate::browser::{Entry, FileBrowser};
use crate::theme::Theme;

pub struct BrowserApp {
    pub backend: Box<dyn FileBrowser>,
    pub entries: Vec<Entry>,
    pub cursor: usize,
    pub cwd: String,
    pub viewer: Option<App>,
    pub browser_visible: bool,
    pub focus: Focus,
    pub status: Option<String>,
    pub should_quit: bool,
    pub theme: &'static Theme,
}

#[derive(Debug, PartialEq)]
pub enum Focus {
    Browser,
    Viewer,
}

impl BrowserApp {
    pub fn new(
        backend: Box<dyn FileBrowser>,
        root_path: String,
        theme: &'static Theme,
    ) -> Self {
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
            if entry.is_dir {
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
    use crate::browser::{BrowserError, Entry, FileBrowser};

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
            name: name.to_string(),
            path: format!("/test/{}", name),
            is_dir: false,
        }
    }

    fn dir_entry(name: &str) -> Entry {
        Entry {
            name: name.to_string(),
            path: format!("/test/{}", name),
            is_dir: true,
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
