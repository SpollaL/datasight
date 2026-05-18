use crate::browser::{classify, BrowserError, Entry, EntryKind, FileBrowser};
use std::fs;

pub struct LocalBackend;

impl FileBrowser for LocalBackend {
    fn list(&self, prefix: &str) -> Result<Vec<Entry>, BrowserError> {
        // Canonicalize the prefix to ensure we get absolute paths
        let abs = fs::canonicalize(prefix).map_err(|e| BrowserError::NotFound(e.to_string()))?;
        let read_dir = fs::read_dir(&abs).map_err(|e| BrowserError::NotFound(e.to_string()))?;

        let mut dirs: Vec<Entry> = Vec::new();
        let mut files: Vec<Entry> = Vec::new();

        for result in read_dir.flatten() {
            let name = result.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            let path = result.path().to_string_lossy().to_string();
            let is_dir = result.file_type().map(|t| t.is_dir()).unwrap_or(false);

            if is_dir {
                dirs.push(Entry {
                    name,
                    path,
                    kind: EntryKind::Dir,
                });
            } else {
                files.push(Entry {
                    kind: classify(&name),
                    name,
                    path,
                });
            }
        }

        dirs.sort_by(|a, b| a.name.cmp(&b.name));
        files.sort_by(|a, b| a.name.cmp(&b.name));
        dirs.extend(files);
        Ok(dirs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_fixtures_classifies_data_files() {
        let backend = LocalBackend;
        let entries = backend.list("tests/fixtures").expect("list should succeed");
        let csv = entries
            .iter()
            .find(|e| e.name == "orders.csv")
            .expect("orders.csv missing");
        assert_eq!(csv.kind, EntryKind::Data);
    }

    #[test]
    fn test_list_fixtures_contains_orders_csv() {
        let backend = LocalBackend;
        let entries = backend.list("tests/fixtures").expect("list should succeed");
        assert!(
            entries
                .iter()
                .any(|e| e.name == "orders.csv" && !e.is_dir()),
            "orders.csv not found in listing"
        );
    }

    #[test]
    fn test_list_dirs_come_before_files() {
        let tmp = std::env::temp_dir().join("datasight_test_list");
        let _ = std::fs::remove_dir_all(&tmp); // clean before setup
        let sub = tmp.join("subdir");
        let _ = std::fs::create_dir_all(&sub);
        let _ = std::fs::write(tmp.join("data.csv"), "a,b\n1,2");
        let backend = LocalBackend;
        let entries = backend
            .list(tmp.to_str().unwrap())
            .expect("list should succeed");
        assert_eq!(entries.len(), 2, "expected exactly 1 dir + 1 file");
        assert!(entries[0].is_dir(), "directory should appear before file");
        assert!(!entries[1].is_dir(), "file should appear after directory");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_list_hidden_files_excluded() {
        let tmp = std::env::temp_dir().join("datasight_test_hidden");
        let _ = std::fs::remove_dir_all(&tmp); // clean before setup
        let _ = std::fs::create_dir_all(&tmp);
        let _ = std::fs::write(tmp.join(".hidden.csv"), "a,b\n1,2");
        let _ = std::fs::write(tmp.join("visible.csv"), "a,b\n1,2");
        let backend = LocalBackend;
        let entries = backend
            .list(tmp.to_str().unwrap())
            .expect("list should succeed");
        assert!(
            entries.iter().any(|e| e.name == "visible.csv"),
            "visible.csv should be present in listing"
        );
        assert!(
            entries.iter().all(|e| !e.name.starts_with('.')),
            "hidden files should be excluded"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_list_includes_text_and_binary_files() {
        let tmp = std::env::temp_dir().join("datasight_test_kinds");
        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::create_dir_all(&tmp);
        let _ = std::fs::write(tmp.join("data.csv"), "a,b\n1,2");
        let _ = std::fs::write(tmp.join("notes.txt"), "hi");
        let _ = std::fs::write(tmp.join("photo.png"), [0u8, 1, 2, 3]);
        let backend = LocalBackend;
        let entries = backend
            .list(tmp.to_str().unwrap())
            .expect("list should succeed");
        let by_name = |n: &str| entries.iter().find(|e| e.name == n).map(|e| e.kind);
        assert_eq!(by_name("data.csv"), Some(EntryKind::Data));
        assert_eq!(by_name("notes.txt"), Some(EntryKind::Text));
        assert_eq!(by_name("photo.png"), Some(EntryKind::Binary));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_list_nonexistent_path_errors() {
        let backend = LocalBackend;
        let result = backend.list("/nonexistent/path/that/does/not/exist");
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_path_is_absolute() {
        let backend = LocalBackend;
        let entries = backend.list("tests/fixtures").expect("list should succeed");
        for e in &entries {
            assert!(
                std::path::Path::new(&e.path).is_absolute(),
                "entry path should be absolute: {}",
                e.path
            );
        }
    }
}
