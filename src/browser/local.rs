use super::{FileBrowser, BrowserError, Entry};

pub struct LocalBackend;

impl FileBrowser for LocalBackend {
    fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
        // Stub: will be implemented in Task 3
        Err(BrowserError::Other("list not implemented yet".to_string()))
    }
}
