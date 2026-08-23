pub mod app;
pub mod download;
pub mod events;
pub mod find;
pub mod local;
pub mod ui;

#[cfg(feature = "azure")]
pub mod azure;

#[cfg(feature = "aws")]
pub mod s3;

use polars::prelude::SerReader;
use std::fmt;

pub trait FileBrowser {
    fn list(&self, prefix: &str) -> Result<Vec<Entry>, BrowserError>;

    /// Download raw bytes for a single file path. Default returns an error.
    /// Cloud backends override this.
    fn download_bytes(&self, path: &str) -> Result<Vec<u8>, BrowserError> {
        Err(BrowserError::Other(format!(
            "download_bytes not implemented for path: {}",
            path
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    /// Tabular file the polars-backed viewer can open.
    Data,
    /// Probably-text file: opened in the text viewer with a UTF-8 sniff.
    Text,
    /// Known-binary extension; rendered grayed out and refused at open time.
    Binary,
}

#[derive(Debug, Clone)]
pub struct Entry {
    /// Display name — last path segment only.
    pub name: String,
    /// Full URI, e.g. "az://container/data/sales.csv" or "/home/user/data.csv".
    pub path: String,
    pub kind: EntryKind,
}

impl Entry {
    pub fn is_dir(&self) -> bool {
        self.kind == EntryKind::Dir
    }
}

#[derive(Debug)]
pub enum BrowserError {
    #[allow(dead_code)]
    Auth(String),
    #[allow(dead_code)]
    Network(String),
    NotFound(String),
    Other(String),
}

impl fmt::Display for BrowserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrowserError::Auth(s) => write!(f, "auth error: {}", s),
            BrowserError::Network(s) => write!(f, "network error: {}", s),
            BrowserError::NotFound(s) => write!(f, "not found: {}", s),
            BrowserError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for BrowserError {}

pub const DATA_EXTENSIONS: &[&str] = &["csv", "tsv", "parquet", "json", "ndjson", "jsonl"];

/// Extensions classified as binary — rendered grayed out, refused at open time.
/// Anything outside both lists falls into [`EntryKind::Text`] and is sniffed
/// for UTF-8 content when the user opens it.
pub const BINARY_EXTENSIONS: &[&str] = &[
    // Executables / compiled artifacts
    "exe", "bin", "so", "dll", "dylib", "o", "a", "out", "class", "jar", "wasm",
    // Images
    "png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "tiff", "svgz", // Audio / video
    "mp3", "mp4", "wav", "mov", "avi", "mkv", "flac", "ogg", "webm", "m4a", "m4v",
    // Archives
    "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", // Documents / fonts
    "pdf", "ttf", "otf", "woff", "woff2", "eot", // Local databases
    "db", "sqlite",
];

/// Classify a file by name. Comparison is case-insensitive on the extension.
/// Files without a recognised extension fall through to [`EntryKind::Text`].
pub fn classify(name: &str) -> EntryKind {
    let ext = name
        .rsplit('.')
        .next()
        .filter(|e| *e != name)
        .map(|e| e.to_lowercase());
    match ext.as_deref() {
        Some(e) if DATA_EXTENSIONS.contains(&e) => EntryKind::Data,
        Some(e) if BINARY_EXTENSIONS.contains(&e) => EntryKind::Binary,
        _ => EntryKind::Text,
    }
}

/// Whether `path` is a cloud URI rather than a local filesystem path.
pub fn is_remote(path: &str) -> bool {
    path.starts_with("az://") || path.starts_with("s3://")
}

/// Detect the URI scheme and construct the appropriate backend.
/// Returns (backend, resolved_root_path).
pub fn build_backend(path: &str) -> Result<(Box<dyn FileBrowser>, String), String> {
    if path.starts_with("az://") {
        #[cfg(feature = "azure")]
        {
            let rest = path.strip_prefix("az://").unwrap_or("");
            let backend = azure::AzureBackend::new(rest)?;
            return Ok((Box::new(backend), path.to_string()));
        }
        #[cfg(not(feature = "azure"))]
        return Err(
            "az:// paths require the 'azure' feature (rebuild with --features azure)".into(),
        );
    }
    if path.starts_with("s3://") {
        #[cfg(feature = "aws")]
        {
            let rest = path.strip_prefix("s3://").unwrap_or("");
            let backend = s3::S3Backend::new(rest)?;
            return Ok((Box::new(backend), path.to_string()));
        }
        #[cfg(not(feature = "aws"))]
        return Err("s3:// paths require the 'aws' feature (rebuild with --features aws)".into());
    }
    // Local path — canonicalize to an absolute path.
    let canonical = std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().to_string())
        .map_err(|e| format!("cannot access '{}': {}", path, e))?;
    Ok((Box::new(local::LocalBackend), canonical))
}

/// Load a file (local or cloud) into a DataFrame for the browser viewer.
pub(crate) fn load_file_for_browser(
    path: &str,
    backend: &dyn FileBrowser,
) -> Result<(polars::prelude::DataFrame, String), Box<dyn std::error::Error>> {
    if is_remote(path) {
        let bytes = backend.download_bytes(path).map_err(|e| e.to_string())?;
        let ext = path.rsplit('.').next().unwrap_or("").to_lowercase();
        let df = if ext == "parquet" {
            polars::prelude::ParquetReader::new(std::io::Cursor::new(bytes)).finish()?
        } else {
            crate::parse_buf(bytes, None)?
        };
        Ok((crate::try_parse_date_columns(df), path.to_string()))
    } else {
        crate::load_dataframe(path, None).map(|df| (df, path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_csv_is_data() {
        assert_eq!(classify("orders.csv"), EntryKind::Data);
    }

    #[test]
    fn test_classify_parquet_is_data() {
        assert_eq!(classify("data.parquet"), EntryKind::Data);
    }

    #[test]
    fn test_classify_tsv_is_data() {
        assert_eq!(classify("data.tsv"), EntryKind::Data);
    }

    #[test]
    fn test_classify_ndjson_is_data() {
        assert_eq!(classify("data.ndjson"), EntryKind::Data);
    }

    #[test]
    fn test_classify_jsonl_is_data() {
        assert_eq!(classify("data.jsonl"), EntryKind::Data);
    }

    #[test]
    fn test_classify_json_is_data() {
        assert_eq!(classify("events.json"), EntryKind::Data);
    }

    #[test]
    fn test_classify_png_is_binary() {
        assert_eq!(classify("photo.png"), EntryKind::Binary);
    }

    #[test]
    fn test_classify_zip_is_binary() {
        assert_eq!(classify("archive.zip"), EntryKind::Binary);
    }

    #[test]
    fn test_classify_pdf_is_binary() {
        assert_eq!(classify("doc.pdf"), EntryKind::Binary);
    }

    #[test]
    fn test_classify_xlsx_is_text() {
        // .xlsx isn't in the binary denylist so it falls through to Text;
        // the open-time UTF-8 sniff will reject it as not-text.
        assert_eq!(classify("report.xlsx"), EntryKind::Text);
    }

    #[test]
    fn test_classify_txt_is_text() {
        assert_eq!(classify("notes.txt"), EntryKind::Text);
    }

    #[test]
    fn test_classify_md_is_text() {
        assert_eq!(classify("README.md"), EntryKind::Text);
    }

    #[test]
    fn test_classify_yaml_is_text() {
        assert_eq!(classify("config.yaml"), EntryKind::Text);
    }

    #[test]
    fn test_classify_log_is_text() {
        assert_eq!(classify("server.log"), EntryKind::Text);
    }

    #[test]
    fn test_classify_no_ext_is_text() {
        assert_eq!(classify("README"), EntryKind::Text);
    }

    #[test]
    fn test_classify_uppercase_extension() {
        assert_eq!(classify("DATA.CSV"), EntryKind::Data);
        assert_eq!(classify("PHOTO.PNG"), EntryKind::Binary);
    }

    #[test]
    fn test_build_backend_local_bare_path() {
        let result = build_backend("tests/fixtures");
        assert!(result.is_ok());
        let (_, resolved) = result.unwrap();
        assert!(
            std::path::Path::new(&resolved).is_absolute(),
            "resolved path should be absolute"
        );
    }

    #[test]
    fn test_build_backend_az_without_feature_errors() {
        #[cfg(not(feature = "azure"))]
        {
            let result = build_backend("az://my-container/");
            assert!(result.is_err());
            if let Err(e) = result {
                assert!(e.contains("azure"));
            }
        }
    }

    #[test]
    fn test_build_backend_s3_without_feature_errors() {
        #[cfg(not(feature = "aws"))]
        {
            let result = build_backend("s3://my-bucket/");
            assert!(result.is_err());
            if let Err(e) = result {
                assert!(e.contains("aws"));
            }
        }
    }
}
