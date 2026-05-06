pub mod app;
pub mod events;
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

#[derive(Debug, Clone)]
pub struct Entry {
    /// Display name — last path segment only.
    pub name: String,
    /// Full URI, e.g. "az://container/data/sales.csv" or "/home/user/data.csv".
    pub path: String,
    pub is_dir: bool,
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

pub const SUPPORTED_EXTENSIONS: &[&str] = &["csv", "tsv", "parquet", "json", "ndjson", "jsonl"];

pub fn is_supported(name: &str) -> bool {
    let lower = name.to_lowercase();
    SUPPORTED_EXTENSIONS
        .iter()
        .any(|ext| lower.ends_with(&format!(".{}", ext)))
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
    if path.starts_with("az://") || path.starts_with("s3://") {
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
    fn test_is_supported_csv() {
        assert!(is_supported("orders.csv"));
    }

    #[test]
    fn test_is_supported_parquet() {
        assert!(is_supported("data.parquet"));
    }

    #[test]
    fn test_is_supported_tsv() {
        assert!(is_supported("data.tsv"));
    }

    #[test]
    fn test_is_supported_ndjson() {
        assert!(is_supported("data.ndjson"));
    }

    #[test]
    fn test_is_supported_jsonl() {
        assert!(is_supported("data.jsonl"));
    }

    #[test]
    fn test_is_supported_json() {
        assert!(is_supported("events.json"));
    }

    #[test]
    fn test_is_supported_rejects_xlsx() {
        assert!(!is_supported("report.xlsx"));
    }

    #[test]
    fn test_is_supported_rejects_no_ext() {
        assert!(!is_supported("README"));
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
