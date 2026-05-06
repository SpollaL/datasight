use crate::browser::{is_supported, BrowserError, Entry, FileBrowser};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    rt: tokio::runtime::Runtime,
}

impl S3Backend {
    /// `rest` is everything after the `s3://` prefix, e.g. `"my-bucket/prefix/"`.
    pub fn new(rest: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        let (bucket, _) = rest.split_once('/').unwrap_or((rest, ""));
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| format!("S3: {}", e))?;
        Ok(Self {
            store: Arc::new(store),
            bucket: bucket.to_string(),
            rt,
        })
    }
}

impl FileBrowser for S3Backend {
    fn list(&self, prefix: &str) -> Result<Vec<Entry>, BrowserError> {
        let rest = prefix.strip_prefix("s3://").unwrap_or(prefix);
        let obj_prefix = rest
            .strip_prefix(&self.bucket)
            .unwrap_or("")
            .trim_matches('/');
        let obj_path = if obj_prefix.is_empty() {
            Path::from("")
        } else {
            Path::from(obj_prefix)
        };

        let list_result = self
            .rt
            .block_on(async { self.store.list_with_delimiter(Some(&obj_path)).await })
            .map_err(|e| BrowserError::Network(e.to_string()))?;

        let mut entries: Vec<Entry> = Vec::new();

        for dir_path in list_result.common_prefixes {
            let name = dir_path
                .filename()
                .unwrap_or("")
                .trim_end_matches('/')
                .to_string();
            if name.is_empty() {
                continue;
            }
            entries.push(Entry {
                name: name.clone(),
                path: format!("s3://{}/{}/", self.bucket, dir_path),
                is_dir: true,
            });
        }

        for obj in list_result.objects {
            let name = obj.location.filename().unwrap_or("").to_string();
            if name.is_empty() || !is_supported(&name) {
                continue;
            }
            entries.push(Entry {
                name: name.clone(),
                path: format!("s3://{}/{}", self.bucket, obj.location),
                is_dir: false,
            });
        }

        entries.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then(a.name.cmp(&b.name)));
        Ok(entries)
    }

    fn download_bytes(&self, path: &str) -> Result<Vec<u8>, BrowserError> {
        let rest = path.strip_prefix("s3://").unwrap_or(path);
        let obj_path_str = rest
            .strip_prefix(&self.bucket)
            .unwrap_or("")
            .trim_start_matches('/');
        let obj_path = Path::from(obj_path_str);

        let bytes = self
            .rt
            .block_on(async { self.store.get(&obj_path).await?.bytes().await })
            .map_err(|e| BrowserError::Network(e.to_string()))?;

        Ok(bytes.to_vec())
    }
}
