use crate::browser::{classify, BrowserError, Entry, EntryKind, FileBrowser};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    container: String,
    rt: tokio::runtime::Runtime,
}

impl AzureBackend {
    /// `rest` is everything after the `az://` prefix, e.g. `"my-container/prefix/"`.
    pub fn new(rest: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        let (container, _) = rest.split_once('/').unwrap_or((rest, ""));
        let base = if let Ok(conn_str) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            builder_from_connection_string(&conn_str)
        } else {
            MicrosoftAzureBuilder::from_env()
        };
        let store = base
            .with_container_name(container)
            .build()
            .map_err(|e| format!("Azure: {}", e))?;
        Ok(Self {
            store: Arc::new(store),
            container: container.to_string(),
            rt,
        })
    }
}

/// Parse an Azure connection string (`Key=Value;...`) into a builder.
/// Values may contain `=` (e.g. base64 account keys), so we split on the first `=` only.
/// For HTTP BlobEndpoint (Azurite / emulator), HTTP is explicitly allowed.
fn builder_from_connection_string(conn_str: &str) -> MicrosoftAzureBuilder {
    let parts: std::collections::HashMap<String, String> = conn_str
        .split(';')
        .filter_map(|seg| {
            let pos = seg.find('=')?;
            Some((seg[..pos].to_lowercase(), seg[pos + 1..].to_string()))
        })
        .collect();

    let mut b = MicrosoftAzureBuilder::new();
    if let Some(name) = parts.get("accountname") {
        b = b.with_account(name);
    }
    if let Some(key) = parts.get("accountkey") {
        b = b.with_access_key(key);
    }
    if let Some(endpoint) = parts.get("blobendpoint") {
        let allow_http = endpoint.starts_with("http://");
        b = b
            .with_endpoint(endpoint.clone())
            .with_allow_http(allow_http);
    }
    b
}

impl FileBrowser for AzureBackend {
    fn list(&self, prefix: &str) -> Result<Vec<Entry>, BrowserError> {
        let rest = prefix.strip_prefix("az://").unwrap_or(prefix);
        let obj_prefix = rest
            .strip_prefix(&self.container)
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
                path: format!("az://{}/{}/", self.container, dir_path),
                kind: EntryKind::Dir,
            });
        }

        for obj in list_result.objects {
            let name = obj.location.filename().unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            entries.push(Entry {
                kind: classify(&name),
                path: format!("az://{}/{}", self.container, obj.location),
                name,
            });
        }

        entries.sort_by(|a, b| b.is_dir().cmp(&a.is_dir()).then(a.name.cmp(&b.name)));
        Ok(entries)
    }

    fn download_bytes(&self, path: &str) -> Result<Vec<u8>, BrowserError> {
        let rest = path.strip_prefix("az://").unwrap_or(path);
        let obj_path_str = rest
            .strip_prefix(&self.container)
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
