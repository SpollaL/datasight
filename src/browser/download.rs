//! Saving a remote object from the browser to a local file.
//!
//! Downloading is remote-only: a local entry is already on disk, so the prompt is
//! never offered for one. [`resolve_dest`] turns the typed destination into the path
//! that will actually be written — it is the only place that decides what a bare
//! directory or a `~` means — and [`download_to`] is the only part that touches the
//! network and the filesystem.

use crate::browser::{is_remote, FileBrowser};
use crate::theme::Theme;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use std::path::{Path, PathBuf};

/// Fallback name when the remote URI has no usable last segment.
const FALLBACK_NAME: &str = "download";

/// Destination prompt for one pending download. Gated by an `Option` on
/// [`crate::browser::app::BrowserApp`] rather than a mode variant, the same way the
/// theme picker is.
pub struct DownloadPrompt {
    /// Remote URI being saved.
    pub source: String,
    /// Destination as typed so far.
    pub input: String,
}

impl DownloadPrompt {
    /// Open the prompt for `source`, pre-filled with its remote file name so `Enter`
    /// alone saves into the current directory under the name it already has.
    pub fn open(source: &str) -> Self {
        Self {
            source: source.to_string(),
            input: default_filename(source),
        }
    }
}

/// The remote file name of `source`: `az://c/data/sales.csv` → `sales.csv`.
///
/// Everything before the last separator is dropped, so the suggestion is always a
/// name in the current directory rather than an unwritable remote path. A URI with
/// nothing after the container (`az://c/`) names a container, not an object, and has
/// no file name to keep.
pub fn default_filename(source: &str) -> String {
    let rest = source.split_once("://").map_or(source, |(_, rest)| rest);
    rest.trim_end_matches('/')
        .rsplit_once('/')
        .map(|(_, name)| name)
        .filter(|name| !name.is_empty())
        .unwrap_or(FALLBACK_NAME)
        .to_string()
}

/// Resolve typed input to the local file that will be written. Returns `None` for
/// blank input.
///
/// A leading `~` expands to the home directory. A destination that names a directory
/// — either an existing one or anything written with a trailing separator — keeps the
/// remote file name, so typing `~/Downloads/` is enough. Unlike the CSV export the
/// extension is left exactly as typed: the bytes are a copy of the remote object, and
/// only its own name describes them correctly.
pub fn resolve_dest(input: &str, source: &str) -> Option<PathBuf> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let expanded = expand_tilde(trimmed);
    let names_dir = trimmed.ends_with('/') || trimmed.ends_with(std::path::MAIN_SEPARATOR);
    Some(if names_dir || expanded.is_dir() {
        expanded.join(default_filename(source))
    } else {
        expanded
    })
}

/// Expand a leading `~` (alone or before a separator) to the home directory.
/// `~user` forms and paths without a leading `~` are returned as typed.
fn expand_tilde(input: &str) -> PathBuf {
    match input
        .strip_prefix('~')
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .zip(dirs::home_dir())
    {
        Some((rest, home)) => home.join(rest.trim_start_matches('/')),
        None => PathBuf::from(input),
    }
}

/// Fetch `source` through `backend` and write it to `dest`. Returns the byte count.
///
/// Blocking, and the whole object is buffered in memory before the write — the same
/// way the viewer already loads a remote file, so a very large object freezes the UI
/// until it lands.
pub fn download_to(backend: &dyn FileBrowser, source: &str, dest: &Path) -> Result<u64, String> {
    // `File::create` on a remote URI fails with a bare "No such file or directory",
    // and typing one here is an easy mistake to make while browsing a bucket.
    if dest.to_str().is_some_and(is_remote) {
        return Err("destination must be a local path".to_string());
    }
    let bytes = backend.download_bytes(source).map_err(|e| e.to_string())?;
    std::fs::write(dest, &bytes).map_err(|e| e.to_string())?;
    Ok(bytes.len() as u64)
}

/// A byte count as a short human-readable string: `812 B`, `41.2 KiB`, `1.4 GiB`.
pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.1} {}", size, UNITS[unit])
    }
}

/// The bottom-bar prompt, replacing the shortcut bar while a download is pending.
/// Renders the resolved destination so the user sees the final path — including a
/// directory input rewritten to keep the remote name — before committing to it.
pub fn prompt_line(prompt: &DownloadPrompt, theme: &Theme) -> Line<'static> {
    // Resolving each frame stats the destination once per keystroke, which is free at
    // typing speed and puts the overwrite warning where the decision is made.
    let (text, bg) = match resolve_dest(&prompt.input, &prompt.source) {
        Some(dest) if dest.exists() => (
            format!(" d {}_ → {} — ⚠ overwrites ", prompt.input, dest.display()),
            theme.warn,
        ),
        Some(dest) => (
            format!(" d {}_ → {} ", prompt.input, dest.display()),
            theme.info,
        ),
        None => (" d _ (type a destination) ".to_string(), theme.info),
    };
    Line::from(Span::styled(
        text,
        Style::default()
            .bg(bg)
            .fg(theme.bg)
            .add_modifier(Modifier::BOLD),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filename_takes_the_last_segment() {
        assert_eq!(default_filename("az://c/data/sales.csv"), "sales.csv");
        assert_eq!(
            default_filename("s3://bucket/orders.parquet"),
            "orders.parquet"
        );
    }

    #[test]
    fn default_filename_falls_back_without_a_segment() {
        // A container root has no object name to keep.
        assert_eq!(default_filename("az://container/"), FALLBACK_NAME);
        assert_eq!(default_filename(""), FALLBACK_NAME);
    }

    #[test]
    fn prompt_opens_on_the_remote_file_name() {
        let prompt = DownloadPrompt::open("az://c/data/sales.csv");
        assert_eq!(prompt.input, "sales.csv");
        assert_eq!(prompt.source, "az://c/data/sales.csv");
    }

    #[test]
    fn resolve_dest_keeps_a_typed_file_name() {
        assert_eq!(
            resolve_dest("out.csv", "az://c/sales.csv").unwrap(),
            PathBuf::from("out.csv")
        );
    }

    #[test]
    fn resolve_dest_keeps_the_extension_as_typed() {
        // Unlike the CSV export, nothing is appended: the bytes are whatever the
        // remote object holds.
        assert_eq!(
            resolve_dest("out.bin", "az://c/sales.parquet").unwrap(),
            PathBuf::from("out.bin")
        );
    }

    #[test]
    fn resolve_dest_appends_the_remote_name_to_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let typed = dir.path().to_str().unwrap();
        assert_eq!(
            resolve_dest(typed, "az://c/data/sales.csv").unwrap(),
            dir.path().join("sales.csv")
        );
    }

    #[test]
    fn resolve_dest_treats_a_trailing_separator_as_a_directory() {
        // The directory need not exist yet — the trailing slash is the intent.
        assert_eq!(
            resolve_dest("exports/", "az://c/sales.csv").unwrap(),
            PathBuf::from("exports/sales.csv")
        );
    }

    #[test]
    fn resolve_dest_trims_input_and_rejects_blank() {
        assert_eq!(
            resolve_dest("  out.csv  ", "az://c/sales.csv").unwrap(),
            PathBuf::from("out.csv")
        );
        assert!(resolve_dest("   ", "az://c/sales.csv").is_none());
        assert!(resolve_dest("", "az://c/sales.csv").is_none());
    }

    #[test]
    fn expand_tilde_resolves_the_home_directory() {
        let home = dirs::home_dir().expect("invariant: tests run with a home directory");
        assert_eq!(expand_tilde("~"), home);
        assert_eq!(expand_tilde("~/out.csv"), home.join("out.csv"));
        // `~user` is not a path this expands — leave it for the OS to reject.
        assert_eq!(
            expand_tilde("~root/out.csv"),
            PathBuf::from("~root/out.csv")
        );
        assert_eq!(expand_tilde("./out.csv"), PathBuf::from("./out.csv"));
    }

    #[test]
    fn human_size_scales_the_unit() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(812), "812 B");
        assert_eq!(human_size(42_188), "41.2 KiB");
        assert_eq!(human_size(1_503_238_553), "1.4 GiB");
    }

    // ── download_to ───────────────────────────────────────────────────────────

    use crate::browser::{BrowserError, Entry};

    struct StubBackend {
        bytes: Vec<u8>,
    }

    impl FileBrowser for StubBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(vec![])
        }
        fn download_bytes(&self, _path: &str) -> Result<Vec<u8>, BrowserError> {
            Ok(self.bytes.clone())
        }
    }

    struct FailingBackend;

    impl FileBrowser for FailingBackend {
        fn list(&self, _prefix: &str) -> Result<Vec<Entry>, BrowserError> {
            Ok(vec![])
        }
        fn download_bytes(&self, _path: &str) -> Result<Vec<u8>, BrowserError> {
            Err(BrowserError::NotFound("no such blob".to_string()))
        }
    }

    #[test]
    fn download_to_writes_the_fetched_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("sales.csv");
        let backend = StubBackend {
            bytes: b"a,b\n1,2\n".to_vec(),
        };
        let written = download_to(&backend, "az://c/sales.csv", &dest).unwrap();
        assert_eq!(written, 8);
        assert_eq!(std::fs::read(&dest).unwrap(), b"a,b\n1,2\n");
    }

    #[test]
    fn download_to_refuses_a_remote_destination() {
        let backend = StubBackend { bytes: vec![] };
        for dest in ["az://c/out.csv", "s3://b/out.csv"] {
            let err = download_to(&backend, "az://c/sales.csv", Path::new(dest)).unwrap_err();
            assert!(err.contains("local path"), "unexpected error: {}", err);
        }
    }

    #[test]
    fn download_to_reports_a_backend_failure_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("sales.csv");
        let err = download_to(&FailingBackend, "az://c/gone.csv", &dest).unwrap_err();
        assert!(err.contains("no such blob"), "unexpected error: {}", err);
        assert!(!dest.exists(), "no file should be created on failure");
    }
}
