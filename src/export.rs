//! Writing the current view to a CSV file.
//!
//! The export always writes CSV. [`resolve_path`] appends the extension when it is
//! missing, so a typo cannot produce a `.parquet` file full of comma-separated text —
//! which removes the whole class of "unsupported format" errors from the input path.
//!
//! [`resolve_path`] depends only on the typed string and `$HOME`; [`write_csv`] is the
//! only part that touches the filesystem. Callers render the resolved path so the user
//! sees the final name before committing to it.

use polars::prelude::*;
use std::path::{Path, PathBuf};

/// Fallback name when the source has no usable stem (stdin, `/`).
const FALLBACK_NAME: &str = "export.csv";

/// Suggested export name for `source`: `orders.csv` → `orders.export.csv`.
///
/// Only the basename is kept, so a remote source (`az://bucket/orders.parquet`)
/// resolves to a writable name in the current directory rather than an unwritable
/// remote one.
pub fn default_filename(source: &str) -> String {
    Path::new(source)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| *stem != crate::config::STDIN_LABEL)
        .map(|stem| format!("{}.export.csv", stem))
        .unwrap_or_else(|| FALLBACK_NAME.to_string())
}

/// Resolve typed input to the path that will actually be written: expand a leading
/// `~`, then append `.csv` unless it is already there. Returns `None` for blank input.
///
/// The prompt looks like a shell path input but no shell is involved, so `~` would
/// otherwise become a literal directory component.
pub fn resolve_path(input: &str) -> Option<PathBuf> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Expand before appending, so a tilde never survives into a filename.
    let path = expand_home(trimmed);
    if path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
    {
        return Some(path);
    }
    let mut name = path.into_os_string();
    name.push(".csv");
    Some(PathBuf::from(name))
}

/// Replace a leading `~` with the home directory. `~user` forms are left alone —
/// only the shell knows how to resolve those.
fn expand_home(path: &str) -> PathBuf {
    let rest = match path.strip_prefix('~') {
        Some("") => "",
        Some(rest) => match rest.strip_prefix('/') {
            Some(rest) => rest,
            None => return PathBuf::from(path),
        },
        None => return PathBuf::from(path),
    };
    match dirs::home_dir() {
        Some(home) => home.join(rest),
        None => PathBuf::from(path),
    }
}

/// Write `df` to `path` as CSV, replacing any existing file and creating any missing
/// parent directories.
///
/// The write goes to a sibling `.tmp` file and is renamed into place only on success.
/// `File::create` truncates immediately and `CsvWriter` emits the header before it
/// rejects nested dtypes, so writing in place would destroy the file the user asked to
/// overwrite and *then* report a failure. The rename also makes the replacement atomic
/// for anything reading the destination.
///
/// Takes `&mut DataFrame` because that is what `CsvWriter::finish` requires; the
/// mutation is confined to this signature and does not reach any caller's state.
pub fn write_csv(df: &mut DataFrame, path: &Path) -> std::io::Result<()> {
    // `File::create` on a remote URI would fail with a bare "No such file or
    // directory" — reachable from `datasight browse az://…`, so name the cause.
    if let Some(scheme) = path.to_str().and_then(remote_scheme) {
        return Err(std::io::Error::other(format!(
            "cannot write to {} paths — export writes local files only",
            scheme
        )));
    }
    // Missing parents are created, the same as `d` in browse mode: the two ways of
    // writing a file out should not disagree about what a destination path means.
    // Unlike the download, there is no fetch to fail first — the frame is already in
    // memory — so a later write error can leave the new directory behind empty.
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let scratch = scratch_sibling(path);
    match write_all(df, &scratch) {
        Ok(()) => std::fs::rename(&scratch, path),
        Err(e) => {
            let _ = std::fs::remove_file(&scratch);
            Err(e)
        }
    }
}

/// Write the whole CSV to `path`, closing the file before returning so the caller can
/// rename it.
fn write_all(df: &mut DataFrame, path: &Path) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    CsvWriter::new(&mut file)
        .finish(df)
        .map_err(std::io::Error::other)
}

/// Scratch path next to the destination — same directory, so the rename stays within
/// one filesystem and cannot fall back to a copy.
fn scratch_sibling(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// The remote URI scheme of `path`, if it has one the export cannot write to.
/// The remote scheme `path` starts with, if any.
///
/// Public so the export prompt can warn before Enter using the same list
/// [`write_csv`] refuses on — a second copy in the UI would silently stop
/// warning the moment a scheme is added here.
pub fn remote_scheme(path: &str) -> Option<&'static str> {
    ["az://", "s3://"]
        .into_iter()
        .find(|prefix| path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filename_derives_from_the_source_stem() {
        assert_eq!(default_filename("orders.csv"), "orders.export.csv");
        assert_eq!(default_filename("data/orders.parquet"), "orders.export.csv");
    }

    #[test]
    fn default_filename_keeps_only_the_basename() {
        // A remote source must not suggest an unwritable remote destination.
        assert_eq!(
            default_filename("az://bucket/data/orders.parquet"),
            "orders.export.csv"
        );
    }

    #[test]
    fn default_filename_falls_back_for_stdin() {
        assert_eq!(default_filename(crate::config::STDIN_LABEL), FALLBACK_NAME);
    }

    #[test]
    fn default_filename_falls_back_when_there_is_no_stem() {
        assert_eq!(default_filename("/"), FALLBACK_NAME);
        assert_eq!(default_filename(""), FALLBACK_NAME);
    }

    #[test]
    fn resolve_path_appends_csv_when_missing() {
        assert_eq!(resolve_path("out").unwrap(), PathBuf::from("out.csv"));
        // Another extension is kept and suffixed: the file really is CSV, and the
        // name says so.
        assert_eq!(
            resolve_path("out.parquet").unwrap(),
            PathBuf::from("out.parquet.csv")
        );
    }

    #[test]
    fn resolve_path_leaves_an_existing_csv_extension_alone() {
        assert_eq!(resolve_path("out.csv").unwrap(), PathBuf::from("out.csv"));
        assert_eq!(resolve_path("out.CSV").unwrap(), PathBuf::from("out.CSV"));
    }

    #[test]
    fn resolve_path_trims_input_and_rejects_blank() {
        assert_eq!(
            resolve_path("  out.csv  ").unwrap(),
            PathBuf::from("out.csv")
        );
        assert!(resolve_path("   ").is_none());
        assert!(resolve_path("").is_none());
    }

    #[test]
    fn resolve_path_expands_a_leading_home_tilde() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(resolve_path("~/out.csv").unwrap(), home.join("out.csv"));
        assert_eq!(resolve_path("~/a/b").unwrap(), home.join("a/b.csv"));
        // A bare `~` names a directory, not a file. It resolves inside the home
        // directory rather than leaving a literal tilde in the name.
        assert_eq!(resolve_path("~").unwrap(), home.join(".csv"));
    }

    #[test]
    fn resolve_path_leaves_a_tilde_that_is_not_a_home_prefix_alone() {
        // `~user` needs a shell to resolve; a mid-path tilde is an ordinary character.
        assert_eq!(
            resolve_path("~someone/out.csv").unwrap(),
            PathBuf::from("~someone/out.csv")
        );
        assert_eq!(
            resolve_path("dir/~/out.csv").unwrap(),
            PathBuf::from("dir/~/out.csv")
        );
    }

    /// A DataFrame `CsvWriter` refuses: nested dtypes reach the viewer from any
    /// `.ndjson` / `.json` / `.parquet` file with a list or struct column.
    fn nested_df() -> DataFrame {
        let inner = Series::new("".into(), &[1i64, 2]);
        DataFrame::new(vec![Series::new("b".into(), &[inner]).into()]).unwrap()
    }

    #[test]
    fn a_failed_write_leaves_the_destination_untouched() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("precious.csv");
        std::fs::write(&target, "keep,me\n1,2\n").unwrap();

        let err = write_csv(&mut nested_df(), &target).unwrap_err();
        assert!(err.to_string().contains("nested"), "{}", err);
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "keep,me\n1,2\n",
            "a failed export must not truncate the file it was overwriting"
        );
    }

    #[test]
    fn a_failed_write_leaves_no_scratch_file_behind() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("out.csv");
        assert!(write_csv(&mut nested_df(), &target).is_err());
        assert!(!target.exists(), "no destination should be created");
        assert!(!scratch_sibling(&target).exists(), "scratch file leaked");
    }

    #[test]
    fn a_successful_write_leaves_no_scratch_file_behind() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("out.csv");
        let mut df = df!("a" => [1i64, 2]).unwrap();
        write_csv(&mut df, &target).unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "a\n1\n2\n");
        assert!(!scratch_sibling(&target).exists(), "scratch file leaked");
    }

    #[test]
    fn write_csv_creates_missing_parent_directories() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("reports").join("q3").join("out.csv");
        let mut df = df!("a" => [1i64]).unwrap();
        write_csv(&mut df, &target).unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "a\n1\n");
    }

    #[test]
    fn write_csv_reports_a_parent_that_is_a_file() {
        // `create_dir_all` cannot turn a regular file into a directory, and the error
        // has to reach the caller rather than being swallowed into a bare write error.
        let dir = tempfile::TempDir::new().unwrap();
        let blocker = dir.path().join("notadir");
        std::fs::write(&blocker, "x").unwrap();
        let mut df = df!("a" => [1i64]).unwrap();
        assert!(write_csv(&mut df, &blocker.join("out.csv")).is_err());
    }

    #[test]
    fn write_csv_refuses_remote_paths() {
        let mut df = df!("a" => [1i64]).unwrap();
        for path in ["az://bucket/out.csv", "s3://bucket/out.csv"] {
            let err = write_csv(&mut df, Path::new(path)).unwrap_err();
            assert!(
                err.to_string().contains("local files only"),
                "unexpected error for {}: {}",
                path,
                err
            );
        }
    }
}
