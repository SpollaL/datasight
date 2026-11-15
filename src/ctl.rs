//! `datasight ctl`: a control socket so an agent in another pane can read and drive the
//! table the user is looking at. Modelled on herdr's API.
//!
//! One JSON object per line over a Unix socket at `<runtime or cache dir>/datasight/<pid>.sock`.
//! Request `{"id","method","params"}`, reply `{"id","result"}` or `{"id","error":{"code","message"}}`.
//!
//! [`listen`] runs on background threads and only moves lines: each request goes to the UI
//! thread over a channel, and [`respond`] answers it there, with the `App` the UI owns, so
//! nothing is locked. [`run_client`] is the `datasight ctl` side.

use crate::app::{App, SortDirection};
use polars::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};

/// A request line, and where its reply goes.
pub type Request = (String, Sender<String>);

/// Every method `respond` knows; `unknown_method` lists them.
pub const METHODS: &[&str] = &[
    "state",
    "rows",
    "filter",
    "clear_filters",
    "sort",
    "goto",
    "export",
];

/// `rows` refuses more than this per call: past it, `export` and read the file.
const MAX_ROWS: usize = 1000;

pub const HELP: &str = r#"Read and drive a running datasight from another terminal.

Examples:
  datasight ctl state
  datasight ctl filter '{"column":"region","query":"= North"}'
  datasight ctl rows '{"offset":0,"limit":20}'

Methods (PARAMS_JSON is a JSON object; leave it out when there is nothing to pass):
  state                                 file, columns and dtypes, filters, sorts, cursor
  rows           {"offset":0,"limit":50}
                                        rows of the current view, at most 1000 per call
  filter         {"column":"region","query":"= North"}
                                        add a filter, as the f key does; filters add up
  clear_filters                         remove every filter
  sort           {"column":"total_amount","direction":"desc"}
                                        asc or desc, or none to drop that column's sort
  goto           {"row":0,"column":"region"}
                                        move the cursor; either key may be left out
  export         {"path":"/tmp/view.csv"}
                                        write the current view as CSV (absolute path)
  Methods that change the view return the new state, as `state` does.

Filter queries (each applies to one column):
  = North     exact text match                 != North   anything but North
  = 500       exact number, numeric columns    > 500      also <, >=, <= (numeric columns)
  = (null)    empty cells                      != (null)  non-empty cells
  Lap         regex match anywhere in the value, case-sensitive: lap misses Laptop,
              3.5 also matches 305, (?i)lap ignores case, ^500$ is an exact match
              that also works on numbers stored as text

Replies: the result goes to stdout (exit 0). An error goes to stderr as
{"code","message"} (exit 1). Codes: parse_error, unknown_method, invalid_params,
unknown_column, out_of_range, invalid_filter, no_table, busy, failed.
"busy" means the user is typing in a prompt: retry in a second.

Sessions: --socket PATH, else $DATASIGHT_SOCKET, else the only datasight running.
Sockets are named <pid>.sock."#;

struct Error {
    code: &'static str,
    message: String,
}

fn err(code: &'static str, message: impl Into<String>) -> Error {
    Error {
        code,
        message: message.into(),
    }
}

#[derive(Deserialize)]
struct Req {
    #[serde(default)]
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Deserialize)]
struct RowsParams {
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct FilterParams {
    column: String,
    query: String,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Direction {
    Asc,
    Desc,
    None,
}

#[derive(Deserialize)]
struct SortParams {
    column: String,
    direction: Direction,
}

#[derive(Deserialize)]
struct GotoParams {
    row: Option<usize>,
    column: Option<String>,
}

#[derive(Deserialize)]
struct ExportParams {
    path: String,
}

/// Answer every request waiting in `rx`. Returns whether any ran, so the caller knows
/// to redraw.
pub fn serve(rx: &Receiver<Request>, mut app: Option<&mut App>) -> bool {
    let mut served = false;
    for (line, reply) in rx.try_iter() {
        let _ = reply.send(respond(app.as_deref_mut(), &line));
        served = true;
    }
    served
}

/// Answer one request line. `None` is browse mode with no table open.
pub fn respond(app: Option<&mut App>, line: &str) -> String {
    let (id, outcome) = match serde_json::from_str::<Req>(line) {
        Ok(req) => {
            let params = if req.params.is_null() {
                json!({})
            } else {
                req.params
            };
            (req.id, dispatch(app, &req.method, params))
        }
        Err(e) => (Value::Null, Err(err("parse_error", e.to_string()))),
    };
    match outcome {
        Ok(result) => json!({ "id": id, "result": result }),
        Err(e) => json!({ "id": id, "error": { "code": e.code, "message": e.message } }),
    }
    .to_string()
}

fn dispatch(app: Option<&mut App>, method: &str, params: Value) -> Result<Value, Error> {
    let unknown = || {
        err(
            "unknown_method",
            format!("unknown method '{method}'; methods: {}", METHODS.join(", ")),
        )
    };
    if !METHODS.contains(&method) {
        return Err(unknown());
    }
    let app = app.ok_or_else(|| err("no_table", "no table is open; open a file first"))?;
    // A half-typed prompt would be applied along with our change, or wiped by it.
    if !matches!(method, "state" | "rows" | "export") && app.is_typing() {
        return Err(err("busy", "the user is typing in a prompt; retry shortly"));
    }
    let done = match method {
        "state" => return Ok(state(app)),
        "rows" => return rows(app, parse(params)?),
        "export" => return export(app, parse(params)?),
        "filter" => filter(app, parse(params)?)?,
        "clear_filters" => {
            app.filter.filters.clear();
            app.update_filter();
            "clear filters".to_string()
        }
        "sort" => sort(app, parse(params)?)?,
        "goto" => goto(app, parse(params)?)?,
        _ => return Err(unknown()),
    };
    app.status = Some(format!(" ✓ ctl: {done} "));
    Ok(state(app))
}

fn parse<T: DeserializeOwned>(params: Value) -> Result<T, Error> {
    serde_json::from_value(params).map_err(|e| err("invalid_params", e.to_string()))
}

/// Index of `name` in what is on screen — the key path resolves columns the same way.
fn column(app: &App, name: &str) -> Result<usize, Error> {
    app.headers.iter().position(|h| h == name).ok_or_else(|| {
        err(
            "unknown_column",
            format!("no column '{name}'; columns: {}", app.headers.join(", ")),
        )
    })
}

fn state(app: &App) -> Value {
    let columns: Vec<Value> = app
        .headers
        .iter()
        .map(|h| {
            let dtype = app.view.column(h).map(|c| c.dtype().to_string());
            json!({ "name": h, "dtype": dtype.unwrap_or_default() })
        })
        .collect();
    let filters: Vec<Value> = app
        .filter
        .filters
        .iter()
        .map(|(c, q)| json!({ "column": c, "query": q }))
        .collect();
    let sorts: Vec<Value> = app
        .sort
        .sorts
        .iter()
        .map(|(i, d)| {
            let dir = match d {
                SortDirection::Ascending => "asc",
                SortDirection::Descending => "desc",
            };
            json!({ "column": app.headers.get(*i), "direction": dir })
        })
        .collect();
    let cursor = (app.view.height() > 0).then(|| {
        json!({
            "row": app.state.selected().unwrap_or(0),
            "column": app.headers.get(app.state.selected_column().unwrap_or(0)),
        })
    });
    json!({
        "file": app.file_path,
        "mode": format!("{:?}", app.mode),
        "rows": app.view.height(),
        "total_rows": app.df.height(),
        "columns": columns,
        "filters": filters,
        "sorts": sorts,
        "grouped": app.groupby.active,
        "cursor": cursor,
    })
}

/// Values are cast to text the way the table renders them, so they match the screen.
/// Numbers and booleans go back to JSON numbers and booleans; decimals, dates and the
/// rest stay strings, so no precision is lost. Not polars' `JsonWriter`: it `todo!()`s
/// on dtypes such as Time, which would take the UI thread down.
fn rows(app: &App, p: RowsParams) -> Result<Value, Error> {
    let limit = p.limit.unwrap_or(50);
    if limit > MAX_ROWS {
        return Err(err(
            "invalid_params",
            format!("limit {limit} is over {MAX_ROWS}; export the view and read the file"),
        ));
    }
    let total = app.view.height();
    let page = app.view.slice(p.offset.min(total) as i64, limit);
    let cols: Vec<(bool, Option<Series>)> = page
        .get_columns()
        .iter()
        .map(|c| {
            let s = c.as_materialized_series();
            let raw = s.dtype().is_primitive_numeric() || s.dtype().is_bool();
            (raw, s.cast(&DataType::String).ok())
        })
        .collect();
    let rows: Vec<Vec<Value>> = (0..page.height())
        .map(|r| {
            cols.iter()
                .map(
                    |(raw, s)| match s.as_ref().and_then(|s| s.str().ok()?.get(r)) {
                        None => Value::Null,
                        // NaN and inf are not JSON numbers; they stay text.
                        Some(t) if *raw => serde_json::from_str(t).unwrap_or_else(|_| t.into()),
                        Some(t) => t.into(),
                    },
                )
                .collect()
        })
        .collect();
    Ok(json!({ "offset": p.offset, "total": total, "columns": app.headers, "rows": rows }))
}

fn filter(app: &mut App, p: FilterParams) -> Result<String, Error> {
    column(app, &p.column)?;
    app.add_filter(&p.column, &p.query)
        .map_err(|e| err("invalid_filter", format!("{e}; see datasight ctl --help")))?;
    Ok(format!("filter {} {}", p.column, p.query))
}

/// The direction changes in place, so a re-sorted column keeps its priority.
fn sort(app: &mut App, p: SortParams) -> Result<String, Error> {
    let idx = column(app, &p.column)?;
    let dir = match p.direction {
        Direction::Asc => Some(SortDirection::Ascending),
        Direction::Desc => Some(SortDirection::Descending),
        Direction::None => None,
    };
    let sorts = &mut app.sort.sorts;
    match (sorts.iter().position(|(c, _)| *c == idx), dir) {
        (Some(i), Some(d)) => sorts[i].1 = d,
        (Some(i), None) => {
            sorts.remove(i);
        }
        (None, Some(d)) => sorts.push((idx, d)),
        (None, None) => {}
    }
    app.update_filter();
    let name = match p.direction {
        Direction::Asc => "asc",
        Direction::Desc => "desc",
        Direction::None => "none",
    };
    Ok(format!("sort {} {name}", p.column))
}

fn goto(app: &mut App, p: GotoParams) -> Result<String, Error> {
    if p.row.is_none() && p.column.is_none() {
        return Err(err("invalid_params", "goto needs row, column or both"));
    }
    let col = p.column.as_deref().map(|c| column(app, c)).transpose()?;
    let height = app.view.height();
    if let Some(row) = p.row.filter(|&r| r >= height) {
        return Err(err(
            "out_of_range",
            format!("row {row} is past the end; the view has {height} rows"),
        ));
    }
    if let Some(row) = p.row {
        app.state.select(Some(row));
    }
    if let Some(c) = col {
        app.state.select_column(Some(c));
    }
    let c = app.state.selected_column().unwrap_or(0);
    Ok(format!(
        "goto row {} {}",
        app.state.selected().unwrap_or(0),
        app.headers.get(c).map_or("", |h| h.as_str())
    ))
}

/// Relative paths are refused: they would resolve against datasight's working
/// directory, not the agent's.
fn export(app: &mut App, p: ExportParams) -> Result<Value, Error> {
    let path = crate::export::resolve_path(&p.path)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            err(
                "invalid_params",
                format!("export needs an absolute path (got '{}')", p.path),
            )
        })?;
    let rows = app.view.height();
    crate::export::write_csv(&mut app.view, &path).map_err(|e| err("failed", e.to_string()))?;
    app.status = Some(format!(" ✓ ctl: wrote {rows} rows → {} ", path.display()));
    Ok(json!({ "path": path, "rows": rows }))
}

/// Removes the socket file when dropped, so a clean exit leaves nothing behind.
#[cfg_attr(not(unix), allow(dead_code))]
pub struct Socket(PathBuf);

impl Drop for Socket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(unix)]
fn socket_dir() -> Option<PathBuf> {
    dirs::runtime_dir()
        .or_else(dirs::cache_dir)
        .map(|d| d.join("datasight"))
}

/// Open this process's socket and answer it on background threads. Keep the returned
/// guard alive for as long as the TUI runs.
#[cfg(unix)]
pub fn listen(tx: Sender<Request>) -> io::Result<Socket> {
    let dir = socket_dir().ok_or_else(|| io::Error::other("no runtime or cache directory"))?;
    unix::bind(dir.join(format!("{}.sock", std::process::id())), tx)
}

#[cfg(not(unix))]
pub fn listen(_tx: Sender<Request>) -> io::Result<Socket> {
    Err(io::ErrorKind::Unsupported.into())
}

/// `datasight ctl <method> [params]`: send one request, print the reply. Returns the
/// process exit code.
pub fn run_client(method: &str, params: Option<&str>, socket: Option<PathBuf>) -> i32 {
    match call(method, params, socket) {
        Ok(reply) if reply.get("error").is_some() => {
            eprintln!("{}", reply["error"]);
            1
        }
        Ok(reply) => {
            println!("{}", reply["result"]);
            0
        }
        Err(message) => {
            eprintln!("datasight ctl: {message}");
            1
        }
    }
}

#[cfg(unix)]
fn call(method: &str, params: Option<&str>, socket: Option<PathBuf>) -> Result<Value, String> {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;

    let params: Value = match params {
        Some(p) => serde_json::from_str(p).map_err(|e| format!("PARAMS_JSON is not JSON: {e}"))?,
        None => json!({}),
    };
    let stream = match socket.or_else(|| std::env::var_os("DATASIGHT_SOCKET").map(PathBuf::from)) {
        Some(path) => UnixStream::connect(&path)
            .map_err(|e| format!("cannot connect to {}: {e}", path.display()))?,
        None => unix::discover(&socket_dir().ok_or("no runtime or cache directory")?)?,
    };
    // Browse mode loads remote files on the UI thread, so a reply can take a while.
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    let request = json!({ "id": 1, "method": method, "params": params });
    writeln!(&stream, "{request}").map_err(|e| format!("send failed: {e}"))?;
    let mut line = String::new();
    BufReader::new(&stream)
        .read_line(&mut line)
        .map_err(|e| format!("no reply: {e}"))?;
    serde_json::from_str(&line).map_err(|e| format!("bad reply: {e}"))
}

#[cfg(not(unix))]
fn call(_: &str, _: Option<&str>, _: Option<PathBuf>) -> Result<Value, String> {
    Err("needs Unix sockets, which this platform does not have".into())
}

#[cfg(unix)]
mod unix {
    use super::{Request, Socket};
    use std::io::{self, BufRead, BufReader, Write};
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{self, Sender};

    pub fn bind(path: PathBuf, tx: Sender<Request>) -> io::Result<Socket> {
        let dir = path.parent().unwrap_or(Path::new("."));
        // The 0700 directory is the real protection: `export` writes files.
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        // A file with our pid is left over from a dead process that had it before.
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let socket = Socket(path);
        std::fs::set_permissions(&socket.0, std::fs::Permissions::from_mode(0o600))?;
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let tx = tx.clone();
                std::thread::spawn(move || connection(stream, tx));
            }
        });
        Ok(socket)
    }

    /// Relay each line to the UI thread and write its reply back, until either side goes.
    fn connection(stream: UnixStream, tx: Sender<Request>) {
        for line in BufReader::new(&stream).lines() {
            let Ok(line) = line else { return };
            if line.trim().is_empty() {
                continue;
            }
            let (reply_tx, reply_rx) = mpsc::channel();
            if tx.send((line, reply_tx)).is_err() {
                return;
            }
            let Ok(reply) = reply_rx.recv() else { return };
            if writeln!(&stream, "{reply}").is_err() {
                return;
            }
        }
    }

    /// The one live socket in `dir`. A socket that refuses the connection belongs to a
    /// datasight that was killed, and is deleted on the way.
    pub fn discover(dir: &Path) -> Result<UnixStream, String> {
        let mut live = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension() != Some("sock".as_ref()) {
                continue;
            }
            match UnixStream::connect(&path) {
                Ok(stream) => live.push((path, stream)),
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    let _ = std::fs::remove_file(&path);
                }
                Err(_) => {}
            }
        }
        match live.len() {
            0 => Err("no running datasight found".into()),
            1 => Ok(live.remove(0).1),
            _ => Err(format!(
                "{} datasight sessions are running; pick one with --socket:\n{}",
                live.len(),
                live.iter()
                    .map(|(p, _)| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::Mode;

    fn make_app() -> App {
        let d = Series::new("d".into(), &[19000i32, 19001, 19002])
            .cast(&DataType::Date)
            .unwrap();
        let mut df = df! {
            "dept" => ["eng", "eng", "hr"],
            "sal"  => [Some(100i64), None, Some(150)],
            "ok"   => [true, false, true],
        }
        .unwrap();
        df.with_column(d).unwrap();
        App::new(df, "test.csv".to_string(), crate::theme::default_theme())
    }

    fn call(app: &mut App, method: &str, params: Value) -> Value {
        let line = json!({ "id": 7, "method": method, "params": params }).to_string();
        serde_json::from_str(&respond(Some(app), &line)).unwrap()
    }

    fn code(reply: &Value) -> &str {
        reply["error"]["code"].as_str().unwrap_or("")
    }

    #[test]
    fn malformed_json_is_a_parse_error_with_a_null_id() {
        let reply: Value = serde_json::from_str(&respond(None, "{nope")).unwrap();
        assert_eq!(code(&reply), "parse_error");
        assert!(reply["id"].is_null());
    }

    #[test]
    fn unknown_method_echoes_the_id_and_lists_every_method() {
        let reply = call(&mut make_app(), "nope", json!({}));
        assert_eq!(code(&reply), "unknown_method");
        assert_eq!(reply["id"], 7);
        let message = reply["error"]["message"].as_str().unwrap();
        assert!(METHODS.iter().all(|m| message.contains(m)), "{message}");
    }

    #[test]
    fn every_method_is_documented_and_dispatched() {
        for method in METHODS {
            assert!(HELP.contains(method), "{method} missing from HELP");
            let reply = call(&mut make_app(), method, json!({}));
            assert_ne!(code(&reply), "unknown_method", "{method}");
        }
    }

    #[test]
    fn missing_null_and_empty_params_are_all_accepted() {
        let mut app = make_app();
        for line in [
            r#"{"id":1,"method":"state"}"#,
            r#"{"id":1,"method":"state","params":null}"#,
            r#"{"id":1,"method":"state","params":{}}"#,
        ] {
            let reply: Value = serde_json::from_str(&respond(Some(&mut app), line)).unwrap();
            assert_eq!(reply["result"]["rows"], 3, "{line}");
        }
    }

    #[test]
    fn no_table_without_an_app() {
        let reply: Value = serde_json::from_str(&respond(None, r#"{"method":"state"}"#)).unwrap();
        assert_eq!(code(&reply), "no_table");
    }

    #[test]
    fn state_describes_the_view() {
        let s = &call(&mut make_app(), "state", json!({}))["result"];
        assert_eq!(s["file"], "test.csv");
        assert_eq!(s["mode"], "Normal");
        assert_eq!(s["total_rows"], 3);
        assert_eq!(s["columns"][0], json!({ "name": "dept", "dtype": "str" }));
        assert_eq!(s["columns"][3]["dtype"], "date");
        assert_eq!(s["filters"], json!([]));
        assert_eq!(s["grouped"], false);
        assert_eq!(s["cursor"], json!({ "row": 0, "column": "dept" }));
    }

    #[test]
    fn rows_keep_json_types_and_page() {
        let mut app = make_app();
        let r = &call(&mut app, "rows", json!({}))["result"];
        assert_eq!(r["total"], 3);
        assert_eq!(r["columns"], json!(["dept", "sal", "ok", "d"]));
        assert_eq!(r["rows"][0], json!(["eng", 100, true, "2022-01-08"]));
        assert_eq!(r["rows"][1][1], Value::Null);

        let r = &call(&mut app, "rows", json!({ "offset": 2, "limit": 5 }))["result"];
        assert_eq!(r["rows"].as_array().unwrap().len(), 1);
        let r = &call(&mut app, "rows", json!({ "offset": 99 }))["result"];
        assert_eq!(r["rows"], json!([]));

        let reply = call(&mut app, "rows", json!({ "limit": 1001 }));
        assert_eq!(code(&reply), "invalid_params");
    }

    #[test]
    fn view_changes_are_busy_while_the_user_types() {
        let mut app = make_app();
        app.mode = Mode::Filter;
        assert_eq!(code(&call(&mut app, "clear_filters", json!({}))), "busy");
        assert!(call(&mut app, "state", json!({}))["result"].is_object());
    }

    #[test]
    fn filter_commits_and_reports_on_the_status_bar() {
        let mut app = make_app();
        let s = &call(
            &mut app,
            "filter",
            json!({ "column": "dept", "query": "= eng" }),
        )["result"];
        assert_eq!(s["rows"], 2);
        assert_eq!(
            s["filters"][0],
            json!({ "column": "dept", "query": "= eng" })
        );
        assert!(app
            .status
            .as_deref()
            .unwrap()
            .contains("ctl: filter dept = eng"));

        // Same filter again is not stacked.
        call(
            &mut app,
            "filter",
            json!({ "column": "dept", "query": "= eng" }),
        );
        assert_eq!(app.filter.filters.len(), 1);
    }

    #[test]
    fn filter_errors_teach() {
        let mut app = make_app();
        let bad = |app: &mut App, column: &str, query: &str| {
            call(app, "filter", json!({ "column": column, "query": query }))
        };
        assert_eq!(code(&bad(&mut app, "nope", "x")), "unknown_column");
        let reply = bad(&mut app, "sal", "> abc");
        assert_eq!(code(&reply), "invalid_filter");
        assert!(reply["error"]["message"]
            .as_str()
            .unwrap()
            .ends_with("see datasight ctl --help"));
        assert_eq!(code(&bad(&mut app, "dept", "> 5")), "invalid_filter");
        assert_eq!(code(&bad(&mut app, "dept", "(")), "invalid_filter");
        assert_eq!(code(&call(&mut app, "filter", json!({}))), "invalid_params");
        assert!(app.filter.filters.is_empty());
    }

    #[test]
    fn clear_filters_removes_them_all() {
        let mut app = make_app();
        app.add_filter("dept", "= eng").unwrap();
        let s = &call(&mut app, "clear_filters", json!({}))["result"];
        assert_eq!(s["rows"], 3);
        assert_eq!(s["filters"], json!([]));
    }

    #[test]
    fn sort_sets_changes_in_place_and_removes() {
        let mut app = make_app();
        let sort = |app: &mut App, column: &str, direction: &str| {
            call(
                app,
                "sort",
                json!({ "column": column, "direction": direction }),
            )["result"]["sorts"]
                .clone()
        };
        sort(&mut app, "dept", "asc");
        sort(&mut app, "sal", "asc");
        // dept stays the primary sort when its direction flips.
        assert_eq!(
            sort(&mut app, "dept", "desc"),
            json!([
                { "column": "dept", "direction": "desc" },
                { "column": "sal", "direction": "asc" },
            ])
        );
        assert_eq!(
            app.view.column("dept").unwrap().str().unwrap().get(0),
            Some("hr")
        );
        assert_eq!(
            sort(&mut app, "dept", "none"),
            json!([{ "column": "sal", "direction": "asc" }])
        );
        let reply = call(
            &mut app,
            "sort",
            json!({ "column": "dept", "direction": "up" }),
        );
        assert_eq!(code(&reply), "invalid_params");
    }

    #[test]
    fn goto_moves_the_cursor_or_refuses_without_moving() {
        let mut app = make_app();
        let s = &call(&mut app, "goto", json!({ "row": 2, "column": "sal" }))["result"];
        assert_eq!(s["cursor"], json!({ "row": 2, "column": "sal" }));
        let reply = call(&mut app, "goto", json!({ "row": 3, "column": "dept" }));
        assert_eq!(code(&reply), "out_of_range");
        assert_eq!(
            app.state.selected_column(),
            Some(1),
            "a refused goto must not move"
        );
        assert_eq!(code(&call(&mut app, "goto", json!({}))), "invalid_params");
    }

    #[test]
    fn export_writes_absolute_paths_only() {
        let mut app = make_app();
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("out");
        let r = &call(&mut app, "export", json!({ "path": path }))["result"];
        assert_eq!(r["rows"], 3);
        let written = PathBuf::from(r["path"].as_str().unwrap());
        assert_eq!(written, dir.path().join("out.csv"));
        assert!(std::fs::read_to_string(written)
            .unwrap()
            .starts_with("dept,sal,ok,d\n"));

        let reply = call(&mut app, "export", json!({ "path": "out.csv" }));
        assert_eq!(code(&reply), "invalid_params");
    }

    #[test]
    fn grouped_views_resolve_columns_by_what_is_on_screen() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key(); // dept
        app.state.select_column(Some(1));
        app.cycle_groupby_agg(); // sal → sum
        app.apply_groupby();
        let agg = app.headers[1].clone();

        let s = &call(
            &mut app,
            "filter",
            json!({ "column": agg, "query": "> 120" }),
        )["result"];
        assert_eq!(s["rows"], 1);
        assert_eq!(s["grouped"], true);
        let reply = call(
            &mut app,
            "filter",
            json!({ "column": "ok", "query": "true" }),
        );
        assert_eq!(code(&reply), "unknown_column");
    }

    #[cfg(unix)]
    #[test]
    fn a_client_reaches_a_listening_socket_and_dead_ones_are_cleaned_up() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::{UnixListener, UnixStream};

        let dir = tempfile::TempDir::new().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let socket = unix::bind(dir.path().join("1.sock"), tx).unwrap();

        // A socket nobody listens on any more: bound, then its listener dropped.
        let dead = dir.path().join("2.sock");
        drop(UnixListener::bind(&dead).unwrap());

        let stream = unix::discover(dir.path()).unwrap();
        assert!(!dead.exists(), "the dead socket should be deleted");

        // Play the UI thread on this one.
        let ui = std::thread::spawn(move || {
            let mut app = make_app();
            let (line, reply) = rx.recv().unwrap();
            reply.send(respond(Some(&mut app), &line)).unwrap();
        });
        writeln!(&stream, r#"{{"id":3,"method":"state"}}"#).unwrap();
        let mut line = String::new();
        BufReader::new(&stream).read_line(&mut line).unwrap();
        ui.join().unwrap();
        let reply: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(reply["id"], 3);
        assert_eq!(reply["result"]["total_rows"], 3);

        let path = socket.0.clone();
        drop(socket);
        assert!(!path.exists(), "dropping the guard removes the socket file");
        assert!(UnixStream::connect(&path).is_err());
    }
}
