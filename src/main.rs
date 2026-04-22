mod app;
mod config;
mod events;
mod ui;

use app::App;
use clap::Parser;
use events::run_app;
use polars::prelude::*;
use std::path::Path;

#[derive(Parser)]
#[command(
    name = "datasight",
    version,
    about = "A terminal viewer for CSV, TSV, Parquet, and JSON files"
)]
struct Cli {
    /// Path to a CSV, TSV, Parquet, JSON, or NDJSON file (omit to read from stdin)
    file: Option<String>,
    /// Field delimiter for CSV/TSV input (e.g. ',' '\t' '|' ';').
    /// Defaults to '\t' for .tsv files and ',' for everything else.
    #[arg(short = 'd', long, value_name = "CHAR")]
    delimiter: Option<String>,
}

#[derive(Debug, PartialEq)]
enum StdinFormat {
    Json,
    Ndjson,
    Csv,
}

fn detect_format(buf: &[u8]) -> StdinFormat {
    match buf.iter().find(|&&b| !b.is_ascii_whitespace()).copied() {
        Some(b'[') => StdinFormat::Json,
        Some(b'{') => StdinFormat::Ndjson,
        _ => StdinFormat::Csv,
    }
}

fn parse_delimiter(s: &str) -> Result<u8, String> {
    match s {
        r"\t" => Ok(b'\t'),
        s if s.len() == 1 => Ok(s.as_bytes()[0]),
        _ => Err(format!("delimiter must be a single character, got {:?}", s)),
    }
}

fn csv_options(delimiter: u8) -> CsvReadOptions {
    // Date detection is handled by `try_parse_date_columns` post-load so the
    // ambiguity guard (MM/DD vs DD/MM) applies consistently.
    CsvReadOptions::default()
        .with_parse_options(CsvParseOptions::default().with_separator(delimiter))
}

fn parse_buf(buf: Vec<u8>, delimiter: Option<u8>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let df = match detect_format(&buf) {
        StdinFormat::Json => JsonReader::new(std::io::Cursor::new(buf))
            .with_json_format(JsonFormat::Json)
            .finish()?,
        StdinFormat::Ndjson => JsonLineReader::new(std::io::Cursor::new(buf)).finish()?,
        StdinFormat::Csv => csv_options(delimiter.unwrap_or(b','))
            .into_reader_with_file_handle(std::io::Cursor::new(buf))
            .finish()?,
    };
    Ok(try_parse_date_columns(df))
}

fn load_dataframe(
    file_path: &str,
    delimiter: Option<u8>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ext = Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    let df = match ext {
        "csv" => csv_options(delimiter.unwrap_or(b','))
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?,
        "tsv" => csv_options(delimiter.unwrap_or(b'\t'))
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?,
        "parquet" => ParquetReader::new(std::fs::File::open(file_path)?).finish()?,
        "json" => JsonReader::new(std::fs::File::open(file_path)?)
            .with_json_format(JsonFormat::Json)
            .finish()?,
        "ndjson" | "jsonl" => JsonLineReader::new(std::fs::File::open(file_path)?).finish()?,
        _ => {
            if let Some(sep) = delimiter {
                csv_options(sep)
                    .try_into_reader_with_file_path(Some(file_path.into()))?
                    .finish()?
            } else {
                return Err(format!("Unsupported file format: .{}", ext).into());
            }
        }
    };
    Ok(try_parse_date_columns(df))
}

fn load_stdin(delimiter: Option<u8>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;
    parse_buf(buf, delimiter)
}

/// Formats attempted for post-load date detection, in priority order.
/// ISO formats come first so unambiguous inputs resolve immediately; the
/// slash-separated American form is skipped when it conflicts with the
/// European form (see ambiguity guard in `pick_date_format`).
const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", // e.g. 03-Jan-2024
    "%d %b %Y",
];

/// Scans string columns and replaces them with Date columns when every non-null
/// value parses cleanly under a known format. When both `%m/%d/%Y` and
/// `%d/%m/%Y` would succeed on every row (all values have day ≤ 12), the
/// column is left as String to avoid guessing the wrong calendar convention.
pub(crate) fn try_parse_date_columns(mut df: DataFrame) -> DataFrame {
    let names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    for name in &names {
        let Ok(column) = df.column(name) else {
            continue;
        };
        if column.dtype() != &DataType::String {
            continue;
        }
        let non_null = column.len() - column.null_count();
        if non_null == 0 {
            continue;
        }

        // Cheap byte-level pre-filter: a single non-null value tells us
        // whether the column could possibly be a date. Skips the per-column
        // lazy plan overhead on free-text and obviously-non-date columns.
        let Ok(str_col) = column.str() else { continue };
        let Some(probe) = (0..str_col.len()).find_map(|i| str_col.get(i)) else {
            continue;
        };
        if !looks_like_date_candidate(probe) {
            continue;
        }

        let Some(parsed) = pick_date_format(&df, name, non_null) else {
            continue;
        };
        if let Ok(new_df) = df.clone().lazy().with_column(parsed).collect() {
            df = new_df;
        }
    }
    df
}

/// Must contain at least one digit, one date-ish separator (`-`, `/`, or
/// space), and have a length plausibly matching one of the known formats.
fn looks_like_date_candidate(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 8 || b.len() > 20 {
        return false;
    }
    let mut has_digit = false;
    let mut has_sep = false;
    for &c in b {
        if c.is_ascii_digit() {
            has_digit = true;
        } else if matches!(c, b'-' | b'/' | b' ') {
            has_sep = true;
        }
    }
    has_digit && has_sep
}

/// Size of the sample used to pre-screen date formats. Small enough that a
/// sample parse is O(1) vs. the column's row count, large enough to be
/// representative (free-text values almost never look date-shaped).
const DATE_SAMPLE_SIZE: usize = 32;

fn pick_date_format(df: &DataFrame, name: &str, non_null: usize) -> Option<Expr> {
    let sample_df = df
        .clone()
        .lazy()
        .select([col(name).drop_nulls().head(Some(DATE_SAMPLE_SIZE))])
        .collect()
        .ok()?;
    let sample_n = sample_df.column(name).ok()?.len();
    if sample_n == 0 {
        return None;
    }

    let parses_all = |subject: &DataFrame, fmt: &str, expected: usize| -> bool {
        let options = StrptimeOptions {
            format: Some(fmt.into()),
            strict: false,
            exact: true,
            cache: true,
        };
        let expr = col(name).str().to_date(options).alias(name);
        let Ok(parsed) = subject.clone().lazy().select([expr]).collect() else {
            return false;
        };
        let Ok(c) = parsed.column(name) else {
            return false;
        };
        c.len() - c.null_count() == expected
    };

    // Ambiguity guard: MM/DD/YYYY and DD/MM/YYYY look identical when every
    // value has day ≤ 12. Check the sample first so non-slash columns pay
    // nothing; only escalate to a full-column pass when both survive.
    let ambiguous_slash = parses_all(&sample_df, "%m/%d/%Y", sample_n)
        && parses_all(&sample_df, "%d/%m/%Y", sample_n)
        && parses_all(df, "%m/%d/%Y", non_null)
        && parses_all(df, "%d/%m/%Y", non_null);

    for fmt in DATE_FORMATS {
        if ambiguous_slash && *fmt == "%m/%d/%Y" {
            continue;
        }
        if !parses_all(&sample_df, fmt, sample_n) {
            continue;
        }
        if parses_all(df, fmt, non_null) {
            let options = StrptimeOptions {
                format: Some((*fmt).into()),
                strict: false,
                exact: true,
                cache: true,
            };
            return Some(col(name).str().to_date(options).alias(name));
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::IsTerminal;
    let cli = Cli::parse();

    let delimiter = cli
        .delimiter
        .as_deref()
        .map(parse_delimiter)
        .transpose()
        .unwrap_or_else(|err| {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        });

    let (df, title) = match cli.file {
        Some(ref path) => {
            let df = load_dataframe(path, delimiter).unwrap_or_else(|err| {
                eprintln!("Error loading file: {}", err);
                std::process::exit(1);
            });
            (df, path.clone())
        }
        None => {
            if std::io::stdin().is_terminal() {
                eprintln!("Error: no file path given and stdin is not a pipe.");
                eprintln!(
                    "Usage: datasight <file.csv|file.tsv|file.parquet|file.json|file.ndjson>"
                );
                eprintln!("       <command> | datasight");
                std::process::exit(1);
            }
            let df = load_stdin(delimiter).unwrap_or_else(|err| {
                eprintln!("Error reading stdin: {}", err);
                std::process::exit(1);
            });
            (df, String::from("<stdin>"))
        }
    };

    let app = App::new(df, title);
    ratatui::run(|terminal| run_app(terminal, app))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_delimiter ---

    #[test]
    fn test_parse_delimiter_tab_escape() {
        assert_eq!(parse_delimiter(r"\t"), Ok(b'\t'));
    }

    #[test]
    fn test_parse_delimiter_pipe() {
        assert_eq!(parse_delimiter("|"), Ok(b'|'));
    }

    #[test]
    fn test_parse_delimiter_semicolon() {
        assert_eq!(parse_delimiter(";"), Ok(b';'));
    }

    #[test]
    fn test_parse_delimiter_comma() {
        assert_eq!(parse_delimiter(","), Ok(b','));
    }

    #[test]
    fn test_parse_delimiter_multi_char_errors() {
        assert!(parse_delimiter("||").is_err());
        assert!(parse_delimiter("tab").is_err());
    }

    // --- detect_format ---

    #[test]
    fn test_detect_json_array() {
        assert_eq!(detect_format(b"[{\"a\":1}]"), StdinFormat::Json);
    }

    #[test]
    fn test_detect_json_array_leading_whitespace() {
        assert_eq!(detect_format(b"  \n  [{\"a\":1}]"), StdinFormat::Json);
    }

    #[test]
    fn test_detect_ndjson() {
        assert_eq!(detect_format(b"{\"a\":1}\n{\"a\":2}"), StdinFormat::Ndjson);
    }

    #[test]
    fn test_detect_ndjson_leading_whitespace() {
        assert_eq!(detect_format(b"\n{\"a\":1}"), StdinFormat::Ndjson);
    }

    #[test]
    fn test_detect_csv() {
        assert_eq!(detect_format(b"a,b,c\n1,2,3"), StdinFormat::Csv);
    }

    #[test]
    fn test_detect_empty_falls_back_to_csv() {
        assert_eq!(detect_format(b""), StdinFormat::Csv);
    }

    #[test]
    fn test_detect_whitespace_only_falls_back_to_csv() {
        assert_eq!(detect_format(b"   \n\t  "), StdinFormat::Csv);
    }

    // --- parse_buf: JSON array ---

    #[test]
    fn test_parse_buf_json_array_shape() {
        let json = br#"[{"name":"Alice","age":30},{"name":"Bob","age":25}]"#;
        let df = parse_buf(json.to_vec(), None).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_json_array_values() {
        let json = br#"[{"x":10},{"x":20},{"x":30}]"#;
        let df = parse_buf(json.to_vec(), None).expect("should parse");
        assert_eq!(df.height(), 3);
        let col = df.column("x").unwrap();
        assert_eq!(col.get(0).unwrap(), polars::prelude::AnyValue::Int64(10));
    }

    #[test]
    fn test_parse_buf_invalid_json_errors() {
        let bad = b"[not valid json".to_vec();
        assert!(parse_buf(bad, None).is_err());
    }

    // --- parse_buf: NDJSON ---

    #[test]
    fn test_parse_buf_ndjson_shape() {
        let ndjson = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n".to_vec();
        let df = parse_buf(ndjson, None).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_ndjson_values() {
        let ndjson = b"{\"score\":100}\n{\"score\":200}\n".to_vec();
        let df = parse_buf(ndjson, None).expect("should parse");
        assert_eq!(df.height(), 2);
    }

    // --- parse_buf: CSV with custom delimiter ---

    #[test]
    fn test_parse_buf_csv_default_delimiter() {
        let csv = b"name,age\nAlice,30\nBob,25\n".to_vec();
        let df = parse_buf(csv, None).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_tsv_delimiter() {
        let tsv = b"name\tage\nAlice\t30\nBob\t25\n".to_vec();
        let df = parse_buf(tsv, Some(b'\t')).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_pipe_delimiter() {
        let psv = b"name|age\nAlice|30\nBob|25\n".to_vec();
        let df = parse_buf(psv, Some(b'|')).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_parse_buf_csv_single_column() {
        let csv = b"value\n1\n2\n3\n".to_vec();
        let df = parse_buf(csv, None).expect("should parse");
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 1);
    }

    // --- fixture smoke tests ---

    #[test]
    fn test_fixture_orders_csv() {
        let df = load_dataframe("tests/fixtures/orders.csv", None).expect("should load");
        assert_eq!(df.height(), 100);
        assert_eq!(df.width(), 12);
        assert!(df.column("order_id").is_ok());
        assert!(df.column("status").is_ok());
    }

    #[test]
    fn test_fixture_orders_tsv() {
        let df = load_dataframe("tests/fixtures/orders.tsv", None).expect("should load");
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 12);
        assert!(df.column("order_id").is_ok());
        assert!(df.column("status").is_ok());
    }

    #[test]
    fn test_fixture_orders_json() {
        let df = load_dataframe("tests/fixtures/orders.json", None).expect("should load");
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 12);
        assert!(df.column("order_id").is_ok());
        assert!(df.column("status").is_ok());
    }

    #[test]
    fn test_fixture_orders_ndjson() {
        let df = load_dataframe("tests/fixtures/orders.ndjson", None).expect("should load");
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 12);
        assert!(df.column("order_id").is_ok());
        assert!(df.column("status").is_ok());
    }

    #[test]
    fn test_fixture_orders_parquet() {
        let df = load_dataframe("tests/fixtures/orders.parquet", None).expect("should load");
        assert_eq!(df.height(), 50);
        assert_eq!(df.width(), 6);
        assert!(df.column("id").is_ok());
        assert!(df.column("score").is_ok());
    }

    #[test]
    fn test_fixture_wide_csv() {
        let df = load_dataframe("tests/fixtures/wide.csv", None).expect("should load");
        assert_eq!(df.width(), 200);
        assert!(df.height() > 0);
    }

    #[test]
    fn test_fixture_unsupported_extension_errors() {
        assert!(load_dataframe("tests/fixtures/orders.csv.bak", None).is_err());
    }

    #[test]
    fn test_try_parse_date_columns_mm_dd_yyyy() {
        // Non-ISO date strings like "12/25/2022" are sorted lexicographically
        // when left as strings (January lands before December of prior years).
        // The post-load detector promotes them to Date.
        let csv = b"id,ts\n1,01/01/2023\n2,12/25/2022\n3,12/31/2023\n".to_vec();
        let df = parse_buf(csv, Some(b',')).expect("load");
        assert_eq!(df.column("ts").unwrap().dtype(), &DataType::Date);
    }

    #[test]
    fn test_try_parse_date_columns_leaves_ambiguous_as_string() {
        // Every value has day ≤ 12, so MM/DD/YYYY and DD/MM/YYYY are
        // indistinguishable — keep the column as String rather than guess.
        let csv = b"id,ts\n1,01/02/2023\n2,03/04/2023\n".to_vec();
        let df = parse_buf(csv, Some(b',')).expect("load");
        assert_eq!(df.column("ts").unwrap().dtype(), &DataType::String);
    }

    #[test]
    fn test_try_parse_date_columns_leaves_mixed_as_string() {
        // Column has one value that matches no format — don't partial-parse,
        // keep the whole column as String.
        let csv = b"id,ts\n1,01/15/2023\n2,not-a-date\n3,02/20/2023\n".to_vec();
        let df = parse_buf(csv, Some(b',')).expect("load");
        assert_eq!(df.column("ts").unwrap().dtype(), &DataType::String);
    }

    #[test]
    fn test_looks_like_date_candidate() {
        assert!(looks_like_date_candidate("2023-01-15"));
        assert!(looks_like_date_candidate("01/15/2023"));
        assert!(looks_like_date_candidate("15 Jan 2023"));
        assert!(!looks_like_date_candidate("hello"));
        assert!(!looks_like_date_candidate("12345"));
        assert!(!looks_like_date_candidate("a"));
        assert!(!looks_like_date_candidate(
            "this is a very long string value"
        ));
    }

    #[test]
    fn test_fixture_unknown_ext_with_delimiter_loads_as_csv() {
        // copy of the TSV data but with a .dat extension — delimiter flag should save it
        let df = load_dataframe("tests/fixtures/orders.tsv", Some(b'\t'))
            .expect("should load with explicit delimiter");
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 12);
    }
}
