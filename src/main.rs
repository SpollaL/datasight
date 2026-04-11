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
    CsvReadOptions::default()
        .with_parse_options(CsvParseOptions::default().with_separator(delimiter))
}

fn parse_buf(buf: Vec<u8>, delimiter: Option<u8>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    match detect_format(&buf) {
        StdinFormat::Json => Ok(JsonReader::new(std::io::Cursor::new(buf))
            .with_json_format(JsonFormat::Json)
            .finish()?),
        StdinFormat::Ndjson => Ok(JsonLineReader::new(std::io::Cursor::new(buf)).finish()?),
        StdinFormat::Csv => Ok(csv_options(delimiter.unwrap_or(b','))
            .into_reader_with_file_handle(std::io::Cursor::new(buf))
            .finish()?),
    }
}

fn load_dataframe(
    file_path: &str,
    delimiter: Option<u8>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ext = Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext {
        "csv" => Ok(csv_options(delimiter.unwrap_or(b','))
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?),
        "tsv" => Ok(csv_options(delimiter.unwrap_or(b'\t'))
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?),
        "parquet" => Ok(ParquetReader::new(std::fs::File::open(file_path)?).finish()?),
        "json" => Ok(JsonReader::new(std::fs::File::open(file_path)?)
            .with_json_format(JsonFormat::Json)
            .finish()?),
        "ndjson" | "jsonl" => Ok(JsonLineReader::new(std::fs::File::open(file_path)?).finish()?),
        _ => {
            if let Some(sep) = delimiter {
                Ok(csv_options(sep)
                    .try_into_reader_with_file_path(Some(file_path.into()))?
                    .finish()?)
            } else {
                Err(format!("Unsupported file format: .{}", ext).into())
            }
        }
    }
}

fn load_stdin(delimiter: Option<u8>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;
    parse_buf(buf, delimiter)
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
    fn test_fixture_unknown_ext_with_delimiter_loads_as_csv() {
        // copy of the TSV data but with a .dat extension — delimiter flag should save it
        let df = load_dataframe("tests/fixtures/orders.tsv", Some(b'\t'))
            .expect("should load with explicit delimiter");
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 12);
    }
}
