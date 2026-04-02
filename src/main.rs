mod app;
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
    about = "A terminal viewer for CSV, Parquet, and JSON files"
)]
struct Cli {
    /// Path to a CSV, Parquet, JSON, or NDJSON file (omit to read from stdin)
    file: Option<String>,
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

fn parse_buf(buf: Vec<u8>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    match detect_format(&buf) {
        StdinFormat::Json => Ok(JsonReader::new(std::io::Cursor::new(buf))
            .with_json_format(JsonFormat::Json)
            .finish()?),
        StdinFormat::Ndjson => Ok(JsonLineReader::new(std::io::Cursor::new(buf)).finish()?),
        StdinFormat::Csv => Ok(CsvReadOptions::default()
            .into_reader_with_file_handle(std::io::Cursor::new(buf))
            .finish()?),
    }
}

fn load_dataframe(file_path: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ext = Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext {
        "csv" => Ok(CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(file_path.into()))?
            .finish()?),
        "parquet" => Ok(ParquetReader::new(std::fs::File::open(file_path)?).finish()?),
        "json" => Ok(JsonReader::new(std::fs::File::open(file_path)?)
            .with_json_format(JsonFormat::Json)
            .finish()?),
        "ndjson" | "jsonl" => Ok(JsonLineReader::new(std::fs::File::open(file_path)?).finish()?),
        _ => Err(format!("Unsupported file format: .{}", ext).into()),
    }
}

fn load_stdin() -> Result<DataFrame, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;
    parse_buf(buf)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::IsTerminal;
    let cli = Cli::parse();

    let (df, title) = match cli.file {
        Some(ref path) => {
            let df = load_dataframe(path).unwrap_or_else(|err| {
                eprintln!("Error loading file: {}", err);
                std::process::exit(1);
            });
            (df, path.clone())
        }
        None => {
            if std::io::stdin().is_terminal() {
                eprintln!("Error: no file path given and stdin is not a pipe.");
                eprintln!("Usage: datasight <file.csv|file.parquet|file.json|file.ndjson>");
                eprintln!("       <command> | datasight");
                std::process::exit(1);
            }
            let df = load_stdin().unwrap_or_else(|err| {
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
        let df = parse_buf(json.to_vec()).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_json_array_values() {
        let json = br#"[{"x":10},{"x":20},{"x":30}]"#;
        let df = parse_buf(json.to_vec()).expect("should parse");
        assert_eq!(df.height(), 3);
        let col = df.column("x").unwrap();
        assert_eq!(col.get(0).unwrap(), polars::prelude::AnyValue::Int64(10));
    }

    #[test]
    fn test_parse_buf_invalid_json_errors() {
        let bad = b"[not valid json".to_vec();
        assert!(parse_buf(bad).is_err());
    }

    // --- parse_buf: NDJSON ---

    #[test]
    fn test_parse_buf_ndjson_shape() {
        let ndjson = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n".to_vec();
        let df = parse_buf(ndjson).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_ndjson_values() {
        let ndjson = b"{\"score\":100}\n{\"score\":200}\n".to_vec();
        let df = parse_buf(ndjson).expect("should parse");
        assert_eq!(df.height(), 2);
    }

    // --- parse_buf: CSV ---

    #[test]
    fn test_parse_buf_csv_shape() {
        let csv = b"name,age\nAlice,30\nBob,25\n".to_vec();
        let df = parse_buf(csv).expect("should parse");
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df.column("name").is_ok());
        assert!(df.column("age").is_ok());
    }

    #[test]
    fn test_parse_buf_csv_single_column() {
        let csv = b"value\n1\n2\n3\n".to_vec();
        let df = parse_buf(csv).expect("should parse");
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 1);
    }
}
