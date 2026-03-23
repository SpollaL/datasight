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
    about = "A terminal viewer for CSV and Parquet files"
)]
struct Cli {
    /// Path to a CSV or Parquet file
    file: String,
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
        _ => Err(format!("Unsupported file format: .{}", ext).into()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let df = load_dataframe(&cli.file).unwrap_or_else(|err| {
        eprintln!("Error loading file: {}", err);
        std::process::exit(1);
    });

    let app = App::new(df, cli.file);
    ratatui::run(|terminal| run_app(terminal, app))
}
