use polars::prelude::*;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut df = df! {
        "id"         => (1i64..=50).collect::<Vec<_>>(),
        "name"       => (1i64..=50).map(|i| format!("Person_{}", i)).collect::<Vec<_>>(),
        "age"        => (1i64..=50).map(|i| 20 + (i % 45)).collect::<Vec<_>>(),
        "city"       => (1i64..=50).map(|i| match i % 5 {
            0 => "London",
            1 => "Paris",
            2 => "Berlin",
            3 => "Madrid",
            _ => "Rome",
        }.to_string()).collect::<Vec<_>>(),
        "score"      => (1i64..=50).map(|i| (i as f64) * 1.5).collect::<Vec<_>>(),
        "active"     => (1i64..=50).map(|i| i % 2 == 0).collect::<Vec<_>>(),
    }?;

    std::fs::create_dir_all("tests/fixtures")?;
    let file = File::create("tests/fixtures/orders.parquet")?;
    ParquetWriter::new(file).finish(&mut df)?;

    println!(
        "Created tests/fixtures/orders.parquet with {} rows",
        df.height()
    );
    Ok(())
}
