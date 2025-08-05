use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use polars::prelude::*;
use rayon::prelude::*;

#[derive(Clone, Debug, Copy, PartialEq, Eq, ValueEnum)]
enum Format {
    Arrow,
    Parquet,
}

#[derive(Parser, Debug)]
#[command(version, long_about = None)]
struct Args {
    /// The path to the input file
    #[arg(long)]
    input: PathBuf,

    /// File format
    #[arg(long)]
    #[clap(value_enum, default_value_t = Format::Parquet)]
    format: Format,

    /// The path to the output files
    #[arg(long)]
    output: PathBuf,
}

fn arrow_to_parquet(filename: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let file = File::open(filename)?;
    let reader = StreamReader::try_new(file, None)?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    let df = batches_to_parquet(&batches)?;

    Ok(df)
}

fn batches_to_parquet(batches: &[RecordBatch]) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Our output file
    let tmp_file = tempfile::tempfile()?;

    // Write the batches to the file
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(tmp_file, batches[0].schema(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    let tmp_file = writer.into_inner()?;

    // Read in parquet file
    let df = ParquetReader::new(tmp_file)
        .with_columns(Some(vec!["audio".to_string()]))
        .finish()?;

    Ok(df)
}

fn read_parquet(filename: PathBuf) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(filename)?;

    let df = ParquetReader::new(file)
        .with_columns(Some(vec!["audio".to_string()]))
        .finish()?;

    Ok(df)
}

fn write_file(filename: PathBuf, data: &[u8]) -> std::io::Result<()> {
    // Skip if the file already exists
    if !filename.exists() {
        // Write the file
        let mut file = File::create(filename)?;
        file.write_all(data)?;
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the command line arguments
    let args = Args::parse();

    // Check if the input exists
    if !args.input.exists() {
        eprintln!("Input file does not exist: {}", args.input.display());
        std::process::exit(1);
    }
    // Check if the input is a file
    if !args.input.is_file() {
        eprintln!("Input is not a file: {}", args.input.display());
        std::process::exit(1);
    }

    // Convert the output path to a string
    let output_display: String = args.output.display().to_string();

    // Create the output folder if it doesn't exist
    std::fs::create_dir_all(args.output)?;

    let filename = args.input;

    // Convert the file to a DataFrame
    let df = match args.format {
        Format::Arrow => arrow_to_parquet(filename)?,
        Format::Parquet => read_parquet(filename)?,
    };

    // Conver

    println!("Number of rows: {}", df.height());

    for row in df.iter() {
        let struct_series = row.struct_()?;

        let all_bytes = struct_series.field_by_name("bytes")?;
        let all_paths = struct_series.field_by_name("path")?;

        // Extract files in parallel
        (0..all_paths.len()).into_par_iter().try_for_each(|idx| {
            let path = all_paths.get(idx)?;
            let bytes = all_bytes.get(idx)?;

            let filename = match path {
                AnyValue::String(b) => b.to_string(),
                _ => {
                    eprintln!("Unexpected value type for string");
                    return Ok::<(), PolarsError>(());
                }
            };

            let bytes = match bytes {
                AnyValue::Binary(b) => b,
                _ => {
                    eprintln!("Unexpected value type for bytes");
                    return Ok(());
                }
            };

            let path = Path::new(&output_display).join(filename.clone());

            write_file(path, bytes)?;

            Ok(())
        })?;
    }

    println!("Done!");

    Ok(())
}
