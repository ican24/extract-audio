use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

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
    #[arg(long, conflicts_with = "input_dir")]
    input: Option<PathBuf>,

    /// The path to a directory with input files
    #[arg(long, conflicts_with = "input")]
    input_dir: Option<PathBuf>,

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
    let file = File::open(filename)?;

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

fn process_file(
    filename: PathBuf,
    format: Format,
    output_dir: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    // Convert the file to a DataFrame
    let df = match format {
        Format::Arrow => arrow_to_parquet(filename)?,
        Format::Parquet => read_parquet(filename)?,
    };

    let num_rows = df.height();

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
                _ => return Ok::<(), PolarsError>(()),
            };

            let bytes = match bytes {
                AnyValue::Binary(b) => b,
                _ => return Ok(()),
            };

            let path = output_dir.join(filename.clone());
            write_file(path, bytes)?;
            Ok(())
        })?;
    }
    Ok(num_rows)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if !args.input.is_some() && !args.input_dir.is_some() {
        eprintln!("Either --input or --input-dir must be provided.");
        std::process::exit(1);
    }

    // Create the output folder if it doesn't exist
    std::fs::create_dir_all(args.output.clone())?;

    if let Some(input_file) = args.input {
        if !input_file.is_file() {
            eprintln!("Input is not a file: {}", input_file.display());
            std::process::exit(1);
        }
        println!("Processing file: {}", input_file.display());
        let rows = process_file(input_file, args.format, &args.output)?;
        println!("Total number of rows processed: {}", rows);
    }

    if let Some(input_dir) = args.input_dir {
        if !input_dir.is_dir() {
            eprintln!(
                "Input directory does not exist or is not a directory: {}",
                input_dir.display()
            );
            std::process::exit(1);
        }

        let files_to_process: Vec<_> = std::fs::read_dir(input_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().is_file()
                    && entry
                        .path()
                        .extension()
                        .map_or(false, |ext| ext == "parquet" || ext == "arrow")
            })
            .collect();

        let total_rows = AtomicUsize::new(0);

        files_to_process.into_par_iter().for_each(|entry| {
            let path = entry.path();
            println!("Processing file: {}", path.display());
            match process_file(path, args.format, &args.output) {
                Ok(rows) => {
                    total_rows.fetch_add(rows, Ordering::SeqCst);
                }
                Err(e) => eprintln!("Error processing file {}: {}", entry.path().display(), e),
            }
        });

        println!(
            "Total number of rows processed: {}",
            total_rows.load(Ordering::SeqCst)
        );
    }

    println!("Done!");

    Ok(())
}
