use std::fs::{create_dir_all, read_dir, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use polars::prelude::*;
use rayon::{prelude::*, ThreadPoolBuilder};

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

    /// Number of threads to use for processing
    #[arg(long, default_value_t = 3)]
    threads: usize,
}

fn arrow_to_parquet(filename: &Path) -> Result<DataFrame> {
    let file = File::open(filename)
        .with_context(|| format!("Failed to open arrow file: {}", filename.display()))?;
    let reader = StreamReader::try_new(file, None)
        .context("Failed to create arrow stream reader")?;

    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<_, _>>()
        .context("Failed to collect record batches from arrow file")?;
    let df = batches_to_parquet(&batches)
        .context("Failed to convert arrow batches to parquet for DataFrame")?;

    Ok(df)
}

fn batches_to_parquet(batches: &[RecordBatch]) -> Result<DataFrame> {
    // In-memory buffer to avoid writing to a temporary file on disk
    let tmp_file = tempfile::tempfile()?;

    // Write the batches to the file
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(tmp_file, batches[0].schema(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    } // writer goes out of scope and finishes writing

    let tmp_file = writer.into_inner()?;

    // Read in parquet file
    let df = ParquetReader::new(tmp_file)
        .with_columns(Some(vec!["audio".to_string()]))
        .finish()?;

    Ok(df)
}

fn read_parquet(filename: &Path) -> Result<DataFrame> {
    let file = File::open(filename)
        .with_context(|| format!("Failed to open parquet file: {}", filename.display()))?;

    let df = ParquetReader::new(file)
        .with_columns(Some(vec!["audio".to_string()]))
        .finish()
        .context("Failed to read parquet file into DataFrame")?;

    Ok(df)
}

fn write_file(filename: &Path, data: &[u8]) -> Result<()> {
    // Skip if the file already exists. Using `Path::try_exists` is slightly more robust.
    if filename.try_exists()? {
        return Ok(());
    }

    // Write the file
    let mut file = File::create(filename)?;
    file.write_all(data)?;

    Ok(())
}

fn process_file(filename: &Path, format: Format, output_dir: &Path) -> Result<usize> {
    // Convert the file to a DataFrame
    let df = match format {
        Format::Arrow => arrow_to_parquet(filename)
            .with_context(|| format!("Error processing arrow file {}", filename.display()))?,
        Format::Parquet => read_parquet(filename)
            .with_context(|| format!("Error processing parquet file {}", filename.display()))?,
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

            let _ = write_file(&path, bytes);

            Ok(())
        })?;
    }
    Ok(num_rows)
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Configure the global thread pool for Rayon
    ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()?;

    if !args.input.is_some() && !args.input_dir.is_some() {
        eprintln!("Either --input or --input-dir must be provided.");
        process::exit(1);
    }

    // Create the output folder if it doesn't exist
    create_dir_all(&args.output)
        .with_context(|| format!("Failed to create output directory: {}", args.output.display()))?;

    if let Some(input_file) = args.input {
        if !input_file.is_file() {
            eprintln!("Input is not a file: {}", input_file.display());
            process::exit(1);
        }
        println!("Processing file: {}...", input_file.display());
        let rows = process_file(&input_file, args.format, &args.output)?;
        println!("Total number of rows processed: {}", rows);
    }

    if let Some(input_dir) = args.input_dir {
        if !input_dir.is_dir() {
            eprintln!(
                "Input directory does not exist or is not a directory: {}",
                input_dir.display()
            );
            process::exit(1);
        }

        let files_to_process: Vec<_> = read_dir(input_dir)?
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

        files_to_process.into_iter().for_each(|entry| {
            let path = entry.path();
            println!("Processing file: {}...", path.display());
            match process_file(&path, args.format, &args.output) {
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
