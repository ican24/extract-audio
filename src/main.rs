use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;
use rayon::prelude::*;

#[derive(Parser, Debug)]
#[command(version, long_about = None)]
struct Args {
    /// The path to the parquet file
    #[arg(long)]
    parquet_file: PathBuf,

    /// The path to the output files
    #[arg(long)]
    output: PathBuf,

    /// Debug mode
    #[arg(long, default_value = "false")]
    debug: bool,
}

fn main() -> Result<(), PolarsError> {
    // Parse the command line arguments
    let args = Args::parse();

    let output_display: String = args.output.display().to_string();

    // Create the output folder if it doesn't exist
    std::fs::create_dir_all(args.output)?;

    // Read the parquet file
    let df = ParquetReader::new(std::fs::File::open(args.parquet_file)?)
        .with_columns(Some(vec!["audio".to_string()]))
        .finish()?;

    println!("Number of rows: {}", df.height());

    for row in df.iter() {
        let struct_series = row.struct_()?;

        let all_bytes = struct_series.field_by_name("bytes")?;
        let all_paths = struct_series.field_by_name("path")?;

        // Extract files in parallel
        (0..all_bytes.len()).into_par_iter().try_for_each(|idx| {
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

            let formatted_path = format!("{}/{}", output_display, filename);
            let path = Path::new(&formatted_path);
            let bytes_len = bytes.len();

            write_file(path, bytes)?;

            if args.debug {
                println!("path: {}, bytes len: {}", filename, bytes_len);
            }

            Ok(())
        })?;
    }

    println!("Done");

    Ok(())
}

fn write_file(filename: &Path, data: &[u8]) -> std::io::Result<()> {
    // Skip if the file already exists
    if filename.exists() {
        return Ok(());
    }

    // Write the file
    let mut file = File::create(filename)?;
    file.write_all(&data)?;

    Ok(())
}
