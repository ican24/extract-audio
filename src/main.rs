use polars::prelude::*;

fn main() -> Result<(), PolarsError> {
    println!("Hello, world!");

    let lf = LazyFrame::scan_parquet("train-00000-of-00003.parquet", Default::default())?
        .select([col("audio")]);

    // iterate over the first 10 rows
    let df = lf.collect()?.head(Some(10));

    for row in df.iter() {
        let struct_series = row.struct_()?;

        // let field_names = struct_series.fields_as_series().iter().map(|f| f.name().to_string()).collect::<Vec<_>>();

        let all_bytes = struct_series.field_by_name("bytes")?;
        let all_paths = struct_series.field_by_name("path")?;

        for (idx, path) in all_paths.iter().enumerate() {
            let bytes = all_bytes.get(idx)?;
            println!("path: {}, bytes: {}", path, bytes);
        }
    }

    // read all data
    Ok(())
}
