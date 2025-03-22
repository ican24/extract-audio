# `extract-audio-parquet`

## Usage

```
Usage: extract-audio-parquet [OPTIONS] --input <INPUT> --output <OUTPUT>

Options:
      --input <INPUT>    The path to the input file
      --format <FORMAT>  File format [default: parquet] [possible values: arrow, parquet]
      --output <OUTPUT>  The path to the output files
      --debug            Debug mode
  -h, --help             Print help
  -V, --version          Print version
```

## Example

```
extract-audio-parquet --format parquet --input train-00000-of-00010.parquet --output files-parquet/

extract-audio-parquet --format arrow --input data-00000-of-01189.arrow --output files-arrow/
```
