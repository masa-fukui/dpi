# dpi (dp-improved)

An improved version of [dp](https://gist.github.com/masa-fukui/9cc56ca66048f8ec8d34cd3fec8b568d). 

DPI (DuckDB Parquet/CSV Inspector) is a CLI tool that lets users inspect Parquet and CSV files using DuckDB.

It accepts a file path or pattern, detects the file format, creates a temporary DuckDB database and table, and launches an interactive DuckDB CLI session for querying. 


## Installation
```sh
$ go install github.com/masa-fukui/dpi@latest
```

## Usage
```
Usage:
  dpi <file or pattern> [flags]

Examples:
  dpi data.parquet
  dpi *.parquet
  dpi data.csv
  dpi -s data.csv     # With strict mode for CSV

Flags:
  -h, --help      help for dpi
  -s, --strict    Enable strict mode (for CSV files)
  -v, --version   version for dpi
```
