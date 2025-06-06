# fs-delta-tracker

A high-performance Rust CLI tool to crawl large file systems, export metadata to TSV, load into PostgreSQL, and compute filesystem deltas over time. Designed for change tracking (added, modified, deleted files) and volume analytics.

## Features

- Parallel directory walk using `ignore::WalkBuilder`  
- Real-time progress logging (files scanned, rate, duration)  
- TSV output of file records (name, extension, path, size, mtime, scan_id)  
- Automatic PostgreSQL staging & finalization  
- SQL templating for custom processing (`process_staging_v2.sql`)  
- Rolling daily logs + flexible log configuration via `tracing`

## Requirements

- Rust (tested with 1.87.0)
- PostgreSQL (tested on 16+)
- `cargo` build tool

## Installation

```bash
git clone https://github.com/dheshanm/rust-fs-delta-tracker.git
cd rust-fs-delta-tracker
cargo build --release
```

The resulting binary will be in `target/release/fs-delta-tracker`.

## Usage

1. **Set Environment Variables**  
   Define `DATA_ROOT` and `DATABASE_URL` in your environment or `.env` file.
2. **Initialize Database**  
   Use the `initialize_db` binary or `assets/templates/sql/init_db.sql` to create the necessary tables.

   ```text
   Command-line tool to initialize the PostgreSQL database for fs-delta-tracker

   Usage: initialize_db [OPTIONS] --database-url <DATABASE_URL>

   Options:
         --database-url <DATABASE_URL>  PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname"
         --log-file <LOG_FILE>          Path to log file (default: logs/app.log) [env: LOG_FILE=data/logs/app.log]
   -h, --help                         Print help
   -V, --version                      Print version
   ```

3. **Run the Tracker**
Use the `fs_delta_tracker` binary to start scanning:
   ```text
   Command-line tool to scan a filesystem directory and track changes in PostgreSQL

   Usage: fs_delta_tracker [OPTIONS] --data-root <DATA_ROOT> --database-url <DATABASE_URL> --output-tsv-file <OUTPUT_TSV_FILE>

   Options:
   -d, --data-root <DATA_ROOT>
            The directory to scan
         --database-url <DATABASE_URL>
            PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname" 
         --log-file <LOG_FILE>
            Path to log file (default: logs/app.log)
         --progress-interval <PROGRESS_INTERVAL>
            Progress logging interval in seconds. Default is 30 seconds
         --output-tsv-file <OUTPUT_TSV_FILE>
            Output TSV file for the scanned files. If not provided, output will be printed to stdout
   -h, --help
            Print help
   -V, --version
            Print version
   ```

Example:

```bash
export DATA_ROOT=/mnt/data
export DATABASE_URL="postgres://user:pass@localhost/mydb"

# Initialize the database (run once)
./initialize_db --database-url "$DATABASE_URL" --log-file "./logs/app.log"

# Run scan, write TSV to ./output/files.tsv, log to default file
./fs-delta-tracker \
  --data-root "$DATA_ROOT" \
  --output-tsv-file "./output/files.tsv" \
  --database-url "$DATABASE_URL"
```

## How It Works

1. **Setup & Logging**  
   Initializes `tracing` subscriber with console + daily rotating file.

2. **Database Connection**  
   Connects to Postgres via `tokio-postgres`, spawns connection task.

3. **Start Scan Record**  
   Inserts a new scan row, returning `scan_id`.

4. **Parallel Directory Walk**  
   - Spawns a blocking task to walk files in parallel  
   - For each file: collect `(name, ext, path, size, mtime, scan_id)`  
   - Send TSV line over channel to a writer thread  
   - Progress thread logs every N seconds  

5. **TSV Load & Processing**  
   - Bulk-load TSV into staging table  
   - Apply custom SQL template (`templates/sql/process_staging_v2.sql`) with `scan_id` param  
   - Clear staging table  

6. **Finalize Scan**  
   - Compute counts, volumes, deltas  
   - Update final results in database  

## Configuration

You can override defaults with environment variables or flags:

- `DATA_ROOT` / `--data-root`  
- `DATABASE_URL` / `--database-url`  
- `LOG_FILE` / `--log-file`  
- `PROGRESS_INTERVAL` / `--progress-interval`  
- `OUTPUT_TSV_FILE` / `--output-tsv-file`

Place a `.env` file in the working directory with:

```dotenv
DATABASE_URL=postgres://user:password@hostname.domain.org:5432/fs_tracker_db
DATA_ROOT=/data/predict/data_from_nda/PHOENIX/PROTECTED

LOG_FILE=data/logs/app.log
OUTPUT_TSV_FILE=data/tsv_files/tsv_file.tsv
```

## Development

- Templates under `assets/templates/sql/`  
- Crawling logic in `src/lib/crawler.rs`  
- Database & data logic in `src/lib/data.rs` and `src/lib/db.rs`  
- Logging setup in `src/lib/logging.rs`


Lint & format:

```bash
cargo fmt
cargo clippy
```
