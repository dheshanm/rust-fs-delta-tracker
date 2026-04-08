# fs-delta-tracker

A high-performance Rust CLI tool to crawl large file systems, export metadata in the open [QDirStat Cache File Format V2.0](https://github.com/shundhammer/qdirstat/blob/master/doc/cache-file-format.txt), load into PostgreSQL, and compute filesystem deltas over time. Designed for change tracking (added, modified, deleted files and directories) and volume analytics.

## Features

- Parallel directory walk using `ignore::WalkBuilder`
- Real-time progress logging (entries scanned, rate, duration)
- Output in **QDirStat V2.0 cache format** — compatible with the [QDirStat](https://github.com/shundhammer/qdirstat) application (Similar to [`qdirstat-cache-writer` Perl script](https://github.com/shundhammer/qdirstat/blob/master/scripts/qdirstat-cache-writer))
  - Tracks files (`F`), directories (`D`), and symlinks (`L`)
  - Records uid, gid, permissions (octal), mtime (hex), and size with K/M/G suffixes
  - Embeds blake3 fingerprints via the `fingerprint:` optional field
  - Paths are URL-encoded per the QDirStat spec
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
         --database-url <DATABASE_URL>  PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname" [env: DATABASE_URL=]
         --log-file <LOG_FILE>          Path to log file (default: logs/app.log) [env: LOG_FILE=]
   -h, --help                         Print help
   -V, --version                      Print version
   ```

3. **Run the Tracker**
Use the `fs_delta_tracker` binary to start scanning:
   ```text
   Command-line tool to scan a filesystem directory and track changes in PostgreSQL

   Usage: fs_delta_tracker [OPTIONS] --data-root <DATA_ROOT> --database-url <DATABASE_URL>

   Options:
   -d, --data-root <DATA_ROOT>
            The directory to scan [env: DATA_ROOT=]
         --database-url <DATABASE_URL>
            PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname" [env: DATABASE_URL=]
         --log-file <LOG_FILE>
            Path to log file (default: logs/app.log) [env: LOG_FILE=]
         --progress-interval <PROGRESS_INTERVAL>
            Progress logging interval in seconds. Default is 30 seconds [env: PROGRESS_INTERVAL=] [default: 30]
         --num-threads <NUM_THREADS>
            Number of threads to use for parallel directory walking. Higher values can improve throughput on network filesystems (NFS). Default is number of logical CPUs [env: NUM_THREADS=] [default: 12]
         --cache-file <CACHE_FILE>
            Path for the QDirStat cache file. Defaults to a temporary file in the system temp directory [env: CACHE_FILE=]
         --preserve-cache-file
            Preserve the QDirStat cache file after the scan completes [env: PRESERVE_CACHE_FILE=]
         --skip-fingerprint
            Skip computing a content fingerprint (hash) for each regular file. Use this flag to speed up scans [env: SKIP_FINGERPRINT=]
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

# Run scan — cache file is written to a temp location automatically
./fs-delta-tracker \
  --data-root "$DATA_ROOT" \
  --database-url "$DATABASE_URL"
```

### Advanced: split crawler and ingest

For large scans it can be useful to run the crawler and the DB ingest separately.
The `crawler` binary writes a QDirStat cache file; `finish_scan` ingests it:

```bash
# 1. Register the scan and obtain a scan_id
scan_id=$(./start_scan --data-root "$DATA_ROOT" --database-url "$DATABASE_URL")

# 2. Crawl — produces a QDirStat V2.0 cache file
./crawler \
  --data-root "$DATA_ROOT" \
  --output-cache-file "./output/scan_${scan_id}.qdirstat.cache"

# 3. Ingest cache file + run processing SQL
./finish_scan \
  --database-url "$DATABASE_URL" \
  --scan-id "$scan_id" \
  --output-cache-file "./output/scan_${scan_id}.qdirstat.cache" \
  --sql-file "./assets/templates/sql/process_staging_v2.sql"
```

You can also ingest a cache file produced by the external `qdirstat-cache-writer` Perl script
or the QDirStat application directly ("File → Write Cache File"), as long as it is V1.0 or V2.0 format (plain text or gzip).

## How It Works

1. **Setup & Logging**
   Initializes `tracing` subscriber with console + daily rotating file.

2. **Database Connection**
   Connects to Postgres via `tokio-postgres`, spawns connection task.

3. **Start Scan Record**
   Inserts a new scan row, returning `scan_id`.

4. **Parallel Directory Walk**
   - Spawns a blocking task to walk the filesystem in parallel using `ignore::WalkBuilder`
   - For each entry: collect metadata (name, path, size, mtime, uid, gid, permissions)
   - For file entries: compute a blake3 fingerprint (sampled for large files)
   - Emit a QDirStat V2.0 cache line (`F`/`D`/`L`) over a channel to a writer thread
   - Progress thread logs entry count, rate, and elapsed time every N seconds

5. **Cache File Load & Processing**
   - Parse the QDirStat cache file (supports V1.0/V2.0, plain text or gzip)
   - Bulk-load entries into the `staging_files` staging table via `COPY FROM STDIN`
   - Apply SQL template (`templates/sql/process_staging_v2.sql`) with `scan_id` substitution to compute adds/modifies/deletes and update `filesystem.files`
   - Clear staging table

6. **Finalize Scan**
   - Compute entry counts, data volumes, and deltas
   - Update final results in `filesystem.scan_runs`

## Configuration

You can override defaults with environment variables or flags:

- `DATA_ROOT` / `--data-root`
- `DATABASE_URL` / `--database-url`
- `LOG_FILE` / `--log-file`
- `PROGRESS_INTERVAL` / `--progress-interval`
- `NUM_THREADS` / `--num-threads`
- `OUTPUT_CACHE_FILE` / `--output-cache-file` *(crawler / finish_scan binaries)*
- `SCAN_ID` / `--scan-id` *(crawler / finish_scan binaries)*

Place a `.env` file in the working directory with:

```dotenv
DATABASE_URL=postgres://user:password@hostname.domain.org:5432/fs_tracker_db
DATA_ROOT=/data/predict/data_from_nda/PHOENIX/PROTECTED

LOG_FILE=data/logs/app.log
OUTPUT_CACHE_FILE=data/cache/scan.qdirstat.cache
```

## Database Schema

The schema lives in `assets/templates/sql/init_db.sql` and uses the `filesystem` schema with four tables:

| Table | Purpose |
|---|---|
| `scan_runs` | One row per scan; holds root, timestamps, counts, and JSONB metadata |
| `files` | Current known state of every file/directory/symlink |
| `file_changes` | Auditable record of every add/modify/delete per scan |
| `staging_files` | Unlogged scratch table; loaded from cache file then cleared |

Key columns added for QDirStat compatibility: `entry_type` (F/D/L), `file_extension`, `uid`, `gid`, `permissions` (full Unix mode bits as integer).

Paths are indexed with PostgreSQL `ltree` (via `entry_to_ltree(path, is_dir)`) enabling efficient subtree queries.

## Development

- Templates under `assets/templates/sql/`
- Crawling logic and QDirStat cache writer in `src/lib/crawler.rs`
- QDirStat cache parser and DB loader in `src/lib/qdirstat.rs`
- Database & data logic in `src/lib/data.rs` and `src/lib/db.rs`
- Logging setup in `src/lib/logging.rs`


Lint & format:

```bash
cargo fmt
cargo clippy
```
