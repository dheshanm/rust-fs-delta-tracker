use anyhow::Ok;
use clap::Parser;

use fs_delta_tracker::{logging, crawler};

/// A Tokio-based, multi-threaded filesystem crawler/scanner.
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
struct Opt {
    /// The directory to scan
    #[arg(short, long, env = "DATA_ROOT")]
    data_root: std::path::PathBuf,

    /// Path to log file (default: logs/app.log).
    #[arg(long, env = "LOG_FILE")]
    log_file: Option<std::path::PathBuf>,

    /// Progress logging interval in seconds.
    /// Default is 30 seconds.
    #[arg(long, env = "PROGRESS_INTERVAL", default_value_t = 30)]
    progress_interval: u64,

    /// Output QDirStat cache file for the scanned entries.
    #[arg(long, env = "OUTPUT_CACHE_FILE")]
    output_cache_file: std::path::PathBuf,

    /// Scan ID to use for this scan.
    #[arg(long, env = "SCAN_ID")]
    scan_id: i32,

    /// Number of threads to use for parallel directory walking.
    /// Higher values can improve throughput on network filesystems (NFS).
    /// Default is number of logical CPUs.
    #[arg(long, env = "NUM_THREADS", default_value_t = num_cpus::get())]
    num_threads: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let opt = Opt::parse();

    let _guard = logging::setup_logging(opt.log_file.as_deref())?;

    tracing::info!("{}", "=".repeat(50));
    tracing::info!("🚀 Starting filesystem crawler");
    tracing::info!("{}", "=".repeat(50));
    tracing::info!("📁 Scanning root: {}", opt.data_root.display());
    tracing::info!("⏱️ Progress interval: {} seconds", opt.progress_interval);
    tracing::info!("📊 Output cache file: {}", opt.output_cache_file.display());
    tracing::info!("🔍 Scan ID: {}", opt.scan_id);
    tracing::info!("🧵 Number of threads: {}", opt.num_threads);
    tracing::info!(
        "📝 Log file: {}",
        opt.log_file
            .as_deref()
            .unwrap_or(std::path::Path::new("logs/app.log"))
            .display()
    );
    tracing::info!("{}", "=".repeat(50));

    // Walk the directory and process files
    tracing::info!("🔍 Starting directory walk...");
    crawler::walk_directory(opt.data_root, opt.progress_interval, opt.scan_id, opt.output_cache_file, opt.num_threads)
        .await
        .map_err(|e| {
            tracing::error!("Failed to walk directory: {}", e);
            anyhow::anyhow!("Directory walk failed: {}", e)
        })?;
    tracing::info!("🔍 Directory walk completed");

    // tracing::info!("🔍 Scan completed with ID: {}", scan_id);
    tracing::info!("✅ Filesystem crawler finished successfully");

    Ok(())
}
