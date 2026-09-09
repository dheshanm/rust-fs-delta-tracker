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
    /// If the path ends with `.gz` the file is written as gzip-compressed (compatible with QDirStat);
    /// otherwise it is written as plain text.
    #[arg(long, env = "OUTPUT_CACHE_FILE")]
    output_cache_file: std::path::PathBuf,

    /// Number of threads to use for parallel directory walking.
    /// Higher values can improve throughput on network filesystems (NFS).
    /// Default is number of logical CPUs.
    #[arg(long, env = "NUM_THREADS", default_value_t = num_cpus::get())]
    num_threads: usize,

    /// Skip computing a content fingerprint (hash) for each regular file.
    /// Use this flag to speed up scans.
    #[arg(long, env = "SKIP_FINGERPRINT")]
    skip_fingerprint: bool,

    /// Cross filesystem boundaries during the walk.
    /// By default the crawler stays on the same filesystem as the scan root.
    #[arg(long, env = "CROSS_FILESYSTEMS")]
    cross_filesystems: bool,

    /// Zero the size field for directory entries.
    /// Useful for distributed filesystems (e.g. CephFS) that report the
    /// subtree total as the directory inode's own size.
    #[arg(long, env = "IGNORE_DIR_SIZE")]
    ignore_dir_size: bool,

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
    tracing::info!("🧵 Number of threads: {}", opt.num_threads);
    tracing::info!("🫆 Fingerprinting: {}", !opt.skip_fingerprint);
    tracing::info!("🌐 Cross filesystems: {}", opt.cross_filesystems);
    tracing::info!("📁 Ignore dir size: {}", opt.ignore_dir_size);
    tracing::info!(
        "📝 Log file: {}",
        opt.log_file
            .as_deref()
            .unwrap_or(std::path::Path::new("logs/app.log"))
            .display()
    );
    tracing::info!("{}", "=".repeat(50));

    // Compression is implied by the file extension: files ending in `.gz` are gzip-compressed.
    let compress = opt.output_cache_file.extension().map_or(false, |ext| ext == "gz");
    tracing::info!("🗜️ Compress output: {}", compress);

    // Walk the directory and process files
    tracing::info!("🔍 Starting directory walk...");
    crawler::walk_directory(opt.data_root, opt.progress_interval, opt.output_cache_file.clone(), opt.num_threads, !opt.skip_fingerprint, compress, opt.cross_filesystems, opt.ignore_dir_size)
        .await
        .map_err(|e| {
            tracing::error!("Failed to walk directory: {}", e);
            anyhow::anyhow!("Directory walk failed: {}", e)
        })?;
    tracing::info!("🔍 Directory walk completed");

    // Print absolute path of the output cache file
    tracing::info!("📂 Output cache file absolute path: {}", opt.output_cache_file.canonicalize()?.display());
    tracing::info!("📂 Output cache file size: {}", bytesize::ByteSize(std::fs::metadata(&opt.output_cache_file)?.len()));


    tracing::info!("✅ Filesystem crawler finished successfully");

    Ok(())
}
