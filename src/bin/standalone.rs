use clap::Parser;
use fs_delta_tracker::crawler;
use fs_delta_tracker::data;
use fs_delta_tracker::db;
use fs_delta_tracker::logging;
use fs_delta_tracker::qdirstat;

static PROJECT_DIR: include_dir::Dir = include_dir::include_dir!("$CARGO_MANIFEST_DIR/assets");

/// Command-line tool to scan a filesystem directory and track changes in PostgreSQL.
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
struct Opt {
    /// The directory to scan
    #[arg(short, long, env = "DATA_ROOT")]
    data_root: std::path::PathBuf,

    /// PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname".
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Path to log file (default: logs/app.log).
    #[arg(long, env = "LOG_FILE")]
    log_file: Option<std::path::PathBuf>,

    /// Progress logging interval in seconds.
    /// Default is 30 seconds.
    #[arg(long, env = "PROGRESS_INTERVAL", default_value_t = 30)]
    progress_interval: u64,

    /// Number of threads to use for parallel directory walking.
    /// Higher values can improve throughput on network filesystems (NFS).
    /// Default is number of logical CPUs.
    #[arg(long, env = "NUM_THREADS", default_value_t = num_cpus::get())]
    num_threads: usize,

    /// Path for the QDirStat cache file.
    /// Defaults to a temporary file in the system temp directory.
    #[arg(long, env = "CACHE_FILE")]
    cache_file: Option<std::path::PathBuf>,

    /// Preserve the QDirStat cache file after the scan completes.
    #[arg(long, env = "PRESERVE_CACHE_FILE", default_value_t = false)]
    preserve_cache_file: bool,

    /// Skip computing a content fingerprint (hash) for each regular file.
    /// Use this flag to speed up scans.
    #[arg(long, env = "SKIP_FINGERPRINT")]
    skip_fingerprint: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let opt = Opt::parse();

    let _guard = logging::setup_logging(opt.log_file.as_deref())?;

    tracing::info!("{}", "=".repeat(50));
    tracing::info!("🚀 Starting fs-delta-tracker!");
    tracing::info!("{}", "=".repeat(50));
    tracing::info!("📁 Scanning root: {}", opt.data_root.display());
    tracing::info!(
        "🔗 Database: {}",
        opt.database_url.split('@').next_back().unwrap_or("***")
    );
    tracing::info!("⏱️ Progress interval: {} seconds", opt.progress_interval);
    tracing::info!("🧵 Number of threads: {}", opt.num_threads);
    tracing::info!(
        "📝 Log file: {}",
        opt.log_file
            .as_deref()
            .unwrap_or(std::path::Path::new("logs/app.log"))
            .display()
    );
    tracing::info!(
        "🗂️ Cache file: {}",
        opt.cache_file
            .as_deref()
            .unwrap_or(std::path::Path::new("temporary"))
            .display()
    );
    tracing::info!(
        "🛡️ Preserve cache file: {}",
        opt.preserve_cache_file
    );
    tracing::info!("{}", "=".repeat(50));

    tracing::info!("🔗 Connecting to database...");
    let (client, connection) =
        tokio_postgres::connect(&opt.database_url, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    tracing::info!("🔗 Connected to database");

    let started_at = chrono::Utc::now();
    let scan_id = data::start_scan(&client, &opt.data_root, started_at).await?;
    tracing::info!("🔍 Scan ID: {}", scan_id);

    // Use a temporary file for output (QDirStat cache format)
    let output_cache_file = opt.cache_file.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("scan_{}.qdirstat.cache", scan_id))
    });
    tracing::info!("📝 Output cache file: {}", output_cache_file.display());

    tracing::info!("🔍 Fingerprinting: {}", !opt.skip_fingerprint);
    tracing::info!("🔍 Starting directory walk...");
    let mut metadata = crawler::walk_directory(
        opt.data_root,
        opt.progress_interval,
        output_cache_file.clone(),
        opt.num_threads,
        !opt.skip_fingerprint,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to walk directory: {}", e);
        anyhow::anyhow!("Directory walk failed: {}", e)
    })?;
    tracing::info!("🔍 Scan completed with ID: {}", scan_id);
    tracing::info!("✅ Filesystem crawler finished successfully");

    tracing::info!(
        "📥 Loading cache file -> staging: {}",
        output_cache_file.display()
    );
    qdirstat::load_qdirstat_file(&client, output_cache_file.clone(), scan_id).await?;
    tracing::info!("📥 Cache file loaded into staging table");

    // Execute the SQL template file
    // Construct a HashMap for parameters
    let mut params = std::collections::HashMap::new();
    params.insert("scan_id".to_string(), scan_id.to_string());

    tracing::info!("📄 Processing staged files...");
    let start_time = std::time::Instant::now();
    let processing_sql = PROJECT_DIR
        .get_file("templates/sql/process_staging_v2.sql")
        .expect("SQL template file not found")
        .contents_utf8()
        .expect("Failed to read SQL template as UTF-8");
    db::execute_sql_template_str(&client, processing_sql, Some(params)).await?;
    let duration = start_time.elapsed();
    tracing::info!("📄 Processed successfully in {:?}", duration);
    metadata.insert(
        "sql_execution_time_s".to_string(),
        duration.as_secs_f64().to_string(),
    );

    tracing::info!("🗑️ Clearing staging table for scan_id: {}", scan_id);
    data::clear_staging(&client, scan_id).await?;
    tracing::info!("🗑️ Staging table cleared for scan_id: {}", scan_id);

    tracing::info!("📊 Updating scan results in database...");
    // Add Hostname to metadata
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    metadata.insert("hostname".to_string(), hostname);
    data::finalize_scan(&client, scan_id, metadata).await?;

    if opt.preserve_cache_file {
        tracing::info!("📁 Cache file preserved at: {}", output_cache_file.display());
    } else {
        tracing::info!("🗑️ Clearing cache file: {}", output_cache_file.display());
        if let Err(e) = std::fs::remove_file(&output_cache_file) {
            tracing::warn!("⚠️ Failed to remove temporary cache file: {}", e);
        } else {
            tracing::info!("🗑️ Temporary cache file removed successfully");
        }
    }

    tracing::info!("✅ Scan completed successfully!");

    Ok(())
}
