use clap::Parser;
use fs_delta_tracker::crawler;
use fs_delta_tracker::data;
use fs_delta_tracker::db;
use fs_delta_tracker::logging;

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
    tracing::info!(
        "📝 Log file: {}",
        opt.log_file
            .as_deref()
            .unwrap_or(std::path::Path::new("logs/app.log"))
            .display()
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

    // Use a temporary file for output
    let output_tsv_file = std::env::temp_dir().join(format!("scan_{}.tsv", scan_id));
    tracing::info!("📝 Output TSV file: {}", output_tsv_file.display());

    tracing::info!("🔍 Starting directory walk...");
    let mut metadata = crawler::walk_directory(
        opt.data_root,
        opt.progress_interval,
        scan_id,
        output_tsv_file.clone(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to walk directory: {}", e);
        anyhow::anyhow!("Directory walk failed: {}", e)
    })?;
    tracing::info!("🔍 Scan completed with ID: {}", scan_id);
    tracing::info!("✅ Filesystem crawler finished successfully");

    tracing::info!(
        "📥 Loading TSV file -> staging: {}",
        output_tsv_file.display()
    );
    data::load_tsv_file(&client, output_tsv_file.clone()).await?;
    tracing::info!("📥 TSV file loaded into staging table");

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
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    metadata.insert("hostname".to_string(), hostname);
    data::finalize_scan(&client, scan_id, metadata).await?;

    tracing::info!("🗑️ Clearing TSV File: {}", output_tsv_file.display());
    // Remove the temporary TSV file
    if let Err(e) = std::fs::remove_file(&output_tsv_file) {
        tracing::warn!("⚠️ Failed to remove temporary TSV file: {}", e);
    } else {
        tracing::info!("🗑️ Temporary TSV file removed successfully");
    }

    tracing::info!("✅ Scan completed successfully!");

    Ok(())
}
