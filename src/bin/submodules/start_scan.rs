use clap::Parser;
use fs_delta_tracker::logging;
use fs_delta_tracker::data;

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
        opt.database_url.split('@').last().unwrap_or("***")
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
    tracing::info!("Starting scan with ID: {}", scan_id);

    Ok(())
}

