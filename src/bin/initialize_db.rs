use anyhow::Ok;
use clap::Parser;

use fs_delta_tracker::{db, logging};

static PROJECT_DIR: include_dir::Dir = include_dir::include_dir!("$CARGO_MANIFEST_DIR/assets");

/// Command-line tool to initialize the PostgreSQL database for fs-delta-tracker.
#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
struct Opt {
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
    tracing::info!("🚀 Initializing database");
    tracing::info!("{}", "=".repeat(50));
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

    tracing::info!("⚠️ This will drop all existing tables and data in the database!");

    tracing::info!("🔗 Connecting to database...");
    let (client, connection) =
        tokio_postgres::connect(&opt.database_url, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    tracing::info!("🔗 Connected to database");

    let processing_sql = PROJECT_DIR
        .get_file("templates/sql/init_db.sql")
        .expect("SQL template file not found")
        .contents_utf8()
        .expect("Failed to read SQL template as UTF-8");
    db::execute_sql_template_str(&client, processing_sql, None)
        .await
        .map_err(|e| {
            tracing::error!("Failed to execute SQL template: {}", e);
            anyhow::anyhow!("SQL execution failed: {}", e)
        })?;

    tracing::info!("✅ Database initialized successfully!");

    Ok(())
}
