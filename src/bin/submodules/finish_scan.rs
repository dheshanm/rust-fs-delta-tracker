use clap::Parser;
use anyhow::Ok;
use fs_delta_tracker::{logging, data, db};

#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
struct Opt {
    /// PostgreSQL connection string, e.g. "postgres://user:password@localhost/dbname".
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Path to log file (default: logs/app.log).
    #[arg(long, env = "LOG_FILE")]
    log_file: Option<std::path::PathBuf>,

    /// Output TSV file for the scanned files.
    #[arg(long, env = "OUTPUT_TSV_FILE")]
    output_tsv_file: std::path::PathBuf,

    /// Path to the SQL file containing the processing logic.
    #[arg(long, env = "SQL_FILE")]
    sql_file: std::path::PathBuf,

    /// Scan ID to use for importing the data.
    /// This should match the scan_id used when the data was generated.
    #[arg(long, env = "SCAN_ID")]
    scan_id: i32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let opt = Opt::parse();

    let _guard = logging::setup_logging(opt.log_file.as_deref())?;

    tracing::info!("{}", "=".repeat(50));
    tracing::info!("🚀 Starting fs-delta-tracker!");
    tracing::info!("{}", "=".repeat(50));
    tracing::info!(
        "🔗 Database: {}",
        opt.database_url.split('@').last().unwrap_or("***")
    );
    tracing::info!(
        "📁 SQL File: {}",
        opt.sql_file.display()
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

    // Load the TSV file into the staging table
    tracing::info!("📥 Loading TSV file -> staging: {}", opt.output_tsv_file.display());
    data::load_tsv_file(&client, opt.output_tsv_file).await?;
    tracing::info!("📥 TSV file loaded into staging table");

    // Execute the SQL template file
    
    // Construct a HashMap for parameters
    let mut params = std::collections::HashMap::new();
    params.insert("scan_id".to_string(), opt.scan_id.to_string());

    tracing::info!("📄 Executing SQL file: {}", opt.sql_file.display());
    db::execute_sql_template(&client, opt.sql_file, Some(params)).await?;
    tracing::info!("📄 SQL file executed successfully");

    tracing::info!("🗑️ Clearing staging table for scan_id: {}", opt.scan_id);
    data::clear_staging(&client, opt.scan_id).await?;
    tracing::info!("🗑️ Staging table cleared for scan_id: {}", opt.scan_id);

    Ok(())
}
