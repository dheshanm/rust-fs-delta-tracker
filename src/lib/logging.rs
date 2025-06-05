use tracing_subscriber::fmt::writer::MakeWriterExt;

pub fn setup_logging(
    log_file: Option<&std::path::Path>,
) -> anyhow::Result<tracing_appender::non_blocking::WorkerGuard> {
    let log_path = log_file.unwrap_or(std::path::Path::new("logs/app.log"));
    let log_dir = log_path.parent().unwrap_or(std::path::Path::new("."));
    let log_filename = log_path
        .file_name()
        .unwrap_or(std::ffi::OsStr::new("app.log"));

    let file_appender = tracing_appender::rolling::daily(log_dir, log_filename);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_ansi(false)
        .with_writer(std::io::stdout.and(non_blocking))
        .init();

    Ok(guard)
}
