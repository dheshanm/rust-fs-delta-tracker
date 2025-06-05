use anyhow::Ok;
use std::io::Write as _;

/// Walk the directory in parallel, printing formatted TSV lines,
#[tracing::instrument(
    skip(output_tsv_file, data_root, progress_log_interval),
)]
pub async fn walk_directory(
    data_root: std::path::PathBuf,
    progress_log_interval: u64,
    scan_id: i32,
    output_tsv_file: std::path::PathBuf,
) -> anyhow::Result<std::collections::HashMap<String, String>> {
    // 1) channel
    let (tx, rx) = crossbeam_channel::unbounded::<String>();
    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(0);

    // 2) progress / done flags
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // 3) writer thread
    let writer_handle = {
        let rx = rx;
        std::thread::spawn(move || {
            // open file or stdout …
            let mut out: Box<dyn std::io::Write> = {
                if let Some(p) = output_tsv_file.parent() {
                    std::fs::create_dir_all(p).unwrap();
                }
                let f = std::fs::File::create(output_tsv_file).unwrap();
                Box::new(std::io::BufWriter::new(f))
            };

            for line in rx {
                let _ = out.write_all(line.as_bytes());
            }
            let _ = out.flush();
        })
    };

    // 4) progress thread
    let progress_handle = {
        let counter = counter.clone();
        // tick channel emits a `()` every `progress_log_interval` seconds
        let ticker = crossbeam_channel::tick(std::time::Duration::from_secs(progress_log_interval));
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut last_cnt = 0;
            let mut last_t = start;

            loop {
                // select! will unblock either on a tick **or** on stop_rx
                crossbeam_channel::select! {
                    // we’ve been told to stop
                    recv(stop_rx) -> _ => {
                        break;
                    },
                    // it’s time to log progress
                    recv(ticker) -> _ => {
                        let now = std::time::Instant::now();
                        let total = counter.load(std::sync::atomic::Ordering::Relaxed);
                        let interval_secs = now.duration_since(last_t).as_secs_f64().max(1e-9);
                        let interval_cnt  = total - last_cnt;
                        let rate_now = interval_cnt as f64 / interval_secs;
                        let total_secs = now.duration_since(start).as_secs();
                        let rate_all = total as f64 / total_secs as f64;
                        let hh = total_secs / 3600;
                        let mm = (total_secs % 3600) / 60;
                        let ss = total_secs % 60;

                        tracing::info!(
                            "📊 Progress: {} files in {:02}:{:02}:{:02}, {:.1} f/s (last {}s), {:.1} f/s (overall)",
                            total, hh, mm, ss,
                            rate_now, progress_log_interval, rate_all
                        );

                        last_cnt = total;
                        last_t = now;
                    }
                }
            }
        })
    };

    // 5) do the blocking parallel walk
    let tx2 = tx.clone();
    let counter2 = counter.clone();
    let done2 = done.clone();
    let root = data_root.clone();

    let start = std::time::Instant::now();
    tracing::debug!("🔍 Starting directory walk in parallel...");

    tokio::task::spawn_blocking(move || {
        let mut builder = ignore::WalkBuilder::new(root);
        builder.ignore(false).hidden(false).git_ignore(false);

        builder.build_parallel().run(|| {
            let tx = tx2.clone();
            let cnt = counter2.clone();
            Box::new(move |res| {
                if let std::result::Result::Ok(ent) = res {
                    if let Some(ft) = ent.file_type() {
                        if ft.is_file() {
                            if let std::result::Result::Ok(meta) = ent.metadata() {
                                let fname = ent.file_name().to_string_lossy();
                                let ext = ent
                                    .path()
                                    .extension()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("unknown");
                                let size = meta.len();
                                let mtime = meta
                                    .modified()
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map(|d| {
                                        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(d.as_secs() as i64, 0)
                                            .unwrap_or_default();
                                        dt.to_rfc3339()
                                    })
                                    .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

                                let line = format!(
                                    "{}\t{}\t{}\t{}\t{}\t{}\n",
                                    fname,
                                    ext,
                                    ent.path().display(),
                                    size,
                                    mtime,
                                    scan_id
                                );
                                cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let _ = tx.send(line);
                            }
                        }
                    }
                }
                ignore::WalkState::Continue
            })
        });

        // done walking: drop all clones in this thread …
        drop(tx2);
        done2.store(true, std::sync::atomic::Ordering::Relaxed);
    })
    .await?; // wait until the walk really finishes

    // drop the original TX here so that the writer thread sees EOF
    tracing::debug!("📂 Directory walk completed, dropping sender...");
    drop(tx);

    // signal the progress thread to stop
    tracing::debug!("🔚 Signaling progress thread to stop...");
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = stop_tx.send(());

    // 6) wait for both threads to finish
    tracing::debug!("⏳ Waiting for progress and writer threads to finish...");
    let _ = progress_handle.join();
    let _ = writer_handle.join();

    // 7) final stats
    let total = counter.load(std::sync::atomic::Ordering::Relaxed) as f64;
    let elapsed = std::time::Instant::now().duration_since(start).as_secs_f64();
    tracing::info!(
        "📊 Final stats: {} files in {:.1}s ({:.1} f/s)",
        total as u64,
        elapsed,
        total / elapsed
    );

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("data_root".to_string(), data_root.to_string_lossy().to_string());
    metadata.insert(
        "crawl_timer_duration_s".to_string(),
        elapsed.to_string(),
    );
    metadata.insert(
        "total_files_processed".to_string(),
        total.to_string(),
    );
    metadata.insert(
        "crawler_files_per_second".to_string(),
        (total / elapsed).to_string(),
    );


    Ok(metadata)
}
