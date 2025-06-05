use futures::SinkExt;
use tokio::io::AsyncBufReadExt;

#[tracing::instrument]
pub async fn clear_staging(client: &tokio_postgres::Client, scan_id: i32) -> anyhow::Result<()> {
    let query = "DELETE FROM filesystem.staging_files WHERE scan_id = $1";
    client.execute(query, &[&scan_id]).await?;
    Ok(())
}

#[tracing::instrument]
pub async fn get_files_count_by_change_type(
    client: &tokio_postgres::Client,
    scan_id: i32,
    change_type: &str,
) -> anyhow::Result<i64> {
    let query = "
        SELECT COUNT(*)
        FROM filesystem.file_changes
        WHERE scan_id = $1 AND change_type = $2";

    let row = client.query_one(query, &[&scan_id, &change_type]).await?;
    let count: i64 = row.get(0);
    Ok(count)
}

#[tracing::instrument]
pub async fn get_file_size_by_change_type(
    client: &tokio_postgres::Client,
    scan_id: i32,
    change_type: &str,
) -> anyhow::Result<i64> {
    let query = "
        SELECT COALESCE(SUM(ABS(COALESCE(new_size_bytes, 0) - COALESCE(old_size_bytes, 0))), 0)::bigint
        FROM filesystem.file_changes
        WHERE scan_id = $1 AND change_type = $2";

    let row = client.query_one(query, &[&scan_id, &change_type]).await?;
    let size: i64 = row.get(0);
    Ok(size)
}

/// Insert a new row into filesystem.scan_runs and return the scan_id
#[tracing::instrument(skip(client, started_at))]
pub async fn start_scan(
    client: &tokio_postgres::Client,
    data_root: &std::path::PathBuf,
    started_at: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<i32> {
    tracing::info!(
        "Starting scan for root: {} at {}",
        data_root.display(),
        started_at
    );
    // Construct a insert statement, returning the scan_id
    let stmt = client
        .prepare(
            "INSERT INTO filesystem.scan_runs (scan_root, started_at) \
            VALUES ($1, $2) RETURNING scan_id",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&data_root.to_string_lossy(), &started_at])
        .await?;

    let scan_id: i32 = row.get(0);
    tracing::info!("Scan started with ID: {}", scan_id);
    Ok(scan_id)
}

#[tracing::instrument(skip(client, input_tsv_file))]
pub async fn load_tsv_file(
    client: &tokio_postgres::Client,
    input_tsv_file: std::path::PathBuf,
) -> anyhow::Result<i32> {
    // Returns the number of rows inserted into the staging table
    let query_header = "
        COPY filesystem.staging_files(
            file_name, file_type, file_path, file_size_bytes, file_mtime, scan_id
        )
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E'\t',
            NULL '',
            HEADER FALSE
        )";

    let file = tokio::fs::File::open(&input_tsv_file).await?;
    let reader = tokio::io::BufReader::new(file);
    let mut lines = reader.lines();

    let writer = client.copy_in(query_header).await?;
    let mut writer = Box::pin(writer);

    let mut line_count = 0;
    while let Some(line) = lines.next_line().await? {
        let line_with_newline = format!("{}\n", line);
        line_count += 1;
        writer
            .send(std::io::Cursor::new(line_with_newline.into_bytes()))
            .await?;
    }

    writer.close().await?;

    Ok(line_count)
}

#[tracing::instrument(skip(client, scan_id, metadata))]
pub async fn finalize_scan(
    client: &tokio_postgres::Client,
    scan_id: i32,
    mut metadata: std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let completed_at = chrono::Utc::now();

    let change_types = ["added", "modified", "deleted"];

    let mut file_counts = std::collections::HashMap::new();
    let mut file_sizes_mb: std::collections::HashMap<String, f64> =
        std::collections::HashMap::new();

    for change_type in &change_types {
        let count = get_files_count_by_change_type(client, scan_id, change_type).await?;
        let size = get_file_size_by_change_type(client, scan_id, change_type).await?;

        file_counts.insert(change_type.to_string(), count);
        // Convert size from bytes to megabytes
        file_sizes_mb.insert(change_type.to_string(), size as f64 / 1024.0 / 1024.0);
    }

    // Update the scan_runs table with all the scan results
    let query = "
        UPDATE filesystem.scan_runs
        SET finished_at = $1,
            total_paths_count = $2,
            added_files_count = $3,
            modified_files_count = $4,
            removed_files_count = $5,
            new_data_mb = $6,
            modified_data_mb = $7,
            deleted_data_mb = $8,
            scan_metadata = $9
        WHERE scan_id = $10";

    let metadata_json = serde_json::to_value(&metadata)
        .map_err(|e| anyhow::anyhow!("Failed to serialize metadata: {}", e))?;

    client
        .execute(
            query,
            &[
                &completed_at,
                &metadata
                    .get("total_files_processed")
                    .unwrap_or(&"0".to_string())
                    .parse::<i64>()
                    .unwrap_or(0),
                &file_counts.get("added").unwrap_or(&0),
                &file_counts.get("modified").unwrap_or(&0),
                &file_counts.get("deleted").unwrap_or(&0),
                &file_sizes_mb.get("added").unwrap_or(&0.0),
                &file_sizes_mb.get("modified").unwrap_or(&0.0),
                &file_sizes_mb.get("deleted").unwrap_or(&0.0),
                &metadata_json,
                &scan_id,
            ],
        )
        .await?;

    metadata.insert("scan_id".to_string(), scan_id.to_string());
    metadata.insert("completed_at".to_string(), completed_at.to_rfc3339());
    metadata.insert(
        "added_files_count".to_string(),
        file_counts.get("added").unwrap_or(&0).to_string(),
    );
    metadata.insert(
        "modified_files_count".to_string(),
        file_counts.get("modified").unwrap_or(&0).to_string(),
    );
    metadata.insert(
        "removed_files_count".to_string(),
        file_counts.get("deleted").unwrap_or(&0).to_string(),
    );
    metadata.insert(
        "new_data_mb".to_string(),
        file_sizes_mb.get("added").unwrap_or(&0.0).to_string(),
    );
    metadata.insert(
        "modified_data_mb".to_string(),
        file_sizes_mb.get("modified").unwrap_or(&0.0).to_string(),
    );
    metadata.insert(
        "deleted_data_mb".to_string(),
        file_sizes_mb.get("deleted").unwrap_or(&0.0).to_string(),
    );

    tracing::info!("📊 Scan metadata:\n{:#?}", metadata);

    Ok(())
}
