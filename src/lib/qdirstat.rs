use anyhow::Context;
use futures::SinkExt;
use std::io::BufRead;
use std::path::Path;

/// Represents a single entry parsed from a QDirStat cache file.
#[derive(Debug, Clone)]
pub struct QDirStatEntry {
    pub entry_type: String,
    pub file_path: String,
    pub file_name: String,
    pub file_extension: Option<String>,
    pub file_size_bytes: i64,
    pub uid: Option<i32>,
    pub gid: Option<i32>,
    pub permissions: Option<i32>,
    pub mtime_unix: i64,
    pub file_fingerprint: Option<String>,
    pub links: Option<i64>,
    pub blocks: Option<i64>,
}

/// URL-decode a QDirStat path (percent-encoded bytes like %20 → space).
fn url_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// Parse size field: bare number in bytes, or with K/M/G suffix.
fn parse_size(s: &str) -> anyhow::Result<i64> {
    if let Some(num) = s.strip_suffix('K') {
        let n: i64 = num.parse().context("invalid size number before K")?;
        Ok(n * 1024)
    } else if let Some(num) = s.strip_suffix('M') {
        let n: i64 = num.parse().context("invalid size number before M")?;
        Ok(n * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix('G') {
        let n: i64 = num.parse().context("invalid size number before G")?;
        Ok(n * 1024 * 1024 * 1024)
    } else {
        s.parse::<i64>().context("invalid size number")
    }
}

/// Parse mtime field: hex (0x...) or decimal seconds since epoch.
fn parse_mtime(s: &str) -> anyhow::Result<i64> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        i64::from_str_radix(hex, 16).context("invalid hex mtime")
    } else {
        s.parse::<i64>().context("invalid decimal mtime")
    }
}

/// Map a QDirStat type string to our canonical entry_type (F, D, L).
/// Returns None for unsupported types (BlockDev, CharDev, FIFO, Socket).
fn normalize_entry_type(raw: &str) -> Option<&'static str> {
    match raw.to_ascii_uppercase().as_str() {
        "F" => Some("F"),
        "D" => Some("D"),
        "L" => Some("L"),
        _ => None,
    }
}

/// Extract file extension from a file name (returns None for dirs/links or no extension).
fn extract_extension(file_name: &str, entry_type: &str) -> Option<String> {
    if entry_type != "F" {
        return None;
    }
    std::path::Path::new(file_name)
        .extension()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Detected cache file version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheVersion {
    V1,
    V2,
}

/// Parse a QDirStat cache file (V1.0 or V2.0, plain text or gzip).
///
/// Returns a vector of parsed entries. Unsupported entry types (BlockDev, CharDev,
/// FIFO, Socket) are logged as warnings and skipped.
pub fn parse_qdirstat_cache_file(path: &Path) -> anyhow::Result<Vec<QDirStatEntry>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open cache file: {}", path.display()))?;

    let reader: Box<dyn BufRead> = if path
        .extension()
        .and_then(|e| e.to_str())
        .map_or(false, |e| e == "gz")
    {
        Box::new(std::io::BufReader::new(flate2::read::GzDecoder::new(file)))
    } else {
        Box::new(std::io::BufReader::new(file))
    };

    let mut entries = Vec::new();
    let mut current_dir: Option<String> = None;
    let mut version: Option<CacheVersion> = None;

    for (line_no, line_result) in reader.lines().enumerate() {
        let line = line_result.with_context(|| format!("read error at line {}", line_no + 1))?;
        let trimmed = line.trim();

        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }

        // Header detection
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let inner = &trimmed[1..trimmed.len() - 1].to_ascii_lowercase();
            if inner.contains("2.0") {
                version = Some(CacheVersion::V2);
            } else if inner.contains("1.0") {
                version = Some(CacheVersion::V1);
            } else if inner.contains("cache file") {
                // Unknown version, default to V2
                tracing::warn!("Unknown cache file version: {}, assuming V2.0", trimmed);
                version = Some(CacheVersion::V2);
            }
            continue;
        }

        // Skip comment lines
        if trimmed.starts_with('#') {
            continue;
        }

        // Must have seen a header by now
        let ver = version.context("data line before header in cache file")?;

        // Parse data line: split by whitespace
        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        if fields.len() < 3 {
            tracing::warn!("Skipping short line {} (only {} fields)", line_no + 1, fields.len());
            continue;
        }

        let raw_type = fields[0];
        let entry_type = match normalize_entry_type(raw_type) {
            Some(t) => t,
            None => {
                tracing::warn!(
                    "Skipping unsupported entry type '{}' at line {}",
                    raw_type,
                    line_no + 1
                );
                continue;
            }
        };

        let raw_path_or_name = url_decode(fields[1]);
        let size_field = fields[2];

        // Determine mandatory field positions based on version
        let (uid, gid, permissions, mtime_field_idx) = match ver {
            CacheVersion::V2 => {
                // V2: type path size uid gid perm mtime [optional...]
                if fields.len() < 7 {
                    tracing::warn!("Skipping V2 line {} (only {} fields, need 7)", line_no + 1, fields.len());
                    continue;
                }
                let uid: i32 = fields[3].parse().unwrap_or(0);
                let gid: i32 = fields[4].parse().unwrap_or(0);
                let perm: i32 = i32::from_str_radix(fields[5], 8).unwrap_or(0);
                (Some(uid), Some(gid), Some(perm), 6)
            }
            CacheVersion::V1 => {
                // V1: type path size mtime [optional...]
                if fields.len() < 4 {
                    tracing::warn!("Skipping V1 line {} (only {} fields, need 4)", line_no + 1, fields.len());
                    continue;
                }
                (None, None, None, 3)
            }
        };

        let size = parse_size(size_field)
            .with_context(|| format!("bad size at line {}", line_no + 1))?;
        let mtime = parse_mtime(fields[mtime_field_idx])
            .with_context(|| format!("bad mtime at line {}", line_no + 1))?;

        // Parse optional fields after mtime
        let mut fingerprint: Option<String> = None;
        let mut links: Option<i64> = None;
        let mut blocks: Option<i64> = None;

        let mut i = mtime_field_idx + 1;
        while i < fields.len() {
            let key = fields[i].to_ascii_lowercase();
            match key.as_str() {
                "blocks:" => {
                    if i + 1 < fields.len() {
                        blocks = fields[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "links:" => {
                    if i + 1 < fields.len() {
                        links = fields[i + 1].parse().ok();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "fingerprint:" => {
                    if i + 1 < fields.len() {
                        fingerprint = Some(fields[i + 1].to_string());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Resolve absolute path
        let absolute_path = if raw_path_or_name.starts_with('/') {
            // Directory entries always have absolute paths; update context
            if entry_type == "D" {
                current_dir = Some(raw_path_or_name.clone());
            }
            raw_path_or_name
        } else {
            // Relative name: prepend current directory
            match &current_dir {
                Some(dir) => {
                    let dir_trimmed = dir.trim_end_matches('/');
                    format!("{}/{}", dir_trimmed, raw_path_or_name)
                }
                None => {
                    tracing::warn!(
                        "Relative path '{}' at line {} but no current directory set, skipping",
                        raw_path_or_name,
                        line_no + 1
                    );
                    continue;
                }
            }
        };

        let file_name = std::path::Path::new(&absolute_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| absolute_path.clone());

        let file_extension = extract_extension(&file_name, entry_type);

        entries.push(QDirStatEntry {
            entry_type: entry_type.to_string(),
            file_path: absolute_path,
            file_name,
            file_extension,
            file_size_bytes: size,
            uid,
            gid,
            permissions,
            mtime_unix: mtime,
            file_fingerprint: fingerprint,
            links,
            blocks,
        });
    }

    tracing::info!("Parsed {} entries from {}", entries.len(), path.display());
    Ok(entries)
}

/// Load a QDirStat cache file into filesystem.staging_files via COPY FROM STDIN.
///
/// Parses the cache file, converts entries to tab-delimited rows, and bulk-inserts
/// them into the staging table. Returns the number of rows inserted.
#[tracing::instrument(skip(client, cache_file))]
pub async fn load_qdirstat_file(
    client: &tokio_postgres::Client,
    cache_file: std::path::PathBuf,
    scan_id: i32,
) -> anyhow::Result<i32> {
    let entries = {
        let path = cache_file.clone();
        tokio::task::spawn_blocking(move || parse_qdirstat_cache_file(&path))
            .await
            .context("spawn_blocking join error")??
    };

    let query_header = "
        COPY filesystem.staging_files(
            file_name, entry_type, file_extension, file_path,
            file_size_bytes, file_mtime, file_fingerprint,
            uid, gid, permissions, scan_id
        )
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E'\t',
            NULL '',
            HEADER FALSE
        )";

    let writer = client.copy_in(query_header).await?;
    let mut writer = Box::pin(writer);

    let mut row_count: i32 = 0;
    for entry in &entries {
        // Convert unix timestamp to RFC 3339 for TIMESTAMPTZ
        let mtime_str = chrono::DateTime::<chrono::Utc>::from_timestamp(entry.mtime_unix, 0)
            .unwrap_or_default()
            .to_rfc3339();

        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            entry.file_name,
            entry.entry_type,
            entry.file_extension.as_deref().unwrap_or(""),
            entry.file_path,
            entry.file_size_bytes,
            mtime_str,
            entry.file_fingerprint.as_deref().unwrap_or(""),
            entry.uid.map_or(String::new(), |v| v.to_string()),
            entry.gid.map_or(String::new(), |v| v.to_string()),
            entry.permissions.map_or(String::new(), |v| v.to_string()),
            scan_id,
        );

        writer
            .send(std::io::Cursor::new(line.into_bytes()))
            .await?;
        row_count += 1;
    }

    writer.close().await?;

    tracing::info!("Loaded {} entries into staging_files", row_count);
    Ok(row_count)
}
