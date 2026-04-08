use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Produce a fixed-cost fingerprint of a file by either:
/// - hashing the entire file if it's <= total_sample_bytes,
/// - or hashing `chunks` equally spaced pieces of size
///   total_sample_bytes // chunks otherwise.
///
/// Returns the hex digest string.
///
/// # Arguments
/// * `file_path` - Path to the file to fingerprint
/// * `total_sample_bytes` - Total bytes to sample from the file (default: 64 * 1024)
/// * `chunks` - Number of chunks to sample for large files (default: 4)
///
/// # Example
/// ```
/// use std::path::Path;
/// use fs_delta_tracker::fingerprint::compute_fingerprint;
///
/// let fingerprint = compute_fingerprint(
///     Path::new("test.txt"),
///     64 * 1024,
///     4,
/// ).unwrap();
/// println!("Fingerprint: {}", fingerprint);
/// ```
pub fn compute_fingerprint(
    file_path: &Path,
    total_sample_bytes: usize,
    chunks: usize,
) -> Result<String> {
    if chunks < 1 {
        anyhow::bail!("`chunks` must be >= 1");
    }
    if total_sample_bytes < chunks {
        anyhow::bail!("`total_sample_bytes` must be >= `chunks`");
    }

    let metadata = file_path
        .metadata()
        .with_context(|| format!("Failed to get metadata for {:?}", file_path))?;
    let size = metadata.len() as usize;

    let mut file = File::open(file_path)
        .with_context(|| format!("Failed to open file {:?}", file_path))?;

    let mut hasher = blake3::Hasher::new();

    // Small file: hash in a streaming fashion
    if size <= total_sample_bytes {
        let mut buffer = vec![0u8; 4096];
        loop {
            let bytes_read = file
                .read(&mut buffer)
                .with_context(|| format!("Failed to read from {:?}", file_path))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        return Ok(hasher.finalize().to_hex().to_string());
    }

    // Large file: sample `chunks` slices of size `piece`
    let piece = total_sample_bytes / chunks;
    let step = (size - piece) as f64 / (chunks - 1) as f64;

    let mut buffer = vec![0u8; piece];
    for i in 0..chunks {
        let offset = (i as f64 * step) as u64;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("Failed to seek in {:?}", file_path))?;

        // Read exactly `piece` bytes or as much as available
        let bytes_to_read = piece.min(size - offset as usize);
        file.read_exact(&mut buffer[..bytes_to_read])
            .with_context(|| format!("Failed to read chunk from {:?}", file_path))?;

        hasher.update(&buffer[..bytes_to_read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

/// Compute fingerprint with default parameters (64KB total, 4 chunks)
pub fn compute_fingerprint_default(file_path: &Path) -> Result<String> {
    compute_fingerprint(file_path, 64 * 1024, 4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_small_file() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(b"Hello, World!")?;
        temp_file.flush()?;

        let fingerprint = compute_fingerprint(temp_file.path(), 64 * 1024, 4)?;
        assert!(!fingerprint.is_empty());
        assert_eq!(fingerprint.len(), 64); // blake3 produces 32 bytes = 64 hex chars

        Ok(())
    }

    #[test]
    fn test_large_file() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        // Create a file larger than 64KB
        let data = vec![0u8; 128 * 1024];
        temp_file.write_all(&data)?;
        temp_file.flush()?;

        let fingerprint = compute_fingerprint(temp_file.path(), 64 * 1024, 4)?;
        assert!(!fingerprint.is_empty());
        assert_eq!(fingerprint.len(), 64);

        Ok(())
    }

    #[test]
    fn test_consistent_fingerprint() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(b"Consistent data")?;
        temp_file.flush()?;

        let fp1 = compute_fingerprint(temp_file.path(), 64 * 1024, 4)?;
        let fp2 = compute_fingerprint(temp_file.path(), 64 * 1024, 4)?;
        assert_eq!(fp1, fp2);

        Ok(())
    }

    #[test]
    fn test_invalid_chunks() {
        let temp_file = NamedTempFile::new().unwrap();
        let result = compute_fingerprint(temp_file.path(), 64 * 1024, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_sample_bytes() {
        let temp_file = NamedTempFile::new().unwrap();
        let result = compute_fingerprint(temp_file.path(), 2, 4);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_fingerprint() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(b"Test with defaults")?;
        temp_file.flush()?;

        let fingerprint = compute_fingerprint_default(temp_file.path())?;
        assert!(!fingerprint.is_empty());
        assert_eq!(fingerprint.len(), 64);

        Ok(())
    }
}
