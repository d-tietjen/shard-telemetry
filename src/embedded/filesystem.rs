use super::*;

pub(super) fn unix_seconds_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn directory_file_bytes(
    directory: &std::path::Path,
) -> Result<(u64, u64), LokiApiError> {
    let mut logical_bytes = 0_u64;
    let mut allocated_bytes = 0_u64;
    let mut pending = vec![directory.to_path_buf()];
    while let Some(path) = pending.pop() {
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(LokiApiError::internal(format!(
                    "inspect embedded data directory {}: {error}",
                    path.display(),
                )));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| {
                LokiApiError::internal(format!("inspect embedded data directory entry: {error}"))
            })?;
            let metadata = match entry.path().symlink_metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(LokiApiError::internal(format!(
                        "inspect embedded path {}: {error}",
                        entry.path().display(),
                    )));
                }
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                logical_bytes = logical_bytes.saturating_add(metadata.len());
                allocated_bytes = allocated_bytes.saturating_add(file_allocated_bytes(&metadata));
            }
        }
    }
    Ok((logical_bytes, allocated_bytes))
}

#[cfg(unix)]
pub(super) fn file_allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
pub(super) fn file_allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}
