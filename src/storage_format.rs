use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::LokiApiError;
use crate::recovery::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};

const DATA_FORMAT_MARKER: &[u8] = b"shard-telemetry-data-format=1\n";

/// Exclusive process ownership and format validation for one data directory.
#[derive(Debug)]
pub(crate) struct DataDirectoryLease {
    _lock: File,
}

impl DataDirectoryLease {
    pub(crate) fn acquire_with_recovery_budget(
        directory: &Path,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, TelemetryRecoveryError> {
        if directory.as_os_str().len() > budget.path_limit() {
            return budget.invalid();
        }
        let parent = budget.ensure_directory(directory)?;
        let lock_path = budget.join_path(directory, std::ffi::OsStr::new("LOCK"))?;
        budget.check()?;
        let lock = budget.open_path(&lock_path, crate::recovery::RecoveryFileAccess::Lock)?;
        budget.io(|| lock.try_lock_exclusive())?;
        let marker_path = budget.join_path(directory, std::ffi::OsStr::new("FORMAT"))?;
        budget.check()?;
        if budget.io(|| marker_path.try_exists())? {
            if budget.read_file(&marker_path)? != DATA_FORMAT_MARKER {
                return budget.invalid();
            }
        } else {
            budget.charge(TelemetryRecoveryUsage {
                work_bytes: DATA_FORMAT_MARKER.len(),
                ..Default::default()
            })?;
            let temporary = budget.suffixed_path(&marker_path, ".tmp")?;
            let mut marker =
                budget.open_path(&temporary, crate::recovery::RecoveryFileAccess::Temporary)?;
            budget.write_file(&mut marker, DATA_FORMAT_MARKER)?;
            budget.io(|| marker.sync_all())?;
            budget.io(|| fs::rename(temporary, marker_path))?;
            budget.io(|| parent.sync_all())?;
        }
        budget.check()?;
        Ok(Self { _lock: lock })
    }

    pub(crate) fn acquire(directory: &Path) -> Result<Self, LokiApiError> {
        fs::create_dir_all(directory).map_err(storage_io_error)?;
        let lock_path = directory.join("LOCK");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(storage_io_error)?;
        lock.try_lock_exclusive().map_err(|error| {
            LokiApiError::configuration(format!(
                "data directory {} is already in use or cannot be locked: {error}",
                directory.display()
            ))
        })?;
        validate_or_create_marker(directory)?;
        Ok(Self { _lock: lock })
    }
}

fn validate_or_create_marker(directory: &Path) -> Result<(), LokiApiError> {
    let marker_path = directory.join("FORMAT");
    match fs::read(&marker_path) {
        Ok(marker) if marker == DATA_FORMAT_MARKER => return Ok(()),
        Ok(marker) => {
            let observed = String::from_utf8_lossy(&marker);
            return Err(LokiApiError::configuration(format!(
                "unsupported ShardTelemetry data format marker {observed:?}"
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage_io_error(error)),
    }

    let temporary = temporary_path(&marker_path);
    let mut marker = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(storage_io_error)?;
    marker
        .write_all(DATA_FORMAT_MARKER)
        .and_then(|()| marker.sync_all())
        .map_err(storage_io_error)?;
    fs::rename(&temporary, &marker_path).map_err(storage_io_error)?;
    File::open(directory)
        .and_then(|parent| parent.sync_all())
        .map_err(storage_io_error)?;
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn storage_io_error(error: std::io::Error) -> LokiApiError {
    LokiApiError::internal(format!("data directory I/O failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn lease_is_exclusive_and_rejects_unknown_formats() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-format-{}-{nonce}",
            std::process::id()
        ));
        let lease = DataDirectoryLease::acquire(&directory).expect("first lease");
        assert_eq!(
            fs::read(directory.join("FORMAT")).unwrap(),
            DATA_FORMAT_MARKER
        );
        assert!(DataDirectoryLease::acquire(&directory).is_err());
        drop(lease);
        fs::write(
            directory.join("FORMAT"),
            b"shard-telemetry-data-format=999\n",
        )
        .expect("replace marker");
        assert!(DataDirectoryLease::acquire(&directory).is_err());
        fs::remove_dir_all(directory).expect("cleanup");
    }
}
