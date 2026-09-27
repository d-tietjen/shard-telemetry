use super::*;

pub(super) fn record_partition_result(
    report: &mut OffloadReport,
    first_error: &mut Option<OffloadError>,
    result: Result<OffloadReport, OffloadError>,
) {
    match result {
        Ok(partition) => {
            report.scanned_partitions = report
                .scanned_partitions
                .saturating_add(partition.scanned_partitions);
            report.offloaded_batches = report
                .offloaded_batches
                .saturating_add(partition.offloaded_batches);
            report.offloaded_records = report
                .offloaded_records
                .saturating_add(partition.offloaded_records);
            report.advanced_offsets = report
                .advanced_offsets
                .saturating_add(partition.advanced_offsets);
            report.checkpoint_writes = report
                .checkpoint_writes
                .saturating_add(partition.checkpoint_writes);
            report.skipped_batches = report
                .skipped_batches
                .saturating_add(partition.skipped_batches);
            report.skipped_records = report
                .skipped_records
                .saturating_add(partition.skipped_records);
        }
        Err(error) if first_error.is_none() => *first_error = Some(error),
        Err(_) => {}
    }
}

pub(super) fn retry_id(
    namespace: RetryNamespace,
    partition: TopicPartition,
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    batch: &NativeTelemetryBatch,
) -> Result<u128, OffloadError> {
    let payload = batch
        .encode_native_append()
        .map_err(|error| OffloadError::new(error.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-upstream-offload-v1\0");
    if let RetryNamespace::Source(source_id) = namespace {
        hasher.update(source_id.as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&partition.topic_id.get().to_le_bytes());
    hasher.update(&partition.partition_id.get().to_le_bytes());
    hasher.update(&first_offset.get().to_le_bytes());
    hasher.update(&last_offset.get().to_le_bytes());
    hasher.update(&payload);
    let digest = hasher.finalize();
    Ok(u128::from_le_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest contains sixteen bytes"),
    ))
}

pub(super) fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

pub(super) fn journal_io_error(error: std::io::Error) -> OffloadError {
    OffloadError::new(format!(
        "upstream offload checkpoint journal I/O failed: {error}"
    ))
}
