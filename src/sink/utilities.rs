use super::*;

pub(super) fn correlation_time_matches(
    query: &CorrelationQuery,
    timestamp_unix_nanos: u64,
) -> bool {
    query
        .start_time_unix_nanos
        .is_none_or(|start| timestamp_unix_nanos >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| timestamp_unix_nanos <= end)
}

pub(super) fn validate_relative_offsets(
    offsets: impl IntoIterator<Item = shard_stream_core::LogicalOffset>,
    count: u32,
) -> TelemetryResult<()> {
    let mut seen = vec![false; count as usize];
    for offset in offsets {
        let ordinal = usize::try_from(offset.get()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let slot = seen
            .get_mut(ordinal)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "signal payload offset is outside its reservation",
            ))?;
        if *slot {
            return Err(TelemetryError::InvalidBlockEncoding(
                "signal payload contains a duplicate relative offset",
            ));
        }
        *slot = true;
    }
    if seen.iter().any(|value| !*value) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "signal payload offsets are not contiguous",
        ));
    }
    Ok(())
}

pub(super) fn absolute_offset(
    topic_partition: TopicPartition,
    first_offset: shard_stream_core::LogicalOffset,
    relative_offset: shard_stream_core::LogicalOffset,
) -> TelemetryResult<shard_stream_core::LogicalOffset> {
    first_offset
        .get()
        .checked_add(relative_offset.get())
        .map(shard_stream_core::LogicalOffset::new)
        .ok_or(TelemetryError::OffsetExhausted(topic_partition))
}

pub(super) fn log_error_to_engine(error: TelemetryError) -> EngineError {
    EngineError::InvalidConfig(error.to_string())
}
