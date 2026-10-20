use super::*;

pub(super) fn encode_seekable_record_lane(
    payload: &[u8],
    checkpoints: &[usize],
) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::with_capacity(payload.len().saturating_add(16 + checkpoints.len() * 3));
    encoded.extend_from_slice(payload);
    let directory_start =
        u32::try_from(encoded.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    write_varint(
        u64::try_from(SEEK_CHECKPOINT_INTERVAL).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut encoded,
    );
    write_varint(
        u64::try_from(checkpoints.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut encoded,
    );
    let mut previous = 0usize;
    for (index, checkpoint) in checkpoints.iter().copied().enumerate() {
        if (index == 0 && checkpoint != 0) || (index > 0 && checkpoint < previous) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "record lane checkpoints are not ordered",
            ));
        }
        write_varint(
            u64::try_from(checkpoint - previous).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        previous = checkpoint;
    }
    encoded.extend_from_slice(&directory_start.to_le_bytes());
    Ok(encoded)
}

pub(super) fn decode_seekable_record_lane(
    encoded: &[u8],
    record_count: usize,
) -> TelemetryResult<SeekableRecordLane<'_>> {
    let mut state = message_core::LaneState::default();
    let mut wire = message_core::WireState::default();
    message_core::visit_lane(
        encoded,
        record_count,
        &mut state,
        &mut wire,
        &mut message_core::Ordinary,
    )
    .map_err(|_| wire.take_error())?;
    Ok(SeekableRecordLane {
        interval: state.interval,
        checkpoints: state.checkpoints,
        payload: &encoded[..state.directory_start],
    })
}

pub(super) fn validate_checkpoint_cursor(
    lane: &SeekableRecordLane<'_>,
    record_ordinal: usize,
    cursor: usize,
) -> TelemetryResult<()> {
    validate_checkpoint_parts(lane.interval, &lane.checkpoints, record_ordinal, cursor)
}

pub(super) fn validate_checkpoint_parts(
    interval: usize,
    checkpoints: &[usize],
    record_ordinal: usize,
    cursor: usize,
) -> TelemetryResult<()> {
    if record_ordinal.is_multiple_of(interval)
        && checkpoints.get(record_ordinal / interval).copied() != Some(cursor)
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "record lane checkpoint does not point to a record",
        ));
    }
    Ok(())
}
