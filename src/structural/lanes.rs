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
    let footer_start =
        encoded
            .len()
            .checked_sub(size_of::<u32>())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "record lane footer is truncated",
            ))?;
    let directory_start = usize::try_from(u32::from_le_bytes(
        encoded[footer_start..]
            .try_into()
            .map_err(|_| TelemetryError::InvalidBlockEncoding("record lane footer is invalid"))?,
    ))
    .map_err(|_| TelemetryError::InvalidBlockEncoding("record lane footer does not fit usize"))?;
    let directory =
        encoded
            .get(directory_start..footer_start)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "record lane directory is invalid",
            ))?;
    let payload = encoded
        .get(..directory_start)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "record lane payload is truncated",
        ))?;
    let mut cursor = 0usize;
    let interval = read_usize(directory, &mut cursor)?;
    if interval == 0 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "record lane checkpoint interval is zero",
        ));
    }
    let checkpoint_count = read_usize(directory, &mut cursor)?;
    if checkpoint_count != record_count.div_ceil(interval) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "record lane checkpoint count mismatch",
        ));
    }
    let mut checkpoints = Vec::with_capacity(checkpoint_count);
    let mut previous = 0usize;
    for index in 0..checkpoint_count {
        let delta = read_usize(directory, &mut cursor)?;
        let checkpoint =
            previous
                .checked_add(delta)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "record lane checkpoint overflow",
                ))?;
        if (index == 0 && checkpoint != 0) || (index > 0 && checkpoint < previous) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "record lane checkpoints are not ordered",
            ));
        }
        checkpoints.push(checkpoint);
        previous = checkpoint;
    }
    require_consumed(directory, cursor)?;
    if checkpoints
        .last()
        .is_some_and(|checkpoint| *checkpoint > payload.len())
        || (record_count == 0 && !payload.is_empty())
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "record lane checkpoint exceeds payload",
        ));
    }
    Ok(SeekableRecordLane {
        interval,
        checkpoints,
        payload,
    })
}

pub(super) fn validate_checkpoint_cursor(
    lane: &SeekableRecordLane<'_>,
    record_ordinal: usize,
    cursor: usize,
) -> TelemetryResult<()> {
    if record_ordinal.is_multiple_of(lane.interval)
        && lane
            .checkpoints
            .get(record_ordinal / lane.interval)
            .copied()
            != Some(cursor)
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "record lane checkpoint does not point to a record",
        ));
    }
    Ok(())
}
