use super::*;

#[derive(Debug)]
pub(super) struct RecoveredSlot {
    pub(super) state: PersistedState,
    pub(super) slot: usize,
    pub(super) generation: u64,
    pub(super) payload_bytes: u64,
    pub(super) compressed: bool,
}

pub(super) enum SlotRead {
    Valid(RecoveredSlot),
    OtherConfiguration,
    Invalid,
}

pub(super) fn read_slot(
    file: &mut File,
    slot: usize,
    slot_bytes: u64,
    max_raw_payload_bytes: usize,
    expected_fingerprint: &[u8; 32],
    feature_count: usize,
    monthly_buckets: usize,
) -> Result<SlotRead, LokiApiError> {
    let slot_offset = slot_bytes.saturating_mul(u64::try_from(slot).unwrap_or(u64::MAX));
    let mut header = [0_u8; HEADER_BYTES];
    file.seek(SeekFrom::Start(slot_offset))
        .and_then(|_| file.read_exact(&mut header))
        .map_err(ledger_io)?;
    if &header[..8] != LEDGER_MAGIC {
        return Ok(SlotRead::Invalid);
    }
    let expected_header_crc = read_u32(&header[64..68]);
    if crc32c::crc32c(&header[..64]) != expected_header_crc {
        return Ok(SlotRead::Invalid);
    }
    if read_u16(&header[8..10]) != LEDGER_VERSION {
        return Ok(SlotRead::Invalid);
    }
    if &header[28..60] != expected_fingerprint {
        return Ok(SlotRead::OtherConfiguration);
    }
    let codec = header[10];
    if !matches!(codec, CODEC_RAW | CODEC_ZSTD) {
        return Ok(SlotRead::Invalid);
    }
    let generation = read_u64(&header[12..20]);
    let payload_len = usize::try_from(read_u32(&header[20..24])).unwrap_or(usize::MAX);
    let raw_len = usize::try_from(read_u32(&header[24..28])).unwrap_or(usize::MAX);
    if payload_len > max_raw_payload_bytes || raw_len > max_raw_payload_bytes {
        return Ok(SlotRead::Invalid);
    }
    let mut payload = vec![0_u8; payload_len];
    file.seek(SeekFrom::Start(
        slot_offset.saturating_add(u64::try_from(HEADER_BYTES).unwrap_or(u64::MAX)),
    ))
    .and_then(|_| file.read_exact(&mut payload))
    .map_err(ledger_io)?;
    if crc32c::crc32c(&payload) != read_u32(&header[60..64]) {
        return Ok(SlotRead::Invalid);
    }
    let raw = if codec == CODEC_ZSTD {
        match zstd::bulk::decompress(&payload, raw_len) {
            Ok(raw) if raw.len() == raw_len => raw,
            Ok(_) | Err(_) => return Ok(SlotRead::Invalid),
        }
    } else {
        if payload_len != raw_len {
            return Ok(SlotRead::Invalid);
        }
        payload
    };
    let state: PersistedState = match rmp_serde::from_slice(&raw) {
        Ok(state) => state,
        Err(_) => return Ok(SlotRead::Invalid),
    };
    if validate_state(&state, feature_count, monthly_buckets).is_err() {
        return Ok(SlotRead::Invalid);
    }
    Ok(SlotRead::Valid(RecoveredSlot {
        state,
        slot,
        generation,
        payload_bytes: u64::try_from(HEADER_BYTES + payload_len).unwrap_or(u64::MAX),
        compressed: codec == CODEC_ZSTD,
    }))
}

pub(super) fn slots_are_pristine(file: &mut File, slot_bytes: u64) -> Result<bool, LokiApiError> {
    for slot in 0..SLOT_COUNT {
        let mut magic = [0_u8; LEDGER_MAGIC.len()];
        file.seek(SeekFrom::Start(slot_bytes.saturating_mul(slot)))
            .and_then(|_| file.read_exact(&mut magic))
            .map_err(ledger_io)?;
        if magic.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn validate_state(
    state: &PersistedState,
    feature_count: usize,
    monthly_buckets: usize,
) -> Result<(), LokiApiError> {
    if state.feature_counts.len() != feature_count
        || state.months.len() > monthly_buckets
        || state
            .months
            .iter()
            .any(|month| month.feature_counts.len() != feature_count)
        || state
            .months
            .windows(2)
            .any(|months| months[0].month_index >= months[1].month_index)
    {
        return Err(LokiApiError::internal(
            "embedded usage ledger state violates fixed-schema bounds",
        ));
    }
    Ok(())
}

pub(super) fn encode_header(
    codec: u8,
    generation: u64,
    payload_len: usize,
    raw_len: usize,
    config_fingerprint: &[u8; 32],
    payload_crc: u32,
) -> Result<[u8; HEADER_BYTES], LokiApiError> {
    let payload_len = u32::try_from(payload_len)
        .map_err(|_| LokiApiError::configuration("embedded usage payload exceeds v1 format"))?;
    let raw_len = u32::try_from(raw_len)
        .map_err(|_| LokiApiError::configuration("embedded usage payload exceeds v1 format"))?;
    let mut header = [0_u8; HEADER_BYTES];
    header[..8].copy_from_slice(LEDGER_MAGIC);
    header[8..10].copy_from_slice(&LEDGER_VERSION.to_le_bytes());
    header[10] = codec;
    header[12..20].copy_from_slice(&generation.to_le_bytes());
    header[20..24].copy_from_slice(&payload_len.to_le_bytes());
    header[24..28].copy_from_slice(&raw_len.to_le_bytes());
    header[28..60].copy_from_slice(config_fingerprint);
    header[60..64].copy_from_slice(&payload_crc.to_le_bytes());
    let header_crc = crc32c::crc32c(&header[..64]);
    header[64..68].copy_from_slice(&header_crc.to_le_bytes());
    Ok(header)
}

pub(super) fn configuration_fingerprint(
    feature_ids: &[Arc<str>],
    monthly_buckets: usize,
    unknown_feature_policy: UnknownFeaturePolicy,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-embedded-usage-v1\0");
    hasher.update(
        &u64::try_from(monthly_buckets)
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(&[match unknown_feature_policy {
        UnknownFeaturePolicy::Reject => 0,
        UnknownFeaturePolicy::AccumulateOverflow => 1,
    }]);
    for feature_id in feature_ids {
        hasher.update(
            &u64::try_from(feature_id.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(feature_id.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

pub(super) fn calendar_month_index(timestamp_unix_seconds: u64) -> Result<u32, LokiApiError> {
    let timestamp = i64::try_from(timestamp_unix_seconds).map_err(|_| {
        LokiApiError::bad_request("embedded usage timestamp is outside the supported calendar")
    })?;
    let date = DateTime::<Utc>::from_timestamp(timestamp, 0).ok_or_else(|| {
        LokiApiError::bad_request("embedded usage timestamp is outside the supported calendar")
    })?;
    let year = u32::try_from(date.year()).map_err(|_| {
        LokiApiError::bad_request("embedded usage timestamps before year zero are unsupported")
    })?;
    Ok(year
        .saturating_mul(12)
        .saturating_add(date.month().saturating_sub(1)))
}

pub(super) fn calendar_month(month_index: u32) -> (i32, u8) {
    let year = i32::try_from(month_index / 12).unwrap_or(i32::MAX);
    let month = u8::try_from(month_index % 12 + 1).unwrap_or(12);
    (year, month)
}

pub(super) fn checked_add(left: u64, right: u64) -> Result<u64, LokiApiError> {
    left.checked_add(right)
        .ok_or_else(|| LokiApiError::bad_request("embedded usage counter overflow"))
}

pub(super) fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("fixed u16 field"))
}

pub(super) fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("fixed u32 field"))
}

pub(super) fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed u64 field"))
}

pub(super) fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn ledger_io(error: std::io::Error) -> LokiApiError {
    LokiApiError::internal(format!("embedded usage ledger I/O failed: {error}"))
}

pub(super) fn ledger_lock_error() -> LokiApiError {
    LokiApiError::internal("embedded usage ledger lock poisoned")
}

pub(super) fn clean_up_failed_open(
    path: &Path,
    path_existed: bool,
    file: File,
    error: LokiApiError,
) -> LokiApiError {
    drop(file);
    if !path_existed {
        let _ = std::fs::remove_file(path);
    }
    error
}

#[cfg(unix)]
pub(super) fn sync_parent_directory(path: &Path) -> Result<(), LokiApiError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(ledger_io)
}

#[cfg(not(unix))]
pub(super) fn sync_parent_directory(_path: &Path) -> Result<(), LokiApiError> {
    Ok(())
}
