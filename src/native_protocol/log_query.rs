use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NativeLogQueryResultInfo<'a> {
    tenant: &'a str,
    record_count: u32,
}

pub(super) fn inspect_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResultInfo<'_>, NativeProtocolError> {
    if payload.len() < LOG_QUERY_RESULT_HEADER_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result is shorter than its header",
        ));
    }
    if payload[0..4] != LOG_QUERY_RESULT_MAGIC {
        return Err(NativeProtocolError::new(
            "invalid native log query result magic",
        ));
    }
    if payload[12..16] != [0; 4] {
        return Err(NativeProtocolError::new(
            "native log query result reserved bytes must be zero",
        ));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    if tenant_len > MAX_TENANT_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result tenant exceeds its limit",
        ));
    }
    let end = LOG_QUERY_RESULT_HEADER_BYTES
        .checked_add(tenant_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| NativeProtocolError::new("native log query result tenant is truncated"))?;
    let tenant = std::str::from_utf8(&payload[LOG_QUERY_RESULT_HEADER_BYTES..end])
        .map_err(|_| NativeProtocolError::new("native log query result tenant is not UTF-8"))?;
    if tenant.is_empty() {
        return Err(NativeProtocolError::new(
            "native log query result tenant must not be empty",
        ));
    }
    Ok(NativeLogQueryResultInfo {
        tenant,
        record_count: u32::from_le_bytes(payload[8..12].try_into().expect("fixed range")),
    })
}

/// Encodes records with labels stored once per stream.
pub fn encode_native_log_query_result(
    tenant: &str,
    entries: Vec<LokiEntry>,
) -> Result<Vec<u8>, NativeProtocolError> {
    validate_tenant(tenant)?;
    let record_count = u32::try_from(entries.len())
        .map_err(|_| NativeProtocolError::new("native batch contains more than u32 records"))?;
    // Native indexed queries usually return one stream. Avoid hashing and
    // comparing a BTreeMap for every record in that case; labels are still
    // checked before taking ownership, so the fast path preserves exact
    // stream grouping and the same negative-timestamp validation as the
    // general path.
    let single_stream = entries.first().is_some_and(|first| {
        first.timestamp_unix_nanos >= 0
            && entries[1..]
                .iter()
                .all(|entry| entry.timestamp_unix_nanos >= 0 && entry.labels == first.labels)
    });
    let streams = if single_stream {
        let mut entries = entries.into_iter();
        let mut first = entries.next().expect("single-stream result is nonempty");
        let labels = std::mem::take(&mut first.labels);
        let mut stream_entries = Vec::with_capacity(record_count as usize);
        stream_entries.push(first);
        for mut entry in entries {
            drop(std::mem::take(&mut entry.labels));
            stream_entries.push(entry);
        }
        vec![(labels, stream_entries)]
    } else {
        let mut streams = HashMap::<BTreeMap<String, String>, Vec<LokiEntry>>::with_capacity(
            entries.len().min(MAX_STREAMS),
        );
        for mut entry in entries {
            if entry.timestamp_unix_nanos < 0 {
                return Err(NativeProtocolError::new(
                    "negative native log timestamps are unsupported",
                ));
            }
            // Move the labels into the grouping key. The old implementation
            // cloned every map before insertion, although only one copy per
            // stream is emitted on the wire.
            let labels = std::mem::take(&mut entry.labels);
            streams.entry(labels).or_default().push(entry);
        }
        streams.into_iter().collect::<Vec<_>>()
    };
    if streams.len() > MAX_STREAMS {
        return Err(NativeProtocolError::new(
            "native batch contains too many streams",
        ));
    }
    let mut streams = streams.into_iter().collect::<Vec<_>>();
    // Keep the wire representation deterministic while using hash lookup for
    // the common case where many entries share one stream label map.
    streams.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut estimated_bytes = LOG_QUERY_RESULT_HEADER_BYTES.saturating_add(tenant.len());
    for (labels, entries) in &streams {
        estimated_bytes = estimated_bytes.saturating_add(8);
        for (key, value) in labels {
            estimated_bytes = estimated_bytes
                .saturating_add(4)
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        for entry in entries {
            estimated_bytes = estimated_bytes
                .saturating_add(16)
                .saturating_add(entry.line.len());
            for (key, value) in &entry.structured_metadata {
                estimated_bytes = estimated_bytes
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
        }
    }
    let mut encoded = Vec::with_capacity(estimated_bytes.min(MAX_NATIVE_FRAME_BYTES));
    encoded.extend_from_slice(&LOG_QUERY_RESULT_MAGIC);
    put_u16(&mut encoded, tenant.len(), "tenant")?;
    put_u16(&mut encoded, streams.len(), "stream count")?;
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(tenant.as_bytes());

    for (labels, entries) in streams {
        if labels.len() > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        put_u16(&mut encoded, labels.len(), "label count")?;
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| NativeProtocolError::new("native stream contains too many entries"))?;
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        for (key, value) in labels {
            put_string16(&mut encoded, &key, "label key")?;
            put_string16(&mut encoded, &value, "label value")?;
        }
        for entry in entries {
            encoded.extend_from_slice(&(entry.timestamp_unix_nanos as u64).to_le_bytes());
            put_u32(&mut encoded, entry.line.len(), "log line")?;
            if entry.structured_metadata.len() > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            put_u16(
                &mut encoded,
                entry.structured_metadata.len(),
                "metadata count",
            )?;
            encoded.extend_from_slice(&0_u16.to_le_bytes());
            encoded.extend_from_slice(entry.line.as_bytes());
            for (key, value) in entry.structured_metadata {
                put_string16(&mut encoded, &key, "metadata key")?;
                put_string16(&mut encoded, &value, "metadata value")?;
            }
        }
    }
    if encoded.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native batch is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct NativeStreamKey(Vec<(Arc<str>, Arc<str>)>);

pub(super) struct NativeProjectedLog {
    timestamp_unix_nanos: u64,
    message: Arc<str>,
    metadata: Vec<(Arc<str>, Arc<str>)>,
}

pub(super) fn normalize_projected_fields(
    mut fields: Vec<(Arc<str>, Arc<str>, usize)>,
) -> Vec<(Arc<str>, Arc<str>)> {
    fields.sort_unstable_by(|left, right| left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2)));
    let mut normalized = Vec::with_capacity(fields.len());
    for (key, value, _) in fields {
        if let Some((existing_key, existing_value)) = normalized.last_mut()
            && *existing_key == key
        {
            *existing_value = value;
        } else {
            normalized.push((key, value));
        }
    }
    normalized
}

/// Encodes projected indexed records without materializing one Loki label and
/// metadata map per result. The native server uses this for the common query
/// path; delete-filtered queries continue through the public Loki entry path.
pub(crate) fn encode_native_log_query_matches(
    tenant: &str,
    matches: Vec<crate::LogMatch>,
) -> Result<Vec<u8>, NativeProtocolError> {
    validate_tenant(tenant)?;
    let record_count = u32::try_from(matches.len())
        .map_err(|_| NativeProtocolError::new("native batch contains more than u32 records"))?;
    let mut streams = HashMap::<NativeStreamKey, Vec<NativeProjectedLog>>::with_capacity(
        matches.len().min(MAX_STREAMS),
    );
    for matched in matches {
        let record = matched.record;
        let mut labels = Vec::new();
        let mut metadata = Vec::new();
        for (index, field) in record.fields.iter().enumerate() {
            if field.key.as_ref().starts_with(NATIVE_LABEL_PREFIX) {
                labels.push((Arc::clone(&field.key), Arc::clone(&field.value), index));
            } else if field.key.as_ref().starts_with(NATIVE_METADATA_PREFIX) {
                metadata.push((Arc::clone(&field.key), Arc::clone(&field.value), index));
            }
        }
        streams
            .entry(NativeStreamKey(normalize_projected_fields(labels)))
            .or_default()
            .push(NativeProjectedLog {
                timestamp_unix_nanos: record.timestamp_unix_nanos,
                message: record.message,
                metadata: normalize_projected_fields(metadata),
            });
    }
    if streams.len() > MAX_STREAMS {
        return Err(NativeProtocolError::new(
            "native batch contains too many streams",
        ));
    }
    let streams = streams.into_iter().collect::<Vec<_>>();

    let mut estimated_bytes = LOG_QUERY_RESULT_HEADER_BYTES.saturating_add(tenant.len());
    for (NativeStreamKey(labels), entries) in &streams {
        estimated_bytes = estimated_bytes.saturating_add(8);
        for (key, value) in labels {
            let key = key
                .as_ref()
                .strip_prefix(NATIVE_LABEL_PREFIX)
                .expect("native projected label prefix");
            estimated_bytes = estimated_bytes
                .saturating_add(4)
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        for entry in entries {
            estimated_bytes = estimated_bytes
                .saturating_add(16)
                .saturating_add(entry.message.len());
            for (key, value) in &entry.metadata {
                let key = key
                    .as_ref()
                    .strip_prefix(NATIVE_METADATA_PREFIX)
                    .expect("native projected metadata prefix");
                estimated_bytes = estimated_bytes
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
        }
    }

    let mut encoded = Vec::with_capacity(estimated_bytes.min(MAX_NATIVE_FRAME_BYTES));
    encoded.extend_from_slice(&LOG_QUERY_RESULT_MAGIC);
    put_u16(&mut encoded, tenant.len(), "tenant")?;
    put_u16(&mut encoded, streams.len(), "stream count")?;
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(tenant.as_bytes());

    for (NativeStreamKey(labels), entries) in streams {
        if labels.len() > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        put_u16(&mut encoded, labels.len(), "label count")?;
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| NativeProtocolError::new("native stream contains too many entries"))?;
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        for (key, value) in labels {
            let key = key
                .as_ref()
                .strip_prefix(NATIVE_LABEL_PREFIX)
                .expect("native projected label prefix");
            put_string16(&mut encoded, key, "label key")?;
            put_string16(&mut encoded, &value, "label value")?;
        }
        for entry in entries {
            encoded.extend_from_slice(&entry.timestamp_unix_nanos.to_le_bytes());
            put_u32(&mut encoded, entry.message.len(), "log line")?;
            if entry.metadata.len() > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            encoded.extend_from_slice(&(entry.metadata.len() as u16).to_le_bytes());
            encoded.extend_from_slice(&0_u16.to_le_bytes());
            encoded.extend_from_slice(entry.message.as_bytes());
            for (key, value) in entry.metadata {
                let key = key
                    .as_ref()
                    .strip_prefix(NATIVE_METADATA_PREFIX)
                    .expect("native projected metadata prefix");
                put_string16(&mut encoded, key, "metadata key")?;
                put_string16(&mut encoded, &value, "metadata value")?;
            }
        }
    }
    if encoded.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native batch is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

/// Decodes and fully validates a grouped native log batch.
pub fn decode_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResult, NativeProtocolError> {
    let info = inspect_native_log_query_result(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, LOG_QUERY_RESULT_HEADER_BYTES + info.tenant.len());
    let mut entries = Vec::with_capacity(info.record_count as usize);
    for _ in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        let mut labels = BTreeMap::new();
        for _ in 0..label_count {
            let key = cursor.string16("label key")?.to_owned();
            let value = cursor.string16("label value")?.to_owned();
            if key.is_empty() || labels.insert(key, value).is_some() {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
        }
        entries
            .len()
            .checked_add(entry_count)
            .filter(|count| *count <= info.record_count as usize)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        for _ in 0..entry_count {
            let timestamp = cursor.u64("timestamp")?;
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let line = cursor.string(line_len, "log line")?.to_owned();
            let mut structured_metadata = BTreeMap::new();
            for _ in 0..metadata_count {
                let key = cursor.string16("metadata key")?.to_owned();
                let value = cursor.string16("metadata value")?.to_owned();
                if key.is_empty() || structured_metadata.insert(key, value).is_some() {
                    return Err(NativeProtocolError::new(
                        "native entry contains empty or duplicate metadata",
                    ));
                }
            }
            let timestamp_unix_nanos = i64::try_from(timestamp).map_err(|_| {
                NativeProtocolError::new("native log timestamp exceeds the signed i64 range")
            })?;
            entries.push(LokiEntry {
                timestamp_unix_nanos,
                labels: labels.clone(),
                line,
                structured_metadata,
            });
        }
    }
    if entries.len() != info.record_count as usize {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {} records, expected {}",
            entries.len(),
            info.record_count
        )));
    }
    cursor.finish()?;
    Ok(NativeLogQueryResult {
        tenant: info.tenant.to_owned(),
        entries,
    })
}
