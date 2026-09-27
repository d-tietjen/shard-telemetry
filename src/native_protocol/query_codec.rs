use super::*;

/// Encodes one bounded native metric query.
pub fn encode_native_metric_query(
    query: &crate::MetricQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(METRIC_QUERY_MAGIC, query, "metric query")
}

/// Decodes one bounded native metric query.
pub fn decode_native_metric_query(
    payload: &[u8],
) -> Result<crate::MetricQuery, NativeProtocolError> {
    decode_messagepack(METRIC_QUERY_MAGIC, payload, "metric query")
}

/// Encodes native metric query results.
pub fn encode_native_metric_query_result(
    points: &[crate::DurableMetricPoint],
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(METRIC_QUERY_RESULT_MAGIC, points, "metric query result")
}

/// Decodes native metric query results.
pub fn decode_native_metric_query_result(
    payload: &[u8],
) -> Result<Vec<crate::DurableMetricPoint>, NativeProtocolError> {
    decode_messagepack(METRIC_QUERY_RESULT_MAGIC, payload, "metric query result")
}

/// Encodes one bounded native trace query.
pub fn encode_native_trace_query(
    query: &crate::TraceQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(TRACE_QUERY_MAGIC, query, "trace query")
}

/// Decodes one bounded native trace query.
pub fn decode_native_trace_query(payload: &[u8]) -> Result<crate::TraceQuery, NativeProtocolError> {
    decode_messagepack(TRACE_QUERY_MAGIC, payload, "trace query")
}

/// Encodes native trace query results.
pub fn encode_native_trace_query_result(
    spans: &[crate::DurableSpan],
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(TRACE_QUERY_RESULT_MAGIC, spans, "trace query result")
}

/// Decodes native trace query results.
pub fn decode_native_trace_query_result(
    payload: &[u8],
) -> Result<Vec<crate::DurableSpan>, NativeProtocolError> {
    decode_messagepack(TRACE_QUERY_RESULT_MAGIC, payload, "trace query result")
}

/// Encodes native server capabilities.
pub fn encode_native_capabilities(
    capabilities: &NativeCapabilities,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(CAPABILITIES_MAGIC, capabilities, "capabilities")
}

/// Decodes native server capabilities.
pub fn decode_native_capabilities(
    payload: &[u8],
) -> Result<NativeCapabilities, NativeProtocolError> {
    decode_messagepack(CAPABILITIES_MAGIC, payload, "capabilities")
}

pub(super) fn encode_messagepack<T: serde::Serialize + ?Sized>(
    magic: [u8; 4],
    value: &T,
    kind: &str,
) -> Result<Vec<u8>, NativeProtocolError> {
    // Write the discriminator and MessagePack value into one buffer. Using
    // `to_vec` first would allocate a temporary payload and copy it again
    // after prepending the native type tag on every query response.
    let mut payload = Vec::with_capacity(magic.len() + 128);
    payload.extend_from_slice(&magic);
    rmp_serde::encode::write(&mut payload, value).map_err(|error| {
        NativeProtocolError::new(format!("native {kind} encoding failed: {error}"))
    })?;
    if payload.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native {kind} exceeds the frame limit"
        )));
    }
    Ok(payload)
}

pub(super) fn decode_messagepack<T: serde::de::DeserializeOwned>(
    magic: [u8; 4],
    payload: &[u8],
    kind: &str,
) -> Result<T, NativeProtocolError> {
    let Some(encoded) = payload.strip_prefix(&magic) else {
        return Err(NativeProtocolError::new(format!(
            "invalid native {kind} magic"
        )));
    };
    rmp_serde::from_slice(encoded)
        .map_err(|error| NativeProtocolError::new(format!("invalid native {kind}: {error}")))
}

/// Encodes an indexed native query.
pub fn encode_native_query(query: &NativeQuery) -> Result<Vec<u8>, NativeProtocolError> {
    validate_query(query)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&QUERY_MAGIC);
    put_u16(&mut encoded, query.tenant.len(), "query tenant")?;
    put_u16(&mut encoded, query.labels.len(), "query label count")?;
    put_u16(&mut encoded, query.terms.len(), "query term count")?;
    encoded.push(match query.direction {
        NativeQueryDirection::OldestFirst => 0,
        NativeQueryDirection::NewestFirst => 1,
    });
    encoded.push(0);
    encoded.extend_from_slice(&query.limit.to_le_bytes());
    encoded.extend_from_slice(
        &query
            .start_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(
        &query
            .end_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(query.tenant.as_bytes());
    for (key, value) in &query.labels {
        put_string16(&mut encoded, key, "query label key")?;
        put_string16(&mut encoded, value, "query label value")?;
    }
    for term in &query.terms {
        put_string16(&mut encoded, term, "query term")?;
    }
    Ok(encoded)
}

/// Decodes and validates an indexed native query.
pub fn decode_native_query(payload: &[u8]) -> Result<NativeQuery, NativeProtocolError> {
    if payload.len() < QUERY_HEADER_BYTES || payload[0..4] != QUERY_MAGIC {
        return Err(NativeProtocolError::new("invalid native query header"));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    let label_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let term_count = usize::from(u16::from_le_bytes(
        payload[8..10].try_into().expect("fixed range"),
    ));
    let direction = match payload[10] {
        0 => NativeQueryDirection::OldestFirst,
        1 => NativeQueryDirection::NewestFirst,
        value => {
            return Err(NativeProtocolError::new(format!(
                "unsupported native query direction {value}"
            )));
        }
    };
    if payload[11] != 0 {
        return Err(NativeProtocolError::new(
            "native query reserved byte must be zero",
        ));
    }
    let limit = u32::from_le_bytes(payload[12..16].try_into().expect("fixed range"));
    let start = u64::from_le_bytes(payload[16..24].try_into().expect("fixed range"));
    let end = u64::from_le_bytes(payload[24..32].try_into().expect("fixed range"));
    let mut cursor = Cursor::at(payload, QUERY_HEADER_BYTES);
    let tenant = cursor.string(tenant_len, "query tenant")?.to_owned();
    let mut labels = BTreeMap::new();
    for _ in 0..label_count {
        let key = cursor.string16("query label key")?.to_owned();
        let value = cursor.string16("query label value")?.to_owned();
        if key.is_empty() || labels.insert(key, value).is_some() {
            return Err(NativeProtocolError::new(
                "native query contains an empty or duplicate label",
            ));
        }
    }
    let mut terms = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        let term = cursor.string16("query term")?.to_owned();
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        terms.push(term);
    }
    cursor.finish()?;
    let query = NativeQuery {
        tenant,
        labels,
        terms,
        start_timestamp_unix_nanos: (start != u64::MAX).then_some(start),
        end_timestamp_unix_nanos: (end != u64::MAX).then_some(end),
        limit,
        direction,
    };
    validate_query(&query)?;
    Ok(query)
}
