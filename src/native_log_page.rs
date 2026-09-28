//! Byte-bounded native log pages and query-bound continuation cursors.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use shard_stream_core::LogicalOffset;

use crate::{
    LokiEntry, NativeProtocolError, NativeQuery, decode_native_query, encode_native_query,
};

const PAGE_QUERY_MAGIC: &[u8; 4] = b"STQ4";
const PAGE_RESULT_MAGIC: &[u8; 4] = b"STR4";
const PAGE_QUERY_HEADER_BYTES: usize = 16;
const CURSOR_BYTES: usize = 52;
/// Largest sum of returned log line, label, and structured-metadata bytes.
pub const MAX_NATIVE_LOG_PAGE_BYTES: u32 = 32 * 1024 * 1024;
/// Maximum number of records examined from each partition per page.
pub(crate) const PAGE_RECORD_BATCH: usize = 256;

/// A native log request with a strict byte budget and optional continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLogPageQuery {
    /// The tenant, filters, time range, order, and per-page record limit.
    pub query: NativeQuery,
    /// Maximum aggregate UTF-8 bytes of returned lines, labels, and metadata.
    pub max_bytes: u32,
    /// Opaque continuation returned by the previous page for this exact query.
    pub cursor: Option<String>,
}

/// One bounded native log page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeLogQueryPage {
    /// Tenant whose records are returned.
    pub tenant: String,
    /// Records in stable timestamp, partition, and offset order.
    pub entries: Vec<LokiEntry>,
    /// Continuation for more records, including a possible empty terminal page.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PagePosition {
    pub(crate) timestamp: u64,
    pub(crate) partition: u32,
    pub(crate) offset: LogicalOffset,
}

fn page_error(message: impl Into<String>) -> NativeProtocolError {
    NativeProtocolError::new(message)
}

pub(crate) fn validate_page_query(
    request: &NativeLogPageQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    let query = encode_native_query(&request.query)?;
    if request.max_bytes == 0 || request.max_bytes > MAX_NATIVE_LOG_PAGE_BYTES {
        return Err(page_error(format!(
            "native log page max_bytes must be in 1..={MAX_NATIVE_LOG_PAGE_BYTES}"
        )));
    }
    if let Some(cursor) = &request.cursor {
        decode_page_cursor(&query, cursor)?;
    }
    Ok(query)
}

pub(crate) fn decode_page_cursor(
    query: &[u8],
    cursor: &str,
) -> Result<PagePosition, NativeProtocolError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| page_error("invalid native log page cursor"))?;
    if bytes.len() != CURSOR_BYTES || &bytes[..32] != blake3::hash(query).as_bytes() {
        return Err(page_error(
            "native log page cursor does not match its query",
        ));
    }
    Ok(PagePosition {
        timestamp: u64::from_le_bytes(bytes[32..40].try_into().expect("fixed cursor range")),
        partition: u32::from_le_bytes(bytes[40..44].try_into().expect("fixed cursor range")),
        offset: LogicalOffset::new(u64::from_le_bytes(
            bytes[44..52].try_into().expect("fixed cursor range"),
        )),
    })
}

pub(crate) fn encode_page_cursor(query: &[u8], position: PagePosition) -> String {
    let mut bytes = [0_u8; CURSOR_BYTES];
    bytes[..32].copy_from_slice(blake3::hash(query).as_bytes());
    bytes[32..40].copy_from_slice(&position.timestamp.to_le_bytes());
    bytes[40..44].copy_from_slice(&position.partition.to_le_bytes());
    bytes[44..52].copy_from_slice(&position.offset.get().to_le_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Counts the bytes in one returned entry before accepting it into a page.
#[must_use]
pub fn native_log_entry_bytes(entry: &LokiEntry) -> usize {
    entry
        .labels
        .iter()
        .chain(entry.structured_metadata.iter())
        .fold(entry.line.len(), |bytes, (key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        })
}

/// Encodes a byte-bounded log-page request for native opcode 9.
pub fn encode_native_log_page_query(
    request: &NativeLogPageQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    let query = validate_page_query(request)?;
    let cursor = request.cursor.as_deref().unwrap_or("");
    let cursor_len = u16::try_from(cursor.len())
        .map_err(|_| page_error("native log page cursor is too long"))?;
    let query_len =
        u32::try_from(query.len()).map_err(|_| page_error("native log page query is too long"))?;
    let mut encoded = Vec::with_capacity(PAGE_QUERY_HEADER_BYTES + cursor.len() + query.len());
    encoded.extend_from_slice(PAGE_QUERY_MAGIC);
    encoded.extend_from_slice(&request.max_bytes.to_le_bytes());
    encoded.extend_from_slice(&cursor_len.to_le_bytes());
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    encoded.extend_from_slice(&query_len.to_le_bytes());
    encoded.extend_from_slice(cursor.as_bytes());
    encoded.extend_from_slice(&query);
    Ok(encoded)
}

/// Decodes and validates a byte-bounded native log-page request.
pub fn decode_native_log_page_query(
    payload: &[u8],
) -> Result<NativeLogPageQuery, NativeProtocolError> {
    if payload.len() < PAGE_QUERY_HEADER_BYTES
        || &payload[..4] != PAGE_QUERY_MAGIC
        || payload[10..12] != [0; 2]
    {
        return Err(page_error("invalid native log page query header"));
    }
    let max_bytes = u32::from_le_bytes(payload[4..8].try_into().expect("fixed page range"));
    let cursor_len =
        u16::from_le_bytes(payload[8..10].try_into().expect("fixed page range")) as usize;
    let query_len =
        u32::from_le_bytes(payload[12..16].try_into().expect("fixed page range")) as usize;
    let end = PAGE_QUERY_HEADER_BYTES
        .checked_add(cursor_len)
        .and_then(|end| end.checked_add(query_len))
        .filter(|end| *end == payload.len())
        .ok_or_else(|| page_error("native log page query length mismatch"))?;
    let query_start = end - query_len;
    let cursor = std::str::from_utf8(&payload[PAGE_QUERY_HEADER_BYTES..query_start])
        .map_err(|_| page_error("native log page cursor is not UTF-8"))?;
    let request = NativeLogPageQuery {
        query: decode_native_query(&payload[query_start..])?,
        max_bytes,
        cursor: (!cursor.is_empty()).then(|| cursor.to_owned()),
    };
    validate_page_query(&request)?;
    Ok(request)
}

/// Encodes a native log page in result order. STR4 uses MessagePack because
/// stream grouping in STR1 would reorder records with different label maps.
pub fn encode_native_log_query_page(
    page: NativeLogQueryPage,
) -> Result<Vec<u8>, NativeProtocolError> {
    if page.entries.len() > PAGE_RECORD_BATCH {
        return Err(page_error("native log page contains too many records"));
    }
    let result = rmp_serde::to_vec(&page)
        .map_err(|error| page_error(format!("native log page encoding failed: {error}")))?;
    let mut encoded = Vec::with_capacity(4 + result.len());
    encoded.extend_from_slice(PAGE_RESULT_MAGIC);
    encoded.extend_from_slice(&result);
    if encoded.len() > crate::MAX_NATIVE_FRAME_BYTES {
        return Err(page_error("native log page exceeds the frame byte limit"));
    }
    Ok(encoded)
}

/// Decodes a bounded native log page and validates its grouped entries.
pub fn decode_native_log_query_page(
    payload: &[u8],
) -> Result<NativeLogQueryPage, NativeProtocolError> {
    if payload.len() < 4
        || &payload[..4] != PAGE_RESULT_MAGIC
        || payload.len() > crate::MAX_NATIVE_FRAME_BYTES
    {
        return Err(page_error("invalid native log page result header"));
    }
    let page: NativeLogQueryPage = rmp_serde::from_slice(&payload[4..])
        .map_err(|error| page_error(format!("invalid native log page result: {error}")))?;
    if page.entries.len() > PAGE_RECORD_BATCH
        || page.tenant.is_empty()
        || page.next_cursor.as_ref().is_some_and(|cursor| {
            URL_SAFE_NO_PAD
                .decode(cursor)
                .map_or(true, |bytes| bytes.len() != CURSOR_BYTES)
        })
    {
        return Err(page_error("invalid native log page result contents"));
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::NativeQueryDirection;

    #[test]
    fn page_wire_preserves_interleaved_stream_order_and_binds_cursor() {
        let query = NativeQuery {
            tenant: "tenant-a".into(),
            labels: BTreeMap::from([("app".into(), "api".into())]),
            terms: vec!["error".into()],
            start_timestamp_unix_nanos: Some(1),
            end_timestamp_unix_nanos: Some(10),
            limit: 3,
            direction: NativeQueryDirection::NewestFirst,
        };
        let query_bytes = encode_native_query(&query).expect("valid query");
        let cursor = encode_page_cursor(
            &query_bytes,
            PagePosition {
                timestamp: 5,
                partition: 2,
                offset: LogicalOffset::new(7),
            },
        );
        let request = NativeLogPageQuery {
            query: query.clone(),
            max_bytes: 128,
            cursor: Some(cursor.clone()),
        };
        let wire = encode_native_log_page_query(&request).expect("page request");
        assert_eq!(
            decode_native_log_page_query(&wire).expect("round trip"),
            request
        );
        let mut wrong_scope = request.clone();
        wrong_scope.query.tenant = "tenant-b".into();
        assert!(encode_native_log_page_query(&wrong_scope).is_err());
        let mut wrong_filter = request.clone();
        wrong_filter.query.terms.push("fatal".into());
        assert!(encode_native_log_page_query(&wrong_filter).is_err());

        let entries = ["a", "b", "a"]
            .into_iter()
            .map(|stream| LokiEntry {
                timestamp_unix_nanos: 5,
                labels: BTreeMap::from([("stream".into(), stream.into())]),
                line: stream.into(),
                structured_metadata: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        let page = NativeLogQueryPage {
            tenant: "tenant-a".into(),
            entries,
            next_cursor: Some(cursor),
        };
        let wire = encode_native_log_query_page(page.clone()).expect("page result");
        assert_eq!(
            decode_native_log_query_page(&wire).expect("result round trip"),
            page
        );
    }
}
