use std::cell::Cell;

use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use super::*;
use crate::{CompressionCohortId, LogQuery};

fn record(offset: u64, message: &str) -> DurableLog {
    DurableLog::new(
        ShardId::new(7),
        TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3)),
        LogicalOffset::new(offset),
        10_000 + offset,
        message,
        CompressionCohortId::new(4),
    )
    .with_field("service.name", "billing")
    .with_field("severity", "ERROR")
}

struct CountingRecord<'a> {
    record: &'a DurableLog,
    field_reads: &'a Cell<usize>,
}

impl StructuralRecordView for CountingRecord<'_> {
    fn structural_offset(&self) -> LogicalOffset {
        self.record.record_ref.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.record.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.record.message
    }

    fn structural_field_count(&self) -> usize {
        self.record.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.field_reads
            .set(self.field_reads.get().saturating_add(1));
        self.record
            .fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }
}

fn legacy_rows(records: &[DurableLog]) -> Vec<u8> {
    let capacity = records
        .iter()
        .map(row_source_bytes)
        .collect::<TelemetryResult<Vec<_>>>()
        .expect("logical source sizes fit")
        .into_iter()
        .sum::<u64>();
    let mut encoded = Vec::with_capacity(usize::try_from(capacity).expect("test fits"));
    for record in records {
        encoded.extend_from_slice(&record.record_ref.offset.get().to_le_bytes());
        encoded.extend_from_slice(&record.timestamp_unix_nanos.to_le_bytes());
        append_legacy_bytes(&mut encoded, record.message.as_bytes());
        encoded.extend_from_slice(
            &u32::try_from(record.fields.len())
                .expect("field count fits")
                .to_le_bytes(),
        );
        for field in record.fields.iter() {
            append_legacy_bytes(&mut encoded, field.key.as_bytes());
            append_legacy_bytes(&mut encoded, field.value.as_bytes());
        }
    }
    encoded
}

fn append_legacy_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("test value fits")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(bytes);
}

fn encode_timestamp_values(values: &[u64]) -> Vec<u8> {
    let records = values
        .iter()
        .copied()
        .enumerate()
        .map(|(offset, timestamp)| {
            let mut record = record(
                u64::try_from(offset).expect("test offset fits"),
                "timestamp test",
            );
            record.timestamp_unix_nanos = timestamp;
            record
        })
        .collect::<Vec<_>>();
    encode_timestamps(&records).expect("timestamps encode")
}

#[test]
fn typed_metadata_dictionaries_round_trip_exact_values() {
    let mut typed = record(4, "typed body");
    typed.observed_timestamp_unix_nanos = 99;
    typed.body = Some(TelemetryValue::Map(Arc::new(vec![
        TelemetryAttribute::new("nested", TelemetryValue::Integer(-7)),
        TelemetryAttribute::new("empty", TelemetryValue::Empty),
    ])));
    typed = typed.with_attribute(TelemetryAttribute::new(
        "ratio",
        TelemetryValue::DoubleBits(0x7ff8_0000_0000_0042),
    ));
    typed.resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout")),
        )]),
        dropped_attributes_count: 2,
        schema_url: Arc::from("https://example.test/resource"),
        entity_refs: Arc::new(Vec::new()),
    });
    typed.scope = Arc::new(ScopeContext {
        name: Arc::from("checkout.instrumentation"),
        version: Arc::from("1.2.3"),
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "scope.enabled",
            TelemetryValue::Boolean(true),
        )]),
        dropped_attributes_count: 1,
        schema_url: Arc::from("https://example.test/scope"),
    });
    typed.severity_number = 17;
    typed.severity_text = Arc::from("ERROR");
    typed.dropped_attributes_count = 3;
    typed.flags = 1;
    typed.trace_id = Some(TraceId::from_bytes([1; 16]).expect("trace ID is valid"));
    typed.span_id = Some(SpanId::from_bytes([2; 8]).expect("span ID is valid"));
    typed.event_name = Arc::from("payment.failed");

    let mut repeated = typed.clone();
    repeated.record_ref.offset = LogicalOffset::new(5);
    repeated.timestamp_unix_nanos = u64::MAX - 4;
    repeated.observed_timestamp_unix_nanos = 3;
    repeated.message = Arc::from("message-backed body");
    repeated.body = Some(TelemetryValue::String(Arc::clone(&repeated.message)));
    let trace_id = repeated
        .trace_id
        .expect("trace ID is populated")
        .to_string();
    let span_id = repeated.span_id.expect("span ID is populated").to_string();
    repeated = repeated
        .with_field("otel.trace_id", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
        .with_field("otel.trace_id", trace_id)
        .with_field("otel.span_id", span_id);
    let records = vec![record(3, "plain body"), typed, repeated];
    let encoded = encode_structural_block(&records).expect("typed block encodes");
    let decoded = decode_structural_block(&encoded).expect("typed block decodes");
    for (decoded, record) in decoded.iter().zip(&records) {
        assert_eq!(
            decoded.observed_timestamp_unix_nanos,
            record.observed_timestamp_unix_nanos
        );
        assert_eq!(decoded.body, record.body);
        assert_eq!(decoded.attributes, record.attributes);
        assert_eq!(decoded.resource, record.resource);
        assert_eq!(decoded.scope, record.scope);
        assert_eq!(decoded.severity_number, record.severity_number);
        assert_eq!(decoded.severity_text, record.severity_text);
        assert_eq!(
            decoded.dropped_attributes_count,
            record.dropped_attributes_count
        );
        assert_eq!(decoded.flags, record.flags);
        assert_eq!(decoded.trace_id, record.trace_id);
        assert_eq!(decoded.span_id, record.span_id);
        assert_eq!(decoded.event_name, record.event_name);
    }
}

#[test]
fn structural_block_round_trips_exact_human_readable_records() {
    let records = vec![
        record(4, "ERROR request_id=req-1001 retry=0 card declined"),
        record(5, "ERROR request_id=req-1002 retry=1 card declined"),
        record(7, "ERROR request_id=req-1003 retry=2 card declined"),
    ];
    let encoded = encode_structural_block(&records).expect("block encodes");
    let decoded = decode_structural_block(&encoded).expect("block decodes");
    assert_eq!(decoded.len(), records.len());
    for (decoded, record) in decoded.iter().zip(&records) {
        assert_eq!(decoded.offset, record.record_ref.offset);
        assert_eq!(decoded.timestamp_unix_nanos, record.timestamp_unix_nanos);
        assert_eq!(decoded.message, record.message);
        assert_eq!(decoded.fields.as_ref(), record.fields.as_ref());
    }
}

#[test]
fn structural_field_plan_reads_each_input_field_once() {
    let records = vec![
        record(4, "ERROR request_id=req-1001 retry=0 card declined").with_field("trace.id", "aaa"),
        record(5, "ERROR request_id=req-1002 retry=1 card declined").with_field("trace.id", "bbb"),
        record(7, "ERROR request_id=req-1003 retry=2 card declined").with_field("trace.id", "aaa"),
    ];
    let expected = encode_indexed_structural_records(&records)
        .expect("owned records encode")
        .structural;
    let field_reads = Cell::new(0);
    let counting = records
        .iter()
        .map(|record| CountingRecord {
            record,
            field_reads: &field_reads,
        })
        .collect::<Vec<_>>();
    let encoded = encode_indexed_structural_records(&counting)
        .expect("counting records encode")
        .structural;
    assert_eq!(encoded, expected);
    assert_eq!(
        field_reads.get(),
        records
            .iter()
            .map(|record| record.fields.len())
            .sum::<usize>()
    );
}

#[test]
fn embedded_index_candidates_select_exact_static_dynamic_and_field_matches() {
    let records = vec![
        record(0, "ERROR request_id=req-1001 card declined").with_field("trace.id", "aaa"),
        record(1, "INFO request_id=req-1002 card accepted").with_field("trace.id", "bbb"),
        record(2, "ERROR request_id=req-1003 card declined").with_field("trace.id", "ccc"),
        record(3, "ERROR request_id=req-1004 card declined")
            .with_field("service.name", "worker")
            .with_field("trace.id", "ddd"),
    ];
    let indexed =
        encode_indexed_structural_records(&records).expect("indexed structural block encodes");
    let recovered =
        decode_embedded_frame_index(&indexed.structural).expect("embedded index recovers");
    assert_eq!(recovered, indexed.index);
    assert!(recovered.timestamp_offset_ordinal_ordered());

    let mut unordered = records.clone();
    unordered[1].timestamp_unix_nanos = 1;
    let unordered = encode_indexed_structural_records(&unordered)
        .expect("unordered indexed structural block encodes");
    assert!(!unordered.index.timestamp_offset_ordinal_ordered());
    assert_eq!(
        decode_embedded_frame_index(&unordered.structural)
            .expect("unordered embedded index recovers"),
        unordered.index
    );

    let assert_query = |candidate_ordinals: Vec<u32>, query: LogQuery| {
        let candidates = decode_structural_records(&indexed.structural, &candidate_ordinals)
            .expect("candidates selectively decode");
        let selected = query
            .select(candidates)
            .into_iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>();
        let expected = query
            .select(decode_structural_block(&indexed.structural).expect("full block decodes"))
            .into_iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>();
        assert_eq!(selected, expected);
    };

    assert_query(
        indexed.index.term_candidate_ordinals("error"),
        LogQuery::new(records[0].record_ref.topic_partition).with_term("error"),
    );
    assert_query(
        indexed.index.term_candidate_ordinals("req"),
        LogQuery::new(records[0].record_ref.topic_partition).with_term("req"),
    );
    let mut expected_union = indexed.index.term_candidate_ordinals("error");
    expected_union.extend(indexed.index.term_candidate_ordinals("req"));
    expected_union.sort_unstable();
    expected_union.dedup();
    assert_eq!(
        indexed
            .index
            .term_candidate_ordinals_union(&["error", "req"]),
        expected_union
    );
    assert_query(
        indexed.index.term_candidate_ordinals("1002"),
        LogQuery::new(records[0].record_ref.topic_partition).with_term("1002"),
    );
    assert_query(
        indexed
            .index
            .field_candidate_ordinals("service.name", "billing"),
        LogQuery::new(records[0].record_ref.topic_partition).with_field("service.name", "billing"),
    );
    assert_query(
        indexed.index.field_candidate_ordinals("trace.id", "bbb"),
        LogQuery::new(records[0].record_ref.topic_partition).with_field("trace.id", "bbb"),
    );
    assert!(
        indexed
            .index
            .term_candidate_ordinals("definitely_absent_987654321")
            .is_empty()
    );
}

#[test]
fn packed_id_columns_choose_the_smaller_lossless_encoding() {
    let repeated_ids = vec![1_u32; 4_096];
    let repeated = pack_ids(&repeated_ids, 2).expect("repeated IDs pack");
    let mut repeated_encoded = Vec::new();
    encode_packed_column(&repeated, 2, 4_096, false, &mut repeated_encoded)
        .expect("repeated column encodes");
    let mut cursor = 0;
    assert_eq!(read_u32(&repeated_encoded, &mut cursor).unwrap(), 2);
    assert_eq!(
        read_byte(&repeated_encoded, &mut cursor).unwrap(),
        PACKED_IDS_RUN_LENGTH
    );
    cursor = 0;
    let (_, decoded, position_ordered) =
        decode_packed_column(&repeated_encoded, &mut cursor, 4_096, true).unwrap();
    assert!(!position_ordered);
    assert_eq!(decoded, repeated);
    require_consumed(&repeated_encoded, cursor).unwrap();

    let alternating_ids = (0..4_096).map(|ordinal| ordinal & 1).collect::<Vec<_>>();
    let alternating = pack_ids(&alternating_ids, 2).expect("alternating IDs pack");
    let mut alternating_encoded = Vec::new();
    encode_packed_column(&alternating, 2, 4_096, false, &mut alternating_encoded)
        .expect("alternating column encodes");
    cursor = 0;
    assert_eq!(read_u32(&alternating_encoded, &mut cursor).unwrap(), 2);
    assert_eq!(
        read_byte(&alternating_encoded, &mut cursor).unwrap(),
        PACKED_IDS_BITPACKED
    );
    cursor = 0;
    let (_, decoded, position_ordered) =
        decode_packed_column(&alternating_encoded, &mut cursor, 4_096, true).unwrap();
    assert!(!position_ordered);
    assert_eq!(decoded, alternating);
    require_consumed(&alternating_encoded, cursor).unwrap();
}

#[test]
fn exact_template_bodies_reuse_index_ids_without_per_record_bytes() {
    let records = (0..600u64)
        .map(|offset| {
            let message = if offset.is_multiple_of(2) {
                "static alpha message"
            } else {
                "static beta message"
            };
            record(offset, message)
        })
        .collect::<Vec<_>>();
    let structural = encode_structural_block(&records).expect("exact templates encode");
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    assert_eq!(read_usize(&structural, &mut cursor).unwrap(), records.len());
    for _ in 0..3 {
        let _ = read_section(&structural, &mut cursor).unwrap();
    }
    let body_lane = decode_seekable_record_lane(
        read_section(&structural, &mut cursor).unwrap(),
        records.len(),
    )
    .expect("body lane opens");
    assert!(body_lane.payload.is_empty());
    assert!(
        body_lane
            .checkpoints
            .iter()
            .all(|checkpoint| *checkpoint == 0)
    );

    let decoded = decode_structural_block(&structural).expect("exact templates decode");
    assert_eq!(decoded.len(), records.len());
    assert!(
        decoded
            .iter()
            .zip(&records)
            .all(|(decoded, record)| decoded.message.as_ref() == record.message.as_ref())
    );
    let selected = [0, 255, 256, 511, 599];
    let decoded = decode_structural_records(&structural, &selected)
        .expect("exact templates selectively decode");
    assert_eq!(decoded.len(), selected.len());
    assert!(decoded.iter().zip(selected).all(|(decoded, ordinal)| {
        decoded.message.as_ref() == records[ordinal as usize].message.as_ref()
    }));
}

#[test]
fn embedded_fingerprint_collisions_only_add_exactly_verified_candidates() {
    let records = vec![
        record(0, "alpha static message"),
        record(1, "beta static message"),
        record(2, "alpha static message"),
        record(3, "beta static message"),
    ];
    let mut indexed = encode_indexed_structural_records(&records).expect("fingerprints encode");
    let alpha = index_fingerprint(membership_hash(b"alpha"));
    let beta = index_fingerprint(membership_hash(b"beta"));
    let beta_layouts = indexed.index.terms[indexed
        .index
        .terms
        .binary_search_by_key(&beta, |locator| locator.fingerprint)
        .expect("beta locator")]
    .layout_ids
    .clone();
    let alpha_position = indexed
        .index
        .terms
        .binary_search_by_key(&alpha, |locator| locator.fingerprint)
        .expect("alpha locator");
    let alpha_locator = &mut indexed.index.terms[alpha_position];
    alpha_locator.layout_ids.extend(beta_layouts);
    alpha_locator.layout_ids.sort_unstable();
    alpha_locator.layout_ids.dedup();

    let candidates = indexed.index.term_candidate_ordinals("alpha");
    assert_eq!(candidates, vec![0, 1, 2, 3]);
    let selected = LogQuery::new(records[0].record_ref.topic_partition)
        .with_term("alpha")
        .select(
            decode_structural_records(&indexed.structural, &candidates)
                .expect("collision candidates decode"),
        );
    assert_eq!(
        selected
            .into_iter()
            .map(|record| record.offset.get())
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
}

#[test]
fn selective_decode_matches_full_decode_without_allocating_other_records() {
    let mut records = (0..1_000u64)
        .map(|offset| {
            record(
                offset,
                &format!(
                    "ERROR request_id=req-{offset:08} retry={} card declined",
                    offset % 4
                ),
            )
            .with_field("request.id", format!("req-{offset:08}"))
            .with_field(
                if offset.is_multiple_of(2) {
                    "otel.severity_text"
                } else {
                    "attr.loki.metadata.severity_text"
                },
                if offset.is_multiple_of(2) {
                    "ERROR"
                } else {
                    "WARN"
                },
            )
        })
        .collect::<Vec<_>>();
    let trace_id = TraceId::from_bytes([7; 16]).expect("trace ID is valid");
    records[7].trace_id = Some(trace_id);
    records[0].body = Some(TelemetryValue::Boolean(true));
    let encoded = encode_structural_block(&records).expect("block encodes");
    let full = decode_structural_block(&encoded).expect("full block decodes");
    let (offsets, timestamps) =
        decode_structural_positions(&encoded).expect("position lanes decode");
    assert_eq!(
        offsets,
        full.iter().map(|record| record.offset).collect::<Vec<_>>()
    );
    assert_eq!(
        timestamps,
        full.iter()
            .map(|record| record.timestamp_unix_nanos)
            .collect::<Vec<_>>()
    );
    let selected_ordinals = [0, 7, 500, 999];
    assert_eq!(
        decode_structural_trace_ids(&encoded, &selected_ordinals).expect("trace IDs decode"),
        vec![None, Some(trace_id), None, None]
    );
    let selected =
        decode_structural_records(&encoded, &selected_ordinals).expect("selection decodes");
    let embedded_index = decode_embedded_frame_index(&encoded).expect("embedded index decodes");
    let templates = decode_structural_templates(&encoded).expect("templates decode");
    let projected =
        decode_structural_records_without_typed_metadata_with_embedded_index_and_templates(
            &encoded,
            &selected_ordinals,
            &embedded_index,
            &templates,
            true,
        )
        .expect("projection selection decodes");
    assert_eq!(
        projected
            .iter()
            .map(|record| (
                record.offset,
                record.timestamp_unix_nanos,
                Arc::clone(&record.message),
                Arc::clone(&record.fields),
            ))
            .collect::<Vec<_>>(),
        selected
            .iter()
            .map(|record| (
                record.offset,
                record.timestamp_unix_nanos,
                Arc::clone(&record.message),
                Arc::clone(&record.fields),
            ))
            .collect::<Vec<_>>()
    );
    assert!(projected.iter().all(|record| record.body.is_none()));
    let severity_projected =
        decode_structural_records_without_typed_metadata_with_embedded_index_and_severity_text(
            &encoded,
            &selected_ordinals,
            &embedded_index,
        )
        .expect("severity projection decodes");
    assert_eq!(
        severity_projected
            .iter()
            .map(|record| record
                .fields
                .iter()
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        selected
            .iter()
            .map(|record| {
                record
                    .fields
                    .iter()
                    .filter(|field| {
                        matches!(
                            field.key.as_ref(),
                            "otel.severity_text" | "attr.loki.metadata.severity_text"
                        )
                    })
                    .map(|field| (field.key.as_ref(), field.value.as_ref()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    );
    let messages =
        decode_structural_messages(&encoded, &selected_ordinals).expect("messages decode");
    let fields = decode_structural_fields(&encoded, &selected_ordinals).expect("fields decode");
    assert_eq!(
        messages,
        selected
            .iter()
            .map(|record| Arc::clone(&record.message))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fields,
        selected
            .iter()
            .map(|record| Arc::clone(&record.fields))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        selected,
        selected_ordinals
            .iter()
            .map(|ordinal| full[usize::try_from(*ordinal).expect("ordinal fits")].clone())
            .collect::<Vec<_>>()
    );
    assert!(decode_structural_records(&encoded, &[7, 7]).is_err());
    assert!(decode_structural_records(&encoded, &[1_000]).is_err());
}

#[test]
fn dictionary_training_sections_ignore_offsets_and_pco_timestamps() {
    let first = vec![
        record(0, "ERROR request_id=req-1001 card declined"),
        record(1, "ERROR request_id=req-1002 card declined"),
    ];
    let mut second = first.clone();
    second[0].record_ref.offset = LogicalOffset::new(100);
    second[1].record_ref.offset = LogicalOffset::new(200);
    second[0].timestamp_unix_nanos = u64::MAX - 1;
    second[1].timestamp_unix_nanos = 17;

    let first_encoded = encode_structural_block(&first).expect("first block encodes");
    let second_encoded = encode_structural_block(&second).expect("second block encodes");
    assert_ne!(first_encoded, second_encoded);
    assert_eq!(
        dictionary_training_sections(&first_encoded).expect("first sections parse"),
        dictionary_training_sections(&second_encoded).expect("second sections parse")
    );
}

#[test]
fn varied_utf8_messages_round_trip_byte_identically() {
    let fragments = [
        "東京",
        "Échec",
        "🚀",
        "ключ",
        "مرحبا",
        "line\nbreak",
        "\0",
        "/api/v1",
        "42",
        "punct:=,[]",
    ];
    let mut state = 0x4d59_5df4_d0f3_3173u64;
    let records = (0u64..256)
        .map(|offset| {
            let mut message = String::new();
            for _ in 0..24 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let index = usize::try_from(
                    state % u64::try_from(fragments.len()).expect("fragment count fits"),
                )
                .expect("fragment index fits");
                message.push_str(fragments[index]);
                message.push(' ');
            }
            record(offset, &message).with_field("unicode.tenant", format!("顧客-{}", offset % 7))
        })
        .collect::<Vec<_>>();

    let encoded = encode_structural_block(&records).expect("UTF-8 block encodes");
    let decoded = decode_structural_block(&encoded).expect("UTF-8 block decodes");
    assert_eq!(decoded.len(), records.len());
    for (decoded, record) in decoded.iter().zip(records) {
        assert_eq!(decoded.offset, record.record_ref.offset);
        assert_eq!(decoded.timestamp_unix_nanos, record.timestamp_unix_nanos);
        assert_eq!(decoded.message, record.message);
        assert_eq!(decoded.fields.as_ref(), record.fields.as_ref());
    }
}

#[test]
fn token_templates_retain_static_terms_and_only_extract_dynamic_tokens() {
    let message = parse_message(b"ERROR request_id=req-1001 retry=2 card declined");
    let values = message
        .values
        .iter()
        .map(|range| message.message[range.clone()].to_vec())
        .collect::<Vec<_>>();
    let literals = message
        .literals
        .iter()
        .map(|range| message.message[range.clone()].to_vec())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![b"req-1001".to_vec(), b"2".to_vec()]);
    assert_eq!(
        literals,
        vec![
            b"ERROR request_id=".to_vec(),
            b" retry=".to_vec(),
            b" card declined".to_vec(),
        ]
    );
}

#[test]
fn rendered_patterns_use_the_structural_dynamic_value_classifier() {
    assert_eq!(
        message_pattern("request id=123456 duration=42ms complete"),
        "request id=<_> duration=<_> complete"
    );
    assert_eq!(message_pattern("static message"), "static message");
}

#[test]
fn tokenized_structural_layout_beats_row_blob_with_zstd() {
    let records = (0u64..4_096)
            .map(|offset| {
                let request_id = offset.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let trace_id = request_id ^ 0xd1b5_4a32_d192_ed03;
                record(
                    offset,
                    &format!(
                        "ERROR checkout request_id=req-{request_id:016x} trace_id={trace_id:016x} card declined"
                    ),
                )
                .with_field("request.id", format!("req-{request_id:016x}"))
            })
            .collect::<Vec<_>>();
    let legacy = legacy_rows(&records);
    let structural = encode_structural_block(&records).expect("structural block encodes");
    let legacy_compressed = zstd::bulk::compress(&legacy, 1).expect("legacy compresses");
    let structural_compressed =
        zstd::bulk::compress(&structural, 1).expect("structural block compresses");

    assert!(
        structural_compressed.len() < legacy_compressed.len(),
        "structural={} legacy={}",
        structural_compressed.len(),
        legacy_compressed.len()
    );
}

#[test]
fn pco_timestamp_column_round_trips_regular_values_and_extremes() {
    let mut values = Vec::with_capacity(1_025);
    let mut timestamp = 1_700_000_000_000_000_000u64;
    for index in 0..1_025u64 {
        timestamp = timestamp.saturating_add(2_250 + (index % 17) * 10);
        values.push(timestamp);
    }
    values[255] = values[254].saturating_add(3_596_626);
    values[256] = values[255].saturating_add(2_250);
    values[511] = u64::MAX;
    values[512] = 0;
    values[513] = 2_250;

    let encoded = encode_timestamp_values(&values);
    let decoded = decode_timestamps(&encoded, values.len()).expect("timestamps decode");
    assert_eq!(decoded, values);
}

#[test]
fn pco_timestamp_column_compacts_regular_input_before_zstd() {
    let mut values = Vec::with_capacity(4_096);
    let mut timestamp = 1_700_000_000_000_000_000u64;
    for index in 0..4_096u64 {
        timestamp += 2_250 + (index % 29) * 10;
        values.push(timestamp);
    }
    let pco = encode_timestamp_values(&values);
    let mut legacy = Vec::new();
    write_varint(values[0], &mut legacy);
    for pair in values.windows(2) {
        legacy.push(0);
        write_varint(pair[1] - pair[0], &mut legacy);
    }
    assert!(
        pco.len() < legacy.len(),
        "pco={} legacy={}",
        pco.len(),
        legacy.len()
    );
}

#[test]
fn arbitrary_timestamp_bit_patterns_round_trip_exactly() {
    let mut state = 0x4d59_5df4_d0f3_3173u64;
    let mut values = Vec::with_capacity(4_097);
    for index in 0..4_097 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.push(match index % 257 {
            0 => 0,
            1 => u64::MAX,
            _ => state,
        });
    }
    let encoded = encode_timestamp_values(&values);
    assert_eq!(
        decode_timestamps(&encoded, values.len()).expect("timestamps decode"),
        values
    );
}

#[test]
fn malformed_pco_timestamps_and_count_mismatches_are_rejected() {
    let values = [10_000, 12_250, 14_500];
    let mut corrupted = encode_timestamp_values(&values);
    corrupted[0] ^= 0xff;
    assert_eq!(
        decode_timestamps(&corrupted, values.len()),
        Err(TelemetryError::InvalidBlockEncoding(
            "invalid Pco timestamp section"
        ))
    );

    let mut truncated = encode_timestamp_values(&values);
    truncated.pop();
    assert!(decode_timestamps(&truncated, values.len()).is_err());

    let encoded = encode_timestamp_values(&values);
    assert_eq!(
        decode_timestamps(&encoded, values.len() - 1),
        Err(TelemetryError::InvalidBlockEncoding(
            "Pco timestamp count mismatch"
        ))
    );
}

#[test]
fn malformed_structural_block_rejects_unbounded_record_count() {
    let error = decode_structural_block(b"STLG\xff\xff\xff\xff\x0f")
        .expect_err("truncated block cannot allocate from its record count");
    assert_eq!(error, TelemetryError::InvalidBlockEncoding("record count"));
}
