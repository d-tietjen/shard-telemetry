use super::*;

#[test]
fn signal_aware_batch_and_partition_ack_round_trip() {
    let topic_partition = TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(7));
    let batch = NativeTelemetryBatch {
        partitions: vec![NativePartitionAppend {
            topic_partition,
            envelope: TelemetryEnvelope::new(
                crate::TelemetrySignal::Traces,
                "tenant-a",
                2,
                &b"route"[..],
                &b"payload"[..],
            )
            .unwrap(),
            transient_context: Some(Arc::<[u8]>::from(&b"context"[..])),
        }],
    };
    let encoded = batch.encode().unwrap();
    assert_eq!(NativeTelemetryBatch::decode(&encoded).unwrap(), batch);
    let (ranged, wire_ranges) = NativeTelemetryBatch::decode_with_envelope_ranges(&encoded)
        .expect("all append wire ranges");
    assert_eq!(ranged, batch);
    assert_eq!(wire_ranges.len(), 1);
    assert_eq!(
        &encoded[wire_ranges[0].0.clone()],
        batch.partitions[0].envelope.encode().unwrap()
    );
    assert_eq!(
        wire_ranges[0]
            .1
            .as_ref()
            .map(|range| &encoded[range.clone()]),
        Some(&b"context"[..])
    );
    let (decoded, envelope_range) =
        NativeTelemetryBatch::decode_native_append_with_envelope_range(&encoded)
            .expect("single append range");
    assert_eq!(decoded, batch);
    assert_eq!(
        &encoded[envelope_range.clone()],
        batch.partitions[0].envelope.encode().unwrap().as_slice()
    );
    let (view, borrowed_envelope_range, transient_range) =
        NativeTelemetryBatch::decode_native_append_view_with_ranges(&encoded)
            .expect("borrowed single append view");
    assert_eq!(view.topic_partition, topic_partition);
    assert_eq!(view.signal, crate::TelemetrySignal::Traces);
    assert_eq!(view.tenant, "tenant-a");
    assert_eq!(view.item_count, 2);
    assert_eq!(borrowed_envelope_range, envelope_range);
    assert_eq!(
        transient_range.map(|range| &encoded[range]),
        Some(&b"context"[..])
    );

    let acknowledgement = NativeTelemetryAppendAck {
        partitions: vec![NativePartitionAck {
            topic_partition,
            first_offset: 10,
            last_offset: 11,
        }],
    };
    assert_eq!(
        NativeTelemetryAppendAck::decode(&acknowledgement.encode().unwrap()).unwrap(),
        acknowledgement
    );
}

fn entries() -> Vec<LokiEntry> {
    vec![
        LokiEntry {
            timestamp_unix_nanos: 10,
            labels: BTreeMap::from([
                ("app".to_owned(), "api".to_owned()),
                ("region".to_owned(), "東京".to_owned()),
            ]),
            line: "request café".to_owned(),
            structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
        },
        LokiEntry {
            timestamp_unix_nanos: 11,
            labels: BTreeMap::from([
                ("app".to_owned(), "api".to_owned()),
                ("region".to_owned(), "東京".to_owned()),
            ]),
            line: "request complete".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
    ]
}

#[test]
fn log_query_result_sorts_streams_after_hash_grouping() {
    let entries = vec![
        LokiEntry {
            timestamp_unix_nanos: 10,
            labels: BTreeMap::from([("app".to_owned(), "worker".to_owned())]),
            line: "worker".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
        LokiEntry {
            timestamp_unix_nanos: 11,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "api".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
    ];
    let mut reversed = entries.clone();
    reversed.reverse();
    assert_eq!(
        encode_native_log_query_result("tenant-a", entries).expect("entries encode"),
        encode_native_log_query_result("tenant-a", reversed).expect("reversed entries encode")
    );
}

#[test]
fn log_query_result_round_trips_byte_exact_text_and_metadata() {
    let expected = entries();
    let encoded =
        encode_native_log_query_result("tenant-a", expected.clone()).expect("query result encodes");
    let decoded = decode_native_log_query_result(&encoded).expect("query result decodes");
    assert_eq!(decoded.tenant, "tenant-a");
    assert_eq!(decoded.entries, expected);
}

#[test]
fn projected_log_query_result_matches_materialized_wire_encoding() {
    let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let labels = Arc::new(vec![
        crate::MetadataField::new("resource.loki.label.app", "api"),
        crate::MetadataField::new("resource.loki.label.region", "東京"),
        crate::MetadataField::new("attr.loki.metadata.trace_id", "abc"),
    ]);
    let mut first = crate::DurableLog::new(
        shard_stream_core::ShardId::new(0),
        topic_partition,
        shard_stream_core::LogicalOffset::new(0),
        10,
        "request café",
        crate::CompressionCohortId::new(0),
    );
    first.fields = Arc::clone(&labels);
    let mut second = crate::DurableLog::new(
        shard_stream_core::ShardId::new(0),
        topic_partition,
        shard_stream_core::LogicalOffset::new(1),
        11,
        "request complete",
        crate::CompressionCohortId::new(0),
    );
    second.fields = Arc::new(
        labels
            .iter()
            .filter(|field| field.key.as_ref() != "attr.loki.metadata.trace_id")
            .cloned()
            .collect(),
    );
    let projected = encode_native_log_query_matches(
        "tenant-a",
        vec![
            crate::LogMatch { record: first },
            crate::LogMatch { record: second },
        ],
    )
    .expect("projected result encodes");
    assert_eq!(
        projected,
        encode_native_log_query_result("tenant-a", entries()).unwrap()
    );
}

#[test]
fn log_query_result_rejects_truncation_count_mismatch_and_trailing_bytes() {
    let encoded = encode_native_log_query_result("tenant-a", entries()).expect("query result");
    for end in 0..encoded.len() {
        assert!(
            decode_native_log_query_result(&encoded[..end]).is_err(),
            "{end}"
        );
    }
    let mut count = encoded.clone();
    count[8..12].copy_from_slice(&3_u32.to_le_bytes());
    assert!(decode_native_log_query_result(&count).is_err());
    let mut reserved = encoded.clone();
    reserved[12] = 1;
    assert!(decode_native_log_query_result(&reserved).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_native_log_query_result(&trailing).is_err());
}

#[test]
fn frame_header_checks_magic_version_length_and_payload_checksum() {
    let frame = NativeFrame::request(NativeOpcode::Append, 42, vec![1, 2, 3]).expect("frame");
    let encoded = frame.encode();
    let header: [u8; NATIVE_FRAME_HEADER_BYTES] = encoded[..NATIVE_FRAME_HEADER_BYTES]
        .try_into()
        .expect("header");
    let decoded = NativeFrameHeader::decode(&header).expect("decode");
    assert_eq!(decoded.request_id, 42);
    decoded
        .verify_payload(&encoded[NATIVE_FRAME_HEADER_BYTES..])
        .expect("checksum");
    assert_eq!(
        decoded
            .verify_payload_and_hash(&encoded[NATIVE_FRAME_HEADER_BYTES..])
            .expect("checksum hash"),
        blake3::hash(&[1, 2, 3])
    );
    assert!(decoded.verify_payload(&[1, 2, 4]).is_err());

    let untracked =
        NativeFrame::request(NativeOpcode::AppendUntracked, 43, vec![1, 2, 3]).expect("frame");
    let untracked_header: [u8; NATIVE_FRAME_HEADER_BYTES] = untracked.encode()
        [..NATIVE_FRAME_HEADER_BYTES]
        .try_into()
        .expect("header");
    assert_eq!(
        NativeFrameHeader::decode(&untracked_header)
            .expect("decode")
            .opcode,
        NativeOpcode::AppendUntracked
    );

    let mut invalid = header;
    invalid[4] = 3;
    assert!(NativeFrameHeader::decode(&invalid).is_err());
}

#[test]
fn indexed_query_round_trips_all_bounds_and_constraints() {
    let query = NativeQuery {
        tenant: "tenant-a".to_owned(),
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        terms: vec!["timeout".to_owned(), "error".to_owned()],
        start_timestamp_unix_nanos: Some(10),
        end_timestamp_unix_nanos: Some(20),
        limit: 100,
        direction: NativeQueryDirection::NewestFirst,
    };
    assert_eq!(
        decode_native_query(&encode_native_query(&query).expect("encode")).expect("decode"),
        query
    );
}

#[test]
fn signal_native_queries_and_capabilities_round_trip() {
    let metric = crate::MetricQuery {
        tenant: std::sync::Arc::from("tenant-a"),
        name: Some(std::sync::Arc::from("requests_total")),
        limit: 10,
        ..crate::MetricQuery::default()
    };
    let mut expected_metric = METRIC_QUERY_MAGIC.to_vec();
    expected_metric.extend(rmp_serde::to_vec(&metric).expect("reference metric encoding"));
    assert_eq!(
        encode_native_metric_query(&metric).expect("encode"),
        expected_metric
    );
    assert_eq!(
        decode_native_metric_query(&encode_native_metric_query(&metric).expect("encode"))
            .expect("decode"),
        metric
    );
    let trace = crate::TraceQuery {
        tenant: std::sync::Arc::from("tenant-a"),
        name: Some(std::sync::Arc::from("checkout")),
        limit: 10,
        ..crate::TraceQuery::default()
    };
    assert_eq!(
        decode_native_trace_query(&encode_native_trace_query(&trace).expect("encode"))
            .expect("decode"),
        trace
    );
    let capabilities = NativeCapabilities {
        protocol_version: 1,
        append_v1: true,
        logical_partitions: [8, 8, 8],
        query_logs: true,
        query_metrics: true,
        query_traces: true,
        query_series: false,
        append_queryable: true,
    };
    assert_eq!(
        decode_native_capabilities(&encode_native_capabilities(&capabilities).expect("encode"))
            .expect("decode"),
        capabilities
    );
}
