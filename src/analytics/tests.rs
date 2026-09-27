use std::io::Cursor;

use arrow_array::{MapArray, StringArray, TimestampNanosecondArray};
use arrow_ipc::reader::StreamReader;
use serde_json::Value as JsonValue;

use super::*;
use crate::LokiEntry;

#[test]
fn channel_writer_coalesces_arrow_flushes_until_size_or_finish() {
    let (sender, mut receiver) = mpsc::channel(8);
    let mut writer = ChannelWriter::new(sender, 64);
    writer.write_all(b"schema").unwrap();
    writer.flush().unwrap();
    assert!(receiver.try_recv().is_err());
    writer.write_all(b"batch").unwrap();
    writer.finish().unwrap();
    assert_eq!(
        receiver.try_recv().unwrap().unwrap().as_ref(),
        b"schemabatch"
    );
}

#[test]
fn request_parses_relation_columns_ids_and_pushdown_constraints() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("relation=spans&start_ns=10&end_ns=20&trace_id=01010101010101010101010101010101&resource.service.name=api&columns=timestamp,trace_id,name&limit=7"),
    )
    .expect("valid scan");
    assert_eq!(request.relation, AnalyticsRelation::Spans);
    assert_eq!(request.start_timestamp_unix_nanos, Some(10));
    assert_eq!(request.end_timestamp_unix_nanos, Some(20));
    assert_eq!(
        request.trace_id.expect("trace").to_string(),
        "01010101010101010101010101010101"
    );
    assert_eq!(
        request.resource_attributes[0],
        MetadataField::new("service.name", "api")
    );
    assert_eq!(
        request.columns,
        vec![
            AnalyticsColumn::Timestamp,
            AnalyticsColumn::TraceId,
            AnalyticsColumn::Name
        ]
    );
    assert_eq!(request.limit, Some(7));
}

#[test]
fn request_rejects_ambiguous_or_relation_incompatible_inputs() {
    for query in [
        "unknown=value",
        "relation=logs&relation=spans",
        "relation=unknown",
        "relation=spans&columns=message",
        "columns=timestamp&columns=message",
        "columns=timestamp,timestamp",
        "columns=",
        "start_ns=20&end_ns=20",
        "start_ns=-1",
        "limit=-1",
        "label.=value",
        "relation=spans&term=error",
        "relation=spans&message_token=error",
        "relation=spans&message_token_ci=error",
        "relation=spans&message_regex=error",
        "trace_id=00",
        "columns=offset&cardinality_only=maybe",
        "columns=offset&cardinality_only=1&cardinality_only=1",
        "columns=message&cardinality_only=1",
        "columns=partition,offset&cardinality_only=1&wire=rowbinary",
        "order=timestamp_desc",
        "limit=1&order=unknown",
        "limit=1&order=timestamp_desc&order=timestamp_asc",
        "relation=spans&limit=1&order=timestamp_desc",
    ] {
        assert!(
            parse_scan_request("tenant-a".to_owned(), Some(query)).is_err(),
            "query must fail closed: {query}"
        );
    }
}

#[test]
fn request_parses_searchbench_token_predicates() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("message_token_prefix=conn&message_token_regex=charg.*&message_phrase=failed|order:2&message_fuzzy=connection:1&message_like=%nnec%"),
    )
    .expect("search predicates parse");
    assert!(matches!(request.predicate, LogPredicate::And(_)));
    assert!(request.validate().is_ok());
}

#[test]
fn request_parses_relevance_order_explicit_or_and_trace_join() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("message_phrase=failed|to|place|order&message_token_ci=charge&predicate_operator=or&limit=100&order=score_desc&columns=timestamp,message,score&wire=jsonl"),
    )
    .expect("relevance request parses");
    assert_eq!(request.order, Some(AnalyticsScanOrder::RelevanceDescending));
    assert!(request.predicate_any);
    assert!(matches!(request.predicate, LogPredicate::Or(_)));
    assert!(request.columns.contains(&AnalyticsColumn::Score));

    let join = parse_scan_request(
        "tenant-a".to_owned(),
        Some("columns=partition&cardinality_only=1&distinct_trace_id=1&trace_join_service=payment&wire=rowbinary"),
    )
    .expect("trace join parses");
    assert!(join.distinct_trace_id);
    assert_eq!(join.trace_join_service.as_deref(), Some("payment"));
    assert!(join.validate().is_ok());
}

#[test]
fn log_projection_only_requires_typed_metadata_for_typed_columns() {
    assert!(!log_columns_need_typed_metadata(&[
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::Message,
        AnalyticsColumn::Labels,
        AnalyticsColumn::Metadata,
    ]));
    assert!(log_columns_need_typed_metadata(&[
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::Message,
        AnalyticsColumn::BodyJson,
    ]));
    assert!(log_columns_need_typed_metadata(&[
        AnalyticsColumn::ResourceId
    ]));
    assert!(!log_columns_need_typed_metadata(&[
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::SeverityText,
        AnalyticsColumn::Message,
    ]));
    assert!(!log_columns_need_structural_fields(&[
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::SeverityText,
        AnalyticsColumn::Message,
    ]));
    assert!(log_columns_need_structural_fields(&[
        AnalyticsColumn::Labels
    ]));
}

#[test]
fn cardinality_request_is_explicit_and_uses_one_fixed_width_lane() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("columns=offset&cardinality_only=1"),
    )
    .expect("cardinality request");
    assert!(request.cardinality_only);
    assert_eq!(request.columns, [AnalyticsColumn::Offset]);

    let rowbinary = parse_scan_request(
        "tenant-a".to_owned(),
        Some("columns=partition&cardinality_only=1&wire=rowbinary"),
    )
    .expect("RowBinary cardinality request");
    assert!(rowbinary.cardinality_only);
    assert_eq!(rowbinary.columns, [AnalyticsColumn::Partition]);

    assert!(
        parse_scan_request(
            "tenant-a".to_owned(),
            Some("columns=offset&cardinality_only=1&wire=jsonl"),
        )
        .is_err()
    );
}

#[test]
fn bounded_timestamp_order_is_parsed_explicitly() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("columns=timestamp,message&limit=100&order=timestamp_desc"),
    )
    .expect("ordered request");
    assert_eq!(request.order, Some(AnalyticsScanOrder::TimestampDescending));
    assert_eq!(request.limit, Some(100));
}

#[test]
fn exact_message_token_is_parsed_for_log_scans() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("message_token=Cannot&message_token_ci=cannot&limit=10&order=timestamp_desc"),
    )
    .expect("exact token scan");
    assert_eq!(request.message_tokens, [Arc::<str>::from("Cannot")]);
    assert_eq!(
        request.case_insensitive_message_tokens,
        [Arc::<str>::from("cannot")]
    );
}

#[test]
fn boolean_message_and_field_predicates_are_parsed() {
    let request = parse_scan_request(
        "tenant-a".to_owned(),
        Some("message_any=error&message_any=failed&message_not=cache&field_numeric.otel.severity_number=ge:13&field_regex.service.name=checkout.*"),
    )
    .expect("predicate scan");
    assert!(matches!(request.predicate, LogPredicate::And(predicates) if predicates.len() == 4));
    let min_match = parse_scan_request(
        "tenant-a".to_owned(),
        Some("message_any=error&message_any=failed&message_any=charge&message_any=cache&message_min_match=2"),
    )
    .expect("min-match scan");
    assert!(matches!(min_match.predicate, LogPredicate::Or(predicates) if predicates.len() == 6));
}

#[test]
fn in_memory_scan_applies_boolean_predicates() {
    let entries = vec![
        LokiEntry {
            timestamp_unix_nanos: 1,
            labels: BTreeMap::new(),
            line: "request failed".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
        LokiEntry {
            timestamp_unix_nanos: 2,
            labels: BTreeMap::new(),
            line: "request failed cache".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
    ];
    let mut request = AnalyticsScanRequest::new("tenant-a");
    request.predicate = LogPredicate::and(vec![
        LogPredicate::message_regex("failed", CaseSensitivity::Sensitive).expect("valid regex"),
        LogPredicate::negate(LogPredicate::message_token(
            "cache",
            CaseSensitivity::Sensitive,
        )),
    ]);
    let mut rows = Vec::new();
    scan_entries(entries, &request, &mut |batch| {
        rows.extend_from_slice(batch);
        Ok(())
    })
    .expect("predicate scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].message.as_deref(), Some("request failed"));
}

#[test]
fn zero_limit_is_a_valid_empty_scan() {
    let entries = vec![LokiEntry {
        timestamp_unix_nanos: 11,
        labels: BTreeMap::new(),
        line: "must not be emitted".to_owned(),
        structured_metadata: BTreeMap::new(),
    }];
    let request =
        parse_scan_request("tenant-a".to_owned(), Some("limit=0")).expect("zero limit is valid");
    let mut called = false;
    scan_entries(entries, &request, &mut |_| {
        called = true;
        Ok(())
    })
    .expect("empty scan");
    assert!(!called);
}

#[test]
fn projected_arrow_batch_preserves_timestamp_message_and_maps() {
    let mut row = AnalyticsRow::empty(Arc::from("tenant-a"), "logs", 123, 4, 9).expect("row");
    row.message = Some(Arc::from("request \"failed\"\\n"));
    row.labels
        .insert("app\nname".to_owned(), "api\\edge".to_owned());
    row.metadata.insert("code".to_owned(), "500".to_owned());
    let columns = vec![
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::Message,
        AnalyticsColumn::Labels,
    ];
    let schema = projection_schema(&columns);
    let batch = record_batch(&[row], &columns, Arc::clone(&schema)).expect("batch");
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("writer");
        writer.write(&batch).expect("write");
        writer.finish().expect("finish");
    }
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).expect("reader");
    let decoded = reader.next().expect("one batch").expect("valid batch");
    assert_eq!(
        decoded
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        123
    );
    assert_eq!(
        decoded
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "request \"failed\"\\n"
    );
    assert_eq!(
        decoded
            .column(2)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap()
            .value_length(0),
        1
    );
}

#[test]
fn jsonlines_projection_preserves_exact_numeric_and_map_values() {
    let mut row = AnalyticsRow::empty(
        Arc::from("tenant-a"),
        "logs",
        1_800_000_000_000_000_001,
        4,
        9,
    )
    .expect("row");
    row.message = Some(Arc::from("request \"failed\"\\n"));
    row.labels
        .insert("app\nname".to_owned(), "api\\edge".to_owned());
    row.metadata.insert("code".to_owned(), "500".to_owned());
    let columns = vec![
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::Offset,
        AnalyticsColumn::Message,
        AnalyticsColumn::Labels,
        AnalyticsColumn::Metadata,
    ];
    let mut output = Vec::new();
    write_jsonlines_row(&mut output, &row, &columns).expect("JSON lines row");
    assert!(output.ends_with(b"\n"));
    let value: JsonValue = serde_json::from_slice(&output).expect("valid JSON line");
    assert_eq!(value["timestamp"], 1_800_000_000_000_000_001_i64);
    assert_eq!(value["offset"], 9_u64);
    assert_eq!(value["message"], "request \"failed\"\\n");
    assert_eq!(value["labels"]["app\nname"], "api\\edge");
    assert_eq!(value["metadata"]["code"], "500");
}

#[test]
fn every_relation_builds_its_complete_declared_arrow_schema() {
    for relation in [
        AnalyticsRelation::Logs,
        AnalyticsRelation::Spans,
        AnalyticsRelation::SpanEvents,
        AnalyticsRelation::SpanLinks,
        AnalyticsRelation::MetricPoints,
        AnalyticsRelation::MetricExemplars,
    ] {
        let row =
            AnalyticsRow::empty(Arc::from("tenant-a"), relation.signal(), 123, 4, 9).expect("row");
        let schema = projection_schema(relation.columns());
        let batch =
            record_batch(&[row], relation.columns(), Arc::clone(&schema)).expect("relation batch");
        assert_eq!(batch.num_rows(), 1, "{relation:?}");
        assert_eq!(
            batch.num_columns(),
            relation.columns().len(),
            "{relation:?}"
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            relation
                .columns()
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            "{relation:?}"
        );
    }
}

#[test]
fn relation_columns_are_unique_and_drawn_from_the_public_catalog() {
    let catalog = ALL_COLUMNS.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(catalog.len(), ALL_COLUMNS.len());
    for relation in [
        AnalyticsRelation::Logs,
        AnalyticsRelation::Spans,
        AnalyticsRelation::SpanEvents,
        AnalyticsRelation::SpanLinks,
        AnalyticsRelation::MetricPoints,
        AnalyticsRelation::MetricExemplars,
    ] {
        let columns = relation.columns().iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(columns.len(), relation.columns().len(), "{relation:?}");
        assert!(columns.is_subset(&catalog), "{relation:?}");
    }
}

#[test]
fn typed_attribute_fingerprints_disambiguate_equal_renderings() {
    let string = TelemetryAttribute::new("code", TelemetryValue::String(Arc::from("1")));
    let integer = TelemetryAttribute::new("code", TelemetryValue::Integer(1));
    assert_eq!(
        attribute_map(std::slice::from_ref(&string)),
        attribute_map(std::slice::from_ref(&integer))
    );
    assert_ne!(attribute_ids(&[string]), attribute_ids(&[integer]));
}

#[test]
fn in_memory_scan_applies_log_constraints() {
    let entries = vec![
        LokiEntry {
            timestamp_unix_nanos: 11,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "request completed".to_owned(),
            structured_metadata: BTreeMap::from([("code".to_owned(), "200".to_owned())]),
        },
        LokiEntry {
            timestamp_unix_nanos: 12,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "request ERROR".to_owned(),
            structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
        },
    ];
    let mut request = AnalyticsScanRequest::new("tenant-a");
    request.start_timestamp_unix_nanos = Some(10);
    request.end_timestamp_unix_nanos = Some(20);
    request.terms.push(Arc::from("error"));
    request.message_tokens.push(Arc::from("ERROR"));
    request.labels.push(MetadataField::new("app", "api"));
    request.metadata.push(MetadataField::new("code", "500"));
    let mut observed = Vec::new();
    scan_entries(entries, &request, &mut |rows| {
        observed.extend_from_slice(rows);
        Ok(())
    })
    .expect("scan");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].message.as_deref(), Some("request ERROR"));
}

#[test]
fn indexed_relevance_score_matches_message_scan() {
    let mut request = AnalyticsScanRequest::new("tenant-a");
    request.message_tokens.push(Arc::from("error"));
    request.predicate = LogPredicate::And(vec![
        LogPredicate::message_token("checkout", CaseSensitivity::Insensitive),
        LogPredicate::message_token("ERROR", CaseSensitivity::Insensitive),
    ]);
    let scorer = RelevanceScorer::from_request(&request);
    let message = "ERROR checkout error";
    let mut document_length = 0_u32;
    let mut frequencies = BTreeMap::<String, u32>::new();
    crate::query::scan_clickhouse_tokens(message, |token| {
        document_length = document_length.saturating_add(1);
        let token = token.to_ascii_lowercase();
        let frequency = frequencies.entry(token).or_default();
        *frequency = frequency.saturating_add(1);
    });
    let indexed = scorer.score_indexed(document_length, |term| {
        frequencies.get(term).copied().unwrap_or_default()
    });
    let indexed_by_index = scorer.score_indexed_by_index(document_length, |index| {
        frequencies
            .get(scorer.terms()[index].as_ref())
            .copied()
            .unwrap_or_default()
    });
    assert_eq!(scorer.score(message).to_bits(), indexed.to_bits());
    assert_eq!(indexed.to_bits(), indexed_by_index.to_bits());
}
