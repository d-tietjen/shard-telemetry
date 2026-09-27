use super::*;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{ResourceContext, ScopeContext, SpanId, TelemetryRecordRef, TelemetrySignal};

fn span(index: u8, parent: Option<u8>, name: &str) -> DurableSpan {
    DurableSpan {
        stream_shard_id: ShardId::new(0),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Traces,
            TopicPartition::new(TopicId::new(2), LogicalPartitionId::new(0)),
            LogicalOffset::new(u64::from(index)),
        ),
        tenant: Arc::from("tenant"),
        resource: Arc::new(ResourceContext::default()),
        scope: Arc::new(ScopeContext::default()),
        trace_id: TraceId::from_bytes([1; 16]).unwrap(),
        span_id: SpanId::from_bytes([index; 8]).unwrap(),
        parent_span_id: parent.map(|parent| SpanId::from_bytes([parent; 8]).unwrap()),
        trace_state: Arc::from(""),
        flags: 0,
        name: Arc::from(name),
        kind: 0,
        start_time_unix_nanos: u64::from(index),
        duration_nanos: 1,
        attributes: Arc::default(),
        dropped_attributes_count: 0,
        events: Arc::default(),
        dropped_events_count: 0,
        links: Arc::default(),
        dropped_links_count: 0,
        status: None,
    }
}

#[test]
fn clean_room_filter_parses_typed_conditions_and_boolean_groups() {
    let filter = TraceFilter::parse(
        r#"{ resource.service.name = "api" && duration >= 250ms || status = 2 }"#,
    )
    .expect("filter parses");
    assert!(matches!(filter, TraceFilter::Or(_)));
}

#[test]
fn exact_trace_id_is_available_for_index_pushdown() {
    let filter =
        TraceFilter::parse("{ trace:id = \"01010101010101010101010101010101\" && duration > 1ms }")
            .unwrap();
    assert_eq!(filter.exact_trace_id(), TraceId::from_bytes([1; 16]).ok());
}

#[test]
fn structural_operators_select_related_spans() {
    let spans = vec![
        span(1, None, "root"),
        span(2, Some(1), "child"),
        span(3, Some(2), "grandchild"),
        span(4, Some(1), "sibling"),
        span(5, None, "orphan"),
    ];
    let descendants = SpansetExpr::parse(r#"{ name = "root" } >> { name = "grandchild" }"#)
        .unwrap()
        .evaluate(&spans);
    assert_eq!(descendants, vec![2]);

    let siblings = SpansetExpr::parse(r#"{ name = "child" } ~ { name = "sibling" }"#)
        .unwrap()
        .evaluate(&spans);
    assert_eq!(siblings, vec![3]);

    let negative = SpansetExpr::parse(r#"{ name = "root" } !>> { name = "orphan" }"#)
        .unwrap()
        .evaluate(&spans);
    assert_eq!(negative, vec![4]);
}

#[test]
fn union_structural_operator_returns_both_sides() {
    let spans = vec![span(1, None, "root"), span(2, Some(1), "child")];
    let selected = SpansetExpr::parse(r#"{ name = "root" } &> { name = "child" }"#)
        .unwrap()
        .evaluate(&spans);
    assert_eq!(selected, vec![0, 1]);
}

#[test]
fn arrays_nil_and_regex_follow_traceql_match_semantics() {
    let mut record = span(1, None, "checkout-handler");
    record.attributes = Arc::new(vec![TelemetryAttribute::new(
        "roles",
        TelemetryValue::Array(Arc::new(vec![
            TelemetryValue::String(Arc::from("reader")),
            TelemetryValue::String(Arc::from("writer")),
        ])),
    )]);
    let spans = vec![record];

    assert_eq!(
        SpansetExpr::parse(r#"{ span.roles = "writer" }"#)
            .unwrap()
            .evaluate(&spans),
        vec![0]
    );
    assert!(
        SpansetExpr::parse(r#"{ span.roles != "reader" }"#)
            .unwrap()
            .evaluate(&spans)
            .is_empty()
    );
    assert_eq!(
        SpansetExpr::parse(r#"{ span.missing = nil }"#)
            .unwrap()
            .evaluate(&spans),
        vec![0]
    );
    assert!(
        SpansetExpr::parse(r#"{ name =~ "checkout" }"#)
            .unwrap()
            .evaluate(&spans)
            .is_empty(),
        "TraceQL regexes are anchored"
    );
}

#[test]
fn pipeline_groups_and_filters_spansets_with_typed_aggregates() {
    let spans = vec![
        span(1, None, "root"),
        span(2, Some(1), "worker"),
        span(3, Some(1), "worker"),
    ];
    assert_eq!(
        TraceqlQuery::parse("{} | count() >= 3")
            .unwrap()
            .evaluate(&spans),
        vec![0, 1, 2]
    );
    assert!(
        TraceqlQuery::parse("{} | count() > 3")
            .unwrap()
            .evaluate(&spans)
            .is_empty()
    );
    let selected =
        TraceqlQuery::parse("{} | by(name) | count() >= 2 | select(name, duration)").unwrap();
    assert_eq!(selected.evaluate(&spans), vec![1, 2]);
    assert_eq!(selected.selected_fields().as_ref(), &["name", "duration"]);
    assert_eq!(
        TraceqlQuery::parse("{} | sum(duration) >= 3ns")
            .unwrap()
            .evaluate(&spans),
        vec![0, 1, 2]
    );
}

#[test]
fn metrics_parser_accepts_grouping_threshold_quantile_and_series_limits() {
    let rate =
        TraceMetricQuery::parse("{} | rate() by (resource.service.name) > 1 | topk(10)").unwrap();
    assert_eq!(rate.function, TraceMetricFunction::Rate);
    assert_eq!(rate.group_by, vec!["resource.service.name"]);
    assert_eq!(rate.series_limit, Some((true, 10)));
    assert!(rate.passes(2.0));
    assert!(!rate.passes(1.0));

    let quantile =
        TraceMetricQuery::parse("{ status = 2 } | quantile_over_time(duration, .99)").unwrap();
    assert_eq!(quantile.function, TraceMetricFunction::Quantile);
    assert_eq!(quantile.field.as_deref(), Some("duration"));
    assert_eq!(quantile.quantile, Some(0.99));
    assert_eq!(quantile.aggregate(&[1.0, 2.0, 3.0], 1.0), 2.98);
}
