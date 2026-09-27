use super::*;
use crate::TRACES_TOPIC_ID;

fn span(offset: u64, trace_byte: u8, span_byte: u8) -> DurableSpan {
    DurableSpan {
        stream_shard_id: ShardId::new(3),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Traces,
            TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(9)),
            LogicalOffset::new(offset),
        ),
        tenant: Arc::from("tenant-a"),
        resource: Arc::new(ResourceContext::default()),
        scope: Arc::new(ScopeContext::default()),
        trace_id: TraceId::from_bytes([trace_byte; 16]).unwrap(),
        span_id: SpanId::from_bytes([span_byte; 8]).unwrap(),
        parent_span_id: None,
        trace_state: Arc::from(""),
        flags: 1,
        name: Arc::from("GET /checkout"),
        kind: 2,
        start_time_unix_nanos: 1_000 + offset,
        duration_nanos: 50,
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "http.status_code",
            crate::TelemetryValue::Integer(200),
        )]),
        dropped_attributes_count: 0,
        events: Arc::new(Vec::new()),
        dropped_events_count: 0,
        links: Arc::new(Vec::new()),
        dropped_links_count: 0,
        status: Some(SpanStatus {
            message: Arc::from("ok"),
            code: 1,
        }),
    }
}

#[test]
fn trace_block_round_trips_after_sorting() {
    let records = vec![span(8, 2, 3), span(2, 1, 2), span(1, 1, 1)];
    let encoded = encode_trace_block(&records).unwrap();
    let decoded = decode_trace_block(&encoded).unwrap();
    assert_eq!(decoded[0], records[2]);
    assert_eq!(decoded[1], records[1]);
    assert_eq!(decoded[2], records[0]);
}

#[test]
fn analytical_trace_pushdown_matches_exact_ids_names_and_rendered_attributes() {
    let mut record = span(1, 1, 2);
    record.resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "service.name",
            crate::TelemetryValue::String(Arc::from("checkout-api")),
        )]),
        ..ResourceContext::default()
    });
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        trace_id: Some(record.trace_id),
        span_id: Some(record.span_id),
        name: Some(Arc::clone(&record.name)),
        exact_attributes: Arc::new(vec![(Arc::from("http.status_code"), Arc::from("200"))]),
        exact_resource_attributes: Arc::new(vec![(
            Arc::from("service.name"),
            Arc::from("checkout-api"),
        )]),
        limit: 1,
        ..TraceQuery::default()
    };
    assert!(trace_query_matches(&query, &record));

    let mut mismatch = query;
    mismatch.exact_resource_attributes = Arc::new(vec![(
        Arc::from("service.name"),
        Arc::from("inventory-api"),
    )]);
    assert!(!trace_query_matches(&mismatch, &record));
}

#[test]
fn bounded_resource_index_preserves_global_trace_order_across_sealing() {
    let with_service = |offset, trace_byte, service: &'static str| {
        let mut record = span(offset, trace_byte, trace_byte);
        record.resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![TelemetryAttribute::new(
                "service.name",
                crate::TelemetryValue::String(Arc::from(service)),
            )]),
            ..ResourceContext::default()
        });
        record
    };
    let mut stripe = TraceStripe::new(4 * 1024 * 1024).unwrap();
    for trace_byte in (1..=50).rev() {
        let record = with_service(
            u64::from(trace_byte),
            trace_byte,
            if trace_byte == 50 {
                "inventory-api"
            } else {
                "checkout-api"
            },
        );
        stripe
            .apply(record, u64::from(trace_byte))
            .expect("span indexes");
    }
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        exact_resource_attributes: Arc::new(vec![(
            Arc::from("service.name"),
            Arc::from("checkout-api"),
        )]),
        limit: 3,
        ..TraceQuery::default()
    };
    let expected = vec![
        TraceId::from_bytes([1; 16]).unwrap(),
        TraceId::from_bytes([2; 16]).unwrap(),
        TraceId::from_bytes([3; 16]).unwrap(),
    ];
    assert_eq!(
        stripe
            .query(&query)
            .unwrap()
            .into_iter()
            .map(|span| span.trace_id)
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        stripe
            .query_projected(&query)
            .unwrap()
            .into_iter()
            .map(|span| span.trace_id)
            .collect::<Vec<_>>(),
        expected
    );
    stripe
        .seal_idle(DEFAULT_TRACE_IDLE_NANOS + 100)
        .expect("idle traces seal");
    assert_eq!(
        stripe
            .query(&query)
            .unwrap()
            .into_iter()
            .map(|span| span.trace_id)
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        stripe
            .query_projected(&query)
            .unwrap()
            .into_iter()
            .map(|span| span.trace_id)
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn projected_resource_index_keeps_span_predicates() {
    let with_service = |offset, trace_byte, service: &'static str| {
        let mut record = span(offset, trace_byte, trace_byte);
        record.resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![TelemetryAttribute::new(
                "service.name",
                crate::TelemetryValue::String(Arc::from(service)),
            )]),
            ..ResourceContext::default()
        });
        record
    };
    let expected = with_service(1, 1, "checkout-api");
    let other_name = with_service(2, 2, "checkout-api");
    let other_service = with_service(3, 3, "inventory-api");
    let mut stripe = TraceStripe::new(4 * 1024 * 1024).unwrap();
    for record in [expected.clone(), other_name, other_service] {
        stripe.apply(record, 1).unwrap();
    }
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        name: Some(Arc::clone(&expected.name)),
        exact_attributes: Arc::new(vec![(Arc::from("http.status_code"), Arc::from("200"))]),
        exact_resource_attributes: Arc::new(vec![(
            Arc::from("service.name"),
            Arc::from("checkout-api"),
        )]),
        start_time_unix_nanos: Some(expected.start_time_unix_nanos),
        end_time_unix_nanos: Some(expected.start_time_unix_nanos + 1),
        limit: 10,
        ..TraceQuery::default()
    };
    let projected = stripe.query_projected(&query).unwrap();
    let expected_projection = TraceProjection::from_span(&expected);
    assert_eq!(projected, vec![expected_projection]);
}

#[test]
fn resource_attribute_postings_intersect_without_bypassing_exact_matching() {
    let with_resource = |offset, trace_byte, service: &'static str, environment: &'static str| {
        let mut record = span(offset, trace_byte, trace_byte);
        record.resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![
                TelemetryAttribute::new(
                    "service.name",
                    crate::TelemetryValue::String(Arc::from(service)),
                ),
                TelemetryAttribute::new(
                    "deployment.environment",
                    crate::TelemetryValue::String(Arc::from(environment)),
                ),
            ]),
            ..ResourceContext::default()
        });
        record
    };
    let expected = with_resource(1, 1, "checkout-api", "production");
    let wrong_environment = with_resource(2, 2, "checkout-api", "staging");
    let wrong_service = with_resource(3, 3, "inventory-api", "production");
    let mut stripe = TraceStripe::new(4 * 1024 * 1024).unwrap();
    for record in [expected.clone(), wrong_environment, wrong_service] {
        stripe.apply(record, 1).unwrap();
    }
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        exact_resource_attributes: Arc::new(vec![
            (Arc::from("service.name"), Arc::from("checkout-api")),
            (Arc::from("deployment.environment"), Arc::from("production")),
        ]),
        limit: 10,
        ..TraceQuery::default()
    };
    assert_eq!(stripe.query(&query).unwrap(), vec![expected.clone()]);
    assert_eq!(
        stripe.query_projected(&query).unwrap(),
        vec![TraceProjection::from_span(&expected)]
    );
}

#[test]
fn selective_trace_decode_materializes_only_matching_sidecars() {
    let with_service = |offset, trace_byte, span_byte, service: &'static str| {
        let mut record = span(offset, trace_byte, span_byte);
        record.resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![TelemetryAttribute::new(
                "service.name",
                crate::TelemetryValue::String(Arc::from(service)),
            )]),
            ..ResourceContext::default()
        });
        record
    };
    let expected = with_service(1, 1, 1, "checkout-api");
    let other = with_service(2, 2, 2, "inventory-api");
    let encoded = encode_trace_block(&[other, expected.clone()]).unwrap();
    let decoded = decode_trace_block_matching(
        &encoded,
        &TraceQuery {
            tenant: Arc::from("tenant-a"),
            exact_resource_attributes: Arc::new(vec![(
                Arc::from("service.name"),
                Arc::from("checkout-api"),
            )]),
            limit: 10,
            ..TraceQuery::default()
        },
    )
    .unwrap();
    assert_eq!(decoded, vec![expected]);
}

#[test]
fn grouped_trace_and_parent_references_compact_common_topologies() {
    let mut records = (1..=8)
        .map(|ordinal| {
            let mut record = span(ordinal, 1, ordinal as u8);
            record.span_id = SpanId::from_bytes(ordinal.to_be_bytes()).unwrap();
            record
        })
        .collect::<Vec<_>>();
    for ordinal in 1..records.len() {
        records[ordinal].parent_span_id = Some(records[ordinal - 1].span_id);
    }
    let mut star = (9..=16)
        .map(|ordinal| {
            let mut record = span(ordinal, 2, ordinal as u8);
            record.span_id = SpanId::from_bytes(ordinal.to_be_bytes()).unwrap();
            record
        })
        .collect::<Vec<_>>();
    let root = star[0].span_id;
    for record in &mut star[1..] {
        record.parent_span_id = Some(root);
    }
    records.extend(star);
    let refs = records.iter().collect::<Vec<_>>();
    let encoded = encode_span_ids(&refs).unwrap();
    let decoded = decode_span_ids(&encoded, records.len()).unwrap();
    let expected = records
        .iter()
        .map(|record| (record.trace_id, record.span_id, record.parent_span_id))
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
    assert!(encoded.len() < records.len() * 6);
}

#[test]
fn ready_traces_share_a_bounded_columnar_block() {
    let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
    stripe.apply(span(1, 1, 1), 10).unwrap();
    stripe.apply(span(2, 2, 2), 10).unwrap();
    let blocks = stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(decode_trace_block(&blocks[0]).unwrap().len(), 2);
    assert_eq!(stripe.pending_blocks.len(), 1);
    assert_eq!(
        stripe.pending_blocks[0].min_signal_identity,
        u128::from_be_bytes([1; 16])
    );
    assert_eq!(
        stripe.pending_blocks[0].max_signal_identity,
        u128::from_be_bytes([2; 16])
    );
    for trace_byte in [1, 2] {
        let summaries = stripe.directory().query(&TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(TraceId::from_bytes([trace_byte; 16]).unwrap()),
            limit: 1,
            ..TraceQuery::default()
        });
        assert_eq!(summaries[0].block_fragments.as_ref(), &[1]);
    }
}

#[test]
fn trace_sidecar_interner_promotes_without_losing_high_cardinality_values() {
    let records = (1..=40)
        .map(|ordinal| {
            let mut record = span(ordinal, ordinal as u8, ordinal as u8);
            record.name = Arc::from(format!("operation-{ordinal}"));
            record.attributes = Arc::new(vec![TelemetryAttribute::new(
                "request.id",
                crate::TelemetryValue::String(Arc::from(format!("request-{ordinal}"))),
            )]);
            record
        })
        .collect::<Vec<_>>();
    let encoded = encode_trace_block(&records).unwrap();
    let decoded = decode_trace_block(&encoded).unwrap();
    assert_eq!(decoded, records);
}

#[test]
fn duplicate_and_conflicting_spans_follow_durable_offset() {
    let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
    let original = span(1, 1, 1);
    assert_eq!(
        stripe.apply(original.clone(), 0).unwrap(),
        TraceApplyOutcome::Inserted
    );
    assert_eq!(
        stripe.apply(original.clone(), 1).unwrap(),
        TraceApplyOutcome::Duplicate
    );
    let mut newer = original.clone();
    newer.record_ref.offset = LogicalOffset::new(2);
    newer.name = Arc::from("changed");
    assert_eq!(stripe.apply(newer, 2).unwrap(), TraceApplyOutcome::Replaced);
    let mut older = original;
    older.name = Arc::from("obsolete");
    assert_eq!(stripe.apply(older, 3).unwrap(), TraceApplyOutcome::Obsolete);
}

#[test]
fn retries_and_conflicts_remain_deterministic_after_sealing() {
    let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
    let original = span(1, 1, 1);
    stripe.apply(original.clone(), 10).unwrap();
    stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();

    let mut retry = original.clone();
    retry.record_ref.offset = LogicalOffset::new(2);
    assert_eq!(
        stripe
            .apply(retry, 10 + DEFAULT_TRACE_IDLE_NANOS + 1)
            .unwrap(),
        TraceApplyOutcome::Duplicate
    );

    let mut replacement = original.clone();
    replacement.record_ref.offset = LogicalOffset::new(3);
    replacement.name = Arc::from("changed");
    let replacement_time = 10 + DEFAULT_TRACE_IDLE_NANOS + 2;
    assert_eq!(
        stripe.apply(replacement, replacement_time).unwrap(),
        TraceApplyOutcome::Replaced
    );
    stripe
        .seal_idle(replacement_time + DEFAULT_TRACE_IDLE_NANOS)
        .unwrap();

    let mut obsolete = original;
    obsolete.record_ref.offset = LogicalOffset::new(2);
    obsolete.name = Arc::from("obsolete");
    assert_eq!(
        stripe
            .apply(obsolete, replacement_time + DEFAULT_TRACE_IDLE_NANOS + 1)
            .unwrap(),
        TraceApplyOutcome::Obsolete
    );

    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
        limit: 10,
        ..TraceQuery::default()
    };
    let summary = stripe.directory().query(&query);
    assert_eq!(summary[0].span_count, 1);
    assert_eq!(summary[0].block_fragments.as_ref(), &[1, 2]);
    let spans = stripe.query(&query).unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name.as_ref(), "changed");

    let paged = stripe
        .query(&TraceQuery {
            partition: Some(spans[0].record_ref.topic_partition),
            start_offset: Some(LogicalOffset::new(3)),
            ..query.clone()
        })
        .unwrap();
    assert_eq!(paged.len(), 1);
    assert_eq!(paged[0].record_ref.offset, LogicalOffset::new(3));
    assert!(
        stripe
            .query(&TraceQuery {
                partition: Some(spans[0].record_ref.topic_partition),
                start_offset: Some(LogicalOffset::new(4)),
                ..query
            })
            .unwrap()
            .is_empty()
    );
}

#[test]
fn idle_trace_seals_and_becomes_directly_queryable() {
    let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
    stripe.apply(span(1, 1, 1), 10).unwrap();
    let blocks = stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();
    assert_eq!(blocks.len(), 1);
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
        limit: 10,
        ..TraceQuery::default()
    };
    let summaries = stripe.directory().query(&query);
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].span_count, 1);
    assert_eq!(stripe.query(&query).unwrap().len(), 1);
}

#[test]
fn late_fragment_extends_the_trace_directory() {
    let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
    stripe.apply(span(1, 1, 1), 10).unwrap();
    stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();

    let late_append = 10 + DEFAULT_TRACE_IDLE_NANOS + 1;
    stripe.apply(span(2, 1, 2), late_append).unwrap();
    stripe
        .seal_idle(late_append + DEFAULT_TRACE_IDLE_NANOS)
        .unwrap();

    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
        limit: 10,
        ..TraceQuery::default()
    };
    let summaries = stripe.directory().query(&query);
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].span_count, 2);
    assert_eq!(summaries[0].block_fragments.as_ref(), &[1, 2]);
    assert_eq!(stripe.query(&query).unwrap().len(), 2);
}

#[test]
fn late_retry_state_stays_within_the_trace_head_budget() {
    let one_span_bytes = span(1, 1, 1).estimated_head_bytes();
    let budget = one_span_bytes.saturating_mul(2);
    let mut stripe = TraceStripe::new(budget).unwrap();
    let mut now = 1u64;
    for trace_byte in 1..=16 {
        stripe
            .apply(span(u64::from(trace_byte), trace_byte, 1), now)
            .unwrap();
        now = now.saturating_add(DEFAULT_TRACE_IDLE_NANOS);
        stripe.seal_idle(now).unwrap();
        now = now.saturating_add(1);
        assert!(stripe.head_bytes() <= budget);
    }
    assert!(stripe.recently_sealed.len() <= 2);
}
