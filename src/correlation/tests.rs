use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use super::*;
use crate::{
    CompressionCohortId, DurableLog, LOGS_TOPIC_ID, ResourceContext, ScopeContext, TelemetryValue,
};

#[test]
fn disabled_navigation_retains_no_allocated_postings_or_identity_caches() {
    let mut index = CorrelationIndex::new(CorrelationConfig::disabled());
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let log = DurableLog::new(
        ShardId::new(0),
        partition,
        LogicalOffset::new(0),
        42,
        "body",
        CompressionCohortId::new(1),
    );
    for tenant in ["tenant-a", "tenant-b"] {
        index.index_log(tenant, &log);
    }
    assert!(index.is_disabled());
    assert_eq!(index.stats(), CorrelationStats::default());
    assert_eq!(index.tenants.capacity(), 0);
    assert_eq!(index.postings.capacity(), 0);
    assert_eq!(index.resource_pointer_ids.capacity(), 0);
    assert_eq!(index.resource_ids.capacity(), 0);
    assert_eq!(index.scope_pointer_ids.capacity(), 0);
    assert_eq!(index.scope_ids.capacity(), 0);
    assert_eq!(index.attribute_pointer_ids.capacity(), 0);
    assert_eq!(index.attribute_ids.capacity(), 0);
    assert!(
        index
            .query(&CorrelationQuery::new("tenant-a").with_resource_id(log.resource_id()))
            .is_empty()
    );
}

#[test]
fn shared_typed_metadata_connects_signals_without_retaining_values() {
    let service = TelemetryAttribute::new(
        "service.name",
        TelemetryValue::String(Arc::from("checkout")),
    );
    let resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![service.clone()]),
        ..ResourceContext::default()
    });
    let scope = Arc::new(ScopeContext::default());
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
    let mut log = DurableLog::new(
        ShardId::new(0),
        partition,
        LogicalOffset::new(7),
        42,
        "request complete",
        CompressionCohortId::new(1),
    );
    log.resource = resource;
    log.scope = scope;
    let mut index = CorrelationIndex::new(CorrelationConfig::default());
    index.index_log("tenant-a", &log);

    let refs = index.query(
        &CorrelationQuery::new("tenant-a")
            .with_resource_id(log.resource_id())
            .with_attribute(&service),
    );
    assert_eq!(refs, vec![log.record_ref]);
    let query = CorrelationQuery::new("tenant-a")
        .with_resource_id(log.resource_id())
        .with_attribute(&service);
    let mut reusable = Vec::with_capacity(8);
    index.query_into(&query, &mut reusable);
    let capacity = reusable.capacity();
    index.query_into(&query, &mut reusable);
    assert_eq!(reusable, refs);
    assert_eq!(reusable.capacity(), capacity);
    assert_eq!(index.stats().refs, 3);
}

#[test]
fn bounds_drop_only_optional_postings() {
    let mut index = CorrelationIndex::new(CorrelationConfig {
        max_keys: 1,
        max_refs_per_key: 1,
        max_total_refs: 1,
    });
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
    for offset in 0..2 {
        let log = DurableLog::new(
            ShardId::new(0),
            partition,
            LogicalOffset::new(offset),
            offset,
            "message",
            CompressionCohortId::new(1),
        );
        index.index_log("tenant-a", &log);
    }
    assert_eq!(index.stats().refs, 1);
    assert!(index.stats().dropped_postings > 0);
}

#[test]
fn bounded_intersection_preserves_order_filters_and_pagination() {
    let first = TelemetryAttribute::new("shared", TelemetryValue::String(Arc::from("yes")));
    let second = TelemetryAttribute::new("region", TelemetryValue::String(Arc::from("east")));
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
    let mut index = CorrelationIndex::new(CorrelationConfig {
        max_keys: 2,
        max_refs_per_key: 20_000,
        max_total_refs: 40_000,
    });
    let tenant_id = index.tenant_id("tenant-a").expect("tenant is admitted");
    for offset in 0..10_000 {
        let record = TelemetryRecordRef::for_signal(
            TelemetrySignal::Logs,
            partition,
            LogicalOffset::new(offset),
        );
        index.insert(
            tenant_id,
            CorrelationKey::Attribute(first.fingerprint()),
            record,
            offset,
        );
        if offset % 2 == 0 {
            index.insert(
                tenant_id,
                CorrelationKey::Attribute(second.fingerprint()),
                record,
                offset,
            );
        }
    }
    let after =
        TelemetryRecordRef::for_signal(TelemetrySignal::Logs, partition, LogicalOffset::new(4_990));
    let result = index.query(
        &CorrelationQuery::new("tenant-a")
            .with_attribute(&first)
            .with_attribute(&second)
            .for_signal(TelemetrySignal::Logs)
            .after(after)
            .with_limit(3),
    );
    assert_eq!(
        result
            .iter()
            .map(|record| record.offset.get())
            .collect::<Vec<_>>(),
        vec![4_992, 4_994, 4_996]
    );
}

#[test]
fn time_bounds_and_retention_remove_expired_navigation_postings() {
    let resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout")),
        )]),
        ..ResourceContext::default()
    });
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
    let mut index = CorrelationIndex::new(CorrelationConfig::default());
    for (offset, timestamp) in [(0, 10), (1, 20)] {
        let mut log = DurableLog::new(
            ShardId::new(0),
            partition,
            LogicalOffset::new(offset),
            timestamp,
            "message",
            CompressionCohortId::new(1),
        );
        log.resource = Arc::clone(&resource);
        index.index_log("tenant-a", &log);
    }
    let query = CorrelationQuery {
        start_time_unix_nanos: Some(15),
        ..CorrelationQuery::new("tenant-a").with_resource_id(resource.id())
    };
    assert_eq!(
        index
            .query(&query)
            .into_iter()
            .map(|record| record.offset.get())
            .collect::<Vec<_>>(),
        vec![1]
    );

    index.retain_since_timestamp(15);
    assert_eq!(index.query(&query).len(), 1);
    assert!(index.stats().refs > 0);
    let expired = CorrelationQuery {
        end_time_unix_nanos: Some(14),
        ..CorrelationQuery::new("tenant-a").with_resource_id(resource.id())
    };
    assert!(index.query(&expired).is_empty());
}

#[test]
fn span_retention_uses_end_time_before_limits_and_preserves_event_time_filters() {
    let trace_id = TraceId::from_bytes([1; 16]).expect("trace ID");
    let attribute = TelemetryAttribute::new("shared", TelemetryValue::String(Arc::from("scope")));
    let resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![attribute.clone()]),
        ..Default::default()
    });
    let scope = Arc::new(ScopeContext::default());
    let partition = TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(0));
    let make_span = |offset, duration| DurableSpan {
        stream_shard_id: ShardId::new(0),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Traces,
            partition,
            LogicalOffset::new(offset),
        ),
        tenant: Arc::from("tenant-a"),
        resource: Arc::clone(&resource),
        scope: Arc::clone(&scope),
        trace_id,
        span_id: crate::SpanId::from_bytes([1; 8]).expect("span ID"),
        parent_span_id: None,
        trace_state: Arc::from(""),
        flags: 0,
        name: Arc::from("crossing cutoff"),
        kind: 1,
        start_time_unix_nanos: 10,
        duration_nanos: duration,
        attributes: Arc::new(vec![attribute.clone()]),
        dropped_attributes_count: 0,
        events: Arc::new(Vec::new()),
        dropped_events_count: 0,
        links: Arc::new(Vec::new()),
        dropped_links_count: 0,
        status: None,
    };
    let expired = make_span(0, 4);
    let crossing = make_span(1, 10);
    let mut index = CorrelationIndex::new(CorrelationConfig::default());
    index.index_span(&expired);
    index.index_span(&crossing);
    let one_key = CorrelationQuery::new("tenant-a")
        .with_trace_id(trace_id)
        .with_limit(1);
    let two_keys = one_key.clone().with_resource_id(resource.id());
    let many_keys = two_keys
        .clone()
        .with_scope_id(scope.id())
        .with_attribute(&attribute);
    for query in [one_key, two_keys, many_keys] {
        assert_eq!(
            index.query(&query),
            vec![expired.record_ref],
            "public unscoped API retains its behavior"
        );
        let mut refs = Vec::new();
        index.query_into_with_retention(&query, [0, 15, 0], &mut refs);
        assert_eq!(
            refs,
            vec![crossing.record_ref],
            "expired lower offsets cannot consume the limit"
        );
        let before_cutoff = CorrelationQuery {
            end_time_unix_nanos: Some(14),
            ..query.clone()
        };
        index.query_into_with_retention(&before_cutoff, [0, 15, 0], &mut refs);
        assert_eq!(
            refs,
            vec![crossing.record_ref],
            "caller bounds still refer to span start"
        );
        let after_start = CorrelationQuery {
            start_time_unix_nanos: Some(11),
            ..query.clone()
        };
        index.query_into_with_retention(&after_start, [0, 15, 0], &mut refs);
        assert!(refs.is_empty());
        let other_tenant = CorrelationQuery {
            tenant: Arc::from("tenant-b"),
            ..query
        };
        index.query_into_with_retention(&other_tenant, [0, 15, 0], &mut refs);
        assert!(refs.is_empty());
    }
    index.retain_since_signal_timestamps([0, 15, 0]);
    let query = CorrelationQuery::new("tenant-a").with_trace_id(trace_id);
    assert_eq!(index.query(&query), vec![crossing.record_ref]);
    index.retain_since_signal_timestamps([0, 21, 0]);
    assert!(index.query(&query).is_empty());
}

#[test]
fn three_and_four_key_correlations_require_every_identity_before_pagination() {
    let trace_id = TraceId::from_bytes([1; 16]).expect("trace ID");
    let attribute = TelemetryAttribute::new("required", TelemetryValue::String(Arc::from("yes")));
    let missing = TelemetryAttribute::new("required", TelemetryValue::String(Arc::from("missing")));
    let resource = Arc::new(ResourceContext::default());
    let wanted_scope = Arc::new(ScopeContext {
        name: Arc::from("wanted"),
        ..ScopeContext::default()
    });
    let wrong_scope = Arc::new(ScopeContext {
        name: Arc::from("wrong"),
        ..ScopeContext::default()
    });
    let absent_scope = ScopeContext {
        name: Arc::from("absent"),
        ..ScopeContext::default()
    };
    let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let mut index = CorrelationIndex::new(CorrelationConfig::default());
    let mut records = Vec::new();
    for offset in 0..3 {
        let mut log = DurableLog::new(
            ShardId::new(0),
            partition,
            LogicalOffset::new(offset),
            10 + offset * 10,
            "three-key conjunction",
            CompressionCohortId::new(1),
        );
        log.trace_id = Some(trace_id);
        log.resource = Arc::clone(&resource);
        log.scope = Arc::clone(if offset == 0 {
            &wrong_scope
        } else {
            &wanted_scope
        });
        log.attributes = Arc::new(vec![attribute.clone()]);
        records.push(log.record_ref);
        index.index_log("tenant-a", &log);
    }
    let two = CorrelationQuery::new("tenant-a")
        .with_trace_id(trace_id)
        .with_resource_id(resource.id());
    assert_eq!(index.query(&two), records);
    let three = two.clone().with_scope_id(wanted_scope.id());
    let four = three.clone().with_attribute(&attribute);
    for query in [three, four] {
        assert_eq!(index.query(&query), records[1..]);
        assert_eq!(index.query(&query.clone().with_limit(1)), vec![records[1]]);
        assert_eq!(
            index.query(&query.clone().after(records[1]).with_limit(1)),
            vec![records[2]]
        );
        assert!(
            index
                .query(&query.clone().with_scope_id(absent_scope.id()))
                .is_empty()
        );
        let other_tenant = CorrelationQuery {
            tenant: Arc::from("tenant-b"),
            ..query.clone()
        };
        assert!(index.query(&other_tenant).is_empty());
        let mut retained = Vec::new();
        index.query_into_with_retention(&query, [25, 0, 0], &mut retained);
        assert_eq!(retained, vec![records[2]]);
    }
    assert!(
        index
            .query(&two.clone().with_attribute(&missing))
            .is_empty()
    );
    assert!(
        index
            .query(
                &two.with_scope_id(wanted_scope.id())
                    .with_attribute(&missing)
            )
            .is_empty()
    );
}
