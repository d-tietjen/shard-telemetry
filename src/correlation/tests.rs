use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use super::*;
use crate::{
    CompressionCohortId, DurableLog, LOGS_TOPIC_ID, ResourceContext, ScopeContext, TelemetryValue,
};

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
