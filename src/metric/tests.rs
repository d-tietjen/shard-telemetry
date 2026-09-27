use super::*;
use crate::{METRICS_TOPIC_ID, TelemetryValue};

fn point(offset: u64, timestamp: u64, value: NumberValue) -> DurableMetricPoint {
    let identity = Arc::new(MetricIdentity {
        tenant: Arc::from("tenant-a"),
        resource: Arc::new(ResourceContext::default()),
        scope: Arc::new(ScopeContext::default()),
        name: Arc::from("http.server.duration"),
        unit: Arc::from("ms"),
        kind: MetricKind::Gauge,
        point_attributes: Arc::new(vec![TelemetryAttribute::new(
            "service",
            TelemetryValue::String(Arc::from("api")),
        )]),
    });
    DurableMetricPoint {
        stream_shard_id: ShardId::new(2),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Metrics,
            TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(11)),
            LogicalOffset::new(offset),
        ),
        identity,
        description: Arc::from("request latency"),
        metadata: Arc::new(Vec::new()),
        start_time_unix_nanos: 0,
        timestamp_unix_nanos: timestamp,
        flags: 0,
        value: MetricValue::Gauge(value),
        exemplars: Arc::new(Vec::new()),
    }
}

#[test]
fn series_fingerprint_is_attribute_order_independent() {
    let first = point(1, 1, NumberValue::Integer(1));
    let mut second = first.clone();
    let mut attributes = vec![
        TelemetryAttribute::new("z", TelemetryValue::Integer(1)),
        TelemetryAttribute::new("a", TelemetryValue::Integer(2)),
    ];
    second.identity = Arc::new(MetricIdentity {
        point_attributes: Arc::new(attributes.clone()),
        ..first.identity.as_ref().clone()
    });
    attributes.reverse();
    let third = MetricIdentity {
        point_attributes: Arc::new(attributes),
        ..second.identity.as_ref().clone()
    };
    assert_eq!(second.series_fingerprint(), third.fingerprint());
}

#[test]
fn metric_chunk_round_trips_nan_payloads_and_out_of_order_input() {
    let points = vec![
        point(2, 200, NumberValue::DoubleBits(0x7ff8_0000_0000_0042)),
        point(1, 100, NumberValue::DoubleBits((-0.0f64).to_bits())),
    ];
    let encoded = encode_metric_chunk(&points).unwrap();
    let decoded = decode_metric_chunk(&encoded).unwrap();
    assert_eq!(decoded[0], points[1]);
    assert_eq!(decoded[1], points[0]);
}

#[test]
fn metric_sidecar_interner_promotes_and_gorilla_lane_stays_bit_exact() {
    let points = (1..=40)
        .map(|ordinal| {
            let mut point = point(
                ordinal,
                ordinal * 100,
                NumberValue::DoubleBits(f64::from_bits(0x3ff0_0000_0000_0000 + ordinal).to_bits()),
            );
            point.description = Arc::from(format!("description-{ordinal}"));
            point.metadata = Arc::new(vec![TelemetryAttribute::new(
                "metadata.id",
                TelemetryValue::Integer(ordinal as i64),
            )]);
            point
        })
        .collect::<Vec<_>>();
    let encoded = encode_metric_chunk(&points).unwrap();
    let decoded = decode_metric_chunk(&encoded).unwrap();
    assert_eq!(decoded, points);

    let bits = points
        .iter()
        .map(|point| match point.value {
            MetricValue::Gauge(NumberValue::DoubleBits(bits)) => bits,
            _ => unreachable!("test points are doubles"),
        })
        .collect::<Vec<_>>();
    let compressed = encode_double_value_lane(2, &bits).unwrap();
    assert_eq!(
        decode_double_value_lane(&compressed[1..], bits.len()).unwrap(),
        bits
    );
    assert!(compressed.len() < 8 * bits.len());
}

#[test]
fn homogeneous_integer_and_histogram_value_lanes_round_trip() {
    let integers = vec![
        point(1, 100, NumberValue::Integer(i64::MIN)),
        point(2, 200, NumberValue::Integer(i64::MAX)),
    ];
    assert_eq!(
        decode_metric_chunk(&encode_metric_chunk(&integers).unwrap()).unwrap(),
        integers
    );

    let histogram = ExplicitHistogramValue {
        count: HistogramCount::Integer(3),
        sum_bits: Some(6.0f64.to_bits()),
        bucket_counts: Arc::new(vec![HistogramCount::Integer(1), HistogramCount::Integer(2)]),
        explicit_bounds_bits: Arc::new(vec![1.0f64.to_bits()]),
        min_bits: Some(1.0f64.to_bits()),
        max_bits: Some(3.0f64.to_bits()),
        reset_hint: 0,
    };
    let mut histograms = integers;
    for point in &mut histograms {
        point.identity = Arc::new(MetricIdentity {
            kind: MetricKind::ExplicitHistogram { temporality: 2 },
            ..point.identity.as_ref().clone()
        });
        point.value = MetricValue::ExplicitHistogram(histogram.clone());
    }
    assert_eq!(
        decode_metric_chunk(&encode_metric_chunk(&histograms).unwrap()).unwrap(),
        histograms
    );
}

#[test]
fn remote_write_rejects_conflicts_while_otlp_uses_offset() {
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    let first = point(1, 100, NumberValue::Integer(1));
    stripe
        .apply(first.clone(), MetricIngestProtocol::RemoteWrite)
        .unwrap();
    let conflicting = point(2, 100, NumberValue::Integer(2));
    assert!(matches!(
        stripe.apply(conflicting.clone(), MetricIngestProtocol::RemoteWrite),
        Err(TelemetryError::MetricSampleConflict { .. })
    ));
    assert_eq!(
        stripe
            .apply(conflicting, MetricIngestProtocol::Otlp)
            .unwrap(),
        MetricApplyOutcome::Replaced
    );
}

#[test]
fn sealed_metric_conflicts_preserve_remote_write_and_offset_semantics() {
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    stripe.chunk_points = 1;
    let first = point(1, 100, NumberValue::Integer(1));
    assert_eq!(
        stripe
            .apply(first.clone(), MetricIngestProtocol::RemoteWrite)
            .unwrap(),
        MetricApplyOutcome::Inserted
    );
    assert!(stripe.series[&first.series_fingerprint()].points.is_empty());

    let mut duplicate = first.clone();
    duplicate.record_ref.offset = LogicalOffset::new(2);
    assert_eq!(
        stripe
            .apply(duplicate, MetricIngestProtocol::RemoteWrite)
            .unwrap(),
        MetricApplyOutcome::Duplicate
    );

    let conflict = point(2, 100, NumberValue::Integer(2));
    assert!(matches!(
        stripe.apply(conflict.clone(), MetricIngestProtocol::RemoteWrite),
        Err(TelemetryError::MetricSampleConflict { .. })
    ));
    let mut obsolete = conflict.clone();
    obsolete.record_ref.offset = LogicalOffset::new(0);
    assert_eq!(
        stripe.apply(obsolete, MetricIngestProtocol::Otlp).unwrap(),
        MetricApplyOutcome::Obsolete
    );
    assert_eq!(
        stripe
            .apply(conflict.clone(), MetricIngestProtocol::Otlp)
            .unwrap(),
        MetricApplyOutcome::Replaced
    );

    let queried = stripe
        .query(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            series: Some(conflict.series_fingerprint()),
            limit: usize::MAX,
            ..MetricQuery::default()
        })
        .unwrap();
    assert_eq!(queried, vec![conflict]);
}

#[test]
fn exact_series_query_limits_early_and_keeps_late_conflict_winners() {
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    stripe.chunk_points = 2;
    let first = point(1, 100, NumberValue::Integer(1));
    let series = first.series_fingerprint();
    for value in [
        first,
        point(2, 200, NumberValue::Integer(2)),
        point(3, 300, NumberValue::Integer(3)),
        point(4, 400, NumberValue::Integer(4)),
    ] {
        stripe.apply(value, MetricIngestProtocol::Otlp).unwrap();
    }
    let conflict = point(5, 100, NumberValue::Integer(9));
    assert_eq!(
        stripe
            .apply(conflict.clone(), MetricIngestProtocol::Otlp)
            .unwrap(),
        MetricApplyOutcome::Replaced
    );

    let first_result = stripe
        .query(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            series: Some(series),
            limit: 1,
            ..MetricQuery::default()
        })
        .unwrap();
    assert_eq!(first_result, vec![conflict]);

    let ranged = stripe
        .query(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            series: Some(series),
            start_time_unix_nanos: Some(201),
            limit: 1,
            ..MetricQuery::default()
        })
        .unwrap();
    assert_eq!(ranged[0].timestamp_unix_nanos, 300);
    let cache = stripe.decoded_chunks.borrow();
    assert!(cache.hits > 0);
    assert!(cache.misses > 0);
    assert!(cache.used_bytes <= cache.max_bytes);
}

#[test]
fn decoded_metric_cache_retains_a_series_working_set() {
    let mut stripe = MetricStripe::new(64 * 1024 * 1024).unwrap();
    stripe.chunk_points = 1;
    let first = point(1, 100, NumberValue::Integer(1));
    let series = first.series_fingerprint();
    stripe.apply(first, MetricIngestProtocol::Otlp).unwrap();
    for offset in 2..=257 {
        stripe
            .apply(
                point(offset, 100 + offset, NumberValue::Integer(offset as i64)),
                MetricIngestProtocol::Otlp,
            )
            .unwrap();
    }

    let query = MetricQuery {
        tenant: Arc::from("tenant-a"),
        series: Some(series),
        limit: usize::MAX,
        ..MetricQuery::default()
    };
    assert_eq!(stripe.query(&query).unwrap().len(), 257);
    let warmed_misses = stripe.decoded_chunks.borrow().misses;
    assert_eq!(warmed_misses, 257);

    assert_eq!(stripe.query(&query).unwrap().len(), 257);
    let cache = stripe.decoded_chunks.borrow();
    assert_eq!(cache.misses, warmed_misses);
    assert!(cache.hits >= 257);
}

#[test]
fn exact_timestamp_query_skips_unrequested_points_and_keeps_winners() {
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    stripe.chunk_points = 2;
    let first = point(1, 100, NumberValue::Integer(1));
    let series = first.series_fingerprint();
    for value in [
        first.clone(),
        point(2, 200, NumberValue::Integer(2)),
        point(3, 300, NumberValue::Integer(3)),
        point(4, 400, NumberValue::Integer(4)),
    ] {
        stripe.apply(value, MetricIngestProtocol::Otlp).unwrap();
    }
    let replacement = point(5, 300, NumberValue::Integer(9));
    assert_eq!(
        stripe
            .apply(replacement.clone(), MetricIngestProtocol::Otlp)
            .unwrap(),
        MetricApplyOutcome::Replaced
    );

    let queried = stripe
        .query_exact_timestamps(
            &MetricQuery {
                tenant: Arc::from("tenant-a"),
                partition: Some(first.record_ref.topic_partition),
                series: Some(series),
                ..MetricQuery::default()
            },
            &[100, 300],
        )
        .unwrap();
    assert_eq!(
        queried,
        vec![first, replacement],
        "the exact probe must omit timestamp 200 and 400"
    );
}

#[test]
fn exact_series_offset_cursor_orders_pages_after_conflict_resolution() {
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    stripe.chunk_points = 2;
    let first = point(1, 300, NumberValue::Integer(1));
    let series = first.series_fingerprint();
    for value in [
        first,
        point(2, 100, NumberValue::Integer(2)),
        point(3, 200, NumberValue::Integer(3)),
    ] {
        stripe.apply(value, MetricIngestProtocol::Otlp).unwrap();
    }
    let replacement = point(4, 300, NumberValue::Integer(9));
    assert_eq!(
        stripe
            .apply(replacement.clone(), MetricIngestProtocol::Otlp)
            .unwrap(),
        MetricApplyOutcome::Replaced
    );

    let partition = replacement.record_ref.topic_partition;
    let first_page = stripe
        .query(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            partition: Some(partition),
            series: Some(series),
            limit: 2,
            ..MetricQuery::default()
        })
        .unwrap();
    assert_eq!(
        first_page
            .iter()
            .map(|point| point.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let second_page = stripe
        .query(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            partition: Some(partition),
            start_offset: Some(LogicalOffset::new(4)),
            series: Some(series),
            limit: 2,
            ..MetricQuery::default()
        })
        .unwrap();
    assert_eq!(second_page, vec![replacement]);
}

#[test]
fn timestamp_delta_of_delta_round_trips_wide_changes() {
    let timestamps = [1, 2, 1_000_000_000_000, 1_000_000_000_001];
    let encoded = encode_timestamp_delta_of_delta(&timestamps).unwrap();
    assert_eq!(
        decode_timestamp_delta_of_delta(&encoded, timestamps.len()).unwrap(),
        timestamps
    );

    let periodic = (0..4_096)
        .map(|ordinal| 1_000_000_000 + ordinal * 15_000_000_000)
        .collect::<Vec<_>>();
    let encoded = encode_timestamp_delta_of_delta(&periodic).unwrap();
    assert!(
        encoded.len() <= 20,
        "periodic timestamps use one bounded run"
    );
    assert_eq!(
        decode_timestamp_delta_of_delta(&encoded, periodic.len()).unwrap(),
        periodic
    );
}

#[test]
fn delta_accumulator_restarts_before_accepting_the_next_point() {
    let mut first = point(1, 100, NumberValue::Integer(5));
    first.identity = Arc::new(MetricIdentity {
        kind: MetricKind::Sum {
            temporality: 1,
            monotonic: true,
        },
        ..first.identity.as_ref().clone()
    });
    first.value = MetricValue::Sum(NumberValue::Integer(5));
    let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
    stripe
        .apply(first.clone(), MetricIngestProtocol::Otlp)
        .unwrap();
    let checkpoint = stripe.accumulator_checkpoints().unwrap();

    let mut recovered = MetricStripe::new(1024 * 1024).unwrap();
    recovered
        .restore_accumulator_checkpoints(&checkpoint)
        .unwrap();
    let mut second = first;
    second.record_ref.offset = LogicalOffset::new(2);
    second.timestamp_unix_nanos = 200;
    second.value = MetricValue::Sum(NumberValue::Integer(7));
    recovered.apply(second, MetricIngestProtocol::Otlp).unwrap();
    let checkpoints: Vec<SeriesAccumulatorCheckpoint> =
        rmp_serde::from_slice(&recovered.accumulator_checkpoints().unwrap()).unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].cumulative, Some(NumberValue::Integer(12)));
    assert_eq!(checkpoints[0].reset_generation, 0);
}
