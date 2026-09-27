use super::*;

#[test]
fn floating_point_values_preserve_every_bit() {
    let bits = 0x7ff8_0000_0000_0042;
    let value = TelemetryValue::from_f64(f64::from_bits(bits));
    assert_eq!(value.as_f64().expect("double").to_bits(), bits);
    assert_eq!(value, TelemetryValue::DoubleBits(bits));
}

#[test]
fn telemetry_ids_use_fixed_width_lowercase_hex() {
    let trace = TraceId::from_bytes([
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ])
    .unwrap();
    let span = SpanId::from_bytes([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]).unwrap();
    assert_eq!(trace.to_string(), "0123456789abcdeffedcba9876543210");
    assert_eq!(span.to_string(), "0123456789abcdef");
}

#[test]
fn signal_topics_and_routes_are_distinct_and_stable() {
    let router = TelemetryRouter::new(NonZeroU16::new(256).unwrap());
    let trace_id = TraceId::from_bytes([7; 16]).unwrap();
    let series = SeriesFingerprint::from_canonical(b"series");
    let trace = router.trace("tenant", trace_id);
    let metric = router.metric("tenant", series);
    let log = router.log("tenant", Some(trace_id), b"ignored");
    assert_eq!(trace.topic_id, TRACES_TOPIC_ID);
    assert_eq!(metric.topic_id, METRICS_TOPIC_ID);
    assert_eq!(log.topic_id, LOGS_TOPIC_ID);
    assert_eq!(
        trace.partition_id,
        router.trace("tenant", trace_id).partition_id
    );
    assert_ne!(trace.topic_id, metric.topic_id);
}

#[test]
fn signal_router_honors_independent_partition_counts() {
    let mut config = ShardTelemetryConfig::default();
    config.logs.logical_partitions = NonZeroU16::new(3).unwrap();
    config.traces.logical_partitions = NonZeroU16::new(1).unwrap();
    config.metrics.logical_partitions = NonZeroU16::new(2).unwrap();
    config.logs.physical_stripes = NonZeroU16::new(1).unwrap();
    config.traces.physical_stripes = NonZeroU16::new(1).unwrap();
    config.metrics.physical_stripes = NonZeroU16::new(1).unwrap();
    let router = TelemetryRouter::from_config(&config);
    let trace_id = TraceId::from_bytes([9; 16]).unwrap();
    let series = SeriesFingerprint::from_canonical(b"independent-series");
    assert!(router.log("tenant", None, b"stream").partition_id.get() < 3);
    assert_eq!(router.trace("tenant", trace_id).partition_id.get(), 0);
    assert!(router.metric("tenant", series).partition_id.get() < 2);
}

#[test]
fn production_defaults_are_bounded() {
    let config = ShardTelemetryConfig::default();
    config.validate().unwrap();
    assert_eq!(config.logs.logical_partitions.get(), 256);
    assert_eq!(
        config.traces.head_memory_bytes_per_stripe,
        256 * 1024 * 1024
    );
    assert_eq!(
        config.metrics.head_memory_bytes_per_stripe,
        512 * 1024 * 1024
    );
    assert_eq!(config.max_otlp_request_bytes, 64 * 1024 * 1024);
}

#[test]
fn context_and_attribute_identities_are_exact_and_order_independent() {
    let service = TelemetryAttribute::new(
        "service.name",
        TelemetryValue::String(Arc::from("checkout")),
    );
    let region = TelemetryAttribute::new(
        "cloud.region",
        TelemetryValue::String(Arc::from("us-east-1")),
    );
    let left = ResourceContext {
        attributes: Arc::new(vec![service.clone(), region.clone()]),
        ..ResourceContext::default()
    };
    let right = ResourceContext {
        attributes: Arc::new(vec![region, service.clone()]),
        ..ResourceContext::default()
    };
    assert_eq!(left.id(), right.id());
    assert_eq!(left.id().to_string().len(), 32);
    assert_eq!(service.fingerprint(), service.clone().fingerprint());

    let mut changed = right;
    changed.dropped_attributes_count = 1;
    assert_ne!(left.id(), changed.id());
    assert_ne!(
        service.fingerprint(),
        TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("payments")),
        )
        .fingerprint()
    );
}
