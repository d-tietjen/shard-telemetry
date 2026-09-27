use super::*;

pub(super) fn benchmark_logs(
    corpus: &Corpus,
    iterations: usize,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = corpus
        .durable_logs
        .iter()
        .try_fold(0usize, |total, record| {
            canonical_log_bytes(record).map(|bytes| total + bytes)
        })?;
    let start = Instant::now();
    let mut stripe = LogStripe::new(ShardId::new(0), StripeConfig::default())?;
    for record in &corpus.durable_logs {
        stripe.apply_durable(record.clone())?;
    }
    stripe.seal_active_blocks()?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = stripe
        .catalog()
        .iter()
        .map(|block| block.stored_bytes as usize)
        .sum();
    let start = Instant::now();
    let mut decoded_count = 0usize;
    for block in stripe.catalog().iter() {
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .ok_or("sealed log block has no staged payload")?;
        let structural = zstd::bulk::decompress(&compressed, block.structural_bytes as usize)?;
        decoded_count += decode_structural_block(&structural)?.len();
    }
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.durable_logs.len());
    let query = LogQuery::new(corpus.durable_logs[0].record_ref.topic_partition)
        .with_field("service.name", "checkout-api")
        .with_term("completed")
        .with_limit(100);
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p95, lookup_p99) =
        measure_lookup(iterations, || stripe.query(&query).len());
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes: 0,
        durable_bytes: payload_bytes,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p95,
        lookup_p99,
    })
}

pub(super) fn benchmark_traces(
    corpus: &Corpus,
    iterations: usize,
    durable_output_dir: Option<&Path>,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = serialized_bytes(&corpus.spans)?;
    let grouped = group_trace_blocks(&corpus.spans)?;
    let start = Instant::now();
    let encoded = grouped
        .iter()
        .map(|spans| {
            let payload = encode_trace_block(spans)?;
            let filter = serde_json::to_vec(&CorrelationBlockFilter::for_spans(spans))?;
            Ok::<_, Box<dyn std::error::Error>>((payload, filter))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let durable_bytes = durable_output_dir.map_or_else(
        || {
            Ok(encoded
                .iter()
                .map(|(payload, filter)| payload.len() + filter.len())
                .sum())
        },
        |output_dir| persist_encoded(output_dir.join("traces.pack"), &encoded),
    )?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = encoded.iter().map(|(payload, _)| payload.len()).sum();
    let auxiliary_bytes = encoded.iter().map(|(_, filter)| filter.len()).sum();
    let start = Instant::now();
    let decoded_count = encoded.iter().try_fold(0usize, |count, (block, _)| {
        decode_trace_block(block).map(|spans| count + spans.len())
    })?;
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.spans.len());

    let mut stripe = TraceStripe::new(512 * 1024 * 1024)?;
    for span in &corpus.spans {
        stripe.apply_ref(span, span.start_time_unix_nanos)?;
    }
    let query = TraceQuery {
        tenant: Arc::from(TENANT),
        trace_id: Some(corpus.spans[corpus.spans.len() / 2].trace_id),
        limit: 32,
        ..TraceQuery::default()
    };
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p95, lookup_p99) =
        measure_lookup(iterations, || {
            stripe.query(&query).map_or(0, |value| value.len())
        });
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes,
        durable_bytes,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p95,
        lookup_p99,
    })
}

pub(super) fn benchmark_resource_selector(
    corpus: &Corpus,
    cardinality: usize,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let resources = (0..cardinality)
        .map(|ordinal| {
            Arc::new(ResourceContext {
                attributes: Arc::new(vec![
                    TelemetryAttribute::new(
                        "service.name",
                        TelemetryValue::String(Arc::from(format!("service-{ordinal}"))),
                    ),
                    TelemetryAttribute::new(
                        "deployment.environment",
                        TelemetryValue::String(Arc::from("production")),
                    ),
                ]),
                schema_url: Arc::from("https://opentelemetry.io/schemas/1.37.0"),
                ..ResourceContext::default()
            })
        })
        .collect::<Vec<_>>();
    let mut stripe = TraceStripe::new(512 * 1024 * 1024)?;
    for (ordinal, source) in corpus.spans.iter().enumerate() {
        let mut span = source.clone();
        span.resource = Arc::clone(&resources[ordinal % cardinality]);
        stripe.apply_ref(&span, source.start_time_unix_nanos)?;
    }

    let target = cardinality / 2;
    let target_service = format!("service-{target}");
    let query = TraceQuery {
        tenant: Arc::from(TENANT),
        exact_resource_attributes: Arc::new(vec![(
            Arc::from("service.name"),
            Arc::from(target_service.as_str()),
        )]),
        limit: 8,
        ..TraceQuery::default()
    };
    let expected = corpus
        .spans
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| ordinal % cardinality == target)
        .count()
        .min(query.limit);
    let (rows, ops, p50, p95, p99) = measure_lookup(iterations, || {
        stripe.query(&query).map_or(0, |value| value.len())
    });
    println!(
        "resource_selector cardinality={cardinality} spans={} expected_rows={expected} rows={rows} lookup_ops_s={ops:.2} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
        corpus.spans.len(),
        p50.as_secs_f64() * 1e6,
        p95.as_secs_f64() * 1e6,
        p99.as_secs_f64() * 1e6,
    );
    Ok(())
}

pub(super) fn benchmark_metrics(
    corpus: &Corpus,
    iterations: usize,
    durable_output_dir: Option<&Path>,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = serialized_bytes(&corpus.points)?;
    let mut series = BTreeMap::<SeriesFingerprint, Vec<DurableMetricPoint>>::new();
    for point in &corpus.points {
        series
            .entry(point.series_fingerprint())
            .or_default()
            .push(point.clone());
    }
    let start = Instant::now();
    let encoded = series
        .values()
        .map(|points| {
            let payload = encode_metric_chunk(points)?;
            let filter = serde_json::to_vec(&CorrelationBlockFilter::for_metrics(points))?;
            Ok::<_, Box<dyn std::error::Error>>((payload, filter))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let durable_bytes = durable_output_dir.map_or_else(
        || {
            Ok(encoded
                .iter()
                .map(|(payload, filter)| payload.len() + filter.len())
                .sum())
        },
        |output_dir| persist_encoded(output_dir.join("metrics.pack"), &encoded),
    )?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = encoded.iter().map(|(payload, _)| payload.len()).sum();
    let auxiliary_bytes = encoded.iter().map(|(_, filter)| filter.len()).sum();
    let start = Instant::now();
    let decoded_count = encoded.iter().try_fold(0usize, |count, (chunk, _)| {
        decode_metric_chunk(chunk).map(|points| count + points.len())
    })?;
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.points.len());

    let mut stripe = MetricStripe::new(512 * 1024 * 1024)?;
    for point in &corpus.points {
        stripe.apply_ref(point, MetricIngestProtocol::Otlp)?;
    }
    let selected = &corpus.points[corpus.points.len() / 2];
    let query = MetricQuery {
        tenant: Arc::from(TENANT),
        series: Some(selected.series_fingerprint()),
        limit: 100,
        ..MetricQuery::default()
    };
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p95, lookup_p99) =
        measure_lookup(iterations, || {
            stripe.query(&query).map_or(0, |value| value.len())
        });
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes,
        durable_bytes,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p95,
        lookup_p99,
    })
}

pub(super) fn benchmark_correlations(corpus: &Corpus, iterations: usize) -> ResultRow {
    let mut index = CorrelationIndex::new(CorrelationConfig {
        max_keys: corpus.spans.len().saturating_mul(8),
        max_refs_per_key: corpus.spans.len().saturating_mul(3),
        max_total_refs: corpus.spans.len().saturating_mul(32),
    });
    for log in &corpus.durable_logs {
        index.index_log(TENANT, log);
    }
    for span in &corpus.spans {
        index.index_span(span);
    }
    for point in &corpus.points {
        index.index_metric(point);
    }
    let query = CorrelationQuery::new(TENANT)
        .with_resource_id(corpus.resource.id())
        .with_attribute(&corpus.label)
        .with_limit(1_000);
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p95, lookup_p99) =
        measure_lookup(iterations, || index.query(&query).len());
    ResultRow {
        source_bytes: 0,
        payload_bytes: 0,
        auxiliary_bytes: 0,
        durable_bytes: 0,
        encode_mib_per_second: 0.0,
        decode_mib_per_second: 0.0,
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p95,
        lookup_p99,
    }
}

pub(super) fn serialized_bytes<T: serde::Serialize>(
    records: &[T],
) -> Result<usize, rmp_serde::encode::Error> {
    records.iter().try_fold(0usize, |total, record| {
        Ok(total + rmp_serde::to_vec(record)?.len())
    })
}

pub(super) fn canonical_log_bytes(record: &DurableLog) -> Result<usize, rmp_serde::encode::Error> {
    let fields = record
        .fields
        .iter()
        .map(|field| 16 + field.key.len() + field.value.len())
        .sum::<usize>();
    Ok(8 * 8
        + record.message.len()
        + fields
        + rmp_serde::to_vec(&record.body)?.len()
        + rmp_serde::to_vec(&record.attributes)?.len()
        + rmp_serde::to_vec(&record.resource)?.len()
        + rmp_serde::to_vec(&record.scope)?.len()
        + record.severity_text.len()
        + record.event_name.len())
}

pub(super) fn measure_lookup(
    mut iterations: usize,
    mut lookup: impl FnMut() -> usize,
) -> (usize, f64, Duration, Duration, Duration) {
    let warm_count = black_box(lookup());
    let mut samples = Vec::with_capacity(iterations);
    let started = Instant::now();
    while iterations > 0 {
        let start = Instant::now();
        black_box(lookup());
        samples.push(start.elapsed());
        iterations -= 1;
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    (
        warm_count,
        samples.len() as f64 / elapsed.as_secs_f64(),
        p50,
        p95,
        p99,
    )
}

pub(super) fn print_result(signal: &str, result: ResultRow) {
    let stored_bytes = result.payload_bytes + result.auxiliary_bytes;
    println!(
        "{signal} source_bytes={} payload_bytes={} auxiliary_bytes={} stored_bytes={} durable_bytes={} ratio={:.2}x encode_mib_s={:.2} decode_mib_s={:.2} lookup_results={} lookup_ops_s={:.2} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
        result.source_bytes,
        result.payload_bytes,
        result.auxiliary_bytes,
        stored_bytes,
        result.durable_bytes,
        result.source_bytes as f64 / result.durable_bytes as f64,
        result.encode_mib_per_second,
        result.decode_mib_per_second,
        result.lookup_count,
        result.lookup_ops_per_second,
        result.lookup_p50.as_secs_f64() * 1e6,
        result.lookup_p95.as_secs_f64() * 1e6,
        result.lookup_p99.as_secs_f64() * 1e6,
    );
}

pub(super) fn persist_encoded(
    path: PathBuf,
    encoded: &[(Vec<u8>, Vec<u8>)],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut output = File::create(&path)?;
    for (payload, filter) in encoded {
        output.write_all(&(payload.len() as u64).to_le_bytes())?;
        output.write_all(&(filter.len() as u64).to_le_bytes())?;
        output.write_all(payload)?;
        output.write_all(filter)?;
    }
    output.sync_all()?;
    Ok(fs::metadata(path)?.len() as usize)
}

pub(super) fn group_trace_blocks(
    spans: &[DurableSpan],
) -> Result<Vec<Vec<DurableSpan>>, Box<dyn std::error::Error>> {
    let mut traces = BTreeMap::<(Arc<str>, TraceId), Vec<DurableSpan>>::new();
    for span in spans {
        traces
            .entry((Arc::clone(&span.tenant), span.trace_id))
            .or_default()
            .push(span.clone());
    }
    let mut blocks = Vec::new();
    let mut block = Vec::new();
    let mut block_bytes = 0usize;
    for trace in traces.into_values() {
        let trace_bytes = serialized_bytes(&trace)?;
        if !block.is_empty() && block_bytes.saturating_add(trace_bytes) > TRACE_BLOCK_SOURCE_BYTES {
            blocks.push(std::mem::take(&mut block));
            block_bytes = 0;
        }
        block_bytes = block_bytes.saturating_add(trace_bytes);
        block.extend(trace);
    }
    if !block.is_empty() {
        blocks.push(block);
    }
    Ok(blocks)
}

pub(super) fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

pub(super) fn trace_id(value: usize) -> Result<TraceId, Box<dyn std::error::Error>> {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&(value as u64).to_be_bytes());
    Ok(TraceId::from_bytes(bytes)?)
}

pub(super) fn make_span_id(value: usize) -> Result<SpanId, Box<dyn std::error::Error>> {
    Ok(SpanId::from_bytes((value as u64).to_be_bytes())?)
}

pub(super) fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
