use super::*;

pub(super) fn benchmark_server_scans(
    corpus: &Corpus,
    store: &DurableTelemetryStore,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if iterations == 0 {
        return Err("--server-scan-iterations must be nonzero".into());
    }
    let selected_trace = corpus.spans[corpus.spans.len() / 2].trace_id;
    let selected_series = corpus.points[corpus.points.len() / 2].series_fingerprint();
    let mut log = AnalyticsScanRequest::for_relation(TENANT, AnalyticsRelation::Logs);
    log.trace_id = Some(selected_trace);
    log.limit = Some(32);
    log.columns = vec![AnalyticsColumn::Timestamp, AnalyticsColumn::Message];
    let mut filtered_log = AnalyticsScanRequest::for_relation(TENANT, AnalyticsRelation::Logs);
    filtered_log
        .resource_attributes
        .push(MetadataField::new("service.name", "checkout-api"));
    filtered_log.limit = Some(1_000);
    filtered_log.columns = vec![AnalyticsColumn::Timestamp, AnalyticsColumn::Message];
    let mut trace = AnalyticsScanRequest::for_relation(TENANT, AnalyticsRelation::Spans);
    trace.trace_id = Some(selected_trace);
    trace.limit = Some(32);
    trace.columns = vec![AnalyticsColumn::Timestamp, AnalyticsColumn::Name];
    let mut metric = AnalyticsScanRequest::for_relation(TENANT, AnalyticsRelation::MetricPoints);
    metric.series_id = Some(selected_series);
    metric.limit = Some(3_000);
    metric.columns = vec![
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::ScalarDoubleBits,
    ];
    let mut resource = AnalyticsScanRequest::for_relation(TENANT, AnalyticsRelation::Spans);
    resource
        .resource_attributes
        .push(MetadataField::new("service.name", "checkout-api"));
    resource.limit = Some(1_000);
    resource.columns = vec![AnalyticsColumn::Timestamp, AnalyticsColumn::Name];
    for (name, request) in [
        ("log", log),
        ("log_filtered", filtered_log),
        ("trace", trace),
        ("metric", metric),
        ("resource", resource),
    ] {
        let (rows, ops, p50, p95, p99) = measure_lookup(iterations, || {
            let mut count = 0;
            store
                .scan_analytics(&request, &mut |batch| {
                    count += batch.len();
                    Ok(())
                })
                .expect("server analytical scan");
            count
        });
        println!(
            "server_scan signal={name} rows={rows} lookup_ops_s={ops:.2} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
            p50.as_secs_f64() * 1e6,
            p95.as_secs_f64() * 1e6,
            p99.as_secs_f64() * 1e6,
        );
    }
    Ok(())
}

pub(super) fn export_clickhouse_corpus(
    corpus: &Corpus,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    let trace_path = output_dir.join("traces.rowbinary");
    let metric_path = output_dir.join("metrics.rowbinary");
    let trace_expected_path = output_dir.join("trace-lookup-expected.rowbinary");
    let metric_expected_path = output_dir.join("metric-lookup-expected.rowbinary");
    let selected_trace = corpus.spans[corpus.spans.len() / 2].trace_id;
    let selected_series = corpus.points[corpus.points.len() / 2].series_fingerprint();

    let mut traces = BufWriter::new(File::create(&trace_path)?);
    let mut trace_expected = Vec::new();
    let mut trace_source_bytes = 0usize;
    let mut trace_expected_rows = 0usize;
    for span in &corpus.spans {
        let raw = rmp_serde::to_vec(span)?;
        trace_source_bytes = trace_source_bytes.saturating_add(raw.len());
        write_trace_row(&mut traces, span, &raw)?;
        if span.trace_id == selected_trace {
            write_rowbinary_string(&mut trace_expected, &raw)?;
            trace_expected_rows += 1;
        }
    }
    traces.flush()?;
    fs::write(&trace_expected_path, trace_expected)?;

    let mut metrics = BufWriter::new(File::create(&metric_path)?);
    let mut metric_expected = Vec::new();
    let mut metric_source_bytes = 0usize;
    let mut metric_expected_rows = 0usize;
    for point in &corpus.points {
        let raw = rmp_serde::to_vec(point)?;
        metric_source_bytes = metric_source_bytes.saturating_add(raw.len());
        write_metric_row(&mut metrics, point, &raw)?;
        if point.series_fingerprint() == selected_series {
            write_rowbinary_string(&mut metric_expected, &raw)?;
            metric_expected_rows += 1;
        }
    }
    metrics.flush()?;
    fs::write(&metric_expected_path, metric_expected)?;

    let resource = corpus.resource.id();
    let manifest = format!(
        concat!(
            "records_per_signal={}\n",
            "trace_source_bytes={}\n",
            "metric_source_bytes={}\n",
            "trace_id_hex={}\n",
            "trace_lookup_rows={}\n",
            "series_id={}\n",
            "series_id_hex={:032x}\n",
            "metric_lookup_rows={}\n",
            "resource_id={}\n",
            "service_name=checkout-api\n"
        ),
        corpus.spans.len(),
        trace_source_bytes,
        metric_source_bytes,
        selected_trace,
        trace_expected_rows,
        selected_series.get(),
        selected_series.get(),
        metric_expected_rows,
        resource.get(),
    );
    fs::write(output_dir.join("manifest.env"), manifest)?;
    Ok(())
}
