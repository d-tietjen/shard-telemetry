//! Signal benchmark ownership: scans, persistence, export, and workloads live in the sibling folder.
#[path = "shard-telemetry-signal-bench/scans.rs"]
mod scans;
use scans::*;
#[path = "shard-telemetry-signal-bench/persist.rs"]
mod persist;
use persist::*;
#[path = "shard-telemetry-signal-bench/export.rs"]
mod export;
use export::*;
#[path = "shard-telemetry-signal-bench/workload.rs"]
mod workload;
use workload::*;
#[cfg(test)]
#[path = "shard-telemetry-signal-bench/tests.rs"]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};
use shard_telemetry::{
    AnalyticsColumn, AnalyticsRelation, AnalyticsScanRequest, CompressionCohortId,
    CorrelationBlockFilter, CorrelationConfig, CorrelationIndex, CorrelationQuery, DurableLog,
    DurableMetricPoint, DurableSpan, DurableTelemetryConfig, DurableTelemetryStore, LOGS_TOPIC_ID,
    LogQuery, LogStripe, LokiStore, METRICS_TOPIC_ID, MetadataField, MetricIdentity,
    MetricIngestProtocol, MetricKind, MetricQuery, MetricStripe, MetricValue,
    NativePartitionAppend, NativeTelemetryBatch, NumberValue, OtlpLogEvent, ResourceContext,
    ScopeContext, SeriesFingerprint, SpanId, SpanStatus, StripeConfig, TRACES_TOPIC_ID,
    TelemetryAttribute, TelemetryEnvelope, TelemetryRecordRef, TelemetryRouter, TelemetrySignal,
    TelemetryValue, TraceId, TraceQuery, TraceStripe, decode_metric_chunk, decode_structural_block,
    decode_trace_block, encode_metric_chunk, encode_trace_block,
};

const TENANT: &str = "production-example";
const TRACE_BLOCK_SOURCE_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut records = 32_768usize;
    let mut iterations = 2_000usize;
    let mut clickhouse_dir = None::<PathBuf>;
    let mut durable_output_dir = None::<PathBuf>;
    let mut server_data_directory = None::<PathBuf>;
    let mut server_shards = 1usize;
    let mut server_partitions = 256usize;
    let mut server_append_linger_micros = 250u64;
    let mut server_scan_iterations = None::<usize>;
    let mut server_only = false;
    let mut server_open_only = false;
    let mut generate_only = false;
    let mut server_recovery_journal = false;
    let mut server_hold_seconds = 0u64;
    let mut resource_cardinality = None::<usize>;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--records" => records = parse_usize(args.next(), "--records")?,
            "--iterations" => iterations = parse_usize(args.next(), "--iterations")?,
            "--clickhouse-dir" => {
                clickhouse_dir = Some(PathBuf::from(
                    args.next().ok_or("missing value for --clickhouse-dir")?,
                ));
            }
            "--durable-output-dir" => {
                durable_output_dir = Some(PathBuf::from(
                    args.next()
                        .ok_or("missing value for --durable-output-dir")?,
                ));
            }
            "--server-data-directory" => {
                server_data_directory = Some(PathBuf::from(
                    args.next()
                        .ok_or("missing value for --server-data-directory")?,
                ));
            }
            "--server-shards" => server_shards = parse_usize(args.next(), "--server-shards")?,
            "--server-partitions" => {
                server_partitions = parse_usize(args.next(), "--server-partitions")?;
            }
            "--server-append-linger-micros" => {
                server_append_linger_micros = args
                    .next()
                    .ok_or("missing value for --server-append-linger-micros")?
                    .parse()?;
            }
            "--server-scan-iterations" => {
                server_scan_iterations =
                    Some(parse_usize(args.next(), "--server-scan-iterations")?);
            }
            "--server-only" => server_only = true,
            "--server-open-only" => server_open_only = true,
            "--generate-only" => generate_only = true,
            "--server-recovery-journal" => server_recovery_journal = true,
            "--server-hold-seconds" => {
                server_hold_seconds = args
                    .next()
                    .ok_or("missing value for --server-hold-seconds")?
                    .parse()?;
            }
            "--resource-cardinality" => {
                resource_cardinality = Some(parse_usize(args.next(), "--resource-cardinality")?);
            }
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if records < 128
        || iterations == 0
        || server_shards == 0
        || server_shards > 256
        || server_partitions == 0
        || server_partitions > usize::from(u16::MAX)
        || server_shards > server_partitions
        || resource_cardinality == Some(0)
    {
        return Err(
            "--records must be at least 128, --iterations must be nonzero, --server-shards must be in 1..=256, and --server-partitions must fit u16 and be at least the shard count"
                .into(),
        );
    }

    let corpus = Corpus::generate(records)?;
    println!(
        "fixture records={} resident_kib={}",
        records.saturating_mul(3),
        resident_set_kib().unwrap_or_default()
    );
    if generate_only {
        black_box((
            corpus.durable_logs.len(),
            corpus.spans.len(),
            corpus.points.len(),
        ));
        println!(
            "generated records_per_signal={} total_records={}",
            records,
            records.saturating_mul(3)
        );
        return Ok(());
    }
    if let Some(output_dir) = clickhouse_dir.as_deref() {
        export_clickhouse_corpus(&corpus, output_dir)?;
    }
    if let Some(output_dir) = durable_output_dir.as_deref() {
        fs::create_dir_all(output_dir)?;
    }
    if let Some(data_directory) = server_data_directory.as_deref() {
        let store = if server_open_only {
            let open_started = Instant::now();
            let store = DurableTelemetryStore::open(server_store_config(
                data_directory,
                server_shards,
                server_partitions,
                server_append_linger_micros,
                server_recovery_journal,
            )?)?;
            println!(
                "embedded_open_only shards={} partitions={} recovery_journal={} open_seconds={:.6} resident_kib={}",
                server_shards,
                server_partitions,
                server_recovery_journal,
                open_started.elapsed().as_secs_f64(),
                resident_set_kib().unwrap_or_default()
            );
            store
        } else {
            let started = Instant::now();
            let (store, phases) = persist_server_store(
                &corpus,
                data_directory,
                server_shards,
                server_partitions,
                server_append_linger_micros,
                server_recovery_journal,
            )?;
            println!(
                "embedded_store records={} shards={} partitions={} append_linger_micros={} recovery_journal={} open_seconds={:.6} logs_seconds={:.6} traces_seconds={:.6} metrics_seconds={:.6} metric_batches={} open_resident_kib={} logs_resident_kib={} traces_resident_kib={} metrics_resident_kib={} elapsed_seconds={:.6}",
                corpus
                    .durable_logs
                    .len()
                    .saturating_add(corpus.spans.len())
                    .saturating_add(corpus.points.len()),
                server_shards,
                server_partitions,
                server_append_linger_micros,
                server_recovery_journal,
                phases.open.as_secs_f64(),
                phases.logs.as_secs_f64(),
                phases.traces.as_secs_f64(),
                phases.metrics.as_secs_f64(),
                phases.metric_batches,
                phases.open_resident_kib,
                phases.logs_resident_kib,
                phases.traces_resident_kib,
                phases.metrics_resident_kib,
                started.elapsed().as_secs_f64()
            );
            store
        };
        if let Some(scan_iterations) = server_scan_iterations {
            benchmark_server_scans(&corpus, &store, scan_iterations)?;
        }
        if server_hold_seconds > 0 {
            std::thread::sleep(Duration::from_secs(server_hold_seconds));
            println!("embedded_idle seconds={server_hold_seconds}");
        }
        let drop_started = Instant::now();
        drop(store);
        println!(
            "embedded_shutdown elapsed_seconds={:.6} resident_kib={}",
            drop_started.elapsed().as_secs_f64(),
            resident_set_kib().unwrap_or_default()
        );
        if server_only {
            return Ok(());
        }
    }
    println!("ShardTelemetry signal benchmark (v1)");
    println!("records_per_signal={records} lookup_iterations={iterations}");

    if let Some(cardinality) = resource_cardinality {
        benchmark_resource_selector(&corpus, cardinality, iterations)?;
    }

    let log_result = benchmark_logs(&corpus, iterations)?;
    let trace_result = benchmark_traces(&corpus, iterations, durable_output_dir.as_deref())?;
    let metric_result = benchmark_metrics(&corpus, iterations, durable_output_dir.as_deref())?;
    let correlation_result = benchmark_correlations(&corpus, iterations);
    print_result("logs", log_result);
    print_result("traces", trace_result);
    print_result("metrics", metric_result);
    println!(
        "correlation refs={} lookup_ops_s={:.2} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
        correlation_result.lookup_count,
        correlation_result.lookup_ops_per_second,
        correlation_result.lookup_p50.as_secs_f64() * 1e6,
        correlation_result.lookup_p95.as_secs_f64() * 1e6,
        correlation_result.lookup_p99.as_secs_f64() * 1e6,
    );
    Ok(())
}

struct EmbeddedPhases {
    open: Duration,
    logs: Duration,
    traces: Duration,
    metrics: Duration,
    metric_batches: usize,
    open_resident_kib: u64,
    logs_resident_kib: u64,
    traces_resident_kib: u64,
    metrics_resident_kib: u64,
}

struct Corpus {
    durable_logs: Vec<DurableLog>,
    spans: Vec<DurableSpan>,
    points: Vec<DurableMetricPoint>,
    resource: Arc<ResourceContext>,
    label: TelemetryAttribute,
}

impl Corpus {
    fn generate(count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let label = TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout-api")),
        );
        let resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![
                label.clone(),
                TelemetryAttribute::new(
                    "deployment.environment",
                    TelemetryValue::String(Arc::from("production")),
                ),
                TelemetryAttribute::new(
                    "cloud.region",
                    TelemetryValue::String(Arc::from("us-east-1")),
                ),
            ]),
            schema_url: Arc::from("https://opentelemetry.io/schemas/1.37.0"),
            ..ResourceContext::default()
        });
        let scope = Arc::new(ScopeContext {
            name: Arc::from("checkout/http"),
            version: Arc::from("2026.8.3"),
            ..ScopeContext::default()
        });
        let log_partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(3));
        let trace_partition = TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(3));
        let metric_partition = TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(3));
        let mut durable_logs = Vec::with_capacity(count);
        let mut spans = Vec::with_capacity(count);
        let mut points = Vec::with_capacity(count);
        let base = 1_785_700_000_000_000_000u64;
        for ordinal in 0..count {
            let trace_id = trace_id(ordinal / 8 + 1)?;
            let span_id = make_span_id(ordinal + 1)?;
            let status = [200, 200, 200, 400, 404, 500, 502, 503][ordinal & 7];
            let route = ["/checkout", "/cart", "/products", "/payment"][ordinal & 3];
            let message = match ordinal & 3 {
                0 => format!(
                    "completed POST {route} status={status} duration_ms={} request_id={:016x}",
                    4 + ordinal % 91,
                    mix(ordinal as u64)
                ),
                1 => format!(
                    "inventory reservation item={} warehouse={} quantity={} trace={trace_id}",
                    ordinal % 10_003,
                    ordinal % 17,
                    1 + ordinal % 5
                ),
                2 => format!(
                    "payment authorization provider=stripe result={} amount_cents={} customer={:012x}",
                    if status < 400 { "approved" } else { "declined" },
                    100 + ordinal % 50_000,
                    mix((ordinal as u64) ^ 0xa5a5)
                ),
                _ => format!(
                    "worker checkpoint partition={} offset={} lag_ms={} node=node-{}",
                    ordinal % 256,
                    ordinal * 97,
                    ordinal % 31,
                    ordinal % 16
                ),
            };
            let attributes = Arc::new(vec![
                TelemetryAttribute::new("http.route", TelemetryValue::String(Arc::from(route))),
                TelemetryAttribute::new(
                    "http.response.status_code",
                    TelemetryValue::Integer(status),
                ),
            ]);
            let fields = Arc::new(vec![
                MetadataField::new("service.name", "checkout-api"),
                MetadataField::new("resource.service.name", "checkout-api"),
                MetadataField::new("attr.http.route", route),
                MetadataField::new("attr.http.response.status_code", status.to_string()),
                MetadataField::new("otel.trace_id", trace_id.to_string()),
                MetadataField::new("otel.span_id", span_id.to_string()),
                MetadataField::new("otel.resource.id", resource.id().to_string()),
                MetadataField::new("otel.scope.id", scope.id().to_string()),
            ]);
            let event = OtlpLogEvent {
                timestamp_unix_nanos: base + ordinal as u64 * 1_000_000,
                observed_timestamp_unix_nanos: base + ordinal as u64 * 1_000_000 + 5_000,
                body: Some(TelemetryValue::String(Arc::from(message.as_str()))),
                message: Arc::from(message),
                fields,
                attributes: Arc::clone(&attributes),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                severity_number: if status >= 500 { 17 } else { 9 },
                severity_text: Arc::from(if status >= 500 { "ERROR" } else { "INFO" }),
                trace_id: Some(trace_id),
                span_id: Some(span_id),
                compression_cohort: CompressionCohortId::new(7),
                ..OtlpLogEvent::default()
            };
            durable_logs.push(event.clone().into_durable(
                ShardId::new(0),
                log_partition,
                LogicalOffset::new(ordinal as u64),
            ));
            spans.push(DurableSpan {
                stream_shard_id: ShardId::new(0),
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Traces,
                    trace_partition,
                    LogicalOffset::new(ordinal as u64),
                ),
                tenant: Arc::from(TENANT),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                trace_id,
                span_id,
                parent_span_id: (ordinal % 8 != 0)
                    .then(|| make_span_id(ordinal))
                    .transpose()?,
                trace_state: Arc::from("vendor=production"),
                flags: 1,
                name: Arc::from(
                    [
                        "POST /checkout",
                        "GET /cart",
                        "GET /products",
                        "POST /payment",
                    ][ordinal & 3],
                ),
                kind: 2,
                start_time_unix_nanos: base + ordinal as u64 * 1_000_000,
                duration_nanos: (4 + ordinal as u64 % 91) * 1_000_000,
                attributes: Arc::clone(&attributes),
                dropped_attributes_count: 0,
                events: Arc::new(Vec::new()),
                dropped_events_count: 0,
                links: Arc::new(Vec::new()),
                dropped_links_count: 0,
                status: Some(SpanStatus {
                    message: Arc::from(if status >= 500 {
                        "upstream failure"
                    } else {
                        ""
                    }),
                    code: if status >= 500 { 2 } else { 1 },
                }),
            });

            let series_ordinal = ordinal % 128;
            let identity = Arc::new(MetricIdentity {
                tenant: Arc::from(TENANT),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                name: Arc::from("http.server.request.duration"),
                unit: Arc::from("ms"),
                kind: MetricKind::Gauge,
                point_attributes: Arc::new(vec![
                    TelemetryAttribute::new(
                        "http.route",
                        TelemetryValue::String(Arc::from(
                            ["/checkout", "/cart", "/products", "/payment"][series_ordinal & 3],
                        )),
                    ),
                    TelemetryAttribute::new(
                        "http.response.status_code",
                        TelemetryValue::Integer([200, 400, 404, 500][(series_ordinal / 4) & 3]),
                    ),
                    TelemetryAttribute::new(
                        "instance",
                        TelemetryValue::String(Arc::from(format!("node-{}", series_ordinal % 16))),
                    ),
                    TelemetryAttribute::new(
                        "benchmark.series",
                        TelemetryValue::Integer(series_ordinal as i64),
                    ),
                ]),
            });
            points.push(DurableMetricPoint {
                stream_shard_id: ShardId::new(0),
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    metric_partition,
                    LogicalOffset::new(ordinal as u64),
                ),
                identity,
                description: Arc::from("HTTP server request duration"),
                metadata: Arc::new(Vec::new()),
                start_time_unix_nanos: base,
                timestamp_unix_nanos: base + (ordinal / 128) as u64 * 15_000_000_000,
                flags: 0,
                value: MetricValue::Gauge(NumberValue::from_f64(
                    4.0 + ((ordinal * 17) % 910) as f64 / 10.0,
                )),
                exemplars: Arc::new(Vec::new()),
            });
        }
        Ok(Self {
            durable_logs,
            spans,
            points,
            resource,
            label,
        })
    }
}

#[derive(Clone, Copy)]
struct ResultRow {
    source_bytes: usize,
    payload_bytes: usize,
    auxiliary_bytes: usize,
    durable_bytes: usize,
    encode_mib_per_second: f64,
    decode_mib_per_second: f64,
    lookup_count: usize,
    lookup_ops_per_second: f64,
    lookup_p50: Duration,
    lookup_p95: Duration,
    lookup_p99: Duration,
}
