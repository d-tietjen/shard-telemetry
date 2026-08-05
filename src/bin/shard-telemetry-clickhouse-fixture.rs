use std::fs;
use std::path::PathBuf;

use clap::Parser;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, exemplar, metric,
        number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
};
use prost::Message;

#[derive(Debug, Parser)]
#[command(
    name = "shard-telemetry-clickhouse-fixture",
    about = "Generate the correlated OTLP protobuf fixture used by ClickHouse compatibility gates"
)]
struct Arguments {
    #[arg(long)]
    output_directory: PathBuf,
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn resource() -> Resource {
    Resource {
        attributes: vec![
            string_attribute("service.name", "checkout-api"),
            string_attribute("deployment.environment.name", "production"),
        ],
        ..Resource::default()
    }
}

fn scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "shard-telemetry-compatibility".into(),
        version: "1.0.0".into(),
        ..InstrumentationScope::default()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    fs::create_dir_all(&arguments.output_directory)?;

    let trace_id = vec![0x11; 16];
    let span_id = vec![0x22; 8];
    let timestamp = 1_800_000_000_000_000_000;

    let logs = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource()),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope()),
                log_records: vec![
                    LogRecord {
                        time_unix_nano: timestamp + 1_000_000,
                        observed_time_unix_nano: timestamp + 1_100_000,
                        severity_number: 17,
                        severity_text: "ERROR".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "checkout request failed with error".into(),
                            )),
                        }),
                        attributes: vec![
                            string_attribute("http.request.method", "POST"),
                            string_attribute("http.response.status_code", "500"),
                        ],
                        ..LogRecord::default()
                    },
                    LogRecord {
                        time_unix_nano: timestamp + 2_000_000,
                        observed_time_unix_nano: timestamp + 2_100_000,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "checkout request completed".into(),
                            )),
                        }),
                        attributes: vec![
                            string_attribute("http.request.method", "POST"),
                            string_attribute("http.response.status_code", "200"),
                        ],
                        trace_id: trace_id.clone(),
                        span_id: span_id.clone(),
                        ..LogRecord::default()
                    },
                    LogRecord {
                        time_unix_nano: timestamp + 3_000_000,
                        observed_time_unix_nano: timestamp + 3_100_000,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "health check completed".into(),
                            )),
                        }),
                        ..LogRecord::default()
                    },
                ],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    };

    let traces = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource()),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope()),
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    name: "POST /checkout".into(),
                    kind: 2,
                    start_time_unix_nano: timestamp,
                    end_time_unix_nano: timestamp + 5_000_000,
                    attributes: vec![
                        string_attribute("http.request.method", "POST"),
                        string_attribute("server.address", "checkout.internal"),
                    ],
                    events: vec![span::Event {
                        time_unix_nano: timestamp + 1_000_000,
                        name: "payment.authorized".into(),
                        attributes: vec![string_attribute("payment.provider", "test")],
                        ..span::Event::default()
                    }],
                    links: vec![span::Link {
                        trace_id: vec![0x33; 16],
                        span_id: vec![0x44; 8],
                        attributes: vec![string_attribute("link.kind", "producer")],
                        ..span::Link::default()
                    }],
                    status: Some(Status {
                        code: 1,
                        ..Status::default()
                    }),
                    ..Span::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };

    let metrics = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope()),
                metrics: vec![Metric {
                    name: "checkout.requests".into(),
                    description: "Completed checkout requests".into(),
                    unit: "{request}".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![string_attribute("http.request.method", "POST")],
                            time_unix_nano: timestamp + 3_000_000,
                            value: Some(number_data_point::Value::AsInt(1)),
                            exemplars: vec![Exemplar {
                                time_unix_nano: timestamp + 3_000_000,
                                value: Some(exemplar::Value::AsInt(1)),
                                trace_id,
                                span_id,
                                ..Exemplar::default()
                            }],
                            ..NumberDataPoint::default()
                        }],
                    })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    };

    for (name, payload) in [
        ("logs.pb", logs.encode_to_vec()),
        ("traces.pb", traces.encode_to_vec()),
        ("metrics.pb", metrics.encode_to_vec()),
    ] {
        let path = arguments.output_directory.join(name);
        fs::write(&path, payload)?;
        println!("{}", path.display());
    }
    Ok(())
}
