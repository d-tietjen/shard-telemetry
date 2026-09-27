use super::*;

pub(super) fn write_success(stats: RemoteWriteStats) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    for (name, value) in [
        ("x-prometheus-remote-write-samples-written", stats.samples),
        (
            "x-prometheus-remote-write-histograms-written",
            stats.histograms,
        ),
        (
            "x-prometheus-remote-write-exemplars-written",
            stats.exemplars,
        ),
    ] {
        response.headers_mut().insert(
            name,
            HeaderValue::from_str(&value.to_string()).expect("u64 is a valid header value"),
        );
    }
    response
}

pub(super) fn write_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))],
        message.to_owned(),
    )
        .into_response()
}

pub(super) fn query_success(value: PromqlValue) -> Response {
    let (result_type, result) = match value {
        PromqlValue::Scalar {
            timestamp_ms,
            value,
        } => (
            "scalar",
            json!([timestamp_seconds(timestamp_ms), format_sample(value)]),
        ),
        PromqlValue::String {
            timestamp_ms,
            value,
        } => ("string", json!([timestamp_seconds(timestamp_ms), value])),
        PromqlValue::Vector(samples) => (
            "vector",
            Value::Array(
                samples
                    .into_iter()
                    .map(|sample| {
                        json!({
                            "metric": sample.labels,
                            "value": [
                                timestamp_seconds(sample.timestamp_ms),
                                format_sample(sample.value)
                            ]
                        })
                    })
                    .collect(),
            ),
        ),
        PromqlValue::Matrix(series) => (
            "matrix",
            Value::Array(
                series
                    .into_iter()
                    .map(|series| {
                        let values = series
                            .samples
                            .into_iter()
                            .map(|(timestamp, value)| {
                                json!([timestamp_seconds(timestamp), format_sample(value)])
                            })
                            .collect::<Vec<_>>();
                        json!({"metric": series.labels, "values": values})
                    })
                    .collect(),
            ),
        ),
    };
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "success",
            "data": {"resultType": result_type, "result": result}
        })),
    )
        .into_response()
}

pub(super) fn api_success(data: Value) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({"status": "success", "data": data})),
    )
        .into_response()
}

pub(super) fn metric_labels(point: &crate::DurableMetricPoint) -> BTreeMap<String, String> {
    let mut labels = crate::prometheus_string_labels(&point.identity)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("__name__".into(), point.identity.name.to_string());
    labels
}

pub(super) fn remote_read_selector(
    matchers: &[prometheus_v1::LabelMatcher],
) -> Result<String, crate::PromqlError> {
    if matchers.is_empty() {
        return Ok("{__name__=~\".+\"}".into());
    }
    let mut selector = String::from("{");
    for (index, matcher) in matchers.iter().enumerate() {
        if matcher.name.is_empty() {
            return Err(crate::PromqlError::new(
                "Remote Read matcher has an empty label name",
            ));
        }
        if index != 0 {
            selector.push(',');
        }
        selector.push_str(&matcher.name);
        selector.push_str(
            match prometheus_v1::LabelMatcherType::try_from(matcher.r#type) {
                Ok(prometheus_v1::LabelMatcherType::Equal) => "=",
                Ok(prometheus_v1::LabelMatcherType::NotEqual) => "!=",
                Ok(prometheus_v1::LabelMatcherType::RegexMatch) => "=~",
                Ok(prometheus_v1::LabelMatcherType::RegexNoMatch) => "!~",
                Err(_) => {
                    return Err(crate::PromqlError::new(
                        "Remote Read matcher has an unknown type",
                    ));
                }
            },
        );
        selector.push_str(
            &serde_json::to_string(&matcher.value)
                .map_err(|error| crate::PromqlError::new(error.to_string()))?,
        );
    }
    selector.push('}');
    Ok(selector)
}

pub(super) fn remote_read_float(value: &MetricValue) -> Option<f64> {
    match value {
        MetricValue::Gauge(value) | MetricValue::Sum(value) => Some(match value {
            NumberValue::Integer(value) => *value as f64,
            NumberValue::DoubleBits(bits) => f64::from_bits(*bits),
        }),
        MetricValue::ExplicitHistogram(_)
        | MetricValue::ExponentialHistogram(_)
        | MetricValue::Summary(_) => None,
    }
}

pub(super) enum RemoteReadValue {
    Sample(prometheus_v1::Sample),
    Histogram(Box<prometheus_v1::Histogram>),
}

pub(super) fn remote_read_value(
    point: &crate::DurableMetricPoint,
    timestamp_ms: i64,
) -> Option<RemoteReadValue> {
    if let Some(value) = remote_read_float(&point.value) {
        return Some(RemoteReadValue::Sample(prometheus_v1::Sample {
            value,
            timestamp: timestamp_ms,
        }));
    }
    let start_timestamp = i64::try_from(point.start_time_unix_nanos / 1_000_000).ok()?;
    let histogram = match &point.value {
        MetricValue::ExplicitHistogram(value) => {
            let (positive_deltas, positive_counts) = histogram_count_lanes(&value.bucket_counts)?;
            let positive_spans = if value.bucket_counts.is_empty() {
                Vec::new()
            } else {
                vec![prometheus_v1::BucketSpan {
                    offset: 0,
                    length: u32::try_from(value.bucket_counts.len()).ok()?,
                }]
            };
            prometheus_v1::Histogram {
                count: Some(histogram_count(value.count)),
                sum: value.sum_bits.map_or(0.0, f64::from_bits),
                schema: -53,
                zero_threshold: 0.0,
                zero_count: Some(prometheus_v1::histogram::ZeroCount::Int(0)),
                negative_spans: Vec::new(),
                negative_deltas: Vec::new(),
                negative_counts: Vec::new(),
                positive_spans,
                positive_deltas,
                positive_counts,
                reset_hint: value.reset_hint,
                timestamp: timestamp_ms,
                custom_values: value
                    .explicit_bounds_bits
                    .iter()
                    .copied()
                    .map(f64::from_bits)
                    .collect(),
                start_timestamp,
            }
        }
        MetricValue::ExponentialHistogram(value) => {
            let (negative_spans, negative_deltas, negative_counts) =
                histogram_buckets(value.negative.as_ref())?;
            let (positive_spans, positive_deltas, positive_counts) =
                histogram_buckets(value.positive.as_ref())?;
            prometheus_v1::Histogram {
                count: Some(histogram_count(value.count)),
                sum: value.sum_bits.map_or(0.0, f64::from_bits),
                schema: value.scale,
                zero_threshold: f64::from_bits(value.zero_threshold_bits),
                zero_count: Some(histogram_zero_count(value.zero_count)),
                negative_spans,
                negative_deltas,
                negative_counts,
                positive_spans,
                positive_deltas,
                positive_counts,
                reset_hint: value.reset_hint,
                timestamp: timestamp_ms,
                custom_values: Vec::new(),
                start_timestamp,
            }
        }
        MetricValue::Gauge(_) | MetricValue::Sum(_) | MetricValue::Summary(_) => return None,
    };
    Some(RemoteReadValue::Histogram(Box::new(histogram)))
}

pub(super) fn histogram_count(value: HistogramCount) -> prometheus_v1::histogram::Count {
    match value {
        HistogramCount::Integer(value) => prometheus_v1::histogram::Count::Int(value),
        HistogramCount::DoubleBits(bits) => {
            prometheus_v1::histogram::Count::Float(f64::from_bits(bits))
        }
    }
}

pub(super) fn histogram_zero_count(value: HistogramCount) -> prometheus_v1::histogram::ZeroCount {
    match value {
        HistogramCount::Integer(value) => prometheus_v1::histogram::ZeroCount::Int(value),
        HistogramCount::DoubleBits(bits) => {
            prometheus_v1::histogram::ZeroCount::Float(f64::from_bits(bits))
        }
    }
}

pub(super) fn histogram_buckets(
    buckets: Option<&ExponentialHistogramBuckets>,
) -> Option<(Vec<prometheus_v1::BucketSpan>, Vec<i64>, Vec<f64>)> {
    let Some(buckets) = buckets else {
        return Some((Vec::new(), Vec::new(), Vec::new()));
    };
    let spans = buckets
        .spans
        .iter()
        .map(|span| prometheus_v1::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect();
    let (deltas, counts) = histogram_count_lanes(&buckets.bucket_counts)?;
    Some((spans, deltas, counts))
}

pub(super) fn histogram_count_lanes(counts: &[HistogramCount]) -> Option<(Vec<i64>, Vec<f64>)> {
    if counts
        .iter()
        .all(|count| matches!(count, HistogramCount::Integer(_)))
    {
        let mut previous = 0_i64;
        let mut deltas = Vec::with_capacity(counts.len());
        for count in counts {
            let HistogramCount::Integer(count) = count else {
                unreachable!("integer lane was checked")
            };
            let count = i64::try_from(*count).ok()?;
            deltas.push(count.checked_sub(previous)?);
            previous = count;
        }
        Some((deltas, Vec::new()))
    } else if counts
        .iter()
        .all(|count| matches!(count, HistogramCount::DoubleBits(_)))
    {
        Some((
            Vec::new(),
            counts
                .iter()
                .map(|count| match count {
                    HistogramCount::DoubleBits(bits) => f64::from_bits(*bits),
                    HistogramCount::Integer(_) => unreachable!("float lane was checked"),
                })
                .collect(),
        ))
    } else {
        None
    }
}

pub(super) fn prometheus_metric_type(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Gauge => "gauge",
        MetricKind::Sum {
            monotonic: true, ..
        } => "counter",
        MetricKind::Sum { .. } => "gauge",
        MetricKind::ExplicitHistogram { .. } | MetricKind::ExponentialHistogram { .. } => {
            "histogram"
        }
        MetricKind::Summary => "summary",
    }
}

pub(super) fn format_number(value: NumberValue) -> String {
    match value {
        NumberValue::Integer(value) => value.to_string(),
        NumberValue::DoubleBits(bits) => format_sample(f64::from_bits(bits)),
    }
}

pub(super) fn render_telemetry_value(value: &TelemetryValue) -> String {
    match value {
        TelemetryValue::Empty => String::new(),
        TelemetryValue::String(value) => value.to_string(),
        TelemetryValue::Boolean(value) => value.to_string(),
        TelemetryValue::Integer(value) => value.to_string(),
        TelemetryValue::DoubleBits(bits) => format_sample(f64::from_bits(*bits)),
        TelemetryValue::Bytes(value) => value.iter().map(|byte| format!("{byte:02x}")).collect(),
        TelemetryValue::Array(_) | TelemetryValue::Map(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
        TelemetryValue::StringTableIndex(value) => value.to_string(),
    }
}

pub(super) fn query_error(status: StatusCode, error_type: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "status": "error",
            "errorType": error_type,
            "error": message
        })),
    )
        .into_response()
}

pub(super) fn format_sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}

pub(super) fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms as f64 / 1_000.0
}

pub(super) fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

pub(super) fn parse_prometheus_time(value: &str) -> Result<i64, String> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| format!("invalid Prometheus timestamp {value:?}"))?;
    if !seconds.is_finite() {
        return Err(format!("invalid Prometheus timestamp {value:?}"));
    }
    let milliseconds = seconds * 1_000.0;
    if milliseconds < i64::MIN as f64 || milliseconds > i64::MAX as f64 {
        return Err("Prometheus timestamp is out of range".into());
    }
    Ok(milliseconds.round() as i64)
}

pub(super) fn parse_prometheus_duration(value: &str) -> Result<i64, String> {
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
        && seconds > 0.0
    {
        return Ok((seconds * 1_000.0).round() as i64);
    }
    let (number, multiplier) = [
        ("ms", 1_i64),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
        ("w", 604_800_000),
        ("y", 31_536_000_000),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .ok_or_else(|| format!("invalid Prometheus duration {value:?}"))?;
    let number = number
        .parse::<f64>()
        .map_err(|_| format!("invalid Prometheus duration {value:?}"))?;
    let milliseconds = number * multiplier as f64;
    if !milliseconds.is_finite() || milliseconds <= 0.0 || milliseconds > i64::MAX as f64 {
        return Err(format!("invalid Prometheus duration {value:?}"));
    }
    Ok(milliseconds.round() as i64)
}
