use super::*;

pub(super) fn resident_set_kib() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_ascii_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

pub(super) fn write_trace_row(
    output: &mut impl Write,
    span: &DurableSpan,
    raw: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    write_rowbinary_string(output, span.tenant.as_bytes())?;
    output.write_all(span.trace_id.as_bytes())?;
    output.write_all(span.span_id.as_bytes())?;
    write_nullable_fixed(
        output,
        span.parent_span_id.map(|value| *value.as_bytes()).as_ref(),
    )?;
    output.write_all(&span.record_ref.offset.get().to_le_bytes())?;
    output.write_all(&span.start_time_unix_nanos.to_le_bytes())?;
    output.write_all(&span.duration_nanos.to_le_bytes())?;
    write_rowbinary_string(output, span.name.as_bytes())?;
    output.write_all(&span.kind.to_le_bytes())?;
    output.write_all(
        &span
            .status
            .as_ref()
            .map_or(0, |status| status.code)
            .to_le_bytes(),
    )?;
    output.write_all(&span.resource_id().get().to_le_bytes())?;
    output.write_all(&span.scope_id().get().to_le_bytes())?;
    write_rowbinary_string(
        output,
        attribute_string(&span.resource.attributes, "service.name"),
    )?;
    write_rowbinary_string(
        output,
        attribute_string(&span.resource.attributes, "deployment.environment"),
    )?;
    write_rowbinary_string(output, attribute_string(&span.attributes, "http.route"))?;
    output.write_all(
        &attribute_integer(&span.attributes, "http.response.status_code")
            .unwrap_or_default()
            .to_le_bytes(),
    )?;
    write_rowbinary_string(output, raw)?;
    Ok(())
}

pub(super) fn write_metric_row(
    output: &mut impl Write,
    point: &DurableMetricPoint,
    raw: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    write_rowbinary_string(output, point.identity.tenant.as_bytes())?;
    output.write_all(&point.series_fingerprint().get().to_le_bytes())?;
    output.write_all(&point.record_ref.offset.get().to_le_bytes())?;
    output.write_all(&point.timestamp_unix_nanos.to_le_bytes())?;
    output.write_all(&point.start_time_unix_nanos.to_le_bytes())?;
    write_rowbinary_string(output, point.identity.name.as_bytes())?;
    write_rowbinary_string(output, point.identity.unit.as_bytes())?;
    write_rowbinary_string(output, b"gauge")?;
    output.write_all(&point.identity.resource_id().get().to_le_bytes())?;
    output.write_all(&point.identity.scope_id().get().to_le_bytes())?;
    write_rowbinary_string(
        output,
        attribute_string(&point.identity.resource.attributes, "service.name"),
    )?;
    write_rowbinary_string(
        output,
        attribute_string(
            &point.identity.resource.attributes,
            "deployment.environment",
        ),
    )?;
    write_rowbinary_string(
        output,
        attribute_string(&point.identity.point_attributes, "http.route"),
    )?;
    output.write_all(
        &attribute_integer(
            &point.identity.point_attributes,
            "http.response.status_code",
        )
        .unwrap_or_default()
        .to_le_bytes(),
    )?;
    write_rowbinary_string(
        output,
        attribute_string(&point.identity.point_attributes, "instance"),
    )?;
    let value = match point.value {
        MetricValue::Gauge(NumberValue::DoubleBits(bits)) => f64::from_bits(bits),
        MetricValue::Gauge(NumberValue::Integer(value)) => value as f64,
        _ => return Err("ClickHouse benchmark exporter currently expects gauge points".into()),
    };
    output.write_all(&value.to_bits().to_le_bytes())?;
    let exemplar_trace = point
        .exemplars
        .iter()
        .find_map(|exemplar| exemplar.trace_id)
        .map(|value| *value.as_bytes());
    write_nullable_fixed(output, exemplar_trace.as_ref())?;
    write_rowbinary_string(output, raw)?;
    Ok(())
}

pub(super) fn attribute_string<'a>(attributes: &'a [TelemetryAttribute], key: &str) -> &'a [u8] {
    attributes
        .iter()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| match &attribute.value {
            Some(TelemetryValue::String(value)) => Some(value.as_bytes()),
            _ => None,
        })
        .unwrap_or_default()
}

pub(super) fn attribute_integer(attributes: &[TelemetryAttribute], key: &str) -> Option<i64> {
    attributes
        .iter()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| match attribute.value.as_ref() {
            Some(TelemetryValue::Integer(value)) => Some(*value),
            _ => None,
        })
}

pub(super) fn write_nullable_fixed<const N: usize>(
    output: &mut impl Write,
    value: Option<&[u8; N]>,
) -> std::io::Result<()> {
    match value {
        Some(value) => {
            output.write_all(&[0])?;
            output.write_all(value)
        }
        None => output.write_all(&[1]),
    }
}

pub(super) fn write_rowbinary_string(output: &mut impl Write, value: &[u8]) -> std::io::Result<()> {
    write_var_uint(output, value.len())?;
    output.write_all(value)
}

pub(super) fn write_var_uint(output: &mut impl Write, mut value: usize) -> std::io::Result<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.write_all(&[byte])?;
        if value == 0 {
            return Ok(());
        }
    }
}

pub(super) fn parse_usize(
    value: Option<String>,
    flag: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(Into::into)
}
