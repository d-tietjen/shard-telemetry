use super::*;

pub(crate) fn group_analytics_rows<S: LokiStore + ?Sized>(
    store: &S,
    request: &AnalyticsScanRequest,
    emit: &mut dyn FnMut(&[AnalyticsGroupRow]) -> Result<(), LokiApiError>,
) -> Result<(), LokiApiError> {
    request.validate()?;
    if request.group_by.is_empty() {
        return Err(LokiApiError::bad_request(
            "grouping requires at least one group key",
        ));
    }
    let mut scan = request.clone();
    scan.group_by.clear();
    scan.group_limit = None;
    scan.group_order = AnalyticsGroupOrder::KeyAscending;
    scan.cardinality_only = false;
    scan.limit = None;
    scan.order = None;
    for key in &request.group_by {
        let column = match key {
            AnalyticsGroupKey::SeverityText => AnalyticsColumn::SeverityText,
            AnalyticsGroupKey::ScopeName => AnalyticsColumn::Metadata,
            AnalyticsGroupKey::Minute => AnalyticsColumn::Timestamp,
        };
        if !scan.columns.contains(&column) {
            scan.columns.push(column);
        }
    }
    let mut groups = BTreeMap::<Vec<Option<Arc<str>>>, u64>::new();
    store.scan_analytics(&scan, &mut |rows| {
        for row in rows {
            let key = request
                .group_by
                .iter()
                .map(|group| group_value(row, *group))
                .collect::<Vec<_>>();
            let count = groups.entry(key).or_default();
            *count = count.saturating_add(1);
        }
        Ok(())
    })?;
    let mut grouped = groups
        .into_iter()
        .map(|(keys, count)| AnalyticsGroupRow { keys, count })
        .collect::<Vec<_>>();
    if request.group_order == AnalyticsGroupOrder::CountDescending {
        grouped.sort_unstable_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.keys.cmp(&right.keys))
        });
    }
    if let Some(limit) = request.group_limit {
        grouped.truncate(limit);
    }
    if !grouped.is_empty() {
        emit(&grouped)?;
    }
    Ok(())
}

pub(super) fn group_value(row: &AnalyticsRow, key: AnalyticsGroupKey) -> Option<Arc<str>> {
    match key {
        AnalyticsGroupKey::SeverityText => row
            .severity_text
            .clone()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                row.metadata
                    .get("severity_text")
                    .map(|value| Arc::<str>::from(value.as_str()))
            }),
        AnalyticsGroupKey::ScopeName => row
            .metadata
            .get("scope_name")
            .map(|value| Arc::<str>::from(value.as_str())),
        AnalyticsGroupKey::Minute => Some(Arc::from(
            (row.timestamp_unix_nanos.div_euclid(60_000_000_000)).to_string(),
        )),
    }
}

pub(crate) fn durable_group_value(record: &DurableLog, key: AnalyticsGroupKey) -> Option<Arc<str>> {
    match key {
        AnalyticsGroupKey::SeverityText => record
            .fields
            .iter()
            .find(|field| field.key.as_ref() == "attr.loki.metadata.severity_text")
            .map(|field| Arc::clone(&field.value))
            .or_else(|| {
                (!record.severity_text.is_empty()).then(|| Arc::clone(&record.severity_text))
            }),
        AnalyticsGroupKey::ScopeName => record
            .fields
            .iter()
            .find(|field| field.key.as_ref() == "attr.loki.metadata.scope_name")
            .map(|field| Arc::clone(&field.value))
            .or_else(|| (!record.scope.name.is_empty()).then(|| Arc::clone(&record.scope.name))),
        AnalyticsGroupKey::Minute => Some(Arc::from(
            (record.timestamp_unix_nanos / 60_000_000_000).to_string(),
        )),
    }
}

pub(crate) fn decoded_group_value(
    record: &crate::DecodedStructuralRecord,
    key: AnalyticsGroupKey,
) -> Option<Arc<str>> {
    match key {
        AnalyticsGroupKey::SeverityText => record
            .fields
            .iter()
            .find(|field| field.key.as_ref() == "attr.loki.metadata.severity_text")
            .map(|field| Arc::clone(&field.value))
            .or_else(|| {
                (!record.severity_text.is_empty()).then(|| Arc::clone(&record.severity_text))
            }),
        AnalyticsGroupKey::ScopeName => record
            .fields
            .iter()
            .find(|field| field.key.as_ref() == "attr.loki.metadata.scope_name")
            .map(|field| Arc::clone(&field.value))
            .or_else(|| (!record.scope.name.is_empty()).then(|| Arc::clone(&record.scope.name))),
        AnalyticsGroupKey::Minute => Some(Arc::from(
            (record.timestamp_unix_nanos / 60_000_000_000).to_string(),
        )),
    }
}

pub(super) fn write_jsonlines_row(
    writer: &mut dyn Write,
    row: &AnalyticsRow,
    columns: &[AnalyticsColumn],
) -> Result<(), LokiApiError> {
    writer.write_all(b"{").map_err(rowbinary_error)?;
    for (index, column) in columns.iter().copied().enumerate() {
        if index != 0 {
            writer.write_all(b",").map_err(rowbinary_error)?;
        }
        serde_json::to_writer(&mut *writer, column.name()).map_err(json_error)?;
        writer.write_all(b":").map_err(rowbinary_error)?;
        write_json_column(writer, row, column)?;
    }
    writer.write_all(b"}\n").map_err(rowbinary_error)
}

pub(super) fn write_json_column(
    writer: &mut dyn Write,
    row: &AnalyticsRow,
    column: AnalyticsColumn,
) -> Result<(), LokiApiError> {
    match column.field().data_type() {
        DataType::Utf8 => write_json_scalar(writer, string_value(row, column)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            write_json_scalar(writer, timestamp_value(row, column))
        }
        DataType::UInt32 => write_json_scalar(writer, u32_value(row, column)),
        DataType::UInt64 => write_json_scalar(writer, u64_value(row, column)),
        DataType::Float64 => write_json_scalar(writer, f64_value(row, column)),
        DataType::Int32 => write_json_scalar(writer, i32_value(row, column)),
        DataType::Int64 => write_json_scalar(writer, row.scalar_integer),
        DataType::Boolean => write_json_scalar(writer, row.monotonic),
        DataType::Map(_, _) => write_json_map(writer, map_value(row, column)),
        _ => unreachable!("public analytical columns use supported JSON types"),
    }
}

pub(super) fn write_json_scalar<T: Serialize>(
    writer: &mut dyn Write,
    value: Option<T>,
) -> Result<(), LokiApiError> {
    serde_json::to_writer(&mut *writer, &value).map_err(json_error)
}

pub(super) fn write_json_map(
    writer: &mut dyn Write,
    values: &BTreeMap<String, String>,
) -> Result<(), LokiApiError> {
    writer.write_all(b"{").map_err(rowbinary_error)?;
    for (index, (key, value)) in values.iter().enumerate() {
        if index != 0 {
            writer.write_all(b",").map_err(rowbinary_error)?;
        }
        serde_json::to_writer(&mut *writer, key).map_err(json_error)?;
        writer.write_all(b":").map_err(rowbinary_error)?;
        serde_json::to_writer(&mut *writer, value).map_err(json_error)?;
    }
    writer.write_all(b"}").map_err(rowbinary_error)
}

pub(super) fn rowbinary_default_value(column: AnalyticsColumn) -> Vec<u8> {
    let field = column.field();
    if field.is_nullable() {
        return vec![1];
    }
    match field.data_type() {
        DataType::Utf8 | DataType::Boolean | DataType::Map(_, _) => vec![0],
        DataType::UInt32 | DataType::Int32 => vec![0; size_of::<u32>()],
        DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::UInt64
        | DataType::Int64
        | DataType::Float64 => {
            vec![0; size_of::<u64>()]
        }
        _ => unreachable!("public analytical columns use supported RowBinary types"),
    }
}

pub(super) fn write_rowbinary_row(
    writer: &mut dyn Write,
    row: &AnalyticsRow,
    columns: &[AnalyticsColumn],
) -> Result<(), LokiApiError> {
    for column in columns {
        let field = column.field();
        match field.data_type() {
            DataType::Utf8 => {
                let value = string_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    write_rowbinary_bytes(writer, value.expect("presence was checked").as_bytes())?;
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let value = timestamp_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::UInt32 => {
                let value = u32_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::UInt64 => {
                let value = u64_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Float64 => {
                let value = f64_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Int32 => {
                let value = i32_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Int64 => {
                let value = row.scalar_integer;
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Boolean => {
                let value = row.monotonic;
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&[u8::from(value.expect("presence was checked"))])
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Map(_, _) => {
                let values = map_value(row, *column);
                write_rowbinary_varuint(writer, values.len())?;
                for (key, value) in values {
                    write_rowbinary_bytes(writer, key.as_bytes())?;
                    write_rowbinary_bytes(writer, value.as_bytes())?;
                }
            }
            _ => {
                return Err(LokiApiError::internal(
                    "unsupported RowBinary analytics type",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn write_rowbinary_presence(
    writer: &mut dyn Write,
    present: bool,
    nullable: bool,
) -> Result<bool, LokiApiError> {
    if nullable {
        writer
            .write_all(&[u8::from(!present)])
            .map_err(rowbinary_error)?;
        Ok(present)
    } else if present {
        Ok(true)
    } else {
        Err(LokiApiError::internal(
            "non-nullable RowBinary column has no value",
        ))
    }
}

pub(super) fn write_rowbinary_bytes(
    writer: &mut dyn Write,
    value: &[u8],
) -> Result<(), LokiApiError> {
    write_rowbinary_varuint(writer, value.len())?;
    writer.write_all(value).map_err(rowbinary_error)
}

pub(super) fn write_rowbinary_varuint(
    writer: &mut dyn Write,
    mut value: usize,
) -> Result<(), LokiApiError> {
    while value >= 0x80 {
        writer
            .write_all(&[((value as u8) & 0x7f) | 0x80])
            .map_err(rowbinary_error)?;
        value >>= 7;
    }
    writer.write_all(&[value as u8]).map_err(rowbinary_error)
}

pub(super) fn rowbinary_error(error: io::Error) -> LokiApiError {
    LokiApiError::internal(error.to_string())
}

pub(super) fn json_error(error: serde_json::Error) -> LokiApiError {
    LokiApiError::internal(error.to_string())
}
