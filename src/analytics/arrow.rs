use super::*;

pub(super) fn write_arrow_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
) -> Result<(), LokiApiError> {
    let schema = projection_schema(&request.columns);
    let mut sink = ChannelWriter::new(sender, STREAM_CHUNK_BYTES);
    {
        let mut writer = StreamWriter::try_new(&mut sink, &schema)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        if request.cardinality_only {
            store.scan_analytics_cardinality(request, &mut |count| {
                let mut remaining = count;
                while remaining > 0 {
                    let rows = remaining.min(DEFAULT_SCAN_BATCH_ROWS as u64) as usize;
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef],
                    )
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    remaining -= rows as u64;
                }
                Ok(())
            })?;
        } else if !store.scan_analytics_arrow(request, &schema, &mut |batch| {
            writer
                .write(batch)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        })? {
            store.scan_analytics(request, &mut |rows| {
                let batch = record_batch(rows, &request.columns, Arc::clone(&schema))?;
                writer
                    .write(&batch)
                    .map_err(|error| LokiApiError::internal(error.to_string()))
            })?;
        }
        writer
            .finish()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    sink.finish()
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(super) fn projection_schema(columns: &[AnalyticsColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .copied()
            .map(AnalyticsColumn::field)
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn record_batch(
    rows: &[AnalyticsRow],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<RecordBatch, LokiApiError> {
    let arrays = columns
        .iter()
        .copied()
        .map(|column| column_array(rows, column))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn direct_metric_record_batch(
    points: &[DurableMetricPoint],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<Option<RecordBatch>, LokiApiError> {
    if !can_direct_metric_projection(columns) {
        return Ok(None);
    }
    let arrays = columns
        .iter()
        .map(|column| -> Result<ArrayRef, LokiApiError> {
            Ok(match column {
                AnalyticsColumn::Timestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(points.len());
                    for point in points {
                        builder.append_value(timestamp_i64(point.timestamp_unix_nanos)?);
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::StartTimestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(points.len());
                    for point in points {
                        if let Some(value) = nonzero_timestamp(point.start_time_unix_nanos)? {
                            builder.append_value(value);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::Partition => {
                    Arc::new(UInt32Array::from_iter_values(points.iter().map(|point| {
                        point.record_ref.topic_partition.partition_id.get()
                    })))
                }
                AnalyticsColumn::Offset => Arc::new(UInt64Array::from_iter_values(
                    points.iter().map(|point| point.record_ref.offset.get()),
                )),
                AnalyticsColumn::Name => {
                    let mut builder = StringBuilder::new();
                    for point in points {
                        builder.append_value(point.identity.name.as_ref());
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::ScalarInteger => {
                    let mut builder = Int64Builder::with_capacity(points.len());
                    for point in points {
                        match point.value {
                            MetricValue::Gauge(NumberValue::Integer(value))
                            | MetricValue::Sum(NumberValue::Integer(value)) => {
                                builder.append_value(value);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::ScalarDoubleBits => {
                    let mut builder = UInt64Builder::with_capacity(points.len());
                    for point in points {
                        match point.value {
                            MetricValue::Gauge(NumberValue::DoubleBits(value))
                            | MetricValue::Sum(NumberValue::DoubleBits(value)) => {
                                builder.append_value(value);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => unreachable!("direct metric projection was validated"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays)
        .map(Some)
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn can_direct_metric_projection(columns: &[AnalyticsColumn]) -> bool {
    columns.iter().all(|column| {
        matches!(
            column,
            AnalyticsColumn::Timestamp
                | AnalyticsColumn::StartTimestamp
                | AnalyticsColumn::Partition
                | AnalyticsColumn::Offset
                | AnalyticsColumn::Name
                | AnalyticsColumn::ScalarInteger
                | AnalyticsColumn::ScalarDoubleBits
        )
    })
}

pub(crate) fn direct_span_record_batch(
    spans: &[DurableSpan],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<Option<RecordBatch>, LokiApiError> {
    if !can_direct_span_projection(columns) {
        return Ok(None);
    }
    let arrays = columns
        .iter()
        .map(|column| -> Result<ArrayRef, LokiApiError> {
            Ok(match column {
                AnalyticsColumn::Timestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(spans.len());
                    for span in spans {
                        builder.append_value(timestamp_i64(span.start_time_unix_nanos)?);
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::EndTimestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(spans.len());
                    for span in spans {
                        if let Some(value) = span.end_time_unix_nanos() {
                            builder.append_value(timestamp_i64(value)?);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::Partition => Arc::new(UInt32Array::from_iter_values(
                    spans
                        .iter()
                        .map(|span| span.record_ref.topic_partition.partition_id.get()),
                )),
                AnalyticsColumn::Offset => Arc::new(UInt64Array::from_iter_values(
                    spans.iter().map(|span| span.record_ref.offset.get()),
                )),
                AnalyticsColumn::Name => {
                    let mut builder = StringBuilder::new();
                    for span in spans {
                        builder.append_value(span.name.as_ref());
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::Kind => Arc::new(Int32Array::from_iter_values(
                    spans.iter().map(|span| span.kind),
                )),
                AnalyticsColumn::DurationNanos => Arc::new(UInt64Array::from_iter_values(
                    spans.iter().map(|span| span.duration_nanos),
                )),
                AnalyticsColumn::StatusCode => Arc::new(Int32Array::from_iter(
                    spans
                        .iter()
                        .map(|span| span.status.as_ref().map(|status| status.code)),
                )),
                _ => unreachable!("direct span projection was validated"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays)
        .map(Some)
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn can_direct_span_projection(columns: &[AnalyticsColumn]) -> bool {
    columns.iter().all(|column| {
        matches!(
            column,
            AnalyticsColumn::Timestamp
                | AnalyticsColumn::EndTimestamp
                | AnalyticsColumn::Partition
                | AnalyticsColumn::Offset
                | AnalyticsColumn::Name
                | AnalyticsColumn::Kind
                | AnalyticsColumn::DurationNanos
                | AnalyticsColumn::StatusCode
        )
    })
}

pub(crate) fn write_direct_metric_rowbinary(
    points: &[DurableMetricPoint],
    columns: &[AnalyticsColumn],
    writer: &mut dyn Write,
) -> Result<bool, LokiApiError> {
    if !can_direct_metric_projection(columns) {
        return Ok(false);
    }
    for point in points {
        for column in columns {
            match column {
                AnalyticsColumn::Timestamp => writer
                    .write_all(&timestamp_i64(point.timestamp_unix_nanos)?.to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::StartTimestamp => {
                    let value = nonzero_timestamp(point.start_time_unix_nanos)?;
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::Partition => writer
                    .write_all(
                        &point
                            .record_ref
                            .topic_partition
                            .partition_id
                            .get()
                            .to_le_bytes(),
                    )
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Offset => writer
                    .write_all(&point.record_ref.offset.get().to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Name => {
                    write_rowbinary_presence(writer, true, true)?;
                    write_rowbinary_bytes(writer, point.identity.name.as_bytes())?;
                }
                AnalyticsColumn::ScalarInteger => {
                    let value = match point.value {
                        MetricValue::Gauge(NumberValue::Integer(value))
                        | MetricValue::Sum(NumberValue::Integer(value)) => Some(value),
                        _ => None,
                    };
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::ScalarDoubleBits => {
                    let value = match point.value {
                        MetricValue::Gauge(NumberValue::DoubleBits(value))
                        | MetricValue::Sum(NumberValue::DoubleBits(value)) => Some(value),
                        _ => None,
                    };
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                _ => unreachable!("direct metric RowBinary projection was validated"),
            }
        }
    }
    Ok(true)
}

pub(crate) fn write_direct_span_rowbinary(
    spans: &[DurableSpan],
    columns: &[AnalyticsColumn],
    writer: &mut dyn Write,
) -> Result<bool, LokiApiError> {
    if !can_direct_span_projection(columns) {
        return Ok(false);
    }
    for span in spans {
        for column in columns {
            match column {
                AnalyticsColumn::Timestamp => writer
                    .write_all(&timestamp_i64(span.start_time_unix_nanos)?.to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::EndTimestamp => {
                    let value = span.end_time_unix_nanos().map(timestamp_i64).transpose()?;
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::Partition => writer
                    .write_all(
                        &span
                            .record_ref
                            .topic_partition
                            .partition_id
                            .get()
                            .to_le_bytes(),
                    )
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Offset => writer
                    .write_all(&span.record_ref.offset.get().to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Name => {
                    write_rowbinary_presence(writer, true, true)?;
                    write_rowbinary_bytes(writer, span.name.as_bytes())?;
                }
                AnalyticsColumn::Kind => {
                    write_rowbinary_presence(writer, true, true)?;
                    writer
                        .write_all(&span.kind.to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
                AnalyticsColumn::DurationNanos => {
                    write_rowbinary_presence(writer, true, true)?;
                    writer
                        .write_all(&span.duration_nanos.to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
                AnalyticsColumn::StatusCode => {
                    let value = span.status.as_ref().map(|status| status.code);
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                _ => unreachable!("direct span RowBinary projection was validated"),
            }
        }
    }
    Ok(true)
}

pub(super) fn column_array(
    rows: &[AnalyticsRow],
    column: AnalyticsColumn,
) -> Result<ArrayRef, LokiApiError> {
    let array: ArrayRef = match column.field().data_type() {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                if let Some(value) = string_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = timestamp_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish().with_timezone("UTC"))
        }
        DataType::UInt32 => {
            let mut builder = UInt32Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = u32_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = u64_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = f64_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = i32_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.scalar_integer {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.monotonic {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Map(_, _) => Arc::new(string_map_array(
            rows.iter().map(|row| map_value(row, column)),
        )?),
        _ => return Err(LokiApiError::internal("unsupported analytics Arrow type")),
    };
    Ok(array)
}

pub(super) fn string_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<&str> {
    match column {
        AnalyticsColumn::Tenant => Some(&row.tenant),
        AnalyticsColumn::Signal => Some(&row.signal),
        AnalyticsColumn::ResourceId => row.resource_id.as_deref(),
        AnalyticsColumn::ScopeId => row.scope_id.as_deref(),
        AnalyticsColumn::TraceId => row.trace_id.as_deref(),
        AnalyticsColumn::SpanId => row.span_id.as_deref(),
        AnalyticsColumn::ParentSpanId => row.parent_span_id.as_deref(),
        AnalyticsColumn::LinkedTraceId => row.linked_trace_id.as_deref(),
        AnalyticsColumn::LinkedSpanId => row.linked_span_id.as_deref(),
        AnalyticsColumn::SeriesId => row.series_id.as_deref(),
        AnalyticsColumn::Message => row.message.as_deref(),
        AnalyticsColumn::BodyJson => row.body_json.as_deref(),
        AnalyticsColumn::Name => row.name.as_deref(),
        AnalyticsColumn::EventName => row.event_name.as_deref(),
        AnalyticsColumn::SeverityText => row.severity_text.as_deref(),
        AnalyticsColumn::StatusMessage => row.status_message.as_deref(),
        AnalyticsColumn::TraceState => row.trace_state.as_deref(),
        AnalyticsColumn::AttributesJson => row.attributes_json.as_deref(),
        AnalyticsColumn::ResourceAttributesJson => row.resource_attributes_json.as_deref(),
        AnalyticsColumn::ScopeAttributesJson => row.scope_attributes_json.as_deref(),
        AnalyticsColumn::EventsJson => row.events_json.as_deref(),
        AnalyticsColumn::LinksJson => row.links_json.as_deref(),
        AnalyticsColumn::Description => row.description.as_deref(),
        AnalyticsColumn::Unit => row.unit.as_deref(),
        AnalyticsColumn::MetricKind => row.metric_kind.as_deref(),
        AnalyticsColumn::ValueType => row.value_type.as_deref(),
        AnalyticsColumn::ValueJson => row.value_json.as_deref(),
        AnalyticsColumn::ExemplarsJson => row.exemplars_json.as_deref(),
        _ => None,
    }
}

pub(super) fn timestamp_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<i64> {
    match column {
        AnalyticsColumn::Timestamp => Some(row.timestamp_unix_nanos),
        AnalyticsColumn::ParentTimestamp => row.parent_timestamp_unix_nanos,
        AnalyticsColumn::ObservedTimestamp => row.observed_timestamp_unix_nanos,
        AnalyticsColumn::StartTimestamp => row.start_timestamp_unix_nanos,
        AnalyticsColumn::EndTimestamp => row.end_timestamp_unix_nanos,
        _ => None,
    }
}

pub(super) fn u32_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<u32> {
    match column {
        AnalyticsColumn::Partition => Some(row.partition),
        AnalyticsColumn::Ordinal => row.ordinal,
        AnalyticsColumn::Flags => row.flags,
        AnalyticsColumn::DroppedAttributesCount => row.dropped_attributes_count,
        AnalyticsColumn::DroppedEventsCount => row.dropped_events_count,
        AnalyticsColumn::DroppedLinksCount => row.dropped_links_count,
        _ => None,
    }
}

pub(super) fn u64_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<u64> {
    match column {
        AnalyticsColumn::Offset => Some(row.offset),
        AnalyticsColumn::DurationNanos => row.duration_nanos,
        AnalyticsColumn::ScalarDoubleBits => row.scalar_double_bits,
        _ => None,
    }
}

pub(super) fn f64_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<f64> {
    match column {
        AnalyticsColumn::Score => row.score,
        _ => None,
    }
}

pub(super) fn i32_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<i32> {
    match column {
        AnalyticsColumn::SeverityNumber => row.severity_number,
        AnalyticsColumn::Kind => row.kind,
        AnalyticsColumn::StatusCode => row.status_code,
        AnalyticsColumn::Temporality => row.temporality,
        _ => None,
    }
}

pub(super) fn map_value(row: &AnalyticsRow, column: AnalyticsColumn) -> &BTreeMap<String, String> {
    match column {
        AnalyticsColumn::Labels => &row.labels,
        AnalyticsColumn::Metadata => &row.metadata,
        AnalyticsColumn::Attributes => &row.attributes,
        AnalyticsColumn::ResourceAttributes => &row.resource_attributes,
        AnalyticsColumn::ScopeAttributes => &row.scope_attributes,
        AnalyticsColumn::AttributeIds => &row.attribute_ids,
        AnalyticsColumn::ResourceAttributeIds => &row.resource_attribute_ids,
        AnalyticsColumn::ScopeAttributeIds => &row.scope_attribute_ids,
        _ => unreachable!("column type checked before map lookup"),
    }
}

pub(super) fn string_map_data_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

pub(super) fn string_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<String, String>>,
) -> Result<arrow_array::MapArray, LokiApiError> {
    let mut builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "keys".into(),
            value: "values".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for values in rows {
        for (key, value) in values {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder
            .append(true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    Ok(builder.finish())
}

pub(super) struct ChannelWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    bytes: Vec<u8>,
    chunk_bytes: usize,
}

impl ChannelWriter {
    pub(super) fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>, chunk_bytes: usize) -> Self {
        Self {
            sender,
            bytes: Vec::with_capacity(chunk_bytes),
            chunk_bytes,
        }
    }

    fn emit(&mut self) -> io::Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::take(&mut self.bytes));
        self.bytes = Vec::with_capacity(self.chunk_bytes);
        self.sender
            .blocking_send(Ok(bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "analytics client disconnected"))
    }

    pub(super) fn finish(&mut self) -> io::Result<()> {
        self.emit()
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        if self.bytes.len() >= self.chunk_bytes {
            self.emit()?;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Arrow's stream writer flushes after the schema and every IPC message.
        // Emitting each flush as its own HTTP body chunk creates a small schema
        // packet followed by a data packet, which can hit the TCP delayed-ACK
        // timer. The size threshold and explicit `finish` retain bounded
        // streaming while coalescing adjacent IPC messages.
        Ok(())
    }
}
