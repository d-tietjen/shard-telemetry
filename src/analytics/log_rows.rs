use super::*;

pub(crate) fn scan_entries(
    entries: Vec<crate::LokiEntry>,
    request: &AnalyticsScanRequest,
    emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
) -> Result<(), LokiApiError> {
    request.validate()?;
    if request.relation != AnalyticsRelation::Logs {
        return Err(LokiApiError::bad_request(
            "the in-memory Loki store exposes only the logs analytical relation",
        ));
    }
    let limit = request.limit.unwrap_or(usize::MAX);
    if let Some(order) = request.order {
        let mut rows = entries
            .into_iter()
            .enumerate()
            .map(|(ordinal, entry)| {
                let mut row = AnalyticsRow::empty(
                    Arc::clone(&request.tenant),
                    "logs",
                    u64::try_from(entry.timestamp_unix_nanos)
                        .map_err(|_| LokiApiError::internal("pre-epoch log timestamp"))?,
                    0,
                    u64::try_from(ordinal).unwrap_or(u64::MAX),
                )?;
                row.message = Some(Arc::from(entry.line));
                row.labels = entry.labels;
                row.metadata = entry.structured_metadata;
                Ok(row)
            })
            .collect::<Result<Vec<_>, LokiApiError>>()?;
        rows.retain(|row| row_matches(row, request));
        if order == AnalyticsScanOrder::RelevanceDescending {
            let scorer = RelevanceScorer::from_request(request);
            for row in &mut rows {
                row.score = Some(scorer.score(row.message.as_deref().unwrap_or_default()));
            }
            rows.sort_unstable_by(|left, right| {
                right
                    .score
                    .unwrap_or_default()
                    .partial_cmp(&left.score.unwrap_or_default())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| right.timestamp_unix_nanos.cmp(&left.timestamp_unix_nanos))
                    .then_with(|| right.offset.cmp(&left.offset))
            });
        } else {
            rows.sort_unstable_by_key(|row| (row.timestamp_unix_nanos, row.offset));
            if order == AnalyticsScanOrder::TimestampDescending {
                rows.reverse();
            }
        }
        rows.truncate(limit);
        for batch in rows.chunks(DEFAULT_SCAN_BATCH_ROWS) {
            emit(batch)?;
        }
        return Ok(());
    }
    let mut rows = Vec::with_capacity(DEFAULT_SCAN_BATCH_ROWS.min(limit));
    let mut emitted = 0usize;
    for (ordinal, entry) in entries.into_iter().enumerate() {
        if emitted == limit {
            break;
        }
        let mut row = AnalyticsRow::empty(
            Arc::clone(&request.tenant),
            "logs",
            u64::try_from(entry.timestamp_unix_nanos)
                .map_err(|_| LokiApiError::internal("pre-epoch log timestamp"))?,
            0,
            u64::try_from(ordinal).unwrap_or(u64::MAX),
        )?;
        row.message = Some(Arc::from(entry.line));
        row.labels = entry.labels;
        row.metadata = entry.structured_metadata;
        if !row_matches(&row, request) {
            continue;
        }
        rows.push(row);
        emitted += 1;
        if rows.len() == DEFAULT_SCAN_BATCH_ROWS {
            emit(&rows)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        emit(&rows)?;
    }
    Ok(())
}

pub(crate) fn log_row(
    tenant: &Arc<str>,
    record: &DurableLog,
    labels: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(tenant),
        "logs",
        record.timestamp_unix_nanos,
        record.record_ref.topic_partition.partition_id.get(),
        record.record_ref.offset.get(),
    )?;
    row.observed_timestamp_unix_nanos = nonzero_timestamp(record.observed_timestamp_unix_nanos)?;
    row.resource_id = Some(Arc::from(record.resource_id().to_string()));
    row.scope_id = Some(Arc::from(record.scope_id().to_string()));
    row.trace_id = record.trace_id.map(|value| Arc::from(value.to_string()));
    row.span_id = record.span_id.map(|value| Arc::from(value.to_string()));
    row.message = Some(Arc::clone(&record.message));
    row.body_json = json(&record.body)?;
    row.event_name = Some(Arc::clone(&record.event_name));
    row.severity_number = Some(record.severity_number);
    row.severity_text = Some(Arc::clone(&record.severity_text));
    row.flags = Some(record.flags);
    row.dropped_attributes_count = Some(record.dropped_attributes_count);
    row.labels = labels;
    row.metadata = metadata;
    populate_attributes(
        &mut row,
        &record.attributes,
        &record.resource.attributes,
        &record.scope.attributes,
    )?;
    Ok(row)
}

/// Returns whether a log projection needs the typed OTLP metadata lane.
/// Storage pushdown has already applied the predicate before the projection
/// is built, so basic columns can avoid materializing omitted OTLP maps and
/// JSON sidecars.
#[must_use]
pub(crate) fn log_columns_need_typed_metadata(columns: &[AnalyticsColumn]) -> bool {
    columns.iter().any(|column| {
        matches!(
            column,
            AnalyticsColumn::ObservedTimestamp
                | AnalyticsColumn::ResourceId
                | AnalyticsColumn::ScopeId
                | AnalyticsColumn::TraceId
                | AnalyticsColumn::SpanId
                | AnalyticsColumn::BodyJson
                | AnalyticsColumn::EventName
                | AnalyticsColumn::SeverityNumber
                | AnalyticsColumn::Flags
                | AnalyticsColumn::DroppedAttributesCount
                | AnalyticsColumn::Attributes
                | AnalyticsColumn::ResourceAttributes
                | AnalyticsColumn::ScopeAttributes
                | AnalyticsColumn::AttributeIds
                | AnalyticsColumn::ResourceAttributeIds
                | AnalyticsColumn::ScopeAttributeIds
                | AnalyticsColumn::AttributesJson
                | AnalyticsColumn::ResourceAttributesJson
                | AnalyticsColumn::ScopeAttributesJson
        )
    })
}

/// Returns whether a log projection needs the normalized structural field lane.
///
/// The lane is separate from typed OTLP metadata because labels and Loki
/// metadata are stored as exact structural fields.
#[must_use]
pub(crate) fn log_columns_need_structural_fields(columns: &[AnalyticsColumn]) -> bool {
    columns
        .iter()
        .any(|column| matches!(column, AnalyticsColumn::Labels | AnalyticsColumn::Metadata))
}

pub(crate) fn projected_log_row(
    tenant: &Arc<str>,
    record: &DurableLog,
    columns: &[AnalyticsColumn],
) -> Result<AnalyticsRow, LokiApiError> {
    if has_only_columns(
        columns,
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::Message,
    ) {
        let mut row = AnalyticsRow::empty(
            Arc::clone(tenant),
            "logs",
            record.timestamp_unix_nanos,
            record.record_ref.topic_partition.partition_id.get(),
            record.record_ref.offset.get(),
        )?;
        row.message = Some(Arc::clone(&record.message));
        return Ok(row);
    }
    let mut row = AnalyticsRow::empty(
        Arc::clone(tenant),
        "logs",
        record.timestamp_unix_nanos,
        record.record_ref.topic_partition.partition_id.get(),
        record.record_ref.offset.get(),
    )?;
    if wants(columns, AnalyticsColumn::ObservedTimestamp) {
        row.observed_timestamp_unix_nanos =
            nonzero_timestamp(record.observed_timestamp_unix_nanos)?;
    }
    if wants(columns, AnalyticsColumn::ResourceId) {
        row.resource_id = Some(Arc::from(record.resource_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::ScopeId) {
        row.scope_id = Some(Arc::from(record.scope_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::TraceId) {
        row.trace_id = record.trace_id.map(|value| Arc::from(value.to_string()));
    }
    if wants(columns, AnalyticsColumn::SpanId) {
        row.span_id = record.span_id.map(|value| Arc::from(value.to_string()));
    }
    if wants(columns, AnalyticsColumn::Message) {
        row.message = Some(Arc::clone(&record.message));
    }
    if wants(columns, AnalyticsColumn::BodyJson) {
        row.body_json = json(&record.body)?;
    }
    if wants(columns, AnalyticsColumn::EventName) {
        row.event_name = Some(Arc::clone(&record.event_name));
    }
    if wants(columns, AnalyticsColumn::SeverityNumber) {
        row.severity_number = Some(record.severity_number);
    }
    if wants(columns, AnalyticsColumn::SeverityText) {
        row.severity_text = Some(Arc::clone(&record.severity_text));
    }
    if wants(columns, AnalyticsColumn::Flags) {
        row.flags = Some(record.flags);
    }
    if wants(columns, AnalyticsColumn::DroppedAttributesCount) {
        row.dropped_attributes_count = Some(record.dropped_attributes_count);
    }
    if wants(columns, AnalyticsColumn::Labels) || wants(columns, AnalyticsColumn::Metadata) {
        for field in record.fields.iter() {
            if wants(columns, AnalyticsColumn::Labels)
                && let Some(name) = field.key.as_ref().strip_prefix("resource.loki.label.")
            {
                row.labels.insert(name.to_owned(), field.value.to_string());
            } else if wants(columns, AnalyticsColumn::Metadata)
                && let Some(name) = field.key.as_ref().strip_prefix("attr.loki.metadata.")
            {
                row.metadata
                    .insert(name.to_owned(), field.value.to_string());
            }
        }
    }
    populate_projected_attributes(
        &mut row,
        columns,
        &record.attributes,
        &record.resource.attributes,
        &record.scope.attributes,
    )?;
    Ok(row)
}
