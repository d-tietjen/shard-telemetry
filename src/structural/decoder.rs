use super::*;

pub(super) fn decode_structural_records_internal(
    encoded: &[u8],
    record_ordinals: &[u32],
    include_typed_metadata: bool,
    cached_typed_metadata: Option<&PackedLogMetadata>,
    cached_embedded_index: Option<&EmbeddedFrameIndex>,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection(
        encoded,
        record_ordinals,
        include_typed_metadata,
        cached_typed_metadata,
        cached_embedded_index,
        FieldProjection::All,
    )
}

pub(super) fn decode_structural_records_internal_with_projection(
    encoded: &[u8],
    record_ordinals: &[u32],
    include_typed_metadata: bool,
    cached_typed_metadata: Option<&PackedLogMetadata>,
    cached_embedded_index: Option<&EmbeddedFrameIndex>,
    field_projection: FieldProjection,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection_and_messages(
        encoded,
        record_ordinals,
        include_typed_metadata,
        cached_typed_metadata,
        cached_embedded_index,
        field_projection,
        None,
        None,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn decode_structural_records_internal_with_projection_and_messages(
    encoded: &[u8],
    record_ordinals: &[u32],
    include_typed_metadata: bool,
    cached_typed_metadata: Option<&PackedLogMetadata>,
    cached_embedded_index: Option<&EmbeddedFrameIndex>,
    field_projection: FieldProjection,
    cached_messages: Option<&[Arc<str>]>,
    cached_templates: Option<&[Vec<Vec<u8>>]>,
    cached_positions: Option<(&[LogicalOffset], &[u64])>,
    cached_attributes: Option<&DecodedAttributeTables>,
    cached_fields: Option<&[Arc<Vec<MetadataField>>]>,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    let offsets_section = read_section(encoded, &mut cursor)?;
    let timestamps_section = read_section(encoded, &mut cursor)?;
    let templates_section = read_section(encoded, &mut cursor)?;
    let bodies_section = read_section(encoded, &mut cursor)?;
    let attributes_section = read_section(encoded, &mut cursor)?;
    let fields_section = read_section(encoded, &mut cursor)?;
    let typed_metadata_section = read_section(encoded, &mut cursor)?;
    let embedded_index_section = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    let embedded_index = match cached_embedded_index {
        Some(index) => Cow::Borrowed(index),
        None => Cow::Owned(EmbeddedFrameIndex::decode(
            embedded_index_section,
            u32::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?,
        )?),
    };
    if embedded_index.record_count as usize != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    let offsets = match cached_positions {
        Some((offsets, _)) if offsets.len() == record_count => Cow::Borrowed(offsets),
        Some(_) => {
            return Err(TelemetryError::InvalidBlockEncoding(
                "cached offset count does not match record count",
            ));
        }
        None => Cow::Owned(decode_offsets(offsets_section, record_count)?),
    };
    let timestamps = match cached_positions {
        Some((_, timestamps)) if timestamps.len() == record_count => Cow::Borrowed(timestamps),
        Some(_) => {
            return Err(TelemetryError::InvalidBlockEncoding(
                "cached timestamp count does not match record count",
            ));
        }
        None => Cow::Owned(decode_timestamps(timestamps_section, record_count)?),
    };
    let messages = if let Some(messages) = cached_messages {
        if messages.len() != record_ordinals.len() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "cached message count does not match selected ordinals",
            ));
        }
        Cow::Borrowed(messages)
    } else {
        let templates = cached_templates
            .map(Cow::Borrowed)
            .map_or_else(|| decode_templates(templates_section).map(Cow::Owned), Ok)?;
        Cow::Owned(decode_selected_bodies(
            bodies_section,
            &templates,
            &embedded_index,
            record_count,
            record_ordinals,
        )?)
    };
    let fields = match field_projection {
        FieldProjection::All => {
            let attributes = match cached_attributes {
                Some(attributes) => Cow::Borrowed(attributes),
                None => Cow::Owned(decode_attribute_tables(attributes_section)?),
            };
            if let Some(cached_fields) = cached_fields {
                if cached_fields.len() != record_ordinals.len() {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "cached field count does not match selected ordinals",
                    ));
                }
                Cow::Borrowed(cached_fields)
            } else {
                Cow::Owned(decode_selected_fields(
                    fields_section,
                    &attributes,
                    record_count,
                    record_ordinals,
                )?)
            }
        }
        FieldProjection::SeverityText => {
            let attributes = match cached_attributes {
                Some(attributes) => Cow::Borrowed(attributes),
                None => Cow::Owned(decode_attribute_tables(attributes_section)?),
            };
            Cow::Owned(decode_selected_fields_for_keys(
                fields_section,
                &attributes,
                record_count,
                record_ordinals,
                &["otel.severity_text", "attr.loki.metadata.severity_text"],
            )?)
        }
        FieldProjection::None => Cow::Owned(projected_empty_metadata_fields(record_ordinals.len())),
    };
    let mut decoded = Vec::with_capacity(record_ordinals.len());
    if include_typed_metadata {
        let typed_metadata = cached_typed_metadata.map_or_else(
            || {
                decode_selected_typed_metadata(
                    typed_metadata_section,
                    record_count,
                    record_ordinals,
                    &timestamps,
                    &messages,
                    &fields,
                )
            },
            |packed| {
                decode_selected_typed_metadata_from_packed(
                    packed,
                    record_count,
                    record_ordinals,
                    &timestamps,
                    &messages,
                    &fields,
                )
            },
        )?;
        for (((record_ordinal, message), fields), metadata) in record_ordinals
            .iter()
            .copied()
            .zip(messages.iter().cloned())
            .zip(fields.iter().cloned())
            .zip(typed_metadata)
        {
            let index = usize::try_from(record_ordinal).map_err(|_| {
                TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
            })?;
            decoded.push(DecodedStructuralRecord {
                offset: offsets[index],
                timestamp_unix_nanos: timestamps[index],
                message,
                fields,
                observed_timestamp_unix_nanos: metadata.observed_timestamp_unix_nanos,
                body: metadata.body,
                attributes: metadata.attributes,
                resource: metadata.resource,
                scope: metadata.scope,
                severity_number: metadata.severity_number,
                severity_text: metadata.severity_text,
                dropped_attributes_count: metadata.dropped_attributes_count,
                flags: metadata.flags,
                trace_id: metadata.trace_id,
                span_id: metadata.span_id,
                event_name: metadata.event_name,
            });
        }
    } else {
        for ((record_ordinal, message), fields) in record_ordinals
            .iter()
            .copied()
            .zip(messages.iter().cloned())
            .zip(fields.iter().cloned())
        {
            let index = usize::try_from(record_ordinal).map_err(|_| {
                TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
            })?;
            decoded.push(DecodedStructuralRecord {
                offset: offsets[index],
                timestamp_unix_nanos: timestamps[index],
                message,
                fields,
                observed_timestamp_unix_nanos: 0,
                body: None,
                attributes: projected_empty_telemetry_attributes(),
                resource: projected_empty_resource_context(),
                scope: projected_empty_scope_context(),
                severity_number: 0,
                severity_text: projected_empty_text(),
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: None,
                span_id: None,
                event_name: projected_empty_text(),
            });
        }
    }
    Ok(decoded)
}

pub(super) fn projected_empty_metadata_fields(count: usize) -> Vec<Arc<Vec<MetadataField>>> {
    static EMPTY: OnceLock<Arc<Vec<MetadataField>>> = OnceLock::new();
    let empty = EMPTY.get_or_init(|| Arc::new(Vec::new()));
    (0..count).map(|_| Arc::clone(empty)).collect()
}

pub(super) fn projected_empty_telemetry_attributes() -> Arc<Vec<TelemetryAttribute>> {
    static EMPTY: OnceLock<Arc<Vec<TelemetryAttribute>>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(Vec::new())))
}

pub(super) fn projected_empty_resource_context() -> Arc<ResourceContext> {
    static EMPTY: OnceLock<Arc<ResourceContext>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(ResourceContext::default())))
}

pub(super) fn projected_empty_scope_context() -> Arc<ScopeContext> {
    static EMPTY: OnceLock<Arc<ScopeContext>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(ScopeContext::default())))
}

pub(super) fn projected_empty_text() -> Arc<str> {
    static EMPTY: OnceLock<Arc<str>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::from("")))
}

/// Decodes only the durable offset and timestamp lanes used to rank selective
/// candidates before reconstructing message and metadata lanes.
pub(crate) fn decode_structural_positions(
    encoded: &[u8],
) -> TelemetryResult<(Vec<LogicalOffset>, Vec<u64>)> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    let offsets_section = read_section(encoded, &mut cursor)?;
    let timestamps_section = read_section(encoded, &mut cursor)?;
    for _ in 0..6 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    Ok((
        decode_offsets(offsets_section, record_count)?,
        decode_timestamps(timestamps_section, record_count)?,
    ))
}

pub(super) fn decode_structural_messages_internal(
    encoded: &[u8],
    record_ordinals: &[u32],
    cached_embedded_index: Option<&EmbeddedFrameIndex>,
    cached_templates: Option<&[Vec<Vec<u8>>]>,
) -> TelemetryResult<Vec<Arc<str>>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let templates_section = read_section(encoded, &mut cursor)?;
    let bodies_section = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let embedded_index_section = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    let embedded_index = match cached_embedded_index {
        Some(index) => Cow::Borrowed(index),
        None => Cow::Owned(EmbeddedFrameIndex::decode(
            embedded_index_section,
            u32::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?,
        )?),
    };
    if embedded_index.record_count as usize != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    let templates = match cached_templates {
        Some(templates) => Cow::Borrowed(templates),
        None => Cow::Owned(decode_templates(templates_section)?),
    };
    decode_selected_bodies(
        bodies_section,
        &templates,
        &embedded_index,
        record_count,
        record_ordinals,
    )
}

pub(crate) fn decode_structural_templates(encoded: &[u8]) -> TelemetryResult<Vec<Vec<Vec<u8>>>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let templates_section = read_section(encoded, &mut cursor)?;
    for _ in 0..5 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    decode_templates(templates_section)
}

pub(crate) fn decode_structural_attribute_tables(
    encoded: &[u8],
) -> TelemetryResult<DecodedAttributeTables> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    for _ in 0..4 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    let attributes_section = read_section(encoded, &mut cursor)?;
    for _ in 0..3 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    decode_attribute_tables(attributes_section)
}

/// Decodes and validates the packed typed metadata lane so query caches can
/// reuse it across projected reads of the same structural frame.
pub(crate) fn decode_structural_typed_metadata(
    encoded: &[u8],
    record_count: usize,
) -> TelemetryResult<(PackedLogMetadata, usize)> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let encoded_record_count = read_usize(encoded, &mut cursor)?;
    if encoded_record_count != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed metadata record count mismatch",
        ));
    }
    for _ in 0..6 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    let typed_metadata_section = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    if typed_metadata_section.is_empty() {
        return Ok((
            PackedLogMetadata {
                bodies: Vec::new(),
                attribute_sets: Vec::new(),
                resources: Vec::new(),
                scopes: Vec::new(),
                strings: Vec::new(),
                rows: vec![None; record_count],
            },
            0,
        ));
    }
    decode_packed_typed_metadata_with_size(typed_metadata_section, record_count)
}

/// Decodes only selected trace IDs from the packed typed metadata lane.
///
/// Most OTLP and Loki records store the trace ID directly in the typed row.
/// Older records can mark the ID as an `otel.trace_id` structural field; only
/// those selected rows fall back to the field lane so this path stays narrow.
pub(crate) fn decode_structural_trace_ids(
    encoded: &[u8],
    record_ordinals: &[u32],
) -> TelemetryResult<Vec<Option<TraceId>>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    for _ in 0..5 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    let _fields_section = read_section(encoded, &mut cursor)?;
    let typed_metadata_section = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    if typed_metadata_section.is_empty() {
        return Ok(vec![None; record_ordinals.len()]);
    }
    let packed = decode_packed_typed_metadata(typed_metadata_section, record_count)?;
    let needs_field_fallback = record_ordinals.iter().any(|ordinal| {
        usize::try_from(*ordinal)
            .ok()
            .and_then(|index| packed.rows.get(index))
            .and_then(Option::as_ref)
            .is_some_and(|row| row.trace_id_from_fields)
    });
    let fields = needs_field_fallback.then(|| decode_structural_fields(encoded, record_ordinals));
    let fields = fields.transpose()?;
    record_ordinals
        .iter()
        .enumerate()
        .map(|(selected_index, ordinal)| {
            let index = usize::try_from(*ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("trace ID ordinal overflow"))?;
            let row = packed
                .rows
                .get(index)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "trace ID ordinal out of range",
                ))?
                .as_ref();
            let Some(row) = row else {
                return Ok(None);
            };
            if row.trace_id_from_fields {
                let fields = fields
                    .as_ref()
                    .and_then(|fields| fields.get(selected_index))
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "trace ID field fallback is missing",
                    ))?;
                Ok(Some(resolve_trace_id_field(fields, "otel.trace_id")?))
            } else {
                Ok(row.trace_id)
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_structural_records_with_cached_frame_data(
    encoded: &[u8],
    record_ordinals: &[u32],
    embedded_index: &EmbeddedFrameIndex,
    templates: &[Vec<Vec<u8>>],
    offsets: &[LogicalOffset],
    timestamps: &[u64],
    include_typed_metadata: bool,
    include_all_fields: bool,
    cached_messages: Option<&[Arc<str>]>,
    packed_typed_metadata: Option<&PackedLogMetadata>,
    cached_attributes: Option<&DecodedAttributeTables>,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection_and_messages(
        encoded,
        record_ordinals,
        include_typed_metadata,
        packed_typed_metadata,
        Some(embedded_index),
        if include_all_fields {
            FieldProjection::All
        } else {
            FieldProjection::None
        },
        cached_messages,
        Some(templates),
        Some((offsets, timestamps)),
        cached_attributes,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_structural_records_with_cached_frame_data_and_fields(
    encoded: &[u8],
    record_ordinals: &[u32],
    embedded_index: &EmbeddedFrameIndex,
    templates: &[Vec<Vec<u8>>],
    offsets: &[LogicalOffset],
    timestamps: &[u64],
    include_typed_metadata: bool,
    include_all_fields: bool,
    cached_messages: Option<&[Arc<str>]>,
    packed_typed_metadata: Option<&PackedLogMetadata>,
    cached_attributes: Option<&DecodedAttributeTables>,
    cached_fields: Option<&[Arc<Vec<MetadataField>>]>,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection_and_messages(
        encoded,
        record_ordinals,
        include_typed_metadata,
        packed_typed_metadata,
        Some(embedded_index),
        if include_all_fields {
            FieldProjection::All
        } else {
            FieldProjection::None
        },
        cached_messages,
        Some(templates),
        Some((offsets, timestamps)),
        cached_attributes,
        cached_fields,
    )
}

/// Decodes only selected metadata fields, leaving typed log metadata and body
/// values compressed until a field predicate has been verified.
pub(crate) fn decode_structural_fields(
    encoded: &[u8],
    record_ordinals: &[u32],
) -> TelemetryResult<Vec<Arc<Vec<MetadataField>>>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let attributes_section = read_section(encoded, &mut cursor)?;
    let fields_section = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    let attributes = decode_attribute_tables(attributes_section)?;
    decode_selected_fields(fields_section, &attributes, record_count, record_ordinals)
}

/// Decodes only the requested metadata fields for selected records.
pub(crate) fn decode_structural_fields_for_keys(
    encoded: &[u8],
    record_ordinals: &[u32],
    wanted_keys: &[&str],
) -> TelemetryResult<Vec<Arc<Vec<MetadataField>>>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let attributes_section = read_section(encoded, &mut cursor)?;
    let fields_section = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    let _ = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    let attributes = decode_attribute_tables(attributes_section)?;
    decode_selected_fields_for_keys(
        fields_section,
        &attributes,
        record_count,
        record_ordinals,
        wanted_keys,
    )
}

/// Returns the structural lanes whose byte vocabulary can benefit from a
/// reusable Zstandard dictionary.
///
/// Offsets and Pco-compressed timestamps are intentionally excluded: their
/// numeric encodings change from block to block and contribute little stable
/// byte vocabulary. The returned slices point at the exact bytes later seen by
/// the enclosing Zstandard frame.
pub(crate) fn dictionary_training_sections(encoded: &[u8]) -> TelemetryResult<[&[u8]; 4]> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let _record_count = read_usize(encoded, &mut cursor)?;
    let _offsets = read_section(encoded, &mut cursor)?;
    let _timestamps = read_section(encoded, &mut cursor)?;
    let templates = read_section(encoded, &mut cursor)?;
    let bodies = read_section(encoded, &mut cursor)?;
    let attribute_tables = read_section(encoded, &mut cursor)?;
    let fields = read_section(encoded, &mut cursor)?;
    let _typed_metadata = read_section(encoded, &mut cursor)?;
    let _embedded_index = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    Ok([templates, bodies, attribute_tables, fields])
}

pub(super) fn decode_typed_metadata(
    encoded: &[u8],
    record_count: usize,
    timestamps: &[u64],
    messages: &[Arc<str>],
    fields: &[Arc<Vec<MetadataField>>],
) -> TelemetryResult<Vec<StructuralLogMetadata>> {
    if timestamps.len() != record_count
        || messages.len() != record_count
        || fields.len() != record_count
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log metadata context count mismatch",
        ));
    }
    if encoded.is_empty() {
        return Ok(vec![StructuralLogMetadata::default(); record_count]);
    }
    let packed = decode_packed_typed_metadata(encoded, record_count)?;
    packed
        .rows
        .iter()
        .zip(timestamps)
        .zip(messages)
        .zip(fields)
        .map(|(((row, timestamp), message), fields)| {
            unpack_typed_metadata(&packed, row.as_ref(), *timestamp, message, fields)
        })
        .collect()
}

pub(super) fn decode_selected_typed_metadata(
    encoded: &[u8],
    record_count: usize,
    selected: &[u32],
    timestamps: &[u64],
    messages: &[Arc<str>],
    fields: &[Arc<Vec<MetadataField>>],
) -> TelemetryResult<Vec<StructuralLogMetadata>> {
    if messages.len() != selected.len()
        || fields.len() != selected.len()
        || timestamps.len() != record_count
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "selected typed log metadata context count mismatch",
        ));
    }
    if encoded.is_empty() {
        return Ok(vec![StructuralLogMetadata::default(); selected.len()]);
    }
    let packed = decode_packed_typed_metadata(encoded, record_count)?;
    decode_selected_typed_metadata_from_packed(
        &packed,
        record_count,
        selected,
        timestamps,
        messages,
        fields,
    )
}

pub(super) fn decode_selected_typed_metadata_from_packed(
    packed: &PackedLogMetadata,
    record_count: usize,
    selected: &[u32],
    timestamps: &[u64],
    messages: &[Arc<str>],
    fields: &[Arc<Vec<MetadataField>>],
) -> TelemetryResult<Vec<StructuralLogMetadata>> {
    if packed.rows.len() != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log metadata count mismatch",
        ));
    }
    selected
        .iter()
        .zip(messages)
        .zip(fields)
        .map(|((ordinal, message), fields)| {
            let index = usize::try_from(*ordinal).map_err(|_| {
                TelemetryError::InvalidBlockEncoding("typed metadata ordinal overflow")
            })?;
            let row = packed
                .rows
                .get(index)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "typed metadata ordinal out of range",
                ))?;
            unpack_typed_metadata(packed, row.as_ref(), timestamps[index], message, fields)
        })
        .collect()
}

pub(super) fn intern_optional_string(
    strings: &mut MetadataInterner<Arc<str>>,
    value: &str,
) -> TelemetryResult<u32> {
    if value.is_empty() {
        return Ok(EMPTY_STRING_ID);
    }
    strings
        .intern(
            value,
            |candidate: &Arc<str>, value| candidate.as_ref() == value,
            |value| Arc::from(value),
        )?
        .checked_add(STRING_DICTIONARY_ID_BASE)
        .ok_or(TelemetryError::RecordTooLarge)
}

pub(super) fn decode_packed_typed_metadata(
    encoded: &[u8],
    record_count: usize,
) -> TelemetryResult<PackedLogMetadata> {
    decode_packed_typed_metadata_with_size(encoded, record_count).map(|(packed, _)| packed)
}

pub(super) fn decode_packed_typed_metadata_with_size(
    encoded: &[u8],
    record_count: usize,
) -> TelemetryResult<(PackedLogMetadata, usize)> {
    if encoded.len() < 4 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated typed log metadata lane",
        ));
    }
    let raw_len = u32::from_le_bytes(encoded[..4].try_into().expect("fixed range")) as usize;
    if raw_len > 64 * 1024 * 1024 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log metadata lane exceeds safety limit",
        ));
    }
    let raw = TYPED_METADATA_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(&encoded[4..], raw_len)
            .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid typed log metadata lane"))
    })?;
    if raw.len() != raw_len {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log metadata length mismatch",
        ));
    }
    let packed: PackedLogMetadata = rmp_serde::from_slice(&raw)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid typed log metadata"))?;
    if packed.rows.len() != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log metadata count mismatch",
        ));
    }
    Ok((packed, raw_len))
}

pub(super) fn unpack_typed_metadata(
    packed: &PackedLogMetadata,
    row: Option<&PackedLogMetadataRow>,
    timestamp: u64,
    message: &Arc<str>,
    fields: &Arc<Vec<MetadataField>>,
) -> TelemetryResult<StructuralLogMetadata> {
    let Some(row) = row else {
        return Ok(StructuralLogMetadata::default());
    };
    let body = match row.body_id {
        ABSENT_LOG_BODY_ID => None,
        MESSAGE_LOG_BODY_ID => Some(TelemetryValue::String(Arc::clone(message))),
        id => Some(resolve_metadata_value(
            &packed.bodies,
            id.checked_sub(LOG_BODY_DICTIONARY_ID_BASE).ok_or(
                TelemetryError::InvalidBlockEncoding("invalid typed log body ID"),
            )?,
            "typed log body",
        )?),
    };
    if row.trace_id_from_fields && row.trace_id.is_some()
        || row.span_id_from_fields && row.span_id.is_some()
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "typed log ID has conflicting sources",
        ));
    }
    let trace_id = if row.trace_id_from_fields {
        Some(resolve_trace_id_field(fields, "otel.trace_id")?)
    } else {
        row.trace_id
    };
    let span_id = if row.span_id_from_fields {
        Some(resolve_span_id_field(fields, "otel.span_id")?)
    } else {
        row.span_id
    };
    Ok(StructuralLogMetadata {
        observed_timestamp_unix_nanos: timestamp.wrapping_add(row.observed_timestamp_delta as u64),
        body,
        attributes: resolve_metadata_value(
            &packed.attribute_sets,
            row.attributes_id,
            "typed log attributes",
        )?,
        resource: resolve_metadata_value(&packed.resources, row.resource_id, "typed log resource")?,
        scope: resolve_metadata_value(&packed.scopes, row.scope_id, "typed log scope")?,
        severity_number: row.severity_number,
        severity_text: resolve_optional_string(
            &packed.strings,
            row.severity_text_id,
            "severity text",
        )?,
        dropped_attributes_count: row.dropped_attributes_count,
        flags: row.flags,
        trace_id,
        span_id,
        event_name: resolve_optional_string(&packed.strings, row.event_name_id, "event name")?,
    })
}

pub(super) fn has_structural_hex_field<R: StructuralRecordView, const N: usize>(
    record: &R,
    key: &str,
    expected: &[u8; N],
) -> bool {
    (0..record.structural_field_count()).any(|index| {
        record
            .structural_field(index)
            .is_some_and(|(field_key, value)| {
                field_key == key && lower_hex_matches(value.as_bytes(), expected)
            })
    })
}

pub(super) fn lower_hex_matches<const N: usize>(encoded: &[u8], expected: &[u8; N]) -> bool {
    encoded.len() == N * 2
        && expected
            .iter()
            .zip(encoded.chunks_exact(2))
            .all(|(byte, pair)| {
                pair[0] == lower_hex_digit(byte >> 4) && pair[1] == lower_hex_digit(byte & 0x0f)
            })
}

const fn lower_hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        _ => b'a' + value - 10,
    }
}

pub(super) fn resolve_trace_id_field(
    fields: &[MetadataField],
    key: &'static str,
) -> TelemetryResult<TraceId> {
    let bytes = resolve_hex_field::<16>(fields, key)?;
    TraceId::from_bytes(bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid typed log trace ID field"))
}

pub(super) fn resolve_span_id_field(
    fields: &[MetadataField],
    key: &'static str,
) -> TelemetryResult<SpanId> {
    let bytes = resolve_hex_field::<8>(fields, key)?;
    SpanId::from_bytes(bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid typed log span ID field"))
}

pub(super) fn resolve_hex_field<const N: usize>(
    fields: &[MetadataField],
    key: &'static str,
) -> TelemetryResult<[u8; N]> {
    for value in fields
        .iter()
        .filter(|field| field.key.as_ref() == key && field.value.len() == N * 2)
    {
        let mut decoded = [0; N];
        let valid = decoded
            .iter_mut()
            .zip(value.value.as_bytes().chunks_exact(2))
            .all(|(byte, pair)| {
                let Some(high) = decode_lower_hex_digit(pair[0]) else {
                    return false;
                };
                let Some(low) = decode_lower_hex_digit(pair[1]) else {
                    return false;
                };
                *byte = high * 16 + low;
                true
            });
        if valid {
            return Ok(decoded);
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "missing or invalid typed log ID field",
    ))
}

const fn decode_lower_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

pub(super) fn resolve_metadata_value<T: Clone>(
    values: &[T],
    id: u32,
    label: &'static str,
) -> TelemetryResult<T> {
    values
        .get(usize::try_from(id).map_err(|_| TelemetryError::InvalidBlockEncoding(label))?)
        .cloned()
        .ok_or(TelemetryError::InvalidBlockEncoding(label))
}

pub(super) fn resolve_optional_string(
    strings: &[Arc<str>],
    id: u32,
    label: &'static str,
) -> TelemetryResult<Arc<str>> {
    if id == EMPTY_STRING_ID {
        return Ok(Arc::from(""));
    }
    resolve_metadata_value(
        strings,
        id.checked_sub(STRING_DICTIONARY_ID_BASE)
            .ok_or(TelemetryError::InvalidBlockEncoding(label))?,
        label,
    )
}

pub(super) fn hash_field_id_pairs(pairs: &[(u32, u32)]) -> u64 {
    let mut hash = (pairs.len() as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
    for (key_id, value_id) in pairs {
        let pair = (u64::from(*key_id) << u32::BITS) | u64::from(*value_id);
        hash ^= pair.wrapping_mul(0x517c_c1b7_2722_0a95);
        hash = hash.rotate_left(23).wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    hash ^ (hash >> 31)
}

pub(super) fn decode_offsets(
    encoded: &[u8],
    record_count: usize,
) -> TelemetryResult<Vec<LogicalOffset>> {
    if record_count == 0 {
        if !encoded.is_empty() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "offsets for empty block",
            ));
        }
        return Ok(Vec::new());
    }
    let mut cursor = 0usize;
    let mut previous = read_varint(encoded, &mut cursor)?;
    let mut offsets = Vec::with_capacity(record_count);
    offsets.push(LogicalOffset::new(previous));
    for _ in 1..record_count {
        let delta = read_varint(encoded, &mut cursor)?;
        previous = previous
            .checked_add(delta)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "offset delta overflow",
            ))?;
        offsets.push(LogicalOffset::new(previous));
    }
    require_consumed(encoded, cursor)?;
    Ok(offsets)
}

pub(super) fn decode_timestamps(encoded: &[u8], record_count: usize) -> TelemetryResult<Vec<u64>> {
    if record_count == 0 {
        if !encoded.is_empty() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "timestamps for empty block",
            ));
        }
        return Ok(Vec::new());
    }
    let mut timestamps = vec![0; record_count];
    let progress = simple_decompress_into(encoded, &mut timestamps)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid Pco timestamp section"))?;
    if progress.n_processed != record_count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "Pco timestamp count mismatch",
        ));
    }
    Ok(timestamps)
}

pub(super) fn decode_templates(encoded: &[u8]) -> TelemetryResult<Vec<Vec<Vec<u8>>>> {
    let mut cursor = 0usize;
    let count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        count,
        encoded.len().saturating_sub(cursor),
        "template count",
    )?;
    let mut templates = Vec::with_capacity(count);
    for _ in 0..count {
        let literal_count = read_usize(encoded, &mut cursor)?;
        if literal_count == 0 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "template has no literals",
            ));
        }
        ensure_count_within(
            literal_count,
            encoded.len().saturating_sub(cursor),
            "template literal count",
        )?;
        let mut literals = Vec::with_capacity(literal_count);
        for _ in 0..literal_count {
            literals.push(read_bytes(encoded, &mut cursor)?.to_vec());
        }
        templates.push(literals);
    }
    require_consumed(encoded, cursor)?;
    Ok(templates)
}

pub(super) fn decode_bodies(
    encoded: &[u8],
    templates: &[Vec<Vec<u8>>],
    index: &EmbeddedFrameIndex,
    record_count: usize,
) -> TelemetryResult<Vec<Arc<str>>> {
    validate_body_layout(index, templates.len(), record_count)?;
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut cursor = 0usize;
    let mut messages = Vec::with_capacity(record_count);
    let mut exact_templates = vec![None::<Arc<str>>; templates.len()];
    let mut previous = None::<Arc<str>>;
    let mut previous_encoded = None::<Range<usize>>;
    let mut previous_template_id = None::<usize>;
    for record_ordinal in 0..record_count {
        validate_checkpoint_cursor(&lane, record_ordinal, cursor)?;
        let record_start = cursor;
        let template_id = body_template_id(index, record_ordinal, templates.len())?;
        let message = match template_id {
            None => {
                let bytes = read_bytes(lane.payload, &mut cursor)?;
                if let Some(previous) = &previous
                    && previous.as_bytes() == bytes
                {
                    Arc::clone(previous)
                } else {
                    decode_text(bytes.to_vec())?
                }
            }
            Some(template_id) => {
                let literals = templates
                    .get(template_id)
                    .ok_or(TelemetryError::InvalidBlockEncoding("unknown template ID"))?;
                if literals.len() == 1 {
                    if let Some(message) = &exact_templates[template_id] {
                        Arc::clone(message)
                    } else {
                        let message = decode_text(literals.first().cloned().ok_or(
                            TelemetryError::InvalidBlockEncoding("template has no first literal"),
                        )?)?;
                        exact_templates[template_id] = Some(Arc::clone(&message));
                        message
                    }
                } else {
                    for _ in &literals[1..] {
                        let _ = read_bytes(lane.payload, &mut cursor)?;
                    }
                    let record_end = cursor;
                    if let (Some(previous), Some(previous_encoded)) = (&previous, &previous_encoded)
                        && previous_template_id == Some(template_id)
                        && lane.payload[previous_encoded.clone()]
                            == lane.payload[record_start..record_end]
                    {
                        Arc::clone(previous)
                    } else {
                        let mut replay = record_start;
                        let mut reconstructed = literals.first().cloned().ok_or(
                            TelemetryError::InvalidBlockEncoding("template has no first literal"),
                        )?;
                        for literal in &literals[1..] {
                            reconstructed.extend_from_slice(read_bytes(lane.payload, &mut replay)?);
                            reconstructed.extend_from_slice(literal);
                        }
                        debug_assert_eq!(replay, record_end);
                        decode_text(reconstructed)?
                    }
                }
            }
        };
        previous = Some(Arc::clone(&message));
        previous_encoded = Some(record_start..cursor);
        previous_template_id = template_id;
        messages.push(message);
    }
    require_consumed(lane.payload, cursor)?;
    Ok(messages)
}

pub(super) fn decode_selected_bodies(
    encoded: &[u8],
    templates: &[Vec<Vec<u8>>],
    index: &EmbeddedFrameIndex,
    record_count: usize,
    selected: &[u32],
) -> TelemetryResult<Vec<Arc<str>>> {
    validate_body_layout(index, templates.len(), record_count)?;
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let template_fixed_bytes = templates
        .iter()
        .map(|literals| {
            literals.iter().try_fold(0usize, |total, literal| {
                total
                    .checked_add(literal.len())
                    .ok_or(TelemetryError::RecordTooLarge)
            })
        })
        .collect::<TelemetryResult<Vec<_>>>()?;
    let mut exact_templates = vec![None::<Arc<str>>; templates.len()];
    let mut selected_index = 0usize;
    let mut messages = Vec::with_capacity(selected.len());
    while selected_index < selected.len() {
        let first_ordinal = usize::try_from(selected[selected_index]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint = first_ordinal / lane.interval;
        let checkpoint_end = (checkpoint + 1)
            .saturating_mul(lane.interval)
            .min(record_count);
        let mut group_end = selected_index + 1;
        while group_end < selected.len()
            && usize::try_from(selected[group_end])
                .ok()
                .is_some_and(|ordinal| ordinal < checkpoint_end)
        {
            group_end += 1;
        }
        let final_ordinal = usize::try_from(selected[group_end - 1]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint_payload = lane.checkpoint_payload(checkpoint)?;
        let mut cursor = 0usize;
        let mut retained = selected_index;
        for record_ordinal in checkpoint * lane.interval..=final_ordinal {
            let retain = usize::try_from(selected[retained])
                .ok()
                .is_some_and(|selected| selected == record_ordinal);
            match body_template_id(index, record_ordinal, templates.len())? {
                None => {
                    let bytes = read_bytes(checkpoint_payload, &mut cursor)?;
                    if retain {
                        messages.push(decode_text(bytes.to_vec())?);
                    }
                }
                Some(template_id) => {
                    let literals = templates
                        .get(template_id)
                        .ok_or(TelemetryError::InvalidBlockEncoding("unknown template ID"))?;
                    let first = literals
                        .first()
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "template has no first literal",
                        ))?;
                    if retain {
                        if literals.len() == 1 {
                            let message = if let Some(message) = &exact_templates[template_id] {
                                Arc::clone(message)
                            } else {
                                let message = decode_text(first.clone())?;
                                exact_templates[template_id] = Some(Arc::clone(&message));
                                message
                            };
                            messages.push(message);
                        } else {
                            let mut reconstructed =
                                Vec::with_capacity(template_fixed_bytes[template_id]);
                            reconstructed.extend_from_slice(first);
                            for literal in &literals[1..] {
                                reconstructed.extend_from_slice(read_bytes(
                                    checkpoint_payload,
                                    &mut cursor,
                                )?);
                                reconstructed.extend_from_slice(literal);
                            }
                            messages.push(decode_text(reconstructed)?);
                        }
                    } else {
                        for _ in &literals[1..] {
                            let _ = read_bytes(checkpoint_payload, &mut cursor)?;
                        }
                    }
                }
            }
            if retain {
                retained += 1;
            }
        }
        if final_ordinal + 1 == checkpoint_end {
            require_consumed(checkpoint_payload, cursor)?;
        }
        selected_index = group_end;
    }
    Ok(messages)
}

pub(super) fn validate_body_layout(
    index: &EmbeddedFrameIndex,
    template_count: usize,
    record_count: usize,
) -> TelemetryResult<()> {
    let expected_layout_count = if record_count == 0 {
        0
    } else {
        u32::try_from(template_count)
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .checked_add(1)
            .ok_or(TelemetryError::RecordTooLarge)?
    };
    if index.record_count as usize != record_count || index.layout_count != expected_layout_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "body layout dictionary disagrees with templates",
        ));
    }
    Ok(())
}

pub(super) fn body_template_id(
    index: &EmbeddedFrameIndex,
    record_ordinal: usize,
    template_count: usize,
) -> TelemetryResult<Option<usize>> {
    let ordinal = u32::try_from(record_ordinal).map_err(|_| TelemetryError::RecordTooLarge)?;
    let layout_id = packed_id(&index.layout_ids, ordinal) as usize;
    if layout_id < template_count {
        Ok(Some(layout_id))
    } else if layout_id == template_count {
        Ok(None)
    } else {
        Err(TelemetryError::InvalidBlockEncoding(
            "body layout ID exceeds templates",
        ))
    }
}

pub(super) fn decode_attribute_tables(encoded: &[u8]) -> TelemetryResult<DecodedAttributeTables> {
    let mut cursor = 0usize;
    let key_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        key_count,
        encoded.len().saturating_sub(cursor),
        "attribute key count",
    )?;
    let mut keys = Vec::with_capacity(key_count);
    let mut values = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        keys.push(decode_text(read_bytes(encoded, &mut cursor)?.to_vec())?);
        let value_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            value_count,
            encoded.len().saturating_sub(cursor),
            "attribute value count",
        )?;
        let mut dictionary = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            dictionary.push(decode_text(read_bytes(encoded, &mut cursor)?.to_vec())?);
        }
        values.push(dictionary);
    }
    require_consumed(encoded, cursor)?;
    Ok((keys, values))
}

pub(super) fn decode_fields(
    encoded: &[u8],
    tables: &DecodedAttributeTables,
    record_count: usize,
) -> TelemetryResult<Vec<Arc<Vec<MetadataField>>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut cursor = 0usize;
    let mut records = Vec::with_capacity(record_count);
    let mut previous = None::<Arc<Vec<MetadataField>>>;
    for record_ordinal in 0..record_count {
        validate_checkpoint_cursor(&lane, record_ordinal, cursor)?;
        let field_count = read_usize(lane.payload, &mut cursor)?;
        ensure_count_within(
            field_count,
            lane.payload.len().saturating_sub(cursor),
            "field count",
        )?;
        let mut fields = if previous
            .as_ref()
            .is_some_and(|previous| previous.len() == field_count)
        {
            None
        } else {
            Some(Vec::with_capacity(field_count))
        };
        for field_index in 0..field_count {
            let key_id = read_usize(lane.payload, &mut cursor)?;
            let key = tables
                .0
                .get(key_id)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "unknown attribute key ID",
                ))?;
            match read_byte(lane.payload, &mut cursor)? {
                DIRECT_ATTRIBUTE_VALUE => {
                    let bytes = read_bytes(lane.payload, &mut cursor)?;
                    let value = std::str::from_utf8(bytes)
                        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid UTF-8 text"))?;
                    if fields.is_none()
                        && previous.as_ref().is_some_and(|previous| {
                            previous[field_index].key.as_ref() == key.as_ref()
                                && previous[field_index].value.as_ref() == value
                        })
                    {
                        continue;
                    }
                    if fields.is_none() {
                        let mut changed = Vec::with_capacity(field_count);
                        changed.extend_from_slice(
                            &previous.as_ref().expect("equal field count")[..field_index],
                        );
                        fields = Some(changed);
                    }
                    fields
                        .as_mut()
                        .expect("changed fields are materialized")
                        .push(MetadataField {
                            key: Arc::clone(key),
                            value: Arc::from(value),
                        });
                }
                DICTIONARY_ATTRIBUTE_VALUE => {
                    let value_id = read_usize(lane.payload, &mut cursor)?;
                    let value = tables
                        .1
                        .get(key_id)
                        .and_then(|values| values.get(value_id))
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "unknown attribute value dictionary ID",
                        ))?;
                    if fields.is_none()
                        && previous.as_ref().is_some_and(|previous| {
                            previous[field_index].key.as_ref() == key.as_ref()
                                && previous[field_index].value.as_ref() == value.as_ref()
                        })
                    {
                        continue;
                    }
                    if fields.is_none() {
                        let mut changed = Vec::with_capacity(field_count);
                        changed.extend_from_slice(
                            &previous.as_ref().expect("equal field count")[..field_index],
                        );
                        fields = Some(changed);
                    }
                    fields
                        .as_mut()
                        .expect("changed fields are materialized")
                        .push(MetadataField {
                            key: Arc::clone(key),
                            value: Arc::clone(value),
                        });
                }
                _ => {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid attribute value kind",
                    ));
                }
            }
        }
        let fields = match fields {
            Some(fields) => Arc::new(fields),
            None => Arc::clone(
                previous
                    .as_ref()
                    .expect("equal field count has a prior record"),
            ),
        };
        previous = Some(Arc::clone(&fields));
        records.push(fields);
    }
    require_consumed(lane.payload, cursor)?;
    Ok(records)
}

pub(super) fn decode_selected_fields(
    encoded: &[u8],
    tables: &DecodedAttributeTables,
    record_count: usize,
    selected: &[u32],
) -> TelemetryResult<Vec<Arc<Vec<MetadataField>>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut selected_index = 0usize;
    let mut records = Vec::with_capacity(selected.len());
    while selected_index < selected.len() {
        let first_ordinal = usize::try_from(selected[selected_index]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint = first_ordinal / lane.interval;
        let checkpoint_end = (checkpoint + 1)
            .saturating_mul(lane.interval)
            .min(record_count);
        let mut group_end = selected_index + 1;
        while group_end < selected.len()
            && usize::try_from(selected[group_end])
                .ok()
                .is_some_and(|ordinal| ordinal < checkpoint_end)
        {
            group_end += 1;
        }
        let final_ordinal = usize::try_from(selected[group_end - 1]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint_payload = lane.checkpoint_payload(checkpoint)?;
        let mut cursor = 0usize;
        let mut retained = selected_index;
        let mut next_selected = usize::try_from(selected[retained]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        for record_ordinal in checkpoint * lane.interval..=final_ordinal {
            let retain = next_selected == record_ordinal;
            let field_count = read_usize(checkpoint_payload, &mut cursor)?;
            ensure_count_within(
                field_count,
                checkpoint_payload.len().saturating_sub(cursor),
                "field count",
            )?;
            let mut fields = retain.then(|| Vec::with_capacity(field_count));
            for _ in 0..field_count {
                let key_id = read_usize(checkpoint_payload, &mut cursor)?;
                let key = tables
                    .0
                    .get(key_id)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "unknown attribute key ID",
                    ))?;
                let value = match read_byte(checkpoint_payload, &mut cursor)? {
                    DIRECT_ATTRIBUTE_VALUE => {
                        let bytes = read_bytes(checkpoint_payload, &mut cursor)?;
                        if retain {
                            Some(decode_text(bytes.to_vec())?)
                        } else {
                            None
                        }
                    }
                    DICTIONARY_ATTRIBUTE_VALUE => {
                        let value_id = read_usize(checkpoint_payload, &mut cursor)?;
                        let value = tables
                            .1
                            .get(key_id)
                            .and_then(|values| values.get(value_id))
                            .ok_or(TelemetryError::InvalidBlockEncoding(
                                "unknown attribute value dictionary ID",
                            ))?;
                        retain.then(|| Arc::clone(value))
                    }
                    _ => {
                        return Err(TelemetryError::InvalidBlockEncoding(
                            "invalid attribute value kind",
                        ));
                    }
                };
                if let Some(fields) = &mut fields {
                    fields.push(MetadataField {
                        key: Arc::clone(key),
                        value: value.expect("retained field has a decoded value"),
                    });
                }
            }
            if let Some(fields) = fields {
                records.push(Arc::new(fields));
                retained += 1;
                if retained < group_end {
                    next_selected = usize::try_from(selected[retained]).map_err(|_| {
                        TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
                    })?;
                }
            }
        }
        if final_ordinal + 1 == checkpoint_end {
            require_consumed(checkpoint_payload, cursor)?;
        }
        selected_index = group_end;
    }
    Ok(records)
}

pub(super) fn decode_selected_fields_for_keys(
    encoded: &[u8],
    tables: &DecodedAttributeTables,
    record_count: usize,
    selected: &[u32],
    wanted_keys: &[&str],
) -> TelemetryResult<Vec<Arc<Vec<MetadataField>>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut selected_index = 0usize;
    let mut records = Vec::with_capacity(selected.len());
    while selected_index < selected.len() {
        let first_ordinal = usize::try_from(selected[selected_index]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint = first_ordinal / lane.interval;
        let checkpoint_end = (checkpoint + 1)
            .saturating_mul(lane.interval)
            .min(record_count);
        let mut group_end = selected_index + 1;
        while group_end < selected.len()
            && usize::try_from(selected[group_end])
                .ok()
                .is_some_and(|ordinal| ordinal < checkpoint_end)
        {
            group_end += 1;
        }
        let final_ordinal = usize::try_from(selected[group_end - 1]).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        let checkpoint_payload = lane.checkpoint_payload(checkpoint)?;
        let mut cursor = 0usize;
        let mut retained = selected_index;
        for record_ordinal in checkpoint * lane.interval..=final_ordinal {
            let retain = usize::try_from(selected[retained])
                .ok()
                .is_some_and(|selected| selected == record_ordinal);
            let field_count = read_usize(checkpoint_payload, &mut cursor)?;
            ensure_count_within(
                field_count,
                checkpoint_payload.len().saturating_sub(cursor),
                "field count",
            )?;
            let mut fields = retain.then(Vec::new);
            for _ in 0..field_count {
                let key_id = read_usize(checkpoint_payload, &mut cursor)?;
                let key = tables
                    .0
                    .get(key_id)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "unknown attribute key ID",
                    ))?;
                let wanted = wanted_keys.iter().any(|wanted| *wanted == key.as_ref());
                let value =
                    match read_byte(checkpoint_payload, &mut cursor)? {
                        DIRECT_ATTRIBUTE_VALUE => {
                            let bytes = read_bytes(checkpoint_payload, &mut cursor)?;
                            wanted.then(|| decode_text(bytes.to_vec())).transpose()?
                        }
                        DICTIONARY_ATTRIBUTE_VALUE => {
                            let value_id = read_usize(checkpoint_payload, &mut cursor)?;
                            let values = tables.1.get(key_id).ok_or(
                                TelemetryError::InvalidBlockEncoding(
                                    "unknown attribute value dictionary ID",
                                ),
                            )?;
                            if wanted {
                                Some(Arc::clone(values.get(value_id).ok_or(
                                    TelemetryError::InvalidBlockEncoding(
                                        "unknown attribute value dictionary ID",
                                    ),
                                )?))
                            } else {
                                if value_id >= values.len() {
                                    return Err(TelemetryError::InvalidBlockEncoding(
                                        "unknown attribute value dictionary ID",
                                    ));
                                }
                                None
                            }
                        }
                        _ => {
                            return Err(TelemetryError::InvalidBlockEncoding(
                                "invalid attribute value kind",
                            ));
                        }
                    };
                if let (Some(fields), Some(value)) = (&mut fields, value) {
                    fields.push(MetadataField {
                        key: Arc::clone(key),
                        value,
                    });
                }
            }
            if let Some(fields) = fields {
                records.push(Arc::new(fields));
                retained += 1;
            }
        }
        if final_ordinal + 1 == checkpoint_end {
            require_consumed(checkpoint_payload, cursor)?;
        }
        selected_index = group_end;
    }
    Ok(records)
}
