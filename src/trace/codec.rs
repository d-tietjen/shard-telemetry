use super::*;

pub(super) fn estimated_span_attributes_bytes(attributes: &Arc<Vec<TelemetryAttribute>>) -> usize {
    estimated_arc_vec_storage::<TelemetryAttribute>(attributes.capacity()).saturating_add(
        attributes
            .iter()
            .map(estimated_telemetry_attribute_bytes)
            .sum(),
    )
}

pub(super) fn estimated_span_events_bytes(events: &Arc<Vec<SpanEvent>>) -> usize {
    estimated_arc_vec_storage::<SpanEvent>(events.capacity()).saturating_add(
        events
            .iter()
            .map(|event| {
                size_of::<SpanEvent>()
                    .saturating_add(estimated_arc_str_bytes(&event.name))
                    .saturating_add(estimated_span_attributes_bytes(&event.attributes))
            })
            .sum(),
    )
}

pub(super) fn estimated_span_links_bytes(links: &Arc<Vec<SpanLink>>) -> usize {
    estimated_arc_vec_storage::<SpanLink>(links.capacity()).saturating_add(
        links
            .iter()
            .map(|link| {
                size_of::<SpanLink>()
                    .saturating_add(estimated_arc_str_bytes(&link.trace_state))
                    .saturating_add(estimated_span_attributes_bytes(&link.attributes))
            })
            .sum(),
    )
}

/// Lightweight span result used when an analytical projection does not need
/// the full resource, scope, attributes, events, or links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceProjection {
    /// Durable record identity.
    pub(crate) record_ref: TelemetryRecordRef,
    /// Trace identity used for deterministic result ordering.
    pub(crate) trace_id: TraceId,
    /// Span start timestamp.
    pub(crate) start_time_unix_nanos: u64,
    /// Span duration.
    pub(crate) duration_nanos: u64,
    /// Span operation name.
    pub(crate) name: Arc<str>,
    /// Span kind.
    pub(crate) kind: i32,
    /// Optional final status code.
    pub(crate) status_code: Option<i32>,
}

impl TraceProjection {
    pub(crate) fn from_span(span: &DurableSpan) -> Self {
        Self {
            record_ref: span.record_ref,
            trace_id: span.trace_id,
            start_time_unix_nanos: span.start_time_unix_nanos,
            duration_nanos: span.duration_nanos,
            name: Arc::clone(&span.name),
            kind: span.kind,
            status_code: span.status.as_ref().map(|status| status.code),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PackedSpanSidecar {
    tenant_id: u32,
    resource_id: u32,
    scope_id: u32,
    trace_state_id: u32,
    flags: u32,
    name_id: u32,
    kind: i32,
    attributes_id: u32,
    dropped_attributes_count: u32,
    events_id: u32,
    dropped_events_count: u32,
    links_id: u32,
    dropped_links_count: u32,
    status_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TraceBlockSidecars {
    tenants: Vec<Arc<str>>,
    resources: Vec<Arc<ResourceContext>>,
    scopes: Vec<Arc<ScopeContext>>,
    trace_states: Vec<Arc<str>>,
    names: Vec<Arc<str>>,
    attribute_sets: Vec<Arc<Vec<TelemetryAttribute>>>,
    event_sets: Vec<Arc<Vec<SpanEvent>>>,
    link_sets: Vec<Arc<Vec<SpanLink>>>,
    statuses: Vec<Option<SpanStatus>>,
    spans: Vec<PackedSpanSidecar>,
}

/// Encodes one partition's spans into the signal-native columnar trace block.
///
/// Records are sorted by trace ID, start time, and durable offset. Offsets
/// remain the durable identity and are reconstructed exactly during decode.
pub fn encode_trace_block(records: &[DurableSpan]) -> TelemetryResult<Vec<u8>> {
    let Some(first) = records.first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block must contain at least one span",
        ));
    };
    let partition = first.record_ref.topic_partition;
    let stream_shard_id = first.stream_shard_id;
    if records.iter().any(|record| {
        record.record_ref.signal != TelemetrySignal::Traces
            || record.record_ref.topic_partition != partition
            || record.stream_shard_id != stream_shard_id
    }) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block records do not share a trace partition and owner",
        ));
    }
    let mut sorted = records.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|record| {
        (
            record.trace_id,
            record.start_time_unix_nanos,
            record.record_ref.offset,
        )
    });
    let offsets = sorted
        .iter()
        .map(|record| record.record_ref.offset.get())
        .collect::<Vec<_>>();
    let starts = sorted
        .iter()
        .map(|record| record.start_time_unix_nanos)
        .collect::<Vec<_>>();
    let durations = sorted
        .iter()
        .map(|record| record.duration_nanos)
        .collect::<Vec<_>>();
    let id_lane = encode_span_ids(&sorted)?;
    let sidecars = encode_trace_sidecars(&sorted)?;
    let sidecar_bytes = rmp_serde::to_vec(&sidecars)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed_sidecars = TRACE_COMPRESSOR.with_borrow_mut(|compressor| {
        compressor
            .compress(&sidecar_bytes)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&TRACE_BLOCK_MAGIC);
    encoded.push(TRACE_BLOCK_VERSION);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&stream_shard_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.topic_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.partition_id.get().to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(sorted.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    for section in [
        compress_u64(&offsets)?,
        compress_u64(&starts)?,
        compress_u64(&durations)?,
        id_lane,
        compressed_sidecars,
    ] {
        append_section(&mut encoded, &section)?;
    }
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes and verifies a signal-native trace block.
pub fn decode_trace_block(encoded: &[u8]) -> TelemetryResult<Vec<DurableSpan>> {
    decode_trace_block_filtered(encoded, None)
}

/// Decodes only spans which can satisfy a native trace query while preserving
/// relevant-block integrity validation and exact conflict resolution.
pub(crate) fn decode_trace_block_matching(
    encoded: &[u8],
    query: &TraceQuery,
) -> TelemetryResult<Vec<DurableSpan>> {
    decode_trace_block_filtered(encoded, Some(query))
}

pub(super) fn decode_trace_block_filtered(
    encoded: &[u8],
    query: Option<&TraceQuery>,
) -> TelemetryResult<Vec<DurableSpan>> {
    const FIXED_HEADER: usize = 36;
    if encoded.len() < FIXED_HEADER + 32 || encoded[..4] != TRACE_BLOCK_MAGIC {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing trace block header",
        ));
    }
    if encoded[4] != TRACE_BLOCK_VERSION || encoded[5..8] != [0, 0, 0] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "unsupported trace block version or flags",
        ));
    }
    let payload_end = encoded.len() - 32;
    let stream_shard_id = ShardId::new(u32::from_le_bytes(
        encoded[8..12].try_into().expect("fixed range"),
    ));
    let topic_partition = TopicPartition::new(
        TopicId::new(u128::from_le_bytes(
            encoded[12..28].try_into().expect("fixed range"),
        )),
        LogicalPartitionId::new(u32::from_le_bytes(
            encoded[28..32].try_into().expect("fixed range"),
        )),
    );
    // Partition ownership is in the fixed header. Recovery validates every
    // authoritative block before it becomes query-visible, so a
    // partition-local scan can reject unrelated blocks without hashing,
    // decompressing, or materializing their column lanes again.
    if query.is_some_and(|query| {
        query
            .partition
            .is_some_and(|partition| partition != topic_partition)
    }) {
        return Ok(Vec::new());
    }
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block checksum mismatch",
        ));
    }
    let count = u32::from_le_bytes(encoded[32..36].try_into().expect("fixed range")) as usize;
    if count == 0 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block has no spans",
        ));
    }
    let mut cursor = FIXED_HEADER;
    let offsets = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let starts = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let durations = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let ids = decode_span_ids(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let compressed_sidecars = read_section(encoded, &mut cursor, payload_end)?;
    if cursor != payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing trace block sections",
        ));
    }
    let sidecar_bytes = TRACE_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(compressed_sidecars, 256 * 1024 * 1024)
            .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace sidecar compression"))
    })?;
    let sidecars: TraceBlockSidecars = rmp_serde::from_slice(&sidecar_bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace sidecars"))?;
    if sidecars.spans.len() != count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace sidecar count mismatch",
        ));
    }
    let mut decoded = Vec::with_capacity(query.map_or(count, |query| query.limit.min(count)));
    for (ordinal, sidecar) in sidecars.spans.iter().enumerate() {
        let tenant = trace_sidecar(&sidecars.tenants, sidecar.tenant_id, "tenant")?;
        let resource = trace_sidecar(&sidecars.resources, sidecar.resource_id, "resource")?;
        let scope = trace_sidecar(&sidecars.scopes, sidecar.scope_id, "scope")?;
        let _ = trace_sidecar(
            &sidecars.trace_states,
            sidecar.trace_state_id,
            "trace state",
        )?;
        let name = trace_sidecar(&sidecars.names, sidecar.name_id, "name")?;
        let attributes = trace_sidecar(
            &sidecars.attribute_sets,
            sidecar.attributes_id,
            "attributes",
        )?;
        let _ = trace_sidecar(&sidecars.event_sets, sidecar.events_id, "events")?;
        let _ = trace_sidecar(&sidecars.link_sets, sidecar.links_id, "links")?;
        let _ = trace_sidecar(&sidecars.statuses, sidecar.status_id, "status")?;
        let (trace_id, span_id, _) = ids[ordinal];
        let start_time_unix_nanos = starts[ordinal];
        let duration_nanos = durations[ordinal];
        if query.is_some_and(|query| {
            tenant.as_ref() != query.tenant.as_ref()
                || query
                    .partition
                    .is_some_and(|partition| partition != topic_partition)
                || query.trace_id.is_some_and(|value| value != trace_id)
                || query.span_id.is_some_and(|value| value != span_id)
                || query
                    .name
                    .as_ref()
                    .is_some_and(|value| value.as_ref() != name.as_ref())
                || !rendered_attributes_match(attributes, &query.exact_attributes)
                || !rendered_attributes_match(
                    &resource.attributes,
                    &query.exact_resource_attributes,
                )
                || !rendered_attributes_match(&scope.attributes, &query.exact_scope_attributes)
                || query.start_time_unix_nanos.is_some_and(|start| {
                    start_time_unix_nanos.saturating_add(duration_nanos) < start
                })
                || query
                    .end_time_unix_nanos
                    .is_some_and(|end| start_time_unix_nanos >= end)
                || query
                    .min_duration_nanos
                    .is_some_and(|minimum| duration_nanos < minimum)
        }) {
            continue;
        }
        let ids = ids[ordinal];
        decoded.push(DurableSpan {
            stream_shard_id,
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Traces,
                topic_partition,
                LogicalOffset::new(offsets[ordinal]),
            ),
            tenant: resolve_sidecar(&sidecars.tenants, sidecar.tenant_id, "tenant")?,
            resource: resolve_sidecar(&sidecars.resources, sidecar.resource_id, "resource")?,
            scope: resolve_sidecar(&sidecars.scopes, sidecar.scope_id, "scope")?,
            trace_id: ids.0,
            span_id: ids.1,
            parent_span_id: ids.2,
            trace_state: resolve_sidecar(
                &sidecars.trace_states,
                sidecar.trace_state_id,
                "trace state",
            )?,
            flags: sidecar.flags,
            name: resolve_sidecar(&sidecars.names, sidecar.name_id, "name")?,
            kind: sidecar.kind,
            start_time_unix_nanos,
            duration_nanos,
            attributes: resolve_sidecar(
                &sidecars.attribute_sets,
                sidecar.attributes_id,
                "attributes",
            )?,
            dropped_attributes_count: sidecar.dropped_attributes_count,
            events: resolve_sidecar(&sidecars.event_sets, sidecar.events_id, "events")?,
            dropped_events_count: sidecar.dropped_events_count,
            links: resolve_sidecar(&sidecars.link_sets, sidecar.links_id, "links")?,
            dropped_links_count: sidecar.dropped_links_count,
            status: resolve_sidecar(&sidecars.statuses, sidecar.status_id, "status")?,
        });
    }
    Ok(decoded)
}

fn encode_trace_sidecars(records: &[&DurableSpan]) -> TelemetryResult<TraceBlockSidecars> {
    let mut tenants = SidecarInterner::new(records.len());
    let mut resources = SidecarInterner::new(records.len());
    let mut scopes = SidecarInterner::new(records.len());
    let mut trace_states = SidecarInterner::new(records.len());
    let mut names = SidecarInterner::new(records.len());
    let mut attribute_sets = SidecarInterner::new(records.len());
    let mut event_sets = SidecarInterner::new(records.len());
    let mut link_sets = SidecarInterner::new(records.len());
    let mut statuses = SidecarInterner::new(records.len());
    let mut spans = Vec::with_capacity(records.len());
    for span in records {
        spans.push(PackedSpanSidecar {
            tenant_id: tenants.intern(&span.tenant)?,
            resource_id: resources.intern(&span.resource)?,
            scope_id: scopes.intern(&span.scope)?,
            trace_state_id: trace_states.intern(&span.trace_state)?,
            flags: span.flags,
            name_id: names.intern(&span.name)?,
            kind: span.kind,
            attributes_id: attribute_sets.intern(&span.attributes)?,
            dropped_attributes_count: span.dropped_attributes_count,
            events_id: event_sets.intern(&span.events)?,
            dropped_events_count: span.dropped_events_count,
            links_id: link_sets.intern(&span.links)?,
            dropped_links_count: span.dropped_links_count,
            status_id: statuses.intern(&span.status)?,
        });
    }
    Ok(TraceBlockSidecars {
        tenants: tenants.into_values(),
        resources: resources.into_values(),
        scopes: scopes.into_values(),
        trace_states: trace_states.into_values(),
        names: names.into_values(),
        attribute_sets: attribute_sets.into_values(),
        event_sets: event_sets.into_values(),
        link_sets: link_sets.into_values(),
        statuses: statuses.into_values(),
        spans,
    })
}

struct SidecarInterner<T> {
    values: Vec<T>,
    ids: Option<HashMap<T, u32>>,
    capacity: usize,
}

impl<T: Clone + Eq + Hash> SidecarInterner<T> {
    fn new(capacity: usize) -> Self {
        Self {
            values: Vec::new(),
            ids: None,
            capacity,
        }
    }

    fn intern(&mut self, value: &T) -> TelemetryResult<u32> {
        if let Some(ids) = &self.ids {
            if let Some(index) = ids.get(value) {
                return Ok(*index);
            }
        } else {
            if let Some(index) = self.values.iter().position(|candidate| candidate == value) {
                return u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge);
            }
            if self.values.len() == 16 {
                self.ids = Some(
                    self.values
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(index, value)| {
                            Ok((
                                value,
                                u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge)?,
                            ))
                        })
                        .collect::<TelemetryResult<HashMap<_, _>>>()?,
                );
                self.ids
                    .as_mut()
                    .expect("interner map was installed")
                    .reserve(self.capacity.min(4_096).saturating_sub(16));
            }
        }
        let index = u32::try_from(self.values.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let value = value.clone();
        self.values.push(value.clone());
        if let Some(ids) = &mut self.ids {
            ids.insert(value, index);
        }
        Ok(index)
    }

    fn into_values(self) -> Vec<T> {
        self.values
    }
}

pub(super) fn resolve_sidecar<T: Clone>(
    values: &[T],
    id: u32,
    lane: &'static str,
) -> TelemetryResult<T> {
    trace_sidecar(values, id, lane).cloned()
}

pub(super) fn trace_sidecar<'a, T>(
    values: &'a [T],
    id: u32,
    lane: &'static str,
) -> TelemetryResult<&'a T> {
    values
        .get(id as usize)
        .ok_or(TelemetryError::InvalidBlockEncoding(match lane {
            "tenant" => "trace tenant sidecar ID is out of range",
            "resource" => "trace resource sidecar ID is out of range",
            "scope" => "trace scope sidecar ID is out of range",
            "trace state" => "trace state sidecar ID is out of range",
            "name" => "trace name sidecar ID is out of range",
            "attributes" => "trace attribute sidecar ID is out of range",
            "events" => "trace event sidecar ID is out of range",
            "links" => "trace link sidecar ID is out of range",
            "status" => "trace status sidecar ID is out of range",
            _ => "trace sidecar ID is out of range",
        }))
}

pub(super) fn encode_span_ids(records: &[&DurableSpan]) -> TelemetryResult<Vec<u8>> {
    let mut trace_groups = Vec::with_capacity(records.len());
    let mut previous_group_trace = [0; 16];
    let mut group_start = 0usize;
    while group_start < records.len() {
        let trace = records[group_start].trace_id.as_bytes();
        let mut group_end = group_start + 1;
        while group_end < records.len() && records[group_end].trace_id.as_bytes() == trace {
            group_end += 1;
        }
        let prefix = trace
            .iter()
            .zip(previous_group_trace)
            .take_while(|(left, right)| **left == *right)
            .count();
        debug_assert!(prefix < 16, "adjacent trace groups must differ");
        trace_groups.push(u8::try_from(prefix).expect("trace prefix is at most 15"));
        trace_groups.extend_from_slice(&trace[prefix..]);
        write_trace_run(group_end - group_start, &mut trace_groups)?;
        previous_group_trace = *trace;
        group_start = group_end;
    }

    let mut span_lane = Vec::with_capacity(records.len().saturating_mul(10));
    let mut previous_trace = [0; 16];
    let mut previous_span = [0; 8];
    let mut previous_parent = [0; 8];
    let mut first_span_in_trace = [0; 8];
    for record in records {
        let trace = record.trace_id.as_bytes();
        let same_trace = trace == &previous_trace;
        encode_xor_id(record.span_id.as_bytes(), &previous_span, &mut span_lane);
        match record.parent_span_id {
            Some(parent) => {
                if same_trace && parent.as_bytes() == &previous_span {
                    span_lane.push(2);
                } else if same_trace && parent.as_bytes() == &first_span_in_trace {
                    span_lane.push(3);
                } else {
                    span_lane.push(1);
                    encode_xor_id(parent.as_bytes(), &previous_parent, &mut span_lane);
                }
                previous_parent = *parent.as_bytes();
            }
            None => span_lane.push(0),
        }
        if !same_trace {
            first_span_in_trace = *record.span_id.as_bytes();
        }
        previous_trace = *trace;
        previous_span = *record.span_id.as_bytes();
    }
    let mut encoded = Vec::with_capacity(4 + trace_groups.len() + span_lane.len());
    append_section(&mut encoded, &trace_groups)?;
    encoded.extend_from_slice(&span_lane);
    Ok(encoded)
}

pub(super) fn encode_xor_id(current: &[u8; 8], previous: &[u8; 8], encoded: &mut Vec<u8>) {
    let xor = u64::from_be_bytes(*current) ^ u64::from_be_bytes(*previous);
    let bytes = xor.to_be_bytes();
    let leading = bytes.iter().take_while(|byte| **byte == 0).count();
    encoded.push(u8::try_from(leading).expect("leading byte count is at most 8"));
    encoded.extend_from_slice(&bytes[leading..]);
}

type DecodedSpanIds = (TraceId, SpanId, Option<SpanId>);

pub(super) fn decode_span_ids(
    encoded: &[u8],
    count: usize,
) -> TelemetryResult<Vec<DecodedSpanIds>> {
    let mut lane_cursor = 0;
    let trace_groups = read_section(encoded, &mut lane_cursor, encoded.len())?;
    let traces = decode_trace_groups(trace_groups, count)?;
    let span_lane = &encoded[lane_cursor..];
    let mut cursor = 0;
    let mut previous_trace = [0; 16];
    let mut previous_span = [0; 8];
    let mut previous_parent = [0; 8];
    let mut first_span_in_trace = [0; 8];
    let mut decoded = Vec::with_capacity(count);
    for trace_id in traces {
        let trace = *trace_id.as_bytes();
        let same_trace = trace == previous_trace;
        let span = decode_xor_id(span_lane, &mut cursor, previous_span)?;
        let parent = match read_byte(span_lane, &mut cursor)? {
            0 => None,
            1 => {
                let parent = decode_xor_id(span_lane, &mut cursor, previous_parent)?;
                previous_parent = parent;
                Some(SpanId::from_bytes(parent)?)
            }
            2 if same_trace => {
                previous_parent = previous_span;
                Some(SpanId::from_bytes(previous_span)?)
            }
            3 if same_trace => {
                previous_parent = first_span_in_trace;
                Some(SpanId::from_bytes(first_span_in_trace)?)
            }
            _ => {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid parent span marker",
                ));
            }
        };
        let span_id = SpanId::from_bytes(span)?;
        decoded.push((trace_id, span_id, parent));
        if !same_trace {
            first_span_in_trace = span;
        }
        previous_trace = trace;
        previous_span = span;
    }
    if cursor != span_lane.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing span ID lane bytes",
        ));
    }
    Ok(decoded)
}

pub(super) fn decode_trace_groups(encoded: &[u8], count: usize) -> TelemetryResult<Vec<TraceId>> {
    let mut cursor = 0usize;
    let mut previous_trace = [0; 16];
    let mut traces = Vec::with_capacity(count);
    while traces.len() < count {
        let prefix = read_byte(encoded, &mut cursor)? as usize;
        if prefix >= 16 || encoded.len().saturating_sub(cursor) < 16 - prefix {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid grouped trace ID prefix lane",
            ));
        }
        let mut trace = previous_trace;
        trace[prefix..].copy_from_slice(&encoded[cursor..cursor + 16 - prefix]);
        cursor += 16 - prefix;
        let run = read_trace_run(encoded, &mut cursor)?;
        if run == 0 || run > count - traces.len() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid grouped trace ID run length",
            ));
        }
        let trace_id = TraceId::from_bytes(trace)?;
        traces.extend(std::iter::repeat_n(trace_id, run));
        previous_trace = trace;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing grouped trace ID bytes",
        ));
    }
    Ok(traces)
}

pub(super) fn write_trace_run(run: usize, encoded: &mut Vec<u8>) -> TelemetryResult<()> {
    let mut value = u64::try_from(run).map_err(|_| TelemetryError::RecordTooLarge)?;
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    Ok(())
}

pub(super) fn read_trace_run(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<usize> {
    let mut value = 0u64;
    for index in 0..10 {
        let byte = read_byte(encoded, cursor)?;
        if index == 9 && byte & 0xfe != 0 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "grouped trace ID run length overflow",
            ));
        }
        let shift = index * 7;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(value).map_err(|_| {
                TelemetryError::InvalidBlockEncoding("grouped trace ID run length overflow")
            });
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "grouped trace ID run length overflow",
    ))
}

pub(super) fn decode_xor_id(
    encoded: &[u8],
    cursor: &mut usize,
    previous: [u8; 8],
) -> TelemetryResult<[u8; 8]> {
    let leading = read_byte(encoded, cursor)? as usize;
    if leading > 8 || encoded.len().saturating_sub(*cursor) < 8 - leading {
        return Err(TelemetryError::InvalidBlockEncoding("invalid XOR ID lane"));
    }
    let mut xor_bytes = [0; 8];
    xor_bytes[leading..].copy_from_slice(&encoded[*cursor..*cursor + 8 - leading]);
    *cursor += 8 - leading;
    let value = u64::from_be_bytes(previous) ^ u64::from_be_bytes(xor_bytes);
    Ok(value.to_be_bytes())
}

pub(super) fn compress_u64(values: &[u64]) -> TelemetryResult<Vec<u8>> {
    simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(TRACE_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
}

pub(super) fn decompress_u64(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace Pco lane count mismatch",
        ));
    }
    Ok(values)
}

pub(super) fn append_section(encoded: &mut Vec<u8>, section: &[u8]) -> TelemetryResult<()> {
    encoded.extend_from_slice(
        &u32::try_from(section.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(section);
    Ok(())
}

pub(super) fn read_section<'a>(
    encoded: &'a [u8],
    cursor: &mut usize,
    payload_end: usize,
) -> TelemetryResult<&'a [u8]> {
    if payload_end.saturating_sub(*cursor) < 4 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated trace block section length",
        ));
    }
    let len = u32::from_le_bytes(
        encoded[*cursor..*cursor + 4]
            .try_into()
            .expect("fixed range"),
    ) as usize;
    *cursor += 4;
    let end = cursor
        .checked_add(len)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "trace section length overflow",
        ))?;
    if end > payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated trace block section",
        ));
    }
    let section = &encoded[*cursor..end];
    *cursor = end;
    Ok(section)
}

pub(super) fn read_byte(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u8> {
    let value = *encoded
        .get(*cursor)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "truncated span ID lane",
        ))?;
    *cursor += 1;
    Ok(value)
}
