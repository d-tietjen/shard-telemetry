use super::*;

pub(super) fn estimated_metric_identity_bytes(identity: &Arc<MetricIdentity>) -> usize {
    size_of::<MetricIdentity>()
        .saturating_add(estimated_arc_str_bytes(&identity.tenant))
        .saturating_add(estimated_resource_context_bytes(&identity.resource))
        .saturating_add(estimated_scope_context_bytes(&identity.scope))
        .saturating_add(estimated_arc_str_bytes(&identity.name))
        .saturating_add(estimated_arc_str_bytes(&identity.unit))
        .saturating_add(estimated_arc_vec_storage::<TelemetryAttribute>(
            identity.point_attributes.capacity(),
        ))
        .saturating_add(
            identity
                .point_attributes
                .iter()
                .map(estimated_telemetry_attribute_bytes)
                .sum(),
        )
}

pub(super) fn estimated_arc_attributes_bytes(attributes: &Arc<Vec<TelemetryAttribute>>) -> usize {
    estimated_arc_vec_storage::<TelemetryAttribute>(attributes.capacity()).saturating_add(
        attributes
            .iter()
            .map(estimated_telemetry_attribute_bytes)
            .sum(),
    )
}

pub(super) fn estimated_arc_exemplars_bytes(exemplars: &Arc<Vec<MetricExemplar>>) -> usize {
    estimated_arc_vec_storage::<MetricExemplar>(exemplars.capacity()).saturating_add(
        exemplars
            .iter()
            .map(|exemplar| {
                size_of::<MetricExemplar>().saturating_add(estimated_arc_attributes_bytes(
                    &exemplar.filtered_attributes,
                ))
            })
            .sum(),
    )
}

pub(super) fn estimated_metric_value_bytes(value: &MetricValue) -> usize {
    match value {
        MetricValue::Gauge(_) | MetricValue::Sum(_) => 0,
        MetricValue::ExplicitHistogram(value) => {
            estimated_arc_vec_storage::<HistogramCount>(value.bucket_counts.capacity())
                .saturating_add(estimated_arc_vec_storage::<u64>(
                    value.explicit_bounds_bits.capacity(),
                ))
        }
        MetricValue::ExponentialHistogram(value) => value
            .positive
            .as_ref()
            .map_or(0, estimated_exponential_histogram_bucket_bytes)
            .saturating_add(
                value
                    .negative
                    .as_ref()
                    .map_or(0, estimated_exponential_histogram_bucket_bytes),
            ),
        MetricValue::Summary(value) => {
            estimated_arc_vec_storage::<SummaryQuantileValue>(value.quantiles.capacity())
        }
    }
}

pub(super) fn estimated_exponential_histogram_bucket_bytes(
    buckets: &ExponentialHistogramBuckets,
) -> usize {
    estimated_arc_vec_storage::<HistogramBucketSpan>(buckets.spans.capacity()).saturating_add(
        estimated_arc_vec_storage::<HistogramCount>(buckets.bucket_counts.capacity()),
    )
}

pub(super) fn same_metric_sample_payload(
    existing: &DurableMetricPoint,
    candidate: &DurableMetricPoint,
) -> bool {
    existing.value == candidate.value
        && existing.flags == candidate.flags
        && existing.exemplars == candidate.exemplars
        && existing.start_time_unix_nanos == candidate.start_time_unix_nanos
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricPointSidecar {
    description_id: u32,
    metadata_id: u32,
    start_time_unix_nanos: u64,
    flags: u32,
    exemplars_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricChunkSidecars {
    identity: Arc<MetricIdentity>,
    descriptions: Vec<Arc<str>>,
    metadata_sets: Vec<Arc<Vec<TelemetryAttribute>>>,
    exemplar_sets: Vec<Arc<Vec<MetricExemplar>>>,
    points: Vec<MetricPointSidecar>,
}

/// Encodes one series' sorted points into a signal-native metric chunk.
pub fn encode_metric_chunk(points: &[DurableMetricPoint]) -> TelemetryResult<Vec<u8>> {
    let Some(first) = points.first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk must contain at least one point",
        ));
    };
    let fingerprint = first.series_fingerprint();
    let partition = first.record_ref.topic_partition;
    let stream_shard_id = first.stream_shard_id;
    if points.iter().any(|point| {
        point.record_ref.signal != TelemetrySignal::Metrics
            || point.record_ref.topic_partition != partition
            || point.stream_shard_id != stream_shard_id
            || point.identity.as_ref() != first.identity.as_ref()
    }) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk points do not share a series, partition, and owner",
        ));
    }
    let mut sorted = points.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
    let offsets = sorted
        .iter()
        .map(|point| point.record_ref.offset.get())
        .collect::<Vec<_>>();
    let timestamps = sorted
        .iter()
        .map(|point| point.timestamp_unix_nanos)
        .collect::<Vec<_>>();
    let values = encode_metric_values(&sorted)?;
    let sidecars = encode_metric_sidecars(&sorted)?;
    let sidecar_bytes = rmp_serde::to_vec(&sidecars)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed_sidecars = METRIC_COMPRESSOR.with_borrow_mut(|compressor| {
        compressor
            .compress(&sidecar_bytes)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&METRIC_CHUNK_MAGIC);
    encoded.push(METRIC_CHUNK_VERSION);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&stream_shard_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.topic_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.partition_id.get().to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(sorted.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&fingerprint.get().to_le_bytes());
    for section in [
        compress_u64(&offsets)?,
        encode_timestamp_delta_of_delta(&timestamps)?,
        values,
        compressed_sidecars,
    ] {
        append_section(&mut encoded, &section)?;
    }
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes and verifies one signal-native metric chunk.
pub fn decode_metric_chunk(encoded: &[u8]) -> TelemetryResult<Vec<DurableMetricPoint>> {
    const FIXED_HEADER: usize = 52;
    if encoded.len() < FIXED_HEADER + 32 || encoded[..4] != METRIC_CHUNK_MAGIC {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing metric chunk header",
        ));
    }
    if encoded[4] != METRIC_CHUNK_VERSION || encoded[5..8] != [0, 0, 0] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "unsupported metric chunk version or flags",
        ));
    }
    let payload_end = encoded.len() - 32;
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk checksum mismatch",
        ));
    }
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
    let count = u32::from_le_bytes(encoded[32..36].try_into().expect("fixed range")) as usize;
    let expected_fingerprint = SeriesFingerprint::from_raw(u128::from_le_bytes(
        encoded[36..52].try_into().expect("fixed range"),
    ));
    if count == 0 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk has no points",
        ));
    }
    let mut cursor = FIXED_HEADER;
    let offsets = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let timestamps =
        decode_timestamp_delta_of_delta(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let values = decode_metric_values(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let compressed_sidecars = read_section(encoded, &mut cursor, payload_end)?;
    if cursor != payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric chunk sections",
        ));
    }
    let sidecar_bytes = METRIC_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(compressed_sidecars, 64 * 1024 * 1024)
            .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric sidecar compression"))
    })?;
    let sidecars: MetricChunkSidecars = rmp_serde::from_slice(&sidecar_bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric sidecars"))?;
    if sidecars.points.len() != count || sidecars.identity.fingerprint() != expected_fingerprint {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric sidecar identity or count mismatch",
        ));
    }
    offsets
        .into_iter()
        .zip(timestamps)
        .zip(values)
        .zip(sidecars.points)
        .map(|(((offset, timestamp_unix_nanos), value), sidecar)| {
            Ok(DurableMetricPoint {
                stream_shard_id,
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    topic_partition,
                    LogicalOffset::new(offset),
                ),
                identity: Arc::clone(&sidecars.identity),
                description: resolve_metric_sidecar(
                    &sidecars.descriptions,
                    sidecar.description_id,
                    "description",
                )?,
                metadata: resolve_metric_sidecar(
                    &sidecars.metadata_sets,
                    sidecar.metadata_id,
                    "metadata",
                )?,
                start_time_unix_nanos: sidecar.start_time_unix_nanos,
                timestamp_unix_nanos,
                flags: sidecar.flags,
                value,
                exemplars: resolve_metric_sidecar(
                    &sidecars.exemplar_sets,
                    sidecar.exemplars_id,
                    "exemplars",
                )?,
            })
        })
        .collect::<TelemetryResult<Vec<_>>>()
}

fn encode_metric_sidecars(points: &[&DurableMetricPoint]) -> TelemetryResult<MetricChunkSidecars> {
    let mut descriptions = MetricSidecarInterner::new(points.len());
    let mut metadata_sets = MetricSidecarInterner::new(points.len());
    let mut exemplar_sets = MetricSidecarInterner::new(points.len());
    let mut packed = Vec::with_capacity(points.len());
    for point in points {
        let description_id = descriptions.intern(&point.description)?;
        let metadata_id = metadata_sets.intern(&point.metadata)?;
        let exemplars_id = exemplar_sets.intern(&point.exemplars)?;
        packed.push(MetricPointSidecar {
            description_id,
            metadata_id,
            start_time_unix_nanos: point.start_time_unix_nanos,
            flags: point.flags,
            exemplars_id,
        });
    }
    Ok(MetricChunkSidecars {
        identity: Arc::clone(&points[0].identity),
        descriptions: descriptions.into_values(),
        metadata_sets: metadata_sets.into_values(),
        exemplar_sets: exemplar_sets.into_values(),
        points: packed,
    })
}

struct MetricSidecarInterner<T> {
    values: Vec<T>,
    ids: Option<HashMap<T, u32>>,
    capacity: usize,
}

impl<T: Clone + Eq + Hash> MetricSidecarInterner<T> {
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

pub(super) fn resolve_metric_sidecar<T: Clone>(
    values: &[T],
    id: u32,
    lane: &'static str,
) -> TelemetryResult<T> {
    values
        .get(id as usize)
        .cloned()
        .ok_or(TelemetryError::InvalidBlockEncoding(match lane {
            "description" => "metric description sidecar ID is out of range",
            "metadata" => "metric metadata sidecar ID is out of range",
            "exemplars" => "metric exemplar sidecar ID is out of range",
            _ => "metric sidecar ID is out of range",
        }))
}

pub(super) fn encode_metric_values(points: &[&DurableMetricPoint]) -> TelemetryResult<Vec<u8>> {
    if let Some(values) = collect_number_lane(points, true, false) {
        return encode_integer_value_lane(1, &values);
    }
    if let Some(values) = collect_double_lane(points, true) {
        return encode_double_value_lane(2, &values);
    }
    if let Some(values) = collect_number_lane(points, false, true) {
        return encode_integer_value_lane(3, &values);
    }
    if let Some(values) = collect_double_lane(points, false) {
        return encode_double_value_lane(4, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::ExplicitHistogram(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::ExplicitHistogram(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(5, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::ExponentialHistogram(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::ExponentialHistogram(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(6, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::Summary(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::Summary(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(7, &values);
    }

    let mut encoded = vec![0];
    let mut previous_float = 0u64;
    for point in points {
        match &point.value {
            MetricValue::Gauge(value) => {
                encoded.push(0);
                encode_number(*value, &mut previous_float, &mut encoded);
            }
            MetricValue::Sum(value) => {
                encoded.push(1);
                encode_number(*value, &mut previous_float, &mut encoded);
            }
            MetricValue::ExplicitHistogram(value) => {
                encoded.push(2);
                append_messagepack(&mut encoded, value)?;
            }
            MetricValue::ExponentialHistogram(value) => {
                encoded.push(3);
                append_messagepack(&mut encoded, value)?;
            }
            MetricValue::Summary(value) => {
                encoded.push(4);
                append_messagepack(&mut encoded, value)?;
            }
        }
    }
    Ok(encoded)
}

pub(super) fn decode_metric_values(
    encoded: &[u8],
    count: usize,
) -> TelemetryResult<Vec<MetricValue>> {
    let (&codec, payload) = encoded
        .split_first()
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "metric value lane is empty",
        ))?;
    match codec {
        1 => {
            return decode_integer_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::Integer)
                    .map(MetricValue::Gauge)
                    .collect()
            });
        }
        2 => {
            return decode_double_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::DoubleBits)
                    .map(MetricValue::Gauge)
                    .collect()
            });
        }
        3 => {
            return decode_integer_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::Integer)
                    .map(MetricValue::Sum)
                    .collect()
            });
        }
        4 => {
            return decode_double_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::DoubleBits)
                    .map(MetricValue::Sum)
                    .collect()
            });
        }
        5 => {
            return decode_zstd_value_lane::<ExplicitHistogramValue>(payload, count).map(
                |values| {
                    values
                        .into_iter()
                        .map(MetricValue::ExplicitHistogram)
                        .collect()
                },
            );
        }
        6 => {
            return decode_zstd_value_lane::<ExponentialHistogramValue>(payload, count).map(
                |values| {
                    values
                        .into_iter()
                        .map(MetricValue::ExponentialHistogram)
                        .collect()
                },
            );
        }
        7 => {
            return decode_zstd_value_lane::<SummaryValue>(payload, count)
                .map(|values| values.into_iter().map(MetricValue::Summary).collect());
        }
        0 => {}
        _ => {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid metric value-lane codec",
            ));
        }
    }
    let encoded = payload;
    let mut cursor = 0;
    let mut previous_float = 0u64;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match read_byte(encoded, &mut cursor)? {
            0 => MetricValue::Gauge(decode_number(encoded, &mut cursor, &mut previous_float)?),
            1 => MetricValue::Sum(decode_number(encoded, &mut cursor, &mut previous_float)?),
            2 => MetricValue::ExplicitHistogram(read_messagepack(encoded, &mut cursor)?),
            3 => MetricValue::ExponentialHistogram(read_messagepack(encoded, &mut cursor)?),
            4 => MetricValue::Summary(read_messagepack(encoded, &mut cursor)?),
            _ => {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid metric value tag",
                ));
            }
        });
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric value bytes",
        ));
    }
    Ok(values)
}

pub(super) fn collect_number_lane(
    points: &[&DurableMetricPoint],
    gauge: bool,
    sum: bool,
) -> Option<Vec<i64>> {
    points
        .iter()
        .map(|point| match point.value {
            MetricValue::Gauge(NumberValue::Integer(value)) if gauge => Some(value),
            MetricValue::Sum(NumberValue::Integer(value)) if sum => Some(value),
            _ => None,
        })
        .collect()
}

pub(super) fn collect_double_lane(points: &[&DurableMetricPoint], gauge: bool) -> Option<Vec<u64>> {
    points
        .iter()
        .map(|point| match point.value {
            MetricValue::Gauge(NumberValue::DoubleBits(value)) if gauge => Some(value),
            MetricValue::Sum(NumberValue::DoubleBits(value)) if !gauge => Some(value),
            _ => None,
        })
        .collect()
}

pub(super) fn encode_integer_value_lane(codec: u8, values: &[i64]) -> TelemetryResult<Vec<u8>> {
    let compressed = simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(METRIC_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let mut encoded = Vec::with_capacity(1 + compressed.len());
    encoded.push(codec);
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

pub(super) fn decode_integer_value_lane(encoded: &[u8], count: usize) -> TelemetryResult<Vec<i64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric integer Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric integer Pco lane count mismatch",
        ));
    }
    Ok(values)
}

pub(super) fn encode_double_value_lane(codec: u8, values: &[u64]) -> TelemetryResult<Vec<u8>> {
    let Some((&first, rest)) = values.split_first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric double lane is empty",
        ));
    };
    let mut writer = MetricBitWriter::new();
    let mut previous = first;
    let mut previous_window = None::<(u8, u8)>;
    for &value in rest {
        let xor = value ^ previous;
        if xor == 0 {
            writer.write_bit(false);
        } else {
            writer.write_bit(true);
            let leading = (xor.leading_zeros() as u8).min(31);
            let trailing = xor.trailing_zeros() as u8;
            if let Some((window_leading, window_trailing)) = previous_window
                && leading >= window_leading
                && trailing >= window_trailing
            {
                writer.write_bit(false);
                writer.write_bits(
                    xor >> window_trailing,
                    64 - window_leading - window_trailing,
                );
            } else {
                writer.write_bit(true);
                let significant = 64 - leading - trailing;
                writer.write_bits(u64::from(leading), 5);
                writer.write_bits(u64::from(significant) & 0x3f, 6);
                writer.write_bits(xor >> trailing, significant);
                previous_window = Some((leading, trailing));
            }
        }
        previous = value;
    }
    let bits = writer.finish();
    let mut encoded = Vec::with_capacity(9 + bits.len());
    encoded.push(codec);
    encoded.extend_from_slice(&first.to_le_bytes());
    encoded.extend_from_slice(&bits);
    Ok(encoded)
}

pub(super) fn decode_double_value_lane(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    if count == 0 || encoded.len() < 8 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric double lane",
        ));
    }
    let first = u64::from_le_bytes(encoded[..8].try_into().expect("fixed range"));
    let mut values = Vec::with_capacity(count);
    values.push(first);
    let mut previous = first;
    let mut previous_window = None::<(u8, u8)>;
    let mut reader = MetricBitReader::new(&encoded[8..]);
    for _ in 1..count {
        let value = if !reader.read_bit()? {
            previous
        } else {
            let (leading, trailing) = if !reader.read_bit()? {
                previous_window.ok_or(TelemetryError::InvalidBlockEncoding(
                    "metric double lane reuses a missing XOR window",
                ))?
            } else {
                let leading = reader.read_bits(5)? as u8;
                let encoded_significant = reader.read_bits(6)? as u8;
                let significant = if encoded_significant == 0 {
                    64
                } else {
                    encoded_significant
                };
                if u16::from(leading) + u16::from(significant) > 64 {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid metric double XOR window",
                    ));
                }
                let trailing = 64 - leading - significant;
                previous_window = Some((leading, trailing));
                (leading, trailing)
            };
            if leading.saturating_add(trailing) >= 64 {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid metric double XOR window",
                ));
            }
            let significant = 64 - leading - trailing;
            previous ^ (reader.read_bits(significant)? << trailing)
        };
        values.push(value);
        previous = value;
    }
    reader.finish()?;
    Ok(values)
}

pub(super) fn encode_zstd_value_lane<T: Serialize>(
    codec: u8,
    values: &[T],
) -> TelemetryResult<Vec<u8>> {
    let raw = rmp_serde::to_vec(values)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed = METRIC_COMPRESSOR.with_borrow_mut(|compressor| {
        compressor
            .compress(&raw)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;
    let mut encoded = Vec::with_capacity(5 + compressed.len());
    encoded.push(codec);
    encoded.extend_from_slice(
        &u32::try_from(raw.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

pub(super) fn decode_zstd_value_lane<T: for<'de> Deserialize<'de>>(
    encoded: &[u8],
    count: usize,
) -> TelemetryResult<Vec<T>> {
    if encoded.len() < 4 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated compressed metric value lane",
        ));
    }
    let raw_len = u32::from_le_bytes(encoded[..4].try_into().expect("fixed range")) as usize;
    if raw_len > 64 * 1024 * 1024 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric value lane exceeds safety limit",
        ));
    }
    let raw = METRIC_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(&encoded[4..], raw_len)
            .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value compression"))
    })?;
    let values: Vec<T> = rmp_serde::from_slice(&raw)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value lane"))?;
    if values.len() != count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric value lane count mismatch",
        ));
    }
    Ok(values)
}

struct MetricBitWriter {
    bytes: Vec<u8>,
    used: u8,
}

impl MetricBitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            used: 8,
        }
    }

    fn write_bit(&mut self, value: bool) {
        if self.used == 8 {
            self.bytes.push(0);
            self.used = 0;
        }
        if value {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << (7 - self.used);
        }
        self.used += 1;
    }

    fn write_bits(&mut self, value: u64, bits: u8) {
        for shift in (0..bits).rev() {
            self.write_bit(value & (1u64 << shift) != 0);
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct MetricBitReader<'a> {
    bytes: &'a [u8],
    bit: usize,
}

impl<'a> MetricBitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, bit: 0 }
    }

    fn read_bit(&mut self) -> TelemetryResult<bool> {
        if self.bit >= self.bytes.len().saturating_mul(8) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "truncated metric double lane",
            ));
        }
        let value = self.bytes[self.bit / 8] & (1 << (7 - self.bit % 8)) != 0;
        self.bit += 1;
        Ok(value)
    }

    fn read_bits(&mut self, bits: u8) -> TelemetryResult<u64> {
        let mut value = 0;
        for _ in 0..bits {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Ok(value)
    }

    fn finish(mut self) -> TelemetryResult<()> {
        let remaining = self.bytes.len().saturating_mul(8).saturating_sub(self.bit);
        if remaining >= 8 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "trailing metric double-lane bytes",
            ));
        }
        while self.bit < self.bytes.len().saturating_mul(8) {
            if self.read_bit()? {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "nonzero metric double-lane padding",
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn encode_number(value: NumberValue, previous_float: &mut u64, encoded: &mut Vec<u8>) {
    match value {
        NumberValue::Integer(value) => {
            encoded.push(0);
            write_varint(zigzag_i64(value), encoded);
        }
        NumberValue::DoubleBits(bits) => {
            encoded.push(1);
            write_varint(bits ^ *previous_float, encoded);
            *previous_float = bits;
        }
    }
}

pub(super) fn decode_number(
    encoded: &[u8],
    cursor: &mut usize,
    previous_float: &mut u64,
) -> TelemetryResult<NumberValue> {
    match read_byte(encoded, cursor)? {
        0 => Ok(NumberValue::Integer(unzigzag_i64(read_varint(
            encoded, cursor,
        )?))),
        1 => {
            let bits = read_varint(encoded, cursor)? ^ *previous_float;
            *previous_float = bits;
            Ok(NumberValue::DoubleBits(bits))
        }
        _ => Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric number tag",
        )),
    }
}

pub(super) fn append_messagepack<T: Serialize>(
    encoded: &mut Vec<u8>,
    value: &T,
) -> TelemetryResult<()> {
    let bytes = rmp_serde::to_vec(value)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    append_section(encoded, &bytes)
}

pub(super) fn read_messagepack<T: for<'de> Deserialize<'de>>(
    encoded: &[u8],
    cursor: &mut usize,
) -> TelemetryResult<T> {
    let section = read_section(encoded, cursor, encoded.len())?;
    rmp_serde::from_slice(section)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value payload"))
}

pub(super) fn encode_timestamp_delta_of_delta(timestamps: &[u64]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let Some(&first) = timestamps.first() else {
        return Ok(encoded);
    };
    encoded.extend_from_slice(&first.to_le_bytes());
    if timestamps.len() == 1 {
        return Ok(encoded);
    }
    let mut previous_delta =
        timestamps[1]
            .checked_sub(first)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamps are not sorted",
            ))?;
    write_varint(previous_delta, &mut encoded);
    let mut previous = timestamps[1];
    let mut remaining = &timestamps[2..];
    while let Some((&timestamp, rest)) = remaining.split_first() {
        let delta = timestamp
            .checked_sub(previous)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamps are not sorted",
            ))?;
        let delta_of_delta = i128::from(delta) - i128::from(previous_delta);
        if delta_of_delta == 0 {
            let mut run = 1usize;
            let mut run_previous = timestamp;
            while let Some(&candidate) = rest.get(run - 1) {
                let candidate_delta = candidate.checked_sub(run_previous).ok_or(
                    TelemetryError::InvalidBlockEncoding("metric timestamps are not sorted"),
                )?;
                if candidate_delta != delta {
                    break;
                }
                run += 1;
                run_previous = candidate;
            }
            write_varint128(0, &mut encoded);
            write_varint(
                u64::try_from(run).map_err(|_| TelemetryError::RecordTooLarge)?,
                &mut encoded,
            );
            previous = run_previous;
            remaining = &remaining[run..];
            continue;
        }
        write_varint128(
            zigzag_i128(delta_of_delta)
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        previous = timestamp;
        previous_delta = delta;
        remaining = rest;
    }
    Ok(encoded)
}

pub(super) fn decode_timestamp_delta_of_delta(
    encoded: &[u8],
    count: usize,
) -> TelemetryResult<Vec<u64>> {
    if count == 0 || encoded.len() < 8 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric timestamp lane",
        ));
    }
    let mut cursor = 8;
    let first = u64::from_le_bytes(encoded[..8].try_into().expect("fixed range"));
    let mut timestamps = Vec::with_capacity(count);
    timestamps.push(first);
    if count == 1 {
        if cursor != encoded.len() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "trailing metric timestamp bytes",
            ));
        }
        return Ok(timestamps);
    }
    let mut previous_delta = read_varint(encoded, &mut cursor)?;
    let mut previous =
        first
            .checked_add(previous_delta)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp overflow",
            ))?;
    timestamps.push(previous);
    while timestamps.len() < count {
        let token = read_varint128(encoded, &mut cursor)?;
        if token == 0 {
            let run = usize::try_from(read_varint(encoded, &mut cursor)?).map_err(|_| {
                TelemetryError::InvalidBlockEncoding("metric timestamp run length overflow")
            })?;
            if run == 0 || run > count - timestamps.len() {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid metric timestamp run length",
                ));
            }
            for _ in 0..run {
                previous = previous.checked_add(previous_delta).ok_or(
                    TelemetryError::InvalidBlockEncoding("metric timestamp overflow"),
                )?;
                timestamps.push(previous);
            }
            continue;
        }
        let delta_of_delta = unzigzag_i128(token - 1);
        let delta = i128::from(previous_delta)
            .checked_add(delta_of_delta)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp delta overflow",
            ))?;
        previous = previous
            .checked_add(delta)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp overflow",
            ))?;
        timestamps.push(previous);
        previous_delta = delta;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric timestamp bytes",
        ));
    }
    Ok(timestamps)
}

pub(super) fn compress_u64(values: &[u64]) -> TelemetryResult<Vec<u8>> {
    simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(METRIC_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
}

pub(super) fn decompress_u64(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric Pco lane count mismatch",
        ));
    }
    Ok(values)
}

pub(super) fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    output.extend_from_slice(bytes);
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
            "truncated metric section length",
        ));
    }
    let len = u32::from_le_bytes(
        encoded[*cursor..*cursor + 4]
            .try_into()
            .expect("fixed range"),
    ) as usize;
    *cursor += 4;
    let end = (*cursor)
        .checked_add(len)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "metric section length overflow",
        ))?;
    if end > payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated metric section",
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
            "truncated metric value",
        ))?;
    *cursor += 1;
    Ok(value)
}

pub(super) fn write_varint(mut value: u64, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

pub(super) fn read_varint(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = read_byte(encoded, cursor)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "metric varint overflow",
    ))
}

pub(super) fn write_varint128(mut value: u128, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

pub(super) fn read_varint128(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u128> {
    let mut value = 0u128;
    for shift in (0..128).step_by(7) {
        let byte = read_byte(encoded, cursor)?;
        value |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "metric wide varint overflow",
    ))
}

const fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

const fn unzigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

const fn zigzag_i128(value: i128) -> u128 {
    ((value << 1) ^ (value >> 127)) as u128
}

const fn unzigzag_i128(value: u128) -> i128 {
    ((value >> 1) as i128) ^ -((value & 1) as i128)
}
