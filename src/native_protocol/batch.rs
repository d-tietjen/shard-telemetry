use super::*;

impl NativeTelemetryBatch {
    /// Encodes a retryable native v1 append.
    ///
    /// Native v1 deliberately accepts one partition per request. A single
    /// durable retry identity cannot make a parallel multi-partition append
    /// atomic when a later partition fails, so callers fan out one request per
    /// routed partition instead. In-process store APIs retain their grouped
    /// fast path and do not use this wire-level constraint.
    pub fn encode_native_append(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        self.encode()
    }

    /// Encodes the bounded signal-aware native v1 payload.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.is_empty() || self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native telemetry batch requires 1..=256 partitions",
            ));
        }
        let mut seen = (self.partitions.len() > 1).then(BTreeSet::new);
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&TELEMETRY_BATCH_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            if let Some(seen) = &mut seen
                && !seen.insert(partition.topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            if partition.topic_partition.topic_id != partition.envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            let envelope = partition
                .envelope
                .encode()
                .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(
                &u32::try_from(envelope.len())
                    .map_err(|_| NativeProtocolError::new("STEL envelope exceeds u32"))?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(&envelope);
            let transient_context = partition.transient_context.as_deref().unwrap_or_default();
            encoded.extend_from_slice(
                &u32::try_from(transient_context.len())
                    .map_err(|_| NativeProtocolError::new("transient context exceeds u32"))?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(transient_context);
        }
        if encoded.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(
                "native telemetry batch exceeds the frame limit",
            ));
        }
        Ok(encoded)
    }

    /// Decodes and verifies every STEL envelope before returning any partition.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        Self::decode_impl(payload, false).map(|(batch, _)| batch)
    }

    /// Decodes and validates a batch while retaining the exact wire ranges for
    /// each envelope and optional transient context.
    pub(crate) fn decode_with_envelope_ranges(
        payload: &[u8],
    ) -> Result<(Self, EnvelopeWireRanges), NativeProtocolError> {
        let (batch, ranges) = Self::decode_impl(payload, true)?;
        Ok((
            batch,
            ranges.expect("range capture was enabled for this decode"),
        ))
    }

    pub(super) fn decode_impl(
        payload: &[u8],
        capture_ranges: bool,
    ) -> Result<(Self, Option<EnvelopeWireRanges>), NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count == 0 || count > 256 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "invalid native telemetry partition count or flags",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        let mut wire_ranges = capture_ranges.then(|| Vec::with_capacity(count));
        let mut seen = (count > 1).then(BTreeSet::new);
        for _ in 0..count {
            let topic_partition = TopicPartition::new(
                TopicId::new(cursor.u128("telemetry topic ID")?),
                LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
            );
            let envelope_len = cursor.u32("telemetry envelope length")? as usize;
            let envelope_start = cursor.offset;
            let envelope =
                TelemetryEnvelope::decode(cursor.bytes(envelope_len, "telemetry envelope")?)
                    .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            let envelope_range = envelope_start..cursor.offset;
            let transient_len = cursor.u32("transient context length")? as usize;
            let transient_start = cursor.offset;
            let transient_context = if transient_len == 0 {
                None
            } else {
                Some(Arc::<[u8]>::from(
                    cursor.bytes(transient_len, "transient context")?,
                ))
            };
            let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
            if topic_partition.topic_id != envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            if let Some(seen) = &mut seen
                && !seen.insert(topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            if let Some(wire_ranges) = wire_ranges.as_mut() {
                wire_ranges.push((envelope_range, transient_range));
            }
            partitions.push(NativePartitionAppend {
                topic_partition,
                envelope,
                transient_context,
            });
        }
        cursor.finish()?;
        Ok((Self { partitions }, wire_ranges))
    }

    /// Decodes a retryable native v1 append after enforcing its single
    /// partition atomicity boundary.
    pub fn decode_native_append(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        let batch = Self::decode(payload)?;
        if batch.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        Ok(batch)
    }

    /// Decodes one native append and returns the exact verified STEL byte
    /// range from the wire payload. The native server forwards that range to
    /// storage so it does not re-encode an already checksummed envelope.
    pub(crate) fn decode_native_append_with_envelope_range(
        payload: &[u8],
    ) -> Result<(Self, std::ops::Range<usize>), NativeProtocolError> {
        let (batch, mut ranges) = Self::decode_with_envelope_ranges(payload)?;
        if batch.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        let (envelope_range, _) = ranges
            .pop()
            .expect("validated single append has one envelope range");
        Ok((batch, envelope_range))
    }

    /// Decodes one native append while retaining the large envelope sections
    /// as borrowed wire slices. The returned ranges can be converted to
    /// `Bytes` without copying once the input buffer is owned by the caller.
    pub(crate) fn decode_native_append_view_with_ranges(
        payload: &[u8],
    ) -> Result<NativeAppendViewRanges<'_>, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count != 1 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let topic_partition = TopicPartition::new(
            TopicId::new(cursor.u128("telemetry topic ID")?),
            LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
        );
        let envelope_len = cursor.u32("telemetry envelope length")? as usize;
        let envelope_start = cursor.offset;
        let envelope_bytes = cursor.bytes(envelope_len, "telemetry envelope")?;
        let envelope = crate::envelope::TelemetryEnvelope::decode_view(envelope_bytes)
            .map_err(|error| NativeProtocolError::new(error.to_string()))?;
        let transient_len = cursor.u32("transient context length")? as usize;
        let transient_start = cursor.offset;
        let transient_context = if transient_len == 0 {
            None
        } else {
            Some(cursor.bytes(transient_len, "transient context")?)
        };
        let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
        if topic_partition.topic_id != envelope.signal.topic_id() {
            return Err(NativeProtocolError::new(
                "native telemetry partition topic disagrees with its signal",
            ));
        }
        cursor.finish()?;
        Ok((
            NativePartitionAppendView {
                topic_partition,
                signal: envelope.signal,
                tenant: envelope.tenant,
                item_count: envelope.item_count,
                transient_context,
            },
            envelope_start..envelope_start + envelope_bytes.len(),
            transient_range,
        ))
    }

    /// Decodes and validates every partition while retaining large sections as
    /// borrowed wire slices. This is the untracked multi-partition fast path;
    /// retryable appends continue to use the single-partition decoder above.
    pub(crate) fn decode_native_append_views_with_ranges(
        payload: &[u8],
    ) -> Result<NativeAppendViewsRanges<'_>, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count == 0 || count > 256 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "invalid native telemetry partition count or flags",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut views = Vec::with_capacity(count);
        let mut wire_ranges = Vec::with_capacity(count);
        let mut seen = (count > 1).then(BTreeSet::new);
        for _ in 0..count {
            let topic_partition = TopicPartition::new(
                TopicId::new(cursor.u128("telemetry topic ID")?),
                LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
            );
            let envelope_len = cursor.u32("telemetry envelope length")? as usize;
            let envelope_start = cursor.offset;
            let envelope_bytes = cursor.bytes(envelope_len, "telemetry envelope")?;
            let envelope = crate::envelope::TelemetryEnvelope::decode_view(envelope_bytes)
                .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            let transient_len = cursor.u32("transient context length")? as usize;
            let transient_start = cursor.offset;
            let transient_context = if transient_len == 0 {
                None
            } else {
                Some(cursor.bytes(transient_len, "transient context")?)
            };
            let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
            if topic_partition.topic_id != envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            if let Some(seen) = &mut seen
                && !seen.insert(topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            views.push(NativePartitionAppendView {
                topic_partition,
                signal: envelope.signal,
                tenant: envelope.tenant,
                item_count: envelope.item_count,
                transient_context,
            });
            wire_ranges.push((
                envelope_start..envelope_start + envelope_bytes.len(),
                transient_range,
            ));
        }
        cursor.finish()?;
        Ok((views, wire_ranges))
    }
}

/// Returns true when a native append payload uses the signal-aware v1 batch format.
#[must_use]
pub fn is_native_telemetry_batch(payload: &[u8]) -> bool {
    payload.starts_with(&TELEMETRY_BATCH_MAGIC)
}

impl NativeTelemetryAppendAck {
    /// Encodes a native v1 multi-partition acknowledgement.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native acknowledgement exceeds 256 partitions",
            ));
        }
        let mut encoded = Vec::with_capacity(8 + self.partitions.len() * 36);
        encoded.extend_from_slice(&TELEMETRY_ACK_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("ack partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.first_offset.to_le_bytes());
            encoded.extend_from_slice(&partition.last_offset.to_le_bytes());
        }
        Ok(encoded)
    }

    /// Decodes a native v1 multi-partition acknowledgement.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_ACK_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count > 256 || payload[6..8] != [0, 0] || payload.len() != 8 + count * 36 {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement length",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(NativePartitionAck {
                topic_partition: TopicPartition::new(
                    TopicId::new(cursor.u128("ack topic ID")?),
                    LogicalPartitionId::new(cursor.u32("ack partition ID")?),
                ),
                first_offset: cursor.u64("ack first offset")?,
                last_offset: cursor.u64("ack last offset")?,
            });
        }
        cursor.finish()?;
        Ok(Self { partitions })
    }
}
