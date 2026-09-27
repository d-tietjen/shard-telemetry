use super::*;

impl DurableTelemetryStore {
    pub(super) fn append_telemetry_partition(
        &self,
        partition: &crate::NativePartitionAppend,
        wait_for_index: bool,
    ) -> Result<crate::NativePartitionAck, LokiApiError> {
        self.check_append_partitions(std::slice::from_ref(partition))?;
        let payload = partition
            .envelope
            .encode()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        self.append_telemetry_partition_with_encoded_envelope(
            partition,
            Bytes::from(payload),
            wait_for_index,
        )
    }

    pub(super) fn append_telemetry_partition_with_encoded_envelope(
        &self,
        partition: &crate::NativePartitionAppend,
        payload: Bytes,
        wait_for_index: bool,
    ) -> Result<crate::NativePartitionAck, LokiApiError> {
        self.append_telemetry_partition_with_fields(
            partition.topic_partition,
            partition.envelope.item_count,
            payload,
            partition
                .transient_context
                .as_deref()
                .map(Bytes::copy_from_slice),
            wait_for_index,
        )
    }

    pub(super) fn append_telemetry_partition_with_fields(
        &self,
        topic_partition: TopicPartition,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<Bytes>,
        wait_for_index: bool,
    ) -> Result<crate::NativePartitionAck, LokiApiError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = AppendRequest {
            request_id: u128::from(request_id),
            topic_id: topic_partition.topic_id,
            partition_id: topic_partition.partition_id,
            record_count,
            payload,
            durability: self.append_durability,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        };
        let appended = if let Some(transient_context) = transient_context {
            self.engine
                .append_with_durable_sink_context(request, transient_context)
        } else {
            self.engine.append(request)
        }
        .map_err(engine_error)?;
        if wait_for_index {
            let target = DurableSinkCheckpoint {
                topic_partition,
                next_placement_sequence: PlacementSequence::new(
                    appended
                        .placement
                        .sequence
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("placement sequence exhausted"))?,
                ),
                next_offset: LogicalOffset::new(
                    appended
                        .last_offset
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("logical offset exhausted"))?,
                ),
            };
            self.engine
                .wait_for_durable_sink_checkpoint(target)
                .map_err(engine_error)?;
        }
        Ok(crate::NativePartitionAck {
            topic_partition,
            first_offset: appended.first_offset.get(),
            last_offset: appended.last_offset.get(),
        })
    }

    pub(super) fn check_append_partitions(
        &self,
        partitions: &[crate::NativePartitionAppend],
    ) -> Result<(), LokiApiError> {
        if partitions.is_empty() {
            return Ok(());
        }
        self.append_gate.as_ref().map_or(Ok(()), |gate| {
            gate.check_append_partitions(partitions)
                .map_err(LokiApiError::unavailable)
        })
    }

    pub(super) fn check_append_partition_encoded(
        &self,
        topic_partition: TopicPartition,
        record_count: u32,
        encoded_envelope: &[u8],
        transient_context: Option<&[u8]>,
        already_validated: bool,
    ) -> Result<(), LokiApiError> {
        let Some(gate) = &self.append_gate else {
            return Ok(());
        };
        let envelope = if already_validated {
            let view = crate::TelemetryEnvelope::decode_view_after_validation(encoded_envelope)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
            crate::TelemetryEnvelope::new(
                view.signal,
                view.tenant,
                view.item_count,
                view.routing_metadata,
                view.payload,
            )
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?
        } else {
            crate::TelemetryEnvelope::decode(encoded_envelope)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?
        };
        if envelope.item_count != record_count {
            return Err(LokiApiError::bad_request(
                "native append metadata count disagrees with its envelope",
            ));
        }
        gate.check_append_partitions(&[crate::NativePartitionAppend {
            topic_partition,
            envelope,
            transient_context: transient_context.map(Arc::<[u8]>::from),
        }])
        .map_err(LokiApiError::unavailable)
    }
}
