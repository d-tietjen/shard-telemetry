use super::*;

impl DurableTelemetryStore {
    /// Appends every partition in one validated native v1 telemetry batch in parallel.
    ///
    /// The response retains request order and contains one acknowledgement per
    /// resulting partition. Any partition failure makes the request retryable;
    /// trace and metric retries resolve idempotently by durable identity.
    pub fn append_telemetry_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let encoded = batch
            .encode()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let (validated, wire_ranges) =
            crate::NativeTelemetryBatch::decode_with_envelope_ranges(&encoded)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        self.append_validated_telemetry_batch_with_encoded_envelopes(
            &validated,
            Bytes::from(encoded),
            &wire_ranges,
            wait_for_index,
        )
    }

    /// Appends envelopes prepared by an in-process trusted transport.
    ///
    /// OTLP decoding has already validated and grouped these envelopes, so
    /// sending them through the native wire codec would only add a full encode
    /// and decode pass before the same partition append work.
    pub(crate) fn append_prepared_telemetry_partitions(
        &self,
        partitions: Vec<crate::NativePartitionAppend>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.append_partitioned_envelopes(partitions, wait_for_index)
    }

    /// Appends one batch under a caller-stable retry ID.
    ///
    /// Matching retries after a connection loss or process restart return the
    /// original acknowledgement. Reusing an ID for different encoded content
    /// is rejected before it can create an ambiguous duplicate.
    pub fn append_telemetry_batch_with_retry_id(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wait_for_index: bool,
        retry_id: u128,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let encoded = batch
            .encode_native_append()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let payload_digest = blake3::hash(&encoded).to_hex().to_string();
        let (validated, envelope_range) =
            crate::NativeTelemetryBatch::decode_native_append_with_envelope_range(&encoded)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let wire = Bytes::from(encoded);
        self.append_validated_telemetry_batch_with_retry_id_and_encoded_envelope(
            &validated,
            wire.slice(envelope_range),
            wait_for_index,
            retry_id,
            payload_digest,
        )
    }

    /// Returns the authoritative local WAL batches beginning at `start_offset`.
    ///
    /// The returned envelopes are checksum-validated and retain their exact
    /// signal payload bytes, making this suitable for a store-and-forward
    /// uploader. It does not mutate retention or acknowledge offload progress.
    pub fn fetch_telemetry_batches(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: u32,
    ) -> Result<Vec<FetchedTelemetryBatch>, LokiApiError> {
        if max_bytes == 0 {
            return Err(LokiApiError::bad_request(
                "telemetry WAL fetch max_bytes must be nonzero",
            ));
        }
        let batches = self
            .engine
            .fetch(FetchRequest {
                request_id: 0,
                topic_id: topic_partition.topic_id,
                partition_id: topic_partition.partition_id,
                start_offset,
                max_bytes,
                mode: FetchMode::Ordered,
            })
            .map_err(engine_error)?;
        batches
            .into_iter()
            .map(|batch| {
                let envelope = crate::TelemetryEnvelope::decode(&batch.payload)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if envelope.signal.topic_id() != topic_partition.topic_id {
                    return Err(LokiApiError::internal(
                        "telemetry WAL batch topic disagrees with its signal envelope",
                    ));
                }
                Ok(FetchedTelemetryBatch {
                    topic_partition,
                    first_offset: batch.first_offset,
                    last_offset: batch.last_offset,
                    envelope,
                })
            })
            .collect()
    }

    /// Returns the first locally retained offset for an offload source partition.
    pub fn telemetry_partition_start_offset(
        &self,
        topic_partition: TopicPartition,
    ) -> Result<LogicalOffset, LokiApiError> {
        self.engine
            .watermarks(topic_partition)
            .map(|watermarks| watermarks.log_start)
            .map_err(engine_error)
    }

    /// Lists all configured local signal partitions in stable signal/partition order.
    #[must_use]
    pub fn telemetry_partitions(&self) -> Vec<TopicPartition> {
        [
            crate::LOGS_TOPIC_ID,
            crate::TRACES_TOPIC_ID,
            crate::METRICS_TOPIC_ID,
        ]
        .into_iter()
        .flat_map(|topic_id| self.signal_partitions(topic_id))
        .collect()
    }

    /// Directly appends normalized log events for an embedded producer.
    ///
    /// The method performs routing and durable append work only when the
    /// producer's background exporter calls it; logging call sites should never
    /// invoke it directly on their hot path.
    pub fn append_log_events(
        &self,
        tenant: &str,
        events: Vec<crate::OtlpLogEvent>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        if events.is_empty() {
            return Ok(crate::NativeTelemetryAppendAck {
                partitions: Vec::new(),
            });
        }
        if tenant.is_empty() {
            return Err(LokiApiError::bad_request(
                "embedded log tenant must not be empty",
            ));
        }
        let router = self.telemetry_router;
        let mut routed = foldhash::HashMap::<TopicPartition, Vec<crate::OtlpLogEvent>>::new();
        for event in events {
            let identity = event.resource.id().get().to_le_bytes();
            let partition = router.log(tenant, event.trace_id, &identity);
            routed.entry(partition).or_default().push(event);
        }
        let partitions = self.install_append_parallelism(|| {
            routed
                .into_par_iter()
                .map(|(topic_partition, events)| {
                    crate::signal_ingest::prepare_log_envelope_owned_with_context(tenant, events)
                        .map(
                            |(envelope, transient_context)| crate::NativePartitionAppend {
                                topic_partition,
                                envelope,
                                transient_context: Some(transient_context),
                            },
                        )
                        .map_err(|error| LokiApiError::bad_request(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        self.append_partitioned_envelopes(partitions, wait_for_index)
    }

    /// Directly appends normalized trace spans for an embedded producer.
    ///
    /// Trace batches are routed by trace identity and remain tenant-isolated
    /// even when multiple tenants hash to the same physical partition. The
    /// path bypasses OTLP and native-protocol encode/decode work; callers pass
    /// already validated [`crate::OtlpSpanEvent`] values from a bounded
    /// exporter worker.
    pub fn append_trace_events(
        &self,
        events: Vec<crate::OtlpSpanEvent>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        if events.is_empty() {
            return Ok(crate::NativeTelemetryAppendAck {
                partitions: Vec::new(),
            });
        }
        let router = self.telemetry_router;
        // Trace blocks carry one envelope tenant. Partition keys therefore
        // include the tenant, while `append_partitioned_envelopes` retains the
        // physical partition's single append order below.
        let mut routed =
            foldhash::HashMap::<(TopicPartition, Arc<str>), Vec<crate::OtlpSpanEvent>>::new();
        for event in events {
            let partition = router.trace(event.tenant(), event.trace_id());
            routed
                .entry((partition, Arc::from(event.tenant())))
                .or_default()
                .push(event);
        }
        let partitions = self.install_append_parallelism(|| {
            routed
                .into_par_iter()
                .map(|((topic_partition, _tenant), events)| {
                    crate::prepare_trace_envelope(topic_partition, events)
                        .map(|envelope| crate::NativePartitionAppend {
                            topic_partition,
                            envelope,
                            transient_context: None,
                        })
                        .map_err(|error| LokiApiError::bad_request(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        self.append_partitioned_envelopes(partitions, wait_for_index)
    }

    /// Directly appends normalized metric points for an embedded producer.
    ///
    /// It preserves native metric kinds, histogram buckets, labels, resource
    /// context, and series identity without an OTLP encode/decode round trip.
    pub fn append_metric_point(
        &self,
        point: crate::DurableMetricPoint,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let event = crate::OtlpMetricEvent::from_durable(point)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let partition = self
            .telemetry_router
            .metric(event.tenant(), event.series_fingerprint());
        let envelope = crate::prepare_metric_envelope(partition, vec![event])
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        self.append_partitioned_envelopes(
            vec![crate::NativePartitionAppend {
                topic_partition: partition,
                envelope,
                transient_context: None,
            }],
            wait_for_index,
        )
    }

    /// Directly appends normalized metric points for an embedded producer.
    ///
    /// It preserves native metric kinds, histogram buckets, labels, resource
    /// context, and series identity without an OTLP encode/decode round trip.
    pub fn append_metric_points(
        &self,
        points: Vec<crate::DurableMetricPoint>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        if points.is_empty() {
            return Ok(crate::NativeTelemetryAppendAck {
                partitions: Vec::new(),
            });
        }
        let router = self.telemetry_router;
        // Fast-telemetry snapshots frequently contain one metric series. Keep
        // that common embedded case on a direct lane: no series map, fan-out
        // map, or Rayon scheduling is needed for one already owned point.
        if points.len() == 1 {
            return self.append_metric_point(
                points
                    .into_iter()
                    .next()
                    .expect("one point was checked above"),
                wait_for_index,
            );
        }
        // A metric chunk is columnar storage for exactly one canonical series,
        // even when multiple series route to the same logical partition. Group
        // before creating envelopes so embedded fast exporters can snapshot an
        // entire fast-telemetry runtime in one direct call.
        let mut partitions = foldhash::HashMap::<
            (TopicPartition, crate::SeriesFingerprint),
            Vec<crate::OtlpMetricEvent>,
        >::new();
        for point in points {
            let event = crate::OtlpMetricEvent::from_durable(point)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
            let series = event.series_fingerprint();
            let partition = router.metric(event.tenant(), series);
            partitions
                .entry((partition, series))
                .or_default()
                .push(event);
        }
        let partitions = self.install_append_parallelism(|| {
            partitions
                .into_par_iter()
                .map(|((topic_partition, _series), events)| {
                    crate::prepare_metric_envelope(topic_partition, events)
                        .map(|envelope| crate::NativePartitionAppend {
                            topic_partition,
                            envelope,
                            transient_context: None,
                        })
                        .map_err(|error| LokiApiError::bad_request(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        self.append_partitioned_envelopes(partitions, wait_for_index)
    }

    pub(super) fn append_partitioned_envelopes(
        &self,
        mut partitions: Vec<crate::NativePartitionAppend>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partitions(&partitions)?;
        // A storage engine partition has a single append order. Metric series
        // commonly share a partition, so execute envelopes from one partition
        // serially while retaining parallelism between independent partitions.
        // This also avoids the native-v1 encode/decode validation round trip:
        // `prepare_*_envelope` already constructed self-validating envelopes
        // from typed in-process data.
        if partitions.len() == 1 {
            let acknowledgement = self.append_telemetry_partition(
                &partitions.pop().expect("one partition was checked above"),
                wait_for_index,
            )?;
            return Ok(crate::NativeTelemetryAppendAck {
                partitions: vec![acknowledgement],
            });
        }
        let mut by_partition = BTreeMap::<TopicPartition, Vec<crate::NativePartitionAppend>>::new();
        for partition in partitions {
            by_partition
                .entry(partition.topic_partition)
                .or_default()
                .push(partition);
        }
        let acknowledgements = self
            .install_append_parallelism(|| {
                by_partition
                    .into_par_iter()
                    .map(|(_, partitions)| {
                        partitions
                            .into_iter()
                            .map(|partition| {
                                self.append_telemetry_partition(&partition, wait_for_index)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?
            .into_iter()
            .flatten()
            .collect();
        Ok(crate::NativeTelemetryAppendAck {
            partitions: acknowledgements,
        })
    }

    /// Appends a native batch that has already been decoded and checksum
    /// validated by the native protocol server.
    pub(crate) fn append_validated_telemetry_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partitions(&batch.partitions)?;
        let acknowledgements = if batch.partitions.len() == 1 {
            vec![self.append_telemetry_partition(&batch.partitions[0], wait_for_index)?]
        } else {
            self.append_submission_pool.install(|| {
                batch
                    .partitions
                    .par_iter()
                    .map(|partition| self.append_telemetry_partition(partition, wait_for_index))
                    .collect::<Result<Vec<_>, _>>()
            })?
        };
        Ok(crate::NativeTelemetryAppendAck {
            partitions: acknowledgements,
        })
    }

    pub(crate) fn append_validated_telemetry_batch_with_encoded_envelopes(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wire: Bytes,
        wire_ranges: &[(std::ops::Range<usize>, Option<std::ops::Range<usize>>)],
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        if batch.partitions.len() != wire_ranges.len() {
            return Err(LokiApiError::internal(
                "validated native batch wire ranges do not match its partitions",
            ));
        }
        self.check_append_partitions(&batch.partitions)?;
        let append = |(partition, (envelope_range, transient_range)): (
            &crate::NativePartitionAppend,
            &(std::ops::Range<usize>, Option<std::ops::Range<usize>>),
        )| {
            self.append_telemetry_partition_with_fields(
                partition.topic_partition,
                partition.envelope.item_count,
                wire.slice(envelope_range.clone()),
                transient_range
                    .as_ref()
                    .map(|range| wire.slice(range.clone())),
                wait_for_index,
            )
        };
        let acknowledgements = if batch.partitions.len() == 1 {
            vec![append((&batch.partitions[0], &wire_ranges[0]))?]
        } else {
            self.append_submission_pool.install(|| {
                batch
                    .partitions
                    .par_iter()
                    .zip(wire_ranges.par_iter())
                    .map(append)
                    .collect::<Result<Vec<_>, _>>()
            })?
        };
        Ok(crate::NativeTelemetryAppendAck {
            partitions: acknowledgements,
        })
    }

    /// Appends a native retryable batch while forwarding its already verified
    /// wire envelope. Native frame decoding has authenticated and parsed this
    /// exact STEL slice, so re-encoding it here would only repeat allocation
    /// and checksum work before shard-stream persists the same bytes.
    pub(crate) fn append_validated_telemetry_batch_with_retry_id_and_encoded_envelope(
        &self,
        batch: &crate::NativeTelemetryBatch,
        encoded_envelope: Bytes,
        wait_for_index: bool,
        retry_id: u128,
        payload_digest: String,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partitions(&batch.partitions)?;
        if batch.partitions.len() != 1 {
            return Err(LokiApiError::bad_request(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        match self.append_receipts.reserve(retry_id, &payload_digest)? {
            AppendReceiptReservation::Existing(acknowledgement) => return Ok(acknowledgement),
            AppendReceiptReservation::Reserved => {}
        }
        let acknowledgement = match self.append_telemetry_partition_with_encoded_envelope(
            &batch.partitions[0],
            encoded_envelope,
            wait_for_index,
        ) {
            Ok(acknowledgement) => crate::NativeTelemetryAppendAck {
                partitions: vec![acknowledgement],
            },
            Err(error) => {
                self.append_receipts.abandon(retry_id);
                return Err(error);
            }
        };
        if let Err(error) =
            self.append_receipts
                .complete(retry_id, payload_digest, acknowledgement.clone())
        {
            self.append_receipts.abandon(retry_id);
            return Err(error);
        }
        Ok(acknowledgement)
    }

    /// Appends one native retryable wire envelope without materializing an
    /// owned `TelemetryEnvelope` for the normal ungated server path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_validated_native_metadata_with_retry_id_and_encoded_envelope(
        &self,
        topic_partition: TopicPartition,
        record_count: u32,
        encoded_envelope: Bytes,
        transient_context: Option<Bytes>,
        wait_for_index: bool,
        retry_id: u128,
        payload_digest: String,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partition_encoded(
            topic_partition,
            record_count,
            &encoded_envelope,
            transient_context.as_deref(),
            true,
        )?;
        match self.append_receipts.reserve(retry_id, &payload_digest)? {
            AppendReceiptReservation::Existing(acknowledgement) => return Ok(acknowledgement),
            AppendReceiptReservation::Reserved => {}
        }
        let acknowledgement = match self.append_telemetry_partition_with_fields(
            topic_partition,
            record_count,
            encoded_envelope,
            transient_context,
            wait_for_index,
        ) {
            Ok(acknowledgement) => crate::NativeTelemetryAppendAck {
                partitions: vec![acknowledgement],
            },
            Err(error) => {
                self.append_receipts.abandon(retry_id);
                return Err(error);
            }
        };
        if let Err(error) =
            self.append_receipts
                .complete(retry_id, payload_digest, acknowledgement.clone())
        {
            self.append_receipts.abandon(retry_id);
            return Err(error);
        }
        Ok(acknowledgement)
    }

    pub(crate) fn append_validated_native_metadata_with_encoded_envelope(
        &self,
        topic_partition: TopicPartition,
        record_count: u32,
        encoded_envelope: Bytes,
        transient_context: Option<Bytes>,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partition_encoded(
            topic_partition,
            record_count,
            &encoded_envelope,
            transient_context.as_deref(),
            true,
        )?;
        Ok(crate::NativeTelemetryAppendAck {
            partitions: vec![self.append_telemetry_partition_with_fields(
                topic_partition,
                record_count,
                encoded_envelope,
                transient_context,
                wait_for_index,
            )?],
        })
    }

    pub(crate) fn append_validated_native_metadata_with_encoded_envelopes(
        &self,
        partitions: &[crate::native_protocol::NativeEncodedPartitionAppend],
        wire: Bytes,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        if partitions.is_empty() {
            return Err(LokiApiError::bad_request(
                "native telemetry batch requires at least one partition",
            ));
        }
        if self.append_gate.is_some() {
            for partition in partitions {
                let envelope = wire.slice(partition.envelope_range.clone());
                let transient_context = partition
                    .transient_range
                    .as_ref()
                    .map(|range| wire.slice(range.clone()));
                self.check_append_partition_encoded(
                    partition.topic_partition,
                    partition.item_count,
                    &envelope,
                    transient_context.as_deref(),
                    true,
                )?;
            }
        }
        let append = |partition: &crate::native_protocol::NativeEncodedPartitionAppend| {
            self.append_telemetry_partition_with_fields(
                partition.topic_partition,
                partition.item_count,
                wire.slice(partition.envelope_range.clone()),
                partition
                    .transient_range
                    .as_ref()
                    .map(|range| wire.slice(range.clone())),
                wait_for_index,
            )
        };
        let acknowledgements = if partitions.len() == 1 {
            vec![append(&partitions[0])?]
        } else {
            self.append_submission_pool.install(|| {
                partitions
                    .par_iter()
                    .map(append)
                    .collect::<Result<Vec<_>, _>>()
            })?
        };
        Ok(crate::NativeTelemetryAppendAck {
            partitions: acknowledgements,
        })
    }
}
