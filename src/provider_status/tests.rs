use super::*;

fn request() -> ProviderStatusRequest {
    ProviderStatusRequest {
        tenant: "tenant-a".into(),
        organization_uuid: Some([7; 16]),
        telemetry_plane: Some("private".into()),
        start_timestamp_unix_nanos: Some(0),
        end_timestamp_unix_nanos: Some(100),
        partitions: vec![ProviderStatusPartition {
            signal: TelemetrySignal::Traces,
            partition_id: 0,
        }],
    }
}

fn unknown_response(request: ProviderStatusRequest) -> ProviderStatusResponse {
    ProviderStatusResponse {
        partitions: request
            .partitions
            .iter()
            .copied()
            .map(LocalPartitionStatus::unknown)
            .collect(),
        request,
        query_retention_coverage: ProviderCoverage::Unknown,
        source_interval_coverage: ProviderCoverage::Unknown,
        scoped_loss_coverage: ProviderCoverage::Unknown,
        ha_epoch: ProviderCoverage::Unknown,
        indexed_quorum: ProviderCoverage::Unknown,
    }
}

#[test]
fn provider_status_codec_checks_declared_counts_before_allocating_and_preserves_context() {
    let request = request();
    let encoded = encode_native_provider_status_request(&request).expect("bounded selection");
    assert_eq!(
        decode_native_provider_status_request(&encoded).expect("selection decode"),
        request
    );
    for end in 0..encoded.len() {
        assert!(decode_native_provider_status_request(&encoded[..end]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_native_provider_status_request(&trailing).is_err());
    let mut huge_count = encoded.clone();
    huge_count[10..12].copy_from_slice(&u16::MAX.to_le_bytes());
    assert!(decode_native_provider_status_request(&huge_count).is_err());
    let mut invalid_flags = encoded.clone();
    invalid_flags[4] |= 0x80;
    assert!(decode_native_provider_status_request(&invalid_flags).is_err());
    let mut invalid_utf8 = encoded.clone();
    invalid_utf8[44] = 0xff;
    assert!(decode_native_provider_status_request(&invalid_utf8).is_err());
    let mut duplicate = request.clone();
    duplicate.partitions.push(duplicate.partitions[0]);
    assert!(encode_native_provider_status_request(&duplicate).is_err());
    assert!(
        decode_native_provider_status_request(&vec![0; MAX_PROVIDER_STATUS_REQUEST_BYTES + 1])
            .is_err()
    );
}

#[test]
fn provider_status_codec_bounds_maximum_selection_and_rejects_unproven_claims() {
    let mut request = request();
    request.tenant = "t".repeat(1_024);
    request.telemetry_plane = Some("p".repeat(64));
    request.partitions = [
        TelemetrySignal::Logs,
        TelemetrySignal::Traces,
        TelemetrySignal::Metrics,
    ]
    .into_iter()
    .flat_map(|signal| {
        (0..256).map(move |partition_id| ProviderStatusPartition {
            signal,
            partition_id,
        })
    })
    .collect();
    assert_eq!(request.partitions.len(), MAX_PROVIDER_STATUS_PARTITIONS);
    let encoded_request =
        encode_native_provider_status_request(&request).expect("maximum bounded selection");
    assert!(encoded_request.len() <= MAX_PROVIDER_STATUS_REQUEST_BYTES);
    let response = unknown_response(request);
    let encoded = encode_native_provider_status_response(&response)
        .expect("maximum bounded unknown response");
    assert!(encoded.len() <= MAX_PROVIDER_STATUS_RESPONSE_BYTES);
    assert_eq!(
        decode_native_provider_status_response(&encoded).expect("maximum response decode"),
        response
    );
    for end in [0, 7, encoded.len() - 1] {
        assert!(decode_native_provider_status_response(&encoded[..end]).is_err());
    }
    let mut forged = encoded.clone();
    forged[8 + encoded_request.len() + 3] = 1;
    assert!(
        decode_native_provider_status_response(&forged).is_err(),
        "a peer cannot advertise an unsupported HA epoch proof"
    );
    let mut huge_request_length = encoded.clone();
    huge_request_length[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_native_provider_status_response(&huge_request_length).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_native_provider_status_response(&trailing).is_err());
}

#[test]
fn provider_status_codec_rejects_indexed_frontiers_outside_stable_wal() {
    let mut response = unknown_response(request());
    response.partitions[0] = LocalPartitionStatus {
        partition: response.request.partitions[0],
        population: LocalPartitionPopulation::Allocated,
        progress: Some(LocalPartitionProgress {
            wal_retained_start: 0,
            allocated_end: 2,
            contiguous_log_end: 2,
            last_stable_offset: 1,
            replicated_high_watermark: 2,
            indexed_end: Some(1),
            indexed_next_placement_sequence: Some(1),
        }),
    };
    let bytes =
        encode_native_provider_status_response(&response).expect("real coordinate constraints");
    assert_eq!(
        decode_native_provider_status_response(&bytes).expect("different stable/replicated ends"),
        response
    );
    let mut forged_sequence = bytes.clone();
    let end = forged_sequence.len();
    forged_sequence[end - 8..].copy_from_slice(&2_u64.to_le_bytes());
    assert!(
        decode_native_provider_status_response(&forged_sequence).is_err(),
        "a native placement cannot consume zero logical records"
    );
    response.partitions[0]
        .progress
        .as_mut()
        .expect("progress")
        .indexed_end = Some(2);
    assert!(
        encode_native_provider_status_response(&response).is_err(),
        "an applied end beyond stable WAL is unknown, not index proof"
    );
    response.partitions[0]
        .progress
        .as_mut()
        .expect("progress")
        .indexed_end = None;
    assert!(
        encode_native_provider_status_response(&response).is_err(),
        "both checkpoint coordinates must be present together"
    );
}

#[test]
fn provider_status_opcode_keeps_existing_frame_and_describe_encoding() {
    let payload = encode_native_provider_status_request(&request()).expect("selection");
    let frame =
        crate::NativeFrame::request(crate::NativeOpcode::ProviderStatus, 17, payload.clone())
            .expect("native frame");
    let header = crate::NativeFrameHeader::decode(&frame.header.encode()).expect("native header");
    assert_eq!(header.opcode, crate::NativeOpcode::ProviderStatus);
    header
        .verify_payload(&payload)
        .expect("status uses existing checksum validation");
    let capabilities = crate::NativeCapabilities {
        protocol_version: 1,
        append_v1: true,
        logical_partitions: [1; 3],
        query_logs: true,
        query_metrics: true,
        query_traces: true,
        query_series: false,
        append_queryable: true,
    };
    let old =
        crate::encode_native_capabilities(&capabilities).expect("unchanged Describe contract");
    assert_eq!(
        crate::decode_native_capabilities(&old).expect("existing Describe decoder"),
        capabilities
    );
}
