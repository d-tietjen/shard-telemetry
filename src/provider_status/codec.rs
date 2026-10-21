use super::*;

const REQUEST_MAGIC: &[u8; 4] = b"STP1";
const RESPONSE_MAGIC: &[u8; 4] = b"STP2";
const REQUEST_HEADER_BYTES: usize = 44;
const PARTITION_BYTES: usize = 5;
const STATUS_BYTES: usize = 63;

/// Encodes a bounded status selection without changing existing Describe messages.
pub fn encode_native_provider_status_request(
    request: &ProviderStatusRequest,
) -> Result<Vec<u8>, NativeProtocolError> {
    request.validate()?;
    let plane = request.telemetry_plane.as_deref().unwrap_or("");
    let mut output = Vec::with_capacity(
        REQUEST_HEADER_BYTES
            + request.tenant.len()
            + plane.len()
            + request.partitions.len() * PARTITION_BYTES,
    );
    output.extend_from_slice(REQUEST_MAGIC);
    output.push(
        u8::from(request.organization_uuid.is_some())
            | (u8::from(request.telemetry_plane.is_some()) << 1)
            | (u8::from(request.start_timestamp_unix_nanos.is_some()) << 2)
            | (u8::from(request.end_timestamp_unix_nanos.is_some()) << 3),
    );
    output.push(0);
    output.extend_from_slice(&(request.tenant.len() as u16).to_le_bytes());
    output.extend_from_slice(&(plane.len() as u16).to_le_bytes());
    output.extend_from_slice(&(request.partitions.len() as u16).to_le_bytes());
    output.extend_from_slice(&request.organization_uuid.unwrap_or([0; 16]));
    output.extend_from_slice(
        &request
            .start_timestamp_unix_nanos
            .unwrap_or(0)
            .to_le_bytes(),
    );
    output.extend_from_slice(&request.end_timestamp_unix_nanos.unwrap_or(0).to_le_bytes());
    output.extend_from_slice(request.tenant.as_bytes());
    output.extend_from_slice(plane.as_bytes());
    for partition in &request.partitions {
        output.push(partition.signal as u8);
        output.extend_from_slice(&partition.partition_id.to_le_bytes());
    }
    Ok(output)
}

/// Decodes status selections with fixed count/length checks before any allocation.
pub fn decode_native_provider_status_request(
    bytes: &[u8],
) -> Result<ProviderStatusRequest, NativeProtocolError> {
    if bytes.len() < REQUEST_HEADER_BYTES || bytes.len() > MAX_PROVIDER_STATUS_REQUEST_BYTES {
        return Err(NativeProtocolError::new(
            "provider status request byte bound exceeded or header truncated",
        ));
    }
    let mut input = Reader::new(bytes);
    if input.take(4)? != REQUEST_MAGIC {
        return Err(NativeProtocolError::new(
            "invalid provider status request magic",
        ));
    }
    let flags = input.byte()?;
    if flags & !15 != 0 || input.byte()? != 0 {
        return Err(NativeProtocolError::new(
            "invalid provider status request flags",
        ));
    }
    let tenant_length = usize::from(input.u16()?);
    let plane_length = usize::from(input.u16()?);
    let count = usize::from(input.u16()?);
    if !(1..=1_024).contains(&tenant_length)
        || plane_length > 64
        || !(1..=MAX_PROVIDER_STATUS_PARTITIONS).contains(&count)
        || bytes.len()
            != REQUEST_HEADER_BYTES + tenant_length + plane_length + count * PARTITION_BYTES
    {
        return Err(NativeProtocolError::new(
            "provider status request count or length mismatch",
        ));
    }
    let organization: [u8; 16] = input
        .take(16)?
        .try_into()
        .map_err(|_| NativeProtocolError::new("truncated organization UUID"))?;
    let start = input.u64()?;
    let end = input.u64()?;
    if (flags & 1 == 0 && organization != [0; 16])
        || (flags & 2 == 0 && plane_length != 0)
        || (flags & 4 == 0 && start != 0)
        || (flags & 8 == 0 && end != 0)
    {
        return Err(NativeProtocolError::new(
            "provider status absent context has nonzero data",
        ));
    }
    let tenant = input.string(tenant_length)?;
    let plane = input.string(plane_length)?;
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        partitions.push(input.partition()?);
    }
    let request = ProviderStatusRequest {
        tenant,
        organization_uuid: (flags & 1 != 0).then_some(organization),
        telemetry_plane: (flags & 2 != 0).then_some(plane),
        start_timestamp_unix_nanos: (flags & 4 != 0).then_some(start),
        end_timestamp_unix_nanos: (flags & 8 != 0).then_some(end),
        partitions,
    };
    request.validate()?;
    Ok(request)
}

/// Encodes local observations; all unsupported proof dimensions are explicitly unknown.
pub fn encode_native_provider_status_response(
    response: &ProviderStatusResponse,
) -> Result<Vec<u8>, NativeProtocolError> {
    response.validate()?;
    let request = encode_native_provider_status_request(&response.request)?;
    let size = 15 + request.len() + response.partitions.len() * STATUS_BYTES;
    if size > MAX_PROVIDER_STATUS_RESPONSE_BYTES {
        return Err(NativeProtocolError::new(
            "provider status response byte bound exceeded",
        ));
    }
    let mut output = Vec::with_capacity(size);
    output.extend_from_slice(RESPONSE_MAGIC);
    output.extend_from_slice(&(request.len() as u32).to_le_bytes());
    output.extend_from_slice(&request);
    // Query retention, source interval, scoped loss, HA epoch, indexed quorum:
    // version one provides no proof for any of these dimensions.
    output.extend_from_slice(&[0; 5]);
    output.extend_from_slice(&(response.partitions.len() as u16).to_le_bytes());
    for status in &response.partitions {
        output.push(status.partition.signal as u8);
        output.extend_from_slice(&status.partition.partition_id.to_le_bytes());
        output.push(match status.population {
            LocalPartitionPopulation::Unknown => 0,
            LocalPartitionPopulation::VerifiedEmpty => 1,
            LocalPartitionPopulation::Allocated => 2,
        });
        output.push(match status.progress {
            None => 0,
            Some(progress) => 1 | (u8::from(progress.indexed_end.is_some()) << 1),
        });
        let progress = status.progress.unwrap_or(LocalPartitionProgress {
            wal_retained_start: 0,
            allocated_end: 0,
            contiguous_log_end: 0,
            last_stable_offset: 0,
            replicated_high_watermark: 0,
            indexed_end: None,
            indexed_next_placement_sequence: None,
        });
        for value in [
            progress.wal_retained_start,
            progress.allocated_end,
            progress.contiguous_log_end,
            progress.last_stable_offset,
            progress.replicated_high_watermark,
            progress.indexed_end.unwrap_or(0),
            progress.indexed_next_placement_sequence.unwrap_or(0),
        ] {
            output.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(output)
}

/// Decodes status responses without trusting remote vector counts or coverage claims.
pub fn decode_native_provider_status_response(
    bytes: &[u8],
) -> Result<ProviderStatusResponse, NativeProtocolError> {
    if bytes.len() < 15 || bytes.len() > MAX_PROVIDER_STATUS_RESPONSE_BYTES {
        return Err(NativeProtocolError::new(
            "provider status response byte bound exceeded or header truncated",
        ));
    }
    let mut input = Reader::new(bytes);
    if input.take(4)? != RESPONSE_MAGIC {
        return Err(NativeProtocolError::new(
            "invalid provider status response magic",
        ));
    }
    let request_length = input.u32()? as usize;
    if request_length > MAX_PROVIDER_STATUS_REQUEST_BYTES {
        return Err(NativeProtocolError::new(
            "provider status echoed request byte bound exceeded",
        ));
    }
    let request = decode_native_provider_status_request(input.take(request_length)?)?;
    if input.take(5)? != [0; 5] {
        return Err(NativeProtocolError::new(
            "unsupported provider status coverage claim",
        ));
    }
    let count = usize::from(input.u16()?);
    if count != request.partitions.len() || input.remaining() != count * STATUS_BYTES {
        return Err(NativeProtocolError::new(
            "provider status response count or length mismatch",
        ));
    }
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        let partition = input.partition()?;
        let population = match input.byte()? {
            0 => LocalPartitionPopulation::Unknown,
            1 => LocalPartitionPopulation::VerifiedEmpty,
            2 => LocalPartitionPopulation::Allocated,
            _ => {
                return Err(NativeProtocolError::new(
                    "invalid provider status population",
                ));
            }
        };
        let flags = input.byte()?;
        if !matches!(flags, 0 | 1 | 3) {
            return Err(NativeProtocolError::new(
                "invalid provider status progress flags",
            ));
        }
        let wal_retained_start = input.u64()?;
        let allocated_end = input.u64()?;
        let contiguous_log_end = input.u64()?;
        let last_stable_offset = input.u64()?;
        let replicated_high_watermark = input.u64()?;
        let indexed_end = input.u64()?;
        let indexed_next_placement_sequence = input.u64()?;
        if (flags == 0
            && [
                wal_retained_start,
                allocated_end,
                contiguous_log_end,
                last_stable_offset,
                replicated_high_watermark,
                indexed_end,
                indexed_next_placement_sequence,
            ] != [0; 7])
            || (flags & 2 == 0 && (indexed_end != 0 || indexed_next_placement_sequence != 0))
        {
            return Err(NativeProtocolError::new(
                "provider status absent progress has nonzero data",
            ));
        }
        partitions.push(LocalPartitionStatus {
            partition,
            population,
            progress: (flags & 1 != 0).then_some(LocalPartitionProgress {
                wal_retained_start,
                allocated_end,
                contiguous_log_end,
                last_stable_offset,
                replicated_high_watermark,
                indexed_end: (flags & 2 != 0).then_some(indexed_end),
                indexed_next_placement_sequence: (flags & 2 != 0)
                    .then_some(indexed_next_placement_sequence),
            }),
        });
    }
    let response = ProviderStatusResponse {
        request,
        partitions,
        query_retention_coverage: ProviderCoverage::Unknown,
        source_interval_coverage: ProviderCoverage::Unknown,
        scoped_loss_coverage: ProviderCoverage::Unknown,
        ha_epoch: ProviderCoverage::Unknown,
        indexed_quorum: ProviderCoverage::Unknown,
    };
    response.validate()?;
    Ok(response)
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], NativeProtocolError> {
        if count > self.remaining() {
            return Err(NativeProtocolError::new(
                "truncated provider status payload",
            ));
        }
        let start = self.position;
        self.position += count;
        Ok(&self.bytes[start..self.position])
    }
    fn byte(&mut self) -> Result<u8, NativeProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, NativeProtocolError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| NativeProtocolError::new("truncated u16"))?,
        ))
    }
    fn u32(&mut self) -> Result<u32, NativeProtocolError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| NativeProtocolError::new("truncated u32"))?,
        ))
    }
    fn u64(&mut self) -> Result<u64, NativeProtocolError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| NativeProtocolError::new("truncated u64"))?,
        ))
    }
    fn string(&mut self, count: usize) -> Result<String, NativeProtocolError> {
        std::str::from_utf8(self.take(count)?)
            .map(str::to_owned)
            .map_err(|_| NativeProtocolError::new("invalid provider status UTF-8"))
    }
    fn partition(&mut self) -> Result<ProviderStatusPartition, NativeProtocolError> {
        let signal = TelemetrySignal::from_wire(self.byte()?)
            .map_err(|error| NativeProtocolError::new(error.to_string()))?;
        Ok(ProviderStatusPartition {
            signal,
            partition_id: self.u32()?,
        })
    }
}
