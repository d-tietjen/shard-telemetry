use super::*;

const ARC_ALLOCATION_OVERHEAD: usize = 2 * size_of::<usize>();

/// Estimates the storage occupied by one Arc-backed string without encoding it.
pub(crate) fn estimated_arc_str_bytes(value: &Arc<str>) -> usize {
    ARC_ALLOCATION_OVERHEAD.saturating_add(value.len())
}

/// Estimates the Arc, Vec header, and element-buffer storage for a vector.
pub(crate) fn estimated_arc_vec_storage<T>(capacity: usize) -> usize {
    ARC_ALLOCATION_OVERHEAD
        .saturating_add(size_of::<Vec<T>>())
        .saturating_add(capacity.saturating_mul(size_of::<T>()))
}

pub(super) fn estimated_telemetry_value_heap_bytes(value: &TelemetryValue) -> usize {
    match value {
        TelemetryValue::String(value) => estimated_arc_str_bytes(value),
        TelemetryValue::Bytes(value) => ARC_ALLOCATION_OVERHEAD.saturating_add(value.len()),
        TelemetryValue::Array(values) => {
            estimated_arc_vec_storage::<TelemetryValue>(values.capacity()).saturating_add(
                values
                    .iter()
                    .map(estimated_telemetry_value_heap_bytes)
                    .sum(),
            )
        }
        TelemetryValue::Map(values) => {
            estimated_arc_vec_storage::<TelemetryAttribute>(values.capacity())
                .saturating_add(values.iter().map(estimated_telemetry_attribute_bytes).sum())
        }
        TelemetryValue::Empty
        | TelemetryValue::Boolean(_)
        | TelemetryValue::Integer(_)
        | TelemetryValue::DoubleBits(_)
        | TelemetryValue::StringTableIndex(_) => 0,
    }
}

pub(crate) fn estimated_telemetry_attribute_bytes(attribute: &TelemetryAttribute) -> usize {
    size_of::<TelemetryAttribute>()
        .saturating_add(estimated_arc_str_bytes(&attribute.key))
        .saturating_add(
            attribute
                .value
                .as_ref()
                .map_or(0, estimated_telemetry_value_heap_bytes),
        )
}

pub(crate) fn estimated_telemetry_attributes_bytes(
    attributes: &Arc<Vec<TelemetryAttribute>>,
) -> usize {
    estimated_arc_vec_storage::<TelemetryAttribute>(attributes.capacity()).saturating_add(
        attributes
            .iter()
            .map(estimated_telemetry_attribute_bytes)
            .sum(),
    )
}

pub(super) fn estimated_entity_ref_bytes(entity: &TelemetryEntityRef) -> usize {
    size_of::<TelemetryEntityRef>()
        .saturating_add(estimated_arc_str_bytes(&entity.schema_url))
        .saturating_add(estimated_arc_str_bytes(&entity.entity_type))
        .saturating_add(estimated_arc_str_vec_bytes(&entity.id_keys))
        .saturating_add(estimated_arc_str_vec_bytes(&entity.description_keys))
}

pub(super) fn estimated_arc_str_vec_bytes(values: &Arc<Vec<Arc<str>>>) -> usize {
    estimated_arc_vec_storage::<Arc<str>>(values.capacity())
        .saturating_add(values.iter().map(estimated_arc_str_bytes).sum())
}

pub(crate) fn estimated_resource_context_bytes(context: &Arc<ResourceContext>) -> usize {
    size_of::<ResourceContext>()
        .saturating_add(estimated_telemetry_attributes_bytes(&context.attributes))
        .saturating_add(estimated_arc_str_bytes(&context.schema_url))
        .saturating_add(estimated_arc_vec_storage::<TelemetryEntityRef>(
            context.entity_refs.capacity(),
        ))
        .saturating_add(
            context
                .entity_refs
                .iter()
                .map(estimated_entity_ref_bytes)
                .sum(),
        )
}

pub(crate) fn estimated_scope_context_bytes(context: &Arc<ScopeContext>) -> usize {
    size_of::<ScopeContext>()
        .saturating_add(estimated_arc_str_bytes(&context.name))
        .saturating_add(estimated_arc_str_bytes(&context.version))
        .saturating_add(estimated_telemetry_attributes_bytes(&context.attributes))
        .saturating_add(estimated_arc_str_bytes(&context.schema_url))
}

macro_rules! context_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub(super) u128);

        impl $name {
            /// Returns the process-independent identity bits.
            #[must_use]
            pub const fn get(self) -> u128 {
                self.0
            }

            /// Reconstructs an identity previously returned by [`Self::get`].
            #[must_use]
            pub const fn from_raw(value: u128) -> Self {
                Self(value)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:032x}", self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:032x}", self.0)
            }
        }
    };
}

context_identity!(
    ResourceContextId,
    "Stable 128-bit identity of an exact resource context."
);
context_identity!(
    ScopeContextId,
    "Stable 128-bit identity of an exact instrumentation scope context."
);
context_identity!(
    AttributeFingerprint,
    "Stable 128-bit identity of one exact typed metadata key/value."
);

/// A validated 128-bit OpenTelemetry trace ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TraceId([u8; 16]);

impl TraceId {
    /// Creates a trace ID. All-zero IDs are rejected.
    pub fn from_bytes(bytes: [u8; 16]) -> TelemetryResult<Self> {
        if bytes == [0; 16] {
            return Err(TelemetryError::InvalidTraceId);
        }
        Ok(Self(bytes))
    }

    /// Parses a trace ID from its OTLP byte representation.
    pub fn from_slice(bytes: &[u8]) -> TelemetryResult<Self> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| TelemetryError::InvalidTraceId)?;
        Self::from_bytes(bytes)
    }

    /// Returns the exact ID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// A validated 64-bit OpenTelemetry span ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SpanId([u8; 8]);

impl SpanId {
    /// Creates a span ID. All-zero IDs are rejected.
    pub fn from_bytes(bytes: [u8; 8]) -> TelemetryResult<Self> {
        if bytes == [0; 8] {
            return Err(TelemetryError::InvalidSpanId);
        }
        Ok(Self(bytes))
    }

    /// Parses a span ID from its OTLP byte representation.
    pub fn from_slice(bytes: &[u8]) -> TelemetryResult<Self> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| TelemetryError::InvalidSpanId)?;
        Self::from_bytes(bytes)
    }

    /// Returns the exact ID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Stable 128-bit identity of a metric series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeriesFingerprint(u128);

impl SeriesFingerprint {
    /// Returns the fingerprint bits.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    pub(crate) const fn from_raw(value: u128) -> Self {
        Self(value)
    }

    pub(crate) fn from_canonical(canonical: &[u8]) -> Self {
        let digest = blake3::hash(canonical);
        Self(u128::from_le_bytes(
            digest.as_bytes()[..16].try_into().expect("fixed digest"),
        ))
    }
}

pub(super) fn append_len(output: &mut Vec<u8>, len: usize) {
    output.extend_from_slice(&(len as u64).to_le_bytes());
}

pub(super) fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    append_len(output, bytes.len());
    output.extend_from_slice(bytes);
}

pub(super) fn fingerprint128(domain: &[u8], canonical: &[u8]) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&[0]);
    hasher.update(&(canonical.len() as u64).to_le_bytes());
    hasher.update(canonical);
    let digest = hasher.finalize();
    u128::from_le_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest contains 16 bytes"),
    )
}

pub(super) fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = [0_u8; 32];
    debug_assert!(bytes.len() <= encoded.len() / 2);
    for (index, byte) in bytes.iter().copied().enumerate() {
        encoded[index * 2] = HEX[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    let encoded = std::str::from_utf8(&encoded[..bytes.len() * 2])
        .expect("hex table contains only valid UTF-8");
    formatter.write_str(encoded)
}
