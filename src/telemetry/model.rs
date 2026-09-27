use super::*;

/// Fixed shard-stream topic for log records.
pub const LOGS_TOPIC_ID: TopicId = TopicId::new(0x0000_0000_3156_5f53_474f_4c5f_4c45_5453);
/// Fixed shard-stream topic for spans.
pub const TRACES_TOPIC_ID: TopicId = TopicId::new(0x0000_3156_5f53_4543_4152_545f_4c45_5453);
/// Fixed shard-stream topic for metric points.
pub const METRICS_TOPIC_ID: TopicId = TopicId::new(0x0031_565f_5343_4952_5445_4d5f_4c45_5453);

/// Telemetry signal stored by ShardTelemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum TelemetrySignal {
    /// OpenTelemetry logs and Loki streams.
    Logs = 1,
    /// OpenTelemetry spans and Tempo-compatible traces.
    Traces = 2,
    /// OpenTelemetry and Prometheus metric points.
    Metrics = 3,
}

impl TelemetrySignal {
    /// Returns the stable shard-stream topic assigned to this signal.
    #[must_use]
    pub const fn topic_id(self) -> TopicId {
        match self {
            Self::Logs => LOGS_TOPIC_ID,
            Self::Traces => TRACES_TOPIC_ID,
            Self::Metrics => METRICS_TOPIC_ID,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> TelemetryResult<Self> {
        match value {
            1 => Ok(Self::Logs),
            2 => Ok(Self::Traces),
            3 => Ok(Self::Metrics),
            _ => Err(TelemetryError::InvalidTelemetryEnvelope(
                "unknown telemetry signal",
            )),
        }
    }
}

/// Exact OpenTelemetry attribute value.
///
/// Floating-point values are represented by their IEEE-754 bits so NaN
/// payloads, negative zero, and infinities survive every storage tier exactly.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TelemetryValue {
    /// An explicitly present `AnyValue` with no selected variant.
    Empty,
    /// UTF-8 string.
    String(Arc<str>),
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer.
    Integer(i64),
    /// Exact IEEE-754 double bits.
    DoubleBits(u64),
    /// Opaque bytes.
    Bytes(Arc<[u8]>),
    /// Ordered, recursively typed array.
    Array(Arc<Vec<TelemetryValue>>),
    /// Ordered key/value list. Order and duplicate keys are retained.
    Map(Arc<Vec<TelemetryAttribute>>),
    /// Development-only OTLP string-table reference retained losslessly.
    StringTableIndex(i32),
}

impl TelemetryValue {
    /// Creates a bit-exact floating-point value.
    #[must_use]
    pub const fn from_f64(value: f64) -> Self {
        Self::DoubleBits(value.to_bits())
    }

    /// Returns this value as a floating-point value when applicable.
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::DoubleBits(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    /// Appends a stable, type-tagged representation for hashing and identity.
    pub(crate) fn append_canonical(&self, output: &mut Vec<u8>) {
        match self {
            Self::Empty => output.push(0),
            Self::String(value) => {
                output.push(1);
                append_bytes(output, value.as_bytes());
            }
            Self::Boolean(value) => {
                output.push(2);
                output.push(u8::from(*value));
            }
            Self::Integer(value) => {
                output.push(3);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::DoubleBits(bits) => {
                output.push(4);
                output.extend_from_slice(&bits.to_le_bytes());
            }
            Self::Bytes(value) => {
                output.push(5);
                append_bytes(output, value);
            }
            Self::Array(values) => {
                output.push(6);
                append_len(output, values.len());
                for value in values.iter() {
                    value.append_canonical(output);
                }
            }
            Self::Map(values) => {
                output.push(7);
                append_len(output, values.len());
                for value in values.iter() {
                    value.append_canonical(output);
                }
            }
            Self::StringTableIndex(value) => {
                output.push(8);
                output.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

impl fmt::Debug for TelemetryValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Empty"),
            Self::String(value) => formatter.debug_tuple("String").field(value).finish(),
            Self::Boolean(value) => formatter.debug_tuple("Boolean").field(value).finish(),
            Self::Integer(value) => formatter.debug_tuple("Integer").field(value).finish(),
            Self::DoubleBits(bits) => formatter
                .debug_struct("Double")
                .field("value", &f64::from_bits(*bits))
                .field("bits", &format_args!("{bits:#018x}"))
                .finish(),
            Self::Bytes(value) => formatter.debug_tuple("Bytes").field(value).finish(),
            Self::Array(value) => formatter.debug_tuple("Array").field(value).finish(),
            Self::Map(value) => formatter.debug_tuple("Map").field(value).finish(),
            Self::StringTableIndex(value) => formatter
                .debug_tuple("StringTableIndex")
                .field(value)
                .finish(),
        }
    }
}

/// One exact OTLP key/value pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TelemetryAttribute {
    /// Literal key. It may be empty when `key_strindex` is populated.
    pub key: Arc<str>,
    /// Profiles string-table key reference, retained even for other signals.
    pub key_strindex: i32,
    /// `None` distinguishes an absent `AnyValue` from [`TelemetryValue::Empty`].
    pub value: Option<TelemetryValue>,
}

impl TelemetryAttribute {
    /// Creates a conventional literal-key attribute.
    #[must_use]
    pub fn new(key: impl Into<Arc<str>>, value: TelemetryValue) -> Self {
        Self {
            key: key.into(),
            key_strindex: 0,
            value: Some(value),
        }
    }

    pub(crate) fn append_canonical(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.key.as_bytes());
        output.extend_from_slice(&self.key_strindex.to_le_bytes());
        match &self.value {
            Some(value) => {
                output.push(1);
                value.append_canonical(output);
            }
            None => output.push(0),
        }
    }

    /// Returns the stable, type-aware identity used to connect the same
    /// metadata key/value across logs, traces, and metrics.
    #[must_use]
    pub fn fingerprint(&self) -> AttributeFingerprint {
        let mut canonical = Vec::new();
        self.append_canonical(&mut canonical);
        AttributeFingerprint(fingerprint128(b"shard-telemetry/attribute/v1", &canonical))
    }
}

/// An OTLP Resource entity reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TelemetryEntityRef {
    /// Schema URL for this entity.
    pub schema_url: Arc<str>,
    /// Entity type.
    pub entity_type: Arc<str>,
    /// Identifying resource attribute keys.
    pub id_keys: Arc<Vec<Arc<str>>>,
    /// Descriptive resource attribute keys.
    pub description_keys: Arc<Vec<Arc<str>>>,
}

/// Exact resource context shared by records from one OTLP resource group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceContext {
    /// Resource attributes in wire order.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Number of resource attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Resource schema URL.
    pub schema_url: Arc<str>,
    /// Resource entity references.
    pub entity_refs: Arc<Vec<TelemetryEntityRef>>,
}

impl ResourceContext {
    /// Returns the exact, content-addressed resource identity shared by every
    /// telemetry signal.
    #[must_use]
    pub fn id(&self) -> ResourceContextId {
        let mut canonical = Vec::new();
        self.append_identity(&mut canonical);
        ResourceContextId(fingerprint128(
            b"shard-telemetry/resource-context/v1",
            &canonical,
        ))
    }

    pub(crate) fn append_identity(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.schema_url.as_bytes());
        output.extend_from_slice(&self.dropped_attributes_count.to_le_bytes());
        let mut attributes = self
            .attributes
            .iter()
            .map(|attribute| {
                let mut encoded = Vec::new();
                attribute.append_canonical(&mut encoded);
                encoded
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        append_len(output, attributes.len());
        for attribute in attributes {
            append_bytes(output, &attribute);
        }
        append_len(output, self.entity_refs.len());
        for entity in self.entity_refs.iter() {
            append_bytes(output, entity.schema_url.as_bytes());
            append_bytes(output, entity.entity_type.as_bytes());
            append_len(output, entity.id_keys.len());
            for key in entity.id_keys.iter() {
                append_bytes(output, key.as_bytes());
            }
            append_len(output, entity.description_keys.len());
            for key in entity.description_keys.iter() {
                append_bytes(output, key.as_bytes());
            }
        }
    }
}

/// Exact instrumentation scope context shared by records from one OTLP scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeContext {
    /// Instrumentation scope name.
    pub name: Arc<str>,
    /// Instrumentation scope version.
    pub version: Arc<str>,
    /// Scope attributes in wire order.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Number of scope attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Scope schema URL.
    pub schema_url: Arc<str>,
}

impl ScopeContext {
    /// Returns the exact, content-addressed instrumentation-scope identity
    /// shared by every telemetry signal.
    #[must_use]
    pub fn id(&self) -> ScopeContextId {
        let mut canonical = Vec::new();
        self.append_identity(&mut canonical);
        ScopeContextId(fingerprint128(
            b"shard-telemetry/scope-context/v1",
            &canonical,
        ))
    }

    pub(crate) fn append_identity(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.name.as_bytes());
        append_bytes(output, self.version.as_bytes());
        append_bytes(output, self.schema_url.as_bytes());
        output.extend_from_slice(&self.dropped_attributes_count.to_le_bytes());
        let mut attributes = self
            .attributes
            .iter()
            .map(|attribute| {
                let mut encoded = Vec::new();
                attribute.append_canonical(&mut encoded);
                encoded
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        append_len(output, attributes.len());
        for attribute in attributes {
            append_bytes(output, &attribute);
        }
    }
}
