//! One charged MessagePack packed-metadata parse, retaining the complete native parent.
use super::bounded_decompression::{
    FrameContinuation, FrameParent, FrameWork, PreparedDecompressedFrame,
};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_metadata::PreparedRecoveredStructuralMetadataBytes;
use crate::{OwnedLogDecompressionLimits, TelemetryRecoveryError, TelemetryRecoveryUsage};
use bytes::Bytes;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::time::Instant;

// Pass this static str BY VALUE. Its selected custom::<&str> conversion requests
// exactly its actual byte length; no default Serde formatter is used here.
const REJECT: &str = "bounded typed log metadata decode refused";

/// Additional finite traversal ceilings, sharing the already-spent setup ledger.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredPackedMetadataLimits {
    /// Largest actual decompressed MessagePack input.
    pub max_input_bytes: usize,
    /// Largest authoritative outer record count.
    pub max_records: usize,
    /// Actual retained arena nodes, including keys, ignored values and drafts.
    pub max_nodes: usize,
    /// Maximum recursive seed depth; this is not a native stack-size proof.
    pub max_depth: usize,
    /// Maximum admitted visitor callbacks.
    pub max_callbacks: usize,
    /// Cumulative prepaid arena/error allocation requests, including returned excess.
    pub max_output_bytes: usize,
    /// Cumulative local allocation/framing slots, never JSON records/files.
    pub max_slots: usize,
    /// Explicit scans, framing, copies, scalar checks and identifier comparisons.
    pub max_work_bytes: usize,
}
/// Actual local fees and entered operations; original cumulative fees are separate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredPackedMetadataUsage {
    /// Complete owned continuation/return/catch/closure framing.
    pub continuation_bytes: usize,
    /// Cumulative admitted arena/error allocation requests.
    pub output_bytes: usize,
    /// Cumulative admitted local allocation/framing slots.
    pub slots: usize,
    /// Cumulative admitted explicit parser work.
    pub work_bytes: usize,
    /// Actual calls into the selected Serde deserializer, after admission.
    pub seed_calls: usize,
    /// Visitor callbacks admitted on the original work.
    pub callbacks: usize,
    /// Actual entered by-value static error conversions, at most one.
    pub syntax_calls: usize,
}
/// A packed record schema; field numbers follow the actual persisted declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredPackedRecord {
    /// The six persisted packed-metadata tables.
    Metadata,
    /// One persisted fourteen-field log-metadata row.
    Row,
    /// An ordered key, string-table index and optional value.
    Attribute,
    /// A resource context, including its entity references.
    Resource,
    /// An instrumentation scope context.
    Scope,
    /// A resource entity reference.
    Entity,
}
/// Exact TelemetryValue discriminants, without constructing Arc-backed values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredPackedValue {
    /// An explicitly present empty value.
    Empty,
    /// A UTF8 string payload.
    String,
    /// A boolean payload.
    Boolean,
    /// A signed64-bit payload.
    Integer,
    /// Exact IEEE754 bits carried in a u64.
    DoubleBits,
    /// An ordered byte sequence.
    Bytes,
    /// An ordered sequence of recursively typed values.
    Array,
    /// Ordered attributes, preserving duplicate keys.
    Map,
    /// A signed32-bit string-table reference.
    StringTableIndex,
}
/// Actual scalar/container state, including incomplete and ignored-value drafts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveredPackedNodeKind {
    /// A charged draft that has not produced an accepted scalar/container.
    Pending,
    /// Actual unit/nil.
    Nil,
    /// An Option discriminator; a present child is field zero.
    Optional(bool),
    /// Actual boolean.
    Boolean(bool),
    /// Checked signed scalar.
    Signed(i64),
    /// Checked unsigned scalar.
    Unsigned(u64),
    /// An exact ignored float's bits, without a lossy conversion.
    Float32(u32),
    /// An exact ignored double's bits.
    Float64(u64),
    /// An ignored character; the selected RMP parser does not emit this callback.
    Character(char),
    /// Valid UTF8 source range into the exact retained raw metadata.
    Text(Range<usize>),
    /// Borrowed byte/identifier range; scalar byte ARRAYs use individual children.
    Binary(Range<usize>),
    /// An ordered sequence; children include actual scalar bytes for Bytes/IDs.
    Sequence,
    /// A declared persisted record, with field-numbered children and retained keys.
    Record(RecoveredPackedRecord),
    /// An exact value tag; an associated payload is field zero.
    Value(RecoveredPackedValue),
    /// An ignored map, including charged key/value children.
    Map,
    /// An ignored extension/newtype wrapper, retaining its actual child.
    Newtype,
}
/// Actual incoming callback value, captured before callback fees or E checks.
/// This is an observation, never an accepted typed scalar or parser permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveredPackedInput {
    /// Actual unit callback.
    Unit,
    /// Actual absent-option callback.
    None,
    /// Actual present-option callback, before its inner child enters.
    Some,
    /// Actual boolean, before shape validation.
    Boolean(bool),
    /// Actual signed scalar, including defensive128-bit callbacks.
    Signed(i128),
    /// Actual unsigned scalar, including defensive128-bit callbacks.
    Unsigned(u128),
    /// Actual IEEE754 single-precision bits.
    Float32(u32),
    /// Actual IEEE754 double-precision bits.
    Float64(u64),
    /// Actual character; unreachable in the selected borrowed adapter graph.
    Character(char),
    /// Actual borrowed text/bytes delivery, including rejected UTF8 fallback.
    Borrowed {
        /// Checked source range; None records a defensive out-of-backing delivery.
        range: Option<Range<usize>>,
        /// Actual delivered byte length, even if the range check failed.
        len: usize,
        /// True for a str callback, false for bytes/fallback callbacks.
        text: bool,
    },
    /// Actual sequence arrival and its native optional size hint.
    Sequence(Option<usize>),
    /// Actual map arrival and its native optional size hint.
    Map(Option<usize>),
    /// Actual extension/newtype arrival.
    Newtype,
    /// Defensive enum arrival; the selected parser uses no native enum adapter.
    Enum,
    /// Defensive owned String; its actual owner is held separately by the child.
    OwnedString {
        /// Actual delivered length.
        len: usize,
        /// Actual delivered capacity.
        capacity: usize,
    },
    /// Defensive owned Vec; its actual owner is held separately by the child.
    OwnedBytes {
        /// Actual delivered length.
        len: usize,
        /// Actual delivered capacity.
        capacity: usize,
    },
}
/// One retained node. Links are indices into this same owner's immutable arena.
#[derive(Clone, Debug)]
pub struct RecoveredPackedNode {
    kind: RecoveredPackedNodeKind,
    field: Option<usize>,
    first: Option<usize>,
    last: Option<usize>,
    next: Option<usize>,
    children: usize,
    lookup: Option<usize>,
    returned: Option<bool>,
    delivered: Option<RecoveredPackedInput>,
}
impl RecoveredPackedNode {
    /// Actual scalar/container, possibly produced before a later refusal.
    pub fn kind(&self) -> &RecoveredPackedNodeKind {
        &self.kind
    }
    /// Declaration field/sequence ordinal; map identifier nodes have no field.
    pub fn field(&self) -> Option<usize> {
        self.field
    }
    /// First actual child node in source/semantic visitation order.
    pub fn first_child(&self) -> Option<usize> {
        self.first
    }
    /// Next sibling node.
    pub fn next_sibling(&self) -> Option<usize> {
        self.next
    }
    /// Actual deserializer result status; None means it has not returned.
    pub fn returned_ok(&self) -> Option<bool> {
        self.returned
    }
    /// Actual borrowed scalar input, including one rejected during UTF8 fallback.
    pub fn captured_input(&self) -> Option<Range<usize>> {
        match &self.delivered {
            Some(RecoveredPackedInput::Borrowed { range, .. }) => range.clone(),
            _ => None,
        }
    }
    /// Actual callback observation before admission/validation; distinct from kind.
    pub fn delivered(&self) -> Option<&RecoveredPackedInput> {
        self.delivered.as_ref()
    }
}
/// First actual typed traversal refusal, separate from the real MessagePack error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredPackedRejection {
    /// An invalid finite parser policy.
    Policy,
    /// An actual local checked counter or fee ceiling refused.
    Limits,
    /// The entered value does not have the required typed shape.
    Shape,
    /// An entered scalar is outside the persisted numeric domain.
    Numeric,
    /// An entered variant identifier is unknown.
    Identifier,
    /// The actual row sequence differs from the authoritative count.
    Count,
    /// A borrowed callback did not refer to the retained backing.
    SourceRange,
    /// The actual local bytes-as-text validation refused.
    Utf8,
    /// The actual arena reservation returned an allocation error.
    Allocation,
    /// The original recovery work or E refused.
    Recovery,
    /// A defensive owned callback outside the selected borrowed graph.
    OwnedCallback,
}
#[derive(Debug)]
enum Failure {
    Admission,
    Parser,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for Failure {}
#[derive(Clone, Copy)]
enum List {
    Bodies,
    AttributeSets,
    Resources,
    Scopes,
    Strings,
    Rows,
    Attributes,
    Entities,
    Texts,
    Values,
    Bytes,
    Id16,
    Id8,
}
#[derive(Clone, Copy)]
enum Optional {
    Row,
    Value,
    Id16,
    Id8,
}
#[derive(Clone, Copy)]
enum Key {
    Field(RecoveredPackedRecord),
    Variant,
}
#[derive(Clone, Copy)]
enum Shape {
    Record(RecoveredPackedRecord),
    List(List),
    Value,
    ScalarValueAdapter,
    Payload(RecoveredPackedValue),
    Text,
    Bool,
    I32,
    I64,
    U8,
    U32,
    U64,
    Optional(Optional),
    Key(Key),
    Ignore,
}
const META_FIELDS: &[&str] = &[
    "bodies",
    "attribute_sets",
    "resources",
    "scopes",
    "strings",
    "rows",
];
const ROW_FIELDS: &[&str] = &[
    "observed_timestamp_delta",
    "body_id",
    "attributes_id",
    "resource_id",
    "scope_id",
    "severity_number",
    "severity_text_id",
    "dropped_attributes_count",
    "flags",
    "trace_id",
    "trace_id_from_fields",
    "span_id",
    "span_id_from_fields",
    "event_name_id",
];
const ATTR_FIELDS: &[&str] = &["key", "key_strindex", "value"];
const RESOURCE_FIELDS: &[&str] = &[
    "attributes",
    "dropped_attributes_count",
    "schema_url",
    "entity_refs",
];
const SCOPE_FIELDS: &[&str] = &[
    "name",
    "version",
    "attributes",
    "dropped_attributes_count",
    "schema_url",
];
const ENTITY_FIELDS: &[&str] = &["schema_url", "entity_type", "id_keys", "description_keys"];
const VARIANTS: &[&str] = &[
    "Empty",
    "String",
    "Boolean",
    "Integer",
    "DoubleBits",
    "Bytes",
    "Array",
    "Map",
    "StringTableIndex",
];
fn fields(record: RecoveredPackedRecord) -> &'static [&'static str] {
    use RecoveredPackedRecord::*;
    match record {
        Metadata => META_FIELDS,
        Row => ROW_FIELDS,
        Attribute => ATTR_FIELDS,
        Resource => RESOURCE_FIELDS,
        Scope => SCOPE_FIELDS,
        Entity => ENTITY_FIELDS,
    }
}
fn variant(index: usize) -> Option<RecoveredPackedValue> {
    use RecoveredPackedValue::*;
    Some(match index {
        0 => Empty,
        1 => String,
        2 => Boolean,
        3 => Integer,
        4 => DoubleBits,
        5 => Bytes,
        6 => Array,
        7 => Map,
        8 => StringTableIndex,
        _ => return None,
    })
}
fn field_shape(record: RecoveredPackedRecord, field: usize) -> Shape {
    use RecoveredPackedRecord as R;
    use Shape::*;
    match (record, field) {
        (R::Metadata, 0) => List(self::List::Bodies),
        (R::Metadata, 1) => List(self::List::AttributeSets),
        (R::Metadata, 2) => List(self::List::Resources),
        (R::Metadata, 3) => List(self::List::Scopes),
        (R::Metadata, 4) => List(self::List::Strings),
        (R::Metadata, 5) => List(self::List::Rows),
        (R::Row, 0) => I64,
        (R::Row, 5) => I32,
        (R::Row, 9) => Optional(self::Optional::Id16),
        (R::Row, 11) => Optional(self::Optional::Id8),
        (R::Row, 10 | 12) => Bool,
        (R::Row, _) => U32,
        (R::Attribute, 0) => Text,
        (R::Attribute, 1) => I32,
        (R::Attribute, 2) => Optional(self::Optional::Value),
        (R::Resource, 0) | (R::Scope, 2) => List(self::List::Attributes),
        (R::Resource, 1) | (R::Scope, 3) => U32,
        (R::Resource, 3) => List(self::List::Entities),
        (R::Entity, 2 | 3) => List(self::List::Texts),
        _ => Text,
    }
}
fn element(list: List) -> Shape {
    match list {
        List::Bodies | List::Values => Shape::Value,
        List::AttributeSets => Shape::List(List::Attributes),
        List::Resources => Shape::Record(RecoveredPackedRecord::Resource),
        List::Scopes => Shape::Record(RecoveredPackedRecord::Scope),
        List::Strings | List::Texts => Shape::Text,
        List::Rows => Shape::Optional(Optional::Row),
        List::Attributes => Shape::Record(RecoveredPackedRecord::Attribute),
        List::Entities => Shape::Record(RecoveredPackedRecord::Entity),
        List::Bytes | List::Id16 | List::Id8 => Shape::U8,
    }
}
fn payload(value: RecoveredPackedValue) -> Shape {
    use RecoveredPackedValue as V;
    match value {
        V::Empty => Shape::Payload(V::Empty),
        V::String => Shape::Text,
        V::Boolean => Shape::Bool,
        V::Integer => Shape::I64,
        V::DoubleBits => Shape::U64,
        V::Bytes => Shape::List(List::Bytes),
        V::Array => Shape::List(List::Values),
        V::Map => Shape::List(List::Attributes),
        V::StringTableIndex => Shape::I32,
    }
}
struct Arena {
    limits: RecoveredPackedMetadataLimits,
    count: usize,
    absent: bool,
    nodes: Vec<RecoveredPackedNode>,
    usage: RecoveredPackedMetadataUsage,
    rejection: Option<RecoveredPackedRejection>,
    recovery: Option<TelemetryRecoveryError>,
    reservation: Option<std::result::Result<(), TryReserveError>>,
    parser: Option<std::result::Result<(), rmp_serde::decode::Error>>,
    utf8: Option<std::result::Result<(), std::str::Utf8Error>>,
    foreign_string: Option<String>,
    foreign_bytes: Option<Vec<u8>>,
}
impl Arena {
    fn reason(&mut self, why: RecoveredPackedRejection) {
        if self.rejection.is_none() {
            self.rejection = Some(why);
        }
    }
    fn pay(
        &mut self,
        owner: &mut RecoveryFrameWork,
        bytes: usize,
        slots: usize,
        work: usize,
    ) -> bool {
        let next = self
            .usage
            .output_bytes
            .checked_add(bytes)
            .zip(self.usage.slots.checked_add(slots))
            .zip(self.usage.work_bytes.checked_add(work));
        let Some(((bytes_total, slots_total), work_total)) = next else {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        };
        if bytes_total > self.limits.max_output_bytes
            || bytes_total > isize::MAX as usize
            || slots_total > self.limits.max_slots
            || work_total > self.limits.max_work_bytes
        {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        }
        if let Err(error) = owner.pay(bytes, slots, work) {
            if self.recovery.is_none() {
                self.recovery = Some(error);
            }
            self.reason(RecoveredPackedRejection::Recovery);
            return false;
        }
        self.usage.output_bytes = bytes_total;
        self.usage.slots = slots_total;
        self.usage.work_bytes = work_total;
        true
    }
    fn observed(&mut self, owner: &mut RecoveryFrameWork, phase: &'static str) -> bool {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(owner.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        if let Err(error) = owner.check() {
            if self.recovery.is_none() {
                self.recovery = Some(error);
            }
            self.reason(RecoveredPackedRejection::Recovery);
            false
        } else {
            true
        }
    }
    fn reserve(&mut self, owner: &mut RecoveryFrameWork) -> bool {
        if self.nodes.len() < self.nodes.capacity() {
            return true;
        }
        let Some(requested) = self
            .nodes
            .capacity()
            .max(1)
            .checked_mul(2)
            .map(|n| n.min(self.limits.max_nodes))
        else {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        };
        if requested <= self.nodes.len() {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        }
        let Some(bytes) = requested.checked_mul(size_of::<RecoveredPackedNode>()) else {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        };
        let Some(work) = self
            .nodes
            .len()
            .checked_mul(size_of::<RecoveredPackedNode>())
            .and_then(|old| old.checked_add(bytes))
        else {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        };
        if !self.pay(owner, bytes, requested, work) {
            return false;
        }
        self.reservation = Some(self.nodes.try_reserve_exact(requested - self.nodes.len()));
        if !self.observed(owner, "packed-reservation") {
            return false;
        }
        if self.reservation.as_ref().is_some_and(|r| r.is_err()) {
            self.reason(RecoveredPackedRejection::Allocation);
            return false;
        }
        let extra = self.nodes.capacity().saturating_sub(requested);
        let Some(bytes) = extra.checked_mul(size_of::<RecoveredPackedNode>()) else {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        };
        if !self.pay(owner, bytes, extra, bytes) {
            return false;
        }
        if self.nodes.capacity() > self.limits.max_nodes {
            self.reason(RecoveredPackedRejection::Limits);
            return false;
        }
        true
    }
    fn node(
        &mut self,
        owner: &mut RecoveryFrameWork,
        parent: Option<usize>,
        field: Option<usize>,
    ) -> Option<usize> {
        if self.nodes.len() >= self.limits.max_nodes || !self.reserve(owner) {
            self.reason(RecoveredPackedRejection::Limits);
            return None;
        }
        if !self.pay(owner, 0, 1, size_of::<RecoveredPackedNode>()) {
            return None;
        }
        let index = self.nodes.len();
        self.nodes.push(RecoveredPackedNode {
            kind: RecoveredPackedNodeKind::Pending,
            field,
            first: None,
            last: None,
            next: None,
            children: 0,
            lookup: None,
            returned: None,
            delivered: None,
        });
        if let Some(parent) = parent {
            let previous = self.nodes[parent].last;
            if let Some(previous) = previous {
                self.nodes[previous].next = Some(index);
            } else {
                self.nodes[parent].first = Some(index);
            }
            self.nodes[parent].last = Some(index);
            self.nodes[parent].children += 1;
        }
        if !self.observed(owner, "packed-node") {
            return None;
        }
        Some(index)
    }
    fn run(
        &mut self,
        input: Option<&[u8]>,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> std::result::Result<(), Failure> {
        type Reader<'a> = rmp_serde::decode::ReadRefReader<'a, [u8]>;
        let carriers = [
            size_of::<Continued>(),
            size_of::<PreparedRecoveredPackedMetadata>(),
            size_of::<RetainedRecoveredPackedMetadataError>(),
            size_of::<PackedResult>(),
            size_of::<PackedResult>(),
            size_of::<std::result::Result<(), Box<dyn std::any::Any + Send>>>(),
            size_of::<std::result::Result<(), Failure>>(),
            size_of::<rmp_serde::Deserializer<Reader<'_>>>(),
            size_of::<Seed<'_, '_>>(),
            size_of::<std::result::Result<(), rmp_serde::decode::Error>>(),
            size_of::<[usize; 11]>(),
        ];
        let all = carriers
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or(Failure::Admission)?;
        let supplied = size_of::<Continued>()
            .checked_mul(3)
            .ok_or(Failure::Admission)?;
        let framing = fixed
            .and_then(|n| n.checked_add(all.saturating_sub(supplied)))
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or(Failure::Admission)?;
        owner
            .pay(framing, carriers.len(), framing)
            .map_err(|error| {
                self.recovery = Some(error);
                self.reason(RecoveredPackedRejection::Recovery);
                Failure::Admission
            })?;
        self.usage.continuation_bytes = framing;
        let Some(input) = input else {
            self.absent = true;
            return Ok(());
        };
        let ceilings = [
            self.limits.max_input_bytes,
            self.limits.max_records,
            self.limits.max_nodes,
            self.limits.max_depth,
            self.limits.max_callbacks,
            self.limits.max_output_bytes,
            self.limits.max_slots,
            self.limits.max_work_bytes,
        ];
        if ceilings.iter().any(|n| *n == 0 || *n > isize::MAX as usize)
            || self.limits.max_depth > u16::MAX as usize
            || input.len() > self.limits.max_input_bytes
            || self.count > self.limits.max_records
        {
            self.reason(RecoveredPackedRejection::Policy);
            return Err(Failure::Admission);
        }
        // Prepay native STR UTF8 plus at most one bytes-as-text fallback scan,
        // and the sole possible static terminal Syntax conversion, before entry.
        let scan = input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(REJECT.len()))
            .ok_or(Failure::Admission)?;
        if !self.pay(owner, REJECT.len(), 1, scan) {
            return Err(Failure::Admission);
        }
        let root = self.node(owner, None, None).ok_or(Failure::Admission)?;
        let mut decoder = rmp_serde::Deserializer::from_read_ref(input);
        let result = Seed {
            arena: self,
            owner,
            input,
            node: root,
            shape: Shape::Record(RecoveredPackedRecord::Metadata),
            depth: 0,
        }
        .deserialize(&mut decoder);
        self.parser = Some(result);
        if !self.observed(owner, "packed-result") {
            return Err(Failure::Admission);
        }
        if self.parser.as_ref().is_some_and(|result| result.is_ok()) {
            Ok(())
        } else {
            Err(Failure::Parser)
        }
    }
}
struct Seed<'a, 'de> {
    arena: &'a mut Arena,
    owner: &'a mut RecoveryFrameWork,
    input: &'de [u8],
    node: usize,
    shape: Shape,
    depth: usize,
}
impl Seed<'_, '_> {
    fn reject<E: de::Error>(&mut self, why: RecoveredPackedRejection) -> E {
        self.arena.reason(why);
        self.arena.usage.syntax_calls += 1;
        E::custom::<&str>(REJECT)
    }
    fn paid<E: de::Error>(
        &mut self,
        bytes: usize,
        slots: usize,
        work: usize,
    ) -> std::result::Result<(), E> {
        if self.arena.pay(self.owner, bytes, slots, work) {
            Ok(())
        } else {
            Err(self.reject(RecoveredPackedRejection::Limits))
        }
    }
    fn callback<T, E: de::Error>(&mut self) -> std::result::Result<(), E> {
        if self.arena.usage.callbacks >= self.arena.limits.max_callbacks {
            return Err(self.reject(RecoveredPackedRejection::Limits));
        }
        let bytes = size_of::<T>()
            .checked_add(size_of::<Self>())
            .and_then(|n| n.checked_add(size_of::<std::result::Result<(), E>>()))
            .ok_or_else(|| self.reject::<E>(RecoveredPackedRejection::Limits))?;
        self.paid(0, 1, bytes)?;
        self.arena.usage.callbacks += 1;
        if self.arena.observed(self.owner, "packed-callback") {
            Ok(())
        } else {
            Err(self.reject(RecoveredPackedRejection::Recovery))
        }
    }
    fn finish<E: de::Error>(
        &mut self,
        kind: RecoveredPackedNodeKind,
    ) -> std::result::Result<(), E> {
        self.arena.nodes[self.node].kind = kind;
        if self.arena.observed(self.owner, "packed-scalar") {
            Ok(())
        } else {
            Err(self.reject(RecoveredPackedRejection::Recovery))
        }
    }
    fn deliver(&mut self, value: RecoveredPackedInput, phase: &'static str) {
        // This node and capture write/carrier were admitted before the native
        // seed invocation. Keep arrival separate from typed acceptance.
        self.arena.nodes[self.node].delivered = Some(value);
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(self.owner.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
    }
    fn capture_bytes(&mut self, bytes: &[u8], text: bool) {
        let start = (bytes.as_ptr() as usize).checked_sub(self.input.as_ptr() as usize);
        let range = start
            .and_then(|start| start.checked_add(bytes.len()).map(|end| start..end))
            .filter(|range| range.end <= self.input.len());
        self.deliver(
            RecoveredPackedInput::Borrowed {
                range,
                len: bytes.len(),
                text,
            },
            "packed-bytes-delivered",
        );
    }
    fn key<E: de::Error>(&mut self, key: Key, bytes: &[u8]) -> std::result::Result<(), E> {
        let names = match key {
            Key::Field(record) => fields(record),
            Key::Variant => VARIANTS,
        };
        let comparisons = names
            .iter()
            .try_fold(0usize, |n, s| n.checked_add(s.len()))
            .ok_or_else(|| self.reject::<E>(RecoveredPackedRejection::Limits))?;
        self.paid(0, 1, comparisons)?;
        self.arena.nodes[self.node].lookup = names.iter().position(|name| name.as_bytes() == bytes);
        if matches!(key, Key::Variant) && self.arena.nodes[self.node].lookup.is_none() {
            return Err(self.reject(RecoveredPackedRejection::Identifier));
        }
        Ok(())
    }
    fn text<E: de::Error>(mut self, bytes: &[u8], text: bool) -> std::result::Result<(), E> {
        self.capture_bytes(bytes, text);
        self.callback::<&[u8], E>()?;
        let range = self.arena.nodes[self.node]
            .captured_input()
            .ok_or_else(|| self.reject::<E>(RecoveredPackedRejection::SourceRange))?;
        match self.shape {
            Shape::Key(key) => {
                self.key(key, bytes)?;
                self.finish(RecoveredPackedNodeKind::Binary(range))
            }
            Shape::Value => {
                self.key(Key::Variant, bytes)?;
                if self.arena.nodes[self.node].lookup != Some(0) {
                    return Err(self.reject(RecoveredPackedRejection::Shape));
                }
                self.finish(RecoveredPackedNodeKind::Value(RecoveredPackedValue::Empty))
            }
            Shape::Text => {
                self.arena.utf8 = Some(std::str::from_utf8(bytes).map(|_| ()));
                if self.arena.utf8.as_ref().is_some_and(|r| r.is_err()) {
                    return Err(self.reject(RecoveredPackedRejection::Utf8));
                }
                self.finish(RecoveredPackedNodeKind::Text(range))
            }
            Shape::Ignore => self.finish(RecoveredPackedNodeKind::Binary(range)),
            _ => Err(self.reject(RecoveredPackedRejection::Shape)),
        }
    }
    fn signed<E: de::Error>(mut self, delivered: i128) -> std::result::Result<(), E> {
        self.deliver(
            RecoveredPackedInput::Signed(delivered),
            "packed-signed-delivered",
        );
        self.callback::<i128, E>()?;
        let value = i64::try_from(delivered)
            .map_err(|_| self.reject::<E>(RecoveredPackedRejection::Numeric))?;
        let kind = match self.shape {
            Shape::I64 | Shape::Ignore => RecoveredPackedNodeKind::Signed(value),
            Shape::I32 if i32::try_from(value).is_ok() => RecoveredPackedNodeKind::Signed(value),
            Shape::U8 if u8::try_from(value).is_ok() => {
                RecoveredPackedNodeKind::Unsigned(value as u64)
            }
            Shape::U32 if u32::try_from(value).is_ok() => {
                RecoveredPackedNodeKind::Unsigned(value as u64)
            }
            Shape::U64 if value >= 0 => RecoveredPackedNodeKind::Unsigned(value as u64),
            _ => return Err(self.reject(RecoveredPackedRejection::Numeric)),
        };
        self.finish(kind)
    }
    fn unsigned<E: de::Error>(mut self, delivered: u128) -> std::result::Result<(), E> {
        self.deliver(
            RecoveredPackedInput::Unsigned(delivered),
            "packed-unsigned-delivered",
        );
        self.callback::<u128, E>()?;
        let value = u64::try_from(delivered)
            .map_err(|_| self.reject::<E>(RecoveredPackedRejection::Numeric))?;
        let kind = match self.shape {
            Shape::U64 | Shape::Ignore => RecoveredPackedNodeKind::Unsigned(value),
            Shape::U8 if u8::try_from(value).is_ok() => RecoveredPackedNodeKind::Unsigned(value),
            Shape::U32 if u32::try_from(value).is_ok() => RecoveredPackedNodeKind::Unsigned(value),
            Shape::I64 if i64::try_from(value).is_ok() => {
                RecoveredPackedNodeKind::Signed(value as i64)
            }
            Shape::I32 if i32::try_from(value).is_ok() => {
                RecoveredPackedNodeKind::Signed(value as i64)
            }
            Shape::Value if value == 0 => {
                RecoveredPackedNodeKind::Value(RecoveredPackedValue::Empty)
            }
            Shape::Key(key) => {
                let names = match key {
                    Key::Field(record) => fields(record),
                    Key::Variant => VARIANTS,
                };
                self.arena.nodes[self.node].lookup =
                    usize::try_from(value).ok().filter(|n| *n < names.len());
                if matches!(key, Key::Variant) && self.arena.nodes[self.node].lookup.is_none() {
                    return Err(self.reject(RecoveredPackedRejection::Identifier));
                }
                RecoveredPackedNodeKind::Unsigned(value)
            }
            _ => return Err(self.reject(RecoveredPackedRejection::Numeric)),
        };
        self.finish(kind)
    }
}
impl<'de> DeserializeSeed<'de> for Seed<'_, 'de> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<(), D::Error> {
        let Seed {
            arena,
            owner,
            input,
            node,
            shape,
            depth,
        } = self;
        let mut seed = Seed {
            arena,
            owner,
            input,
            node,
            shape,
            depth,
        };
        if depth > seed.arena.limits.max_depth {
            return Err(seed.reject(RecoveredPackedRejection::Limits));
        }
        // Concrete ingress/working/reborrowed visitor carriers, source scalar
        // bindings, incoming adapter and both actual seed/access return shapes.
        // These are explicit framing/copy fees, not a Rust call-stack bound.
        let frames = [
            size_of::<Self>(),
            size_of::<Self>(),
            size_of::<Self>(),
            size_of::<D>(),
            size_of::<(usize, Shape, usize, &[u8])>(),
            size_of::<bool>(),
            size_of::<std::result::Result<(), D::Error>>(),
            size_of::<std::result::Result<Option<()>, D::Error>>(),
            // The actual delivered value argument and Option destination write
            // are prepaid before this native invocation can deliver anything.
            size_of::<RecoveredPackedInput>(),
            size_of::<Option<RecoveredPackedInput>>(),
            // This actual twelve-entry framing table is source state too.
            size_of::<[usize; 12]>(),
            // One marker plus the largest selected numeric/length read;
            // variable string scans were admitted before parser construction.
            1 + size_of::<u64>().max(size_of::<u32>()),
        ];
        let bytes = frames
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or_else(|| seed.reject::<D::Error>(RecoveredPackedRejection::Limits))?;
        seed.paid(0, frames.len(), bytes)?;
        // Classify EACH actual incoming adapter, never an inherited root flag.
        // Unlike a real RMP unsigned0 tag, an actual BIN U8 scalar adapter is
        // not an enum. Invoke it and retain its real delivered scalar, then
        // reject that shape in the explicit visitor rather than dropping it.
        let shape = if matches!(shape, Shape::Value) && decoder.is_human_readable() {
            Shape::ScalarValueAdapter
        } else {
            shape
        };
        let result = {
            let child = Seed {
                arena: &mut *seed.arena,
                owner: &mut *seed.owner,
                input,
                node,
                shape,
                depth,
            };
            // Increment only immediately before the actual selected invocation.
            child.arena.usage.seed_calls += 1;
            match shape {
                Shape::List(_) => decoder.deserialize_seq(child),
                Shape::Optional(_) => decoder.deserialize_option(child),
                _ => decoder.deserialize_any(child),
            }
        };
        seed.arena.nodes[node].returned = Some(result.is_ok());
        result
    }
}
impl<'de> Visitor<'de> for Seed<'_, 'de> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REJECT)
    }
    fn visit_bool<E: de::Error>(mut self, value: bool) -> std::result::Result<(), E> {
        self.deliver(
            RecoveredPackedInput::Boolean(value),
            "packed-bool-delivered",
        );
        self.callback::<bool, E>()?;
        if matches!(self.shape, Shape::Bool | Shape::Ignore) {
            self.finish(RecoveredPackedNodeKind::Boolean(value))
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_i8<E: de::Error>(self, v: i8) -> std::result::Result<(), E> {
        self.signed(i128::from(v))
    }
    fn visit_i16<E: de::Error>(self, v: i16) -> std::result::Result<(), E> {
        self.signed(i128::from(v))
    }
    fn visit_i32<E: de::Error>(self, v: i32) -> std::result::Result<(), E> {
        self.signed(i128::from(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<(), E> {
        self.signed(i128::from(v))
    }
    fn visit_i128<E: de::Error>(self, v: i128) -> std::result::Result<(), E> {
        self.signed(v)
    }
    fn visit_u8<E: de::Error>(self, v: u8) -> std::result::Result<(), E> {
        self.unsigned(u128::from(v))
    }
    fn visit_u16<E: de::Error>(self, v: u16) -> std::result::Result<(), E> {
        self.unsigned(u128::from(v))
    }
    fn visit_u32<E: de::Error>(self, v: u32) -> std::result::Result<(), E> {
        self.unsigned(u128::from(v))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<(), E> {
        self.unsigned(u128::from(v))
    }
    fn visit_u128<E: de::Error>(self, v: u128) -> std::result::Result<(), E> {
        self.unsigned(v)
    }
    fn visit_f32<E: de::Error>(mut self, v: f32) -> std::result::Result<(), E> {
        self.deliver(
            RecoveredPackedInput::Float32(v.to_bits()),
            "packed-float32-delivered",
        );
        self.callback::<f32, E>()?;
        if matches!(self.shape, Shape::Ignore) {
            self.finish(RecoveredPackedNodeKind::Float32(v.to_bits()))
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_f64<E: de::Error>(mut self, v: f64) -> std::result::Result<(), E> {
        self.deliver(
            RecoveredPackedInput::Float64(v.to_bits()),
            "packed-float64-delivered",
        );
        self.callback::<f64, E>()?;
        if matches!(self.shape, Shape::Ignore) {
            self.finish(RecoveredPackedNodeKind::Float64(v.to_bits()))
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_char<E: de::Error>(mut self, v: char) -> std::result::Result<(), E> {
        self.deliver(RecoveredPackedInput::Character(v), "packed-char-delivered");
        self.callback::<char, E>()?;
        if matches!(self.shape, Shape::Ignore) {
            self.finish(RecoveredPackedNodeKind::Character(v))
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<(), E> {
        self.text(v.as_bytes(), true)
    }
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> std::result::Result<(), E> {
        self.text(v.as_bytes(), true)
    }
    fn visit_string<E: de::Error>(mut self, v: String) -> std::result::Result<(), E> {
        let capacity = v.capacity();
        let len = v.len();
        self.arena.foreign_string = Some(v);
        self.deliver(
            RecoveredPackedInput::OwnedString { len, capacity },
            "packed-owned-string-delivered",
        );
        self.callback::<String, E>()?;
        self.paid(capacity, 1, capacity)?;
        Err(self.reject(RecoveredPackedRejection::OwnedCallback))
    }
    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<(), E> {
        self.text(v, false)
    }
    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> std::result::Result<(), E> {
        self.text(v, false)
    }
    fn visit_byte_buf<E: de::Error>(mut self, v: Vec<u8>) -> std::result::Result<(), E> {
        let capacity = v.capacity();
        let len = v.len();
        self.arena.foreign_bytes = Some(v);
        self.deliver(
            RecoveredPackedInput::OwnedBytes { len, capacity },
            "packed-owned-bytes-delivered",
        );
        self.callback::<Vec<u8>, E>()?;
        self.paid(capacity, 1, capacity)?;
        Err(self.reject(RecoveredPackedRejection::OwnedCallback))
    }
    fn visit_none<E: de::Error>(mut self) -> std::result::Result<(), E> {
        self.deliver(RecoveredPackedInput::None, "packed-none-delivered");
        self.callback::<(), E>()?;
        if matches!(self.shape, Shape::Optional(_) | Shape::Ignore) {
            self.finish(RecoveredPackedNodeKind::Optional(false))
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_some<D: de::Deserializer<'de>>(
        mut self,
        decoder: D,
    ) -> std::result::Result<(), D::Error> {
        self.deliver(RecoveredPackedInput::Some, "packed-some-delivered");
        self.callback::<D, D::Error>()?;
        let shape = match self.shape {
            Shape::Optional(Optional::Row) => Shape::Record(RecoveredPackedRecord::Row),
            Shape::Optional(Optional::Value) => Shape::Value,
            Shape::Optional(Optional::Id16) => Shape::List(List::Id16),
            Shape::Optional(Optional::Id8) => Shape::List(List::Id8),
            Shape::Ignore => Shape::Ignore,
            _ => return Err(self.reject(RecoveredPackedRejection::Shape)),
        };
        self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Optional(true);
        let child = self
            .arena
            .node(self.owner, Some(self.node), Some(0))
            .ok_or_else(|| self.reject::<D::Error>(RecoveredPackedRejection::Limits))?;
        Seed {
            arena: &mut *self.arena,
            owner: &mut *self.owner,
            input: self.input,
            node: child,
            shape,
            depth: self.depth + 1,
        }
        .deserialize(decoder)
    }
    fn visit_unit<E: de::Error>(mut self) -> std::result::Result<(), E> {
        self.deliver(RecoveredPackedInput::Unit, "packed-unit-delivered");
        self.callback::<(), E>()?;
        if matches!(
            self.shape,
            Shape::Payload(RecoveredPackedValue::Empty) | Shape::Ignore
        ) {
            self.finish(RecoveredPackedNodeKind::Nil)
        } else {
            Err(self.reject(RecoveredPackedRejection::Shape))
        }
    }
    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        mut self,
        decoder: D,
    ) -> std::result::Result<(), D::Error> {
        self.deliver(RecoveredPackedInput::Newtype, "packed-newtype-delivered");
        self.callback::<D, D::Error>()?;
        if !matches!(self.shape, Shape::Ignore) {
            return Err(self.reject(RecoveredPackedRejection::Shape));
        }
        self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Newtype;
        let child = self
            .arena
            .node(self.owner, Some(self.node), Some(0))
            .ok_or_else(|| self.reject::<D::Error>(RecoveredPackedRejection::Limits))?;
        Seed {
            arena: &mut *self.arena,
            owner: &mut *self.owner,
            input: self.input,
            node: child,
            shape: Shape::Ignore,
            depth: self.depth + 1,
        }
        .deserialize(decoder)
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> std::result::Result<(), A::Error> {
        self.deliver(
            RecoveredPackedInput::Sequence(seq.size_hint()),
            "packed-sequence-delivered",
        );
        self.callback::<A, A::Error>()?;
        let fixed = match self.shape {
            Shape::Record(record) => {
                self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Record(record);
                Some(fields(record).len())
            }
            Shape::List(List::Id16) => Some(16),
            Shape::List(List::Id8) => Some(8),
            Shape::List(_) | Shape::Ignore => {
                self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Sequence;
                None
            }
            _ => return Err(self.reject(RecoveredPackedRejection::Shape)),
        };
        if fixed.is_some() && !matches!(self.shape, Shape::Record(_)) {
            self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Sequence;
        }
        let mut index = 0usize;
        loop {
            // Fixed visitors read exactly N then return: native array left-check
            // rejects excess, while the native BIN adapter intentionally does not.
            if fixed.is_some_and(|n| index == n) || seq.size_hint() == Some(0) {
                break;
            }
            if matches!(self.shape, Shape::List(List::Rows)) && index >= self.count() {
                return Err(self.reject(RecoveredPackedRejection::Count));
            }
            let shape = match self.shape {
                Shape::Record(record) => field_shape(record, index),
                Shape::List(list) => element(list),
                Shape::Ignore => Shape::Ignore,
                _ => return Err(self.reject(RecoveredPackedRejection::Shape)),
            };
            let previous = self.arena.nodes[self.node].last;
            let child = self
                .arena
                .node(self.owner, Some(self.node), Some(index))
                .ok_or_else(|| self.reject::<A::Error>(RecoveredPackedRejection::Limits))?;
            let result = seq.next_element_seed(Seed {
                arena: &mut *self.arena,
                owner: &mut *self.owner,
                input: self.input,
                node: child,
                shape,
                depth: self.depth + 1,
            })?;
            if result.is_none() {
                self.arena.nodes.pop();
                self.arena.nodes[self.node].children -= 1;
                self.arena.nodes[self.node].last = previous;
                if let Some(previous) = previous {
                    self.arena.nodes[previous].next = None;
                } else {
                    self.arena.nodes[self.node].first = None;
                }
                break;
            }
            index += 1;
        }
        if fixed.is_some_and(|n| index != n) {
            return Err(self.reject(RecoveredPackedRejection::Shape));
        }
        if matches!(self.shape, Shape::List(List::Rows)) && index != self.count() {
            return Err(self.reject(RecoveredPackedRejection::Count));
        }
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> std::result::Result<(), A::Error> {
        self.deliver(
            RecoveredPackedInput::Map(map.size_hint()),
            "packed-map-delivered",
        );
        self.callback::<A, A::Error>()?;
        let Some(count) = map.size_hint() else {
            return Err(self.reject(RecoveredPackedRejection::Shape));
        };
        let (domain, record) = match self.shape {
            Shape::Record(record) => {
                self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Record(record);
                (Some(Key::Field(record)), Some(record))
            }
            Shape::Value if count == 1 => (Some(Key::Variant), None),
            Shape::Ignore => {
                self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Map;
                (None, None)
            }
            _ => return Err(self.reject(RecoveredPackedRejection::Shape)),
        };
        let mut seen = 0u16;
        for ordinal in 0..count {
            let key = self
                .arena
                .node(self.owner, Some(self.node), None)
                .ok_or_else(|| self.reject::<A::Error>(RecoveredPackedRejection::Limits))?;
            let present = map.next_key_seed(Seed {
                arena: &mut *self.arena,
                owner: &mut *self.owner,
                input: self.input,
                node: key,
                shape: domain.map_or(Shape::Ignore, Shape::Key),
                depth: self.depth + 1,
            })?;
            if present.is_none() {
                return Err(self.reject(RecoveredPackedRejection::Shape));
            }
            let field = self.arena.nodes[key].lookup;
            let shape = if let Some(record) = record {
                if let Some(field) = field {
                    let bit = 1u16 << field;
                    if seen & bit != 0 {
                        return Err(self.reject(RecoveredPackedRejection::Shape));
                    }
                    seen |= bit;
                    field_shape(record, field)
                } else {
                    Shape::Ignore
                }
            } else if matches!(self.shape, Shape::Value) {
                let value = field
                    .and_then(variant)
                    .ok_or_else(|| self.reject::<A::Error>(RecoveredPackedRejection::Identifier))?;
                self.arena.nodes[self.node].kind = RecoveredPackedNodeKind::Value(value);
                payload(value)
            } else {
                Shape::Ignore
            };
            let field = if record.is_some() {
                field
            } else {
                Some(ordinal)
            };
            let child = self
                .arena
                .node(self.owner, Some(self.node), field)
                .ok_or_else(|| self.reject::<A::Error>(RecoveredPackedRejection::Limits))?;
            map.next_value_seed(Seed {
                arena: &mut *self.arena,
                owner: &mut *self.owner,
                input: self.input,
                node: child,
                shape,
                depth: self.depth + 1,
            })?;
        }
        if let Some(record) = record {
            let mut required = (1u16 << fields(record).len()) - 1;
            match record {
                RecoveredPackedRecord::Row => required &= !((1u16 << 9) | (1u16 << 11)),
                RecoveredPackedRecord::Attribute => required &= !(1u16 << 2),
                _ => {}
            }
            if seen & required != required {
                return Err(self.reject(RecoveredPackedRejection::Shape));
            }
        }
        Ok(())
    }
    fn visit_enum<A: de::EnumAccess<'de>>(mut self, _data: A) -> std::result::Result<(), A::Error> {
        self.deliver(RecoveredPackedInput::Enum, "packed-enum-delivered");
        self.callback::<A, A::Error>()?;
        Err(self.reject(RecoveredPackedRejection::Shape))
    }
}
impl Seed<'_, '_> {
    fn count(&self) -> usize {
        self.arena.count
    }
}

type Continued = FrameContinuation<
    PreparedRecoveredStructuralMetadataBytes,
    Arena,
    Failure,
    TelemetryRecoveryError,
>;
type PackedResult =
    std::result::Result<PreparedRecoveredPackedMetadata, RetainedRecoveredPackedMetadataError>;
/// One accepted typed packed arena and complete original metadata/native parent.
/// Dictionary references and final log rows have not been assembled or authorized.
pub struct PreparedRecoveredPackedMetadata {
    held: Continued,
}
/// Terminal typed parse outcome, including all actual partial nodes and parents.
pub struct RetainedRecoveredPackedMetadataError {
    held: Continued,
}
impl FrameParent<RecoveryFrameWork> for PreparedRecoveredPackedMetadata {
    fn with_frame<T>(
        &mut self,
        operation: impl FnOnce(&mut PreparedDecompressedFrame<RecoveryFrameWork>) -> T,
    ) -> T {
        self.held.parent.with_frame(operation)
    }
}
macro_rules! views {
    () => {
        /// Actual absent/default-lane branch; no parser entry is invented.
        pub fn is_absent(&self) -> bool {
            self.held.child.absent
        }
        /// Actual retained arena, including partial drafts on a refusal.
        pub fn nodes(&self) -> &[RecoveredPackedNode] {
            &self.held.child.nodes
        }
        /// Original exact raw bytes, including any first-value tail.
        pub fn raw_bytes(&self) -> Option<&[u8]> {
            self.held.parent.raw_bytes()
        }
        /// Actual captured source bytes for a borrowed scalar range.
        pub fn source_bytes(&self, range: Range<usize>) -> Option<&[u8]> {
            self.raw_bytes()?.get(range)
        }
        /// Actual parser result, captured before the original-E postcheck.
        pub fn parser_result(&self) -> Option<&std::result::Result<(), rmp_serde::decode::Error>> {
            self.held.child.parser.as_ref()
        }
        /// Actual last local UTF8 check; the RMP parser result stays separate.
        pub fn utf8_result(&self) -> Option<&std::result::Result<(), std::str::Utf8Error>> {
            self.held.child.utf8.as_ref()
        }
        /// First typed reason; actual native parser and original refusal stay separate.
        pub fn rejection(&self) -> Option<RecoveredPackedRejection> {
            self.held.child.rejection
        }
        /// Exact original ledger refusal, not a formatted substitute.
        pub fn recovery_refusal(&self) -> Option<&TelemetryRecoveryError> {
            self.held.child.recovery.as_ref()
        }
        /// Real last arena reservation result, retained before late checks.
        pub fn reservation_result(&self) -> Option<&std::result::Result<(), TryReserveError>> {
            self.held.child.reservation.as_ref()
        }
        /// Actual local fees/operations, separate from original cumulative usage.
        pub fn usage(&self) -> RecoveredPackedMetadataUsage {
            self.held.child.usage
        }
        /// Complete real second native stream plus index/Pco/outer parents.
        pub fn parent_metadata(&self) -> &PreparedRecoveredStructuralMetadataBytes {
            &self.held.parent
        }
        /// Original absolute expiry.
        pub fn original_expiry(&self) -> Instant {
            self.held.parent.original_expiry()
        }
        /// Same cumulative original setup ledger, never a child replacement.
        pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
            self.held.parent.recovery_usage()
        }
        /// Actual child Ok before separate postcheck; presence is not acceptance.
        pub fn child_succeeded(&self) -> bool {
            self.held.actual.as_ref().is_some_and(|r| r.is_ok())
        }
        /// Defensive custody if an owned callback entered. The selected borrowed
        /// parser and its closed adapters produce no such input.
        pub fn has_owned_callback(&self) -> bool {
            self.held.child.foreign_string.is_some() || self.held.child.foreign_bytes.is_some()
        }
        /// Actual caught observer/parser unwind.
        pub fn has_panic(&self) -> bool {
            self.held.panic.is_some()
        }
    };
}
impl PreparedRecoveredPackedMetadata {
    views!();
    /// Consume this completed typed arena before the next frame on SAME work/E.
    #[allow(
        clippy::result_large_err,
        reason = "preserve original ledger and complete native outcome without refusal allocation"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: OwnedLogDecompressionLimits,
    ) -> std::result::Result<crate::PreparedRecoveredLogFrame, crate::RetainedRecoveredLogFrameError>
    {
        let FrameContinuation {
            child,
            initial,
            actual,
            post,
            panic,
            parent,
            ..
        } = self.held;
        drop((child, initial, actual, post, panic));
        parent.decompress_next_until(input, expected_bytes, expected_records, limits)
    }
}
impl RetainedRecoveredPackedMetadataError {
    views!();
}
impl fmt::Debug for PreparedRecoveredPackedMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredPackedMetadata")
            .field("usage", &self.usage())
            .field("nodes", &self.nodes().len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedRecoveredPackedMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredPackedMetadataError")
            .field("usage", &self.usage())
            .field("reason", &self.rejection())
            .field("parser", &self.parser_result())
            .field("panic", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredPackedMetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredPackedMetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.recovery_refusal() {
            return Some(error);
        }
        if let Some(Err(error)) = self.parser_result() {
            return Some(error);
        }
        if let Some(Err(error)) = self.reservation_result() {
            return Some(error);
        }
        if let Some(Err(error)) = &self.held.post {
            return Some(error);
        }
        self.held
            .initial
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .map(|e| e as &(dyn Error + 'static))
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete real parser/partial arena/native parent on every outcome"
)]
pub(super) fn prepare(
    parent: PreparedRecoveredStructuralMetadataBytes,
    limits: RecoveredPackedMetadataLimits,
) -> PackedResult {
    let count = parent
        .parent_index()
        .parent_timestamps()
        .parent_offsets()
        .parent_sections()
        .record_count() as usize;
    let child = Arena {
        limits,
        count,
        absent: false,
        nodes: Vec::new(),
        usage: RecoveredPackedMetadataUsage::default(),
        rejection: None,
        recovery: None,
        reservation: None,
        parser: None,
        utf8: None,
        foreign_string: None,
        foreign_bytes: None,
    };
    let held =
        parent.continue_raw_until(child, |raw, work, child, fixed| child.run(raw, work, fixed));
    if held.accepted() {
        Ok(PreparedRecoveredPackedMetadata { held })
    } else {
        Err(RetainedRecoveredPackedMetadataError { held })
    }
}
