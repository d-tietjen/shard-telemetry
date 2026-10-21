//! Explicitly admitted, bounded immutable telemetry evidence publication.
//!
//! This primitive accepts already organization-split STEL bytes. The trusted
//! service must establish original record ownership and consent before calling
//! it; a tenant string or an `org_uuid` attribute is not admission authority.
//! The host must recheck its current writer grant and export consent before
//! every publish/retry, including already prepared batches. Preparation is not
//! a revocation service or continuing authorization.

use std::fmt;
use std::time::Instant;

use serde::Serialize;
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::{TelemetryError, TelemetryObjectStore, TelemetryRecordRef, TelemetrySignal};

mod publish;
#[cfg(test)]
mod tests;
mod validate;

/// Version of the immutable shared-evidence dataset and its JSON manifest.
pub const EVIDENCE_DATASET_VERSION: u16 = 1;
/// Format ceiling for one already encoded STEL object, independent of node quotas.
pub const MAX_EVIDENCE_OBJECT_BYTES: usize = 1024 * 1024;
/// Format ceiling for one complete commit manifest.
pub const MAX_EVIDENCE_MANIFEST_BYTES: usize = 64 * 1024;
/// Format ceiling for original record identities in one manifest.
pub const MAX_EVIDENCE_EVENTS: usize = 256;

/// Original service-admitted owner of one independently written source prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceOwner {
    /// Non-nil organization UUID established by service admission, not attributes.
    pub organization_id: [u8; 16],
    /// Exact SDK tenant for this profile, used as the telemetry plane.
    pub plane: String,
    /// Non-nil stable source UUID; independent writers must not share this owner.
    pub source_id: [u8; 16],
    /// Non-nil durable WAL incarnation UUID, changed when source offsets reset.
    pub source_incarnation: [u8; 16],
}

/// Trusted caller's explicit enrollment and consent attestation for a batch.
///
/// Construct this only after the service has registered the writer, checked the
/// original provenance of **every** record, split mixed-organization input, and
/// checked current organization/plane/signal export consent. STEL's tenant is
/// only the plane in this profile. Neither this struct nor record attributes
/// authenticate an organization. Storage credentials must enforce the owned
/// namespace independently. Traces require explicit membership in `allowed_signals`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceAdmission {
    /// Requested dataset capability; unsupported versions fail before any I/O.
    pub dataset_version: u16,
    /// Original admitted owner, preserved across retries and process restarts.
    pub owner: EvidenceOwner,
    /// Exact service consent revision, not inferred from a payload attribute.
    pub consent_revision: String,
    /// Sorted distinct allowed signals; there is no default trace export permission.
    pub allowed_signals: Vec<TelemetrySignal>,
}

/// Source-qualified original identity corresponding to one STEL item, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceEventIdentity {
    /// Original WAL signal/partition/offset; never a newly assigned import offset.
    pub original: TelemetryRecordRef,
    /// Existing producer event ID, when present; never replaced by a bundle ID.
    pub producer_event_id: Option<String>,
    /// Non-nil process incarnation recorded at original capture, not upload time.
    pub process_incarnation: [u8; 16],
}

/// Capture clock provenance; absent values remain explicitly unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceClock {
    /// Original capture clock domain, when known.
    pub domain: Option<String>,
    /// Original capture Unix time, when known; payload timestamps stay unchanged.
    pub captured_unix_nanos: Option<u64>,
    /// Known clock uncertainty; requires a known clock domain.
    pub uncertainty_nanos: Option<u64>,
}

/// What this bundle can say about its population, not fleet completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCompleteness {
    /// No completeness claim was established by the admitted source.
    Unknown,
    /// Exactly the listed original records were selected; gaps may be intentional.
    SelectedRecords,
}

/// Original source's reason for an explicitly reported unavailable interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLossReason {
    /// A declared bounded outage/pressure policy permitted release.
    CapacityPolicy,
    /// Source retention removed records before export admission.
    SourceRetention,
    /// Consent or deletion policy excluded a source interval.
    ConsentPolicy,
}

/// One half-open interval in the manifest's original source partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceLossInterval {
    /// First unavailable original offset.
    pub first_offset: u64,
    /// First offset after the unavailable interval.
    pub next_offset: u64,
    /// Declared source policy, retained without claiming this SDK authorized loss.
    pub reason: EvidenceLossReason,
}

/// Source loss evidence; unknown is distinct from an explicitly empty report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLossStatus {
    /// Exact source loss accounting was not established.
    Unknown,
    /// Bounded, sorted disjoint intervals reported by the admitted source.
    Reported(Vec<EvidenceLossInterval>),
}

/// Original source progress and provenance included in one immutable manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceBatchContext {
    /// Source-wide durable sequence, unique across its signals and partitions;
    /// reuse with different content is refused.
    pub source_sequence: u64,
    /// Original physical source shard, retained as provenance rather than authority.
    pub source_shard_id: ShardId,
    /// Original partition shared by all included item identities.
    pub source_partition: TopicPartition,
    /// Observed original source next offset, not a remote commit or query frontier.
    pub observed_source_next_offset: LogicalOffset,
    /// Observed retained floor, or unknown; not permission to release source data.
    pub retained_source_start_offset: Option<LogicalOffset>,
    /// Original capture clock evidence.
    pub clock: EvidenceClock,
    /// Explicit population qualification; never inferred from upload success.
    pub completeness: EvidenceCompleteness,
    /// Explicit known/unknown loss evidence, not an eviction acknowledgement.
    pub loss: EvidenceLossStatus,
}

/// Per-preparation ceilings, which may lower but cannot raise format limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceExportLimits {
    /// Maximum encoded STEL object accepted before hashing or copying it.
    pub max_object_bytes: usize,
    /// Maximum canonical JSON manifest; bounded during serialization.
    pub max_manifest_bytes: usize,
    /// Maximum original event identities, checked before iteration/serialization.
    pub max_events: usize,
}

impl Default for EvidenceExportLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: MAX_EVIDENCE_OBJECT_BYTES,
            max_manifest_bytes: MAX_EVIDENCE_MANIFEST_BYTES,
            max_events: 128,
        }
    }
}

/// Exact immutable commit reference; publication is distinct from queryability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceCommitReference {
    /// Stable digest of the complete versioned logical bundle input.
    pub bundle_id: String,
    /// Source-sequence commit key; different content at that key is a conflict.
    pub manifest_key: String,
    /// Exact BLAKE3 digest of the serialized commit manifest.
    pub manifest_digest: String,
    /// Exact committed manifest byte count.
    pub manifest_bytes: u64,
    /// Content-addressed, source-owned STEL key.
    pub object_key: String,
    /// Exact BLAKE3 digest of the original STEL bytes.
    pub object_digest: String,
    /// Exact original STEL byte count.
    pub object_bytes: u64,
}

/// Successful complete manifest publication, including actual late completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidencePublication {
    /// Exact immutable data/manifest receipt, not a central query acknowledgement.
    pub committed: EvidenceCommitReference,
    /// True if the successful final synchronous PUT completed after its deadline.
    pub completed_after_deadline: bool,
}

/// Bounded immutable bytes ready for the caller's durable retry journal.
///
/// Preparation retains only bounded copies of the STEL and manifest, with no
/// background tasks or object-store handle. Callers must durably save the exact
/// original inputs/sequence before upload and reconcile uncertain responses using
/// the same inputs. This is not a spool, retention pin, or source checkpoint.
pub struct PreparedEvidenceBatch {
    object: Vec<u8>,
    manifest: Vec<u8>,
    reference: EvidenceCommitReference,
}

impl fmt::Debug for PreparedEvidenceBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedEvidenceBatch")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}

/// Failure to admit, encode, or publish this bounded evidence batch.
#[derive(Debug)]
pub enum EvidenceExportError {
    /// Malformed, inconsistent, unpermitted, or unsupported caller input.
    InvalidInput(&'static str),
    /// A configured or format resource ceiling was exceeded.
    Limits(&'static str),
    /// The original deadline elapsed before the next object-store call.
    Expired,
    /// Actual bounded preparation allocation failure.
    Allocation(std::collections::TryReserveError),
    /// Actual JSON serialization error, retained without an I/O retry.
    Encoding(serde_json::Error),
    /// Actual envelope or object-store error, including immutable conflicts.
    Telemetry(TelemetryError),
}

impl fmt::Display for EvidenceExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(s) => write!(f, "invalid evidence input: {s}"),
            Self::Limits(s) => write!(f, "evidence limit: {s}"),
            Self::Expired => f.write_str("evidence publication deadline elapsed"),
            Self::Allocation(e) => write!(f, "evidence allocation: {e}"),
            Self::Encoding(e) => write!(f, "evidence manifest encoding: {e}"),
            Self::Telemetry(e) => write!(f, "evidence operation: {e}"),
        }
    }
}

impl std::error::Error for EvidenceExportError {}

impl From<TelemetryError> for EvidenceExportError {
    fn from(error: TelemetryError) -> Self {
        Self::Telemetry(error)
    }
}

impl PreparedEvidenceBatch {
    /// Validates trusted admission and original identities, then copies only
    /// bounded bytes. Payload signal decoding and original organization admission
    /// must already be complete in the trusted caller; STEL framing/checksum,
    /// plane, signal, count and source-address invariants are checked here.
    pub fn prepare(
        admission: &EvidenceAdmission,
        context: &EvidenceBatchContext,
        events: &[EvidenceEventIdentity],
        encoded_envelope: &[u8],
        limits: EvidenceExportLimits,
    ) -> Result<Self, EvidenceExportError> {
        publish::prepare(admission, context, events, encoded_envelope, limits)
    }

    /// Exact input bytes, unchanged by publication or retry.
    pub fn object_bytes(&self) -> &[u8] {
        &self.object
    }

    /// Exact versioned canonical JSON bytes; payload presence alone is not commit.
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest
    }

    /// Stable keys/digests for journaling; this accessor does not perform I/O.
    pub fn reference(&self) -> &EvidenceCommitReference {
        &self.reference
    }

    /// Makes at most two synchronous atomic immutable byte PUTs, data then
    /// manifest. There are no automatic retries or writer-catalog operations.
    ///
    /// The caller serializes this work within its declared upload/concurrency
    /// budget and configures transport timeouts. Synchronous store calls cannot
    /// be preempted by this deadline. Real store errors remain errors even if a
    /// call finishes late; a verified successful final commit is returned with
    /// its late flag. Retry uncertain responses using these exact bytes.
    ///
    /// Before every call, including retries of already prepared batches, the
    /// host must recheck the current writer grant and organization/plane/signal
    /// export consent. A saved attestation or prepared batch does not authorize
    /// publication after revocation; this primitive provides no revocation check.
    pub fn publish_before(
        &self,
        store: &dyn TelemetryObjectStore,
        deadline: Instant,
    ) -> Result<EvidencePublication, EvidenceExportError> {
        publish::publish(self, store, deadline)
    }
}
