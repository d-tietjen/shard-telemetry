use std::io::{self, Write};

use super::*;

#[derive(Serialize)]
struct ManifestInput<'a> {
    format_version: u16,
    payload_schema: &'static str,
    checksum_algorithm: &'static str,
    admission: &'a EvidenceAdmission,
    source: &'a EvidenceBatchContext,
    events: &'a [EvidenceEventIdentity],
    object_key: &'a str,
    object_bytes: u64,
    object_digest: &'a str,
}

#[derive(Serialize)]
struct Manifest<'a> {
    bundle_id: &'a str,
    #[serde(flatten)]
    input: ManifestInput<'a>,
}

pub(super) fn prepare(
    admission: &EvidenceAdmission,
    context: &EvidenceBatchContext,
    events: &[EvidenceEventIdentity],
    encoded: &[u8],
    limits: EvidenceExportLimits,
) -> Result<PreparedEvidenceBatch, EvidenceExportError> {
    validate::input(admission, context, events, encoded, limits)?;
    let owner = &admission.owner;
    let prefix = format!(
        "datasets/v1/organizations/{:032x}/planes/{}/sources/{:032x}/incarnations/{:032x}",
        u128::from_be_bytes(owner.organization_id),
        owner.plane,
        u128::from_be_bytes(owner.source_id),
        u128::from_be_bytes(owner.source_incarnation),
    );
    let digest = blake3::hash(encoded).to_hex().to_string();
    let object_key = format!(
        "{prefix}/batches/{:020}/objects/{digest}.stel",
        context.source_sequence
    );
    let input = ManifestInput {
        format_version: EVIDENCE_DATASET_VERSION,
        payload_schema: "STELv1",
        checksum_algorithm: "blake3",
        admission,
        source: context,
        events,
        object_key: &object_key,
        object_bytes: encoded.len() as u64,
        object_digest: &digest,
    };
    // The complete canonical input, including original addresses and policy
    // revision, is identity material. No wall clock or process ID is generated.
    let identity = encode(&input, limits.max_manifest_bytes)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-evidence-bundle-v1\0");
    hasher.update(&identity);
    let bundle_id = hasher.finalize().to_hex().to_string();
    drop(identity);
    let manifest = encode(
        &Manifest {
            bundle_id: &bundle_id,
            input,
        },
        limits.max_manifest_bytes,
    )?;
    let manifest_digest = blake3::hash(&manifest).to_hex().to_string();
    let mut object = Vec::new();
    object
        .try_reserve_exact(encoded.len())
        .map_err(EvidenceExportError::Allocation)?;
    object.extend_from_slice(encoded);
    let reference = EvidenceCommitReference {
        bundle_id,
        manifest_key: format!("{prefix}/commits/{:020}.json", context.source_sequence),
        manifest_digest,
        manifest_bytes: manifest.len() as u64,
        object_key,
        object_digest: digest,
        object_bytes: object.len() as u64,
    };
    Ok(PreparedEvidenceBatch {
        object,
        manifest,
        reference,
    })
}

pub(super) fn publish(
    batch: &PreparedEvidenceBatch,
    store: &dyn TelemetryObjectStore,
    deadline: Instant,
) -> Result<EvidencePublication, EvidenceExportError> {
    with_clock(batch, store, deadline, Instant::now)
}

pub(super) fn with_clock(
    batch: &PreparedEvidenceBatch,
    store: &dyn TelemetryObjectStore,
    deadline: Instant,
    mut now: impl FnMut() -> Instant,
) -> Result<EvidencePublication, EvidenceExportError> {
    if now() >= deadline {
        return Err(EvidenceExportError::Expired);
    }
    let reference = &batch.reference;
    let uploaded = store.put_bytes_if_absent(&reference.object_key, &batch.object)?;
    verify(uploaded, reference.object_bytes, &reference.object_digest)?;
    if now() >= deadline {
        return Err(EvidenceExportError::Expired);
    }
    // This fixed sequence key is the only commit marker. No mutable CURRENT,
    // listing, file upload, writer recovery, cleanup or catalog CAS is involved.
    let committed = store.put_bytes_if_absent(&reference.manifest_key, &batch.manifest)?;
    verify(
        committed,
        reference.manifest_bytes,
        &reference.manifest_digest,
    )?;
    Ok(EvidencePublication {
        committed: reference.clone(),
        completed_after_deadline: now() >= deadline,
    })
}

fn verify(
    metadata: crate::ObjectMetadata,
    bytes: u64,
    digest: &str,
) -> Result<(), EvidenceExportError> {
    if metadata.bytes != bytes || metadata.content_digest != digest {
        return Err(
            TelemetryError::CorruptTier("evidence PUT acknowledgement mismatch".into()).into(),
        );
    }
    Ok(())
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("evidence manifest byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, EvidenceExportError> {
    let mut writer = BoundedJson {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    writer
        .bytes
        .try_reserve_exact(limit)
        .map_err(EvidenceExportError::Allocation)?;
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        return Err(if writer.exceeded {
            EvidenceExportError::Limits("manifest bytes")
        } else {
            EvidenceExportError::Encoding(error)
        });
    }
    Ok(writer.bytes)
}
