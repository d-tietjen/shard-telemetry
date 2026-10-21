# Immutable evidence publication primitive

`PreparedEvidenceBatch` prepares and publishes one bounded, already admitted,
organization-split STEL envelope. It reuses `TelemetryObjectStore` and the existing
STEL/signal codecs. It does not construct a store, fetch a WAL, open a writer
catalog, discover credentials, or change any existing ingestion/forwarding mode.

## Admission authority

This is a trusted service API, not an authorization endpoint. Before preparing a
batch, the host must register the writer and establish the original organization,
plane and signal provenance of **each record**, validate its signal payload, split
mixed-organization input, and check current explicit export consent. Caller
attributes such as `org_uuid` do not establish that authority. A fetched envelope
must never be relabeled wholesale with one configured organization.

`EvidenceAdmission` records that attestation and its consent revision. In this
profile the existing SDK tenant is the telemetry plane, and it must exactly match
the admitted plane. Organization identity remains separate. Organization, stable
source and durable source-incarnation UUIDs must be non-nil. Plane tokens are
lowercase ASCII namespace components. Allowed signals must be sorted and distinct;
traces require explicit permission. Raw profiling is not a log signal adapter in
this format: a future typed capability requires its own versioned implementation.

The SDK checks STEL framing/checksum, exact plane, allowed signal, count, original
partition/offset identities and resource ceilings. It does not re-decode signal
payloads or authenticate organization provenance. Storage credentials and the
service must independently enforce the owned namespace, encryption, deletion and
retention policy. Sharing an endpoint grants no additional authority.

Before **every publication attempt**, including a retry of an already prepared
batch, the host must recheck its current writer grant and the organization's
plane/signal export consent. A captured attestation, saved journal entry or
prepared object does not authorize publication after revocation. Preparation is
not a revocation service and the SDK does not perform that host authority check.

## Version 1 layout and identity

Keys are relative to the supplied object's store prefix:

```text
datasets/v1/organizations/<32-hex-org>/planes/<plane>/
  sources/<32-hex-source>/incarnations/<32-hex-durable-source-incarnation>/
    batches/<20-digit-source-sequence>/objects/<blake3>.stel
    commits/<20-digit-source-sequence>.json
```

The caller journals a source-wide durable sequence before upload. It must be
unique across all signals/partitions sharing that source/incarnation; a
per-partition counter alone is insufficient. Source reset/wipe obtains a new
durable incarnation. Upload restarts do not generate new event IDs, sequences,
process incarnations or clocks.

The canonical JSON manifest contains `bundle_id`, `format_version`,
`payload_schema` (`STELv1`), `checksum_algorithm` (`blake3`), the complete
`admission`, original `source` context, ordered `events`, and the exact
`object_key`, `object_bytes`, `object_digest`. Struct field order and existing
typed enum serialization define version 1's canonical input. The bundle ID is
BLAKE3 over the domain `shard-telemetry-evidence-bundle-v1\0` followed by those
canonical fields without `bundle_id`. The receipt also carries the exact manifest
BLAKE3 digest and length. ETag/version tokens are not content digests.

Topic IDs in source and event addresses are 128-bit JSON integers. Consumers,
including central discovery/query adapters, must parse them losslessly using
typed 128-bit integer decoding or an equivalent decimal-preserving parser.
Do not round or narrow them through floating-point JSON values when comparing
original identities.

The source context also retains the original physical shard as provenance, not
authorization or part of the immutable logical event identity. Each event retains
its original signal/partition/offset, existing producer event
ID when present, and original process incarnation. Owner/source/incarnation plus
the original address provide the fallback identity; repacking and bundle sequence
changes do not replace it. Addresses are strictly increasing within one original
partition. Noncontiguous selected offsets are allowed and do not imply that gaps
were lost. Original workload, request, trace, experiment and other context remains
in the unchanged signal payload. Capture clock fields and loss evidence preserve
unknown values explicitly. `selected_records` describes the exact listed input,
not complete source history. Observed local next/retained offsets are not remote
acknowledgements or central query frontiers. Reported loss intervals are sorted,
disjoint, source-bounded, and cannot contain a published event.

## Publication and uncertain outcomes

Preparation copies only the original STEL and serialized manifest. It has no
background work or store handle. The host durably journals the exact original
inputs and sequence, then calls `publish_before` within its background work budget:

1. Atomic `put_bytes_if_absent` for the content-addressed STEL object; verify the
   returned complete length and BLAKE3 digest.
2. Atomic `put_bytes_if_absent` for the fixed source-sequence manifest key LAST;
   verify its complete length and digest.
3. Return the complete immutable receipt. Central discovery/index/queryability
   and permission to reclaim local records remain separate states.

There are at most two PUT calls, no automatic retries, no streamed/file upload,
no mutable `CURRENT`/CAS, and no source-prefix recovery or deletion. Equal retries
are accepted by the existing immutable-create contract. Different bytes at a
source-sequence commit key are refused. A conflict can leave an uncommitted data
object; payload presence alone is never commit. Cleanup of exact unreferenced
keys is a separately owned task and must not delete committed input.

Actual operation errors remain errors, including an uncertain/lost response after
remote commit. Reprepare from the saved original inputs and retry the same keys
and bytes to reconcile. A deadline is checked before each call; synchronous
object-store calls cannot be preempted. A verified successful final manifest PUT
is returned even when late, with `completed_after_deadline=true`. A late data PUT
cannot start the next call. Configure transport timeouts and retain ownership of
the synchronous call until it returns.

## Explicit S3 transport policy

`S3ObjectStore::open_with_limits(config, configured_threads, limits)` accepts an
`S3ObjectStoreTransportLimits` value with positive request, connect and retry-window
milliseconds and a finite additional retry count. A connect timeout cannot exceed
the request timeout. For example, a host that owns retries can select
`max_retries: 0` while setting its per-request timeouts explicitly. The default
value matches the pinned provider's 30s request, 5s connect, 180s retry window and
ten retries. The existing `open` and `open_with_threads` paths continue using their
environment-derived policy without applying this new default.

The opt-in constructor overrides only the HTTP request/connect timeouts and the
provider retry count/window. Existing proxy, TLS, read-timeout, user-agent and other
client settings remain intact. In the pinned `object_store` version, the standard
environment builder has no retry/backoff configuration key; its default backoff
is preserved. Credentials still use the existing environment/workload/ECS/instance
chain. Credential refresh, cache locks, token-file reads and additional credential
requests occur outside an individual S3 request's retry timer; instance-metadata
connect policy can also differ.

A provider retry window is checked after failures before another retry. It does
not preempt an in-flight request or clamp a pending backoff. An immutable retry
may also require a separate GET to verify an existing object's size and digest.
These HTTP controls do not establish a whole-call wall-clock, queue, memory,
credential-refresh or upload-concurrency bound. A host must retain its physical
upload permit until the synchronous call actually returns, keep the exact prepared
key/bytes for uncertain failures, and retain the verified late-success behavior
described above. Bounded admission, durable spool/restart, source-incarnation and
remote-commit-or-authorized-loss retention remain host integration work.

The transport regression fixtures use the actual provider and an owned unsigned
loopback HTTP endpoint for stalls, 503 retry counts, immutable conflict/GET
verification and a final manifest response after the caller's deadline. They do
not qualify production credential refresh, real S3, Runtime admission or complete
end-to-end timing.

## Bounds and remaining integration

Positive caller limits can lower the format ceilings of 1 MiB encoded STEL,
64 KiB canonical manifest and 256 event identities (default 128). Individual
metadata strings are limited to 128 UTF-8 bytes, plane tokens to 32 ASCII bytes,
and loss intervals to 16. Length/count validation precedes hashing/copying;
serialization has a checked bounded writer, including escaped strings. Prepared
state owns at most the object copy, one manifest buffer, and fixed-size bounded
receipt strings; identity serialization uses one temporary manifest buffer.
These are declared Rust/input bounds, not allocator/RSS or elapsed syscall limits.
Caller-owned decoded records are outside this primitive's retained state.

The host must reserve these buffers and upload concurrency/rate/retry/deadline
work **inside** its node budget. This primitive provides no durable spool,
checkpoint, retention hold, loss-consent decision, worker lifecycle, or central
reader. A complete Runtime adapter must prove per-record service admission and
splitting, durable source identity/journaling, explicit trace consent, bounded
outage behavior, and remote-commit-or-authorized-loss reclamation. Central
discovery and pinned read-only snapshot querying are separate capabilities.

Qualify deterministic local/fault/retry/restart tests first, then the canonical
object-store contract and exact configured backend. S3-compatible emulator
results are not real AWS S3 evidence. Follow CONTRIBUTING.md and RELEASING.md;
this format/API alone does not complete those deployment acceptance requirements.
