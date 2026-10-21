# ShardTelemetry native protocol

The native protocol is ShardTelemetry's high-throughput binary interface on TCP port `3101`. This pre-release repository ships exactly one protocol and one durable format: native protocol v1 carrying checksummed `STEL` v1 envelopes. There is no legacy append decoder or dual-format storage path.

## Frame

Every request and response begins with a fixed 32-byte little-endian header:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `STNP` |
| 4 | 1 | Version `1` |
| 5 | 1 | Opcode: append `1`, query `2`, ping `3`, authenticate `4`, metric query `5`, trace query `6`, describe `7`, untracked append `8`, log page query `9`, local provider status `10` |
| 6 | 1 | Flags; bit 0 marks a response |
| 7 | 1 | Status |
| 8 | 16 | Caller-selected request ID |
| 24 | 4 | Payload length |
| 28 | 4 | First four bytes of the BLAKE3 payload digest |

Frames are limited to 64 MiB. Unknown flags, versions, opcodes, oversized payloads, malformed UTF-8, truncation, trailing bytes, count mismatches, and checksum mismatches fail closed. Multiple requests may be in flight on one connection and responses may complete out of order.

In production mode the first frame must authenticate with the configured bearer token. Append and query tenants must match the authenticated single tenant.

When a product request gate is configured, successful log, log-page, metric,
trace and provider-status responses recheck that same query gate after response
encoding and before returning success. Existing request and error handling
remains in place. `Describe` performs that completion check only when its encoded
capabilities advertise query support or queryable appends, so an HA node can
still advertise append-only service. This check does not create indexed-replay
ownership or a complete query replica; the product gate must validate those
actual dependencies.

## Signal-aware append

An append payload is one `STB1` batch containing 1–256 routed partitions. Its header contains the partition count and reserved zero bytes. Each partition stores:

| Bytes | Field |
| ---: | --- |
| 16 | Signal-specific shard-stream topic ID |
| 4 | Logical partition ID |
| 4 | Encoded envelope length |
| variable | One checksummed `STEL` envelope |
| 4 | Process-local transient-context length |
| variable | Optional transient context; `SLT1` for indexed log appends |

The transient context is not part of the durable envelope and is never written
to shard-stream WAL or object storage. For log appends it carries the embedded
frame-index bytes produced while the structural pack is encoded. The owner
stripe validates the context against the durable `SLW1` frame and installs the
index without decompressing the structural payload. If the context is absent,
the owner falls back to decompressing the embedded index. Recovery always uses
the embedded durable index, so losing the process-local context cannot affect
correctness.

The topic must match the envelope signal. Duplicate topic/partition pairs are rejected. The server validates the complete request before appending partitions in parallel under bounded backpressure.

A successful append returns one `STM1` acknowledgement entry per input partition, in request order. Each entry contains topic ID, partition ID, first durable offset, and last durable offset. Every offset represents exactly one log record, span, or metric point.

### Retry identity

The frame request ID is also the durable retry identity for append. The server
persists the accepted `STM1` receipt with a BLAKE3 digest of the complete `STB1`
payload. Retrying the same ID and bytes after a reconnect or server restart
returns that receipt without appending another copy. Reusing an ID with different
bytes is rejected. Receipt records follow the local retention window; upstream
store-and-forward clients therefore derive a stable ID from their source
partition, offset range, and envelope bytes.

## Indexed log query

The current native query opcode exposes the fastest exact log lookup primitive. Its `STQ1` request supports tenant, exact labels, exact case-insensitive message terms, an optional time range, result limit, and sort direction.

Log query responses use the response-only `STR1` format. Labels are grouped once per returned stream, but `STR1` is never accepted by append or storage code. Trace and metric native query messages will use their signal-native result schemas rather than reusing the log response.

### Byte-bounded log pages

Native opcode `9` accepts an `STQ4` page request containing the existing `STQ1`
query, a nonzero `max_bytes` no greater than 32 MiB, and an optional opaque
cursor. The initial lookup holds at most one projected candidate per active
tenant partition, so its candidate count can exceed 256 when more than 256
partitions are active. The merge examines at most 256 candidates per page in
result order. It refills a partition head only after advancing that partition,
within the same 256-candidate merge budget.
It counts each returned log line, label key/value, and structured metadata
key/value in UTF-8 bytes before copying the entry into the result.
One entry that exceeds `max_bytes` fails the request explicitly; it is never
silently skipped. A page can contain fewer records than the requested record
limit when its byte budget is reached.

The `STR4` response contains an ordered MessagePack page with the tenant,
entries, and optional `next_cursor`. Page order is timestamp, logical
partition, then durable offset, reversed for newest-first queries. A cursor is
exclusive and bound to the complete query (including tenant, labels, terms,
time range, direction, and record limit). Changing any of those fields while
reusing a cursor fails validation. A cursor is a continuation hint, not a
snapshot: appends, deletions, or retention between pages can change the
visible set. A final empty page may follow a full batch or a batch of deleted
records. Callers continue until `next_cursor` is absent and must still check
the response tenant against their authenticated scope.

Opcode `2` and `STR1` keep their existing behavior for older callers. Opcode
`9` requires a page-capable server; an older server rejects it as unsupported.
Deploy the server before migrating clients to `query_logs_page`, and pin a
published ShardTelemetry crate revision that includes the page API for both
embedded and remote clients. The default native server outbound window is
8 MiB, including the frame and MessagePack overhead, so callers using that
default should request a page budget below 8 MiB (for example, 7 MiB) or
raise the outbound window. A configured outbound cap can return
`TooManyRequests` when the encoded page exceeds it.

Full LogQL, PromQL, and TraceQL remain on the compatible HTTP APIs.

## Local provider status

Opcode `10` accepts a fixed-format `STP1` selection and returns `STP2` local
partition observations. It preserves existing `Describe` fields and encoding;
older servers can reject the unknown opcode or close the connection. Clients
must preserve unknown coverage on every protocol, authentication, admission,
timeout, or transport error. An old peer's failed exchange is not a successful
empty status. Deploy a status-capable server before using the client API.

`ProviderStatusRequest` selects 1–768 nonduplicate signal/partition pairs, in
caller order. Its tenant is 1–1024 UTF-8 bytes; an optional plane is 1–64 bytes.
An optional raw 16-byte organization UUID and inclusive-start/exclusive-end
timestamps are echoed exactly. These optional fields are caller context, not
authorization or proof. This opcode requires configured production controls;
an anonymous/default listener refuses it even though older development
operations keep their existing behavior. Production authenticates the existing
single-tenant credential and requires the request tenant to match. The metadata describes
whole native partitions, including other organizations that share them.

The request has a 44-byte fixed header: `STP1`, one flags byte (UUID, plane,
start, end), one reserved zero byte, little-endian u16 tenant length, plane
length and count, raw UUID (zero when absent), and two little-endian u64
timestamps (zero when absent). UTF-8 tenant and plane bytes follow, then one
u8 signal and u32 partition ID per selection. Counts and exact byte lengths
are checked before allocation. The native reader rejects requests larger than
8 KiB before allocating or reading the body.

The response contains `STP2`, a u32 encoded-request length and exact request,
five zero bytes for the unknown proof dimensions below, a u16 status count,
and fixed 63-byte partition observations. Each observation has signal and
partition ID, a population byte (unknown, verified empty, allocated), progress
flags, and seven u64 values: WAL retained start, allocated end, contiguous log
end, last stable offset, replicated high watermark, applied index end, and
applied next placement sequence. Flags distinguish unknown progress and absent
checkpoints from observed zero. Unknown/absent values have zero wire storage,
not a positive zero watermark. All offsets are exclusive ends except the WAL
retained start; offset zero is a real first record. The response is at most
64 KiB; the client checks this bound before allocation and rejects changed
scope, order, counts, impossible frontiers, unsupported proof claims, malformed
UTF-8, truncation or trailing bytes. Existing connection frame/outbound bounds
can impose a smaller limit and refuse the response explicitly.

`DurableTelemetryStore::local_provider_status` reads the actual per-partition
StreamEngine watermarks, then its actual durable-sink checkpoint, then the same
watermarks again. It performs no record query or whole-catalog enumeration.
Concurrent frontier changes, metadata errors, impossible checkpoint coordinates
or deadline exhaustion leave that partition unknown. Observed applied ends
cannot exceed the sampled stable WAL frontier. The distinct placement sequence
is not converted into a record offset. These are sequential per-partition
observations, not a transaction across the selected partitions.

Verified empty requires an SDK-owned standalone partition, a successfully
loaded initial sink checkpoint, and coherent zero allocation/resolved/replica
frontiers. Attached or HA zero-frontier partitions remain unknown. Allocated
means an offset was assigned; resolved frontiers can include aborted slots and
do not establish that a durable telemetry record or retained query row exists.
An applied checkpoint records previously completed indexing/publication, and
may remain advanced after query payloads expire. The WAL retained floor does
not identify the earliest queryable object-tier row. This API therefore keeps
query-retention coverage, requested source-interval coverage, scoped loss,
HA assignment epoch, and indexed replica quorum explicitly **unknown**. It
never reports full availability or an organization-scoped queryable watermark.

Status uses the existing complete-query product gate, lifecycle/query admission,
authentication, inbound/outbound reservations and production query deadline.
One sampling budget stops additional metadata reads after expiration. An engine
checkpoint operation already in progress can outlive that budget; its physical
blocking worker retains the query permit until it finishes, including after a
native timeout response. Direct Rust callers must provide equivalent physical
admission and authorization. Remote receipt of this status is local evidence
from that server, not indexed-quorum proof or a source/loss completeness claim.

## Ports and durability

`shard-telemetry-server` listens on Loki/Prometheus HTTP `3100`, native TCP `3101`, OTLP/gRPC `4317`, and OTLP/HTTP `4318` by default. Native append acknowledgement means the authoritative shard-stream append is durable; configurations may additionally wait for owner-stripe query visibility.

The corpus loader's `--protocol native` mode constructs `STEL` log envelopes and `STB1` partition batches. No other ShardTelemetry native protocol version is accepted because the product has not released a compatibility contract.

OTLP, Prometheus Remote Write, and Tempo retain the version identifiers defined
by those external specifications. They decode into this one ShardTelemetry v1
storage and native-protocol representation.

## Embedded nodes and upstream offload

The Rust crate exposes `EmbeddedTelemetryRuntime` for a single local owner and
`UpstreamOffloader` for a background store-and-forward deployment. The runtime
recovers its local WAL, indexes, object catalogs, and metric accumulators before
the host marks it ready. The offloader reads authoritative local `STEL` batches
in partition order and persists a source-offset checkpoint only after the
upstream native server acknowledges the batch. Local source reclamation remains
under local retention control, so an offload outage cannot turn into telemetry
loss. S3 offload uses the existing immutable object-tier catalog path.
