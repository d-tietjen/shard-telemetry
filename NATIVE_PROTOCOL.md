# ShardTelemetry native protocol

The native protocol is ShardTelemetry's high-throughput binary interface on TCP port `3101`. This pre-release repository ships exactly one protocol and one durable format: native protocol v1 carrying checksummed `STEL` v1 envelopes. There is no legacy append decoder or dual-format storage path.

## Frame

Every request and response begins with a fixed 32-byte little-endian header:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `STNP` |
| 4 | 1 | Version `1` |
| 5 | 1 | Opcode: append `1`, query `2`, ping `3`, authenticate `4` |
| 6 | 1 | Flags; bit 0 marks a response |
| 7 | 1 | Status |
| 8 | 16 | Caller-selected request ID |
| 24 | 4 | Payload length |
| 28 | 4 | First four bytes of the BLAKE3 payload digest |

Frames are limited to 64 MiB. Unknown flags, versions, opcodes, oversized payloads, malformed UTF-8, truncation, trailing bytes, count mismatches, and checksum mismatches fail closed. Multiple requests may be in flight on one connection and responses may complete out of order.

In production mode the first frame must authenticate with the configured bearer token. Append and query tenants must match the authenticated single tenant.

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

Full LogQL, PromQL, and TraceQL remain on the compatible HTTP APIs.

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
