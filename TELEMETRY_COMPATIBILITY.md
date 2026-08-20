# OTLP, Prometheus, and Tempo compatibility

ShardTelemetry exposes production-compatible protocol profiles for signal
ingestion and common Grafana, Prometheus, and Tempo query paths. Compatibility
means the surfaces listed here are tested and fail closed. It does not mean the
single-node storage process implements Prometheus rule scheduling,
Alertmanager, Tempo's distributed control plane, or every query-language
function shipped by those products.

## OTLP

The server accepts protobuf or JSON OTLP over HTTP on `/v1/logs`, `/v1/traces`,
and `/v1/metrics`, plus the Loki-compatible `/otlp/v1/logs` alias. The matching
gRPC Logs, Trace, and Metrics services support gzip. Request bodies are bounded
at 64 MiB after decompression.

A complete export request is decoded and validated before partition appends
begin. Typed attributes, nested arrays/maps, resource/scope context, schema
URLs, dropped counts, IDs, flags, events, links, exemplars, temporality,
histograms, summaries, stale markers, and floating-point bit patterns enter the
signal-native durable records. Invalid input rejects the request; the server
does not acknowledge a silently truncated subset.

`otlp_server::tests::every_otlp_http_and_grpc_signal_accepts_an_empty_valid_export`
executes every registered HTTP path and all three gRPC service methods. The
codec/property suites cover non-empty typed records and exact reconstruction.

## Prometheus protocol profile

The HTTP API registers:

- Remote Write v1 and negotiated v2 on `POST /api/v1/write`;
- sampled and streamed Remote Read on `POST /api/v1/read`; and
- `query`, `query_range`, `series`, `labels`, label values, `metadata`, and
  `query_exemplars`.

Remote Write preserves samples, native histograms, exemplars, stale-NaN
markers, and NaN payload bits. Conflicting same-timestamp Remote Write samples
are rejected; OTLP overlap uses the highest durable offset with diagnostics.
Remote Read emits Prometheus XOR chunks with CRC framing when streamed.

The Rust evaluator uses `promql-parser` for the AST and currently supports:

- instant/range selectors, exact/negative/regex label matchers, lookback,
  offsets, `@`, subqueries, and stale-marker removal;
- scalar/vector arithmetic and comparison, `bool`, vector matching,
  `group_left`/`group_right`, and `and`/`or`/`unless`;
- `sum`, `avg`, `count`, `group`, `min`, `max`, `stddev`, and `stdvar`, with
  `by`/`without`;
- `rate`, `irate`, `increase`, `delta`, `idelta`, `changes`, `resets`,
  `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`,
  `count_over_time`, `last_over_time`, and `present_over_time`; and
- `time`, `vector`, `scalar`, `abs`, `ceil`, `floor`, `exp`, `ln`, `log2`,
  `log10`, `sqrt`, and `sgn`.

Other syntactically valid PromQL functions and aggregators return an explicit
HTTP 400 error. They are not approximated. This is the production support
boundary until differential Prometheus oracle evidence expands it.

## Tempo protocol profile

The HTTP API registers Tempo-compatible trace-by-ID v2, search, tag discovery,
tag values, TraceQL metrics range/instant responses, and the ShardTelemetry
correlation extension. Trace-by-ID resolves winning span versions and late
fragments; search is bounded by trace/span/time limits.

The clean-room TraceQL evaluator supports typed span/resource/event/link
fields, intrinsics, boolean and regex comparisons, structural relationships,
spanset boolean operators, `by`, `select`, count/sum/avg/min/max predicates,
and trace-derived `rate`, count/sum/min/max/avg/quantile-over-time metrics with
grouping, thresholds, top-k, and bottom-k. Unsupported pipeline stages or
functions return HTTP 400 and never produce partial approximations.

No Tempo source or AGPL fixture is included. Differential qualification uses a
pinned external Tempo binary as an oracle and retains only independently
generated cases and aggregate evidence.

## Executable gates

- Loki routes: `stable_loki_route_surface_has_no_missing_or_wrong_method_routes`
- Prometheus routes: `stable_prometheus_route_surface_has_no_missing_or_wrong_method_routes`
- Tempo routes: `stable_tempo_route_surface_has_no_missing_or_wrong_method_routes`
- OTLP transports: `every_otlp_http_and_grpc_signal_accepts_an_empty_valid_export`
- ClickHouse SQL boundary: the pinned stock-ClickHouse matrices documented in
  [CLICKHOUSE_COMPATIBILITY.md](CLICKHOUSE_COMPATIBILITY.md)
- External competitive oracle campaign:
  `scripts/run-competitive-oracles.sh` on Linux/amd64, which sends one
  generated OTLP fixture to Prometheus, Loki, Tempo, and ShardTelemetry; runs
  the stock ClickHouse SQL matrix; and validates the bounded DuckDB NDJSON
  analytical interchange.

Production qualification must also run the pinned external oracles on Linux.
The repository's unit/route matrices prove registration and internal semantics;
they are not substitutes for a same-version differential campaign.

The external campaign is scheduled in GitHub Actions and retains fixture hashes,
image identities, responses, server logs, and ClickHouse evidence. It is a
functional compatibility gate. Larger equal-host performance campaigns remain
separate and may only publish measurements with the corpus, CPU allocation,
build profile, and verification artefacts required by `CONTRIBUTING.md`.
