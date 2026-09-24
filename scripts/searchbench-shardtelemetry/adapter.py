#!/usr/bin/env python3
"""SearchBench adapter for ShardTelemetry's analytical scan endpoint."""

from __future__ import annotations

import datetime as dt
import http.client
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, ResourceLogs, ScopeLogs
from opentelemetry.proto.resource.v1.resource_pb2 import Resource


HOST = os.environ.get("ST_HTTP_HOST", "127.0.0.1")
PORT = int(os.environ.get("ST_HTTP_PORT", "32100"))
OTLP_PORT = int(os.environ.get("ST_OTLP_PORT", "34318"))
TENANT = os.environ.get("ST_TENANT", "benchmark")
TOKEN = os.environ.get("ST_TOKEN", "shard-telemetry-searchbench-token")
QUERY_FILE = Path(os.environ["ST_QUERY_FILE"])


def kv(key: str, value: str) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(string_value=value))


def columns(table: pa.Table) -> dict[str, list]:
    result = {}
    for name in table.column_names:
        if name in ("ResourceAttributes", "ScopeAttributes", "LogAttributes"):
            continue
        array = table[name].combine_chunks()
        if name in ("Timestamp", "TimestampTime"):
            array = array.cast(pa.int64())
        result[name] = array.to_pylist()
    return result


def string_map(items) -> list[KeyValue]:
    return [kv(str(key), str(item)) for key, item in (items or [])]


def make_otlp(table: pa.Table) -> bytes:
    data = columns(table)
    groups: dict[tuple, ScopeLogs] = {}
    resources: dict[tuple, ResourceLogs] = {}
    for index in range(table.num_rows):
        service = str(data["ServiceName"][index] or "")
        scope_name = str(data["ScopeName"][index] or "")
        # SearchBench never predicates or groups on the high-cardinality map
        # columns. Keep the benchmark projection identical on every engine for
        # its searchable fields without spending the load run re-encoding the
        # same large resource maps on every OTLP record.
        resource_attrs = (("service.name", service),)
        scope_attrs = ()
        resource_key = resource_attrs
        scope_key = (resource_key, scope_name, scope_attrs)
        if resource_key not in resources:
            attrs = dict(resource_attrs)
            attrs.setdefault("service.name", service)
            resources[resource_key] = ResourceLogs(
                resource=Resource(attributes=[kv(str(k), str(v)) for k, v in attrs.items()])
            )
        if scope_key not in groups:
            groups[scope_key] = ScopeLogs(
                scope=InstrumentationScope(
                    name=scope_name,
                    attributes=string_map(scope_attrs),
                )
            )

        trace_id = str(data["TraceId"][index] or "")
        span_id = str(data["SpanId"][index] or "")
        groups[scope_key].log_records.append(
            LogRecord(
                time_unix_nano=int(data["Timestamp"][index] or 0),
                observed_time_unix_nano=int(data["Timestamp"][index] or 0),
                severity_number=int(data["SeverityNumber"][index] or 0),
                severity_text=str(data["SeverityText"][index] or ""),
                body=AnyValue(string_value=str(data["Body"][index] or "")),
                attributes=[],
                trace_id=bytes.fromhex(trace_id) if trace_id else b"",
                span_id=bytes.fromhex(span_id) if span_id else b"",
                flags=int(data["TraceFlags"][index] or 0),
            )
        )
    for resource_key, resource in resources.items():
        resource.scope_logs.extend(
            group for key, group in groups.items() if key[0] == resource_key
        )
    return ExportLogsServiceRequest(resource_logs=list(resources.values())).SerializeToString()


def load(data_dir: str) -> None:
    inputs = sorted(Path(data_dir).glob("part_*.parquet"))
    if not inputs:
        raise SystemExit(f"no parquet input in {data_dir}")
    total = 0
    started = time.monotonic()

    def send(batch: pa.RecordBatch) -> int:
        payload = make_otlp(pa.Table.from_batches([batch]))
        connection = http.client.HTTPConnection(HOST, OTLP_PORT, timeout=180)
        connection.request(
            "POST",
            "/v1/logs",
            body=payload,
            headers={
                "Content-Type": "application/x-protobuf",
                "Content-Length": str(len(payload)),
            },
        )
        response = connection.getresponse()
        response.read()
        connection.close()
        if response.status != 200:
            raise RuntimeError(f"OTLP load failed: HTTP {response.status}")
        return batch.num_rows

    worker_count = int(os.environ.get("ST_LOAD_WORKERS", "16"))
    if worker_count < 1:
        raise SystemExit("ST_LOAD_WORKERS must be positive")
    pending = set()
    with ThreadPoolExecutor(max_workers=worker_count) as workers:
        for path in inputs:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=8192, use_threads=True):
                pending.add(workers.submit(send, batch))
                if len(pending) >= worker_count * 2:
                    completed = next(as_completed(pending))
                    pending.remove(completed)
                    total += completed.result()
        for completed in as_completed(pending):
            total += completed.result()
    elapsed = time.monotonic() - started
    print(f"loaded rows: {total}")
    print(f"load elapsed seconds: {elapsed:.6f}")
    print(f"rows per second: {total / elapsed:.2f}")


def normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def query_map() -> dict[str, int]:
    result: dict[str, int] = {}
    current = None
    for raw in QUERY_FILE.read_text().splitlines():
        tag = re.match(r"\s*--\s*(Q\d+)\s+task=", raw)
        if tag:
            current = int(tag.group(1)[1:])
        elif current is not None and raw.strip() and not raw.lstrip().startswith("--"):
            result[normalize(raw)] = current
            current = None
    return result


def add(params: list[tuple[str, str]], key: str, value: str) -> None:
    params.append((key, value))


def spec(q: int) -> list[tuple[str, str]]:
    p: list[tuple[str, str]] = []

    def terms(*items: str) -> None:
        for item in items:
            add(p, "message_token_ci", item)

    def any_terms(*items: str, minimum: int | None = None) -> None:
        for item in items:
            add(p, "message_any", item)
        if minimum is not None:
            add(p, "message_min_match", str(minimum))

    def phrase(*items: str, gap: int = 0) -> None:
        add(p, "message_phrase", "|".join(items) + f":{gap}")

    def regex(pattern: str) -> None:
        add(p, "message_token_regex_ci", pattern)

    def service(name: str) -> None:
        add(p, "resource.service.name", name)

    def window(minutes: int) -> None:
        start = int(dt.datetime(2025, 9, 23, tzinfo=dt.timezone.utc).timestamp() * 1_000_000_000)
        end = start + minutes * 60 * 1_000_000_000
        add(p, "start_ns", str(start))
        add(p, "end_ns", str(end))

    if q == 1:
        terms("error")
    elif q == 2:
        terms("payment")
    elif q == 3:
        terms("failed", "order")
    elif q == 4:
        terms("failed", "charge", "card", "cache")
    elif q == 5:
        terms("failed", "send", "order", "confirmation", "email", "service", "expected", "post")
    elif q == 6:
        any_terms("error", "failed")
    elif q == 7:
        any_terms("connection", "request", "conversion", "post")
    elif q == 8:
        any_terms("payment", "exception", "refused", "send", "confirmation", "email", "expected", "deadline")
    elif q == 9:
        any_terms("error", "failed", "charge", "cache", minimum=2)
    elif q == 10:
        phrase("place", "order")
    elif q == 11:
        phrase("failed", "to", "place", "order")
    elif q == 12:
        phrase("post", "to", "email", "service", "expected", "200", "got", "500")
    elif q == 13:
        phrase("failed", "order", gap=2)
    elif q == 14:
        add(p, "predicate_operator", "or")
        phrase("failed", "to", "place", "order")
        any_terms("charge")
    elif q == 15:
        phrase("failed", "to", "place", "order")
        terms("charge")
    elif q == 16:
        regex("charg.*")
    elif q == 17:
        regex("ord.*")
    elif q == 18:
        regex("conn.*")
    elif q == 19:
        regex("c.che")
    elif q == 20:
        add(p, "message_token_prefix_ci", "conn")
    elif q == 21:
        add(p, "message_token_prefix_ci", "charg")
    elif q == 22:
        add(p, "message_fuzzy", "connection:1")
    elif q == 23:
        add(p, "message_fuzzy", "connection:2")
    elif q == 24:
        add(p, "message_fuzzy", "connection:2")
        add(p, "message_token_prefix_ci", "conn")
    elif q == 25:
        add(p, "message_like", "conn%")
    elif q == 26:
        add(p, "message_like", "%tion")
    elif q == 27:
        add(p, "message_like", "%nnec%")
    elif q == 28:
        terms("error")
        add(p, "message_not", "cache")
    elif q == 29:
        any_terms("error", "failed")
        add(p, "message_not", "charge")
    elif q == 30:
        terms("error")
        window(360)
    elif q == 31:
        service("frontend")
        terms("failed")
        window(360)
    elif q == 32:
        any_terms("payment", "exception", "refused", "send", "confirmation", "email", "expected", "deadline")
        window(360)
    elif 33 <= q <= 53:
        add(p, "order", "relevance_desc")
        add(p, "limit", "100")
        if q in (33, 34, 35):
            terms(("charge", "connection", "payment")[q - 33])
        elif q == 36:
            terms("failed", "order")
        elif q == 37:
            terms("failed", "charge", "card", "cache")
        elif q == 38:
            terms("failed", "send", "order", "confirmation", "email", "service", "expected", "post")
        elif q == 39:
            any_terms("error", "failed")
        elif q == 40:
            any_terms("connection", "request", "conversion", "post")
        elif q == 41:
            any_terms("payment", "exception", "refused", "send", "confirmation", "email", "expected", "deadline")
        elif q == 42:
            any_terms("error", "failed", "charge", "cache", minimum=2)
        elif q == 43:
            phrase("place", "order")
        elif q == 44:
            phrase("failed", "to", "place", "order")
        elif q == 45:
            phrase("post", "to", "email", "service", "expected", "200", "got", "500")
        elif q == 46:
            regex("conn.*")
        elif q == 47:
            add(p, "message_token_prefix_ci", "charg")
        elif q == 48:
            add(p, "message_fuzzy", "connection:1")
        elif q == 49:
            add(p, "message_fuzzy", "connection:2")
        elif q == 50:
            add(p, "message_like", "conn%")
        elif q == 51:
            add(p, "predicate_operator", "or")
            phrase("failed", "to", "place", "order")
            any_terms("charge")
        elif q == 52:
            terms("error")
            add(p, "message_not", "cache")
        elif q == 53:
            service("payment")
            terms("charge")
            window(360)
    elif 54 <= q <= 67:
        add(p, "wire", "json")
        if q in (54, 57, 60, 67):
            any_terms("error", "failed")
        elif q == 55:
            terms("charge")
        elif q == 56:
            terms("failed", "order")
        elif q == 58:
            regex("charg.*")
        elif q == 59:
            add(p, "message_fuzzy", "connection:1")
        elif q in (61, 65):
            terms("error")
        elif q == 62:
            terms("failed", "order")
        elif q == 63:
            phrase("failed", "to", "place", "order")
        elif q == 64:
            any_terms("error", "failed", "charge")
        elif q == 66:
            service("frontend")
            terms("failed")
        if q in (54, 55, 56, 66):
            add(p, "group_by", "severity_text")
        elif q in (57, 58, 59, 60):
            add(p, "group_by", "scope_name")
        elif q in (61, 62, 63, 64, 65):
            add(p, "group_by", "minute")
        else:
            add(p, "group_by", "severity_text,scope_name")
        if q in (54, 55, 57, 58, 59, 66, 67):
            add(p, "group_order", "count_desc")
        if q in (57, 58, 59, 67):
            add(p, "group_limit", "20")
    elif 68 <= q <= 83:
        add(p, "limit", "100")
        if q in (68, 69, 70, 71, 72, 74, 75):
            add(p, "order", "timestamp_desc")
        if q in (68, 76):
            service("checkout")
            terms("failed", "order")
        elif q in (69, 77):
            any_terms("error", "failed", "charge")
            add(p, "field_numeric.otel.severity_number", "ge:13")
        elif q in (70, 78):
            terms("error")
        elif q in (71, 79):
            phrase("failed", "to", "place", "order")
        elif q in (72, 80):
            service("payment")
            terms("charge")
        elif q in (73, 81):
            service("cart")
        elif q in (74, 82):
            regex("charg.*")
        elif q in (75, 83):
            any_terms("connection", "request", "conversion")
        window(30 if q in (68, 69, 72, 73, 75, 76, 77, 80, 81, 83) else 360)
    elif 84 <= q <= 92:
        add(p, "cardinality_only", "1")
        add(p, "columns", "offset")
        if q == 84:
            service("frontend")
            terms("failed")
            add(p, "trace_join_service", "payment")
        elif q == 85:
            any_terms("error", "failed")
            add(p, "trace_join_service", "payment")
        elif q == 86:
            phrase("failed", "to", "place", "order")
            add(p, "trace_join_service", "payment")
        elif q == 87:
            regex("charg.*")
            add(p, "trace_join_service", "frontend")
        elif q == 88:
            terms("failed", "order")
            add(p, "trace_join_service", "cart")
        elif q == 89:
            any_terms("connection", "request")
            add(p, "trace_join_service", "frontend")
        elif q == 90:
            terms("charge", "request")
            add(p, "trace_join_service", "frontend")
        elif q == 91:
            terms("order")
            add(p, "trace_join_service", "payment")
        elif q == 92:
            add(p, "message_token_prefix_ci", "charg")
            add(p, "trace_join_service", "frontend")
    else:
        raise ValueError(f"unsupported SearchBench query Q{q:02d}")

    if q not in range(1, 33) and q not in range(54, 68) and q not in range(84, 93):
        add(p, "columns", "timestamp,message")
    if q in range(1, 33):
        add(p, "cardinality_only", "1")
        add(p, "columns", "offset")
        add(p, "wire", "rowbinary")
    elif q in range(33, 54) or q in range(68, 84):
        add(p, "wire", "json")
    elif q in range(84, 93):
        add(p, "wire", "rowbinary")
    return p


def perform(sql: str, q: int) -> tuple[bytes, float]:
    params = spec(q)
    path = "/shardtelemetry/api/v1/clickhouse/scan?" + urlencode(params)
    connection = http.client.HTTPConnection(HOST, PORT, timeout=180)
    connection.connect()
    started = time.monotonic()
    connection.request(
        "GET",
        path,
        headers={"Authorization": f"Bearer {TOKEN}", "X-Scope-OrgID": TENANT},
    )
    response = connection.getresponse()
    body = response.read()
    elapsed = time.monotonic() - started
    connection.close()
    if response.status != 200:
        raise RuntimeError(f"Q{q:02d} HTTP {response.status}: {body[:500]!r}")
    return body, elapsed


def run_query() -> None:
    sql = sys.stdin.read()
    q = query_map().get(normalize(sql))
    if q is None:
        raise SystemExit(f"query is not present in {QUERY_FILE}: {normalize(sql)[:120]}")
    body, elapsed = perform(sql, q)
    rows = len(body) // 8 if dict(spec(q)).get("cardinality_only") == "1" else body.count(b"\n")
    print(f"SEARCHBENCH_ROWS={rows}", file=sys.stderr)
    print(f"{elapsed:.6f}", file=sys.stderr)
    sys.stdout.buffer.write(body)


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in {"load", "query"}:
        raise SystemExit("usage: adapter.py load DATA_DIR | adapter.py query")
    if sys.argv[1] == "load":
        if len(sys.argv) != 3:
            raise SystemExit("usage: adapter.py load DATA_DIR")
        load(sys.argv[2])
    else:
        if len(sys.argv) != 2:
            raise SystemExit("usage: adapter.py query")
        run_query()
