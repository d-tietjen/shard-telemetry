# Security Policy

## Supported versions

Security fixes are applied to the latest release and the `main` branch. Before
the first stable release, only the most recent `0.x` release is supported.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's
**Security** tab and select **Report a vulnerability** to submit a private
security advisory to the maintainers.

Include the affected version or commit, deployment assumptions, reproduction
steps, impact, and any proposed remediation. We will acknowledge a report
within five business days and coordinate disclosure after a fix is available.

Never include production credentials, customer logs, or other sensitive data
in a report. Use synthetic evidence or redact it first.

## Production object storage

Production requires either the synchronized local object backend or the
Rust-native S3 backend. S3 credentials are never accepted as command-line
fields; use short-lived workload identity through the standard AWS credential
chain. Scope permissions to one deployment prefix, block public access, enable
bucket versioning, and require TLS. Plaintext S3-compatible endpoints are
rejected whenever authentication enables production mode.

ShardTelemetry does not list buckets or run a tracing garbage collector.
Publication first records a bounded set of transaction-owned exact keys in
`PENDING`. Expired uncommitted transactions and superseded catalog generations
are deleted only by those authenticated keys after their writer/reader leases
expire. Treat `CURRENT` and `PENDING` as integrity-critical control objects;
operators must never delete or edit them to clear an incident.

The S3 adapter explicitly aborts multipart uploads on reported read, part, or
completion failures. A hard process or host failure can occur before an
incomplete multipart upload has a visible object key, so configure the object
provider to abort incomplete multipart uploads after a bounded interval. Do
not apply an age-based lifecycle expiration to completed objects.

Keep the administrative ClickHouse scan route and native protocol on private
networks. `/ready` and `/metrics` are intentionally unauthenticated and should
be exposed only to trusted health and monitoring infrastructure.

## Reviewed advisory exceptions

`RUSTSEC-2025-0141` reports that `bincode` is unmaintained; it does not report
a vulnerability, unsoundness, or affected function. RustSec records no patched
version and states that the maintainers consider `1.3.3` complete. The pinned
Apache-2.0 `promql-parser 0.10.0` dependency reaches `bincode 1.3.3` only through
`lrlex/lrpar 0.13.10`.

The release gate permits only that exact dependency path. The executable
`scripts/check-advisory-exceptions.sh` fails if the version, parent chain, or a
direct ShardTelemetry use changes. Every vulnerability and every other warning
remains denied. Re-review or remove this exception by 2026-11-11, and remove it
immediately if RustSec changes the advisory classification or the parser stack
offers a maintained replacement without a compatibility regression.
