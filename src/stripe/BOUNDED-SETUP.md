# Admitted empty log-stripe setup

`LogStripe::prepare_until` constructs the actual ordinary empty stripe fields
under the supplied `TelemetryRecoveryBudget` and its original expiry. Its
opaque `PreparedLogStripe` is the source accepted by the owned log sink
factory. There is no ordinary-stripe converter, public mutable stripe
extractor or readiness assertion. A terminal error retains the original
configuration, partial vectors/collator, native context and actual Results,
or a complete stripe refused at its last check.

Checked sizing prepays fixed carriers, both ordinary 1024-entry lookup caches,
the configured locality profile capacity (also when locality is disabled),
and a native context reservation before configuration validation, allocation
or native setup. Dictionary-cache capacity is a future logical ceiling; the
empty dictionary cache allocates no dictionary payload. Empty maps, catalogs
and query caches keep the ordinary constructor's initial state. No shared
dictionary catalog, realtime trainer or object-tier operation enters setup.

Each real vector uses `try_reserve_exact`. Actual Results/capacities are saved
before later checks; measured capacity excess is admitted before population.
The locality helper constructs the same ordinary configuration and diagnostic
state with an initially empty vector. The original constructor is unchanged.

Selected zstd 0.14 bulk `Default` wraps zstd-safe 8 `CCtx::create`, whose one
native allocation is `ZSTD_createCCtx`; its NULL panic is caught in the whole
attempt. The successful actual bulk owner is retained immediately. Compression
level setup and empty dictionary loading call its real context separately;
their original Results and native `sizeof` are saved before postchecks and
observers. Thus a later native error, expiry or panic does not drop a completed
context inside an opaque `with_dictionary` constructor. The completed stripe
uses that same bulk compressor; no alternate compression algorithm is added.

The context byte limit prepays a logical reservation and checks observed native
`sizeof`. Excess admission is attempted and the actual allocation is retained
on refusal. It cannot prevent a larger native allocation, bound native scratch
or allocator metadata/RSS, or impose a hard bound on a native call's duration.
Source work includes configuration validation's two native level-bound queries,
create/set/load and three size observations. Setup does not bound subsequent
ingest, queries or destruction. Startup accounting never grants live authority.

Six source regression groups use actual constructors/native contexts and the
real original recovery ledger: ordinary configuration/locality/cache/query and
compressed-byte parity (including dictionary changes), prior composing work,
exact/one-less original byte/work limits, invalid or expired input, ten real
captured phases with late checks/panics, and actual native size excess. These
are component tests, not host, current-control or factory readiness evidence.

The complete product still needs admitted factory and registered worker setup,
adopted replay, other signals/object modes, private product/host joins and
original-graph compilation/runtime qualification. No shipping pin, manifest or
lock change is part of this constructor.
