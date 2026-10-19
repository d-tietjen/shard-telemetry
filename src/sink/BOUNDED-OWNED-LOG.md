# Actual owned log factory and worker

`OwnedLogSinkFactory` implements the actual `DurableAppendSinkFactory` trait.
Its real `ShardTelemetryStripeSink` owned-state variant implements
`DurableAppendSink::apply_until`. This is an executable strict recovered-log
route, using the same native dispatcher that supplies the real input and
`NativeResumeWork`. It reaches the consuming
[journal/index/application transaction](BOUNDED-APPLY.md); there is no ordinary
apply fallback or replacement work owner.

Setup consumes `Vec<OwnedLogShard>`. Each source requires an opaque
`PreparedLogStripe` from `LogStripe::prepare_until` and an opaque
`AnchoredSinkJournal` from its actual descriptor-based `open_until`. There is
no ordinary-journal or ordinary-stripe converter. The caller passes the SAME
original `TelemetryRecoveryBudget` through these producers and factory setup.
The factory also retains and checks the minimum of its caller's setup E and
every actual source setup E. Source metadata does not independently establish
ledger identity, namespace ownership, current HA permission or readiness.

The complete original source vector enters custody before fees. A prepaid
full census refuses mismatched/duplicate physical shard IDs, mixed journal
formats, and any journal rows supplied to the original `new_until` entry. Fresh real application-checkpoint/cache owners are allocated in
stages, with each completed owner retained before its following check. Actual
source/vector capacities, fixed Arc requests, worker names, requested stacks,
closure boxes and scalar-table search work are admitted on the original
recovery ledger. Each fallible reservation's measured excess is admitted
before the next allocation. `OwnedLogFactoryLimits::setup_cost` exposes the
full configured declaration for outer pre-admission; the actual producer
still spends its shared ledger and refuses excess. These are declared source
values and capacities, not allocator/RSS or scheduling-duration bounds.

`new_with_replay_until` takes an explicit complete configured log-partition
scope and finite replay limits. Its first actual native checkpoint callback
owns the original `NativeResumeWork`; it validates every recovered source row,
its payload/count/checksum, and the complete global per-partition placement
chain before any index mutation. Actual journal-only inputs enter the same
registered worker, real bounded decoder/index producer and shared application
map. They never become fabricated `DurableAppend` or reservation values and
never return `DurableSinkApply::Applied`. The factory remains quarantined until
whole-plan completion. Any partial/late/error outcome remains owned and closes
the factory; there is no retry or mutable-owner extractor.

The original `open_until` produces `STELSNK1` sources and retains the exact
per-anchor placement comparison. `open_application_until` explicitly produces
`STELAPP2` sources for actual global application edges spread across physical
shards. Its only writer takes a closed borrowed seal from the real held global
application-checkpoint transaction and uses that seal's original coordinates
and work. A local physical source may skip placements only after that genuine
global comparison succeeds. Frame shard IDs and native expected/next remain
unchanged. Both openers reject the other format, and factory setup rejects
mixed formats. The application format requires explicit replay opt-in even
when empty. No source is relabeled or automatically migrated.

Each physical shard has one fixed central pending command slot. There is no
hidden channel queue. Before copying the actual borrowed native append, the
strict callback pays for its retained error carrier, command box, visible
input bytes and every possible bytes 1.12.1 Shared promotion header. The copy
is saved before a later check. The actual queue attempt separately spends
`NativeResumeWork::reserve_job`, which charges the original jobs counter.
Contention refuses through the already-owned carrier; it does not enter an
ordinary handler or wait reentrantly on another command's mutex.

The actual worker enters the frozen consuming transaction with the same
original input/work and shared journal/checkpoint/cache owners. It saves the
complete actual success/error, effect and observer panic before marking the
slot settled. After queue publication, the callback waits for physical
settlement even if E expires or an observer unwinds. An accepted queued source
cannot disappear into a disconnected channel. The complete late/error result
stays in the already-paid callback carrier, which retains the actual worker
and anchored owners. Only accepted success retires its receipt and restores
the actual mutable stripe for a later command. An error cannot extract the
partially moved native owner for retry.

Negative drain and command errors close the actual anchored dependency. The
idle loop uses a retained native `Thread` unpark token, initialized before its
first predicate read; queue and negative wakeups cannot be lost between a
predicate check and parking. Accepted-command completion still uses the
mutex-protected settled predicate and Condvar. Entered physical operations
settle and retain their outcomes before the callback returns an error.

Call `factory.begin_draining()` before joining the host's actual task registry,
while factory/sink/engine Arcs can remain alive. It closes all same worker
slots without needing their physical-work mutex. Idle loops wake, and accepted
commands physically settle or retain their actual pre-entry refusal. Then
the host must join its genuine native handles, including TLS completion,
before releasing engine/directory custody. The worker's `exited` flag denotes
body completion only; it is not a native join proof. Registration errors retain
their actual returned Result and source owner even when the host registered a
thread before reporting late failure. The host registry contract remains
responsible for every actual entered native handle. The worker captures only
shared state, avoiding a worker/registry strong-reference cycle.

The recovery-ledger decoder entry point consumes the original already-spent
setup ledger and uses the same retained streaming decoder as native resume.
Only recovery retained/work axes are charged; decoder entries keep their
explicit child ceiling and are not JSON records or filesystem reads. A success
can consume itself to reconstruct the next real frame on the same cumulative
ledger and original E, releasing the completed previous decoder first. A
failure retains the real native context, results, output prefix and ledger
without a retry or mutable native/work extractor. The native resume adapter
keeps its actual work owner, field layout and sized admission formula. This
is structural-frame decoder substrate; bounded typed-row reconstruction, the
owned query worker/facade and host/HA query ownership remain separate gates.
The new adapter regressions require actual qualification on their own source.

Existing fixture groups define actual filesystem/WAL/constructor/worker coverage.
The positive group calls a real recovered engine's complete bounded resume
through this actual factory and asserts native Applied observation, job debit,
normal query rows and the real shared application checkpoint. Other groups
cover copied/queued/settled/worker-result expiry and panic, retained actual
durable success despite later refusal, real reentrant Busy using the original
completed native append/work, source capacity/ID mismatch, explicit component
registry admission refusal, a genuinely entered native task with a later
registry deadline error, cumulative original setup exhaustion, and drain plus
actual native handle join while the factory Arc remains live. The fixture
registry starts and joins real threads; it does not fabricate a private
Prepared host or current control grant. The adoption and format groups use
true v1 reopen, application-format rotating native
placements, a later genuine WAL suffix returning to the first physical lane,
a second true reopen with exact rows/frame IDs, whole-plan duplicate refusal,
late/panic physical-outcome retention, unsealed-writer refusal and both header
mismatches without mutation. They do not establish host or HA readiness.

On 2026-10-19, the selected development source tuple passed all eight gates:
formatting, strict Clippy across all targets and features, and all six genuine
journal-adoption regression selections. Canonical tests of the same code,
split across the library and remaining targets, recorded 502 passed, eight
TCP-bind `EPERM` failures, and one existing live S3 test ignored. The aggregate
exit status remains 101. These results do not qualify the shipping dependency
graph, release build or release gate, full product/host installation, or HA
query ownership.

Existing legacy factories keep ordinary setup, callbacks and query workers.
This owned route explicitly lacks ordinary live `validate_append`, ordinary
apply, `TelemetryService` serving, exact-content retry, object/other-signal/atomic modes, private product and installed host
qualification. Those remain real gates. A strict recovered-log Applied result
does not itself qualify any of them. Shipping manifests, locks and dependency
pins are unchanged.
