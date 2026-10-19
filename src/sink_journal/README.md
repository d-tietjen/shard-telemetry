# Bounded journal recovery

The additive Unix `SinkJournal::open_until` is the actual journal producer for a
future owned factory. It returns the ordinary appendable journal and its real
replayed transactions. The existing `SinkJournal::open` remains unchanged.

The caller supplies its actual retained directory `File`, explicit journal
limits and the same `TelemetryRecoveryBudget` used by other setup producers.
The producer keeps that ledger's original expiry. It duplicates the directory
descriptor with close-on-exec and opens the fixed shard filename with
`openat`, `NOFOLLOW` and `NONBLOCK`. It requires a single-linked regular file,
takes its exclusive advisory lock and retains the actual file descriptor.
All reads, initialization, tail repair, seeks and syncs use that same inode.
They do not rewalk ancestor paths. Missing files are created exclusively only
after file and parent cleanup syncs are prepaid. Name/inode/size checks reject observed
replacement or external mutation. An ancestor rename cannot redirect repair
into a replacement directory tree.

The advisory lock coordinates bounded constructors. The consuming host must
also coordinate ordinary writers and namespace changes with its genuine
directory owner. An uncooperative writer can race namespace changes between
checks; these checks are not an atomic namespace lease or a distributed write
fence. This producer does not manufacture current write permission.

Local positive limits bound file/frame bytes, cumulative transactions,
appends and payload bytes, admitted I/O attempts and requested I/O/truncate
bytes. Every filesystem attempt is separately admitted against those limits
and the shared ledger. Cleanup slots may be admitted before entry and remain
spent if the mutation itself refuses. Explicit read/write loops recheck the saved expiry and
pay each short or interrupted attempt. Other I/O errors are terminal. A
filesystem syscall can outlast the expiry; this is not a wall-time guarantee.

After checksum validation, a first frame census verifies the fixed 76-byte
transaction header and at least 16 bytes per claimed append before allocating
an append vector. It pays cumulative shared record limits before typed replay,
then checks complete payload ranges, physical shards, offset order, trailing
bytes and monotone checkpoint chains. The second pass creates typed appends
whose `Bytes` slices share the already-measured frame allocation. Fallible
vector and map reservations are prepaid, and measured capacity excess is
admitted before use. Every checkpoint get/contains/insert prepays probe work
from actual retained capacity, including spare slots; growth separately pays
old-capacity movement and new-capacity work. These are declared state/capacity bounds; allocator,
`Bytes`/hash-table implementation overhead and process RSS are not measured.
Growing replay vectors conservatively charge each new reservation and copy
work; a small cumulative budget can refuse a large replay before native work.

Refusal retains the actual file/directory descriptors, partial frame and typed
replay, allocation/I/O/refusal or panic, and completed initialization/repair
observations. A real successful file operation is recorded before the
post-operation clock check. File/parent durability attempts are prepaid before
exclusive creation, initialization writes or tail truncation. Once mutation
enters, each required real sync runs despite expiry, finite refusal or unwind,
without renewing the ledger. The primary mutation error/panic is saved before
cleanup; independent file and parent sync errors/panics remain retained, and
file-sync failure cannot suppress the prepaid parent sync. Failed sync is an
actual retained refusal, never a durable-success claim. The shared ledger becomes terminal on failure.
Dropping the error releases the advisory lock; a later independent recovery
can read the actual repaired file. No failed constructor retries on its
denied ledger or claims that a completed truncate/sync did not happen.

Filesystem regression sources cover ordinary-wire replay and append/restart,
descriptor anchoring across ancestor replacement, symlink/FIFO/hard-link
refusal, impossible append counts, invalid offsets/checkpoints/trailing bytes,
cumulative count/payload and file/frame/I/O refusal, real short-tail repair,
corruption with retained partial replay/locks, and actual completed repair
followed by finite or original-expiry refusal with the prepaid sync completed.
Creation/initialization fixtures require both real file and parent syncs after
finite/E/unwind refusal, including independent post-sync panics. The deadline fixture pauses
after a real truncate until its real original expiry; it does not fake I/O or
construct a replacement graph. Separate fixtures retain the actual post-repair
panic and a genuine symlink-open error observed after the saved expiry.

The caller must still bound stripe/dictionary/object recovery, replay indexing,
worker creation and the complete factory's retained state. This journal unit
does not connect an owned product, qualify a full attachment or publish
readiness. Compilation and runtime acceptance require the unchanged original
dependency graph; source-defined tests alone do not establish either.
