# ADR 0005: LocalStore v1 and native SQLite

Status: accepted (2026-09-21).
Task: NIM-SDK-001

Independent data, code, QA, security and license content reviews approved candidate `27bc0424288c14c127080db0f2a6b9f84c89912b`. Final acceptance evidence for the integrated commit and delivery verification are recorded separately.

## Boundary

The platform-neutral core owns bounded requests/completions and transaction preconditions. It has no SQL, JSON, SQLite, network or executor dependency. Hosts dispatch requests and consume completions, checking account/instance/generation/operation identity. The native adapter executes one request at a time and publishes a completion only after commit; its submit method may block and belongs on a host storage worker. IndexedDB will implement the same contract in a later platform task.

SQLite 3.53.4 is built from the official checked amalgamation, statically linked through non-bundled rusqlite 0.40.2/libsqlite3-sys 0.38.2. Their bundled source is 3.53.2 and is not used. Explicit source preparation and cargo fetch may access the network; engine build and all native Cargo commands are offline and reject missing caches. Engine identity and compile options are checked before opening files. Core/protocol wasm builds exclude the native crate.

## Operations and atomicity

A database directory belongs to one account and must be private (mode 0700); hostile concurrent path replacement by another process with the same OS UID is outside this boundary. Existing path components and owned children must not be symlinks; operations are confined to its own active/quarantine directories and never overwrite a quarantine. A kernel file lock admits one live adapter/recovery owner; process death releases it. Metadata persists a random instance identity, generation and revision. Every operation supplies the full fence; advancing generation revokes old requests/completions. A rebuild creates a different instance. Requests have positive operation IDs; mutating IDs increase within a generation. The latest successful enqueue/apply/trim mutation retains its bounded exact request representation and receipt. Its identical retry returns Replayed without applying any change. An older expired operation returns OperationExpired and requires bounded read/replan with stable message IDs; there is no unbounded receipt ledger or automatic retry loop. AdvanceGeneration invalidates the old fence and resets operation identity; a lost completion requires reopening to read the new persisted fence.

A batch contains expected revision, message insert preconditions, optional absolute conversation snapshots, optional opaque cursor and explicit pending resolutions. All changes and the receipt commit together, or none do. Duplicate identity keys within one batch are rejected before any writes, so an Existing result never refers to an insert rolled back by that same batch. A message insert expecting absence that encounters an existing identity returns Existing with the stored record and rolls back the entire batch. This is a normal reconciliation result. A caller that has read that existing record may explicitly request PreserveExisting; this checks its exact storage snapshot and never overwrites payload. Thus a duplicate can complete normally without becoming an unrecoverable error. Revision/identity changes require bounded reload/replan outside the adapter.

The adapter checks unique server ID, sender/client ID and conversation/sequence mappings. Contradictory mappings are IdentityConflict. It does not infer JSON semantic equality: different whitespace/escapes are not automatically payload conflicts, and no existing persisted payload replacement API is provided. Core/caller supplies absolute unread/summary values, not increments, after deduplication. Unexpected Existing cannot advance unread/cursor or remove pending. Explicit pending resolution only attaches an already present matching authoritative identity and deletes that pending entry in the same transaction; determining trusted server ACK and send success belongs to the future send coordinator, not codec or storage.

Payloads are owned bounded bytes. The codec boundary validates PRT inputs; storage neither parses nor rewrites JSON. Huge exponent tokens, integers and escaped NUL survive BLOB storage. Numeric sequence/time indexes use nonnegative signed 64-bit integers; pending has no server sequence. Account identifiers, schema/type bounds and byte budgets are validated before SQL. A query returns at most 64 records and 256 KiB; pagination uses numeric sequence/ordered pending keys, never timestamps. Operation/batch inputs have bounded counts and aggregate bytes. Local pending count is capped with explicit CapacityExceeded rather than silently trimming unsent work.

## Schema and maintenance

WAL, synchronous=FULL, foreign_keys=ON, trusted_schema=OFF; explicit transactions contain schema versions and exact migration SQL checksums. Initial migrations 1 and 2 are genuine incremental schema steps, not claimed historical releases. Unknown future versions, altered migration history or missing schema are refused. No destructive downgrade is supported. Migration failure rolls back to the preceding supported schema.

Trim removes only persisted payload cache bytes in bounded batches; identity rows remain as deduplication metadata. It never deletes pending, conversations or cursor, and makes no server retention decision. A trimmed record is explicitly payload-absent; no fake empty payload is returned. Identity metadata has a hard 10,000-record ceiling (hosts may lower it), pending has 256 entries, and conversation/user caches have 1,000 entries each. SQLite has a 128 MiB page ceiling (hosts may lower it); WAL is checkpointed separately. CapacityExceeded/StorageFull are explicit and observable. Trimming payload does not free identity capacity; only an explicit recovery/rebuild coordinated by a future authoritative bootstrap can reset it, and this task does not claim that bootstrap is implemented. Compact is separate, bounded by an interrupt deadline, and reports Busy/Full/Cancelled/IO errors. Metrics include page/freelist/page size, DB/WAL bytes, pending count and revision. No production SQL/text/body/account/cursor logs are emitted.

## Crash and corruption recovery

A process killed before commit leaves no partial batch; a kill after commit may lose completion but the receipt makes the last mutation replayable. This tests process termination, not arbitrary power loss or faulty hardware. SQLite errors map to stable local codes; ambiguous commit errors return CommitOutcomeUnknown and require reopen/check before replan. Corrupt/ambiguous-commit results freeze the adapter until it is reopened.

Open never deletes a corrupt database. Recovery acquires the same kernel lock, writes and syncs a recovery marker, then moves the entire closed active directory (DB/WAL/SHM) to an exclusive quarantine directory. An interrupted recovery is recognizable and refuses ordinary open. Rebuild creates a fresh active database with a new instance, keeps quarantine intact and reports unresolved pending recovery; it is not a claim that unsent messages were recovered. Readable pending can be exported in bounded pages from a quarantined DB; corrupt/unreadable pending remains explicit RecoveryRequired. Original files are never overwritten. A separate explicit reconciliation step is required before considering all local pending recovered; SDK001 provides no silent "discard pending and healthy" path.

## Compatibility and acceptance

First local API/schema release, no deployed migration claim. No wire/send/ACK/sync/recall/edit semantics or Web runtime implementation. Tests use actual disk SQLite for identity/revision/fence/receipt/batch rollback, numeric indexes, codec roundtrip, migrations, trim, fullness, locks, subprocess kill and quarantine/rebuild. Core conformance scenarios are reusable and run without SQLite/JSON. Root native build/check and portable wasm build remain required. Dependency provenance and exact graph are reviewed separately; implementation author does not sign independent acceptance.

## Initialized-file loss correction

An existing SQLite file with schema version zero is not evidence of a new store. SQLite accepts zero-byte files as empty databases, so initialization now requires an explicit capability granted only when this open call exclusively creates a previously absent active directory. Existing active directories must contain a nonempty supported database; missing/truncated/empty-schema files fail before migration or journal-mode writes and retain their bytes. The migration entry separately refuses version zero without this capability.

The durable root `initialized` marker binds account and store instance. Valid first-release generic markers can be upgraded only after verifying a supported database; existing full markers must match the instance before migration. A supported committed database without a completed marker can finish initialization using the same instance. Incomplete schema creation remains RecoveryRequired and is quarantined explicitly. Marker publication uses a synced temporary file inside the active directory plus atomic rename and directory sync; an interrupted publication is either resumed for the same bytes or rejected without discarding evidence.

Explicit rebuild can create a new instance only after quarantine authorization and keeps recovery_required visible. Rebuilding an already-present active directory never reinitializes an empty file. If rebuild itself was interrupted before a supported schema was committed, a new uniquely named quarantine can preserve that partial active directory while retaining every previous quarantine, then a new rebuild attempt proceeds. The account binding cannot change during rebuild. This is a fail-closed recovery workflow, not automatic healthy storage recreation.

Legacy generic markers contain no account identity. Before rebuild creates or opens active data, a missing/generic binding must be resolved from the current quarantine's read-only, bounded, supported SQLite metadata and must match the requested account. If that identity cannot be safely read, rebuild returns RecoveryRequired without creating active files or changing either marker or quarantine. A readable legacy store can therefore rebuild only under its original account. This also applies when the initialized marker was lost. Unbound first-creation remnants with no committed readable identity are retained for inspection; this API does not offer an account override to recreate them in place. The host can explicitly choose a separate new directory without claiming that the quarantined data was recovered.

## Additive pending mutation port (NIM-SDK-002)

Status: accepted amendment, 2026-09-23.

The existing `LocalStore`, `Action` enum, request bytes and receipt semantics remain unchanged.
SDK Core adds a separate additive `PendingMutationStore` port for the two outbox-only CAS
transitions:

- `update_pending(PendingMutation) -> PendingReceipt`
- `remove_pending(PendingRemoval) -> PendingReceipt`

Each request carries the exact `(sender_id, client_id, conversation_id)` identity and the last
observed revision. The native adapter updates or removes exactly one matching pending row and
returns `IdentityConflict` on a missing/contradictory identity or `StaleRevision` when the CAS
revision does not match. Success increments the store revision and commits the mutation and
receipt atomically. The adapter does not parse the opaque pending payload, does not insert or
modify message rows, and does not change `pending_outbox` schema or migrations. `remove_pending`
is an explicit terminal/auth-recovery dismissal; ordinary queue cleanup cannot call it. The
wire-neutral coordinator remains responsible for classifying terminal states before requesting
removal and for all ACK authority checks.
