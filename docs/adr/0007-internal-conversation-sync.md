# ADR 0007: Durable internal conversation projection and pagination

Status: proposed; architecture, data and security design review required before implementation.
Date: 2026-09-21
Task: NIM-SYN-003

## Context and decision

PostgreSQL storage in ADR 0004 has users, membership, conversation summaries, messages and a transactional outbox. A conversation's `last_seq` orders its messages; it cannot order changes across one user's conversations. Outbox timestamps and sequence allocation with `nextval()` cannot supply a committed account watermark: a transaction with an earlier allocation could commit after a reader has advanced past it.

Introduce an internal Go conversation-sync application service, a PostgreSQL adapter, and three additive projection tables. Per-account row locks serialize revision allocation and commit. Immutable full-state revisions support bounded bootstrap and delta queries without reading message history. Opaque authenticated cursors carry a durable continuation position; no database connection or transaction survives between requests.

影响 / Impact: this introduces internal Go APIs, a cursor format and schema migration 003. It changes no existing persisted-message/send wire contract and no applied migration. The application package must not import a PostgreSQL driver. The adapter implements its storage interface with parameterized SQL and real transactions.

This is a storage/query capability, not a deployed multi-device service. There is no HTTP/WebSocket handler, listener, login hook, Rust codec, client merge implementation or automatic call from `persist_message`. Device policy, read/unread, mute, drafts, user-facing conversation deletion, existing-account backfill and authenticated network integration remain separate decisions and implementation. A source mutation that bypasses the new projection writer is not claimed to be synchronized. Publication of a complete conversation-sync service requires closing those write paths and validating every authorized mutation.

## Identity and internal contracts

The independent methods are `BeginBootstrap`, `ContinueBootstrap`, `BeginDelta` and `ContinueDelta`. A trusted caller supplies a principal separately from the request. No request user ID can replace that principal. An internal principal value is not authentication; only a later reviewed authentication boundary may construct one from external input.

Every operation validates the principal's user, the account projection state and cursor account binding. Every upsert returned to that principal also requires current membership in the same database read snapshot. Database credentials remain trusted service configuration. This is application-enforced account isolation, not a claim that the shared database role is an end-user security boundary.

Projection items contain only `conversationId`, positive account `revision`, the `upsert` or `remove` discriminator, and, for upserts, `latestConversationSeq` and nullable `latestServerMsgId`. IDs retain the accepted ASCII grammar and 128-byte bound; sequences retain the nonnegative signed BIGINT domain. No message body, user profile or device/read/mute state is included.

A remove records the existing fact that this account no longer has authorized visibility. It does not perform the removal, delete business data, choose a retention period or define a cross-device delete operation. The writer must verify that membership is absent and that the account previously had an upsert for this conversation. An unknown account/conversation cannot be used to invent a tombstone or discover another user's state. Repeating an existing remove returns its revision.

## Schema 003

`infra/db/migrations/003_conversation_sync.sql` adds exactly these tables in `newim`, with restrictive foreign keys and no new SQL functions or triggers:

| Table | Columns and keys | Purpose |
| --- | --- | --- |
| `im_conversation_sync_accounts` | `user_id` primary key and FK to users; positive `epoch`; nonnegative `last_change_seq`, `min_valid_seq`, with floor <= head | Committed account watermark and cursor invalidation |
| `im_conversation_sync_keys` | `(user_id, conversation_id)` primary key; account/conversation FKs; positive `first_change_seq` | Stable directory for bootstrap keyset traversal |
| `im_conversation_sync_changes` | `(user_id, change_seq)` primary key; directory FK; `conversation_id`, `kind`, `latest_seq`, `latest_server_msg_id` | Immutable full-state upsert/remove revisions |

`kind` is constrained to upsert/remove. Upsert requires nonnegative `latest_seq`; remove requires both summary columns to be null. An upsert's non-null server-message reference uses the existing `(conversation_id, server_msg_id)` unique key/FK, without reading payloads. A null latest pointer remains representable; sequence/pointer interpretation stays with the authoritative conversation summary. Foreign keys do not cascade deletion. No messages, membership or historical migrations are rewritten.

Add a B-tree `(user_id, conversation_id, change_seq DESC)` to the changes table. The directory primary key uses the existing identifier's `C` collation. Delta uses its account/revision primary key. Authorization uses the existing membership `(user_id, conversation_id)` index. The directory avoids `DISTINCT` or grouping across all revisions to find conversations.

The tables retain all revisions in this stage. Directory entries survive removes. Cursor expiry does not authorize deleting data. Physical compaction, preserving a floor anchor per conversation and handling backfill/rebuild safely, requires a subsequent reviewed design. Capacity growth and required monitoring are explicit operational limits.

## Empty-account initialization and migration readiness

Account-state existence means that its projection was initialized, not merely that a user row exists. Missing state returns `SYNC_NOT_READY`, including users with pre-existing membership after migration. Empty results must never disguise an uninitialized projection.

`InitializeEmptyAccount` owns a short READ COMMITTED transaction. Its first lock is a `SHARE` table lock on membership, before account/user locks; this conflicts with membership writes. It verifies that the user exists, has no membership, and has no existing projection history, then creates the initial account state `(epoch=1, head=0, floor=0)`. Concurrent membership insertion must wait or time out. Repeating initialization of the same still-empty initial account is idempotent; it must never reset head, epoch or floor. A populated account is refused, not silently imported.

Initialization is separate from mutation batches and cannot be nested after a caller has already acquired account or conversation locks. Its brief table-wide lock is limited to onboarding, not normal reads or projection updates. Source writes after initialization must be integrated with the projection writer before any complete service is exposed. The lock only protects the initialization transaction; it is not a promise to intercept future direct database writes.

Migration 003 preserves populated storage but does not make those existing accounts ready. No login path performs automatic backfill. Existing-account backfill, maintenance fencing and the closure of unintegrated source-write paths belong to the complete synchronization service.

## Mutation transaction and lock order

The adapter provides a transaction-scoped projection writer. The caller owns the real PostgreSQL transaction and its commit. The writer neither commits that transaction nor emits a persistence ACK.

Before changing source state, the caller declares the complete batch of existing conversation IDs and account IDs. The batch is bounded to 100 distinct conversations and 100 distinct accounts; oversize input fails before SQL. The adapter locks all conversation rows in ascending byte order, then all account rows in ascending byte order. Missing or uninitialized accounts fail the batch. Once an account lock is held, no new conversation lock may be acquired. Re-entering a conversation lock already acquired for the batch, for example through `persist_message`, is permitted. Every mutation, repair and future fanout path using this primitive must follow this order. Initializing an account is not part of such a batch.

After the authorized source mutation, `RecordUpsert` reads the authoritative summary and verifies membership; it does not trust caller-supplied summary fields. `RecordRemoval` verifies absent membership and prior visibility. Both verify that the target was declared in the batch. Within the same transaction they compare the most recent projection, allocate a checked `head + 1`, insert a directory entry if needed, append the full state and update the account head. The account row remains locked until outer commit.

The same current fact does not allocate another revision. This is projection idempotency, not a general mutation-deduplication service: an old request replayed after an intervening change cannot be identified solely from equal final values. The business writer remains responsible for its stable mutation identity and authorized retries. No externally supplied account revision is accepted.

Consequently, observing committed head H implies all revisions <= H are committed; another writer for that account cannot publish a later head while an earlier revision is uncommitted. Rollback, backend termination and uniqueness errors must leave source facts, head, directory and revisions all unchanged. A writer exception requires outer rollback; callers must not catch an error and commit only the source mutation. Unrelated accounts do not share a writer lock, apart from source conversations they actually share.

All writer operations require READ COMMITTED, matching `persist_message`. Unsupported isolation, invalid batch ordering and source/projection inconsistencies fail without partial success. Sequence/epoch overflow fails explicitly. Deadlocks, serialization failures and ambiguous connection loss require whole-transaction rollback/retry by the caller; the primitive does not loop indefinitely or claim that a returned row proves commit.

## Read transaction and bootstrap

Each page uses a short read-only REPEATABLE READ transaction, so account head/floor, versions and membership are interpreted at one database snapshot. The API has one request deadline and releases every transaction/connection on success, error or cancellation. This read isolation does not invoke the READ COMMITTED-only persistence function.

Beginning bootstrap captures account epoch and committed fence H, plus the greatest directory key visible in the same snapshot. That terminal key bounds the traversal; an empty directory immediately yields a checkpoint at H. The cursor also carries the last examined directory key.

For each page, seek directory keys after that position and no greater than the terminal key. Inspect at most 200 keys, then for each key with `first_change_seq <= H` use the version index to read the last revision <= H. Apply the revision/time-fence condition after the bounded directory batch: pushing an unbounded filter before LIMIT could scan arbitrarily many future keys. Future keys may be inspected and skipped, but do not appear in the snapshot. A remove is not an active bootstrap item.

Stop at the requested item limit (1..100) or response budget. Advance only through examined/consumed keys; an item withheld because of a page boundary remains eligible on the next request. Empty pages caused by removes or future keys are valid and must advance their position. The returned `hasMore` is conservative: reaching the 200-key bound may require a final empty page, instead of an unbounded look-ahead. A terminal page yields a delta checkpoint at H. The initial request does not materialize or count an account-wide snapshot.

After H, immutable revisions ensure updates/removes cannot alter the historical full state selected for a key. New conversations with first revision > H are excluded and later arrive through delta. No exported PostgreSQL snapshot, open transaction, connection-local cursor or in-memory session is required to resume after process restart.

Current membership remains the authorization floor. On the first bootstrap page, exclude unauthorized candidates and advance the examined key. During continuation, if an active historical item is no longer authorized, fail with `SYNC_CURSOR_EXPIRED`, discard that page and require fresh bootstrap; never return the historical summary. A fresh bootstrap can skip stale unauthorized projection entries, avoiding an endless expiry loop. Properly integrated membership revocation and its remove must commit atomically; this defensive check does not claim all future write paths are integrated. Authorization is evaluated at the page's database snapshot, not as a promise to retract data already returned before revocation.

## Delta and merge contract

`BeginDelta` accepts a valid checkpoint, checks epoch and floor, and captures a new committed upper bound H2. Its continuation freezes H2. Read changes in ascending revision with `after < change_seq <= H2`, fetching at most `limit + 1` (at most 101) rows. Return at most the requested limit and advance only through consumed revisions. The terminal page advances the checkpoint to H2; later commits belong to the next round. No same-conversation compression can hide a remove followed by a recreation.

For an upsert whose current membership is absent, return `SYNC_CURSOR_EXPIRED` without that page, just as for historical bootstrap disclosure. Removes need no current membership but remain confined to the principal's previously recorded visibility. An actual source change without a corresponding projection is outside the complete-service guarantee and must be prevented during subsequent integration.

Retries of an unexpired page reproduce its logical revision range while authorization remains valid. A reference consumer applies an item only if its revision is greater than the locally stored revision for that account/conversation; remove keeps its revision marker. This makes duplicate and out-of-order pages converge and prevents an old upsert resurrecting a newer tombstone. A checkpoint may be advanced only atomically with all page effects. The tests implement an independent reference merge; this ADR does not claim a production SDK merge is delivered.

## Cursor format, time and resource bounds

Cursor v1 has distinct bootstrap-page, delta-page and checkpoint kinds. Use a deterministic length-prefixed binary payload: fixed-width big-endian unsigned integers restricted to signed BIGINT bounds, bounded ASCII IDs, a fixed field order and no optional/trailing fields outside the selected kind. A one-byte version/kind, key ID, principal account, epoch, issuance/expiry, page limit and position/fence fields are authenticated. Bootstrap also binds terminal/last directory keys. No JSON parser or floating-point conversion is required.

Encode the authenticated payload and HMAC-SHA256 tag with canonical unpadded base64url. The key ID is covered by the MAC. Decode rejects malformed/noncanonical encoding, unknown fields/kinds, trailing bytes, invalid numbers, inconsistent ranges or input longer than 2048 bytes before expensive work. Compare MACs with a constant-time primitive; authenticate before trusting account/range claims. Key IDs are bounded to 32 ASCII identifier bytes. Every secret key has at least 256 random bits, comes from explicit trusted configuration and is not generated afresh on server startup. Rotation retains keys needed by outstanding cursors; unknown key IDs or invalid MACs give `SYNC_INVALID_CURSOR` without revealing key state.

A bootstrap or delta page round lasts at most 15 minutes. A terminal checkpoint lasts at most 24 hours from its issuance. Continuation and retries preserve the original round expiry; only a newly completed round issues a new checkpoint. Replaying an older valid token cannot increase that token's own lifetime. Beginning a round from a checkpoint creates a new fixed upper bound and bounded round; it does not alter retained history.

Use server wall time only for validity, never message/revision ordering. Capture time once per request, require nonnegative values and safe arithmetic, reject `now >= expiry` and `now < issued`. Invalid configuration/clock input fails explicitly. This detects a cursor issued in the apparent future, not every possible clock rollback across a restart; operators must maintain synchronized server clocks. Context/SQL deadlines bound execution independently of cursor wall-clock validity.

The fixed response representation counts at most 65,536 UTF-8 bytes, including page metadata and tokens; request page limit defaults to 100 and rejects values outside 1..100. Input limits and SQL candidate limits apply before allocating output; encoding applies the byte bound again. Never skip an oversized single item silently. Cursor generation happens only after successful page validation; a failed operation does not return an advanced position.

Request deadline is at most 5 seconds, SQL statement timeout 3 seconds and lock timeout 1 second. The adapter has a default pool limit of 8 and finite connect/acquire timeouts. The application exposes cancellation. Set the driver maximum protocol-message body length to 1 MiB; exercise that bound and the smaller application response limit independently. This bounds one backend protocol message, not the total transaction result or the number of rows. Production TCP connections require certificate/host verification; isolated container tests may use their reviewed Unix socket configuration. Secrets, service-file input, DSNs, driver messages and backend statement context are never returned or logged.

## Cursor floor and failure semantics

A privileged internal maintenance method may monotonically advance `min_valid_seq` to at most the committed head, incrementing epoch in the same transaction. No-op advancement leaves epoch unchanged. Overflow, regression or advancement beyond head fails. The method acquires only its account row and never subsequently locks conversations. Existing cursors then expire. It does not delete revisions, membership, messages or media. A future physical compactor must have its own reviewed anchor/retention/recovery contract.

| Stable code | Condition and caller consequence |
| --- | --- |
| `SYNC_CURSOR_EXPIRED` | Authenticated cursor expired, epoch/floor invalid, or old page lost authorized visibility; start bounded bootstrap |
| `SYNC_INVALID_CURSOR` | Bad shape/MAC/key/type/range or principal binding; no data or advanced cursor |
| `SYNC_FORBIDDEN` | Invalid principal, unauthorized projection operation or invented remove; no information about another account |
| `SYNC_NOT_READY` | Existing user lacks initialized projection, or nonempty account requested empty initialization |
| `SYNC_LIMIT_EXCEEDED` | Invalid page/output/input bounds or invalid declared batch; no partial result |
| `SYNC_STORAGE_UNAVAILABLE` | Database/configuration/clock/isolation failure or inconsistent stored state; fixed error only, no empty-success fallback |
| `SYNC_SEQUENCE_EXHAUSTED` | Account head/epoch cannot advance within signed BIGINT |

Error values include no SQL, IDs, cursor, message or credential content. Whether a storage failure is safe to retry depends on the outer operation/transaction, including ambiguous commit. Counts, latency, scanned/returned rows and response bytes are observed by operation and fixed error only; account/conversation values are not metric labels.

## Dependencies and provenance

Use Go 1.27.1 and the independently examined candidate: pgx/v5 v5.11.0, pgpassfile v1.0.0, pgservicefile v0.0.0-20240606120523-5a60cdf6a761, puddle/v2 v2.2.2, x/sync v0.21.0 and x/text v0.39.0. Pin the actual module graph/checksums; its graph-only x/mod v0.37.0 and x/tools v0.47.0 are not automatically runtime linkage. Any graph/import change requires renewed examination. The highest runtime module minimum is Go 1.25.0, within the existing toolchain.

Do not adopt pgx's unmodified dependency resolution with x/text v0.29.0. Official [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970) identifies invalid-UTF-8 normalization loops before v0.39.0; static preparation found a pgx SCRAM/precis/norm call path. That is not proof of exploitability under a particular configuration. [pgx v5.11.0](https://github.com/jackc/pgx/releases/tag/v5.11.0) and its MIT license provide the direct upstream source/maintenance record.

Retain the four jackc MIT texts, Go BSD and applicable PATENTS, Unicode License V3 and the original CLDR 32 data copyright/permission notice for generated tables. Unicode data attribution does not imply ICU linkage or endorsement. If graph-only sources are redistributed, also retain their applicable mixed yaml MIT/Apache/NOTICE and inline goyacc/check notices. Product dependency documentation and third-party notices must match the actual build, not copy preparation reports as acceptance.

Actual product acceptance repeats `go mod verify`, the complete module/import inventory and pinned `govulncheck v1.8.0` on real product roots/build targets. Scanner JSON exit 0 may contain findings; examine findings and require SBOM coverage of the actual imports. Test-only scanning that omits the driver from the SBOM is insufficient. A clean probe is not approval of a future product commit, TLS configuration or target platform.

## Migration compatibility and validation

Keep applied 001/002 bytes unchanged. The existing migration generator defaults to the highest contiguous source version and accepts explicit historical targets and a source directory; reuse it without changing `runtime.py` or `migrate.py`. Adapting `infra/db/test.py` is necessary: its exact 11-table expectation, fixed future version 3 and restart count 2 otherwise break when 003 becomes real.

Preserve explicit populated 001 -> 002 validation, then default-head 003 upgrade/replay. Verify ledger versions 1..head and each source SHA, derive future=head+1 from validated local sources, and delete only that injected future row. Extend the exact table set by these three tables, keep all original constraints/codec/index/sequence/repair tests, and retain exact function/public-privilege checks. The default path must continue to test latest head rather than being pinned to 002.

Keep existing 002 fault tests. Additionally inject a failure into an owned copy of 003 at an exactly-one marker, and terminate a real backend before populated 002 -> 003 commit. Assert no 003 tables/ledger remain, all 001/002 data/checksums/permissions are preserved, and retry with unmodified sources succeeds. Cover empty-to-head, repeated/concurrent migrators, edited latest checksum, gaps and unsupported future versions. Recovery adds populated projection/floor/epoch/cursor verification across PostgreSQL crash and logical backup/restore, not merely empty-table preservation.

Required product commands are `make sync-bootstrap`, `sync-delta`, `sync-cursor`, `sync-query-plan`, `sync-recovery`, `sync-migrations`, all four existing `db-schema/db-migrations/db-sequence/db-repair` targets, and `make build check`. `sync-check` aggregates the six sync suites in CI while keeping existing storage, protocol, native and portable checks.

Each sync target invokes `python3 -B infra/db/sync_suite.py <suite>`, compiles the actual tagged Go integration package for the verified Linux container architecture, and runs it inside an owned networkless PostgreSQL 18.6 container with durability settings enabled. `-test.list` must identify exactly the intended nonzero top-level test, then the actual test must pass without skips. Runtime helpers have fixed product-root cwd; the harness must explicitly record build environment/cwd and actual expanded Docker/SQL argv, not invent helper parameters. Recovery must stop a real backend/container and resume with a new process and the saved keyring/cursor.

The suites must demonstrate:

- Bootstrap at 0/1/101/100001 conversations; count/byte bounds; empty remove/future-key pages with forward progress; concurrent insert/update/remove; stable fence and complete subsequent delta.
- Per-account writer barriers preventing late-commit gaps; duplicate facts; rollback/backend death; sequence exhaustion; unrelated-account progress; remove/recreate and reordered-page convergence.
- Identity/token swaps, nonmembership, invented tombstones, revocation during bootstrap/delta, exact expiry/floor/epoch/key boundaries, process restart, and no cursor advancement on errors.
- Real cancellation, bounded pools, deterministic lock-order stress, committed-response loss, and source/projection atomicity. Snapshot reads are not permission to expose network endpoints without authentication.
- `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` with 100001 conversations, 100000 versions of one hot conversation and unrelated-account data after ANALYZE. Verify bounded directory/version/delta candidate paths and selective membership indexes, with default planner settings. Do not scan `im_messages`, aggregate complete revision history, disable sequential scans to force plans, or claim a latency guarantee solely from one machine's elapsed milliseconds.

This proposed ADR fixes the design to be reviewed. It is not a claim that the schema, service, migration or any of these acceptance tests have been implemented or passed.
