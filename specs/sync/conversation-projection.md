# Internal conversation sync projection

Status: implementation contract for NIM-SYN-003
Protocol impact: none; this is an internal Go/PG boundary, not a client wire contract.

## Purpose and boundary

This component gives a trusted internal caller a durable, account-scoped view of
conversation visibility. It exists so bootstrap and delta reads use committed
per-account revisions instead of timestamps, outbox ordering or message-history
scans.

It does not authenticate callers, expose HTTP/WebSocket routes, merge state in an
SDK, import existing accounts or make `persist_message` fan out automatically. A
trusted authentication boundary must construct `Principal`; a request payload
must never be able to replace it. A source write outside the projection writer is
not claimed to be synchronized.

## Application API

The package `server/sync/conversation` owns these operations and must not import a
PostgreSQL driver:

```go
BeginBootstrap(ctx, principal, limit) (Page, error)
ContinueBootstrap(ctx, principal, cursor) (Page, error)
BeginDelta(ctx, principal, checkpoint, limit) (Page, error)
ContinueDelta(ctx, principal, cursor) (Page, error)
```

`Page` contains immutable account revisions and an authenticated continuation or
checkpoint cursor. It includes only `conversationId`, positive `revision`,
`upsert|remove`, and for an upsert the nonnegative conversation sequence plus a
nullable server message ID. It never includes payload bytes, profiles, read
state, mute state, devices or deletion requests.

A reference consumer applies an item only when its revision is greater than the
locally stored revision for that account/conversation. A remove retains that
revision marker. Page effects and cursor advancement are atomic independently of
the transport: an error returns no items, no advanced cursor and no checkpoint.

## Bootstrap

`BeginBootstrap` captures the account epoch, committed head `H`, floor and largest
directory key from one read-only `REPEATABLE READ` snapshot. The current membership
check and every returned upsert are interpreted in that same snapshot.

The directory traversal is keyset-based, after the cursor's last examined key and
at or below the captured terminal key. It examines at most 200 directory keys per
page. Immutable revisions at or below `H` are selected through the account /
conversation / descending-change index. New directory keys with first revision
above `H` do not belong to the bootstrap and arrive later through delta.

Removes are omitted from active bootstrap output but consume examined positions.
Future-key and remove-only pages are valid and must advance. `hasMore` may be
conservative at the 200-key boundary, so a final empty page is allowed. A terminal
page returns a checkpoint at `H`.

When a historical upsert at `H` has no current membership, the reader must load
exactly the latest projection for that same account and conversation in the same
snapshot:

- A later remove makes the old round `SYNC_CURSOR_EXPIRED`.
- A latest upsert or a missing projection returns `SYNC_NOT_READY`.
- A latest remove at or below `H` contradicts the selected upsert and returns
  `SYNC_STORAGE_UNAVAILABLE`.

This prevents a deep revoked key from being retried forever and prevents a
missing projection from being treated as successful self-repair.

## Delta

`BeginDelta` starts a new bounded round at a valid checkpoint. It reads immutable
changes after the checkpoint and at or below the captured fence, ordered by
account change sequence. It reads at most 101 candidate rows to return at most
100 items.

Each historical upsert is checked against current membership in the snapshot. If
membership is absent, the latest account/conversation projection is loaded once.
A later remove invalidates the historical upsert, including a remove inside the
same delta fence. A latest upsert or missing projection is `SYNC_NOT_READY`; a
remove at or below the historical upsert revision is inconsistent storage. A
remove row itself remains enumerable without requiring current membership.

`ContinueDelta` preserves the original fence and expiry. Only a complete round
issues a new checkpoint. An expired cursor or floor/epoch change requires a fresh
bootstrap.

## Cursor v1

Cursor payloads use a deterministic binary field order with fixed-width
big-endian unsigned integers restricted to the signed `BIGINT` domain, bounded
ASCII identifiers and no optional or trailing fields. Kinds distinguish a
bootstrap page, delta page and checkpoint. The authenticated fields include
version, kind, key ID, principal account, epoch, issuance, expiry, page limit,
position and the relevant fence/terminal keys.

The payload and HMAC-SHA256 tag use canonical unpadded base64url. Authentication
happens before account/range claims are trusted. Decode rejects non-canonical
encoding, bad MAC, unknown fields/kinds, trailing bytes, invalid numbers,
inconsistent ranges, identity swaps, expired/future tokens, keys longer than
2048 bytes and values outside signed `BIGINT` before doing page work. MAC
comparison is constant-time.

Page rounds expire after at most 15 minutes. Checkpoints expire after at most 24
hours from issuance. A continuation or retry cannot extend its original expiry.
Starting from a checkpoint creates a fresh bounded round. Keys have at least 256
random bits, come from explicit trusted configuration, survive ordinary process
restart and remain retained while outstanding cursors need them. Unknown key IDs
and invalid MACs return `SYNC_INVALID_CURSOR` without disclosing key state.

## Resource and failure contract

- Page limit defaults to 100 and accepts only `1..100`.
- Encoded pages and cursors are capped at 65,536 UTF-8 bytes and 2,048 bytes
  respectively. An oversized item is an error, never a silent skip.
- One request is capped at five seconds; adapter SQL statement timeout is three
  seconds, lock timeout is one second, and the pool has at most eight connections.
- SQL is parameterized. DSNs, driver errors, cursors, identifiers and secrets are
  not returned or logged. Metrics use fixed operation/error labels and aggregate
  counts only.
- TCP service connections require verified TLS; local Unix sockets are allowed
  only by explicit test configuration.

Stable errors:

| Code | Meaning |
| --- | --- |
| `SYNC_CURSOR_EXPIRED` | Cursor is validly authenticated but outside the current epoch/floor or lost current authorization. Start bootstrap. |
| `SYNC_INVALID_CURSOR` | Shape, MAC, key, kind, range or principal binding is invalid. |
| `SYNC_FORBIDDEN` | Principal or requested projection operation is not authorized. |
| `SYNC_NOT_READY` | Account projection is uninitialized or an initialized source membership lacks its matching latest projection. |
| `SYNC_LIMIT_EXCEEDED` | Input, output, candidate or response bound is invalid or exceeded. |
| `SYNC_STORAGE_UNAVAILABLE` | Database/configuration/clock/isolation failure or inconsistent stored state. |
| `SYNC_SEQUENCE_EXHAUSTED` | Account head or epoch cannot advance within signed `BIGINT`. |

## Writer and schema contract

Migration 003 adds exactly three tables:

- `im_conversation_sync_accounts` holds the account epoch, committed
  `last_change_seq` and `min_valid_seq` floor, with `floor <= head`.
- `im_conversation_sync_keys` is the stable `(user_id, conversation_id)`
  directory and records the first revision.
- `im_conversation_sync_changes` stores an immutable full-state revision at
  `(user_id, change_seq)`, using `upsert` or `remove` and restrictive foreign
  keys.

`InitializeEmptyAccount` takes membership `SHARE` before user/account rows and
only succeeds for a genuinely empty account with no prior projection. Repeating
it is idempotent; a populated or previously initialized account is refused with
`SYNC_NOT_READY`. Migration does not backfill existing accounts.

Every mutation batch runs in a caller-owned fresh `READ COMMITTED` transaction.
`PrepareBatch` first takes `ROW EXCLUSIVE` on `im_conversation_members`, then locks
declared conversation rows and account rows in ascending byte order. All source
mutations happen afterwards. `RecordUpsert` reads the authoritative summary and
verifies membership. `RecordRemoval` verifies absent membership and prior
account visibility. Both append at most one checked `head + 1` revision and update
the head in the same outer transaction; the adapter never commits or emits an ACK.
Equal current facts are idempotent. Rollback, backend death or uniqueness errors
leave source, directory, revisions and head unchanged.

`AdvanceFloor` only takes the account row, monotonically advances
`min_valid_seq <= head`, and increments epoch only on a real advance. It never
deletes projection history or participates in a batch lock order.
