# ADR 0010: SDK Core outbox and send state machine

Status: accepted product decision, 2026-09-23.

## Context

The SDK needs durable client send recovery without coupling core semantics to the JSON wire
codec, transport, SQLite or a platform runtime. The server-assigned persistence result remains
authoritative; the client may only publish local message identity after validating an exact
persisted ACK and removing the matching pending intent in the same storage transaction.

## Decision

`sdk/core` owns a protocol-neutral, host-driven send coordinator. The platform adapter validates
wire frames and maps them to these domain values:

- `SendIntent` contains the immutable `clientMsgId`, conversation, protocol/schema version,
  message type and original opaque payload bytes.
- `PersistedAck` is produced only after the adapter validates `SERVER_PERSISTED` and carries the
  trusted sender, client identity, conversation, server identity, sequence/time, schema/type and
  original payload bytes.
- `SendFailure` carries the original identity, a validated stable code and the core-owned
  disposition `RetrySameIntent`, `AuthRecovery` or `PermanentFailure`.
- `SendContext` carries the trusted sender, a LocalStore `Fence`, and an independent opaque
  connection generation. The accepted single-account identity model requires the trusted sender
  to equal the store fence account; core never creates, refreshes or owns a connection.
- `OutboxObservation` and all public diagnostics use bounded operation/state/error-class,
  attempt and elapsed buckets. They never contain payloads, account IDs, client IDs, tokens,
  server IDs or server error text.

The pending `payload` column stores a versioned, bounded binary envelope. Version 2 records the
intent, the original enqueue fence and connection generation, the active dispatch fence and
connection generation, state, attempt count, original enqueue time, persisted deadline and last
stable code with explicit length prefixes and a checksum. The original fields remain immutable
for audit; explicit resume may replace only the active fields. Unknown versions, truncation,
non-canonical lengths, invalid state/deadline/code combinations and oversized values fail closed
as `OUTBOX_RECORD_INVALID`; legacy/raw SDK-001 or version-1 payloads are never guessed,
converted or deleted.

States are `Ready`, `InFlight`, `RetryWait`, `AuthRecovery` and `PermanentFailure`. A dispatch
persists `InFlight` with revision CAS before releasing the intent to the transport adapter.
After restart, `InFlight` and `RetryWait` are eligible only when the active store and connection
generations match and the persisted absolute deadline is due. A record whose original store
generation differs from the current store generation is persisted as `AuthRecovery`; a stale
connection generation cannot dispatch or apply a failure classification. Retry
limits are frozen at eight attempts total, 24 hours from original enqueue, 1 second base delay,
factor 2, and a 5 minute maximum delay. Deadline arithmetic is checked; clock rollback never
makes a retry early. Overflow, attempt exhaustion or age exhaustion persists
`PermanentFailure` with `OUTBOX_RETRY_EXHAUSTED` across restart.

A normal reconnect that changes only the connection generation does not auto-rebind a pending
record. The host calls `rebind_connection` with the exact sender/fence and last observed revision.
It accepts only `Ready`, `InFlight` and `RetryWait`, requires the exact current active store
account/instance/generation fence, and updates only the active connection generation. The immutable
intent/client identity, attempt count, original enqueue age/deadline, state and last code remain
unchanged. `PermanentFailure` and `AuthRecovery` are rejected; those states remain on the
existing explicit resume/remove paths. After a successful CAS rebind, normal dispatch can use
the remaining attempts and persisted deadline with the same `clientMsgId` and intent.

Only `RetrySameIntent` enters `RetryWait`. Auth codes and store-generation changes enter
`AuthRecovery` and require an explicit host resume with the current generation. Resume replaces
the active store/connection generation while retaining the original audit fields. Every other
valid code, including unknown codes, enters `PermanentFailure`. Terminal and auth-recovery
states never auto-send. Only an explicit revision-CAS removal may dismiss a terminal/auth-
recovery pending row; removal checks the same trusted sender and account/instance identity.

For a validated ACK, Core requires the same trusted sender and store account/instance identity,
but does not require the current connection generation to equal the generation that produced
the pending record. This keeps an exact persisted ACK reconcilable after reconnect. Core builds
one existing `store::Batch` with `MessageWrite::Insert` and `PendingResolution`, using the last
observed revision. `Committed` is the only success. Any
failure or rollback leaves pending present and the message absent. If storage returns an exact
present `Existing` message, Core may submit a second `PreserveExisting` batch bound to the same
snapshot; a trimmed payload returns `OUTBOX_ACK_INTENT_UNVERIFIED` and keeps pending, while a
different payload/result fails closed. `CommitOutcomeUnknown` never reports success and requires
authoritative reload before another decision.

`PendingMutationStore` is additive and leaves `LocalStore` and exhaustive `Action` unchanged.
It exposes the adapter's exact current store fence so core can reject a stale caller context
before dispatch, failure application, rebind, resume or terminal removal. The SQLite adapter
performs exact three-field, revision-CAS updates/removals without a schema migration or
request-byte change. It also observes the existing `requires_reopen` freeze: after corruption or
an ambiguous commit, pending mutations return `RecoveryRequired` until reopen.

## Consequences

Transport, reconnect, lifecycle, credential refresh, platform storage wrappers, UI/unread
updates and authoritative bootstrap reconciliation remain outside this task. Adding those
adapters must preserve the domain boundary and may not reinterpret pending bytes or ACK
authority in a platform wrapper.
