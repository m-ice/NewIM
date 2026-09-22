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
  connection generation. Core never creates, refreshes or owns a connection.
- `OutboxObservation` and all public diagnostics use bounded operation/state/error-class,
  attempt and elapsed buckets. They never contain payloads, account IDs, client IDs, tokens,
  server IDs or server error text.

The pending `payload` column stores a versioned, bounded binary envelope. Version 1 records the
intent, fence, connection generation, state, attempt count, original enqueue time, persisted
deadline and last stable code with explicit length prefixes and a checksum. Unknown versions,
truncation, non-canonical lengths, invalid states and oversized values fail closed as
`OUTBOX_RECORD_INVALID`; legacy/raw SDK-001 payloads are never guessed, converted or deleted.

States are `Ready`, `InFlight`, `RetryWait`, `AuthRecovery` and `PermanentFailure`. A dispatch
persists `InFlight` with revision CAS before releasing the intent to the transport adapter.
After restart, `InFlight` and `RetryWait` are eligible only when the persisted store fence and
the separate connection generation match and the persisted absolute deadline is due. Retry
limits are frozen at eight attempts total, 24 hours from original enqueue, 1 second base delay,
factor 2, and a 5 minute maximum delay. Deadline arithmetic is checked; clock rollback never
makes a retry early. Overflow, attempt exhaustion or age exhaustion persists
`PermanentFailure` with `OUTBOX_RETRY_EXHAUSTED` across restart.

Only `RetrySameIntent` enters `RetryWait`. Auth codes enter `AuthRecovery` and require an
explicit host resume with the current generations. Every other valid code, including unknown
codes, enters `PermanentFailure`. Terminal and auth-recovery states never auto-send. Only an
explicit revision-CAS removal may dismiss a terminal/auth-recovery pending row.

For a validated ACK, Core builds one existing `store::Batch` with `MessageWrite::Insert` and
`PendingResolution`, using the last observed revision. `Committed` is the only success. Any
failure or rollback leaves pending present and the message absent. If storage returns an exact
present `Existing` message, Core may submit a second `PreserveExisting` batch bound to the same
snapshot; a trimmed payload returns `OUTBOX_ACK_INTENT_UNVERIFIED` and keeps pending, while a
different payload/result fails closed. `CommitOutcomeUnknown` never reports success and requires
authoritative reload before another decision.

`PendingMutationStore` is additive and leaves `LocalStore` and exhaustive `Action` unchanged.
It performs exact three-field, revision-CAS updates/removals in the SQLite adapter without a
schema migration or request-byte change.

## Consequences

Transport, reconnect, lifecycle, credential refresh, platform storage wrappers, UI/unread
updates and authoritative bootstrap reconciliation remain outside this task. Adding those
adapters must preserve the domain boundary and may not reinterpret pending bytes or ACK
authority in a platform wrapper.
