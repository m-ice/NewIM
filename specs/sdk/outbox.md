# SDK Core outbox and send state v2

This contract is wire-neutral. The platform adapter decodes and validates protocol frames before
constructing the domain events below. Core contains no JSON parser, transport, SQLite or
platform dependency.

## Domain inputs

`SendIntent` is immutable from enqueue through resolution: protocol version, `clientMsgId`,
conversation, schema version, message type and original opaque payload bytes. Retries must reuse
the same intent and `clientMsgId`.

`PersistedAck` is accepted only after the adapter validates `SERVER_PERSISTED` and supplies the
trusted sender/client/conversation/server identity, nonnegative sequence/time, schema/type and
the original payload. Core rejects any mismatch.

`SendFailure` carries the original client/conversation identity and a validated stable code.
Core classifies `SERVER_TEMPORARY_UNAVAILABLE` as `RetrySameIntent`, `AUTH_REQUIRED` and
`AUTH_TOKEN_EXPIRED` as `AuthRecovery`, and every other valid code as `PermanentFailure`.

`SendContext` supplies the trusted sender, the current LocalStore `Fence`, and a separate opaque
connection generation. In the accepted single-account model, the trusted sender must equal the
fence account. Store generation and connection generation are never interchangeable.

## Envelope

`pending_outbox.payload` is a bounded version-2 binary envelope. It carries the intent, the
original enqueue fence/connection generation, the active dispatch fence/connection generation,
state, attempt count, original enqueue time, persisted deadline and last stable code. The
original fields remain immutable audit data; explicit resume may replace only active fields.
Fields are length-prefixed within the existing pending byte and identifier limits. The envelope
never contains a decoded JSON object or arbitrary ingress frame.

Unknown versions, truncation, checksum failure, impossible state/deadline/attempt/code
combinations and oversized encodings return `OUTBOX_RECORD_INVALID` or
`OUTBOX_RECORD_TOO_LARGE`. In particular, `RetryWait` requires a positive deadline in the
future-relative-to-enqueue range and the correct temporary-retry or explicit-resume code.
Legacy SDK-001 and version-1 payloads fail closed. The SQLite adapter does not parse or convert
them.

## State and retry

- `Ready`: newly enqueued, never dispatched.
- `InFlight`: persisted with CAS before the adapter can send.
- `RetryWait`: persisted after `RetrySameIntent`, with an absolute deadline.
- `AuthRecovery`: persisted after auth failure; no automatic retry.
- `PermanentFailure`: terminal, including `OUTBOX_RETRY_EXHAUSTED`.

Dispatch requires the trusted sender/account identity and matching active store/connection
generations. A store-generation change is persisted as `AuthRecovery`; a stale connection
generation is not sent and does not apply a failure classification. `InFlight`/`RetryWait` are due
only at their persisted deadline. Clock rollback cannot make a
record due early. The constants are eight total attempts, 24 hours from enqueue, 1 second base
delay, factor 2 and 5 minutes maximum individual delay. Overflow or either cap persists
`OUTBOX_RETRY_EXHAUSTED`. Terminal/auth states survive restart and never auto-send.

Connection-generation rebinding is an explicit host recovery action through `rebind_connection`.
It requires the same trusted sender and exact current active store account/instance/generation
fence and a single revision-CAS update. It accepts only `Ready`, `InFlight` and `RetryWait`, and updates only
the active connection generation; intent/client identity, attempts, original enqueue age,
deadline, state and last code are preserved. It rejects `PermanentFailure` and `AuthRecovery`,
which remain on the explicit resume/remove paths. A successful rebind lets normal dispatch use
the remaining attempts and persisted deadline with the same `clientMsgId`/intent. Stale
generations still cannot dispatch, apply a failure, or rebind without the current fence and CAS.

Auth recovery requires an explicit host resume with the current generation. Resume updates the
active store/connection generation while preserving the original audit fields. A terminal or
auth-recovery row can be removed only by explicit revision-CAS `remove_pending`; queued work and
non-terminal records cannot be silently removed. Resume and removal require the same trusted
sender and account/instance identity but do not require the original connection generation.

## ACK resolution

For an exact validated ACK from the same trusted sender and store account/instance, Core submits
one `store::Batch` containing:

- `MessageWrite::Insert` with server identity, sequence/time, schema/type and original payload;
- `PendingResolution` for the same sender/client/server identity;
- the last observed store revision.

The current connection generation may differ from the generation stored with the pending record;
ACK reconciliation is keyed by account/store identity, trusted sender, client identity and exact
intent, not by connection generation. `Committed` is the only success; it removes pending and
publishes local message identity atomically. Every failure/rollback preserves pending and inserts
no message.

If storage returns an exact present `Existing` snapshot, Core submits a second
`PreserveExisting`/`PendingResolution` batch bound to the same snapshot. A trimmed payload
returns `OUTBOX_ACK_INTENT_UNVERIFIED` and keeps pending. A different payload/result returns a
stable conflict and keeps pending. `CommitOutcomeUnknown` requires authoritative reload; Core
must not report success or blindly resubmit.

## Observable errors

Stable codes/state markers include `OUTBOX_RECORD_INVALID`, `OUTBOX_RETRY_EXHAUSTED`,
`STORE_GENERATION_CHANGED`, `AUTH_RECOVERY_RESUMED`, `OUTBOX_ACK_INTENT_UNVERIFIED`,
`OUTBOX_ACK_CORRELATION_MISMATCH`, `OUTBOX_ACK_RESULT_CONFLICT`,
`OUTBOX_FAILURE_CORRELATION_MISMATCH`,
`OUTBOX_GENERATION_MISMATCH`, `OUTBOX_TRANSITION_INVALID` and
`OUTBOX_UNEXPECTED_RESPONSE`. Store errors retain their existing stable `STORE_*` codes.

Diagnostics contain only operation/state/error class and bounded attempt/elapsed buckets. They
never include payload bytes, client IDs, account IDs, tokens, server IDs or message bodies.
