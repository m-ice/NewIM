# Internal durable send transaction

Status: implementation contract for NIM-SRV-002.
Protocol impact: none beyond the existing `core/protocol/go` send/ACK types.

## Application API

`server/message` owns `Service`, `Store`, `IDGenerator`, `Clock`, stable
`Code`/`Error` values and the internal `PersistedMessage` result. Its public
operation is:

```go
Send(ctx context.Context, identity session.ConnectionIdentity, request protocol.Send) (protocol.ServerFrame, error)
```

`Send` accepts only a complete trusted `session.ConnectionIdentity`; the
request supplies client intent only. It validates the request through the
existing protocol codec, obtains authorization and persistence from `Store`, and
returns exactly one `protocol.ServerFrame` ACK on success. Errors contain only a
stable code and `RetryDisposition`.

The storage adapter implements `Store.Persist` with the trusted sender principal,
the validated request and a generator callback. The callback is invoked only
inside the adapter's READ COMMITTED transaction after conversation
authorization. It supplies `serverMsgId`, outbox `eventId` and nonnegative
server-time milliseconds.

`server/conversation` defines the trusted `Principal`, `Authorizer`, `Tx` and
`Row` seam. It contains no credential, device, kick, read, mute or transport
policy. The storage adapter supplies the SQL implementation and owns the
transaction.

## Persistence contract

The adapter calls existing `newim.persist_message`; it does not create another
persistence function, trigger or migration. Lock order is conversation row,
membership row, then the persistence primitive's checked conversation work. The
primitive returns the original message row for an equal
`(sender_id, client_msg_id)` retry.

The adapter reconstructs the persisted intent and calls
`core/protocol/go.SameIntent` before commit:

- equal intent: return the original `serverMsgId`, `conversationSeq` and
  `serverTime`;
- unequal intent: return `SEND_ID_CONFLICT` and roll back;
- invalid persisted row: return `SEND_STORAGE_UNAVAILABLE` and roll back.

The adapter commits the outer transaction before returning a result. Any commit
error maps to `SEND_STORAGE_UNAVAILABLE`; no ACK is returned. A retry of the same
identity converges to the committed row if the earlier commit outcome was
ambiguous.

## Error contract

Codes and retry dispositions are frozen in ADR 0009. The service maps stable
adapter errors without wrapping driver diagnostics. Unknown errors map to
`SEND_UNKNOWN` with `StopAutomaticRetry`. Retryable storage/lock errors map to
the protocol temporary code only when a `send_error` frame is needed. No error
string contains message payloads, SQL parameters, tokens, DSNs or database
credentials.

## Testing boundary

`tests/integration/message` builds real PostgreSQL tests. `make message-check`
covers equal retry, unequal intent, ACK-after-commit and exact correlation.
`make message-recovery` covers atomic message/latest/outbox state, rollback,
backend termination, restart, disconnect/ambiguous commit convergence and
restore. `make message-errors` covers permanent versus retryable classification,
including missing conversation, unauthorized membership, sequence exhaustion,
lock/storage failures and unknown errors.

## Non-goals

No HTTP/WebSocket or gateway listener, credentials or login policy, public wire
change, Rust codec change, push, delivery/read receipts, moderation, media,
broadcast fan-out, cross-device sync, client retry state machine or claim that a
message reached another user.
