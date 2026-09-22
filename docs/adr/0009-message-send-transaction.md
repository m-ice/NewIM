# ADR 0009: Durable send transaction and persisted ACK

Status: accepted design for NIM-SRV-002 implementation; independent implementation and operational acceptance remain separate.
Date: 2026-09-23
Task: NIM-SRV-002

## Context and boundary

ADR 0004 provides `newim.persist_message` under READ COMMITTED. It locks the
conversation, returns the original `(sender_id, client_msg_id)` row when that
identity already exists, otherwise allocates a conversation sequence and inserts
the message, latest pointer and outbox row in one subtransaction. ADR 0006
defines the public send/ACK frames and intent equality. ADR 0008 provides a
trusted `session.ConnectionIdentity`.

This ADR accepts the narrow server-side send transaction needed between those
boundaries. It does not add HTTP/WebSocket, authentication, credential
verification, login/device policy, fan-out, push, delivery/read receipts or an
SDK retry state machine. Codec parsing remains in `core/protocol/go`.

## Trust boundary

`server/message.Service.Send` accepts a `session.ConnectionIdentity` separately
from the request. The identity must contain all five trusted IDs; no request
field can supply `senderId`, `serverMsgId`, `conversationSeq`, `serverTime` or
status. The service uses the identity's user ID as the only sender authority.
Codec validation is not permission and a parsed request is not a persisted ACK.

`server/conversation` owns the trusted authorization seam. Its `Authorizer`
interface runs inside the storage transaction and checks the sender's membership
in the requested conversation. The storage implementation locks the conversation
row before the membership row, then calls `persist_message` in the same
transaction. A missing conversation is permanent
`SEND_CONVERSATION_NOT_FOUND`; absent membership is permanent
`SEND_UNAUTHORIZED`. No read state, mute, device or kick policy is applied.

## Transaction and ACK ordering

The storage adapter begins a fresh bounded READ COMMITTED transaction, runs the
conversation authorizer, invokes the trusted server-ID/time generator callback,
calls `newim.persist_message`, compares persisted intent, and commits. The
service receives a `PersistedMessage` only after `COMMIT` succeeds and only then
constructs and returns a `protocol.ServerFrame{Ack: ...}` with status
`SERVER_PERSISTED`.

A rollback, termination or connection loss before commit returns no success ACK.
A commit error is ambiguous: the adapter returns
`SEND_STORAGE_UNAVAILABLE` with `RetrySameIntent`; a bounded retry using the same
`senderId` and `clientMsgId` returns the original committed identity if the
commit had succeeded. A result before commit is never exposed as an ACK.

The server message ID and event ID are generated through injected trusted
`IDGenerator` implementations, and server time comes from an injected `Clock`.
Generation occurs inside the storage operation after authorization and before
persistence. Invalid generated IDs or time are `SEND_INVALID_INPUT`; generator
failure is `SEND_STORAGE_UNAVAILABLE`. The event ID is stored only in the
transactional outbox and is not part of the ACK.

## Idempotency and unequal intent

The permanent key is `(trusted senderId, clientMsgId)`. On a duplicate,
`persist_message` returns the original row without writing another message or
outbox event. The adapter reconstructs the original intent from the stored
protocol version, schema version, conversation, type and raw payload bytes and
compares it using `core/protocol/go.SameIntent`. Equal intent returns the
original `serverMsgId`, `conversationSeq` and `serverTime`. Unequal intent
returns `SEND_ID_CONFLICT` and rolls back without changing the original row.
Unknown outer/body fields remain ignored by the protocol contract and therefore
do not create a conflict.

## Stable errors and retry dispositions

Internal errors contain only a stable code and a local retry disposition.

| Condition | Internal code | Disposition |
| --- | --- | --- |
| Malformed request or trusted identity, invalid generated value | `SEND_INVALID_INPUT` | StopAutomaticRetry |
| Sender is not a conversation member | `SEND_UNAUTHORIZED` | StopAutomaticRetry |
| Conversation row is missing | `SEND_CONVERSATION_NOT_FOUND` | StopAutomaticRetry |
| Conversation sequence is exhausted | `SEND_SEQUENCE_EXHAUSTED` | StopAutomaticRetry |
| Existing key has unequal intent | `SEND_ID_CONFLICT` | StopAutomaticRetry |
| Lock/deadlock/serialization/statement timeout | `SEND_LOCK_UNAVAILABLE` | RetrySameIntent |
| Connection, commit ambiguity or temporary storage failure | `SEND_STORAGE_UNAVAILABLE` | RetrySameIntent |
| Unknown error not understood by the adapter/service | `SEND_UNKNOWN` | StopAutomaticRetry |

Retryable internal codes map to protocol `SERVER_TEMPORARY_UNAVAILABLE` when a
send error frame is produced. Permanent codes map unchanged. Unknown protocol
codes remain conservative: automatic retry stops unless the caller has separate
evidence. The service never classifies an unknown error as retryable.

## Observability and non-goals

The application may emit bounded operation, code, elapsed-time and aggregate
counts. It never emits message bodies, payload bytes, raw SQL/driver errors,
tokens, DSNs, user identifiers or generated server IDs. The outbox is durable
side-effect intent only; no dispatcher, exactly-once delivery or peer delivery
claim is made.

There is no new migration or persistence function. Existing schema 001-004,
protocol v1 and all prior storage/sync/auth invariants remain the compatibility
baseline.
