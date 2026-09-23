# ADR 0012: Internal bounded message-delta read

Status: accepted design for NIM-SYN-005 implementation after control-plane registration NIM-CTL-016.
Date: 2026-09-23
Task: NIM-SYN-005

## Context

`im_messages` is already the durable message log. `im_conversations.last_seq`
is the committed per-conversation head, and `im_conversation_members` is the
authorization source. `NIM-SYN-003` already provides account-level conversation
discovery and cursor semantics; it does not return message payloads. The full
`NIM-SYN-002` task remains blocked by public wire and login-dependent work.

This ADR adds only the policy-neutral server read primitive needed by a later
transport layer. It does not add a second message projection, public wire,
HTTP/WebSocket, login, device policy, read/unread, retention, block policy,
push, send behavior, or a client gap state machine.

## Decision

Introduce a Go application package `server/sync/message` and a PostgreSQL
adapter `server/storage/messagesync`. The application package must not import
`pgx` or any storage driver. The adapter implements the application `Store`
port.

The public-in-package operation is:

```text
ReadAfterSeq(ctx, trusted ConnectionIdentity, ReadRequest) (Page, error)
```

A trusted authentication boundary supplies the `ConnectionIdentity`. The
primitive validates structural completeness and membership, but it does not
prove token/session authenticity or revocation freshness. Those remain upstream
authentication responsibilities.

The request contains `ConversationID`, explicit `HasAfterSeq`, signed `AfterSeq`,
and `Limit`. The response contains immutable message items, the same-snapshot
`LatestSeq`, `HasNext`, and nullable `NextAfterSeq`.

## Validation order

1. No database access: validate complete ConnectionIdentity structure.
2. No database access: validate `conversationId` against the existing
   `newim.identifier` grammar.
3. Start `REPEATABLE READ READ ONLY` and assert both transaction settings.
4. Read current membership, conversation head, and latest pointer in the same
   snapshot. Unknown and non-member conversations are `SYNC_FORBIDDEN`.
5. Validate sequence-domain and latest-pointer invariants. Violations are
   `SYNC_STORAGE_UNAVAILABLE`.
6. For `AfterSeq > 0`, point-query the anchor row. Missing anchor is
   `SYNC_INVALID_CURSOR`.
7. Validate cursor range and then `limit`.
8. Read at most `limit + 1` rows through the existing
   `(conversation_id, conversation_seq)` index.

A database or transaction-guard failure after structural validation returns
`SYNC_STORAGE_UNAVAILABLE` and takes precedence over membership, cursor, and
limit errors in that transaction.

## Transaction and consistency

The adapter uses a fresh read-only snapshot per request. No transaction or
connection escapes the call. Authorization is linearized at the snapshot's
first database read: a revoke committed before snapshot acquisition is visible;
a revoke committed after snapshot acquisition does not retroactively invalidate
that read. The service does not claim revoke-wins semantics for in-flight reads.

The supported writer starts a conversation at sequence 1 and updates
`last_seq`/`latest_server_msg_id` atomically. The reader therefore requires:

- empty conversation: `last_seq = 0`, null latest pointer, no messages;
- nonempty conversation: sequence 1 is the minimum, `last_seq` is the maximum,
  and the latest pointer references the message at `last_seq`;
- no sequence 0 or sequence above `last_seq`;
- page rows start at `afterSeq + 1` and are strictly contiguous;
- a requested positive `afterSeq` must exist as an anchor, unless the request is
  the explicit start form `HasAfterSeq=false`.

The reader uses bounded first/last/head/anchor probes. It detects gaps along the
bounded page path and at page boundaries; it does not scan the full history to
prove a global no-gap property.

## Limits and errors

`Limit=0` means `100`; otherwise the accepted range is `1..100`. The page budget
is 262144 bytes, computed as `64 + sum(itemCost)`, where item cost is payload
length plus identifier/type lengths plus 128 bytes. At least one legal item fits
under the current schema, so budget truncation is multi-item only.

Stable codes are `SYNC_FORBIDDEN`, `SYNC_INVALID_CURSOR`,
`SYNC_LIMIT_EXCEEDED`, and `SYNC_STORAGE_UNAVAILABLE`. Errors are sealed to a
Code and never wrap database, SQL, DSN, or timeout details. Every error returns
a zero Page and cannot advance a caller checkpoint.

## Consequences

This provides a real, recoverable server read path for later SDK/network
integration without changing existing schema, protocol, send ACK, or outbox
behavior. The later `NIM-SYN-002`/`NIM-SYN-001` work remains responsible for
public wire, login/auth integration, client gap jobs, reconnection, and
end-to-end acceptance.
