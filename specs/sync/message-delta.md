# Internal message delta contract

Task: NIM-SYN-005

This document defines an internal Go contract. It is not a public wire format
and does not provide authentication or transport.

## Request

```text
ReadRequest {
  ConversationID string
  HasAfterSeq    bool
  AfterSeq       int64
  Limit          int
}
```

`ConversationID` uses the existing `newim.identifier` grammar
`^[A-Za-z0-9_-]{1,128}$`. `HasAfterSeq=false` requires `AfterSeq=0`.
`HasAfterSeq=true` accepts the signed BIGINT range; negative values are
`SYNC_INVALID_CURSOR`. `Limit=0` means 100; accepted values are `1..100`.

## Result

```text
Item {
  ServerMsgID, ClientMsgID, SenderID, ConversationID string
  ConversationSeq int64
  ProtocolVersion, SchemaVersion uint32
  MessageType string
  ServerTime int64
  Payload []byte
}

Page {
  Items        []Item
  LatestSeq    int64
  HasNext      bool
  NextAfterSeq *int64
}
```

`LatestSeq` is the same-snapshot conversation head. `NextAfterSeq` is nil iff
`Items` is empty; otherwise it is the last returned `ConversationSeq`.
`HasNext` is true iff the same snapshot contains a candidate row not included
in the page because of the limit or byte budget. An empty page never advances a
checkpoint.

Rows are ordered only by `ConversationSeq`. For a positive continuation,
`AfterSeq` must exist as an anchor. The first returned row must be
`AfterSeq+1`, and every following row must increment by one. The explicit start
form does not require a sequence-zero row; the supported writer starts at 1.

## Snapshot authorization

The adapter uses `REPEATABLE READ READ ONLY`, verifies both settings, and reads
membership, head, latest pointer, bounded probes, and page rows in one snapshot.
Unknown and non-member conversations return `SYNC_FORBIDDEN`. Authorization is
linearized at the first database read. A revoke after snapshot acquisition does
not retroactively fail that read.

## Stable errors

| Code | Condition |
| --- | --- |
| `SYNC_FORBIDDEN` | malformed structural identity/conversation or invisible conversation |
| `SYNC_INVALID_CURSOR` | presence/range conflict, negative cursor, missing positive anchor, or cursor beyond head |
| `SYNC_LIMIT_EXCEEDED` | limit outside 0/default or 1..100, or the page budget cannot be assembled |
| `SYNC_STORAGE_UNAVAILABLE` | DB/transaction/mode failure, sequence-domain/head/pointer inconsistency, or unknown storage failure |

Errors are sealed, contain no wrapped pgx/SQL/DSN detail, and return an empty
Page. Observations contain only fixed operation/code labels, counts, byte
counts, and elapsed time; no identifiers, payloads, tokens, DSNs, or SQL.
