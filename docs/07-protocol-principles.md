# Protocol principles

## Identity

Every persistent message carries `clientMsgId`, `serverMsgId`, `conversationId`, `conversationSeq`, `senderId`, message `type`, schema `version`, payload and authoritative `serverTime`.

## ACK states

`LOCAL_CREATED -> SERVER_ACCEPTED -> SERVER_PERSISTED -> DELIVERED -> READ`, with terminal/repair states such as `FAILED`. Reliable send success is `SERVER_PERSISTED` or later.

## Sync

- Conversation delta: cursor/version + changed conversations + tombstones + next cursor.
- Message delta: afterSeq + bounded page + latestSeq + hasMore.
- Cursor expiration returns `SYNC_CURSOR_EXPIRED`; client rebuilds by Bootstrap instead of infinite retry.
- Unknown fields are ignored. Unknown message types render as unsupported without crashing.

## Persistent vs ephemeral

Typing, recording and presence heartbeat are ephemeral and do not enter the permanent message table. Text/media/system/recall/edit events are persistent according to registry policy.
