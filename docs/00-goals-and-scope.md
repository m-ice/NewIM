# Goals and scope

## V1 commercial-grade baseline

V1 focuses on private 1v1 conversations: account mapping, text/image/audio/gift/system messages, conversation list, server-authoritative unread/read state, recall, local DB, offline sync, push, block, report hook, stranger-first-message policy, basic rate limiting, moderation adapter, WebSocket realtime, HTTP/WS sync, metrics/tracing/logging and backup/restore.

Deferred unless an explicit roadmap decision promotes them: complex multi-device semantics, large groups, super groups/channels, full-text search, RTC, end-to-end encryption and cross-region active-active.

## Reliability definition

The system must converge to explainable state after ACK loss, duplicate packets, reordering, network switches, app kill, process restart, gateway restart, push failure, webhook failure and cursor expiration.

## Product decisions still required

- Exact multi-device policy per platform.
- Blocked-sender UX: visible failure vs apparent success/drop.
- Retention periods for messages/media/audit/report evidence.
- Content-moderation mode per media type: pre-delivery vs post-delivery.
- Which upgraded capabilities require platform authorization.
