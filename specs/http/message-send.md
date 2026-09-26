# Loopback bearer text message send

Status: review; implementation contract for NIM-SRV-008. This document does not establish independent product acceptance.
Task: NIM-SRV-008
API version: `v1`

ADR 0020 defines the design decision and amends the dynamic route method
contract. This specification freezes the externally visible HTTP behavior of
the exact loopback text-send adapter.

## Route and mounting

The only route is the exact escaped path `/api/v1/messages` with method `POST`.
Trailing slashes, percent-encoded aliases, query strings, fragments and all
other paths are not aliases.

```http
POST /api/v1/messages
Authorization: Bearer n1_<tokenId>_<secret>
Content-Type: application/json
```

The route is mounted only when `NEWIM_MESSAGE_HTTP=1`, `NEWIM_AUTH_DSN` is a
single local Unix-socket DSN with no fallback/multi-host list,
`NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1` is explicit, and the API listener is a loopback
IP literal. The process opens independent auth and message pools from that same
DSN; there is no `NEWIM_MESSAGE_DSN`.

With the gate disabled, the path is absent, returns `404 HTTP_ROUTE_NOT_FOUND`,
and opens no message pool. Existing session and current-token revocation route
behavior remains unchanged. Missing, empty, non-local-socket, fallback or
otherwise invalid required configuration fails before listener bind with
`SERVER_INVALID_MESSAGE_CONFIG`; it does not fall back to session-only mode.

## Request body

The body must be one strict protocol v1 `send` frame. Only inner version `1` and
type `text` are accepted:

```json
{
  "protocolVersion": 1,
  "kind": "send",
  "body": {
    "clientMsgId": "client_1",
    "conversationId": "conversation_1",
    "version": 1,
    "type": "text",
    "payload": {"text": "hello"}
  }
}
```

`protocol.DecodeSend` is authoritative. It rejects duplicate/escape-equivalent
keys, invalid UTF-8/JSON, trailing JSON, excessive/deep JSON, overlong numeric
tokens, invalid identifiers/version/type and malformed text. A `send` frame must
not contain `senderId`, `serverMsgId`, `conversationSeq`, `serverTime` or
`status`; those fields never influence persistence. Unknown outer fields are
validated by the codec and unknown inner payload fields remain subject to
protocol intent equality, but the HTTP adapter does not invent additional JSON
semantics.

`protocol.TooLarge` for outer metadata, frame or body limits maps to
`413 HTTP_BODY_TOO_LARGE`. `protocol.TooDeep`, overlong numeric tokens, other
invalid JSON/message/version/shape errors and non-text/unsupported types map to
`400 SEND_INVALID_INPUT`.

## Transport policy and precedence

Every request follows this order:

```text
route -> method -> query -> known Content-Length/max body
-> Content-Type/Encoding -> bounded body read -> bearer parse -> authenticate
-> protocol decode/type -> membership/send transaction
```

- The route must be exact. An unmatched path is 404
  `HTTP_ROUTE_NOT_FOUND`.
- A different method on the exact route is 405
  `HTTP_METHOD_NOT_ALLOWED` with `Allow: POST`.
- `RawQuery != ""`, `ForceQuery`, or a fragment is 400
  `HTTP_INVALID_QUERY` before size/media checks.
- Known `Content-Length` greater than the positive route `MaxBodyBytes` is 413
  `HTTP_BODY_TOO_LARGE` before Content-Type/Encoding and decode checks.
- `Content-Type` must be exactly one parseable `application/json` value. Any
  charset parameter, if present, must case-insensitively be `utf-8`. Missing,
  repeated, malformed or other media types are 415
  `HTTP_UNSUPPORTED_MEDIA_TYPE`.
- `Content-Encoding` must be absent, empty, or one case-insensitive `identity`
  value. Repeated or any other encoding is 415
  `HTTP_UNSUPPORTED_MEDIA_TYPE`; input is never decompressed.
- Unknown-length or chunked input first receives the Content-Type/Encoding
  checks. Invalid headers return 415 without reading. Valid headers are read
  with a bounded reader; overflow returns 413.
- `MaxBodyBytes=0` retains the API foundation's bodyless meaning. A non-empty,
  unknown-length or chunked request returns
  `400 HTTP_BODY_NOT_ALLOWED`; it is neither a zero-byte limit nor 413.

The exact message route is composed with a positive `MaxBodyBytes` and
`RejectQuery=true`. These options are valid only for a POST-only route.
`server/api` remains policy-neutral and does not import auth, message or storage.
`net/http` may reject malformed protocol framing before the handler; this
specification applies after a request reaches it.

## Authentication and identity

`Authorization` must appear exactly once and use the strict bearer grammar
defined by ADR 0008 and `specs/http/bearer-session.md`. Missing, repeated,
malformed, unknown, tampered, expired, revoked, session-revoked or binding-error
credentials all return `401 AUTH_INVALID_TOKEN` with
`WWW-Authenticate: Bearer realm="newim-session"`.

Only the persisted bearer/session snapshot supplies identity:

```text
userID       = BearerSession.UserID()
deviceID     = BearerSession.DeviceID()
sessionID    = BearerSession.SessionID()
connectionID = BearerSession.TokenID()
tokenID      = BearerSession.TokenID()
```

`connectionID` is a stable token-scoped logical connection identity, not a
socket, gateway registration or proof of device/kick policy. Requests cannot
provide or override user, device, session, connection or sender identity.

## Authorization, persistence and ACK

The handler passes the validated request and trusted identity to the existing
`server/message.Service`. Authorization is a real membership check inside the
existing `server/storage/message` transaction. It runs before idempotency lookup
on every request. A sender who lost membership returns
`403 SEND_UNAUTHORIZED` even if `(senderId, clientMsgId)` previously committed.

The durable transaction keeps the existing behavior:

- a new message atomically commits message, conversation latest pointer and
  transactional outbox rows;
- an equal intent retry returns the original server message ID, sequence and
  time without adding rows;
- an unequal intent for the same trusted sender/client key returns
  `409 SEND_ID_CONFLICT` without mutation;
- ambiguous commit or storage failure returns `503` and no ACK; a retry after
  commit can return the original ACK only while current membership remains
  authorized.

Success is exactly `200 OK` with one `core/protocol/go.EncodeServerFrame`
`send_ack` body:

```http
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Cache-Control: no-store
X-Content-Type-Options: nosniff
```

```json
{
  "protocolVersion": 1,
  "kind": "send_ack",
  "body": {
    "status": "SERVER_PERSISTED",
    "clientMsgId": "client_1",
    "conversationId": "conversation_1",
    "senderId": "user_1",
    "serverMsgId": "server_1",
    "conversationSeq": "1",
    "serverTime": "1800000000000"
  }
}
```

The ACK is written only after commit. HTTP `202`, bare ACKs, `send_error` as
success and any pre-commit success are forbidden. The ACK proves server
persistence only; it does not claim delivery, read state, push or another
user's receipt.

## Stable errors

All application errors use:

```json
{"error":{"code":"STABLE_CODE"}}
```

| HTTP | Code | Condition |
| --- | --- | --- |
| `400` | `HTTP_BODY_NOT_ALLOWED` | `MaxBodyBytes=0` route receives a non-empty, unknown-length or chunked body |
| `400` | `HTTP_INVALID_QUERY` | Query, `ForceQuery` or fragment on the exact message route |
| `400` | `SEND_INVALID_INPUT` | Invalid protocol JSON/shape/version/type or unacceptable request form |
| `401` | `AUTH_INVALID_TOKEN` | Missing/repeated/malformed credential or unknown/tampered/expired/revoked/binding error |
| `403` | `SEND_UNAUTHORIZED` | Current membership check rejects the sender |
| `404` | `SEND_CONVERSATION_NOT_FOUND` | Conversation does not exist |
| `404` | `HTTP_ROUTE_NOT_FOUND` | Route is absent or path is not exact |
| `405` | `HTTP_METHOD_NOT_ALLOWED` | Method is not POST; includes `Allow: POST` |
| `409` | `SEND_ID_CONFLICT` | Same trusted sender/client key has unequal intent |
| `409` | `SEND_SEQUENCE_EXHAUSTED` | Conversation sequence is exhausted |
| `413` | `HTTP_BODY_TOO_LARGE` | Known body, bounded read, metadata, frame or body exceeds its limit |
| `415` | `HTTP_UNSUPPORTED_MEDIA_TYPE` | Content-Type or Content-Encoding is not allowed |
| `500` | `HTTP_INTERNAL_ERROR` | Handler panic recovered without reflecting its value |
| `500` | `SEND_UNKNOWN` | Unknown internal send result |
| `503` | `AUTH_UNAVAILABLE` | Auth repository/dependency is unavailable or fails closed |
| `503` | `SERVER_TEMPORARY_UNAVAILABLE` | Lock/storage/commit result is uncertain/retryable; may include `Retry-After: 1` |

401 responses set `WWW-Authenticate`. Application responses set
`Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, and JSON
Content-Type. Error bodies never reflect token, payload, SQL, DSN, driver error,
request headers or panic values.

## Database identity guard

The auth and message repositories share one `postgresidentity.Guard`. Each new
connection and each pre-business-SQL acquisition verifies the same logical
PostgreSQL incarnation on that connection. The baseline digest is SHA-256 over
canonical compact UTF-8 JSON with fixed field order:

```json
{"database":"...","oid":0,"postmasterStart":"<UTC RFC3339Nano>","systemIdentifier":"...","timelineId":0}
```

`timelineId` must come from the current WAL insertion timeline,
`pg_split_walfile_name(pg_walfile_name(pg_current_wal_insert_lsn()))`, with
recovery isolated so `pg_is_in_recovery()=false` is required. The guard must not
use the timeline reported by `pg_control_checkpoint()`, which can lag until the
next checkpoint.

Identity mismatch, recovery/standby state, malformed/unsupported results, and
deterministic configuration or privilege/function failures (including SQLSTATE
`42501` or `42883` for `pg_control_system`, `pg_postmaster_start_time`,
`pg_is_in_recovery`, or the WAL location/name/split functions) are permanent
terminal conditions. Terminal state blocks later acquisitions/transactions,
disables readiness, cancels the request/run context, and a coordinator outside
the pgx hook closes the message pool then the auth pool and exits nonzero with
`SERVER_INVALID_MESSAGE_CONFIG` within 10 seconds. There is no automatic
rebaseline. Restart, clone, restore, replacement, postmaster change or timeline
promotion requires an explicit `newim-server` restart before a new baseline can
be established.

Connection creation/reset/refusal, SQLSTATE class `08`, `57P01`, `57P02`,
`57P03`, `57014`, and context cancellation/deadline are transient. They map to
the applicable 503 response, do not set or replace the baseline, and may
succeed after the dependency recovers in the same process.

## Lifecycle, security and non-goals

All routes are constructed before bind. A later failure closes the message pool
before the auth pool. SIGINT/SIGTERM makes readiness false, drains the server,
then closes both independent pools within the existing 10-second total deadline;
timeout forces a nonzero exit. Readiness remains process/listener readiness and
does not probe either database. Database outage remains a route-level 503.

Responses, logs, metrics and errors must not contain raw tokens, payloads,
client/conversation/sender IDs from an ACK, secrets, digests, DSNs, SQL or
driver diagnostics. Metrics may add only the bounded `route="message_send"`
label.

This route does not implement login, refresh, logout, device limits or kick,
gateway/WebSocket registration, public ingress, TLS, CORS, WAF, rate limiting,
abuse/replay controls, media HTTP, Push, sync/read/unread, Webhook endpoint
management, retention, moderation, premium or license behavior. It does not
change protocol wire versions, database schema or dependencies.

## Acceptance status

This document is a review contract, not product acceptance. NIM-SRV-008 may be
concluded only after the implemented revision passes the real PostgreSQL and
real `newim-server` process gates, both HTTP security gates, the gate negative
self-tests, existing message/auth/API/regression checks, and independent
architecture, protocol, data, security, QA and reviewer inspection. No such
result is claimed here.
