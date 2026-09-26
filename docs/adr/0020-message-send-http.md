# ADR 0020: Loopback bearer text message send HTTP boundary

Status: accepted for NIM-SRV-008 implementation; independent product acceptance remains separate.
Date: 2026-09-25
Task: NIM-SRV-008
Owner: server
Reviewers: architect, protocol, data, security, qa, reviewer

## Context

ADR 0009 defines the durable internal send transaction. ADRs 0017 through 0019
define policy-neutral bearer authentication, the loopback composition boundary,
and current-token revocation. A client still cannot submit a text message to the
running process and receive a result only after persistence commits.

This decision adds one internal HTTP adapter over those existing capabilities.
It does not select login, credential, device, kick, gateway, delivery or public
network policy, and it does not make authentication and message persistence one
transaction.

## Decision

Add exactly one route:

```http
POST /api/v1/messages
Authorization: Bearer <raw-token>
Content-Type: application/json
```

The body is a protocol v1 `send` frame whose inner version is `1` and type is
`text`. The route is mounted only when all of the following are true:

- `NEWIM_MESSAGE_HTTP=1`;
- `NEWIM_AUTH_DSN` is present and is a local Unix-socket DSN with one socket and
  no fallback or multi-host list;
- `NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1` explicitly acknowledges local-socket use;
- the API listener is a loopback IP literal.

When the message gate is disabled, the route is absent, `/api/v1/messages`
returns the existing 404 contract, and no message pool is opened. Existing
session/revocation routes and their pool behavior remain unchanged. Invalid
message configuration or a failed required startup check exits before listener
bind with `SERVER_INVALID_MESSAGE_CONFIG`; it never falls back to a
session/revocation-only process. No `NEWIM_MESSAGE_DSN` is introduced.

`cmd/newim-server` opens independent auth and message PostgreSQL pools from the
same DSN. The message pool uses `newim-message-http` and at most eight
connections. `server/api` gains only bounded exact-route method, query and body
policy; it does not import auth, message or storage packages.

## Identity and persistence

The handler reuses the strict bearer header grammar and
`AuthenticateBearer`. A successful authentication resolves the persisted
user/device/session/token binding:

```text
userID       = BearerSession.UserID()
deviceID     = BearerSession.DeviceID()
sessionID    = BearerSession.SessionID()
connectionID = BearerSession.TokenID()
tokenID      = BearerSession.TokenID()
```

This `connectionID` is a token-scoped logical identity. It is not a socket,
gateway registration, connection generation, kick target or multi-device
policy. Request-supplied identity and server metadata are never trusted.

The adapter rejects media and every unsupported frame/type/version before
calling `server/message.Service`. The existing service and storage transaction
remain authoritative for current conversation membership, the permanent
`(senderId, clientMsgId)` idempotency key, server ID/sequence/time generation,
message/latest-pointer/outbox atomicity and commit. Authorization runs before
idempotency lookup on every request, so membership loss returns
`403 SEND_UNAUTHORIZED` even when the key already committed.

An equal retry returns the original ACK without adding rows. Unequal intent
returns `409 SEND_ID_CONFLICT` without mutation. A successful response is
exactly HTTP `200` with one protocol `send_ack` whose status is
`SERVER_PERSISTED`; it is written only after commit. There is no 202, bare ACK,
`send_error` success or commit-before-success behavior. Token revocation after
authentication does not cancel an already in-flight send and does not create a
cross-auth/message linearization claim.

## Transport precedence and errors

The exact route uses this order:

```text
route -> method -> query -> known Content-Length/max body
-> Content-Type/Encoding -> bounded body read -> bearer parse -> authenticate
-> protocol decode/type -> membership/send transaction
```

Only `POST` is accepted; other methods return `405` with `Allow: POST`.
`RawQuery`, `ForceQuery` and fragments are rejected with
`400 HTTP_INVALID_QUERY`. `Content-Type` must be exactly one valid
`application/json`; an optional charset must be `utf-8`. `Content-Encoding`
must be absent, empty or a single `identity`; compressed input is never decoded.

Known `Content-Length` above the positive route limit returns
`413 HTTP_BODY_TOO_LARGE` before media/type checks. Unknown or chunked input with
invalid media headers returns `415 HTTP_UNSUPPORTED_MEDIA_TYPE` before reading;
after valid headers, it is read with a bounded reader and overflow returns the
same 413. `MaxBodyBytes=0` retains the foundation's bodyless contract: a body or
unknown-length transfer returns `400 HTTP_BODY_NOT_ALLOWED`, not 413.

`protocol.DecodeSend` is the only body decoder. `protocol.TooLarge` maps to 413
`HTTP_BODY_TOO_LARGE`. `protocol.TooDeep`, overlong numeric tokens, invalid JSON,
invalid message shape, unsupported version/frame and non-text requests map to
`400 SEND_INVALID_INPUT`. Application errors use the stable
`{"error":{"code":"STABLE_CODE"}}` envelope. The full mapping and redaction
requirements are frozen in `specs/http/message-send.md`.

## Database identity and lifecycle

Both repositories share one `postgresidentity.Guard`. It runs on every new
connection and before business SQL uses a pooled connection. On that connection
it verifies the canonical compact JSON digest:

```json
{"database":"...","oid":0,"postmasterStart":"<UTC RFC3339Nano>","systemIdentifier":"...","timelineId":0}
```

The digest is SHA-256 over those UTF-8 bytes. `timelineId` comes from the current
WAL insertion timeline,
`pg_split_walfile_name(pg_walfile_name(pg_current_wal_insert_lsn()))`, guarded by
`pg_is_in_recovery()=false`; it must not use the potentially lagging
`pg_control_checkpoint()` timeline.

Identity mismatch, recovery/standby state, malformed or unsupported results,
and deterministic configuration or privilege/function failures (including
SQLSTATE `42501` or `42883` for `pg_control_system`,
`pg_postmaster_start_time`, `pg_is_in_recovery`, or the WAL location/name/split
functions) are terminal. The guard rejects subsequent acquisition and
transaction attempts, disables readiness, cancels the request/run context, and
a server-owned coordinator outside the pgx hook closes the message pool then
the auth pool and exits nonzero with `SERVER_INVALID_MESSAGE_CONFIG` within the
10-second shutdown bound. It never rebaselines in the same process. PostgreSQL
restart, clone, restore, replacement or timeline change therefore requires an
explicit `newim-server` restart.

Connection creation/reset/refusal, SQLSTATE class 08, `57P01`, `57P02`,
`57P03`, `57014`, and context cancellation/deadline are transient. They return
the applicable 503 response without setting or replacing the baseline; the same
process can retry after recovery.

All routes are constructed before listener bind. A later startup failure closes
the message pool before the auth pool. SIGTERM/SIGINT first makes readiness
false, then drains and closes both pools within the existing 10-second total
deadline; timeout forces a nonzero exit. Readiness remains listener/process
readiness and does not probe either database.

## Route method amendment

ADR 0019 previously allowed dynamic routes to configure only `GET` and `DELETE`.
ADR 0020 adds only `POST`; canonical order is `GET, POST, DELETE` with one comma
plus one ASCII space in `Allow`. `MaxBodyBytes` may be positive only for a
POST-only route, and `RejectQuery` may be set only for a POST-only route.
GET/DELETE, health, readiness and metrics retain bodyless behavior. The exact
message route is the only current positive-body/query-reject composition.

## Boundary and security

The loopback route is for already-issued bearer tokens and trusted local
clients. It does not implement login, refresh, logout, device policy,
kick/notification, gateway or WebSocket registration, public ingress, TLS,
CORS, WAF, rate limiting, abuse/replay controls, media HTTP, Push, sync/read,
Webhook management, retention, moderation, premium or license behavior. It
does not claim delivery to another user or change protocol/schema versions and
dependencies.

Responses, production logs, metrics and errors must not contain raw tokens,
message bodies, client/conversation/sender IDs from an ACK, SQL, DSNs or driver
errors. Metrics may add only the bounded `route="message_send"` label.

## Consequences

The process can expose the existing durable send path through one explicit,
loopback-only HTTP adapter while preserving authentication, membership and
message transaction boundaries. Rollback removes the route composition and
message adapter without changing wire fields or database schema.

This ADR is not acceptance evidence. NIM-SRV-008 is complete only after the
implemented commit passes the real PostgreSQL/process gates, independent
architecture/protocol/data/security/QA/reviewer checks, and the required
regression set. No such result is claimed by this document.
