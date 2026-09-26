# Server implementation

`server/api` implements the loopback-only HTTP API/Ops foundation: `GET
/api/v1/health` on the API listener and `GET /ready` plus `GET /metrics` on the
ops listener. It binds both listeners before readiness, redacts request logs,
and drains on SIGINT/SIGTERM. See
[ADR 0013](../docs/adr/0013-http-api-service-foundation.md) and the
[service specification](../specs/http/service-foundation.md).

`buildinfo` reads embedded Go build metadata without repository/network access.
`cmd/newim-buildinfo` prints it as JSON for artifact diagnostics. Unknown source
fields remain absent; invalid metadata produces a stable error code.

The server tree now contains application and PostgreSQL storage packages for
messages, auth/session, conversation/message sync, and internal media metadata.
For example, `server/message` validates persistence intent and
`server/storage/message` implements the PostgreSQL message/latest-pointer/outbox
transaction; auth, sync, and media services follow the same application/storage
split. These are internal packages and require a trusted caller/identity.

`server/auth/bearerhttp` provides the `GET /api/v1/session` handler for
already-issued bearer tokens. `newim-server` mounts it only when
`NEWIM_AUTH_DSN` is explicitly configured and the API listener is a loopback IP
literal; without that setting the route stays absent. It resolves the persisted
user/device/session/token binding without trusting request-supplied identity,
uses strict route/method/body/header checks, returns stable redacted errors and
does not provide login, device/kick policy, a public listener, or rate limiting.
See [ADR 0017](../docs/adr/0017-bearer-session-http.md),
[ADR 0018](../docs/adr/0018-bearer-session-route-mount.md) and the
[bearer handler specification](../specs/http/bearer-session.md). The same
loopback runtime also mounts `DELETE /api/v1/session/tokens/current` for current
token revocation: it revokes only the presented token idempotently and never
performs session-wide logout. See [ADR 0019](../docs/adr/0019-bearer-token-revocation.md) and the
[token revocation specification](../specs/http/session-logout.md).

The NIM-SRV-008 slice defines `server/messagehttp` as the exact loopback
`POST /api/v1/messages` adapter for already-issued bearer tokens and protocol
text v1 requests. It is enabled only by `NEWIM_MESSAGE_HTTP=1` with the same
single local-socket `NEWIM_AUTH_DSN`, explicit local-socket opt-in, and a
loopback API listener. It derives the token-scoped logical
`session.ConnectionIdentity`, delegates current membership authorization and
persistence to `server/message`/`server/storage/message`, and returns the framed
`SERVER_PERSISTED` ACK only after commit. Invalid message configuration fails
before bind with `SERVER_INVALID_MESSAGE_CONFIG`; no message pool is opened when
the gate is disabled. See [ADR 0020](../docs/adr/0020-message-send-http.md) and
the [message-send specification](../specs/http/message-send.md). Independent
NIM-SRV-008 product acceptance remains separate.

`server/webhook` and `server/storage/webhook` add an internal at-least-once
webhook delivery core over the existing message outbox. The worker is optional
inside `cmd/newim-server`, is enabled only by explicit DSN/master-key
configuration, uses the PRT-003 `webhook-v1` signature contract, and has no
public endpoint-management API. Migration 006 adds endpoint revisions and fenced
delivery state. See [ADR 0015](../docs/adr/0015-webhook-delivery.md) and the
[delivery specification](../specs/webhook/delivery.md).

There is no public network server or UI, WebSocket gateway, push delivery, or
complete multi-device login/reconnect policy. The API/Ops foundation defaults
to loopback and does not expose message persistence through a public route. The
NIM-SRV-008 send route does not implement login, device/kick, rate limiting,
media HTTP, Push, sync/read, Webhook management or delivery. Run
the root `make build`, `make check`, `make docs-check`, `make api-check`,
`make api-recovery`, `make api-security`, `make auth-http-check`,
`make auth-http-security`, `make auth-route-check`, `make auth-logout-check`,
`make auth-logout-security`, `make webhook-protocol`,
`make webhook-recovery`,
`make webhook-security`, and `make webhook-redaction`
commands. See
[architecture](../docs/architecture.md) and
[third-party notices](../THIRD_PARTY_NOTICES.md).
