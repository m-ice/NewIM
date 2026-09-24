# ADR 0017: Policy-neutral bearer session HTTP handler

Status: accepted for NIM-SRV-005 implementation; independent product acceptance remains separate.
Date: 2026-09-25
Task: NIM-SRV-005
Owner: server
Reviewers: architect, security, qa, reviewer

## Context

ADR 0008 defines opaque token issuance, authentication and revocation for callers
that already know a trusted `(user_id, device_id, session_id)` binding. A real
bearer request contains only the raw token, so it cannot safely use that
pre-bound API. `NIM-SRV-001` remains manually blocked on credential verification,
device limits, login conflict, kick behavior and public network abuse controls.

A narrower transport boundary can resolve the persisted token row without
choosing any login or device policy. It must not fabricate a connection identity,
mount a public route, or claim complete authentication.

## Decision

`server/auth/session` adds `AuthenticateBearer(ctx, rawToken)` and a read-only
`BearerSession` containing the persisted user, device, session and token IDs plus
the exact expiry. The method reuses ADR 0008 token grammar, SHA-256 digest,
constant-time comparison, session-before-token lock order, revocation and exact
`now < expiresAt` semantics. The raw token is the only caller input; user,
device, session and expiry come from the locked token/session snapshot.

`server/auth/bearerhttp` adds an independently mounted `SessionHandler` with one
route:

```text
GET /api/v1/session
Authorization: Bearer <raw-token>
```

The handler rejects non-exact routes, non-GET methods, request bodies and
anything other than exactly one strict `Authorization` header. It maps token
format, unknown, tampered, expired, revoked and binding failures to a uniform
`401 AUTH_INVALID_TOKEN`; storage/runtime failures map to
`503 AUTH_UNAVAILABLE`; handler panics map to `500 AUTH_INTERNAL_ERROR`. Every
401 sets `WWW-Authenticate: Bearer realm="newim-session"`. The success response
is versioned and contains only non-secret IDs and expiry. Raw tokens, secrets,
digests, DSNs, SQL and request headers never appear in responses or logs.

## Boundary

This task does not mount the handler in `server/api` or `cmd/newim-server`, does
not add a public route, and does not implement credential verification, login,
refresh, logout, multi-device limits, kick/revocation notification, gateway
connection registration, rate limits, abuse/replay controls, WebSocket, message
or sync APIs, Push, retention, moderation, Premium or license behavior. The
handler is suitable only for loopback or an already trusted process until
SRV-001 supplies the public network boundary.

## Consequences

Consumers can inspect an already-issued session without supplying untrusted
identity fields. `NIM-SRV-001`, `NIM-SYN-001/002`, `NIM-SEC-001`, `NIM-WHK-002`
and `DEC-001/002/003` retain their existing manual or dependency blockers. The
handler's `AUTH_INTERNAL_ERROR` and route/metrics ownership are intentionally
separate from the HTTP foundation until a later integration task freezes the
central behavior. Rollback removes only the new session method, handler and
tests; existing token/session data is unchanged.
