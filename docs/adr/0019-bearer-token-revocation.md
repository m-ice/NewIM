# ADR 0019: Idempotent current bearer token revocation

Status: accepted for NIM-SRV-007 implementation; independent product acceptance remains separate.
Date: 2026-09-25
Task: NIM-SRV-007
Owner: server
Reviewers: architect, data, security, qa, reviewer

## Context

ADR 0017 and ADR 0018 expose a read-only `GET /api/v1/session` for a persisted
bearer token when the loopback API process has an explicit auth DSN. A client
can inspect a token but cannot durably revoke the token it presented. Waiting for
expiry can leave a leaked credential usable.

`NIM-SRV-001` remains blocked on credential verification, multi-device limits,
login conflict and kick policy. This decision must not select any of those
policies. It revokes exactly one token: the token submitted in the request. It
does not revoke its session, sibling tokens, devices or connections.

## Decision

Add the exact route:

```text
DELETE /api/v1/session/tokens/current
Authorization: Bearer <raw-token>
```

The route is available only through the existing `NEWIM_AUTH_DSN` plus loopback
`newim-server` composition. Without that configuration the path remains absent
and returns the existing 404. The handler accepts exactly one canonical Bearer
header, rejects request bodies, and returns `204 No Content` without reflecting
any identity or credential data.

The operation is idempotent. For a raw token whose token ID and SHA-256 digest
match persisted data:

- an active token is revoked and returns `AUTH_REVOKE_OK`;
- an already revoked token returns `AUTH_REVOKE_NOOP` without changing its
  original `revoked_at`;
- an expired token or a token whose session is already revoked may still be
  revoked/no-op because the request proves possession of the matching credential
  and does not grant active-session access;
- repeated, concurrent and lost-response retries therefore converge to 204.

Malformed, unknown or tampered credentials return the uniform
`401 AUTH_INVALID_TOKEN` with `WWW-Authenticate: Bearer realm="newim-session"`.
Storage, transaction or ambiguous-commit failures return `503 AUTH_UNAVAILABLE`.
A handler panic returns `500 AUTH_INTERNAL_ERROR`. Response errors do not reveal
whether a token was active, expired, revoked or session-revoked.

## Atomic storage boundary

`server/auth/session.Store` adds a `RevokeBearer` port. The PostgreSQL adapter
first resolves the token row's session, enforces session before token locking,
constructs the existing bounded `TokenSnapshot`, and invokes an application
callback for constant-time digest verification while both rows are locked. The
adapter then updates only `newim.im_auth_tokens.revoked_at` and commits one
transaction.

The application callback never trusts caller-provided user, device or session
identifiers. The service parses only the raw token, derives the token ID and
digest, and receives the persisted binding from the locked snapshot. Production
code must not call `RevokeSession` for this route.

An unknown token, a missing session, a digest mismatch and malformed input fail
closed. A repeated revoke does not overwrite `revoked_at`. No database migration
or new dependency is required; migration 004 already stores the required token
digest, revocation timestamp and lookup keys.

## Route method gate amendment

ADR 0018 and `specs/http/service-foundation.md` previously admitted only
trusted dynamic GET routes. ADR 0019 amends ADR 0018's narrow method contract:

- omitted or nil `APIRoute.AllowedMethods` keeps the existing GET default;
- a non-nil empty list is invalid and fails startup;
- `GET` and `DELETE` are the only accepted configured methods in this task;
- duplicates, `POST`, `HEAD`, `OPTIONS`, `PUT`, `PATCH`, wildcard and unknown
  methods are rejected;
- `Allow` is canonical on single and multiple values. The order is `GET, DELETE`
  and multiple methods use exactly one comma plus one ASCII space.

Route-before-method and method-before-body precedence, exact escaped paths,
GET-only health/ready/metrics, and the existing session GET behavior remain
unchanged. `server/api` continues to avoid auth/storage imports; a trusted
process composer supplies the exact method and handler.

## Observability and security

The service emits one bounded `revoke_bearer` observation per public operation
with only operation, stable outcome/error code, elapsed time and known non-secret
IDs. Responses, logs, metrics and errors must not contain raw tokens, secrets,
digests, DSNs, SQL, driver errors, request headers or user/device/session IDs.

The route remains an internal loopback boundary. It is not complete login,
session-wide logout, device management, kick delivery, refresh, gateway or
public abuse control. TLS, CORS, WAF, public rate limiting and ingress remain
future work under `NIM-SRV-001`/security decisions.

## Consequences

A client can durably revoke the credential it currently presents, and a retry
after response loss is safe. Other tokens in the same session remain valid,
which is required until multi-device policy is decided. Rollback removes the new
handler, route, service/store method and documentation; existing token and
session data remain compatible.

This ADR does not unblock `NIM-SRV-001`, `NIM-SYN-001/002`, `NIM-SEC-001`,
`NIM-WHK-002` or `DEC-001/002/003`.
