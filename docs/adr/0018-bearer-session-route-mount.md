# ADR 0018: Mount the policy-neutral bearer session route in the loopback server

Status: accepted for NIM-SRV-006 implementation; independent product acceptance remains separate.
Date: 2026-09-25
Task: NIM-SRV-006
Owner: server
Reviewers: architect, sre, security, qa, reviewer

## Context

ADR 0017 defines a strict, unmounted `GET /api/v1/session` handler. The running
`newim-server` must be able to expose that handler when an operator explicitly
configures an existing auth-session database. This must not select login,
credential, multi-device, kick, gateway or public-network policy.

## Decision

`server/api` gains a bounded `APIRoute` composition surface. It accepts a
verified name, exact `/api/v1/` path and non-nil `http.Handler`; it does not
import auth or storage packages. Dynamic routes remain GET-only and bodyless,
use exact path matching, and add only the configured bounded route name to the
existing metrics labels.

`server/cmd/newim-server` owns the optional composition:

- `NEWIM_AUTH_DSN` absent: no auth repository, no route, `/api/v1/session`
  returns the existing 404 contract.
- `NEWIM_AUTH_DSN` present: the API listener must be a loopback IP literal.
  DNS names, wildcard addresses, non-loopback addresses and malformed DSNs fail
  before bind with a stable configuration or storage error.
- A Unix socket DSN is allowed only when
  `NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1`.
- Startup order is configuration/DSN validation, repository open, session
  service and handler construction, API route composition, then listener bind.
  A later construction or bind failure closes the acquired repository/listener
  and exits nonzero.
- The route uses the existing `specs/http/bearer-session.md` contract without
  changing token parsing, digest comparison, revocation, expiry or HTTP errors.
- `/ready` remains process-listener readiness and does not probe PostgreSQL.
  A database outage returns `503 AUTH_UNAVAILABLE` for the session route while
  readiness retains its process meaning. The pool must recover after the
  database returns; a transient outage is not cached permanently.
- SIGTERM becomes not-ready, drains the server, and closes the auth repository
  within the existing 10-second total shutdown bound. Timeout forces a nonzero
  process exit.

## Boundary and non-goals

This ADR does not add a public listener, TLS, CORS, rate limiting, WAF, replay
protection, login, password/OTP/passkey verification, refresh/logout,
multi-device policy, kick delivery, gateway registration, message/sync/read
APIs, Webhook management, Push, retention, moderation, Premium or license
behavior. The session route is an introspection/verification surface for
already-issued tokens and is suitable only for loopback or a trusted process
boundary until SRV-001 supplies the public controls.

## Consequences

The process can expose the accepted session handler without coupling
`server/api` to auth/storage. The route is opt-in, bounded and reversible by
removing `NEWIM_AUTH_DSN` or reverting the composition commit; existing token,
session and migration data remain unchanged.
