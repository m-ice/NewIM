# ADR 0013 — Loopback HTTP API and Ops service foundation

Status: review
Date: 2026-09-24
Task: NIM-SRV-004
Owner: server
Reviewers: sre, security, qa, reviewer

## Context

The product has durable message, auth-session, sync, media, and SDK primitives,
but no process-level HTTP boundary. Public auth, identity, multi-device, and
message APIs depend on product decisions that are not yet available. A
transport-only service skeleton can still establish stable lifecycle, error,
and observability behavior without deciding those semantics.

## Decision

`newim-server` runs two standard-library `net/http` listeners in one process:

- API listener: default `127.0.0.1:8080`, exact `GET /api/v1/health`.
- Ops listener: default `127.0.0.1:9090`, exact `GET /ready` and `GET /metrics`.

The CLI accepts only `--api-addr`, `--ops-addr`, and `--help`. It does not read
addresses from environment variables or configuration files. Both listeners
bind before readiness becomes true. On SIGINT/SIGTERM, readiness becomes false
before both listeners drain concurrently with a 10-second bound.

Application-handled errors use `{"error":{"code":"STABLE_CODE"}}` and the
status/code matrix in `specs/http/service-foundation.md`. Responses are
`no-store` and `nosniff`. Metrics are a standard-library-generated Prometheus
text subset with bounded `listener`, `route`, `method`, and `status` labels.
Logs use `log/slog` and contain only normalized request metadata.

## Boundary

This ADR does not define authentication, identity, sessions, devices, kick or
multi-device policy, blocked-sender behavior, retention, moderation, message or
sync APIs, WebSocket, push, webhook, bot, group, TLS, CORS, public rate limiting,
FeatureGate, license, database connections, or third-party dependencies. The
ops listener is not a public ingress and the API listener is loopback-only by
default. Non-loopback use is an operator decision outside this task and requires
the relevant security work before public exposure.

## Consequences

The service has a testable liveness/readiness boundary and bounded operational
metrics without committing to business protocol semantics. Future auth and
gateway tasks can add versioned routes behind the same lifecycle and redaction
rules. Serving plain HTTP behind a proxy remains the deployment responsibility;
this ADR does not claim production HA, TLS, or abuse resistance.
