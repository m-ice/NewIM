# ADR 0015: In-process durable webhook delivery

Status: accepted design for NIM-WHK-001. The product implementation is an internal,
policy-neutral delivery core; it does not expose a management API and does not
implement Bot, login, Push, retention, moderation or public webhook routes.

## Context

PRT-003 freezes the signed `webhook-v1` HTTP envelope. The message transaction
already inserts a durable event ID into `im_outbox_events` in the same commit as
the message. Webhook delivery must be asynchronous, recoverable, bounded and
independent from message persistence. The repository topology keeps logical
modules in one deployable process until operational evidence justifies a split.

## Decision

`server/webhook` runs in `server/cmd/newim-server` only when an explicit database
DSN and 32-byte AES-256-GCM master key are configured. Missing configuration means
the worker is disabled; incomplete or invalid configuration fails startup. The
worker never exposes HTTP admin routes and never changes message/session behavior.

PostgreSQL migration 006 adds:

- `im_webhook_endpoints` and append-only `im_webhook_endpoint_revisions` for trusted
  internal destination URL, key ID, and encrypted secret material;
- `im_outbox_events.webhook_fanout_at`, a Webhook-only fan-out marker and index;
- fenced delivery state (`pending`, `retry`, `leased`, `delivered`, `dead_letter`,
  `cancelled`) with attempts, lease owner/token/expiry, next-attempt time, HTTP
  status, stable error code and timestamps.

Pre-006 outbox rows are cut over as already fanned out so an upgrade cannot replay
history. Existing delivery rows remain as non-claimable dead-letter legacy records.
Fan-out and `webhook_fanout_at` commit in one transaction; a crash before commit
leaves the event pending, and a crash after commit is idempotent by the
`(event_id,destination_id)` unique key. Delivery is at-least-once and unordered;
consumers must use `deliveryId`/`eventId` for business idempotency.

A worker claims due deliveries with `FOR UPDATE SKIP LOCKED`, a random fencing
token and a lease shorter than the maximum request deadline bound. The attempt
counter is incremented only after a fenced `BeginAttempt`; `Finish` requires the
same live token. Expired leases are requeued without consuming an HTTP attempt.
Retry is bounded exponential backoff with deterministic identity-derived jitter.
Transient network/408/429/5xx are retried; other 4xx and redirects are terminal;
the maximum attempt budget produces dead-letter state. Revoked endpoints cancel
pending/retry work and cannot be claimed; history is retained.

The outbound client resolves all addresses once, rejects unsafe/reserved ranges,
pins an approved IP into the dialer, preserves the original hostname/SNI/TLS
verification, ignores proxy environment, disables redirects, and bounds timeout
and response bytes. Production permits HTTPS only. Test policies that allow
loopback/HTTP are explicit constructor input, never a production environment
switch. Secrets are decrypted only through a `SecretResolver`; AES-GCM AAD binds
destination ID, revision and key ID. Errors and observations carry only stable
codes, never URLs, secrets, signatures, payloads or response bodies.

Global and per-destination pending counts gate new fan-out with high/low-water
thresholds. Backlog pressure pauses new fan-out without deleting outbox rows or
delivery history; no transient secret or storage failure is reported as success.
The current deployment contract is one Webhook worker process per database. Its
global/per-destination concurrency and token-bucket limits are hard within that
process; a later HA task must provide a shared limiter before running multiple
delivery replicas.
SIGTERM stops new claims, cancels/waits for bounded in-flight work and leaves
lease-recoverable state. Runtime storage errors use bounded idle retry.

## Compatibility and operational boundary

Migration 006 is additive and preserves all 001-005 rows. It does not introduce
retention/deletion, endpoint management API, public route, Push, Bot, login,
multi-device, blocked-sender or license behavior. A future process split, shared
replay cache, management API or retention policy requires a separate ADR/task.

## Verification

`make webhook-protocol`, `make webhook-recovery`, `make webhook-security`,
`make webhook-redaction` and `make build check` are required. The recovery and
security suites run real PostgreSQL in the owned container and real local HTTP
receivers where applicable; unit tests alone are not acceptance evidence.
