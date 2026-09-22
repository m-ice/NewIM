# ADR 0008: Policy-neutral auth session/token core

Status: accepted design for NIM-SRV-003 implementation; independent implementation and security acceptance remain separate.
Date: 2026-09-23
Task: NIM-SRV-003

## Context and trust boundary

NewIM already has users, devices and sessions in migration 001. Those rows do not
define an access token, a constant-time verification path or a durable revocation
primitive. `NIM-SRV-001` remains manually blocked on credential verification,
multi-device/login policy, kick delivery and the public network boundary.

This ADR accepts only the internal substrate needed by later server work. A
trusted server component supplies `user_id`, `device_id` and `session_id` after
already performing credential and product-policy decisions. No request payload
may replace those values. This task does not verify passwords, OTPs, passkeys,
QR assertions or external identity claims.

The application package is `server/auth/session`. It depends on explicit
`Store`, `Clock`, `Observer` and `LoginPolicy` interfaces and does not import a
PostgreSQL driver, transport package or client storage. `server/storage/authsession`
is the PostgreSQL adapter. Migration 004 is additive. There is no HTTP/WebSocket,
gateway registration, push notification, refresh token, credential encryption or
public protocol change.

## Token format and entropy

An access token is the exact ASCII value:

```text
n1_<32-lowercase-hex-tokenId>_<43-character-unpadded-base64url-secret>
```

The fixed total length is 79 bytes. `tokenId` is 16 bytes (128 random bits) read
from `crypto/rand` and encoded as 32 lowercase hexadecimal characters. The secret
is 32 bytes (256 random bits) read from `crypto/rand` and encoded as canonical
unpadded base64url. Base64url `=` padding, uppercase hexadecimal, alternate
prefixes, whitespace and non-canonical encodings are malformed.

The SHA-256 digest of the complete raw token bytes is stored in a 32-byte
`bytea`. The raw token is returned to the caller only after the insert transaction
commits. The raw token, secret, digest, DSN and database credentials must never
appear in errors, structured observations, traces or production logs.
`token_id` is a 128-bit lookup identifier, not a secret, and may be included in a
bounded observation when it was successfully parsed.

Evidence before issue returns: the row is durable and the returned value is not
written back to storage. A commit error, connection loss or ambiguous commit
returns `AUTH_STORAGE_UNAVAILABLE`; no successful token is returned.

## Schema 004

Migration 004 adds exactly one table without rewriting or deleting 001-003 data:

| Column | Constraint |
| --- | --- |
| `token_id text COLLATE "C"` | primary key, exactly 128 lowercase hex characters |
| `token_digest bytea` | not null, exactly 32 bytes, unique |
| `session_id newim.identifier` | not null, restrictive FK to `im_sessions` |
| `created_at timestamptz` | not null, service-supplied issue instant |
| `expires_at timestamptz` | not null, strictly after `created_at` |
| `revoked_at timestamptz` | nullable, at or after `created_at` when present |

A lookup index on `(session_id, token_id)` supports session revocation. Token
rows are not deleted by this task. Unique constraints mean collision retries must
use a new token; no `ON CONFLICT DO UPDATE` or overwrite is permitted.

## Application API

The service exposes a session binding and these operations:

- `Issue(ctx, IssueRequest)`: trusted `(user_id, device_id, session_id)` plus a
  TTL from exactly 1 second through 24 hours. It validates the session ownership
  before checking session revocation, generates the token only after those
  checks, inserts and commits. At most three token-collision attempts are made.
- `Authenticate(ctx, AuthenticateRequest)`: raw token plus trusted expected
  `(user_id, device_id, session_id, connection_id)`. It parses `token_id`, loads
  exactly that row, compares the SHA-256 digest with `crypto/subtle` constant time
  and only then checks ownership, token revocation, session revocation and exact
  expiry.
- `RevokeSession(ctx, SessionBinding)`: locks and revokes the session, then marks
  every active token for that session revoked in one commit.
- `RevokeToken(ctx, userID, tokenID)`: locks the owning session and token, checks
  ownership and marks the token revoked in one commit.
- `PlanLogin(ctx, LoginRequest)`: requires an injected `LoginPolicy`, validates
  the policy's `KickTarget`s and ownership, and returns a plan without issuing a
  token or performing a kick. With no policy it fails closed with
  `AUTH_POLICY_REQUIRED`.

`ConnectionIdentity` contains all five trusted values: user, device, session,
connection and token IDs. Its constructor rejects any missing or invalid value;
callers cannot obtain a usable identity from a partial row.

## Stable errors and precedence

`Error` contains only a stable `Code`. The service redacts unknown storage and
policy errors to stable codes; it never wraps driver errors or secrets.

| Condition | Stable code |
| --- | --- |
| Malformed API input, invalid identifier/length, nil context, non-positive/overflow TTL | `AUTH_INVALID_INPUT` |
| Raw token syntax, length or encoding invalid | `AUTH_TOKEN_MALFORMED` |
| TokenId/digest not found, or a token references a missing session | `AUTH_TOKEN_UNKNOWN` |
| Token is known but `now >= expiresAt` | `AUTH_TOKEN_EXPIRED` |
| Token row is revoked | `AUTH_TOKEN_REVOKED` |
| Token is active but its session is revoked | `AUTH_SESSION_REVOKED` |
| Direct session lookup or existing-session revoke miss | `AUTH_SESSION_NOT_FOUND` |
| Token revoke target does not exist | `AUTH_TOKEN_UNKNOWN` |
| Wrong user/device/session binding, or existing target owned by another user | `AUTH_FORBIDDEN` |
| Storage unavailable, transaction rollback, disconnect or ambiguous commit | `AUTH_STORAGE_UNAVAILABLE` |
| Login policy is not configured | `AUTH_POLICY_REQUIRED` |
| Login policy returns an invalid or unsupported target | `AUTH_POLICY_INVALID` |
| Observer is not configured | `AUTH_OBSERVER_REQUIRED` |
| Clock is not configured | `AUTH_CLOCK_REQUIRED` |
| CSPRNG failure during issue | `AUTH_ENTROPY_UNAVAILABLE` |
| Collision retries exhausted | `AUTH_TOKEN_COLLISION` |

Construction evaluates clock before observer. Authentication evaluates
malformed input, malformed token, storage failure, unknown token/missing session,
binding mismatch, revoked token, revoked session, exact expiry, then success.
Issue evaluates malformed input, session lookup/storage, missing session,
binding/ownership, revoked session, CSPRNG, insert/commit, then collision
exhaustion. Revoke evaluates malformed input, target lookup/storage, absent
target, ownership, then success/no-op. Login policy evaluates policy-required,
then policy-invalid and ownership constraints.

An existing same-owner active revoke target succeeds with outcome
`AUTH_REVOKE_OK`. An existing same-owner already-revoked target succeeds with
outcome `AUTH_REVOKE_NOOP`. An absent session target returns
`AUTH_SESSION_NOT_FOUND`; an absent token target returns `AUTH_TOKEN_UNKNOWN`.
Cross-account targets never mutate another account's row.

## Transaction and locking semantics

All adapter operations use fresh, bounded READ COMMITTED transactions with a
five-second request bound. Session locks are acquired before token locks. Authentication and token revocation lock
the session first and then the token row; session revocation locks the session
and updates its tokens. A committed revoke can therefore never be overtaken by a
later authentication that observed the old snapshot.

Issue locks the session row, verifies the trusted binding, checks
`revoked_at`, then asks the entropy callback for a token. The insert and commit
are in that transaction. If a primary-key or digest unique constraint collides,
the whole attempt rolls back and is retried within the bounded service budget. If
the retry budget is exhausted the service returns `AUTH_TOKEN_COLLISION`.

Authentication locks/reads one consistent session/token snapshot for the
constant-time comparison, binding checks, revocation checks and expiry check.
The transaction commits only after the verification callback succeeds. Any
commit error is fail-closed. Time comparison is `now.Before(expiresAt)`; a token
is invalid exactly at and after its persisted expiry. The injected clock is used
for issuance, revocation timestamps and authentication boundaries. Production
clock synchronization is an operational prerequisite, not provided here.

## Observability

An observer is mandatory for a completed service. Every public issue,
authentication, revocation and storage-failure operation makes at most one
best-effort emission attempt. An event contains only operation, stable
outcome/error code, bounded timing, and known `token_id`/`session_id`. It never
contains a raw token, secret, digest, DSN, credential, user payload or unbounded
database error. Observer panics/failures are contained and do not change the
security result. Observation is not audit durability or exactly-once delivery.

## Non-goals and impact

This decision does not select per-platform or concurrent-device limits, kick
targets, credential verification, public wire format, HTTP/WebSocket/gateway
connection registration, push, refresh tokens, device attestation, rate limits,
cross-device state synchronization, media, messages or moderation. It does not
claim full authentication. A later task must register the missing network and
credential boundary separately.

Migration 004 is additive and keeps populated 001-003 bytes intact. Existing
protocol, storage, sync, native and portable builds remain valid. Rollback of an
uncommitted migration is provided by the existing single migration transaction;
there is no destructive down migration.
