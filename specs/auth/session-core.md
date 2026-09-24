# Internal auth session core

Status: implementation contract for NIM-SRV-003.
Protocol impact: none; this is an internal Go/PostgreSQL boundary.

## Purpose

This component provides opaque access-token issuance, constant-time verification,
durable token/session revocation, a complete connection identity and an injected
login-policy seam. It is not a login endpoint and does not verify user
credentials. The trusted server caller supplies user, device and session IDs.

## Types and methods

Package `server/auth/session` owns the stable codes, `Service`, `Config`, the
`Store`, `Clock`, `Observer` and `LoginPolicy` interfaces, and these public
operations:

```go
Issue(ctx, IssueRequest) (IssuedToken, error)
Authenticate(ctx, AuthenticateRequest) (ConnectionIdentity, error)
RevokeSession(ctx, SessionBinding) (RevocationOutcome, error)
RevokeToken(ctx, userID, tokenID string) (RevocationOutcome, error)
PlanLogin(ctx, LoginRequest) (LoginPlan, error)
```

`IssueRequest` contains `SessionBinding{UserID, DeviceID, SessionID}` and a TTL
from 1 second through 24 hours. `AuthenticateRequest` contains the raw token, the
same trusted binding and a non-empty `ConnectionID`.
`ConnectionIdentity` has unexported fields and getters/constructor validation:
all five IDs must be present and valid.

The adapter is `server/storage/authsession`. It implements `session.Store` with
parameterized pgx queries and has no application policy. It does not expose raw
tokens or driver errors. Pool limits, TLS verification, local-socket opt-in and
timeouts follow `server/storage/conversationsync`; each request is limited to five
seconds and issue collision retries are bounded to three attempts.

## Bearer authentication extension

`AuthenticateBearer(ctx, rawToken)` is the transport-facing read operation added by
NIM-SRV-005. It parses the raw token, locks the token/session snapshot, verifies
the digest and revocation/expiry state, and returns:

```go
type BearerSession struct {
    // unexported persisted binding, token ID and expiry
}
```

Its getters expose `UserID`, `DeviceID`, `SessionID`, `TokenID` and `ExpiresAt`.
The caller cannot supply or override those values. This operation does not create
a `ConnectionIdentity`, does not issue or refresh a token, and does not select a
device or kick policy. Failure semantics and observation redaction are the same
as `Authenticate`; the observation operation is `authenticate_bearer`.

## Token contract

Raw form:

```text
n1_<32-lowercase-hex>_<43-unpadded-base64url>
```

`tokenId` is 128 random bits; the secret is 256 random bits. Both come from
`crypto/rand` by default and the reader is injectable only through trusted
configuration for deterministic tests. The digest is SHA-256 over the complete
raw token. Parsing rejects any non-canonical secret encoding. Stored data contains
only `token_id`, 32-byte digest, session reference, issue/expiry and revocation
timestamps.

## Authentication and revocation

The adapter first resolves `token_id` and fails with `AUTH_TOKEN_UNKNOWN` if the
row or its session is absent. It then locks/loads the session and token snapshot
and invokes the application verification callback while the snapshot is held.
The callback performs constant-time digest comparison, binding checks, token
revocation, session revocation and exact expiry checks in that order. The
transaction is committed after verification. Commit failure maps to
`AUTH_STORAGE_UNAVAILABLE`.

Session revocation locks the session row before updating its tokens. Token
revocation resolves the token, locks the owning session first, checks ownership,
then locks the token. A duplicate same-owner revoke returns
`AUTH_REVOKE_NOOP`; an active target returns `AUTH_REVOKE_OK`. Missing and
cross-account targets never mutate state.

## Error and observation contract

Stable codes are exactly those frozen in ADR 0008. Every API method returns only
a code-bearing error; unknown errors are redacted. A healthy observer receives at
most one terminal event per public operation. Events contain operation, code,
elapsed time and known token/session IDs only. Observer panics are recovered and
do not affect the return value. Raw token, secret, digest and DSN are never
observed or logged.

## Login policy seam

`LoginPolicy` is injected, not defaulted. `PlanLogin` first rejects a missing
policy with `AUTH_POLICY_REQUIRED`; then it validates the requested binding and
the policy result. Every `KickTarget` must use valid IDs and the requested user;
the service also checks the target session's user/device through the store. A
policy cannot change the trusted user/device/session IDs or bypass ownership.
This task does not perform the kick and does not select device limits.

## Migration and tests

Migration 004 creates `newim.im_auth_tokens` additively. `infra/db/test.py`
checks the exact catalog, constraints and populated 001-003 preservation through
a fault and terminated transaction. `infra/db/auth_suite.py` builds the tagged
Go integration package and runs it against an owned PostgreSQL 18.6 container.
`make auth-check`, `make auth-recovery` and `make auth-policy` execute real
PostgreSQL paths and retain command/log evidence.

## Non-goals

No password/OTP/passkey verification, HTTP/WebSocket/gateway listener, connection
registry, kick delivery, push, refresh token, public wire/schema change, rate
limiting, device attestation, media/message behavior or cross-device state
synchronization. Completion of this component is not completion of full user
authentication.
