# Current bearer token revocation

Status: implementation contract for NIM-SRV-007
Task: NIM-SRV-007
API version: `v1`

ADR 0019 amends the loopback dynamic-route method contract in ADR 0018 and
`specs/http/service-foundation.md`: omitted/nil route methods remain GET-only,
and this exact route may additionally be configured for `DELETE`.

## Route

```text
DELETE /api/v1/session/tokens/current
Authorization: Bearer n1_<tokenId>_<secret>
```

The route revokes only the token submitted in the request. It does not revoke
the session, sibling tokens, devices, connections or other users. It is mounted
only when `NEWIM_AUTH_DSN` is explicitly configured and the API listener is a
loopback IP literal. Without that configuration the path is absent and returns
`HTTP_ROUTE_NOT_FOUND`; no auth pool is opened.

## Request

- The escaped path must be exactly `/api/v1/session/tokens/current`.
- The method must be `DELETE`; a different method returns 405 with `Allow: DELETE`.
- A body or chunked transfer is rejected with `HTTP_BODY_NOT_ALLOWED`.
- `Authorization` must appear exactly once and use the strict canonical Bearer
  grammar defined by ADR 0008/spec `auth/session-core.md`.
- No user, device, session or connection identifier is accepted in the body,
  query or headers.

## Success and idempotency

Every successful call returns:

```http
204 No Content
Cache-Control: no-store
X-Content-Type-Options: nosniff
```

The first call for an active matching token returns `AUTH_REVOKE_OK`. Repeated
or concurrent calls for the same token return `AUTH_REVOKE_NOOP` or
`AUTH_REVOKE_OK` and never overwrite an existing `revoked_at`. A valid matching
token that is already expired, revoked or belongs to a revoked session still
returns 204; the request proves possession of the matching digest but does not
grant active-session access.

Only `newim.im_auth_tokens.revoked_at` for the presented token may change.
`newim.im_sessions.revoked_at` and sibling token rows must remain unchanged in
the token-only operation.

## Errors

| Status | Code | Condition |
| --- | --- | --- |
| `400` | `HTTP_BODY_NOT_ALLOWED` | DELETE has a body or chunked transfer |
| `401` | `AUTH_INVALID_TOKEN` | Missing/duplicate/malformed header, unknown, tampered or forbidden proof |
| `404` | `HTTP_ROUTE_NOT_FOUND` | Escaped path is not exact |
| `405` | `HTTP_METHOD_NOT_ALLOWED` | Method is not DELETE; includes `Allow: DELETE` |
| `500` | `AUTH_INTERNAL_ERROR` | Handler panic recovered |
| `503` | `AUTH_UNAVAILABLE` | Storage/transaction failed or commit result was not confirmed |

Every 401 sets `WWW-Authenticate: Bearer realm="newim-session"`. Error
precedence is route, method, body, bearer parsing, storage/revocation.
Application errors use the stable `{"error":{"code":"STABLE_CODE"}}` envelope.

Ambiguous commit is never reported as success: the first response is 503.
Because revocation is monotonic, a retry with the same raw token returns 204
once the committed row is observed (or succeeds if the first attempt did not
commit). Response loss is recoverable for the same reason.

## Security and redaction

- The persisted token row and constant-time SHA-256 digest are the only proof;
  supplied identity fields are never trusted.
- The operation uses session-before-token locking and does not call
  `RevokeSession` in production.
- Responses, logs, metrics and errors never contain raw tokens, secrets,
  digests, DSNs, SQL, driver errors, request headers or user/device/session IDs.
- The route remains loopback-only. It is not a public ingress, TLS, CORS,
  rate-limit, abuse-control, WebSocket/gateway, refresh or complete login
  service.
