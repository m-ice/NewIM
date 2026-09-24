# Bearer session HTTP handler

Status: implementation contract for NIM-SRV-005
Task: NIM-SRV-005
API version: `v1`

## Route

The handler handles the exact escaped path `/api/v1/session` and only accepts
`GET`. `newim-server` may mount it only when `NEWIM_AUTH_DSN` is explicitly
configured and the API listener is a loopback IP literal. Without that DSN the
route remains absent and the process opens no auth database pool. The handler
itself still does not create a listener.

## Request

```http
GET /api/v1/session HTTP/1.1
Authorization: Bearer n1_<tokenId>_<secret>
```

The `Authorization` header must appear exactly once. The scheme is
case-insensitive `Bearer`; the token must be non-empty, contain no surrounding
whitespace or comma/control separators, and be accepted by the SRV-003 token
grammar. No user, device, session or connection identifier is accepted from the
request.

## Success

```http
200 OK
Content-Type: application/json; charset=utf-8
Cache-Control: no-store
X-Content-Type-Options: nosniff
```

```json
{"version":1,"session":{"userId":"...","deviceId":"...","sessionId":"...","tokenId":"...","expiresAt":"2026-09-25T00:00:00Z"}}
```

`expiresAt` is the exact persisted expiry in UTC RFC3339Nano form. The token ID
is non-secret; the raw token is never echoed.

## Errors

All application responses use:

```json
{"error":{"code":"STABLE_CODE"}}
```

| Status | Code | Condition |
| --- | --- | --- |
| `400` | `HTTP_BODY_NOT_ALLOWED` | GET request has body or chunked transfer |
| `401` | `AUTH_INVALID_TOKEN` | Missing/duplicate/malformed header, unknown, tampered, expired, revoked or binding error |
| `404` | `HTTP_ROUTE_NOT_FOUND` | Escaped path is not exactly `/api/v1/session` |
| `405` | `HTTP_METHOD_NOT_ALLOWED` | Method is not GET; includes `Allow: GET` |
| `500` | `AUTH_INTERNAL_ERROR` | Handler panic recovered |
| `503` | `AUTH_UNAVAILABLE` | Storage/runtime dependency unavailable or failed closed |

Every 401 response sets:

```text
WWW-Authenticate: Bearer realm="newim-session"
```

Error precedence is route, method, body, bearer parsing, authentication.
`net/http` may reject protocol-level malformed headers before the handler; the
JSON contract applies only after a request reaches the handler.

## Security limits

- The persisted token/session lock snapshot is the only identity source.
- Unknown/tampered/expired/revoked/binding errors are indistinguishable in the
  HTTP response.
- Responses and logs must not contain raw tokens, secrets, digests, DSNs, SQL,
  request headers or panic values.
- The handler is not a public route and does not provide TLS, rate limiting,
  abuse controls, replay protection, WAF, metrics or gateway ownership.
- The handler has no credential-verification or login-policy behavior.
