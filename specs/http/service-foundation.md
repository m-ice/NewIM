# HTTP service foundation

Status: review
Task: NIM-SRV-004
API version: `v1`

This specification defines only the loopback process skeleton. It does not
define authentication, identity, message, sync, media, push, or public API
semantics.

## Listeners and routes

| Listener | Default | Route | Method | Success |
| --- | --- | --- | --- | --- |
| API | `127.0.0.1:8080` | `/api/v1/health` | `GET` | `200` `{"status":"ok"}` |
| Ops | `127.0.0.1:9090` | `/ready` | `GET` | `200` `{"status":"ready"}` |
| Ops | `127.0.0.1:9090` | `/metrics` | `GET` | `200` Prometheus text |

Routes are exact raw paths: trailing slashes, percent-encoded aliases, and the
special `OPTIONS *` target are not accepted as aliases. Health is liveness only;
it does not probe a database. Readiness is true only after both listeners bind
and serving starts, and becomes false before shutdown drain begins. A trusted
process composer may add bounded exact `/api/v1/` routes. Omitted or nil
`AllowedMethods` preserves the GET default; non-nil empty input is invalid.
ADR 0019 amends ADR 0018's GET-only dynamic route decision to allow only
`GET` and `DELETE` for a newly registered exact path. NIM-SRV-006 adds the
GET-only `route="session"` and NIM-SRV-007 adds the DELETE-only
`route="session_token_revoke"` only when an explicit loopback auth DSN is
configured. The foundation itself still does not import auth or storage.

## Errors

Application-handled errors are JSON:

```json
{"error":{"code":"STABLE_CODE"}}
```

| Status | Code | Condition |
| --- | --- | --- |
| `400` | `HTTP_BODY_NOT_ALLOWED` | An allowed method has a body or chunked transfer |
| `404` | `HTTP_ROUTE_NOT_FOUND` | Unknown path, including trailing slash |
| `405` | `HTTP_METHOD_NOT_ALLOWED` | Known path with a method outside its AllowedMethods; includes exact `Allow` |
| `500` | `HTTP_INTERNAL_ERROR` | Recovered handler panic |
| `503` | `SERVER_NOT_READY` | `/ready` while draining |

Error precedence is route, method, body, readiness/handler. Responses set
`Cache-Control: no-store` and `X-Content-Type-Options: nosniff`. The service
does not reflect raw paths, queries, headers, bodies, panic values, SQL, or
transport errors.

## Metrics

`/metrics` is generated from an in-memory registry with these families:

```text
newim_server_info{server_version="..."} 1
newim_process_start_time_seconds <gauge>
newim_http_requests_in_flight{listener,route} <gauge>
newim_http_request_duration_seconds_sum{listener,route,method}
newim_http_request_duration_seconds_count{listener,route,method}
newim_http_requests_total{listener,route,method,status}
```

`listener` is `api|ops`; `route` is `health|ready|metrics|unmatched` plus explicitly registered bounded names (`session`, `session_token_revoke`); `method`
is one of the bounded HTTP method classes; `status` is a three-digit handler
status. Request paths, query strings, headers, client addresses, tokens,
message data, and revisions are never labels.

## Lifecycle and security limits

- Both listeners bind before readiness becomes true.
- Failure to bind the second listener closes the first and exits nonzero.
- Timeouts: read-header 5s, read 10s, write 10s, idle 60s; max header 16 KiB.
- A `GET` body is rejected without reading it.
- Panic recovery returns only `HTTP_INTERNAL_ERROR`.
- SIGINT/SIGTERM causes not-ready then a concurrent 10-second drain. A timeout
  closes both servers and preserves a nonzero process failure.
- The default bind is loopback. Ops must not be exposed through public ingress.
- No TLS, CORS, authentication, public rate limiting, WAF, or DDoS protection is
  provided by this foundation.

## Non-goals

The service contains only the explicit loopback session verification and
current-token revocation routes described above. It does not contain login,
refresh, session-wide logout/kick, device policy, read state, message or
conversation APIs, WebSocket/gateway, push, webhook, bot, group, moderation,
retention, FeatureGate, license, or public-ingress behavior. These require their
own registered tasks and independent acceptance.
