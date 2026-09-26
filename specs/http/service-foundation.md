# HTTP service foundation

Status: review
Task: NIM-SRV-004
API version: `v1`

This specification defines only the loopback process skeleton. It does not
define public API semantics. ADR 0017 through ADR 0020 define the separately
composed bearer session, token revocation and text-send boundaries.

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
ADR 0019 amended ADR 0018's GET-only decision to allow `GET` and `DELETE`.
ADR 0020 adds `POST` for the exact message-send route; accepted dynamic methods
are `GET`, `POST` and `DELETE`, with canonical `Allow` order `GET, POST, DELETE`.
NIM-SRV-006 adds the GET-only `route="session"`, NIM-SRV-007 adds the
DELETE-only `route="session_token_revoke"`, and NIM-SRV-008 may add the
POST-only `route="message_send"` only under the exact loopback/local-socket
configuration in [message-send.md](message-send.md). The foundation itself still
does not import auth, message or storage.

`APIRoute.MaxBodyBytes=0` keeps every method on that route bodyless. A positive
limit is valid only for a POST-only route and bounds the decoded request body.
`APIRoute.RejectQuery=true` is valid only for a POST-only route and rejects
`RawQuery` or `ForceQuery` with 400 `HTTP_INVALID_QUERY` before size checks. The
current message route is the only route using either option.

## Errors

Application-handled errors are JSON:

```json
{"error":{"code":"STABLE_CODE"}}
```

| Status | Code | Condition |
| --- | --- | --- |
| `400` | `HTTP_BODY_NOT_ALLOWED` | A bodyless route receives a non-empty, unknown-length or chunked body |
| `400` | `HTTP_INVALID_QUERY` | A query-reject route receives `RawQuery` or `ForceQuery` |
| `404` | `HTTP_ROUTE_NOT_FOUND` | Unknown path, including trailing slash |
| `405` | `HTTP_METHOD_NOT_ALLOWED` | Known path with a method outside its AllowedMethods; includes exact `Allow` |
| `413` | `HTTP_BODY_TOO_LARGE` | Known or bounded-read body exceeds a positive POST-route limit |
| `415` | `HTTP_UNSUPPORTED_MEDIA_TYPE` | Message route Content-Type or Content-Encoding is not allowed |
| `500` | `HTTP_INTERNAL_ERROR` | Recovered handler panic |
| `503` | `SERVER_NOT_READY` | `/ready` while draining |

For existing dynamic routes, error precedence is route, method, body policy,
readiness/handler. The exact message route freezes route, method, query,
known-length/max, Content-Type/Encoding, bounded read, bearer, protocol,
membership/send order. Known-length overflow is 413 before media checks; an
unknown/chunked body with disallowed media headers is 415 before reading, and a
valid-header unknown/chunked body that overflows during bounded read is 413.
`MaxBodyBytes=0` never means a zero-byte limit. Responses set `Cache-Control:
no-store` and `X-Content-Type-Options: nosniff`. The service does not reflect raw
paths, queries, headers, bodies, panic values, SQL, or transport errors.

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

`listener` is `api|ops`; `route` is `health|ready|metrics|unmatched` plus explicitly registered bounded names (`session`, `session_token_revoke`, `message_send`); `method`
is one of the bounded HTTP method classes (`GET`, `POST`, `DELETE` and other
normalized classes); `status` is a three-digit handler
status. Request paths, query strings, headers, client addresses, tokens,
message data, and revisions are never labels.

## Lifecycle and security limits

- Both listeners bind before readiness becomes true.
- Failure to bind the second listener closes the first and exits nonzero.
- Timeouts: read-header 5s, read 10s, write 10s, idle 60s; max header 16 KiB.
- Bodyless routes reject bodies without reading them.
- The message route reads only after method/query/media/size checks and uses a
  bounded body reader; its configured limit is positive.
- Panic recovery returns only `HTTP_INTERNAL_ERROR`.
- SIGINT/SIGTERM clears readiness, keeps both listeners accept-capable for a
  bounded 500 ms readiness-observation window so `/ready` can return `503`, then
  starts a concurrent drain inside the same 10-second budget. A readiness
  request after the draining state is entered and before listener close ends
  the observation wait early; startup-time `503` responses do not satisfy this
  barrier. A timeout closes both servers and preserves a nonzero process
  failure.
- The default bind is loopback. Ops must not be exposed through public ingress.
- No TLS, CORS, authentication, public rate limiting, WAF, or DDoS protection is
  provided by this foundation.

## Non-goals

The service contains only the explicit loopback session verification,
current-token revocation and opt-in text-send routes described above. The
text-send route is an adapter over the existing internal persistence
transaction, not a public message API or delivery service. The foundation does
not contain login, refresh, session-wide logout/kick, device policy, read state,
public conversation APIs, WebSocket/gateway, push, webhook management, bot,
group, moderation, retention, FeatureGate, license, or public-ingress behavior.
These require their own registered tasks and independent acceptance.
