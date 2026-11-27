# Server implementation

`server/api` implements the loopback-only HTTP API/Ops foundation: `GET
/api/v1/health` on the API listener and `GET /ready` plus `GET /metrics` on the
ops listener. It binds both listeners before readiness, redacts request logs,
and drains on SIGINT/SIGTERM. See
[ADR 0013](../docs/adr/0013-http-api-service-foundation.md) and the
[service specification](../specs/http/service-foundation.md).

`buildinfo` reads embedded Go build metadata without repository/network access.
`cmd/newim-buildinfo` prints it as JSON for artifact diagnostics. Unknown source
fields remain absent; invalid metadata produces a stable error code.

The server tree now contains application and PostgreSQL storage packages for
messages, auth/session, conversation/message sync, and internal media metadata.
For example, `server/message` validates persistence intent and
`server/storage/message` implements the PostgreSQL message/latest-pointer/outbox
transaction; auth, sync, and media services follow the same application/storage
split. These are internal packages and require a trusted caller/identity.

`server/auth/bearerhttp` provides the `GET /api/v1/session` handler for
already-issued bearer tokens. `newim-server` mounts it only when
`NEWIM_AUTH_DSN` is explicitly configured and the API listener is a loopback IP
literal; without that setting the route stays absent. It resolves the persisted
user/device/session/token binding without trusting request-supplied identity,
uses strict route/method/body/header checks, returns stable redacted errors and
does not provide login, device/kick policy, a public listener, or rate limiting.
See [ADR 0017](../docs/adr/0017-bearer-session-http.md),
[ADR 0018](../docs/adr/0018-bearer-session-route-mount.md) and the
[bearer handler specification](../specs/http/bearer-session.md). The same
loopback runtime also mounts `DELETE /api/v1/session/tokens/current` for current
token revocation: it revokes only the presented token idempotently and never
performs session-wide logout. See [ADR 0019](../docs/adr/0019-bearer-token-revocation.md) and the
[token revocation specification](../specs/http/session-logout.md).

可信应用可为已登记会话签发令牌。设置 `NEWIM_AUTH_ISSUER_TOKEN_FILE` 与
`NEWIM_AUTH_ISSUER_TTL_SECONDS` 后，服务会在现有鉴权回环监听上启用
`POST /api/v1/internal/session/token`。凭证文件由运行服务的账号拥有，使用
0400 或 0600 权限，保存 32 个随机字节的 64 位小写十六进制编码。调用方在业务鉴权
成功后提交用户、设备与会话的绑定。签发成功的令牌可直接用于会话查询和当前令牌撤销。
[签发接口](../specs/http/session-issuance.md)。

设置 `NEWIM_AUTH_DEVICE_ADMISSION=1` 后，可信应用可为已登记账号准入设备。账号和
同平台上限默认分别为 8 和 3，由 `NEWIM_AUTH_MAX_DEVICES` 及
`NEWIM_AUTH_MAX_DEVICES_PER_PLATFORM` 调整。`NEWIM_AUTH_SINGLE_DEVICE=1`
启用单设备模式。启用前按可信应用记录核验存量设备与会话，并统一切换共享数据库的写入进程。
[设备准入接口](../specs/http/device-admission.md)。

The NIM-SRV-008 slice defines `server/messagehttp` as the exact loopback
`POST /api/v1/messages` adapter for already-issued bearer tokens and protocol
text v1 requests. It is enabled only by `NEWIM_MESSAGE_HTTP=1` with the same
single local-socket `NEWIM_AUTH_DSN`, explicit local-socket opt-in, and a
loopback API listener. It derives the token-scoped logical
`session.ConnectionIdentity`, delegates current membership authorization and
persistence to `server/message`/`server/storage/message`, and returns the framed
`SERVER_PERSISTED` ACK only after commit. Invalid message configuration fails
before bind with `SERVER_INVALID_MESSAGE_CONFIG`; no message pool is opened when
the gate is disabled. See [ADR 0020](../docs/adr/0020-message-send-http.md) and
the [message-send specification](../specs/http/message-send.md). Independent
NIM-SRV-008 product acceptance remains separate.

`server/webhook` and `server/storage/webhook` add an internal at-least-once
webhook delivery core over the existing message outbox. The worker is optional
inside `cmd/newim-server`, is enabled only by explicit DSN/master-key
configuration, uses the PRT-003 `webhook-v1` signature contract, and has no
public endpoint-management API. Migration 006 adds endpoint revisions and fenced
delivery state. See [ADR 0015](../docs/adr/0015-webhook-delivery.md) and the
[delivery specification](../specs/webhook/delivery.md).

Server-side lifecycle observability is additive: the API package owns a generic
bounded sink/source seam, message/webhook packages emit fixed stage/result
observations, and the server composes independent PostgreSQL snapshots for
outbox fanout and Webhook delivery gauges. See [ADR 0021](../docs/adr/0021-server-message-lifecycle-observability.md)
and the [lifecycle specification](../specs/observability/server-message-lifecycle.md).

`newim-gateway` 为原生客户端提供独立的实时消息监听。启动前应用迁移 009，
并统一设置 `NEWIM_AUTH_DEVICE_ADMISSION=1`。必填项为
`NEWIM_GATEWAY_ADDR`、`NEWIM_GATEWAY_TLS_CERT_FILE`、`NEWIM_GATEWAY_TLS_KEY_FILE`
和 `NEWIM_AUTH_DSN`。TLS 证书与私钥使用绝对路径，数据库 TCP 连接校验证书。
数据库角色需有既有会话和消息增量读取权限、账号行锁和设备活动更新权限，
以及握手记录的读写和过期清理权限。
使用本地数据库套接字时显式设置 `NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1`。网关按直接
入口地址限流，部署时可在可信的四层转发环境使用直连 TLS。

| 网关选项 | 默认值 |
| --- | ---: |
| `NEWIM_GATEWAY_MAX_CONNECTIONS` | 128 |
| `NEWIM_GATEWAY_IP_ATTEMPTS_PER_MINUTE` | 64 |
| `NEWIM_GATEWAY_DEVICE_ATTEMPTS_PER_MINUTE` | 12 |
| `NEWIM_GATEWAY_SUBSCRIBE_TIMEOUT_MS` | 3000 |
| `NEWIM_GATEWAY_WRITE_TIMEOUT_MS` | 3000 |
| `NEWIM_GATEWAY_DRAIN_TIMEOUT_MS` | 12000 |

[接入规范](../specs/gateway/websocket.md) · [ADR 0025](../docs/adr/0025-wss-gateway.md)。
在 PostgreSQL 测试环境运行 `make gateway-check gateway-security gateway-recovery`。

应用迁移 010 后，可在 `newim-server` 和 `newim-gateway` 启用
`NEWIM_USER_PROFILE=1`。前者在可信应用鉴权后预配账号，后者提供已登录用户的资料查询和自助更新。
资料请求按直接连接地址及账号独立限流。每进程每分钟默认分别为 120 和 60 次，可用
`NEWIM_USER_PROFILE_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_USER_PROFILE_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。
数据库角色需具备账号和资料读取权限，按进程用途授予账号插入或资料写入权限。
[资料接口](../specs/user/profile.md)。运行 `make user-check user-security` 验证。

应用迁移 011 后，在 TLS 网关设置 `NEWIM_FRIEND_HTTP=1` 可启用好友申请、关系维护和按修订读取。
每进程每分钟的地址与账号预算默认分别为 120 和 60，可用
`NEWIM_FRIEND_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_FRIEND_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。
[好友接口](../specs/friend/relationships.md)。运行 `make friend-check friend-security friend-recovery` 验证。

Run the root `make build`, `make check`, `make docs-check`, `make api-check`,
`make api-recovery`, `make api-security`, `make auth-http-check`,
`make auth-http-security`, `make auth-route-check`, `make auth-logout-check`,
`make auth-logout-security`, `make webhook-protocol`,
`make webhook-recovery`,
`make webhook-security`, `make webhook-redaction`,
`make message-observability-check`, `make message-observability-metrics`,
`make message-observability-security`, and
`make message-observability-recovery` commands. See
[architecture](../docs/architecture.md) and
[third-party notices](../THIRD_PARTY_NOTICES.md).
