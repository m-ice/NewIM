# Webhook 运维管理

## 启用

在应用 019 迁移后，以 `NEWIM_WEBHOOK_DSN`、`NEWIM_WEBHOOK_MASTER_KEY_B64` 启动 worker。
将 `NEWIM_WEBHOOK_MANAGEMENT_TOKEN_FILE` 指向部署凭据文件即可挂载运维路由。文件用绝对路径，
由进程账号持有，权限为 0400 或 0600。凭据为 `whm_` 后接 64 位小写十六进制字符，文件可带末尾换行。
API 监听地址须为数字 IP 回环地址。通过 `Authorization: Bearer <token>` 认证。

## 请求

全部入口使用 `POST`、`application/json`、至多 8192 字节的 JSON 对象。路径前缀为
`/api/v1/webhooks/`。请求字段区分大小写，修订按规范十进制字符串传输，`limit` 是整数。
目标、投递、请求标识符为 1 至 128 位 ASCII 字母、数字、下划线或短横线，`keyId` 至多 64 位。

| 路径 | 必填字段 | 可选字段 |
| --- | --- | --- |
| `endpoints/create` | `requestId`, `destinationId`, `url`, `keyId`, `secret` | |
| `endpoints/get` | `destinationId` | |
| `endpoints/list` | | `afterId`, `limit` |
| `endpoints/update` | `requestId`, `destinationId`, `expectedRevision`, `url` | |
| `endpoints/rotate` | `requestId`, `destinationId`, `expectedRevision`, `keyId`, `secret` | |
| `endpoints/revoke` | `requestId`, `destinationId`, `expectedRevision` | |
| `deliveries/list` | `destinationId` | `afterId`, `limit`, `deliveryId` |
| `deliveries/replay` | `requestId`, `destinationId`, `deliveryId` | |

创建返回 201，其余成功返回 200。目标结果包含 ID、状态、当前修订、key ID 和时间，
投递结果包含投递、目标和事件 ID、冻结修订、状态、尝试次数和诊断码。URL 和 secret 仅用于写入。
列表返回 `items`，有下一页时返回 `nextAfterId`，继续时传入 `afterId`。页大小默认 50，
范围 1 至 100。指定 `deliveryId` 可按原投递身份查当前结果，此时省略 `afterId`。

## 状态

创建获得修订 1，最多启用 64 个目标。修改 URL 或轮换生成新修订，已经入队的投递继续使用
原修订。URL 使用 HTTPS，出站仍逐次检查并固定批准地址。`secret` 为 64 位小写十六进制字符。
轮换选择该目标从未使用的 key ID，接收方应在旧修订投递结束前保留对应验签密钥。
吊销保留历史且禁止复用目标 ID。worker 停止为该目标领取及授权新尝试，已授权请求按原期限结束。

手动回放允许当前修订中 HTTP 永久失败或常规尝试耗尽的死信。入队沿用相同投递、事件和修订，
由 worker 执行最多八次普通尝试。每条记录只有一次手动入队额度。回执包含请求、目标和投递 ID、
修订及回放时间。调用结果未知时用相同请求与投递 ID 重试，已提交的回放返回原回执。
成功写操作与数据库审计一起提交。审计记录服务端运维身份、操作、请求 ID、目标、修订和时间，
回放另记录原终态的尝试次数、诊断码和完成时间。

| HTTP | 错误码 |
| --- | --- |
| 400 | `WEBHOOK_MANAGEMENT_INVALID_INPUT` |
| 401 | `AUTH_INVALID_TOKEN` |
| 404 | `WEBHOOK_MANAGEMENT_NOT_FOUND` |
| 409 | `WEBHOOK_MANAGEMENT_CONFLICT`, `WEBHOOK_MANAGEMENT_NOT_REPLAYABLE` |
| 503 | `WEBHOOK_MANAGEMENT_CAPACITY`, `WEBHOOK_STORAGE_UNAVAILABLE` |

请求方法、大小、查询字符串和媒体类型复用 [HTTP 服务规则](../http/service-foundation.md)。
