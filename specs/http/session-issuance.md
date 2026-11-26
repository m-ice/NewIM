# 会话令牌签发接口

`POST /api/v1/internal/session/token` 由可信应用后端在回环网络调用。请求使用单值 `Authorization: Bearer <应用签发凭证>` 及 UTF-8 JSON。正文不超过 1024 字节，只包含 `userId`、`deviceId`、`sessionId` 三个字符串字段。

事务提交后返回 200 和 `{"version":1,"token":"…","expiresAt":"…"}`。调用方将令牌交给已鉴权用户。令牌可用于现有会话查询与当前令牌撤销接口。响应带 `Cache-Control: no-store`。

| 错误 | 状态 |
| --- | --- |
| `AUTH_INVALID_TOKEN` | 401 |
| `AUTH_INVALID_INPUT` | 400 |
| `AUTH_SESSION_NOT_ELIGIBLE` | 403 |
| `AUTH_UNAVAILABLE` | 503 |

会话不存在、绑定不一致或已撤销时统一拒绝签发。路径、方法、查询、类型、正文上限和错误响应结构沿用服务的 HTTP 约定。
