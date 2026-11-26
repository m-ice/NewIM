# 多端设备准入接口

`POST /api/v1/internal/session/admit` 由回环网络上的可信应用调用，使用与会话签发入口相同的单值 Bearer 凭证。UTF-8 JSON 请求不超过 1024 字节，仅含 `userId`、`deviceId`、`platform` 三个字符串。平台由应用核验并提交稳定的小写标识。

成功返回 200 和 `{"version":1,"userId":"…","deviceId":"…","sessionId":"…","token":"…","expiresAt":"…"}`。返回的令牌沿用现有会话查询与撤销接口，响应禁用缓存。设备准入、超限撤销和令牌签发提交后才返回。

| 错误 | HTTP 状态 |
| --- | --- |
| `AUTH_INVALID_TOKEN` | 401 |
| `AUTH_INVALID_INPUT` | 400 |
| `AUTH_SESSION_NOT_ELIGIBLE` | 403 |
| `AUTH_DEVICE_LIMIT_EXCEEDED` | 409 |
| `AUTH_UNAVAILABLE` | 503 |

现有会话签发、会话查询和撤销保留已验收的接口契约。启用时，旧签发入口验证已有会话的准入状态。
