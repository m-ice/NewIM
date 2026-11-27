# 账号级会话撤销

可信应用使用发行凭证调用回环的 `POST /api/v1/internal/session/revoke-account`。请求体为 UTF-8 JSON 对象，上限 1024 字节，字段均必填。请求标识由调用方生成，同一动作使用同一标识。

```json
{"userId":"user_123","requestId":"logout_123","reason":"signed_out_all"}
```

| 原因 | 对应的已核验动作 |
| --- | --- |
| `password_changed` | 改密 |
| `signed_out_all` | 主动退出全部设备 |
| `account_blocked` | 封禁账号 |

成功回应为 `{"version":1,"revokedSessions":2}`。计数表示该动作首次提交时新撤销的会话数，可能为零。提交后，目标账号的这些会话及令牌均失效，每条会话撤销事件保存动作原因。

同一账号和请求标识重复调用时，服务器返回首次结果。需要包含此后建立的会话时，使用新的请求标识。

| 状态 | 代码 | 含义 |
| --- | --- | --- |
| 400 | `AUTH_INVALID_INPUT` | 字段缺失或值无效 |
| 401 | `AUTH_INVALID_TOKEN` | 缺少有效应用发行凭证 |
| 403 | `AUTH_SESSION_NOT_ELIGIBLE` | 目标账号不可用于此操作 |
| 409 | `AUTH_REVOCATION_CONFLICT` | 已记录请求标识对应另一原因 |
| 503 | `AUTH_UNAVAILABLE` | 无法确认事务结果，可用同一请求标识重试 |

如已建立 WSS 连接，网关在会话复核时发送 `1008 session_revoked`。设备超限的既有关闭原因为 `device_limit`。

部署先应用迁移 013，在运行设备准入且配置发行凭证的回环进程设置 `NEWIM_AUTH_ACCOUNT_REVOCATION=1`。回退先关闭开关，再回退二进制。已提交的事件和回执保留。
