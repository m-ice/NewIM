# TLS 会话同步读取

客户端使用会话 Bearer 令牌通过 TLS 发送 POST。请求体为 UTF-8 JSON 对象，上限 4096 字节。地址与账号独立限流。

| 路径 | 初始请求 | 续页请求 |
| --- | --- | --- |
| `/api/v1/sync/bootstrap` | `{}` 或 `{"limit":50}` | `{"cursor":"…"}` |
| `/api/v1/sync/delta` | `{"checkpoint":"…"}`，可选 `limit` | `{"cursor":"…"}` |

`limit` 默认 100，显式传 0 时也使用默认值。可设为 1 到 100。字段值需要匹配当前请求类型。

响应使用现有 [会话分页协议](conversation-projection.md)，包含 `items`、`hasMore` 与 `nextCursor`，上限 65,536 字节。中间页的 `nextCursor` 用于继续当前轮次。终页的 `nextCursor` 是下一轮 delta 的检查点。客户端将一页的修订和下一游标一起持久化。所有响应禁止缓存。

| 状态 | 代码 | 客户端处理 |
| --- | --- | --- |
| 400 | `SYNC_INVALID_INPUT` / `SYNC_INVALID_CURSOR` / `SYNC_LIMIT_EXCEEDED` | 校验字段、游标类型和账号 |
| 401 | `AUTH_INVALID_TOKEN` | 重新登录 |
| 403 | `SYNC_FORBIDDEN` | 停止读取 |
| 409 | `SYNC_CURSOR_EXPIRED` | 重新开始 bootstrap |
| 429 | `SYNC_RATE_LIMITED` | 退避后续读 |
| 503 | `SYNC_NOT_READY` / `SYNC_STORAGE_UNAVAILABLE` / `AUTH_UNAVAILABLE` | 退避后续读 |

## 配置与换钥

会话源写入和存量回填完成后，在 TLS 网关启用 `NEWIM_CONVERSATION_SYNC=1`。承载会话写入的网关同时启用 `NEWIM_CONVERSATION_PROJECTION=1`。环境变量 `NEWIM_CONVERSATION_SYNC_KEYS_FILE` 指向当前运行用户所有的绝对文件路径，权限为 0400 或 0600。文件为不超过 4096 字节的 JSON，包含 `activeKeyId` 和 `keys`。键标识最多 32 个字母、数字、下划线或连字符。至多配置八个密钥，值为 32 字节的小写十六进制。

换钥时，先为每个实例加入新验证密钥，继续使用原签名密钥。全部实例就绪后逐一切换签名密钥。旧密钥在最后一次签名后保留至少 24 小时。回退保留仍在有效期内的游标所需的验证密钥。

每进程每分钟地址、账号预算默认分别为 120、60，可通过
`NEWIM_CONVERSATION_SYNC_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_CONVERSATION_SYNC_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。
