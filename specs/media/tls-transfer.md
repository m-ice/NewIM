# TLS 媒体传输

三个入口都使用 TLS、POST 和现有会话 Bearer。JSON 请求使用 UTF-8，最大 4096 字节。文件大小使用十进制字符串，最少 1 字节，最多 100 MiB。SHA-256 使用 64 个小写十六进制字符。

| 路径 | 请求 | HTTP 200 结果 |
| --- | --- | --- |
| `/api/v1/media/uploads/grant` | JSON `conversationId`、`kind`、`contentType`、`size`、`sha256` | JSON `uploadToken`、`mediaKey`、`expiresAt` |
| `/api/v1/media/uploads/complete` | 单个 `X-NewIM-Upload-Token` 头和 `application/octet-stream` 对象字节 | JSON `mediaKey`、`kind`、`contentType`、`size`、`sha256` |
| `/api/v1/media/download` | JSON `mediaKey` | `application/octet-stream` 附件，含 `Content-Length` |

上传凭证默认有效 120 秒，绑定发放时的账号、设备、会话、Bearer 令牌和聊天会话。服务先核对完整字节和摘要，再提交就绪元数据。提交的结果未知时，使用原凭证和原对象字节重试。已就绪凭证过期后仍可重试，调用方的会话、令牌及聊天成员资格须有效。

对象提交后，首次发送媒体消息使用发放凭证时的 Bearer 令牌。通过消息发送接口把返回的元数据作为媒体消息载荷。接收方通过 WSS 或按序号补拉得到同一元数据。[消息发送](../http/tls-message-send.md) · [按序号补拉](../sync/conversation-http.md#消息补拉)

下载要求资产已就绪且读者是聊天会话的当前成员。服务使用打开的文件句柄验证内容，在返回附件前再次核对权限。响应带 `Cache-Control: no-store` 和 `X-Content-Type-Options: nosniff`。客户端可按媒体消息中的摘要检验收到的内容。

错误体为 `{"error":{"code":"..."}}`。

| HTTP | 错误码 |
| --- | --- |
| 400 | `MEDIA_INVALID_INPUT`、`MEDIA_INVALID_TOKEN` |
| 401 | `AUTH_INVALID_TOKEN` |
| 403 | `MEDIA_UNAUTHORIZED` |
| 404 | `MEDIA_NOT_FOUND` |
| 405 | `HTTP_METHOD_NOT_ALLOWED` |
| 409 | `MEDIA_CONFLICT` |
| 410 | `MEDIA_EXPIRED` |
| 413 | `HTTP_BODY_TOO_LARGE` |
| 415 | `HTTP_UNSUPPORTED_MEDIA_TYPE` |
| 429 | `MEDIA_RATE_LIMITED` |
| 503 | `AUTH_UNAVAILABLE`、`MEDIA_STORAGE_UNAVAILABLE` |

已有 TLS 消息网关可用 `NEWIM_MEDIA_HTTP=1` 启用媒体入口。应用迁移 005 后配置 `NEWIM_MEDIA_LOCAL_ROOT` 为绝对路径，目录由网关运行用户持有并设为 0700，且无额外 ACL 授权。多个实例使用同一私有持久目录。对象 I/O 限时 2 分钟，其间每次数据库调用使用原有的短期限。地址、账号的请求预算默认分别为每分钟 120 和 60，可用 `NEWIM_MEDIA_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_MEDIA_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。关闭媒体开关后仍保留对象及元数据。
