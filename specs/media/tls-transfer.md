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

## S3 兼容存储

通过 `NEWIM_MEDIA_STORAGE=s3` 选择 S3。继续使用同一套上传、发送和下载接口。媒体配置为：

| 变量 | 值 |
| --- | --- |
| `NEWIM_MEDIA_S3_BUCKET` | 已创建的专用私有桶 |
| `NEWIM_MEDIA_S3_REGION` | 对象服务区域 |
| `NEWIM_MEDIA_S3_ENDPOINT` | 可选。自托管 HTTPS 源地址，使用 path-style 寻址 |
| `NEWIM_MEDIA_S3_CA_FILE` | 可选。额外受信 CA 的 PEM 文件绝对路径 |
| `NEWIM_MEDIA_S3_STAGING_ROOT` | 0700 私有临时目录的绝对路径，由网关运行用户持有 |
| `NEWIM_MEDIA_S3_MAX_TRANSFERS` | 每个网关同时使用临时文件的传输数量，默认 8，范围 1 到 32 |

网关从 AWS SDK 的凭据链取得部署凭据，启动时检查桶是否可访问。桶权限需要在专用桶内读取和条件写入对象，并能识别查询的对象是否存在。可在桶策略要求写请求携带 `If-None-Match: *`。自托管连接校验证书和主机名。每个传输至多暂存 100 MiB，磁盘容量按每进程传输上限乘该值规划。暂存名额或空间不足返回 `503 MEDIA_STORAGE_UNAVAILABLE`。

同一个媒体库需要使用同一后端。切换步骤如下。

1. 排空全部网关的媒体写入。
2. 把媒体库引用的对象复制到目标后端，核对最终对象集、长度和 SHA-256。
3. 将全部网关切换到目标后端，确认可以读取对象后恢复写入。

切换后如需回退，按相同步骤把新对象反向复制、核对并同步切换。数据库继续使用已有媒体表。
