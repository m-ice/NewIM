# 会话已读位点

同一账号的设备共用一个会话已读位点。客户端使用现有会话 Bearer，经 TLS 请求 `/api/v1/conversations/{conversationId}/read`。GET 请求体为空。POST 使用 UTF-8 JSON，最多 4096 字节，格式为 `{"lastReadSeq":"42"}`。

成功响应包含 `conversationId`、`lastReadSeq`、`historyAfterSeq`、`latestSeq` 和 `updatedAt`。所有序号都是非负十进制字符串，最大值为 `9223372036854775807`。历史起点为排他边界。较旧或相同的上报直接返回当前位点。最后一次显式推进的时间使用 RFC 3339，可为 `null`。尚无时间的存量记录和仅有历史起点的位点返回 `null`。

| HTTP | 错误码 | 含义 |
| --- | --- | --- |
| 400 | `READ_INVALID_INPUT` | 路径、上报格式或序号无效，或上报超过已有尾序号 |
| 401 | `AUTH_INVALID_TOKEN` | 会话令牌无效、撤销或过期 |
| 404 | `READ_NOT_FOUND` | 会话对当前账号不可访问 |
| 405 | `HTTP_METHOD_NOT_ALLOWED` | 可使用 GET 或 POST |
| 413 | `HTTP_BODY_TOO_LARGE` | 上报超过 4096 字节 |
| 415 | `HTTP_UNSUPPORTED_MEDIA_TYPE` | POST 请求的内容类型或编码无效 |
| 429 | `READ_RATE_LIMITED` | 超出地址或账号预算 |
| 503 | `READ_STORAGE_UNAVAILABLE`、`AUTH_UNAVAILABLE` | 服务暂不可用 |

部署先应用迁移 015，再使用 `NEWIM_READ_WATERMARK_HTTP=1` 启用。地址和账号每分钟默认预算分别为 120 和 60，可通过 `NEWIM_READ_WATERMARK_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_READ_WATERMARK_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。关闭入口后会话位点继续保留。
