# 用户预配与资料接口

成功响应返回版本为 `1` 的 JSON，错误码放在 `error.code`。资料对象包含 `userId`、`nickname`、`faceUrl` 和 `ex`。后面三个字段为空时返回空字符串。扩展字段属于公开资料，应用可存储供其他账号阅读的业务数据。

| 方法和路径 | 认证 | 请求与结果 |
| --- | --- | --- |
| `POST /api/v1/internal/users/provision` | 回环可信应用凭证 | 需要 `userId`，可携带初始资料。成功返回 `profile`。同一账号重试返回已提交资料。 |
| `GET /api/v1/users/me` | TLS 和会话 Bearer | 返回当前身份的 `profile`。 |
| `POST /api/v1/users/me/update` | TLS 和会话 Bearer | 至少提交一个可写资料字段。缺省保留，显式空字符串清空。成功返回提交后的 `profile`。 |
| `POST /api/v1/users/lookup` | TLS 和会话 Bearer | 提交 `userIds`，返回按请求顺序去重的 `profiles`，跳过没有账号记录的标识。 |

用户标识沿用现有 1 至 128 字节的标识规则。昵称、头像和扩展字符串最长分别为 128、2048 和 4096 个 UTF-8 字节，字段禁止空字符。查询单次接收 1 至 50 个标识，请求体上限为 16 KiB。请求字段精确匹配，拒绝重复字段和查询串。账号更新目标来自已核验会话。

部署启用账号分类预配后，可信预配请求可携带 `actorType`。赋值、幂等和消息表现见 [机器人账号与消息身份](../bot/identity.md)。

| 状态 | 错误码 |
| --- | --- |
| 400 | `USER_INVALID_INPUT` |
| 401 | `AUTH_INVALID_TOKEN` |
| 404 | `USER_NOT_FOUND` |
| 405 | `HTTP_METHOD_NOT_ALLOWED` |
| 409 | `USER_ACTOR_TYPE_CONFLICT` |
| 413 | `HTTP_BODY_TOO_LARGE` |
| 415 | `HTTP_UNSUPPORTED_MEDIA_TYPE` |
| 429 | `USER_RATE_LIMITED` |
| 503 | `USER_STORAGE_UNAVAILABLE` |
