# Webhook v1 envelope and signature

本文定义服务端 Webhook 出站请求的 `webhook-v1` envelope、HTTP 安全 header 和
HMAC 验证契约。它不定义 endpoint 管理、投递重试、SSRF、数据库或 Bot；这些由后续
独立任务负责。

## Envelope

```json
{
  "eventId": "evt_01JABCDEF0123456789",
  "eventType": "message.persisted",
  "occurredAt": "1790189000000",
  "schemaVersion": 1,
  "payload": {"serverMsgId": "msg_alpha"}
}
```

字段规则：

- `eventId`、`eventType`：非空 ASCII；event ID 使用 `[A-Za-z0-9_-]{1,128}`；
  event type 使用 `[a-z][a-z0-9_.]{0,127}`。
- `occurredAt`：canonical 非负十进制字符串，范围到 signed BIGINT。
- `schemaVersion`：正整数且当前只接受 `1`。
- `payload`：JSON object；完整 envelope 最多 65,536 bytes、容器深度最多 32。
- 同一 object 内重复 key、非法 UTF-8、未配对 surrogate、未知 schema 版本和尾随
  JSON 必须失败；v1 的未知普通字段可忽略以支持 additive evolution。

## Headers

每个请求必须恰好包含一次：

```text
X-NewIM-Event-Id
X-NewIM-Delivery-Id
X-NewIM-Timestamp
X-NewIM-Nonce
X-NewIM-Key-Id
X-NewIM-Signature-Version: 1
X-NewIM-Signature: v1=<base64url>
```

`X-NewIM-Timestamp` 是本次 attempt 的 canonical Unix 毫秒字符串；每次请求重新生成。
`X-NewIM-Nonce` 是 16 个随机字节的 22 字符无 padding base64url；不得重复使用。
`X-NewIM-Event-Id` 和 `X-NewIM-Delivery-Id` 在重试和租约接管时保持不变。
`X-NewIM-Key-Id` 使用 `[A-Za-z0-9_-]{1,64}`，用于 secret 轮换。
header 名大小写不敏感；重复安全 header、CR/LF 或非 ASCII 值必须拒绝。

## Signature

`signingBytes` 是以下 ASCII 字段以单个 LF 连接、并以一个 LF 结尾：

```text
v1
<keyId>
<deliveryId>
<eventId>
<timestamp>
<nonce>
<lowercase hex SHA-256(rawBody)>
```

签名是 `v1=` 加 `base64url(HMAC-SHA256(secret, signingBytes))` 的无 padding 编码。
secret 至少 32 bytes。signature 的 base64url 必须严格规范，解码后重新编码必须与
输入完全一致。验证必须先做 constant-time HMAC 比较，再解析 envelope、绑定 header
`X-NewIM-Event-Id` 与 body `eventId`，最后检查时间窗和原子 nonce reservation；
失败不得回显 secret、body、signature 或完整 URL。

## Verification

- 可信时钟的默认允许窗口为 5 分钟，包含边界。
- 最老边界的 nonce 有效期必须至少覆盖到 `timestamp + window + 1ms`，避免刚好落在
  inclusive 边界时被 guard 当作已过期。
- 只有签名正确且时间戳在窗口内的请求才能调用 replay guard。
- replay guard 必须原子返回“首次预留/已重放/容量失败”；不得 check-then-insert，
  不得因容量压力淘汰仍在有效期内的 nonce。容量失败返回
  `WEBHOOK_REPLAY_CAPACITY_EXCEEDED` 或 `WEBHOOK_REPLAY_UNAVAILABLE`，必须 fail-closed。
- 业务幂等以 `deliveryId`（或稳定的 `eventId`）为准；nonce 只用于网络重放拒绝。
- `keyId` 必须由显式 `WebhookKeyResolver` 解析；未知或退役 key 返回
  `WEBHOOK_UNKNOWN_KEY`，临时 secret-store 故障返回 `WEBHOOK_KEY_UNAVAILABLE`。
  resolver 必须在 header/envelope/签名格式校验通过后才调用。header 与 body 的
  event ID 不一致返回 `WEBHOOK_IDENTITY_MISMATCH`，不得只验证其中一处。

固定正例、边界、key rotation、decode 和 negative 向量位于
`tests/compatibility/webhook/fixtures/cases.json`。Go API 位于
`core/protocol/go/webhook.go`；`MemoryWebhookReplayGuard` 仅是有界单进程参考实现，
生产共享缓存必须另行提供同样的原子接口并把 verifier 的 trusted `now` 传入 guard。
