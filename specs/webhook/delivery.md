# Internal webhook delivery core

本文定义 NIM-WHK-001 的内部 Webhook fan-out 与投递状态机。它复用 PRT-003 的
`webhook-v1` envelope，不定义公开管理 API、Bot、Push、登录、retention 或消息事务。

## Inputs and boundaries

- 输入是既有 `im_outbox_events` 和已提交消息，不新增消息业务规则。
- endpoint 与 secret 只由受信任内部配置/数据库操作创建；本任务不提供 HTTP CRUD。
- 生产只允许 HTTPS；secret 以加密封存材料保存，worker 通过 `SecretResolver` 解封。
- 投递是 at-least-once、可乱序；消费者按 `deliveryId`/`eventId` 幂等。

### `message.persisted` payload v1

消息事件的 envelope `payload` 是 JSON object，字段固定为：
`clientMsgId`、`conversationId`、`conversationSeq`、`serverMsgId`、`serverTime`、
`senderId`、`type`、`protocolVersion`、`version`、`payload`。其中 `version` 是消息
schema version（与持久消息的 `version` 一致），`protocolVersion` 是持久消息协议版本，
内层 `payload` 保留原始已验证 JSON object。所有 64 位计数使用十进制字符串；
消费者必须忽略未来新增字段。该映射由 `make webhook-protocol` 的真实 HTTP receiver
测试验证，不允许实现端另建未登记的私有事件格式。

若完整消息 payload 加上信封元数据会超过 Webhook v1 的 65,536-byte 或 depth-32 上限，
worker 使用同一 schema 的 bounded fallback：内层 `payload` 置为 `{}`，并新增
`payloadOmitted=true`、`payloadSha256` 和 `payloadSize`（十进制字符串）。事件身份、
签名、尝试语义不变；消费者必须显式处理 omitted payload，不得把它当作完整正文。

## Fan-out

worker 在一个 PostgreSQL 事务内领取未标记 outbox 事件，为 active endpoint 的
frozen revision 插入 delivery，然后只更新 `im_outbox_events.webhook_fanout_at`。
不得更新共享 `completed_at`。`(event_id,destination_id)` 唯一键使崩溃后的重放安全。
迁移 006 把既有 outbox 行标记为已 cutover，新行保持 pending。

## Delivery state machine

```text
pending -> leased -> delivered
pending -> leased -> retry -> leased
pending -> leased -> dead_letter
pending/retry/leased -> cancelled (endpoint revoked)
```

- `Claim` 只选择 pending/retry、到期且未被 revoke 的 delivery，使用行锁、随机
  `lease_token` 和有限租约。到期判断、租约期限和终态时间使用 PostgreSQL
  `clock_timestamp()`，不能依赖跨进程 wall clock。
- `BeginAttempt` 必须在 HTTP 请求开始前以同一 token 原子递增 `attempts`；租约过期、
  token 不匹配或 attempt 耗尽不得发请求。
- `Finish` 必须校验 live token。成功 2xx 为 delivered；不可重试 4xx/3xx 为
  dead_letter；网络、408、429、5xx 在 attempt 未耗尽时进入 retry。退避有上限并
  带稳定 jitter；429 的 `Retry-After` 只能在硬上限内采纳；终态不可重新领取。
- endpoint revoke 后新 claim、fan-out 被禁止，pending/retry/leased 可取消；历史
  delivery 不删除。既有已开始的单次请求只在 request deadline 内完成。

## Outbound safety

`SecureClient` 一次解析全部地址，拒绝 loopback/private/link-local/multicast/reserved、
documentation、IPv4-mapped IPv6、NAT64、6to4、Teredo 和 site-local 绕过；实际 dial 只连接已批准 IP，保留原始
Host/SNI/TLS 校验，禁止 proxy 环境和重定向。超时、响应体、并发、fan-out endpoint
数量、单 endpoint 队列、速率、重试和 backlog 都有硬上限。生产默认 HTTPS；测试 loopback/HTTP policy 只能由
测试构造器显式传入。

当前部署契约是每个数据库一个 Webhook worker 进程；上述并发和 token-bucket 是该进程内的硬上限。
多副本共享限流属于后续 HA 任务，不能把本实现描述为跨进程全局限流。

## Secrets and observability

AES-GCM 材料绑定 destination ID、revision、URL、key ID；resolver 失败 fail-closed，不能回退
明文；secret nonce 在进程主密钥下不得复用（数据库全局唯一约束）。日志/错误/指标只包含稳定码和有界 operation；不得包含 secret、signature、
payload、query、userinfo、完整 URL 或响应体。endpoint 配置表可以保存经过校验的 URL，
但运维日志和 metrics 使用不可逆短标识。提供 pending/retry/dead-letter、attempt、latency、
endpoint state 观测。

## Lifecycle and commands

worker 在 `server/cmd/newim-server` 进程内，只有显式配置 DSN 与 master key 才启动。
未配置时不启动；配置不完整或初始化失败则启动失败。SIGTERM 先停止新 claim，取消/等待
有界 in-flight，join worker 完成后再关闭数据库池，保留可恢复租约。生产日志 observer
只记录 operation 和稳定 code；运行期存储/HTTP 故障用有界退避，不静默成功。

```sh
make webhook-protocol
make webhook-recovery
make webhook-security
make webhook-redaction
make build check
```

目标必须运行真实 PostgreSQL 隔离容器和适用真实 HTTP receiver；不能只断言 mock。
