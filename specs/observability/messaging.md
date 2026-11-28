# 消息运行指标

网关启用 `NEWIM_GATEWAY_METRICS_ADDR` 后，在单独的回环监听提供 Prometheus
`GET /metrics`，TLS 消息监听提供 `POST /api/v1/observability/sdk`。
发送阶段、服务端持久消息扇出和 Webhook 积压沿用既有契约。
[服务端消息观测](server-message-lifecycle.md)

| 指标 | 含义 |
| --- | --- |
| `newim_gateway_active_subscriptions` | 本进程当前已经通过订阅校验的活跃 WSS 连接数 |
| `newim_gateway_operations_total{operation,result}` | 按固定动作和结果统计网关操作 |
| `newim_gateway_operation_duration_seconds_{sum,count}{operation}` | 按动作累计经过时间和次数 |
| `newim_sdk_events_total{event}` | SDK 上报的连接、重连、回执、缺口、页面及消息提交次数 |
| `newim_sdk_ack_duration_seconds_{sum,count}` | SDK 从发送尝试到本地提交回执的累计时长和样本数 |
| `newim_sdk_outbox_depth_{sum,count}` | SDK 从存储读取的待发任务量之和、读取得到的样本数 |
| `newim_push_dispatch_total{result}` | 推送工作进程持久提交的接受、重试、终止失败、无效及取消次数 |

网关动作包括 `handshake`、`subscribe`、`delivery`、`visibility_notice`、
`directory_bootstrap`、`directory_delta`、`message_page` 和 `visibility_page`。
结果包括 `success`、`rejected` 和 `temporary`。实时投递以网关当前帧写入结果为准。
SDK 的接收指标以当前连接票据对应的持久存储结果为准。

启用上报的 SDK 宿主在连接就绪后提取 `Connection::take_metrics_report`，以当前会话
Bearer 通过受信 TLS 连接发送编码后的 JSON。汇总包由 SDK 排除账号、设备、业务标识、追踪标识和正文。
每个事件计数最多 1024，报告最多 2048 字节。采集端会校验当前会话、速率、字段和值域，并以 HTTP 204
确认接收。SDK 汇总包用于尽力观测，宿主一次性提交已取出的汇总。上报失败时，后续发送与同步照常运行。
采集器只使用固定事件标签，SDK 数据是客户端采样数据。

[消息诊断看板](../../infra/observability/README.md)
