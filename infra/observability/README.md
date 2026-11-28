# NewIM 消息诊断看板

将 [消息运行看板](messaging-dashboard.json) 导入 Grafana，选择采集当前部署的 Prometheus
数据源。采集 `newim-server` 运维监听和所有 `newim-gateway` 实例的回环运维监听，路径均为
`/metrics`。网关通过 `NEWIM_GATEWAY_METRICS_ADDR=127.0.0.1:9091` 启用此监听。使用同主机
或同网络命名空间的采集器访问回环地址。5 分钟速率窗口适用于 30 秒采集间隔。

| 看板 | 诊断方式 |
| --- | --- |
| 发送服务回执成功率 | 核对进入发送服务的尝试中有多少生成了回执。幂等重放按尝试计数。 |
| 平均发送确认耗时 | 分别检查服务端提交耗时和采样到的 SDK 回执保存耗时。 |
| 活跃订阅及网关操作 | 查看活跃 WSS 订阅。按动作和结果查看握手、订阅、实时写入以及目录、消息、可见性补拉。 |
| SDK 连接及恢复 | 查看连接尝试、重连、缺口、补拉及消息提交。SDK 面板随上报样本更新。 |
| 离线通知 | 查看供应商接受、重试、失败、无效及任务取消的处理速率。 |
| 各队列深度 | SDK 使用近期上报样本的平均深度。服务端按状态读取持久队列。共享同一数据库的多个目标用最大值去重，每套数据库应使用独立的看板数据源。 |

持续出现发送成功率降低时，先核对发送耗时和消息扇出积压。实时写入健康、客户端缺口持续增加时，
查看 SDK 重连、补拉失败及网关查询结果。离线通知单独查看供应商重试和失败，结合消息发送指标
判断影响范围。没有分母样本时，相关比率面板留空。

授权诊断时，从同一已验证 TLS 回执保留 `X-NewIM-Trace-Id`、`clientMsgId` 和
`serverMsgId`，用受控数据库读取核对已提交回执与后续记录。看板查询使用固定操作、结果和状态标签。

[运行指标契约](../../specs/observability/messaging.md) ·
[Grafana JSON 模型](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/view-dashboard-json-model/) ·
[Prometheus 查询函数](https://prometheus.io/docs/prometheus/latest/querying/functions/)
