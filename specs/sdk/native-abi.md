# 原生 SDK C ABI

在自己的串行 SDK 工作线程调用 [newim_native.h](../../sdk/native/include/newim_native.h)。
先核对 `newim_native_abi_version()`。路径与账号为 UTF-8 字节序列。存储根路径须为
绝对路径，按账号分配独占目录，并遵守 SQLite 适配器的权限和符号链接约束。

输入和输出缓冲区在调用期间由宿主持有。传入长度在允许范围内时，对应内存必须可读或可写。
输出长度和句柄使用对齐的独立存储，宿主不得让它们与其他缓冲区重叠。事件以实际长度返回，
末尾无额外终止字节。每个事件最多 1 MiB。

| 入口 | 契约 |
| --- | --- |
| `newim_native_open` | 打开账号存储，返回非零句柄。失败时将有效的输出句柄置零。 |
| `newim_native_submit` | 处理一条 UTF-8 JSON 事件。有待提取输出或同句柄正在处理调用时返回忙，宿主提取或等待后可重试。 |
| `newim_native_next` | 按序取出一个动作或通知。空输出指针与零容量可查询大小。容量不足时保留队首并返回所需字节数。无事件时长度为零。 |
| `newim_native_close` | 释放账号实例及队列，停止向旧句柄提交完成。与同句柄的调用重叠时可返回忙，宿主在调用完成后重试。旧句柄返回已关闭。 |

状态码按 C 头文件解释。调用成功后，宿主提取当前输出至 `NEWIM_NATIVE_NO_EVENT`。
返回其他错误状态时也可提取此前已发出的输出。提交与提取都在宿主 SDK 工作线程执行。

## 输入事件

事件包装为 `{"nowMs":1710000000000,"random":123,"event":{"type":"network_available"}}`。
`nowMs`、`expiresAtMs` 和重试位点使用同一跨进程时间基准，单位为毫秒。
宿主向同一实例提交的时间要非递减，并在定时唤醒和传输异常时提供新的随机数。

| `event.type` | 附加字段和处理 |
| --- | --- |
| `network_available`、`network_changed`、`network_lost`、`tick`、`resume`、`logout` | 无。宿主定时器和系统网络回调共同驱动 SDK。 |
| `select_conversation`、`refresh_directory` | 选择操作的 `conversationId` 可为字符串或 null。刷新沿用当前选择。 |
| `credentials` | `operation`、`account`、`expiresAtMs`、`verified`。由宿主为指定账号验证当前会话。Bearer 由宿主持有。 |
| `http` | `operation`、`status`、`verified`、`body`。正文是原始 UTF-8 文本字符串。`traceId` 可选，只接收来自同一已验证响应的单值诊断头。 |
| `socket_opened`、`socket_subscribed` | 均含 `operation`。打开完成还包含 `verified` 与实际 `subprotocol`。 |
| `frame`、`socket_closed` | 均含当前套接字 `operation`。帧包含 `body` 文本。关闭包含 `code` 与 `reason`，由共享协议分类。 |
| `transport_failed` | `operation` 和 `reason`，后者使用 `temporary`、`unauthorized`、`revoked`、`expired`、`device_limit`、`conversation_forbidden` 或 `protocol`。 |
| `queue_send` | `body` 为当前协议的发送帧文本。先提交待发意图，再返回网络动作。 |
| `resume_send`、`dismiss_send` | `clientId` 表示已持久化意图。继续鉴权恢复或移除终态项使用 SDK 出站队列规则。 |
| `list_conversations`、`list_messages`、`list_pending` | `limit` 是页大小，`after` 是上一页返回的可选位点。`list_messages` 还需要 `conversationId`。 |
| `notification`、`take_metrics` | 通知携带 `marker`，平台收到的 `newim: sync` 触发常规刷新。指标按显式调用输出。 |

`operation` 是 SDK 发出的十进制字符串，宿主需原样关联完成信息。WebSocket 订阅、消息帧
和关闭使用同一个连接动作标识。SDK 拒绝失效连接的回调。经过 TLS 验证且与未确认发送匹配的
权威回执，在同一鉴权世代重连后仍可提交。同一发送收到已验证的鉴权拒绝时，
SDK 立即停止本地读取和发送，待 SQLite 提交新的鉴权世代后再请求凭据。
如果 SQLite 暂时不可用，宿主继续提交 `tick` 和网络状态。此时仍可提交 `logout`，
待 SQLite 恢复后完成注销。

## 输出事件

`credentials_required`、`http`、`open_socket` 和 `subscribe` 发起宿主动作。
HTTP 动作给出请求方法、相对路径和原始正文字符串。WS 动作给出相对路径、子协议或订阅正文。
宿主从其实际认证连接交回结果。收到 `cancel` 时，按原动作标识停止对应工作。
`metrics` 由宿主尽力发送，无需交回完成。

`ready` 表示当前会话的目录及订阅恢复条件已满足。`directory_changed` 与
`messages_changed` 对应已提交的本地结果。用相应的列表入口取得 SQLite 视图。
`conversations`、`messages` 与 `pending` 包含 `items` 和可选的 `next`。
序号、游标中的持久版本，以及未读计数使用十进制字符串。消息和待发项的 `payload` 保留原始
文本，正文不可用时返回 null。

`send_queued` 表示本地意图已持久化，`send_ack` 表示权威回执已和本地意图一并提交。
`send_wait` 提供下一尝试时间，`send_failed` 提供固定失败码。宿主使用 `tick` 驱动到期工作。
关闭或重启能保留已持久化的待发项。重新打开会读取 SQLite 已提交的操作编号与消息位点。
