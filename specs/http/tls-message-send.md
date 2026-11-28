# TLS 消息收发

客户端使用有会话 Bearer 的 TLS 请求。每个写入口使用 POST，并按既有规范校验请求体。

| 路径 | 契约 |
| --- | --- |
| `/api/v1/conversations/direct` | 以目标用户标识取得直接会话 |
| `/api/v1/messages` | 提交直接会话或已启用的受管群会话文本帧，事务提交后返回 `send_ack` |
| `/api/v1/relationships/block` | 当前账号拉黑指定用户 |
| `/api/v1/relationships/unblock` | 当前账号取消拉黑 |

规范和结果沿用 [私聊发送](message-send.md) 和 [方向性拉黑](private-message-policy.md)。地址或已核验账号超过新请求预算时返回 HTTP 429 和 `HTTP_RATE_LIMITED`。

消息发送还按认证账号与设备计数，`NEWIM_TLS_MESSAGE_DEVICE_ATTEMPTS_PER_MINUTE` 默认等于账号上限。地址单独计数，账号和设备在两者都有剩余额度时一起计数。关系接口沿用地址和账号预算。设置 `NEWIM_STRANGER_MESSAGE_POLICY=1` 后，已分类私聊执行首条消息规则。[私聊发送策略](private-message-policy.md)

启用 `NEWIM_GROUP_HTTP=1` 后，受管群使用其群会话 ID。发送事务使用会话保存的类型执行 [小群权限](../group/membership.md)。新群消息要求当前成员权限并遵守禁言，仍在群内的发送者可重放已提交的同意图回执。旧群会话继续使用既有回环发送方式。

启用 `NEWIM_MEDIA_HTTP=1` 后，`/api/v1/messages` 也接受 `type: "media"`、`version: 1`。将上传完成响应作为原发送帧的 `payload`，使用同一个会话 ID。发送服务核验已就绪媒体属于当前发送者和会话，且元数据完全一致。详见 [媒体传输](../media/tls-transfer.md)。

已提交发送的回执可以附带 `X-NewIM-Trace-Id`。网关在回执编码完成后重新校验当前发送者会话，
成功时才写入该头。同一个持久回执的每次重试使用当次服务端生成的 32 位小写十六进制追踪标识。
无追踪标识时仍按原方式处理回执。SDK 从同一已验证的 TLS 响应获取该头和回执。

消息接收沿用 [实时消息网关](../gateway/websocket.md)。客户端按会话保存已持久化的消息序号，从该位置续读。
