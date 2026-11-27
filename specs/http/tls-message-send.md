# TLS 私聊收发

客户端使用有会话 Bearer 的 TLS 请求。每个写入口使用 POST，并按既有规范校验请求体。

| 路径 | 契约 |
| --- | --- |
| `/api/v1/conversations/direct` | 以目标用户标识取得直接会话 |
| `/api/v1/messages` | 提交直接会话文本帧，事务提交后返回 `send_ack` |
| `/api/v1/relationships/block` | 当前账号拉黑指定用户 |
| `/api/v1/relationships/unblock` | 当前账号取消拉黑 |

规范和结果沿用 [私聊发送](message-send.md) 和 [方向性拉黑](private-message-policy.md)。地址或已核验账号超过新请求预算时返回 HTTP 429 和 `HTTP_RATE_LIMITED`。

消息接收沿用 [实时消息网关](../gateway/websocket.md)。客户端按会话保存已持久化的消息序号，从该位置续读。
