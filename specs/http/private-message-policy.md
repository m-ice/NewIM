# 私聊和拉黑接口

`NEWIM_PRIVATE_MESSAGE_POLICY=1` 与既有 loopback 消息入口一同启用。请求使用有效的会话 Bearer token 和 JSON 对象 `{"peerUserId":"用户ID"}`。使用现有错误响应格式，受理后才写入关系数据。

| 方法与路径 | 成功响应 |
| --- | --- |
| `POST /api/v1/conversations/direct` | 200 和 `{"conversationId":"会话ID"}` |
| `POST /api/v1/relationships/block` | 204 |
| `POST /api/v1/relationships/unblock` | 204 |

创建私聊可重复调用并返回同一会话。拉黑与解除可重复调用。接收方已拉黑发送方时，新私聊及新私聊消息返回 `403 BLOCKED_BY_RECIPIENT`。已提交的同意图消息重试返回原有 ACK。群聊和既有历史仍按会话权限访问。
