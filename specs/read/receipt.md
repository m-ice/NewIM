# 私聊消息已读回执

发送者可以查询自己已提交的一条私聊消息。使用当前会话 Bearer，经 TLS 向 `/api/v1/conversations/{conversationId}/messages/{serverMsgId}/read-receipt` 发送 GET 请求，请求体为空。

成功响应例如 `{"serverMsgId":"msg_123","read":true}`。当消息位于对端的授权历史内，并且对端持久位点已到达该条消息时，`read` 为 true。其他情况下为 false。拉黑期间，已持久私聊消息的历史回执按相同规则查询。

未知会话、未知消息、非发送者、非成员和非私聊统一返回 `404 READ_NOT_FOUND`。鉴权失败、请求超限和服务故障沿用会话已读位点的错误码。

回执与会话位点共用 `NEWIM_READ_WATERMARK_HTTP` 开关及地址和账号请求预算。换设备、换网关或重连后，可以使用原会话和消息标识重新查询。[会话已读位点](watermark.md)
