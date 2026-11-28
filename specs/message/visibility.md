# 权威未读与消息可见性

会话目录在同一修订返回 `lastReadSeq`、`unreadCount` 和 `visibilityRevision`。
未读为成员可见范围内、有效已读和清空下界之后的接收消息数。个人删除、撤回、管理移除
及自发消息不计入。SDK 按目录检查点持久发布未读。

`POST /api/v1/messages/visibility` 接受 `conversationId`、`serverMsgId`、
`requestId` 和 `action`。作者用 `recall` 撤回，当前群拥有者及管理员用
`adminRemove` 移除。当前账号用 `deleteForMe` 隐藏该消息，各设备共享该状态。
会话清空沿用个人历史下界接口。相同操作账号以相同请求标识重试会返回已提交的事件，
请求标识与另一意图冲突时返回 `SEND_ID_CONFLICT`。

受影响设备的历史和 WSS 回放保留消息身份与序号，类型为 `message_hidden`，载荷由
`action` 和 `countAsUnread: false` 组成。全局撤回、管理移除也保护待投递 Webhook。
个人删除只参与发起账号的可见性同步。

`POST /api/v1/sync/message-visibility` 接受 `conversationId`、十进制字符串
`afterRevision`、`throughRevision` 和可选 `limit`。目标取自当前账号的已发布目录。
读取只返回授权历史区间和当前受众的事件。客户端提交整页及返回的 `nextAfterRevision`。
终页在事件被过滤时仍推进到请求目标，分页期间保持原目标。连接恢复等待消息与可见性两个
进度均落盘。普通重连沿用账号和设备的认证世代，授权变化后旧回调失效。

升级后运行现有会话目录回填，准备仍缺权威未读的旧账号。新目录修订发布后，旧游标按
现有失效规则重新引导。三个验收命令覆盖 TLS 与 WSS、真实 SQLite、旧库重启和可见性分页。
