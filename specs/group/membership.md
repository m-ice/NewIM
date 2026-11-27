# 小群管理

在启用小群的 TLS 网关上，以当前会话的 Bearer 令牌发送 POST 请求。路径位于 `/api/v1/groups/` 下，请求体是 UTF-8 JSON 对象，最大 256 KiB。成功响应含 `version: 1` 及下表所列字段。请求及响应中的账号、群和请求标识使用现有标识格式。

| 路径后缀 | 请求字段 | 成功字段 |
| --- | --- | --- |
| `create` | `groupId`、`profile`、可选 `joinMode`、`memberLimit`、`membersCanInvite`、`historyEnabled`、`memberIds` | `view` |
| `get` | `groupId` | `view` |
| `update` | `groupId` 和至少一项资料或设置 | `view` |
| `join` | `groupId`、`requestId`、`kind`、邀请目标 `userId`、可选 `message` | `request` |
| `respond` | `groupId`、原申请的 `actorId` 与 `requestId`、`decision` | `request` |
| `members` | `groupId`、可选 `afterUserId` 与 `limit` | `page` |
| `member/action` | `groupId`、`action`、目标 `userId`、禁言时的 `muteSeconds` | `membership` |
| `mute` | `groupId`、`muted` | `view` |
| `dissolve` | `groupId` | `dissolved` |
| `requests` | `groupId`、可选 `afterActorId` 与 `afterRequestId`、`limit` | `page` |
| `changes` | `afterRevision`、可选 `limit` | `page` |

`profile` 中的 `name` 必填，长度上限 128 字节。可选字段 `faceUrl` 的上限是 2048 字节，`description`、`notification` 和 `ex` 各 4096 字节。更新时把要修改的资料字段直接放在请求对象中，也可修改 `joinMode`、`membersCanInvite` 和 `historyEnabled`。

创建时客户端生成并保存 `groupId`。群主重试已创建的当前群时，服务端返回现有群资料。群主及去重后的初始成员计入容量，`memberLimit` 默认 1000，范围为 1 至 1000。`membersCanInvite` 与 `historyEnabled` 默认开启。

## 入群与管理

`join` 使用 `kind: "join"` 为本人申请，或使用 `kind: "invite"` 邀请其他用户。请求留言最多 512 字节。同一次操作使用相同 `requestId`。响应状态为 `pending`、`accepted` 或 `rejected`。审批决策取 `accepted` 或 `rejected`。

| `joinMode` | 入群申请 | 群主或管理员邀请 | 获准邀请的普通成员 |
| --- | --- | --- | --- |
| 0，默认 | 审批 | 直接入群 | 直接入群 |
| 1 | 审批 | 直接入群 | 审批 |
| 2 | 直接入群 | 直接入群 | 直接入群 |

群主和管理员可修改资料、审批和切换群禁言。群主可提升或撤销管理员，禁言或移除管理员及普通成员，也可转让或解散群。管理员可禁言或移除普通成员。群主先转让才能退出。转让后，旧群主取得接任者此前的角色。群主和管理员可查看当前群的申请列表，申请执行者或对象可查看各自参与的记录。群解散后待审批记录保留，审批停止。

成员动作使用 `leave`、`remove`、`promote`、`demote`、`transfer_owner`、`mute` 或 `unmute`。退出时目标可省略或填写当前账号。个人禁言时长以秒计，范围为 1 至 2147483647。成员重入后继续沿用原有禁言期限。群禁言约束普通成员，管理员仍受个人禁言约束。

## 消息和同步

群 ID 即群会话 ID。当前成员沿用 [TLS 发送](../http/tls-message-send.md)，使用同一发送回执和 [WSS 消息序列](../gateway/websocket.md)。个人或群禁言约束新消息，仍在群内的发送者可重放已提交的同意图回执。

当前成员可查询群资料和成员。首次入群时，关闭历史记录会将当时已提交的序号保存为该成员的 `historyAfterSeq`，只允许读取此序号之后的消息。退群、移除和解散时记录成员可读的最后序号 `departedThroughSeq`，包括序号 `"0"`。离开后可继续读取已获授权的消息，群资料查询、新消息和媒体访问均要求当前成员权限。重入清除离开上限。关闭历史时，新起点更新到重入时的尾序号。开启历史时保留原起点。

各列表的 `page` 包含 `items` 和 `hasMore`。`limit` 默认 50，上限 100。成员页按 `userId` 排序，申请页按 `actorId`、`requestId` 排序。下一页使用上一页最后一项的完整键。

群变更按当前账号独立编号。首次读取传 `afterRevision: "0"`，随后使用最后一个已保存事件的 `revision`，同时跟踪响应的 `latestRevision`。修订和消息序号使用无前导零的十进制字符串。事件含群 ID，类型为 `group_changed`、`group_removed` 或 `request_changed`。变更携带本人的成员状态，申请事件只投递给申请执行者与对象。客户端收到群资料或成员变更事件后，可用当前会话重新读取群和成员列表。

错误响应含 `error.code`。业务输入无效为 400，权限不足为 403，群或记录不存在为 404，状态冲突或容量已满为 409，超出地址或账号预算为 429，暂不可用为 503。
