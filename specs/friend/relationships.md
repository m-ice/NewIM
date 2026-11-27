# 好友申请与关系接口

以下请求使用 TLS 和会话 Bearer。成功响应包含 `version: 1`，错误码位于 `error.code`。各入口均使用 POST 和 JSON。

| 路径 | 参数 | 结果 |
| --- | --- | --- |
| `/api/v1/friends/applications` | `requestId`、`peerId`，可选 `message` | 返回申请，同一标识可重试 |
| `/api/v1/friends/applications/respond` | `applicantId`、`requestId`、`decision` | 收件方用 `accept` 或 `reject` 响应 |
| `/api/v1/friends/applications/list` | `direction`、可选 `afterApplicantId`、`afterRequestId` 和 `limit` | 查询 `incoming` 或 `outgoing` 申请 |
| `/api/v1/friends/list` | 可选 `afterPeerId`、`limit` 和 `excludeBlocked` | 查询当前账号拥有的好友与备注 |
| `/api/v1/friends/remark` | `peerId`、`remark` | 修改当前账号的备注，空字符串清空 |
| `/api/v1/friends/remove` | `peerId` | 删除当前账号的一条好友记录，可重试 |
| `/api/v1/friends/changes` | `afterRevision`、可选 `limit` | 读取当前账号已提交的申请、好友更新和删除事件 |

标识沿用现有账号标识格式。备注和申请留言最长 512 个 UTF-8 字节。列表按账号标识和申请标识排序，页大小默认 50，上限 100。变更修订采用非负十进制字符串，首次查询使用 `"0"`。每页返回当前修订及是否还有下一页。客户端在本地提交变更后保存已处理的修订。

接受申请将状态更新为 `accepted`，成立双方各自的好友记录。拒绝更新为 `rejected`。已响应的申请保留原结果。申请的发起者及收件方可读取申请信息，处理权限属于收件方。好友备注和单向删除事件只供所属账号读取。

请求体上限为 16 KiB。已有待处理申请返回 `FRIEND_APPLICATION_PENDING`，申请标识对应的内容或处理结果冲突时返回 `FRIEND_APPLICATION_CONFLICT`。分页使用上一页尾部的标识继续，申请列表须一并传两个游标字段。初次本地同步使用未筛选的完整好友列表，再根据账号修订消费变更。
