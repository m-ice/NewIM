# Media credential and metadata flow

此规范定义 NIM-MED-001 的内部媒体契约；它不定义公开 HTTP/WS 接口。

## Media v1 payload

已登记的持久化/发送 schema 为 `type=media`, `version=1`。payload 必须是恰好包含
以下字段的 JSON 对象：

```json
{"mediaKey":"Abc_123-xyz","kind":"image","contentType":"image/jpeg","size":"12345","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
```

- `mediaKey`: `[A-Za-z0-9_-]{1,128}`，只能由服务端生成。
- `kind`: `image | video | audio | file`。
- `contentType`: 小写 ASCII MIME，无参数，精确匹配
  `^[a-z0-9][a-z0-9!#$&^_.+-]{0,62}/[a-z0-9][a-z0-9!#$&^_.+-]{0,63}$`，最多 128 字节。
- `size`: 精确匹配 `^[1-9][0-9]{0,8}$`，范围 1..104857600。
- `sha256`: 64 个小写十六进制字符。
- 完整编码 payload 最多 4096 字节。
- URL、signed URL、二进制/base64/data、未来字段或任何额外字段均拒绝。
- 未知消息类型和未知 schema version 保持 opaque；文本 v1 的宽松额外字段行为不变。

Go 与 Rust 共享 `tests/compatibility/media/fixtures/cases.json` 和对应兼容性测试。
codec 成功只证明形状合法，不证明资产 ready、消息已持久化或发送者有权发送。

## Upload grants

`BeginUpload` 只接受可信 `session.ConnectionIdentity` 和可信宿主提供的
kind/contentType/size/SHA-256 意向。它先验证当前成员关系以及完整
user/device/session/connection/token 绑定；失败必须在生成 key、grant、DB 行或
对象之前返回 `MEDIA_UNAUTHORIZED`。

成功前必须显式校验完整 user/device/session/connection/token 身份；成功后服务端生成：

- 随机 `mediaKey`；
- 32 位小写十六进制 `grantId`；
- 32 个随机字节并编码为 43 字符无填充 base64url secret；
- raw token `grantId.secret`，只返回给宿主一次；
- DB 仅保存完整 raw token 的 SHA-256 digest。

默认上传 TTL 为 120 秒，硬上限 300 秒。`now >= expiresAt` 时 pending 新完成返回
`MEDIA_EXPIRED`。相同完整身份、相同 raw digest、相同 bytes/metadata 的 ready replay
在过期后仍成功；cross-identity、 revoked session/device/token binding 或无权限成员
在过期检查和 ready replay 之前返回 `MEDIA_UNAUTHORIZED`。冲突 bytes 返回
`MEDIA_CONFLICT`。

稳定 token 格式错误必须在存储查询前返回 `MEDIA_INVALID_TOKEN`；未知 grant ID 或
digest 不匹配返回 `MEDIA_NOT_FOUND`。错误文本不得包含 raw token、digest、mediaKey
或对象内容。

## Object store and completion

`CompleteUpload` 在过期或 ready replay 之前比较全部五个 trusted identity 字段和可选 Original media intent；随后通过 provider-neutral object store 写入不可变对象。adapter 必须：

- 只允许服务器生成的 mediaKey 作为对象名，拒绝绝对路径、`..`、分隔符和 symlink
  组件；
- 使用独占临时文件、fsync 文件、no-replace 发布并 fsync 父目录；
- 禁止普通可覆盖 rename；
- 已有对象必须精确 size + SHA-256 比较；不同 bytes 返回冲突；
- 不实现 retention/deletion/cleanup。

对象成功写入但 metadata transaction 失败时，不得产生 ready asset；孤儿对象风险是
显式、已知的，并在本任务中不清理。重复提交相同对象和 metadata 是幂等成功；不同
对象或 hash 不替换原资产。

## Database

`infra/db/migrations/005_media_assets.sql` 是唯一新增的媒体表：
`newim.im_media_assets`。它保存 `media_key`、完整 trusted identity 字段、conversation、
kind/contentType/size/SHA-256、grant ID/digest、expiry、`completed_at` 和 closed
`pending/ready` state。具体 FK 绑定 membership、device、session 和 auth token/session。
表中不得存在 signed URL、object URL、public ACL 或 list capability。媒体消息 payload
仍只保存 mediaKey 和封闭 metadata，不保存 URL 或 bytes。

## Private download

`ResolvePrivateDownload` 先读取 ready asset，再验证当前 membership、调用方全部五个可信身份字段和持久资产全部五个身份字段，
全部成功后最后调用注入 signer。默认 60 秒，最大 300 秒。未知、pending、非成员或
身份不匹配必须在 signer 之前返回稳定错误；URL 不得进入 PostgreSQL、日志、trace 或
metrics。

## Message persistence seam

`server/message` 只对 media v1 调用 `MediaValidator`，且调用发生在持久化事务之前。
validator 要求资产 ready、owner 等于可信 sender、conversation 精确匹配，并逐字段
比较 `mediaKey`、`kind`、`contentType`、declared/actual `size` 和 `sha256`。任一
不匹配都返回发送授权错误，且 message/pending/outbox 行数保持不变。文本路径和既有
send/ACK 语义不改变。

## Acceptance commands

```sh
make media-protocol
make media-db
make media-security
make media-authz
make media-check
make build check db-schema db-migrations db-sequence db-repair
make sync-check auth-check auth-recovery auth-policy message-check message-recovery message-errors
```

NIM-MED-001 不实现流量限速、审核、保留/删除/清理、账户删除、premium/license 或云
对象 SDK。DEC-003 与 SEC-001 的后续决策/边界仍保持独立。
