# 会话投影回填

迁移 014 增加按账号保存的源就绪状态和回填位置。开启 `NEWIM_CONVERSATION_PROJECTION=1` 后，生产写入将首次建私聊、建群、新消息、入群和重入、退群、移除和解散同步提交到 [会话投影](conversation-projection.md)。新预配账号从就绪的空投影开始。相同已提交消息的重试沿用原回执和修订。

投影按成员历史范围记录最新可见摘要。尚无可见消息的成员收到零摘要序号和空指针。离开和解散为已投影会话写入移除记录。读取遇到历史授权已经收紧的旧摘要时，返回 `SYNC_CURSOR_EXPIRED`，客户端重新开始 bootstrap。

## 上线顺序

1. 应用迁移 014，升级回环及 TLS 写进程，并排空旧版本。
2. 在所有承载相关写入的进程开启投影。通过实际源写入验证投影落库。
3. 对旧账号运行回填，最后一页提交时自动标记该账号就绪。要求源就绪的读取对待回填账号返回 `SYNC_NOT_READY`。

回填在每批锁住候选会话后重新检查现有成员和投影键，包含已离开且曾有投影的会话。进度同摘要或移除记录一起提交。重启后从保存的位置继续。

## 命令

通过 `NEWIM_AUTH_DSN` 配置数据库。TCP 连接须验证数据库 TLS。使用本地套接字时显式设置 `NEWIM_AUTH_ALLOW_LOCAL_SOCKET=1`。

```sh
newim-sync-backfill run --limit 100
newim-sync-backfill step --user example_user --limit 20
newim-sync-backfill reset --user example_user
```

`run` 按固定的账号目录页继续所有待回填账号，全部就绪时输出 `SYNC_SOURCE_COMPLETE`。`step` 提交一个账号的一批数据，输出 `SYNC_SOURCE_PENDING` 或 `SYNC_SOURCE_READY`。批次上限为 100。失败输出稳定错误码，可继续执行。

回退到旧写进程前先停止对外读取，并使用 `reset` 对受影响的账号推进代际，使原游标过期。数据和持久修订保留。全量回填和重置互斥，运行期间重置返回 `SYNC_NOT_READY`。重新启用双写后运行回填再恢复读取。
