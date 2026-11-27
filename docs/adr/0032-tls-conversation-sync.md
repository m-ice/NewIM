# 0032 · TLS 会话同步读取

现有 TLS 网关通过 `POST /api/v1/sync/bootstrap` 和 `POST /api/v1/sync/delta` 提供会话读取。网关从当前 Bearer 会话取得账号，经现有按账号的投影读取。服务连接共用数据库身份保护并要求投影源就绪。

初始 bootstrap 接受可选 `limit`。初始 delta 接受 `checkpoint` 和可选 `limit`。两种续页请求均只接受 `cursor`。JSON 请求至多 4096 字节。直接写出已有 `Page`，沿用签名游标、检查点、十进制字符串序号和 64 KiB 响应预算。旧游标与未就绪分别返回 `409 SYNC_CURSOR_EXPIRED` 和 `503 SYNC_NOT_READY`。

集群实例从受限文件加载同一签名密钥环。发行凭证与游标密钥共用绝对路径、文件类型、所有权、权限及 ACL 检查。换钥先给全部实例部署新旧验证密钥，再滚动变更签名密钥。最后一次使用旧密钥签名后，至少保留 24 小时。

部署在投影双写和回填验证后开启 `NEWIM_CONVERSATION_SYNC=1`。同时承载会话写入的网关开启 `NEWIM_CONVERSATION_PROJECTION=1`。回退关闭会话读开关，维护持久投影和填充进度。
