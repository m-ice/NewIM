# 离线推送

消息成功由原消息事务确定。推送服务从事务生成的 outbox 读取已提交事件，分别检查每个
已准入设备。目标会话存在有效在线订阅时由 WSS 送达。失联租约在 45 秒内到期，正常断开
时释放对应连接的记录。静音、已读、已隐藏和权限失效会取消尚未发送的通知。

消息、账号与设备组合在队列中最多一条记录。供应商超时后可重试同一队列项，供应商已接收
但服务端尚未记录时可能再次收到请求。通知仅提示打开应用，APNs 和 FCM Android 使用固定
合并键。通知保留期为 15 分钟，从该次供应商请求计。通知到达和点击都不能授权访问消息。

## 设备接口

网关在已验证 TLS 与已准入的 Bearer 会话上提供 `/api/v1/push/registration`。
`PUT` 接受 `{"provider":"fcm","token":"..."}`，返回 `{"version":"1"}`。
`DELETE` 接受 `{"provider":"fcm","version":"1"}`，完成返回 204。
支持 `apns`、`fcm` 和 `pushkit`。同一设备在相同会话内重试同一令牌保留版本。
迟到的删除请求或供应商失效响应只处理原版本。HTTP 响应与日志只保留固定状态和错误码。

供应商负载使用 `newim=sync` 表示唤起。SDK 宿主把标记交给 `push::open_notification`，
使用当前连接票据取得权威目录和消息。退出或认证恢复期间的通知不会推进世代，也不会注入
消息正文、未读、会话身份或补拉位点。

## 部署

APNs 和 FCM 需要数据库迁移 022。启用 Push Kit 时，先应用迁移 023，再升级并配置共享
队列的全部 worker，然后升级已启用推送登记的网关，最后发布鸿蒙客户端。

| 位置 | 配置 |
| --- | --- |
| 网关 | `NEWIM_GATEWAY_PUSH_PRESENCE=1` 记录订阅租约 |
| 网关 | `NEWIM_PUSH_REGISTRATION_HTTP=1` 开放设备令牌接口 |
| 网关与 worker | `NEWIM_PUSH_KEYS_FILE` 指向 0400 或 0600 的令牌密钥 JSON，格式为 `{"active":"key1","keys":{"key1":"<32 字节密钥的 Base64>"}}` |
| 服务端 | `NEWIM_PUSH_WORKER=1`，使用 `NEWIM_PUSH_DSN` 或同一数据库的 `NEWIM_AUTH_DSN` |
| 服务端 | 本地 Unix socket 需要 `NEWIM_PUSH_ALLOW_LOCAL_SOCKET=1` |
| APNs | `NEWIM_PUSH_APNS_KEY_FILE`、`NEWIM_PUSH_APNS_KEY_ID`、`NEWIM_PUSH_APNS_TEAM_ID`、`NEWIM_PUSH_APNS_TOPIC`。沙箱设 `NEWIM_PUSH_APNS_SANDBOX=1` |
| FCM | `NEWIM_PUSH_FCM_CREDENTIALS_FILE` 指向服务账号凭据文件 |
| Push Kit | `NEWIM_PUSH_KIT_CREDENTIALS_FILE` 指向 0400 或 0600 的服务账号凭据文件 |

同一数据库上的 worker 使用相同的供应商配置和可用密钥。首次启用只处理启用时间之后的
outbox 事件。轮换密钥时保留旧 key，直到相应设备重新登记。worker 日志按窗口输出固定
结果计数，存储故障按分钟限频记录。

`make push-delivery-check push-token-check push-recovery push-sdk-check` 验证真实 PostgreSQL、
网关、SDK 与本地 TLS 供应商端点。供应商控制台配置和真机收取由各客户端适配验证。

## 鸿蒙通知

鸿蒙安装使用 `pushkit` 通道走现有版本化登记。通知只携带通用提示和点击后的同步标记。
服务端部署和设备验收见 [HarmonyOS 离线推送决策](../../docs/adr/0051-harmony-push-service.md)。
