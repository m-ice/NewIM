# HarmonyOS 离线推送

使用 Push Kit V3 增加第三种离线通知供应商。投递复用现有队列、带版本的加密令牌登记和
通用同步提示。通知采用 IM 分类，点击参数只携带 `newim=sync`，客户端按已有流程回源同步。
使用 IM 分类的生产项目须具备相应权益。 [通知接入指南](https://developer.huawei.com/consumer/cn/doc/harmonyos-guides/push-send-alert/)

服务端读取 `NEWIM_PUSH_KIT_CREDENTIALS_FILE` 指向的 AppGallery Connect 服务账号文件。
用固定受众签发 PS256 JWT，并直连对应项目的固定 HTTPS V3 端点。签名使用 Go 标准库。
供应商明确返回 JWT 过期时，清除使用中的 JWT 并交由队列重试；供应商明确指出当前目标的
令牌格式无效时，现有版本校验才允许退役登记。项目、权限、包名错误保留已登记令牌。
[JWT 鉴权](https://developer.huawei.com/consumer/cn/doc/harmonyos-guides/push-jwt-token/) [供应商答复](https://developer.huawei.com/consumer/en/doc/harmonyos-references/push-scenariozed-api-response/)

迁移 023 保留既有登记、队列和租约记录。部署时先迁移数据库，再升级并配置共享队列的全部
投递进程，再升级已启用推送登记的网关，最后发布鸿蒙客户端。

`go test -race ./server/push ./server/cmd/newim-server ./server/cmd/newim-gateway` 验证 TLS
请求、签名和响应分类，以及服务配置。`python3 -B infra/db/pushkit_suite.py` 用隔离数据库
验证升级重放、旧数据和真实 TLS 网关的登记、轮换、退订及队列。设备收到真实通知及点击补拉
由使用具备 IM 权益的测试项目和设备验证。
