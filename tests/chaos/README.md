# 消息可靠性验收

在产品仓库根目录运行下列目标。TLS、WSS 和 PostgreSQL 用例采用独立测试进程与临时容器，
SQLite 用例采用原生 SDK 桥和临时数据库。故障仅作用于这些夹具。运行依赖和镜像锁定方式见
[数据库验收](../../infra/db/README.md)。

| 命令 | 验收 |
| --- | --- |
| `make reliability-transport` | 首条消息经 WSS 到达后终止网关。离线期间提交第二条消息，发送端丢失回执。更换网关后，WSS 从首条位点续读，同意图重放返回相同回执。真实 PostgreSQL 行锁等待超时返回可重试错误，数据库仍只有前两条消息。释放锁后第三条提交，两台设备看见相同序列。 |
| `make message-sdk-sync-check message-sdk-sync-convergence message-sdk-sync-recovery sdk-outbox-restart` | 实际 TLS 帧经过乱序、重放、缺口与进程退出，恢复到连续序列。待发送意图随 SQLite 重新打开而恢复。 |
| `make message-recovery tls-message-recovery` | 数据库重启、提交前中断和备份恢复后，检查消息、序号、摘要、outbox 和原回执。 |
| `make reliability-notifications push-recovery webhook-recovery` | 本地通知接收器真实超时后，原推送任务与 Webhook 投递继续重试。核对持久任务状态、Webhook 投递标识、次数和已提交消息。 |

当前部署以 PostgreSQL 支撑消息、在线租约与持久队列。项目接入 Redis 运行时后，需要在对应
部署验收中加入 Redis 延迟与不可用测试。

套件打印实际执行的测试名和记录目录。重试通过条件包括已提交消息和回执身份稳定、恢复序列连续、
到期任务保持同一投递身份，以及超时事务没有推进消息序号或产生额外投递。
