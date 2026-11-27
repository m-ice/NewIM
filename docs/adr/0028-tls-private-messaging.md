# 0028 · 客户端 TLS 私聊收发

`newim-gateway` 通过 `NEWIM_TLS_PRIVATE_MESSAGES=1` 接入原生客户端的直接会话创建、文本发送及方向性拉黑。网关复用同一会话认证服务、私聊控制器和持久化发送服务，发送存储与鉴权、增量读取使用同一 PostgreSQL 身份守卫。启用时要求一致部署 `NEWIM_PRIVATE_MESSAGE_POLICY=1`。

客户端从可信账号标识创建直接会话，随后用返回的会话标识发送和订阅。发送沿用现有请求及持久回执：事务提交后才能成功，重试使用原客户端消息标识。公开发送在事务内读取可信会话类型及当前成员资格，再进入既有重试和方向性拉黑分支。实时接收继续使用提交序列。

四个私聊 HTTP 入口使用独立的直接地址及会话账号预算。每进程每分钟默认分别为 120 和 60，可用 `NEWIM_TLS_MESSAGE_IP_ATTEMPTS_PER_MINUTE` 和 `NEWIM_TLS_MESSAGE_ACCOUNT_ATTEMPTS_PER_MINUTE` 调整。

[接入规范](../../specs/http/tls-message-send.md)
