<img src="https://github.com/m-ice/NewIM/blob/main/raw/main/Resources/banner.png?raw=true"/>
# NewIM SDK

Rust SDK Core 维护连接、同步、出站队列和权威回执。平台运行系统网络、凭据和生命周期
适配，并将已验证的网络完成交回 SDK。Core 支持原生与 `wasm32-unknown-unknown` 构建。
原生宿主可以通过共享 C 桥访问现有 Core 和 SQLite 存储。

## 共享原生接口

`sdk/native` 提供静态链接库及 [C 头文件](native/include/newim_native.h)。
可通过固定的 SQLite 构建入口生成原生库。

```sh
python3 sdk/storage/sqlite/engine.py run cargo build -p newim-sdk-native --locked --offline
make native-sdk-abi-check native-sdk-bridge-integration
```

C 链接测试覆盖缓冲区、关闭和账号隔离。端到端测试通过同一个 C ABI，在隔离的 PostgreSQL、
实际 TLS 网关和原生 SQLite 上验证会话、持久回执、WSS、补拉及通知刷新。当前验收是共享桥的
宿主测试。移动系统生命周期、系统推送和各端打包属于对应平台适配任务。

[原生 ABI](../specs/sdk/native-abi.md)

## Client send outbox

`sdk/core` now exposes the protocol-neutral outbox/send state machine, bounded version-2 pending
envelope, retry policy and exact persisted-ACK resolution contract. Platform adapters must
validate wire frames and map them to `SendIntent`, `PersistedAck`, `SendFailure` and
`SendContext`; core does not parse JSON, own a socket or import SQLite.

The additive `PendingMutationStore` port provides revision-CAS pending update/removal without
changing `LocalStore` or exhaustive `Action`. The public `rebind_connection` recovery action lets
the host explicitly bind a non-terminal pending record to a new connection generation while
preserving intent, attempt/deadline and retry-cap fields; terminal/auth states remain on the
existing resume/remove paths. `remove_pending` is reserved for explicit terminal or
auth-recovery dismissal. The native SQLite adapter implements this port without schema
migrations and does not parse pending bytes.

Run the registered real-binary gates:

```sh
make sdk-outbox-restart
make sdk-outbox-retry
make sdk-outbox-ack
make sdk-outbox-terminal
make sdk-outbox-check
```

See [ADR 0010](../docs/adr/0010-sdk-outbox-send-state.md) and
[the outbox specification](../specs/sdk/outbox.md).
