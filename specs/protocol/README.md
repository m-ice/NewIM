# 持久消息协议 v1 / Persisted messages

提供 Go 与 Rust 的 `Decode/decode`、`Encode/encode`、`Validate/validate`，用于服务端和 SDK 共用的消息契约。支持文本 v1 与纯元数据 media v1；未知消息类型或未来 schema 保留 payload 并报告 unsupported。解码成功仅表示格式有效，不代表消息已持久化、已送达或发送者有权限。

## 契约

消息必填字段：`protocolVersion`、`version`、`clientMsgId`、`serverMsgId`、`conversationId`、`conversationSeq`、`senderId`、`type`、`serverTime`、`payload`。协议版本当前为 1；schema version 为 1..2147483647 的正整数。ID 是 1..128 个 ASCII 字母、数字、下划线或连字符。type 符合 `[a-z][a-z0-9_]{0,63}`。序列和毫秒时间使用无前导零的十进制字符串，范围 0..9223372036854775807，避免 JavaScript 精度损失。序列 0 仅为可表示值，不定义服务端分配策略。

`payload` 为对象，文本 v1 的 `text` 必填且为 1..16384 个 UTF-8 字节；不裁剪或归一化文本。media v1 的 payload 是封闭元数据对象，精确字段、语法、长度、4 KiB 上限与 URL/二进制拒绝规则见 [media credential metadata](../media/credential-metadata.md)。未知 envelope 字段可丢弃；未知文本 payload 字段保留。未知类型/未来文本版本不执行已知文本校验，调用方必须检查 `Supported()/supported()`。未知 protocolVersion 拒绝。

整体输入上限 65536 字节；容器深度 32（含 envelope）；数字 token 上限 128 字节。拒绝重复键（含转义等价键）、非法 UTF-8、未配对 surrogate、尾随 JSON、缺失/null 必填字段和错误字段类型。payload 的数字 token 不经过浮点转换，1e9999 与超 2^53 整数可原样保留。编码也执行限制；对象键序、空白和合法转义不构成跨语言差异。

## 稳定错误

| Code | Meaning |
| --- | --- |
| MESSAGE_TOO_LARGE | 输入或输出超过整体字节限制 |
| PROTOCOL_INVALID_JSON | JSON 语法、Unicode、重复键或数字 token 违反约束 |
| PROTOCOL_NESTING_EXCEEDED | 超出容器深度 |
| PROTOCOL_INVALID_MESSAGE | 字段形状、类型、数值范围或已知 payload 不合法 |
| PROTOCOL_UNSUPPORTED_VERSION | envelope 形状合法但协议版本不支持 |

优先级：整体大小 → 原始 JSON/深度 → 完整 envelope 必填/类型/范围 → 协议版本 → 已知 payload。错误不包含消息内容。

## 验证与使用

`make protocol-golden`、`make protocol-unknown-fields`、`make protocol-unknown-type`、`make protocol-limits` 每项都运行 Go/Rust 对同一 fixtures/cases.json 的实际测试；`make check` 运行全部检查，`make build` 构建宿主和 wasm。Cargo.lock 固定依赖，首次构建需要下载其列出的 crates；依赖已缓存时可单独使用 Cargo 的 `--offline`。

Go import: `github.com/m-ice/NewIM/core/protocol/go`（包名 protocol）；Rust workspace crate: `newim-protocol`。具体公有类型见各自源码。Go decode 后持有独立 payload 副本；Rust持有 Box<RawValue>，均无网络、数据库或 UI 依赖。完整决策见 ../../docs/adr/0003-protocol-v1.md。本阶段不实现鉴权、持久化 ACK、WebSocket 收发、离线同步或消息渲染。

Error tie-breaking: after checking overall byte size and UTF-8, scan JSON left-to-right. An already encountered excessive container depth takes precedence over a later malformed Unicode escape. Encode first rejects a raw public-field byte sum that already exceeds the wire limit, then validates the complete output envelope with Decode rules; it must not check envelope shape before raw payload errors.

Additive send/ACK/error framing and pure intent/correlation helpers are documented in [send-ack.md](send-ack.md). Media v1 send/ACK compatibility is documented in [media credential metadata](../media/credential-metadata.md). These codecs do not implement a server commit, authentication, WebSocket service or SDK send state machine.
