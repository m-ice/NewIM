# NewIM

NewIM is an independently developed messaging platform for 澜遇科技. The current
foundation includes Go/Rust message and send/ACK/error protocol v1 codecs,
PostgreSQL schema, atomic message/outbox persistence primitives, a durable
internal conversation-sync projection and policy-neutral opaque auth-session/token
primitives, plus a portable SDK local-store contract with a native SQLite adapter.
Storage has real migration, idempotency, pagination, query-plan, concurrency,
rollback and recovery tests. User credential verification, HTTP/WS/gateway login,
refresh tokens, multi-device login/kick policy, network send/ACK and retry
orchestration, a complete multi-device sync service, platform adapters and UI
remain future work.
These libraries and SQL primitives do not yet form an end-to-end messaging service.

## Build and test

Install Go **1.27.1**, Rust/Cargo **1.98.1**, rustfmt, Clippy, Make, Python **3.11+**,
a native C compiler/linker and `ar`. Rust uses edition 2024. The current SQLite
adapter supports macOS/Linux; the wasm build includes only portable core/protocol
libraries. Prepare the pinned source, Cargo dependencies and native engine first:

```sh
rustup toolchain install 1.98.1 --profile minimal --component rustfmt,clippy --target wasm32-unknown-unknown --no-self-update
make store-prepare
cargo fetch --locked
make store-engine
make build
make check
make store-idempotency store-migrations store-maintenance store-recovery
```

`store-prepare` downloads and verifies the locked official SQLite source;
`cargo fetch --locked` prepares registry dependencies. Engine compilation and the
native build/check/store targets run offline and reject absent or mismatched
caches. They do not fall back to the host's SQLite. Warm runs reuse this checkout's
verified native engine. See the [SQLite guide](sdk/storage/sqlite/README.md) and
[dependency provenance](docs/dependencies/local-store.md).

The commands reject other Go/Rust/Cargo patch versions. Build compiles all Go
packages and `build/newim-buildinfo`, the native Rust workspace, and the portable
SDK/protocol libraries for wasm. Check runs formatting, Go vet, Clippy, behavioral
tests and the shared protocol corpus. Cargo uses the committed lockfile; Go uses
the locked module graph in `go.mod`/`go.sum`. wasm compilation establishes compiler
compatibility, not browser behavior or an exported JavaScript API.

For PostgreSQL suites, additionally install Docker with a running daemon:

```sh
make db-prepare
make db-schema db-migrations db-sequence db-repair
make sync-check
make auth-check auth-recovery auth-policy
```

Preparation verifies the locked official PostgreSQL image and pulls it if absent.
The suites use their own isolated containers and labeled volumes, with no host
ports or user database connection. Docker is required for these database suites,
not for the Go/Rust checks above. See the [database guide](infra/db/README.md) for
image verification, migration, backup and recovery contracts, and its
[dependency provenance](infra/db/DEPENDENCIES.md).

The GitHub workflow declares the same preparation, build, check, SQLite and
PostgreSQL suites, the six conversation-sync suites and the three auth suites on
Ubuntu 24.04. A successful local run does not establish that a hosted CI job ran.

## Build identity and schema versions

```sh
./build/newim-buildinfo
./build/newim-buildinfo --help
```

The utility emits JSON containing `serverVersion`, `goVersion` and optional
`sourceRevision` / `sourceModified`. Missing provenance stays absent; a source
archive does not imply a clean release. This utility does not start an IM server.

Rust callers use `newim_sdk_core::build_info::current()`. It reports the SDK's
independent package version and optional compile-time `NEWIM_BUILD_REVISION`.
Revision labels must be complete lowercase Git hashes; validation establishes
syntax only. Ordinary SDK builds leave that optional label unset. The SDK does
not infer clean state or wire compatibility from a revision.

Server and SDK versions are independently maintained at `0.1.0-dev`; the message
protocol is version 1. PostgreSQL has additive migration stages 001 through 004;
SQLite has its separate migration history. They are not interchangeable schemas or
previously released upgrade histories. See the
[relational storage ADR](docs/adr/0004-relational-storage.md),
[local-store ADR](docs/adr/0005-local-store.md),
[architecture](docs/architecture.md), [build ADR](docs/adr/0002-language-toolchain.md)
and [contribution guide](CONTRIBUTING.md). The internal projection contract is in
[conversation projection](specs/sync/conversation-projection.md), with reviewed
dependency provenance in [conversation sync dependencies](docs/dependencies/conversation-sync.md).

## macOS toolchain note

Rust 1.98.1's macOS arm64 `rust-lld` can fail to find its bundled
`libLLVM.dylib` when linking wasm modules. If that specific error occurs, use the
same toolchain's library directory for the current build process:

```sh
export DYLD_FALLBACK_LIBRARY_PATH="$(rustc --print sysroot)/lib:${DYLD_FALLBACK_LIBRARY_PATH:-/usr/local/lib:/usr/lib}"
make build
```

This does not modify compiler files or shell profiles. The present SDK is a Rust
library without a C/JavaScript ABI; platform packaging and runtime validation are
separate work. Linux CI does not require this macOS setting.

The project currently makes no software distribution license grant. The SDK
manifest disables publication; adding a license or publishing packages requires
the project's licensing decision. Third-party build tools retain their licenses.

## 消息协议 v1

已提供 Go/Rust 持久消息及发送请求、ACK、错误编解码和验证：文本 v1、未知类型保留、序列十进制字符串、严格 JSON/Unicode/大小边界，以及两端共享兼容测试。见 [协议说明](specs/protocol/README.md) 与 [决策](docs/adr/0003-protocol-v1.md)。协议编解码、数据库持久化原语与本地存储已有实现。内部会话同步投影提供 bootstrap/delta 分页、认证 HMAC 游标、成员隔离、事务写入和恢复原语；它不包含 HTTP/WS、登录鉴权、既有账户回填或客户端合并，完整多端同步、网络发送确认和聊天界面尚未完成。

完成上述准备后运行 `make check`。协议专项检查包括 `make protocol-golden protocol-unknown-fields protocol-unknown-type protocol-limits` 和 `make send-protocol-golden send-protocol-errors send-protocol-limits`。Rust 依赖版本固定在 Cargo.lock；首次通过 `cargo fetch --locked` 显式准备依赖。第三方来源和许可见 [协议依赖记录](docs/dependencies/protocol-v1.md) 与上述存储依赖记录。
