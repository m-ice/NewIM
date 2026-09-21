# NewIM

NewIM is an independently developed messaging platform for 澜遇科技. The current
foundation provides server and SDK build-identity diagnostics, strict Go/Rust
message protocol v1 codecs, and shared build/test entry points. Message transport, persistence, sync, platform adapters and UI are
not implemented yet.

## Build and test

Install Go **1.27.1**, Rust/Cargo **1.98.1**, rustfmt, Clippy, Make and a native C
linker. Rust uses edition 2024. The wasm standard library is also required:

```sh
rustup toolchain install 1.98.1 --profile minimal --component rustfmt,clippy --target wasm32-unknown-unknown --no-self-update
make build
make check
```

The commands reject other Go/Rust/Cargo patch versions. Build compiles all Go
packages and the `build/newim-buildinfo` utility, the native Rust SDK library, and
the SDK and protocol libraries for wasm. Check runs formatting, Go vet, Clippy,
behavioral unit tests and the shared protocol corpus. Cargo uses the committed
lockfile; the protocol crate uses serde_json with its locked transitive dependencies.
Go uses only the standard library.

The GitHub workflow uses these same commands on Ubuntu 24.04. A successful local
run does not establish that a hosted CI job ran. wasm compilation establishes
compiler compatibility, not browser behavior or an exported JavaScript API.

## Build identity

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

Server and SDK versions are independently maintained at `0.1.0-dev`. The message protocol is version 1;
database schema versions remain undefined until their own contracts exist.
See [architecture](docs/architecture.md), the [build ADR](docs/adr/0002-language-toolchain.md)
and [contribution guide](CONTRIBUTING.md).

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

已提供 Go/Rust 持久消息编解码和验证：文本 v1、未知类型保留、序列十进制字符串、严格 JSON/Unicode/大小边界，以及两端共享兼容测试。见 [协议说明](specs/protocol/README.md) 与 [决策](docs/adr/0003-protocol-v1.md)。这项库能力不代表服务端消息持久化或客户端聊天界面已经实现。

运行 `make check` 进行完整检查；四组兼容测试也可分别使用 `make protocol-golden`、`make protocol-unknown-fields`、`make protocol-unknown-type`、`make protocol-limits`。新增 Rust 解析依赖已固定在 Cargo.lock，首次构建会下载锁定依赖；缓存具备后可以使用 Cargo 的 `--offline`。第三方来源和许可见 [依赖记录](docs/dependencies/protocol-v1.md)。
