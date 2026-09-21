# NewIM architecture and build provenance

The current implementation contains a Go server build-identity library and
diagnostic executable, and a Rust SDK build-identity library. They have no network,
message, persistence or UI behavior. They let support and future packaging tools
identify artifacts without fabricating unknown source metadata.

## Module boundaries

| Module | Dependency contract |
|---|---|
| `core/domain` (planned) | Go server domain rules, independent of HTTP, wire DTOs and SQL. |
| `core/protocol` (planned) | Versioned language-neutral wire schemas/fixtures and separate Go/Rust codecs; no UI or database authority. |
| `server` | Application services depend on domain rules and ports; transport/storage adapters implement those ports. Build diagnostics use only Go standard libraries. |
| `sdk/core` | Rust platform-neutral contracts; no Go runtime, platform storage, browser API or UI dependency. |
| Platform wrappers (planned) | Translate host networking, lifecycle, storage and FFI concerns into SDK contracts; message semantics stay in core. |
| Premium extensions (planned) | Depend on OSS extension interfaces; OSS core never imports premium modules. |

Server version, SDK version, protocol version and database schema version have
separate lifecycles. Only server/SDK development versions exist today. A build
revision label is informational and provides neither authenticity nor protocol
negotiation. The [build ADR](adr/0002-language-toolchain.md) defines exact unknown,
validation and error behavior.

The intended recovery design treats server persistence as the reliable-send
boundary and database/sync as recovery truth. WebSocket is realtime delivery;
push is a notification hint. Server-assigned conversation order and durable
transactional side effects belong to later domain, protocol and data modules.
These are architectural constraints, not implemented functionality.

## Build and CI

One root Go module and one Cargo workspace keep native build commands usable.
`make build` checks exact versions, builds the diagnostic utility and SDK, and
compiles the SDK for wasm. `make check` performs non-mutating formatting checks,
Go vet, Clippy and behavioral tests in both languages. No documentation-check
script substitutes for independent review of these boundaries.

The workflow uses `ubuntu-24.04`, an explicit OS-family label whose hosted image
continues to change. Exact language versions and action commits are fixed;
this is not a claim of a byte-identical whole operating-system image or a
reproducible production package. Local macOS and hosted Linux results must be
reported separately. Native and wasm compiler checks do not validate platform
FFI, browser execution, lifecycle recovery or device packaging.

## Third-party provenance

Checked 2026-09-21. Runtime source dependencies are Go/Rust standard libraries
only. No external Go module, Rust crate or copied vendor implementation is
included. Toolchain sources are [Go 1.27.1 downloads](https://go.dev/dl/)
and the [Rust 1.98.1 release](https://blog.rust-lang.org/2026/09/03/Rust-1.98.1/).
Go uses [BSD-3-Clause](https://go.dev/LICENSE); Rust's main licensing is
[MIT/Apache-2.0](https://rust-lang.org/policies/licenses/). Their distribution
notices include additional bundled components; a later binary release must
review actual compiler/standard-library notices, not assume a single root
license covers everything. Make is build-only tooling; no toolchain is vendored
or distributed by this repository.

| CI dependency | Fixed source | License and maintenance |
|---|---|---|
| `actions/checkout` v7.0.1 | [`3d3c42e5aac5ba805825da76410c181273ba90b1`](https://github.com/actions/checkout/commit/3d3c42e5aac5ba805825da76410c181273ba90b1) | [MIT](https://github.com/actions/checkout/blob/3d3c42e5aac5ba805825da76410c181273ba90b1/LICENSE); [release](https://github.com/actions/checkout/releases/tag/v7.0.1) includes security/dependency fixes. Maintainers restrict external contributions while continuing security updates and major fixes. |
| `actions/setup-go` v7.0.0 | [`b7ad1dad31e06c5925ef5d2fc7ad053ef454303e`](https://github.com/actions/setup-go/commit/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e) | [MIT](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/LICENSE); [release](https://github.com/actions/setup-go/releases/tag/v7.0.0) updates dependencies and ESM support. |

These actions are used only to fetch source and select the Go toolchain in CI.
Their exact-commit LICENSE, action.yml and package.json files were read from the
official repositories. Both use Node 24 and require Actions runner 2.327.1 or
later. The checked JavaScript entry points total approximately 1.45 MB for
checkout and 6.96 MB for setup-go's setup/post steps; this is CI cost, not SDK
size. They bundle actions-toolkit dependencies; no npm installation or copying
of action source into NewIM occurs. Redistribution of an action or CI image
would require reviewing its complete bundled notices separately.

The [checkout security policy](https://github.com/actions/checkout/security/policy)
and [setup-go security policy](https://github.com/actions/setup-go/security/policy)
direct reports to GitHub's security program. Their public advisory pages did not
list a published advisory during the check; this is not a vulnerability-free
claim. The workflow limits permissions to `contents: read`, disables checkout
credential persistence and caches, and runs no deployment, publication or secret
injection. Pull requests use the `pull_request` event. Full action commits follow
[GitHub's secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use).

No project distribution license is chosen by this foundation. Cargo publication
is disabled. Product release licensing, notices, packaging and stable release
gates require their own review before distributing binaries or SDK packages.
