# NewIM architecture and build provenance

The current implementation contains Go server application/storage packages for
PostgreSQL message persistence, auth/session, conversation and message sync, and
internal media metadata; Go/Rust protocol codecs; a Rust SDK outbox state machine;
and a native SQLite adapter. Build identity lets support and future packaging tools
identify artifacts without fabricating unknown source metadata.

The following remain future work: a public network server or UI, an HTTP/WebSocket
gateway, push delivery, complete credential verification and multi-device login or
kick policy, reconnect/lifecycle integration, retention/moderation/account erasure,
and platform wrappers. This document does not claim those capabilities.

## Module boundaries

| Module | Dependency contract |
|---|---|
| `core/domain` (planned) | Go server domain rules, independent of HTTP, wire DTOs and SQL. |
| `core/protocol` | Versioned language-neutral wire schemas/fixtures and separate Go/Rust codecs; no UI or database authority. Media v1 is closed metadata-only. |
| `server/auth/session` | Policy-neutral token issuance, authentication, and revocation application logic over a store port. |
| `server/message` | Send validation, media preconditions, idempotent persistence orchestration, and persisted ACK construction. |
| `server/sync/*` | Internal bounded conversation bootstrap/delta and message delta read services; trusted principals are supplied by their callers. |
| `server/media` | Protocol-neutral media application rules and ports for grants, metadata, validation, and private download authorization. |
| `server/storage/*` | PostgreSQL adapters for message, auth/session, sync, and media metadata; no wire or UI authority. |
| `server` | Application services depend on domain rules and ports; transport/storage adapters implement those ports. The only transport executable today is the local `newim-buildinfo` diagnostic. |
| `sdk/core` | Rust platform-neutral contracts and the host-driven outbox state machine; no Go runtime, SQLite, browser API, or UI dependency. |
| `sdk/storage/sqlite` | Native SQLite LocalStore adapter with migrations, recovery, pending-outbox CAS, and bounded receipts. |
| Platform wrappers (planned) | Translate host networking, lifecycle, storage and FFI concerns into SDK contracts; message semantics stay in core. |
| Premium extensions (planned) | Depend on OSS extension interfaces; OSS core never imports premium modules. |

Server version, SDK version, protocol version and database schema version have
separate lifecycles. Only server/SDK development versions exist today. A build
revision label is informational and provides neither authenticity nor protocol
negotiation. The [build ADR](adr/0002-language-toolchain.md) defines exact unknown,
validation and error behavior.

## Implemented persistence and recovery boundaries

PostgreSQL is the server truth for messages, auth/session state, and sync
projections. The message send path requires a trusted `session.ConnectionIdentity`,
authorizes membership inside a bounded READ COMMITTED transaction, allocates the
server `conversationSeq` from the conversation `last_seq`, and commits the message,
latest pointer, and transactional outbox row atomically through
`newim.persist_message`. `server/message` returns `SERVER_PERSISTED` only after
commit; an equal retry returns the original identity, while unequal intent for the
same trusted `(senderId, clientMsgId)` fails. The outbox is durable side-effect
intent; no dispatcher or exactly-once delivery claim is made. See
[ADR 0009](adr/0009-message-send-transaction.md).

`server/auth/session` implements opaque token issue/authenticate/revoke operations
with persisted session/token bindings and revocation checks. `server/sync/*`
implements bounded, resumable internal bootstrap, conversation delta, and message
delta reads using server-assigned sequence and pagination contracts; this is not a
public sync endpoint. PostgreSQL media persistence stores metadata, complete
identity bindings, and SHA-256 grant digests. Binary objects are stored by
`server/media/localfs` through the object-store port. Neither persistence boundary
stores signed/object URLs. Upload authorization precedes grant generation, message
persistence requires a ready/owned/conversation-bound asset, and private download
authorization precedes signer invocation.

On clients, `sdk/core` owns the protocol-neutral outbox state machine and
`sdk/storage/sqlite` provides the native SQLite adapter with atomic batch writes,
generation fencing, recovery, and pending CAS. The recovery contract treats
PostgreSQL persistence plus sync as truth; WebSocket remains the intended realtime
path, but no public WebSocket transport is implemented. Push is future notification
infrastructure, not the message truth source. See
[ADR 0008](adr/0008-auth-session-core.md),
[ADR 0007](adr/0007-internal-conversation-sync.md),
[ADR 0011](adr/0011-media-credential-metadata.md),
[ADR 0005](adr/0005-local-store.md), and
[ADR 0010](adr/0010-sdk-outbox-send-state.md).

## Build and CI

One root Go module and one Cargo workspace keep native build commands usable.
`make build` checks exact versions, builds the diagnostic utility and SDK, and
compiles the SDK for wasm. `make check` performs non-mutating formatting checks,
`go vet`, `go test -race -shuffle=on -count=1 ./...`, Clippy, behavioral tests
in both languages, and the `docs-check` documentation guard. The guard validates
known stale claims, current dependency/persistence markers, and local link
targets; it does not replace independent architecture review.

The workflow uses `ubuntu-24.04`, an explicit OS-family label whose hosted image
continues to change. Exact language versions and action commits are fixed;
this is not a claim of a byte-identical whole operating-system image or a
reproducible production package. Local macOS and hosted Linux results must be
reported separately. Native and wasm compiler checks do not validate platform
FFI, browser execution, lifecycle recovery or device packaging.

## Third-party provenance

The dependency records verified on 2026-09-21 are
[conversation sync](dependencies/conversation-sync.md), which pins `pgx` v5.11.0
and its selected Go module graph; [protocol v1](dependencies/protocol-v1.md),
which pins `serde_json` 1.0.151; and [native local store](dependencies/local-store.md),
which pins `rusqlite` 0.40.2, `libsqlite3-sys` 0.38.2, and SQLite 3.53.4. Exact
third-party attribution and license text is in
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md). Those records are provenance
for the selected dependencies; this document makes no new license commitment.
Toolchain sources are [Go 1.27.1 downloads](https://go.dev/dl/) and the
[Rust 1.98.1 release](https://blog.rust-lang.org/2026/09/03/Rust-1.98.1/). Go
uses [BSD-3-Clause](https://go.dev/LICENSE); Rust's main licensing is
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
