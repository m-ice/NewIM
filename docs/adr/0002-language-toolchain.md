# ADR 0002: Product build foundation and build identity

Status: accepted
Date: 2026-09-21

The decision is accepted following independent code, QA, security and licensing
content reviews. The final implementation candidate still requires review
coverage and test evidence bound to its exact commit before delivery.

NewIM needs separately identifiable server and SDK artifacts before its message,
storage and platform modules can be developed. Missing source metadata must remain
unknown: a build from an archive must not claim to be a clean Git release.

## Decision

The server uses Go 1.27.1 and one root Go module. The SDK core uses Rust 1.98.1,
edition 2024, in a Cargo workspace. Standard libraries are sufficient for this
foundation. The root Makefile aggregates the native tools; it does not install or
upgrade them. Cargo.lock is committed, and compilation uses locked, offline
dependency resolution. Go toolchain auto-selection is disabled. Version checks
reject a different Go, rustc or Cargo patch release before building.

The first capability is diagnostic build identity. The Go library and
`newim-buildinfo` command report the independent server version, compiler version,
and optional Git revision and modified state supplied by Go's embedded build
information. Unknown revision and modified state are omitted. Recognized metadata
keys must occur at most once. A supplied VCS must be `git`; revision/modified
fields without that VCS declaration are invalid. Supplied modified state accepts
only `true` or `false`.

The Rust library reports its Cargo package version and an optional compile-time
`NEWIM_BUILD_REVISION`. Both languages accept a source revision only when it is
exactly 40 or 64 lowercase ASCII hexadecimal characters. This checks syntax, not
authenticity or repository membership. An absent revision is unknown; a present
empty revision is invalid. Rust does not infer clean state from a revision.
Packagers must verify the source tree themselves before supplying this optional
label; ordinary builds leave it absent.

These APIs are diagnostic contracts, with Chinese and English comments on
non-obvious behavior. They do not negotiate protocol compatibility. Server and
SDK versions start independently at `0.1.0-dev`. No protocol or database schema
version is assigned while those contracts remain unimplemented.

Invalid metadata returns the stable local code `NIM_BUILD_INVALID_METADATA`.
The Go command writes one JSON object to stdout, or returns exit 2 for invalid
metadata/arguments and exit 1 for output failure. Diagnostics use fixed codes and
never echo supplied metadata or command arguments. Its only optional argument is
`--help`; invalid arguments use `NIM_BUILD_INVALID_ARGUMENT`, and output errors use
`NIM_BUILD_OUTPUT_FAILED`. These are local tool/API errors, not wire error codes.

`make build` compiles the Go packages and diagnostic executable, the native Rust
library, and that same library for `wasm32-unknown-unknown`. `make check` checks
formatting, runs Go vet and Rust Clippy, and executes nonzero behavioral tests in
both languages. CI calls the same two targets with exact toolchain releases.
Compiling a Rust library for wasm is not a browser test or a stable foreign ABI.

## Boundaries and alternatives

The planned service domain remains independent from transport and storage. Wire
schemas/codecs will have their own versioned contract. Rust core remains free of
UI, Go runtime, browser and platform dependencies; adapters will translate host
concerns without reimplementing message semantics. Premium modules may depend on
OSS extension interfaces, never the reverse.

A separate SemVer parser, message identifier type, network listener and placeholder
codec would add unrelated contracts to this foundation. They are unnecessary for
build diagnostics. No message send, retry, outbox, sync, local store, FFI or
database functionality is implemented here.

The intended recovery architecture uses WebSocket for realtime delivery and
database/sync as recovery truth. Future native storage is SQLite and browser
storage is IndexedDB; PostgreSQL is the selected server database. Those choices
do not introduce drivers or imply tested storage in this change. JSON wire
serialization and its validation rules belong to a separate protocol decision.

## Compatibility, verification and licensing

There is no released API, protocol or database to migrate. This change introduces
only the diagnostic APIs and their explicit failure behavior. Subsequent changes
to those public contracts require review and documentation.

Tests cover missing and malformed metadata, duplicate keys, exact revision
boundaries, dirty state, diagnostic output and error propagation. Toolchain
patch versions are checked before both build and check. macOS and hosted Linux
results are recorded separately; neither implies support for all SDK platforms.

Go/Rust compiler and standard-library licenses remain applicable to any later
binary distribution. GitHub Actions are CI tooling, pinned to full commits and
reviewed separately from runtime dependencies. No external Go module or Cargo
crate is introduced. NewIM's distribution license is not selected by this ADR;
the SDK package is unpublished and no publication permission is implied.
