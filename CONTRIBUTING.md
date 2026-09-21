# Contributing to NewIM

Start from a branch with a clean understanding of existing changes. Keep one
reviewable capability per change and describe observable behavior, compatibility
impact and actual test results. Do not overwrite another contributor's work.

Use the exact toolchains in [README](README.md). Before requesting review, run
`make build` and `make check`. Add tests that exercise the changed contract,
including invalid inputs and failure propagation where applicable. Preserve a
failed check's diagnosis; do not report an unexecuted check as passing. CI uses
the same root commands. Review documentation for accuracy as part of code review.

Public APIs should document non-obvious contracts in concise Chinese and English.
Write or update a product ADR before changing public API, protocol or database
contracts. Record migration and compatibility effects; current build identity
does not grant protocol compatibility or an API stability promise for later
messaging features.

Follow the dependency boundaries in [architecture](docs/architecture.md). Keep
domain logic separate from transport and persistence. Platform adapters translate
host concerns and must not implement independent message ordering, retry or sync
semantics. OSS modules must not depend on premium modules.

Use independently written code and reviewed dependencies. For new dependencies,
document source/version, purpose, runtime/build/test scope, licenses and required
notices, maintenance, security review and package size impact. Do not copy
proprietary or incompatible licensed client implementations or reverse-engineered
protocols. Updating a pinned tool or CI action requires reviewing its provenance
and rerunning affected checks.

Production diagnostics must not contain message bodies, credentials, phone
numbers, precise locations or long-lived signed URLs. Diagnostic errors should
have stable codes and exclude raw untrusted input. Build identity reads embedded
information without probing local repositories or contacting external services.

Project licensing and package publication are separate decisions. The initial
Cargo package is unpublished, and this guide grants no distribution rights.
