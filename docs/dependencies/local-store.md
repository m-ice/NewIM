# Native LocalStore dependencies

Task: NIM-SDK-001. Verified 2026-09-21. Independent acceptance is required.

The portable `newim-sdk-core` has no external dependencies. `newim-store-sqlite` introduces SQLite through `rusqlite =0.40.2` with defaults disabled and only `limits`, plus exact `libsqlite3-sys =0.38.2`. The adapter uses the protocol crate and its existing exact serde_json 1.0.151 only for integration tests. No UI, async executor, network client or SQLite wasm dependency enters core.

## Source and lock graph

`Cargo.lock` fixes all 19 registry packages; `sdk/storage/sqlite/dependency-lock.json` records the actual archive SHA-256, bytes, repository, license and packaged VCS identity of each. All archive bytes were checked against Cargo.lock. Existing protocol versions are unchanged. Eight added crates are:

| Crate | Version | Role | License path |
|---|---|---|---|
| rusqlite | 0.40.2 | Safe Rust SQLite API | MIT |
| libsqlite3-sys | 0.38.2 | Native FFI/link setup | MIT |
| bitflags | 2.13.2 | Flags | MIT |
| fallible-iterator | 0.3.0 | Query iteration | MIT |
| fallible-streaming-iterator | 0.1.9 | Query iteration | MIT |
| smallvec | 1.16.1 | Small parameter buffers | MIT |
| pkg-config | 0.3.34 | Upstream build dependency; system fallback disabled | MIT |
| vcpkg | 0.2.15 | Upstream build dependency; not used on accepted host | MIT |

Full actual license texts are retained in THIRD_PARTY_NOTICES.md. Bundled SQLite 3.53.2 and SQLCipher in the sys archive are not built; source archive redistribution must retain their original notices, including SQLCipher's BSD notice. No copied commercial/AGPL implementation is used.

The upstream [rusqlite repository](https://github.com/rusqlite/rusqlite) and [exact registry release](https://crates.io/crates/rusqlite/0.40.2) provide maintenance/source history. Current release archives were fetched explicitly and are not yanked per the registry metadata observed during preparation. Versions do not by themselves prove absence of vulnerabilities; independent security review covers the selected feature graph, source build and filesystem/recovery boundaries. No automated vulnerability scanner result is claimed.

## SQLite engine

[SQLite 3.53.4 release](https://sqlite.org/releaselog/3_53_4.html) publishes the source ID and sqlite3.c SHA3-256 used in `sdk/storage/sqlite/source-lock.json`. The official amalgamation archive SHA-256 is `1e71ddf93849c6a6ecf58b827c0692073d2dd7ee40196158068f7b29f422e87d`; extraction is restricted to three named source/header entries. Their individual SHA-256 values are pinned in source-lock, independent of cache metadata. SQLite's [public-domain statement](https://sqlite.org/copyright.html) covers deliverable SQLite code; SEE/SQLCipher and upstream auxiliary build scripts are not used.

The native compiler/archiver are host `/usr/bin/cc` and `/usr/bin/ar`; cache identity includes host Rust target, resolved C compiler identity, flags and source archive digest. This is a reproducible input/cache identity, not a claim of cross-compiler bitwise reproducibility. Supported build hosts are macOS/Linux. C flags use O2/PIC, serialized thread mode, default foreign keys, API armor, OMIT_LOAD_EXTENSION, DQS=0 and disabled default memory statistics. The runtime rejects a different SQLite version/source or required compile options. Engine preparation is explicit; build/run are offline, and no system or pkg-config SQLite fallback is allowed. Both native core/adapter tests and separate core/protocol wasm build are mandatory.

Local Apple clang 17 arm64 static SQLite archive measured 1,520,032 bytes in the initial actual probe; the final candidate's build log records current artifact sizes. This is unstripped archive size, not a final shipped SDK size. Runtime costs include SQLite page/cache/WAL storage and the Rust wrapper; no benchmark or mobile/browser binary-size claim is made. Upgrade engine/bindings only together with migration, corruption, crash and compatibility regression tests.
