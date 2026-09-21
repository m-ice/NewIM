# Native SQLite LocalStore

Local atomic storage primitives for NewIM. This is not a network send/ACK or synchronization coordinator. The host validates authoritative inputs with the protocol codec, computes absolute conversation state after deduplication, then dispatches bounded core requests on a storage worker.

```
make store-prepare
cargo fetch --locked
make store-engine
make build check
make store-idempotency store-migrations store-maintenance store-recovery
```

Only prepare/fetch use the network. Missing or mismatched native caches fail closed. The core/protocol wasm build excludes this native crate. Current native host support is macOS/Linux; Windows/phone integration requires a separate platform task.

Each account has a private mode-0700 directory and one kernel-lock owner. Use a resolved path without symlink components. The adapter rejects sharing the same directory across accounts. Hosts must keep the directory private and trusted; protection against another process with the same OS UID concurrently replacing path components is outside this filesystem boundary. SQLite files contain plaintext local data; OS encryption/key storage belongs to platform/security tasks. Production code emits no SQL/payload/account/path logs. Public error codes and Metrics provide host observability.

Operation IDs increase for mutations within each generation. The most recent successful enqueue/apply/trim request has an exact retry receipt; older operations expire explicitly. Queries don't consume IDs. AdvanceGeneration returns a new fence and invalidates old requests/completions; if that completion is lost, reopen to obtain the persisted fence. Existing message/pending results roll back the whole attempted batch, including unread/cursor changes, and require explicit reconciliation. No existing payload overwrite API exists. Limits reject excess data instead of dropping pending entries.

## Recovery runbook

1. Stop submitting requests; retain stable operation and message IDs. Close the adapter. Do not delete DB/WAL/SHM files or open them through a different SQLite build.
2. Call `quarantine(root, unique_name)`. It obtains the same exclusive lock, durably records a recovery marker and moves the complete closed active directory. A retained marker makes ordinary open fail. Repeating the same interrupted quarantine resumes without replacing an existing quarantine.
3. `salvage_pending` reads bounded pages from quarantine and returns `RecoveryRequired` if bytes/structure are not safely readable. It does not assert that all pre-corruption entries were recovered. Keep quarantined raw files private; never upload them as routine diagnostics.
4. Call `rebuild(root, account, limits)` only as an explicit recovery decision. The new DB has a new instance and reset cursor. The original quarantine remains. Interrupted rebuild can resume from the durable marker.
5. The rebuilt database persists `recovery_required=true`, including across reopen. The caller must separately reconcile exported/unrecoverable pending and perform authoritative bootstrap; neither workflow is implemented here. There is no API that silently clears this state or claims lost pending recovered.

A new quarantine name is required for every new incident. If a rebuild was interrupted before committing a supported schema, call quarantine again with a fresh name to preserve the partial active directory; prior quarantines remain intact, and rebuild can then start a new attempt. The initialized marker binds account/instance. Missing, truncated or empty-schema data in any existing active directory is an error, not automatic fresh storage. Only the call that exclusively creates a previously absent active directory may initialize schema zero. A committed supported database whose marker publication was interrupted resumes the same instance; partial schema creation requires explicit quarantine/rebuild. Read queries and migration checks have bounded input/page limits; SQLite opening/recovery checks use interrupt deadlines, while OS filesystem I/O cannot be given a hard deadline by SQLite interrupt. Compact has a requested interrupt deadline (1–30,000 ms) and can return Busy/StorageFull/Cancelled/Io. Its temporary attachment allowance is private to VACUUM and restored afterward. Process-kill tests exercise SQLite transaction/WAL recovery, not arbitrary power failure or broken hardware.

See [ADR 0005](../../../docs/adr/0005-local-store.md) and [dependency review](../../../docs/dependencies/local-store.md).
