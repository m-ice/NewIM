# ADR 0004: PostgreSQL relational storage and recovery

Status: proposed for independent acceptance.
Task: NIM-DAT-001

## Decision and boundary

PostgreSQL 18.6 is the sole server database dialect. This first schema provides relational constraints, a narrow SQL persistence primitive, immutable numbered migrations and real isolated database tests. No Go database driver, transport, authorization service, sync cursor interpretation or persistence ACK is added. Callers must authorize and validate a message through the accepted protocol before invoking storage; a returned SQL row is not an outer transaction commit or a network ACK.

Product destinations are infra/db/, this ADR, Makefile and the existing product CI. The storage task leaves all existing build/protocol checks intact. It does not decide TTL, account deletion, moderation, retention or the product license. Foreign keys restrict deletion rather than silently cascading message loss.

Protocol v1 IDs/type use the accepted ASCII grammar. protocol_version is 1; message schema_version is a positive signed INT. Both conversation_seq and server_time_ms preserve the full nonnegative signed BIGINT domain; server time is not converted to PostgreSQL timestamp. Stored sequences may represent zero; the allocator starts at one. BYTEA stores the validated raw JSON payload without numeric conversion, including huge exponents and escaped NUL. Its 65536 byte ceiling is only a storage bound: complete envelope byte/depth/JSON validation remains the codec boundary. SQL does not parse JSONB or normalize text, and does not define a content fingerprint.

## Transaction and ordering

newim.persist_message is a SECURITY INVOKER SQL storage primitive. READ COMMITTED is required. It checks the global (sender_id, client_msg_id) identity, locks the conversation row, checks identity again, increments the checked BIGINT counter, inserts the message, updates the latest pointer and inserts an outbox row in one transaction. Duplicate identity returns the first stored row, irrespective of retries' newly proposed IDs or byte formatting; it never rewrites a payload or invents content equivalence. Caller authorization must precede invocation, including authorization to retrieve an existing identity. This does not introduce a public send API.

A same-conversation writer waits on that conversation row; unrelated conversations do not use a global writer lock. PostgreSQL nextval is not used for conversation sequencing because rollback does not undo it. At MAX, a new message fails with stable SQLSTATE NI002; missing conversation is NI001. Unsupported transaction isolation is NI003. SQLSTATE messages are fixed text without user content. IDs and FK/unique/check failures retain PostgreSQL SQLSTATE/constraint names for internal diagnostics; an eventual service must map them to reviewed wire error codes.

The inner PL/pgSQL exception block rolls back ALL changes it made, including counter/summary/outbox, on a unique violation. Only the named sender/client uniqueness constraint is treated as a concurrent duplicate; a fresh READ COMMITTED lookup then returns the committed original. Unrelated server ID/event ID/sequence collisions propagate. The outer caller transaction still owns COMMIT. A caller rollback, backend termination or process crash before commit cannot leave a partial persisted result. Lock order is conversation row before message/summary/outbox; future multi-conversation operations must order conversation IDs deterministically. Adding permission/membership locks requires review of the resulting order.

Deadlocks (40P01), serialization failures (40001) and connection loss require whole-transaction rollback/retry, stable clientMsgId and bounded backoff. On ambiguous COMMIT, reconnect and query the indexed identity. This task tests the DB contract and rollback behavior; server retry timers/ACK logic remain a subsequent task. Outbox events are durable identities plus message references; no event wire format or exactly-once delivery is claimed. Polling can use the pending index, but dispatcher leases/retries are not implemented here.

## Schema and migrations

Migration 001 builds core entities and messages/read state. Migration 002 adds durable side-effect tables, outbox indexing and the persistence primitive. These are meaningful stages of the first installation, not previously released production versions. A populated 001→002 test verifies preservation while completing the initial schema; services must not use an incomplete migration set. No historical production upgrade claim is made.

A migration ledger stores exact SQL SHA-256 values and sequence numbers. One psql connection takes a database advisory transaction lock, validates ledger prefix/checksums/future versions, and applies missing SQL and ledger rows in one transaction. SQL bytes hashed are the bytes executed. No down migration or nontransactional concurrent DDL is present. Empty install, populated staged install, replay, edited/gapped/future ledger, competing migrators, SQL failure and terminated session are tested. A failed batch leaves the last committed schema/data usable. Later migrations must stay compatible or explicitly register a new upgrade/repair strategy; immutable applied scripts must never be edited to repair a database.

Unique indexes enforce server ID, sender/client identity and conversation/seq. History uses keyset bounds and limit on the latter B-tree. Membership has a user/conversation lookup index. Query-plan checks use populated selective fixtures rather than forced index settings or tiny-table performance promises. SQL query consumers must enforce page limits. Schema/table counts and query plans provide diagnostics; no message/token values belong in production logs.

## Preparation, testing and recovery

infra/db/image-lock.json fixes the official multiarch index and independently verified arm64/amd64 manifest/config/rootfs identities. Cold preparation pulls an explicit platform by immutable reference; local prepared images are accepted only after actual descriptor/platform/rootfs verification, never by tag name alone. Tests execute the resolved image ID with --pull never and explicit architecture. Preparation failures are visible and bounded, not skipped tests. A verified local OCI import may supply the same official platform manifest when a daemon registry route is unavailable; a local tag is not the permanent dependency.

Each test instance has a unique labeled container and durable volume, no host port, no user bind mounts and no external network. PostgreSQL 18 data volume mounts at /var/lib/postgresql. Readiness, Docker commands, SQL locks/statements and suites are bounded. Cleanup only touches exact IDs created by this run. PostgreSQL fsync, full_page_writes and synchronous_commit stay enabled. SQL errors do not print statements, parameters, context, tokens or payloads. Harness output contains test labels, counts, timing and fixed SQLSTATE/constraint identifiers only.

The four suites run real SQL, multiple independent sessions, committed/uncommitted process-crash cases, and logical dump/restore into a separate fresh volume. Codec fixtures pass through the actual accepted Go codec, relational columns/raw bytes, and the codec again; this helper is test plumbing, not a fake database adapter. Pure SQL schema checks cannot replace this boundary test.

Rollback means abort a failed transaction or restore a verified backup to a separate instance, then apply a reviewed forward repair. No destructive downgrade is automated. A SQL dump is a logical database snapshot, not WAL/PITR or cluster-wide role/config/secret backup. Tests verify rows, payload hashes, relationships, counter/outbox state and resumed allocation after restore. Container SIGKILL is a process-crash experiment on a running host, not a machine power-loss simulation. Production RPO/RTO, replicas and scheduled backups require their own deployment evidence.

## Validation

Required real commands: make db-schema, make db-migrations, make db-sequence, make db-repair, make check; make build is also run for regression. CI prepares a locked image from a cold runner and invokes all four database suites. Independence, licenses/security and delivered commit acceptance are recorded outside the product implementation; this ADR does not assert those approvals have already happened.
