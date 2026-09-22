# Relational storage v1

This directory implements PostgreSQL constraints, atomic message persistence,
transactional migrations, the durable message-send transaction, the durable
auth-session/token storage primitive and the digest-only media grant/asset
storage primitive. It supplies no credential verification, network ACK,
dispatcher, sync cursor, media endpoint, retention policy or public
authentication endpoint. See ADRs 0004, 0008, 0009 and 0011 for the caller
contracts.

Prerequisites: Python 3.11+, Docker (classic or OCI descriptor-aware image store),
the root pinned Go/Rust tools. No Python package or additional Go database
driver is needed; product code uses the existing reviewed pgx dependency. The SQL
dialect is PostgreSQL 18.6; deployment compatibility with other versions has not
been established.

```sh
make db-prepare
make db-schema db-migrations db-sequence db-repair
make auth-check auth-recovery auth-policy
make media-protocol media-db media-security media-authz media-check
make message-check message-recovery message-errors
make build check
```

Preparation checks immutable official image metadata and pulls its digest only
if absent. `NEWIM_DB_IMAGE` may select an already imported original OCI image;
the actual descriptor/config/platform/ordered rootfs must still match the lock.
A wrong or unavailable explicit local image fails without substitution. Docker's
platform image ID can be the manifest digest or config digest, bound by the saved,
hashed upstream chain in `image-metadata/`. A classic store with no Descriptor
must expose the exact locked config ID, OS/architecture/variant, ordered rootfs
DiffIDs and an official PostgreSQL RepoDigest for the locked index/platform.
Only `postgres`, `library/postgres`, `docker.io/library/postgres` are accepted
official repository aliases. A present malformed or wrong Descriptor is rejected,
never treated as classic. Both paths trust the local daemon/content store. Inspect
avoids the --platform option unavailable in Docker 28.0.4; pull and execution
still explicitly select the verified native platform. No daemon reconfiguration,
unlocked image, tag-only acceptance or image-store conversion is required.

Each suite uses its own labeled durable volume and isolated container, no host
port, no user bind mount and no external network. Local socket trust is solely
for these unreachable disposable test instances; it is not a production auth
configuration. PostgreSQL fsync/full_page_writes/synchronous_commit stay on.
Initialization readiness requires the final PostgreSQL PID 1, avoiding the
temporary initdb server. Cleanup verifies exact resource ownership labels.
Command/SQL/lock/readiness bounds prevent indefinite waits; CI additionally has
an 8 minute bound around all four suites. Failures are errors, never skips.

Records under ignored `build/db-runs/<suite>-<unique>/`,
`build/auth/<suite>-<unique>/`, `build/message/<suite>-<unique>/` and
`build/media/<suite>-<unique>/` contain
actual argv, working directory, exit code, duration and stdin SHA-256, image
identity, compilation/test logs and suite result. SQL parameters and raw errors
are not logged. The test-only codec binary has no production API and no database
driver.

## Schema and queries

Core tables cover users/devices/sessions, conversations/members, messages/read
state, blocks, outbox, webhook delivery references and opaque encrypted push
tokens. Migration 004 adds `im_auth_tokens` with a lowercase-hex `token_id`
primary key, unique 32-byte SHA-256 token digest, restrictive session FK,
issue/expiry timestamps and nullable revocation timestamp. Restrictive FKs avoid
implicit account/message deletion. The table stores no raw token or secret and
does not implement credential verification, token refresh, transport or login
policy.

Migration 005 adds `im_media_assets` with digest-only upload grants, complete
trusted identity bindings, a closed `pending`/`ready` state and no URL/object/ACL
columns. It does not implement retention, deletion, moderation, account erasure
or cleanup. The media suite uses real PostgreSQL plus the local filesystem
adapter and bounded process-restart replay tests.

`newim.persist_message` requires a trusted, authorized, protocol-validated caller
and READ COMMITTED. Invoke it in a transaction and only acknowledge persistence
after COMMIT succeeds. It returns the original `(sender_id,client_msg_id)` row on
retry. Retry IDs remain stable. Counter/message/latest-pointer/outbox changes
commit or roll back together. NI001 means missing conversation, NI002 exhausted
BIGINT allocation and NI003 unsupported isolation. These internal SQLSTATEs are
not public wire error codes. Whole-transaction retries must handle 40P01, 40001
and ambiguous connection loss with bounded backoff. Retrieving a duplicate also
requires caller authorization; SQL SECURITY INVOKER does not enforce membership.

The `server/message` application service and `server/storage/message` adapter use
that primitive for an internal durable send. The adapter runs READ COMMITTED,
locks the conversation and membership rows, invokes the trusted generator
callback, persists, compares the original intent, and commits before returning a
result. Equal retries return the original server ID, sequence and time; unequal
intent returns `SEND_ID_CONFLICT` without writes. A `SERVER_PERSISTED` ACK is
constructed only after commit. Retryable storage/lock failures remain distinct
from permanent input, authorization, missing-conversation, sequence-exhaustion
and ID-conflict failures. No dispatcher, peer delivery or public network ACK is
implemented here.

The privileged service role can write tables directly, so the invariant is an
application transaction contract, not protection against a compromised service.
PUBLIC has neither schema access nor function execution. Production role/grant
provisioning remains deployment work and must preserve the migration/service
privilege boundary. A SQL function result before the outer commit is provisional.

Use numeric keyset pagination with validated bounded limits, for example:

```sql
SELECT server_msg_id, conversation_seq, payload_bytes
FROM newim.im_messages
WHERE conversation_id = $1 AND conversation_seq > $2
ORDER BY conversation_seq LIMIT $3;
```

Required B-trees cover global server ID, sender/client identity, conversation/seq,
and user/conversation membership. The pending outbox index is partial on
`completed_at IS NULL`, ordered by available time/event ID. The schema suite
checks actual EXPLAIN ANALYZE/BUFFERS on 10,000 rows with default planner settings;
this proves fixture access paths, not a production latency or throughput SLO.
Query callers must separately cap limits, scope permissions and enforce deadlines.

`payload_bytes` preserves codec-validated raw UTF-8 JSON bytes, including huge
number tokens and escaped NUL. Do not cast it through JSONB/numeric. The SQL byte
bound does not replace full envelope, text, nesting or JSON validation.

The auth PostgreSQL harness builds the tagged `tests/integration/auth` package
for Linux and executes it in the same owned, networkless PostgreSQL 18.6 setup.
`make auth-check` exercises token format, digest-only storage, constant-time
verification, binding/ownership, stable errors and mandatory redacted observer
paths. `make auth-recovery` checks concurrent revoke/authenticate ordering,
commit-time failure/disconnect, database restart and logical restore without
token resurrection. `make auth-policy` exercises the injected `LoginPolicy` seam
and complete five-ID connection identity. These targets do not start HTTP, WS,
gateway, credential, push, refresh-token or multi-device policy implementations.

The message PostgreSQL harness builds the tagged `tests/integration/message`
package and runs it in the same owned PostgreSQL environment. `make message-check`
covers equal retry, unequal intent, ACK-after-commit and identity correlation.
`make message-recovery` covers rollback, backend termination, restart/restore and
lost-response retry convergence with one durable message/outbox identity.
`make message-errors` covers permanent and retryable classifications. These
targets do not implement HTTP/WebSocket, peer delivery, fan-out, read receipts
or an SDK retry state machine.

## Migration and recovery runbook

1. Pin and review the migration source revision. Back up the current database
   before changing it. Stop application use of an incomplete migration set.
2. Render local checked SQL with `python3 -B infra/db/migrate.py` and feed it to
   an explicitly configured `psql -X -v ON_ERROR_STOP=1` connection using a
   migration role. Keep credentials outside command arguments/logs. Do not pipe
   a remote script or edit a previously applied migration.
3. The generated session uses one advisory transaction lock and validates the
   entire applied prefix/checksums before executing missing SQL. Apply and ledger
   updates commit together. A failure/terminated connection rolls the batch back;
   rerun the same immutable set after fixing the external cause. Unknown future
   schema, modified checksums and gaps fail closed (NM001/NM002/NM003).
4. Stages 001 and 002 are parts of this first installation, not previously
   released schemas. Populated 001→002 is a supported staged installation test;
   do not deploy a service with only 001. Migration 002 does not generate events
   for manually populated 001 fixtures. Migration 003 adds the conversation
   projection, migration 004 adds the opaque token table and migration 005 adds
   digest-only media grants/assets. Migration 005 keeps all populated 001-004
   rows and performs no implicit media backfill. No
   production backlog conversion is claimed and no destructive down migration
   exists.
5. For backup, use `pg_dump --format=custom --no-owner` with the explicitly
   selected database and protected output location. Preserve ACLs; do not add
   `--no-privileges`. Test tooling captures the dump in memory and logs only its
   digest. Production backups contain private data and require access control,
   encryption and independent retention decisions.
6. Restore into a separate fresh database/volume using `pg_restore
   --exit-on-error --single-transaction --no-owner`. Provision required roles
   first and verify the ACLs. Do not overwrite the original database. Compare
   all committed rows, IDs/seq, payload hashes, relationships, ledger and outbox.
   Replaying migrations must be a no-op, and the next committed allocation must
   continue above the restored counter. The repair suite executes this procedure
   and verifies PUBLIC remains denied.
7. If only derived latest-message pointers are damaged, quiesce affected service
   traffic and review `repair-summary.sql` against the incident. It locks
   conversations in deterministic order, rebuilds pointers from existing numeric
   sequences and refuses counters behind stored messages (NR001). It never
   deletes messages, renumbers IDs, lowers counters or resolves retention policy.
   Execute in one transaction and verify the resulting rows before resuming.
8. Unrecoverable corruption or a counter behind persisted data requires incident
   analysis and a separately reviewed forward repair or restore. Do not silently
   reset, truncate or downgrade. Retain original data for investigation.

`db-repair` terminates an uncommitted backend and SIGKILLs its own PostgreSQL
container, then restarts the same durable volume and verifies exact committed
state. It restores a logical backup onto another new volume, runs the forward
repair and resumes allocation. This is process crash testing on a running host,
not host power-loss, disk corruption tolerance, WAL/PITR or replication proof.
Logical backup is not cluster-role/config/secret backup; production RPO/RTO and
backup scheduling require separate operational acceptance.
