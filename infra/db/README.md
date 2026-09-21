# Relational storage v1

This directory implements PostgreSQL constraints, atomic message persistence and
transactional migrations. It supplies no authentication, network ACK, dispatcher,
sync cursor or retention policy. See ADR 0004 for the caller contract.

Prerequisites: Python 3.11+, Docker (classic or OCI descriptor-aware image store),
the root pinned Go/Rust tools. No Python package or Go database driver is needed.
The SQL dialect is PostgreSQL 18.6; deployment compatibility with other versions
has not been established.

```sh
make db-prepare
make db-schema db-migrations db-sequence db-repair
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

Records under ignored `build/db-runs/<suite>-<unique>/` contain actual argv,
working directory, exit code, duration and stdin SHA-256, image identity, suite
result and populated query plans. SQL parameters and raw errors are not logged.
The test-only codec binary has no production API and no database driver.

## Schema and queries

Core tables cover users/devices/sessions, conversations/members, messages/read
state, blocks, outbox, webhook delivery references and opaque encrypted push
tokens. Restrictive FKs avoid implicit account/message deletion. Session/token
rows do not implement revocation policy, credential encryption or token refresh.

`newim.persist_message` requires a trusted, authorized, protocol-validated caller
and READ COMMITTED. Invoke it in a transaction and only acknowledge persistence
after COMMIT succeeds. It returns the original `(sender_id,client_msg_id)` row on
retry. Retry IDs remain stable. Counter/message/latest-pointer/outbox changes
commit or roll back together. NI001 means missing conversation, NI002 exhausted
BIGINT allocation and NI003 unsupported isolation. These internal SQLSTATEs are
not public wire error codes. Whole-transaction retries must handle 40P01, 40001
and ambiguous connection loss with bounded backoff. Retrieving a duplicate also
requires caller authorization; SQL SECURITY INVOKER does not enforce membership.

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
   for manually populated 001 fixtures. No production backlog conversion is
   claimed and no destructive down migration exists.
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
