# ADR 0011: Media credential and metadata flow

Status: accepted for NIM-MED-001; additive to protocol v1, migration 004 and the
existing message transaction.

## Decision

Media is a closed metadata-only persisted/send schema at `type=media`, `version=1`.
The exact payload fields are `mediaKey`, `kind`, `contentType`, `size` and
`sha256`. The encoded payload is at most 4096 bytes. Binary bytes, object URLs,
signed URLs and every unknown field are rejected. Text v1, unknown non-media
message types, send framing and ACK framing retain their existing behavior.

Upload grants are server-generated, short-lived and digest-only. The raw token is
`grantId.secret` with a 32-character lowercase hexadecimal grant ID, one dot and a
43-character unpadded base64url encoding of 32 random bytes. Only SHA-256 of the
complete raw token is persisted. BeginUpload authorizes the complete trusted
`session.ConnectionIdentity` as a current conversation member before invoking the
grant generator. Default expiry is 120 seconds; maximum is 300 seconds. At or
after expiry a pending completion is rejected. An identity/intent-matched ready
asset remains an idempotent completion replay after expiry.

The trusted identity binding is explicit for all five fields: user, device,
session, connection and token. The grant stores them, completion compares all
five before expiry/ready replay, message validation requires all five exact asset
bindings, and download validates a complete caller identity plus a complete
stored asset identity before signing.

The application layer depends on provider-neutral store, object-store and signer
ports. The local filesystem adapter owns a mode-0700 root, rejects unsupported
object names and symlink escapes, writes a same-directory temporary file with
exclusive/no-follow semantics, fsyncs it, publishes only through no-replace
`link`, and fsyncs the parent. An existing object is accepted only after exact
size and SHA-256 comparison. Ordinary overwriting rename is not used.

Migration `005_media_assets.sql` stores media metadata, complete trusted
user/device/session/connection/token bindings where the schema permits, the grant
ID and grant digest, expiry and the closed `pending`/`ready` state. It stores no
URL, object URL, public ACL or list capability. Concrete FKs bind membership,
device, session and token/session identity. Migration 001-004 remain immutable.

Private download resolves a ready asset, verifies current membership and complete
trusted identity, and only then calls the injected signer. The default URL TTL is
60 seconds and the maximum is 300 seconds. URLs are returned in memory and must
never be persisted, logged or emitted as metrics/traces.

`server/message` calls a narrow `MediaValidator` only for media v1 before
starting persistence. It requires a ready asset owned by the trusted sender and
bound to the requested conversation, with exact media key, kind, content type,
declared/actual size and SHA-256 equality. A mismatch has no message, pending or
outbox side effect.

## Explicit boundaries

This ADR does not implement an HTTP or WebSocket endpoint, transport
authentication, rate limiting, abuse controls, moderation, retention/deletion,
cleanup, account erasure, premium/license behavior or a cloud storage SDK. These
remain separate decisions and must not be claimed as complete here. DEC-003
retention/moderation/account-disposal outcomes and SEC-001 transport/rate/abuse
boundaries remain explicit. The orphan-object risk after object publication and
a failed metadata commit is documented and has no cleanup policy in this task.

## Compatibility and verification

The change adds Go/Rust codec APIs and fixtures, one additive PostgreSQL table and
migration, an internal server application/adapters, and one narrow message-service
extension. `make media-protocol media-db media-security media-authz media-check`
plus the existing `build check db-schema db-migrations db-sequence db-repair
sync-check auth-check auth-recovery auth-policy message-check message-recovery
message-errors` gates are required. Actual PostgreSQL and local filesystem tests
are mandatory; codec or mock-only success is not a media-flow acceptance claim.
