# Data model notes

PostgreSQL is the server truth. Message payload bytes remain validated protocol
JSON; conversation order is the server-assigned nonnegative `conversation_seq`.
The relational schema uses restrictive foreign keys rather than cascading loss.

Migration 005 adds `newim.im_media_assets`. It stores the server-generated
`media_key`, owner/device/session/connection/token and conversation binding,
kind/content type/declared and actual size/SHA-256, upload grant ID and digest,
expiry/completion timestamps and a closed `pending`/`ready` state. It has concrete
membership, device, session and session-token foreign keys. It stores no signed
URL, object URL, public ACL or list capability; media message payloads continue to
store only the metadata envelope.

The media object store is provider-neutral. The local adapter owns a mode-0700
root, publishes only with no-replace semantics, fsyncs the file and parent, and
compares an existing object by exact size and SHA-256. Private download URLs are
short-lived memory values and are never persisted or logged. Retention, deletion,
moderation, account erasure and cleanup remain outside this data model revision.

Migration 006 adds the Webhook delivery state. `im_outbox_events.webhook_fanout_at`
is a Webhook-only cutover/fan-out marker and never replaces the shared
`completed_at` field. `im_webhook_endpoints` stores trusted internal destination
state and an append-only `active_revision` pointer; immutable
`im_webhook_endpoint_revisions` rows store the validated URL, key ID, secret nonce
and encrypted secret material. `im_webhook_deliveries` extends the original
event/destination identity with status, attempts, next-attempt time, lease
owner/token/expiry, last HTTP status/error code and timestamps. Pre-006 outbox
rows are cut over as already fanned out, `webhook_fanout_error` can durably quarantine
an event whose trusted endpoint configuration exceeds a hard fan-out bound, and
pre-006 delivery rows are preserved as
non-claimable dead-letter legacy rows. No retention, deletion, public endpoint CRUD,
Push, Bot or login policy is defined by this revision.
