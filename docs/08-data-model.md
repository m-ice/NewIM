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
