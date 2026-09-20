# Server logical modules

- Gateway: connection/session ownership, protocol framing, drain and node routing.
- Auth: access token validation/revocation and device/session semantics.
- Conversation: membership/summary/latest state/tombstones.
- Message: validation, permission, persistence, sequence allocation, transactional outbox.
- Sync: conversation delta, message delta, bootstrap, cursor expiration.
- Presence: TTL-based best-effort state only.
- Push: offline notification delivery; never truth storage.
- Media: upload credentials, media metadata and signed URL resolution.
- Webhook: signed, versioned, idempotent async delivery.
- Moderation adapter: product-configurable sync/async policy without contaminating message storage contracts.
- Platform API: trusted server-side management surface separated from client APIs.
