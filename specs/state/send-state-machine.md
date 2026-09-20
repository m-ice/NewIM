# Send state machine

1. Persist outbox entry with stable `clientMsgId`.
2. Attempt send when connectivity/policy allows.
3. On timeout/network/temp-unavailable, retry same `clientMsgId`.
4. On permanent business/payload/auth rejection, transition to terminal failure or explicit auth recovery.
5. On `SERVER_PERSISTED`, merge authoritative IDs/seq and mark/remove outbox entry atomically according to local-store design.

Socket write completion is never a persistence ACK.
