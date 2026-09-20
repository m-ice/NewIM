# Target architecture

## Core rule

Realtime is an optimization; sync restores truth.

```text
Client SDK
  ├─ local store + outbox
  ├─ connection/auth state machine
  ├─ sync/recovery
  └─ API/event facade
        │
        ├── WSS realtime
        └── HTTPS control/sync/media credentials
              │
          API / WS Gateway
              │
   ┌──────────┼─────────────┐
 Auth     Message      Conversation
              │              │
           Sync/Read      Permission
              │
      relational database
              │ TX
        transactional outbox
       ┌──────┼─────────┬───────┐
   online   push     webhook   async moderation
```

## V1 infrastructure

- Relational DB (PostgreSQL or MySQL chosen by ADR).
- Redis only for ephemeral routing/presence/rate-limit/cache/session helpers.
- Object storage for media.
- OpenTelemetry-compatible metrics/traces and structured logs.
- No Kafka by default; add a durable broker only after measured constraints justify it.

## Service boundaries

Deployable service count may initially be smaller than logical module count. Keep logical boundaries in code first; split processes when operational data demands it.
