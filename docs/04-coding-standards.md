# Coding standards

## General

- Functions do one job; keep branching/state transitions explicit.
- Prefer immutable values and explicit ownership across concurrent boundaries.
- No hidden retries in low-level adapters; retry policy belongs to an orchestration layer.
- Time, randomness, IDs and network clients should be injectable at test boundaries.
- Public errors expose stable codes; human messages are not control flow.
- Configuration is validated at startup; invalid critical configuration fails fast.

## Comments

Use brief bilingual comments only where intent/contract is non-obvious:

```text
// Persist before ACK so reconnect can recover the message.
// ACK 前先持久化，确保重连后可恢复消息。
```

Avoid narrating syntax, generated-sounding commentary, or large prose blocks inside source files.

## Tests

Prefer state/result assertions over implementation-detail mocks. Reliability logic must include retry/duplicate/reorder/restart cases.
