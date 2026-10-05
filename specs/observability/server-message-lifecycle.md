# Server message lifecycle observability

Status: implementation contract for NIM-OBS-002
Task: NIM-OBS-002

This specification is additive to the durable send, outbox and Webhook
contracts. It does not define public wire fields, database schema, auth/session,
media, gateway, sync or delivery semantics.

## Send lifecycle

Every service send attempt generates a trusted 128-bit lowercase-hex `traceId`.
It is not accepted from the request and is not emitted to protocol, ACK, SQL,
Webhook payload, metric labels, production errors or raw logs.

The bounded sequence is:

```text
accepted -> validated -> persisting -> persisted -> ack_ready
```

- `accepted` is emitted when the service begins one send attempt.
- `validated` is emitted only after all validation callbacks succeed.
- `persisting` is emitted after validation and before the storage persistence
  result is accepted.
- `persisted` is emitted only after the storage port returns a committed result
  or an idempotent committed replay. It is never emitted on an ambiguous or
  rollback outcome.
- `ack_ready` is emitted only after `EncodeServerFrame` succeeds. The HTTP
  adapter may still perform its own bounded response write; observability never
  changes that result.

Failures produce one `rejected` observation with a stable `code`, `result` in
`success|retryable|permanent|unknown`, and the request-local `traceId`. A
successful equal-intent retry is marked `replayed`; a different intent for the
same sender/client key is `conflict`.

## Webhook lifecycle

The code-owned operations are `cycle`, `queue`, `fanout`, `claim`, `attempt`,
`secret`, `envelope`, `signature`, `delivery` and `finish`. The closed mapping
uses fixed `component/stage/result/code` values:

| Operation | Stage |
| --- | --- |
| `cycle`, `queue` | `enqueued` |
| `fanout` | `fanout` |
| `claim` | `claimed` |
| `attempt`, `secret`, `envelope`, `signature`, `delivery` | `attempt` |
| `finish` | `finished`, `delivery_success`, `delivery_retry` or `delivery_dead_letter` |

Unknown operation/code values converge to `unknown`, `unknown` and
`WEBHOOK_UNKNOWN`; arbitrary strings never become metric labels. A successful
`Finish` transition is the authority for terminal delivery observations. When
an exhausted lease is transitioned to `dead_letter` by authoritative
`BeginAttempt`, that committed transition emits the same
`delivery_dead_letter` observation without a fabricated `Finish` result.

## Metrics and bounds

The exposition adds:

```text
newim_message_lifecycle_events_total{component,stage,result}
newim_message_lifecycle_errors_total{component,code}
newim_message_lifecycle_duration_seconds_sum{component,stage}
newim_message_lifecycle_duration_seconds_count{component,stage}
newim_message_send_idempotency_total{outcome}
newim_message_outbox_fanout_pending{state}
newim_webhook_delivery_pending{state}
newim_webhook_deliveries_total{stage,result}
newim_observability_dropped_total{reason}
```

Fixed domains are:

- `component`: `send|outbox|webhook`
- `stage`: `accepted|validated|persisting|persisted|ack_ready|rejected|enqueued|fanout|claimed|attempt|finished|delivery_success|delivery_retry|delivery_dead_letter|unknown`
- `result`: `success|retryable|permanent|unknown`
- `outcome`: `new|replayed|conflict`
- outbox `state`: `pending|quarantined`
- webhook `state`: `pending|retry|leased|dead_letter`
- `reason`: `invalid_label|writer_error|trace_id_failure|queue_full|storage_unavailable`

Labels are validated/normalized by the registry. Series are bounded; unknown
codes converge to the component fallback, invalid label sets are dropped, and
the fixed dropped counter is incremented. No raw `clientMsgId`,
`conversationId`, `senderId`, `serverMsgId`, `eventId`, `traceId`, URL, token,
secret, signature, SQL, DSN or driver diagnostic is a label or production log
field.

## Durable gauges

`newim_message_outbox_fanout_pending` is computed at scrape time from
`newim.im_outbox_events`:

```text
pending     = webhook_fanout_at IS NULL AND webhook_fanout_error IS NULL
quarantined = webhook_fanout_at IS NULL AND webhook_fanout_error IS NOT NULL
```

`newim_webhook_delivery_pending` is computed separately from
`newim.im_webhook_deliveries.status` and counts only `pending`, `retry`,
`leased` and `dead_letter`. These are independent gauges from independent
database snapshots. A failed or timed-out snapshot omits the affected gauge and
increments `storage_unavailable`; it never exports a guessed zero and never
claims a process-local value is durable truth.

## Non-blocking and recovery contract

- Registry updates use bounded memory; sink enqueue is non-blocking.
- `/metrics` snapshots under a bounded lock, releases the lock, then writes.
- Source snapshots run outside registry/observer locks and use short deadlines.
- Observer callbacks use a bounded FIFO dispatcher, so a blocked observer cannot
  backpressure legitimate work; overflow is counted as `queue_full` and
  observer panics are isolated as `writer_error`.
- Source panics are isolated and counted as `writer_error`.
- Queue overflow is counted as `queue_full`; a slow writer/scrape cannot change
  send, commit, ACK, outbox or Webhook outcomes.
- Restart resets process counters but rehydrates both durable gauges from
  PostgreSQL. Recovery must not duplicate message, outbox or delivery rows.

## Verification commands

```sh
make message-observability-check
make message-observability-metrics
make message-observability-security
make message-observability-recovery
make message-check message-http-check media-check auth-check auth-http-check auth-http-security auth-route-check api-check api-security webhook-protocol webhook-security
make build check docs-check
```

Tests must use real authoritative outcomes for commit/ACK, idempotent replay,
conflict, Webhook final transitions and PostgreSQL recovery. Mock-only calls or
file-existence checks are not acceptance evidence.
