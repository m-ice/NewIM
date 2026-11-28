# ADR 0021: Server-side message lifecycle observability

Status: accepted for implementation under NIM-OBS-002; independent product acceptance remains separate.
Task: NIM-OBS-002

## Context

The durable send, transactional outbox and Webhook delivery paths already return
authoritative outcomes, but their internal lifecycle is not exposed through the
existing HTTP metrics registry. Adding observability must not move authorization,
validation, commit, ACK, fan-out or delivery into a new authority.

## Decision

`server/api` owns a generic inert exposition seam: a bounded in-memory registry,
an optional bounded non-blocking sink, and read-only `MetricsSource` snapshots.
It never imports `server/message`, `server/webhook`, storage, auth or protocol.
`server/message` and `server/webhook` own their lifecycle labels and invoke the
sink best-effort. `server/cmd/newim-server` composes the sink and the two
independent PostgreSQL gauge sources before the server starts.

Each send attempt has a server-generated 128-bit lowercase-hex `traceId`. The
service generates the value without client input. The authorized TLS send
response can carry it in a response header after commit and successful ACK
encoding. ACK frames, database rows, Webhook envelopes, logs, metric labels and
errors omit the identifier. A generation failure increments
`newim_observability_dropped_total{reason="trace_id_failure"}` and the send
continues. [TLS receipt trace](0046-message-lifecycle-observability.md)

The send lifecycle is `accepted -> validated -> persisting -> persisted ->
ack_ready`. `persisted` is emitted only after the storage port returns following
outer transaction commit. `ack_ready` is emitted only after the ACK server frame
encodes successfully. Failures emit `rejected` with the stable code and one of
`success`, `retryable`, `permanent`, `unknown`; idempotent replay and conflict
use the fixed `new`/`replayed`/`conflict` outcome.

Webhook operations are code-owned and bounded: `cycle`, `queue`, `fanout`,
`claim`, `attempt`, `secret`, `envelope`, `signature`, `delivery` and `finish`.
Unknown operation or code values converge to `unknown` and
`WEBHOOK_UNKNOWN`. Terminal delivery labels are `delivery_success`,
`delivery_retry` and `delivery_dead_letter`. An exhausted attempt transition
performed by authoritative `BeginAttempt` emits the same
`delivery_dead_letter` observation even though no `Finish` call occurs.

## Metrics

The additive families are:

```text
newim_message_lifecycle_events_total{component,stage,result}
newim_message_lifecycle_errors_total{component,code}
newim_message_lifecycle_duration_seconds_sum{component,stage}
newim_message_lifecycle_duration_seconds_count{component,stage}
newim_message_send_idempotency_total{outcome}
newim_message_outbox_fanout_pending{state=pending|quarantined}
newim_webhook_delivery_pending{state=pending|retry|leased|dead_letter}
newim_webhook_deliveries_total{stage,result}
newim_observability_dropped_total{reason}
```

`component` is fixed to `send|outbox|webhook`; `stage` and `result` are fixed to
the enums above; `outcome` is `new|replayed|conflict`; `reason` is
`invalid_label|writer_error|trace_id_failure|queue_full|storage_unavailable`.
Business identifiers are never labels. Registry/sink series are bounded and
overflow is dropped with a fixed counter, never backpressure.

The two durable gauges are refresh-time read-only PostgreSQL snapshots. The
message gauge uses only `webhook_fanout_at`/`webhook_fanout_error`:
`pending` means both are null, and `quarantined` means `webhook_fanout_at` is
null with a non-null error. The Webhook gauge maps the authoritative
`im_webhook_deliveries.status` values `pending|retry|leased|dead_letter`; it is
not process-local cache or a completion guess. Query failure omits the gauge
and increments `storage_unavailable`; it is never exported as a guessed zero.

## Failure isolation

Metric updates happen after or alongside authoritative work and cannot return an
error or block it. Observer callbacks run through a bounded FIFO dispatcher;
queue overflow is dropped as `queue_full`, and panics are isolated and counted
as `writer_error`. A blocked observer therefore cannot backpressure send or
delivery, while observations retain submission order until the bounded queue
overflows. Slow writers are called after registry locks are released.
Source reads execute outside registry/observer locks with short deadlines.
Queue overflow drops a sample and increments `queue_full`. A storage snapshot
failure does not alter send, commit, ACK, outbox or Webhook delivery state.

## Compatibility and rollback

This decision adds no wire field, protocol version, ACK field, database schema,
migration, public route, auth/session behavior, media behavior, gateway, sync,
secret or limiter semantic. Existing HTTP `newim_http_*` metrics and the bounded
`route="message_send"` label remain unchanged. Rollback removes the observer
composition and source wiring; authoritative database and network behavior has
no dependency on the added samples.
