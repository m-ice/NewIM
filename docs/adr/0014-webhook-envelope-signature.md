# ADR 0014: Webhook v1 envelope and signature

Status: accepted design; implementation is limited to the Go reference contract and
compatibility fixtures in this change. It does not implement HTTP delivery,
endpoint management, Bot identity, Push, or a replay cache suitable for production.

## Context

The transactional outbox already stores a durable event ID with a persisted server
message, but it deliberately does not define an outbound webhook wire format. The
webhook consumer must be able to authenticate a request, identify a retry without
depending on message order, reject replay, and tolerate additive fields. The
signature cannot depend on arbitrary JSON re-serialization because object key order
and whitespace are not stable across implementations.

## Decision

The outbound contract is `webhook-v1`:

- The body is a JSON object with exactly the semantic fields `eventId`, `eventType`,
  `occurredAt`, `schemaVersion`, and `payload`. `schemaVersion` is 1; `occurredAt` is
  a canonical nonnegative decimal Unix-millisecond string. `payload` is a JSON object.
  Unknown object fields are additive and ignored by v1 consumers.
- JSON uses the existing strict parser rules: UTF-8, no duplicate keys in any object,
  no unpaired surrogate escapes, no trailing token, 65,536-byte maximum body and
  maximum container depth 32.
- Security headers are case-insensitive:
  `X-NewIM-Event-Id`, `X-NewIM-Delivery-Id`, `X-NewIM-Timestamp`,
  `X-NewIM-Nonce`, `X-NewIM-Key-Id`, `X-NewIM-Signature-Version`, and
  `X-NewIM-Signature`. Each must occur exactly once, with no control or non-ASCII
  bytes. `X-NewIM-Signature-Version` is `1`; the signature value starts with `v1=`.
- IDs use the persisted identifier grammar, key IDs use the same ASCII grammar up to
  64 bytes, timestamps are canonical decimal milliseconds, and nonces are 16 random
  bytes encoded as 22-character unpadded base64url. HMAC secrets are at least
  32 bytes.
- The canonical signing bytes are the following ASCII fields joined by one LF and
  terminated by one final LF, in this exact order:
  `v1`, `keyId`, `deliveryId`, `eventId`, `timestamp`, `nonce`, lowercase
  hex SHA-256 of the raw body bytes. The signature is unpadded base64url of
  HMAC-SHA256 over those bytes, prefixed with `v1=`.
- Each HTTP attempt gets a fresh timestamp and nonce. `eventId` and `deliveryId`
  remain stable across retries and lease takeover. A consumer accepts a timestamp
  within 5 minutes of its trusted clock, atomically reserves the nonce for the full
  validity interval (plus one millisecond past the inclusive oldest boundary), and
  treats the delivery/event identity as the business idempotency key. The verifier
  may perform bounded size/JSON syntax preflight before resolver lookup, but it must
  perform the constant-time HMAC comparison before semantic envelope decoding and
  requires the decoded `eventId` to equal the signed header;
  callers that use multiple key IDs must resolve the secret through an explicit
  `WebhookKeyResolver`. Replay reservation failure is fail-closed; capacity
  exhaustion must not evict an unexpired nonce. A bounded single-process reference
  guard is provided for tests, not as the production shared cache. The guard treats
  `expiresAt` itself as still reserved and reclaims only after `now > expiresAt`;
  a short resolver key is a signature/configuration failure, not replay unavailability.
- Stable webhook errors are separate from persisted-message errors:
  `WEBHOOK_INVALID_JSON`, `WEBHOOK_INVALID_ENVELOPE`, `WEBHOOK_ENVELOPE_TOO_LARGE`,
  `WEBHOOK_ENVELOPE_TOO_DEEP`, `WEBHOOK_UNSUPPORTED_SCHEMA`,
  `WEBHOOK_INVALID_HEADERS`, `WEBHOOK_INVALID_SIGNATURE`,
  `WEBHOOK_TIMESTAMP_OUT_OF_WINDOW`, `WEBHOOK_REPLAY_DETECTED`,
  `WEBHOOK_REPLAY_UNAVAILABLE`, `WEBHOOK_REPLAY_CAPACITY_EXCEEDED`,
  `WEBHOOK_UNKNOWN_KEY`, `WEBHOOK_KEY_UNAVAILABLE`, and
  `WEBHOOK_IDENTITY_MISMATCH`.

## Compatibility and limits

This is additive to the persisted message protocol and does not change send/ACK,
storage, auth, sync, or media behavior. The body is for an HTTP webhook consumer;
the existing message codecs remain unchanged. The reference implementation uses
only the Go standard library. A future durable replay guard must provide the same
atomic reservation contract. Unknown schema versions fail closed in v1; additive
unknown fields in a v1 envelope are ignored.

## Tests

`make webhook-envelope` validates canonical encoding, fixed HMAC vectors, additive
unknown fields, key rotation vectors, inclusive clock boundaries, and the positive
verifier path. `make webhook-envelope-security` validates tampering, wrong or
retired keys, non-canonical signature encoding, header/body identity mismatch,
invalid encodings/headers, resolver short-circuiting and transient failures,
concurrent nonce reservation, unknown schema versions, oversized encoder input,
and fail-closed capacity. Both targets use the fixed fixture under
`tests/compatibility/webhook/`.
