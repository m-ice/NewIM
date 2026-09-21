# ADR 0006: Send requests and persistence results v1

Status: accepted following independent architecture, code, QA and security review of the protocol design and implementation. Final task acceptance evidence is maintained separately.

## Compatibility

Add separate send/server frame APIs. Existing bare persisted Message APIs, their five error variants, error precedence, 65536-byte limit and depth32 remain unchanged. This first transport framing contract is additive. It does not implement authentication, database persistence, networking or SDK convergence. No new dependencies. Unknown body/outer fields are validated then ignored; payload extensions remain lossless and participate in intent equality. Future semantic fields require explicit version evolution.

## Wire and authority

A frame is an object with required protocolVersion (positive integer token <=2147483647), kind (string), body (object). Supported outer version is 1. Client direction permits only send; server direction permits send_ack, send_error, message. Unknown or wrong-direction kind yields PROTOCOL_UNSUPPORTED_FRAME.

Send body requires clientMsgId, conversationId, version, type, payload, with existing persisted field/payload rules. senderId, serverMsgId, conversationSeq, serverTime, status are forbidden at send outer/body including null and escape-equivalent keys; identical payload keys are ordinary data. Stable clientMsgId survives retries. Sender identity and conversation permission come exclusively from actual authenticated server context.

send_ack requires status=SERVER_PERSISTED, clientMsgId, conversationId, senderId, serverMsgId, conversationSeq, serverTime. IDs and decimal strings follow persisted rules. It does not accept SERVER_ACCEPTED/DELIVERED/READ. A producer may issue this result only from a committed message/summary/transactional-outbox transaction, or recovery of that same result. Codec success is not evidence of commit. Server acceptance must later test rollback/no ACK, disconnect after commit and concurrent retry recovery against a real database.

send_error requires clientMsgId, conversationId, code matching [A-Z][A-Z0-9_]{0,63}. Only reliably parsed requests may be correlated; no fabricated IDs. There is no exposed debug/message/token or server-controlled retryable field. SERVER_TEMPORARY_UNAVAILABLE means RetrySameIntent; AUTH_REQUIRED/AUTH_TOKEN_EXPIRED means AuthRecovery; all other codes, including unknown valid codes, mean StopAutomaticRetry. SDK owns bounded backoff and account/connection generation fencing; late errors must never revoke an observed persisted result.

message body uses the complete existing persisted envelope and codec. No heuristic acceptance of bare messages in framed APIs. Pure correlation validates request, trusted sender and result identity; message additionally matches version/type/payload intent. Comparing persisted results with the same sender/client/conversation requires identical server ID/sequence/time, otherwise SEND_RESULT_CONFLICT. Wrong identity or intent yields SEND_CORRELATION_MISMATCH. These helpers perform no storage or unread updates.

## Versioned intent equality

Permanent key is trusted senderId + clientMsgId. Equality compares protocolVersion, conversationId, schema version, type and full payload; caller separately checks the key. Unknown outer/body fields do not participate. Server must atomically retain original intent or a versioned lossless representation with the result, never infer it from an enriched output message. Equal intent returns the original result; unequal intent for the same key returns SEND_ID_CONFLICT without creating a new message or silently replacing clientMsgId.

Both operands are validated first. Object keys are decoded and compared as a set, arrays in order, strings by decoded UTF-8, booleans/null by type. Missing differs from null. No Unicode normalization. Exact numbers compare canonical sign/coefficient/exponent: remove insignificant leading/trailing coefficient zeros and adjust the signed decimal-string exponent by fractional length/trailing zeros. All signed zeros compare equal. No float, machine-size exponent conversion, or expansion by exponent value. Number tokens <=128 bytes; normalized exponent <=129 digits; token work and memory bounded by token length. Ordinary $serde_json::private::Number keys remain ordinary. Input payloads are not mutated or logged.

## Bounds and error precedence

Old limits stay intact. Frame maximum is 66048 bytes/depth33; exact raw body excluded, all metadata/whitespace is <=512 bytes. Body limits: send61440, ACK/error4096, message65536, all depth32. Unknown kinds use maximum65536 for common checks. Known text remains <=16384 UTF-8 bytes and raw numeric tokens <=128. A canonical message wrapper costs46 bytes, so an accepted maximum old body can be wrapped without shrinking its domain. Send budget reserves room for authoritative persisted metadata; future server enrichment must be separately bounded before commit.

Decode order: total bytes; whole raw JSON UTF-8/duplicate/number/depth scan left-to-right; outer fields/body object and send forbidden authority keys; metadata/body byte budgets; body raw bounds and full kind-specific field shape (including known ACK status/code shape); outer supported version; supported kind/direction; inner message version and known payload semantics. Unknown kind body has only common object/raw bounds. Inner version errors and text errors occur after outer version/direction. Encode checks public raw-size lower bound before allocating, validates independent raw fragments against injection, then uses the same full-frame validation. New send/ACK/error encoders use JSON wire escaping (quote, backslash and controls), without HTML/script escaping of <, >, &, U+2028 or U+2029, so encoded size versus invalid-field shape priority agrees across languages. Existing Message encoder behavior is unchanged. Error text contains only stable codes. New send errors wrap old codes without changing the existing Rust enum.

## Validation

Shared wire-string fixtures and paired intent fixtures; explicit Go packages and Rust integration targets for golden, errors and limits. Runtime constructions exercise exact byte/depth/token boundaries and adjacent invalid cases, maximum old-message wrapping, huge decimal exponents, malformed Unicode/duplicates, output injection, immutability, correlation arrival permutations and conflicting persisted results. Retain all existing persisted fixtures and native/wasm build/check gates. No claim that codec tests prove database commit or SDK state convergence.
