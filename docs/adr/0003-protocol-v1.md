# ADR 0003: Persisted message protocol v1

Status: accepted design; implementation requires compatibility testing.

影响 / Impact: first protocol release; no existing wire API is changed. This codec provides parsing and validation only, without sending messages or asserting persistence.

Decision:

The protocol is independently specified; it does not import server storage, network transport, or UI.

Persisted message envelope: protocolVersion=1, version=1 for known text schema, clientMsgId/serverMsgId/conversationId/senderId (nonempty ASCII [A-Za-z0-9_-], <=128 bytes), conversationSeq canonical nonnegative decimal string within signed BIGINT, type nonempty ASCII [a-z][a-z0-9_]* <=64 bytes, serverTime canonical nonnegative decimal millisecond string within signed BIGINT, payload JSON object. This describes persisted messages only; a successful Decode does not acknowledge persistence or authorize sends. Sequences permit zero as a representation; allocation rules are server domain task.

Known text payload: text string with 1..16384 UTF-8 bytes, no normalization/trimming. Unknown payload fields allowed. Empty text invalid. Future message types or unknown text schema versions preserve raw payload and yield Supported=false; protocolVersion !=1 rejected. Known text version1 validates text. Envelope fields unknown to v1 tolerated and may be dropped on re-encode. Unknown payload preserved as JSON value, not byte-for-byte whitespace.

Every required field must exist once, not null, with exact JSON type. version/protocolVersion positive integer JSON tokens <=2147483647, no exponent/fraction accepted. Other numbers anywhere are valid RFC8259 lexical tokens with token length <=128 bytes and finite mathematical representation not required for opaque payloads (raw tokens retained). Go and Rust must reject duplicate object keys at ANY depth (escape-equivalent keys included), invalid UTF-8, unpaired UTF-16 surrogate escapes, trailing JSON. Limits before semantic parse: 65536 encoded input bytes inclusive, maximum container nesting32 including outer object. Encode applies same limits and validation, preserves supported core values and opaque payload semantics. Raw JSON retention in Go and Rust must not silently round integers.

Stable error strings: PROTOCOL_INVALID_JSON, PROTOCOL_INVALID_MESSAGE, PROTOCOL_UNSUPPORTED_VERSION, MESSAGE_TOO_LARGE, PROTOCOL_NESTING_EXCEEDED. Size first; raw syntax/depth next; complete envelope presence/type/shape next; unsupported protocolVersion next; known payload last. Golden combined errors fix this priority. Error text contains no message content. Unknown type is a successful opaque envelope, not a parser error, and never automatically executed/rendered.

Go standard library decoder structural token walk checks duplicates, strings and valid unicode escapes before typed decode; use json.Number/RawMessage for opaque numeric preservation. Rust serde_json configured raw_value, storing payload as RawValue following strict raw token scanning for duplicates/depth/number length; parser rejects invalid Unicode. Never round or reformat opaque number tokens through serde_json::Number/Value. Shared raw fixtures enforce exact cross-language behavior and semantic equivalence of known messages (not identical whitespace, object order or legal escapes). Decimal sequence/time strings remain canonical. Include adversarial ordinary payload object key $serde_json::private::Number to ensure it is not interpreted as parser metadata; no wire field is reserved for serde internals. No network transport/database state or arbitrary custom type execution.

Tests include canonical encode/decode text, unknown fields/types/schema versions, numeric boundaries 0/9/10/99/100/2^53±1/MAX, bad sequence forms, missing/null, version shape, duplicate escaped keys top/nested, malformed strings and surrogate escape, invalid UTF8, trailing input, max byte/depth within/outside, encode invalid objects, opaque big number preservation and fuzz no-panic/roundtrip invariants.
