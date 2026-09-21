# Send and persistence result frames v1

The additive Go DecodeSend/EncodeSend and DecodeServerFrame/EncodeServerFrame APIs, and Rust send equivalents, validate independent JSON frames. Existing bare persisted-message APIs remain available.

See [ADR 0006](../../docs/adr/0006-send-ack.md) for authoritative fields, budgets, error precedence, exact-number intent equality and compatibility. A frame has protocolVersion=1, kind and object body. send is the client direction; send_ack, send_error and message are server direction. ACK status is exclusively SERVER_PERSISTED. Successful parsing never authenticates a sender or confirms database commit.

SameIntent/same_intent compares versioned send intent after validating both requests. Correlate/correlate requires caller-supplied trusted sender context. CompareResults/compare_results detects inconsistent persisted IDs/sequence/time; callers retain their own connection generation and durable state. Disposition/disposition derives retry policy locally from a stable error code; unknown codes stop automatic retry.

Run make send-protocol-golden, make send-protocol-errors, make send-protocol-limits and make build check. These validate codecs and pure helpers only; authentication, actual durable sends and SDK recovery remain separate implementation work.
