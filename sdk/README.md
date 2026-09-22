# SDK foundation

`core` is a std-only Rust library reporting SDK build identity. It compiles for
native and `wasm32-unknown-unknown` targets, without a C or JavaScript export ABI.
The current public API validates diagnostic source revision labels and preserves
unknown provenance. It does not implement messaging.

Platform facades and adapters will depend on core. UI, lifecycle, network and
storage APIs stay outside core; shared message rules will not be reimplemented
in adapters. Run the root `make build` and `make check` commands.

## Client send outbox

`sdk/core` now exposes the protocol-neutral outbox/send state machine, bounded version-1 pending
envelope, retry policy and exact persisted-ACK resolution contract. Platform adapters must
validate wire frames and map them to `SendIntent`, `PersistedAck`, `SendFailure` and
`SendContext`; core does not parse JSON, own a socket or import SQLite.

The additive `PendingMutationStore` port provides revision-CAS pending update/removal without
changing `LocalStore` or exhaustive `Action`. `remove_pending` is reserved for explicit terminal
or auth-recovery dismissal. The native SQLite adapter implements this port without schema
migrations and does not parse pending bytes.

Run the registered real-binary gates:

```sh
make sdk-outbox-restart
make sdk-outbox-retry
make sdk-outbox-ack
make sdk-outbox-terminal
make sdk-outbox-check
```

See [ADR 0010](../docs/adr/0010-sdk-outbox-send-state.md) and
[the outbox specification](../specs/sdk/outbox.md).
