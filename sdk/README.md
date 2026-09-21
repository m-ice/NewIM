# SDK foundation

`core` is a std-only Rust library reporting SDK build identity. It compiles for
native and `wasm32-unknown-unknown` targets, without a C or JavaScript export ABI.
The current public API validates diagnostic source revision labels and preserves
unknown provenance. It does not implement messaging.

Platform facades and adapters will depend on core. UI, lifecycle, network and
storage APIs stay outside core; shared message rules will not be reimplemented
in adapters. Run the root `make build` and `make check` commands.
