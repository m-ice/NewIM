# Core contracts

This directory contains protocol v1 schemas, compatibility fixtures and Go/Rust
codecs for persisted messages, send/ACK/error frames and closed media v1 metadata.
Domain logic remains independent of transport, SQL and wire models. Future
protocol schemas require their own explicit contract and compatibility tests.

See [architecture](../docs/architecture.md) for dependency direction.
