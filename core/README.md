# Core contracts

This directory reserves the architectural boundary for server domain rules and
language-neutral protocol contracts. It currently contains no implementation.
Domain logic will not depend on transport, SQL or wire models. Protocol schemas,
compatibility fixtures and codecs require their own explicit contract.

See [architecture](../docs/architecture.md) for dependency direction.
