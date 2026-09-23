# Server implementation

`buildinfo` reads embedded Go build metadata without repository/network access.
`cmd/newim-buildinfo` prints it as JSON for artifact diagnostics. Unknown source
fields remain absent; invalid metadata produces a stable error code.

The server tree now contains application and PostgreSQL storage packages for
messages, auth/session, conversation/message sync, and internal media metadata.
For example, `server/message` validates persistence intent and
`server/storage/message` implements the PostgreSQL message/latest-pointer/outbox
transaction; auth, sync, and media services follow the same application/storage
split. These are internal packages and require a trusted caller/identity.

There is no public network server or UI, HTTP/WebSocket gateway, push delivery,
or complete multi-device login/reconnect policy. Message persistence is
implemented, but it is not exposed by a network API. Run the root `make build`,
`make check`, and `make docs-check` commands. See
[architecture](../docs/architecture.md) and
[third-party notices](../THIRD_PARTY_NOTICES.md).
