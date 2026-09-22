# Server foundation

`buildinfo` reads embedded Go build metadata without repository/network access.
`cmd/newim-buildinfo` prints it as JSON for artifact diagnostics. Unknown source
fields remain absent; invalid metadata produces a stable error code.

Future application services depend on domain rules and ports. Transport and SQL
belong to outer adapters. It includes message, auth-session, conversation-sync and media
application packages with storage adapters, but no public network server or
gateway. Media stores metadata and grant digests; object URLs are never persisted
and public upload/download endpoints, moderation and retention are separate. Run the root `make build` and `make check` commands.
