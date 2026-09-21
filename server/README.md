# Server foundation

`buildinfo` reads embedded Go build metadata without repository/network access.
`cmd/newim-buildinfo` prints it as JSON for artifact diagnostics. Unknown source
fields remain absent; invalid metadata produces a stable error code.

Future application services depend on domain rules and ports. Transport and SQL
belong to outer adapters. This foundation does not include a network server or
message persistence. Run the root `make build` and `make check` commands.
