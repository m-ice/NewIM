# Versioning

Version independently:

- `serverVersion`
- `sdkVersion`
- `protocolVersion`
- `dbSchemaVersion`

Handshake reports platform, app version, SDK version, protocol version and device ID. Server may enforce a minimum supported version, but compatibility policy must avoid unnecessary same-day invalidation of older clients.

Production uses traceable stable releases/tags. Main/develop and prereleases are not production artifacts unless an explicit release exception is recorded.
