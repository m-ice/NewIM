# Connection state machine

States: `IDLE`, `CONNECTING`, `AUTHENTICATING`, `SYNCING`, `READY`, `RECONNECTING`, `KICKED`, `TOKEN_EXPIRED`, `CLOSED`.

Connection identity includes `userId`, `deviceId`, `sessionId`, `connectionId`, `tokenId`. Reconnect uses exponential backoff + jitter with foreground acceleration while respecting platform background restrictions.
