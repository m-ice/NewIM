# Risk register

| Risk | Early control |
|---|---|
| ACK loss creates duplicates | immutable clientMsgId + unique constraint |
| Reconnect/session race | explicit user/device/session/connection/token identity |
| Local DB growth from gap recovery | bounded pages, deduped jobs, trim metrics |
| Login scans huge conversation set | cursor delta + bootstrap split |
| Presence becomes business truth | TTL + UNKNOWN state + best-effort contract |
| Push diverges from message state | push is hint; sync is final truth |
| Multi-agent code conflicts | write scopes + contract-first dependency graph |
| License contamination | provenance review + clean-room default |
| Premature infra complexity | measured benchmark gate before broker/sharding |
| Untraceable intermittent loss | traceId/clientMsgId/serverMsgId lifecycle tracing |
