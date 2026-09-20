# Client matrix

| Target | Primary responsibility | Shared truth |
|---|---|---|
| Flutter | Cross-platform facade/reference UI | SDK Core |
| iOS | lifecycle, APNs, audio session, native bridge | SDK Core |
| Android | lifecycle/doze, vendor push, network, native bridge | SDK Core |
| HarmonyOS | lifecycle/push/network, ArkTS bridge | SDK Core |
| uni-app | UTS/JS plugin facade | SDK Core or supported native binding |
| Windows/macOS/Linux | desktop storage/network/packaging | SDK Core |
| Web | browser transport/storage constraints | protocol + compatible store/sync semantics |

A `ChatPage` or equivalent view must not own WebSocket, dedup, unread truth or a second conversation listener.
