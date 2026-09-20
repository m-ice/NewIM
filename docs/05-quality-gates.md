# Quality gates

## Pull request gate

- Build/lint/static checks pass for touched modules.
- Unit + integration tests cover changed behavior.
- Protocol/public API changes include compatibility fixtures and version impact.
- DB changes include forward migration, rollback/repair strategy and migration test.
- Security-sensitive changes receive `security` review.
- Third-party or copied snippets receive `license` review.
- Metrics/logging updated for new failure modes.
- No secrets or full private message bodies in logs/fixtures.

## Commercial stable release gate

Must demonstrate at minimum: stable clientMsgId retry dedup, reconnect catch-up, outbox recovery after app kill, seq-gap recovery, unread correction, push/sync convergence, server-side block/permission enforcement, tested DB migration, token-expiry/kick state handling, gateway restart recovery, push/webhook outage isolation, traceability by serverMsgId, P95/P99 benchmarks, restore drill, protocol compatibility tests and real-device background/weak-network tests.

## Implemented control-plane gate

`make check` validates task contracts, graph/release coverage, workstream membership and done evidence, then runs control-script regression tests. PR CI also checks changed paths against declared task scopes at base and head. See `docs/15-control-plane-checks.md`.

The product gates above remain requirements, not currently executed checks. Pending acceptance plans must become concrete commands/evidence requirements before implementation. A green control-plane CI is not a product quality certification.
