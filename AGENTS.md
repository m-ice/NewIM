# NewIM repository instructions

## Mission

Build NewIM as an independent, commercial-grade IM platform for 澜遇科技. Reliability and recoverability come before feature count.

## Non-negotiable rules

1. Do not copy AGPL/commercial client implementations, reverse-engineered protocols, or proprietary code. Learn from public behavior and architecture only unless a reviewed dependency is explicitly approved.
2. `clientMsgId` is stable across retries. A reliable send is not successful before server persistence.
3. WebSocket is the realtime path; database + sync is the recovery truth. Push is never the message truth source.
4. Ordering uses server-assigned `conversationSeq`; client timestamps never define final order.
5. Sync is idempotent, paginated, resumable, and bounded. Bootstrap and delta sync are separate flows.
6. Business permission checks are authoritative on the server. Clients are untrusted.
7. Persistent side effects use a transactional outbox or an equivalent durability mechanism.
8. Local client storage must support migration, trimming, corruption recovery, indexes, and rebuild.
9. Every externally visible behavior needs stable error codes and protocol/schema versions.
10. No module is complete without observability and tests appropriate to its failure modes.

## Engineering style

- Prefer small modules with explicit interfaces. Avoid god services, shared mutable globals, and utility dumping grounds.
- Keep domain logic independent from transport and storage adapters.
- Public APIs require concise Chinese + English comments when the intent is not obvious. Comments explain *why/contract*, not line-by-line syntax.
- Use straightforward production naming. Avoid phrases such as “AI generated”, “smart magic”, “ultimate”, “best practice” in code/comments.
- Never leave fake implementations, silent TODO fallbacks, or tests that only assert mocks were called.
- New dependencies require a reason, license check, maintenance check, and size/security consideration.
- Secrets, tokens, message bodies, phone numbers, precise location and long-lived signed URLs must not appear in production logs.

## Architecture boundaries

- `core/domain`: protocol-neutral domain rules and state machines.
- `core/protocol`: versioned wire models and compatibility tests.
- `server/*`: server applications/services; no client storage assumptions.
- `sdk/core`: client state machine, sync, outbox, local-store interfaces; no UI dependency.
- platform wrappers/adapters only translate platform concerns; they do not reimplement core message semantics.
- premium features depend on OSS extension interfaces. OSS core must not import premium modules.

## Change discipline

Before coding:
1. Read the task entry in `tasks/manifest.json`.
2. Read relevant specs/ADRs and the nearest `AGENTS.md`.
3. State affected modules and compatibility impact.
4. For protocol/db/public API changes, create or update an ADR first.

Before finishing:
1. Run the task acceptance commands and relevant tests.
2. Check idempotency, retries, crash/restart, duplicate delivery, ordering and migration impact where relevant.
3. Update docs/specs if behavior changed.
4. Produce a handoff using `templates/handoff.md` when another workstream depends on the change.
5. Do not mark a task done when required commands were not run; report the exact gap.

## Review priorities

Correctness > data safety > security/privacy > compatibility > operability > performance > style.

## Source of truth

The user-provided baseline is preserved at `reference/NewIM_自研IM技术基线与风险清单.txt`. If a task conflicts with it, stop the conflicting change and record an ADR or explicit product decision instead of silently overriding the baseline.

## Control-plane acceptance

- Follow `docs/15-control-plane-checks.md` and manifest schema v2. Resolve pending acceptance checks before starting implementation.
- Run `make check` for control-plane changes. Never launch product services or task orchestration unless the user authorizes that work.
- Distinguish manual blockers from dependency waits. Do not bypass a manual blocker.
- Keep tasks in review until command results and all required reviewer evidence are available for the tested commit. Do not invent evidence or sign for another reviewer.
- Register scope expansions separately before using them. PR bodies must declare each task on a `Task: NIM-XXX-001` line.

## Project location

- Product repository: `/Users/luckyice/Desktop/NewIM工程/NewIM`.
- Origin: `git@github.com:m-ice/NewIM.git`.
- Original control-plane source: `/Users/luckyice/Desktop/NewIM工程/NewIM-Agent-Engineering`.
- `tasks/manifest.json.repository` records these locations. After baseline import, tracked files in the product repository are authoritative; do not implement product code in the original control-plane directory.
- All task write_scopes are relative to the product repository root. See `docs/16-project-location.md` before changing files or running task commands.
- Registration or a Git commit does not authorize starting agents, services, or product tasks.
