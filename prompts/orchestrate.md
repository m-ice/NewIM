# NewIM workstream orchestration

Project location: `/Users/luckyice/Desktop/NewIM工程/NewIM`; origin: `git@github.com:m-ice/NewIM.git`. Read `docs/16-project-location.md` and `tasks/manifest.json.repository` first. Work only in the product Git repository after baseline import. These instructions apply only when the user explicitly authorizes starting the task; path registration and committing files do not start orchestration.

Use `tasks/manifest.json` as the task graph and root `AGENTS.md` as policy.

For this run:
- choose up to the configured safe parallelism among dependency-ready tasks;
- delegate narrow investigations/implementations to matching project subagents;
- avoid overlapping write scopes;
- integrate only after each worker reports validation evidence;
- ask `reviewer` for a fresh defect-focused review and `qa` for reliability-sensitive changes;
- involve `security` and `license` whenever task flags require them;
- stop fan-out when an upstream protocol/schema/ADR decision is unresolved.

Prefer completing a small number of production-quality tasks over generating many unfinished modules.

Before selection, run `make check`. Use `scripts/ready_tasks.py`; dependency readiness is not permission to bypass manual blockers or pending acceptance plans. Follow `docs/15-control-plane-checks.md` for scope checks and completion evidence. Never mark done from an agent assertion alone.
