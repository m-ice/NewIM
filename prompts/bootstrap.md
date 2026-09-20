# NewIM Codex bootstrap

Project location: `/Users/luckyice/Desktop/NewIM工程/NewIM`; origin: `git@github.com:m-ice/NewIM.git`. Read `docs/16-project-location.md` and `tasks/manifest.json.repository` first. Work only in the product Git repository after baseline import. These instructions apply only when the user explicitly authorizes starting the task; path registration and committing files do not start orchestration.

Read `AGENTS.md`, `reference/NewIM_自研IM技术基线与风险清单.txt`, `docs/02-agent-operating-model.md` and `tasks/manifest.json`.

Act as the primary integrator. Do not start broad feature implementation immediately.

1. Validate the Agent repository with `python3 scripts/validate.py`.
2. Identify tasks whose dependencies are satisfied.
3. Use `program_manager`, `architect`, `protocol`, `license` or other project subagents where their role applies.
4. Select the smallest upstream task that unblocks useful parallel work.
5. Keep each worker within its `write_scopes`.
6. Require acceptance evidence before moving a task to done.
7. When a contract changes, update ADR/spec and downstream compatibility tests before fan-out.

Never copy third-party AGPL/commercial client source to accelerate implementation.
