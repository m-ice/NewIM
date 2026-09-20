# Agent operating model

## Roles

The primary Codex session acts as integrator. Specialized subagents inspect or change narrow areas. A task has one accountable owner and optional reviewers.

## Parallel work policy

Parallelize only tasks whose `write_scopes` do not overlap or whose shared contract is already frozen. Protocol/schema/DB contract tasks are upstream of implementation tasks. Platform wrappers may proceed in parallel once SDK Core interfaces are stable.

## Work cycle

1. Program Manager selects `ready` tasks from `tasks/manifest.json`.
2. Architect/Protocol/Data agents freeze the smallest contract needed by downstream tasks.
3. Implementation agent edits only listed scopes.
4. QA adds failure-path tests independently where possible.
5. Reviewer performs a fresh pass; Security/License participate when task flags require them.
6. Acceptance commands run locally/CI.
7. Handoff records public contract, migration notes, known limits and next dependents.

## Anti-patterns

- One agent modifies protocol, DB, server and all clients in one task.
- Multiple agents redesign the same interface in parallel.
- “Done” means code compiles but crash/retry/migration paths are untested.
- Review agent rewrites style instead of finding defects.
- Agent invents a third-party behavior not supported by public docs.
