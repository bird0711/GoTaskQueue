# AGENTS.md

This repository uses controlled AI development documents.

## Required Workflow

Before making any code changes, Codex must read these files in order:

1. `docs/rules.md`
2. `docs/status.md`
3. `docs/decisions.md`
4. `docs/next.md`

After reading the control documents, Codex must inspect the relevant source code before editing.

`AGENTS.md` is the canonical workflow document. `docs/rules.md` should only contain project-specific additions or clarifications, and should not duplicate or restate the workflow, risk controls, verification rules, or final-response requirements defined here.

## Document Roles

- `docs/rules.md`: project-specific additions and clarifications to these rules.
- `docs/status.md`: current project state and known issues.
- `docs/decisions.md`: durable technical and business decisions.
- `docs/next.md`: the only task Codex should execute now.
- `docs/log.md`: execution history.

## Task Control

- Only execute the task described in `docs/next.md`.
- Do not start tasks from `status.md`, `log.md`, backlog files, README, or comments unless the user explicitly asks.
- Do not do out-of-scope optimization or refactoring.
- Keep changes focused and small.

Before development starts, `docs/next.md` may be intentionally empty. An empty `docs/next.md` means no implementation task is currently assigned.

If the user explicitly assigns a task in the current conversation while `docs/next.md` is empty, that user request may be treated as the current task. For implementation work after development has started, prefer recording the task in `docs/next.md` before executing it.

## High-Risk Changes

Codex must stop and ask before:

- changing database schema
- changing authentication or permission logic
- changing payment or billing logic
- changing public API response formats
- introducing new dependencies
- deleting large amounts of code
- doing broad refactors

## Verification

After implementation, run relevant checks when available:

- tests
- lint
- typecheck
- build

If a check cannot be run, explain why.

## Documentation Updates

After completing the task:

- update `docs/status.md` only when project state has materially changed
- update `docs/decisions.md` only if a durable decision was made
- update `docs/log.md` only for meaningful implementation milestones when the record is useful later

Small changes do not need to be recorded in `docs/status.md` or `docs/log.md`.

## Document Maintenance

- Keep `docs/next.md` specific enough for execution, including scope, non-goals, and completion criteria.
- Periodically compact `docs/status.md` when it becomes historical rather than current-state focused.
- Do not record small implementation details in `docs/log.md` unless they are useful for future debugging, handoff, or release history.
- Do not write assumptions into `docs/decisions.md` as decisions. Mark unresolved items as open questions elsewhere or leave them out.

## Final Response

Report:

- changed files
- what changed
- checks run and results
- remaining risks
