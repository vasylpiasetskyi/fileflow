---
name: git
description: Git workflow rules
alwaysApply: true
---

# Git Rules

Claude must never run `git commit` on its own.

When a change is ready, Claude stages it (`git add` is fine) and outputs a
short, ready-to-use commit message. The user either tells Claude to commit
with it or commits themselves. This holds mid-plan and even when a
batch-commit workflow is otherwise in effect.

- One logical change per commit (normally one phase).
- Do not mix refactoring with new functionality.
- Update docs (`README.md`, `docs/PLAN.md`) in the same commit as the change
  they describe.
- Commit messages: imperative, concise, e.g. `Add transactional outbox publisher`.
