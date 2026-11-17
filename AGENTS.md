# Agent workflow

## Branches and worktrees

- Before starting each major change, create a new, dedicated Git branch and a new Git worktree for that branch. Perform the change in that worktree.
- Major changes include new features, substantial bug fixes, refactors, migrations, and changes spanning multiple components. When in doubt, treat the work as a major change.
- Use the `codex/` branch prefix and a descriptive name, for example `codex/improve-archive-scanning`.
- Create the branch and worktree before editing files, for example: `git worktree add -b codex/<change-name> <new-worktree-path> <base-branch>`.
- Do not reuse a branch or worktree from a different major change, or perform major changes directly on the default branch.

## Commits

- Commit every meaningful change as a separate, focused commit once its relevant checks pass. Do not defer all commits until the end of a larger task or combine unrelated changes.
- Use semantic commit messages following Conventional Commits: `<type>(<optional-scope>): <description>`, for example `feat(scanner): support nested archives` or `docs: define agent workflow`.
- Choose the type that describes the change, such as `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `ci`, or `chore`. Mark breaking changes with `!` and explain them in the commit body.
- Review the diff and stage only files belonging to that meaningful change. Preserve unrelated user changes and never commit secrets or generated artifacts unless they are intentionally tracked.
