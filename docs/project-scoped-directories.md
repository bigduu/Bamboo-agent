# Repository-scoped Bamboo directories

Bamboo keeps repository-bound runtime files below the Git root. These files
are the current Workspace overlay; they are not the first-class Project home
described in [Project identity and shared resources](project-identity.md).

```text
<git-root>/.bamboo/
├── settings.json
├── settings.local.json
├── worktree/<name>/
└── tmp/subagents/<child-id>/
```

`settings.json` remains suitable for source control. When Bamboo creates project runtime
directories it incrementally maintains `.bamboo/.gitignore` with `worktree/`, `tmp/`, and
`settings.local.json`; existing ignore entries are preserved.

Managed worktrees use a validated name containing only ASCII letters, digits, `-`, or `_`, and
the branch `bamboo/<name>`. Creation fails before invoking Git if either the destination or branch
already exists. The owner should call the remove API when the task ends; it runs
`git worktree remove --force` followed by `git worktree prune`. A retention-based garbage collector
recovers worktrees left behind after a process failure. It removes a checkout only when Bamboo's
ownership marker and directory have both expired and the checkout still has the exact expected
`bamboo/<name>` branch. Unowned, fresh, detached, or branch-mismatched directories are preserved.

Sub-agents with an explicit `storage_dir` keep that directory. Without one, a worker whose
workspace belongs to a Git project uses `.bamboo/tmp/subagents/<child-id>`. Workspace-less
broker/fabric workers retain the OS temporary-directory fallback. Project search tools exclude
`.bamboo/worktree/` so a checkout is not indexed recursively through its sibling worktrees.
