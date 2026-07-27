# Project identity and shared resources

A Bamboo Project is a stable user-local identity shared by sessions. It is not
derived from a workspace, repository name, remote URL, or path hash. A session's
`project_id` remains unchanged when its current workspace moves between a main
checkout, linked worktree, subdirectory, or an unregistered temporary directory.

Projects live below the configured Bamboo data directory:

```text
${BAMBOO_DATA_DIR}/projects/<opaque-project-id>/
├── project.json
├── settings.json
├── skills/
├── skills-<mode>/
├── commands/
├── memory/v1/
├── artifacts/
└── state/
```

`project.json` is authoritative. `projects/index.json` is rebuildable. Project
updates use revision/ETag compare-and-swap. The opaque ID, not the display name,
is the directory component, so renaming a Project does not move its resources.
Archived Projects keep their sessions and resources.

## Project and Workspace

Every new active Project has one canonical `project_path`: the user's source
folder and the Project's default execution directory. It is distinct from
`project_home`, Bamboo's private resource directory shown above. A Project can
also register additional workspace/worktree roots. All roots own their
descendant paths. Bamboo rejects a Project path CAS update, session assignment,
or Workspace tool change when the confinement-resolved destination belongs to
another Project. An unregistered directory remains an ephemeral workspace and
is not added to the registry.

Assigned sessions resolve their effective workspace with one precedence:

```text
explicit request/tool workspace
  > persisted session workspace
  > Project.project_path
```

Resolution stops there. Global `default_work_area` and session-scoped temporary
directories are compatibility fallbacks only for Unassigned/legacy sessions.
Missing, moved, non-directory, or confinement-relocated Project paths fail
closed with `project_path_missing` or `project_path_unavailable`; they never
drift to a global, foreign-Project, or temporary directory.

Resource precedence is:

```text
builtin < global/user < Project home < current Workspace < session activation
```

Workspace-local overlays remain in `<git-root>/.bamboo/`. Project skills and
commands are shared by all Project sessions; Workspace skills and commands can
override them. Deterministic workflows remain `workflow.yaml` files inside skill
bundles—there is no standalone Project `workflows/` directory.

For repository compatibility only, a Workspace may contain legacy read-only
`.bamboo/workflows/*.md` sources. They can be explicitly cloned into that
Workspace's `.bamboo/skills/` directory; migration never creates a Project-home
`workflows/` directory or writes legacy content into Project storage.

Project memory and Dream data are stored in
`projects/<id>/memory/v1`. Assigned sessions never derive a write scope from a
workspace path. Legacy unassigned sessions may read old path-derived memory
during migration, but cannot create new Project-scoped writes.

## API and propagation

The `/api/v1/projects` API creates, lists, updates, binds, unbinds, inspects,
and archives Projects. Create requires `project_path`; list/detail return it;
PATCH can change it without changing `project_id`. Mutations require the
current revision through `If-Match`. The current primary path cannot be
unbound—select a replacement with Project CAS first. Session
create/list/detail and chat contracts expose `project_id`; explicit session
reassignment also requires `If-Match` and is rejected while the session is
running.

Child, resident, guardian, remote actor, schedule, connect, headless, TUI, and
SDK creation paths propagate the typed Project ID. Normal chat and Workspace
changes never reassign it.

The system prompt uses separate Project and Workspace marker blocks. The
Project block distinguishes `Project path` from `Project home (Bamboo data)`;
the Workspace block reports the effective path plus `explicit`, `session`, or
`project_default` source. Project identity/path remain stable while ordinary
Workspace changes replace only the Workspace block. Resource counts and
revisions are dynamic per-round context, not part of the cacheable identity
prefix. Prompt and resource APIs expose only redacted names, status, counts,
and revisions—never MCP headers, environment values, or credential secrets.

## Legacy migration

Migration dry-runs match only exact canonical bindings or a safely resolved
common Git directory. Ambiguous names, missing paths, remote URLs, and path
hashes remain Unassigned. Memory migration is copy/verify/commit, resumable and
idempotent; it does not overwrite Project-home documents or delete the legacy
source. A Project can retain read-only `legacy_project_keys` aliases during the
migration window. Manifest v1 migration promotes an old binding only when
exactly one exists. Zero bindings remain `needs_configuration`; multiple
bindings remain `needs_selection` and are never resolved by vector order or a
`main` label.
