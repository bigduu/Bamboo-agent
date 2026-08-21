# Skills and project instructions

Bamboo rescans skills when its skill store starts or is explicitly refreshed. Sources use this
precedence when the same skill id appears more than once (highest first):

1. `<workspace>/.bamboo/skills[-<mode>]`
2. `${BAMBOO_DATA_DIR}/projects/<project-id>/skills[-<mode>]`
3. `~/.bamboo/skills[-<mode>]`
4. `~/.agents/skills/**/SKILL.md`
5. installed plugin skills

Mode-specific Bamboo directories override their generic sibling. Same-tier collisions keep the
first deterministic discovery result and emit a warning. Invalid, unreadable, or missing skills
are logged and skipped without preventing other skills from loading. Discovery never follows
directory symlinks.

Legacy repository workflow files in `<workspace>/.bamboo/workflows/*.md` are
discovered read-only and appear in the workflow catalog with `legacy: true`
and `migration_status: "available"`. Plugin `workflows/*.md` files use the
same adapter in place; installation never copies them into the user's global
workflow directory. A legacy file without a description receives a placeholder
description and is explicit/manual-only, never automatically invoked.

Migration is an explicit clone through
`POST /api/v1/bamboo/workflow-catalog/<id>/migrate`, using a trusted session id
to select the current workspace. It creates
`<workspace>/.bamboo/skills/<id>/SKILL.md`, never modifies or deletes the
legacy source, never overwrites an existing target, and is idempotent. The
optional request `description` replaces the placeholder; otherwise the
migrated bundle remains manual-only. The catalog reports the canonical winner
as `migration_status: "migrated"` and retains the legacy adapter as a shadowed
diagnostic.

Product clients build one metadata-only Workflow Library from instruction
entries returned by `GET /api/v1/commands` and orchestration/legacy entries
returned by `GET /api/v1/bamboo/workflow-catalog`. Neither listing contains
instructions or resource bytes. Changes, invalid definitions, and LKG recovery
for both namespaces publish `workflow.changed`, `workflow.invalid`, or
`workflow.recovered` on the account feed; the 30-second client cache is only a
fallback when the event stream is unavailable.

Versioned builtins are read-only. A client may clone one exact builtin through
`POST /api/v1/bamboo/workflow-catalog/<id>/clone` with
`{"source":"builtin","revision":<n>,"target":"user"}` or target
`"project"` plus a trusted `session_id`. Bamboo resolves every destination
from server-owned state, writes the bundle outside the scanned skills tree,
fsyncs it, and atomically renames it into place. A stale source, existing
override, divergent recovery marker, symlink, or client filesystem path is
rejected; the builtin is never changed. Interrupted publications resume only
when their source revision and content digest still match.

Explicit instruction activation uses the typed `POST /api/v1/chat` field
`workflow_selection: {id, source, revision, args}`. The request never carries
expanded instructions. Bamboo validates arguments, pins the exact immutable
bundle before acknowledging the chat turn, and persists the candidate snapshot
with the session so a restart between chat and execute cannot substitute a
newer revision. Stale or missing identities fail before the user message is
committed. `GET /api/v1/sessions/<id>` returns the public-safe
`active_workflow` identity after activation; list rows remain lightweight.

[Claude Code Skills and legacy custom-command compatibility](https://code.claude.com/docs/en/slash-commands)
remain rooted in `.claude/skills` and `<workspace>/.claude/commands`; Bamboo
does not introduce or label a `.claude/workflows` convention. Repository
Skills also follow the documented
[Codex Skill bundle model](https://learn.chatgpt.com/docs/build-skills).

For repository instructions Bamboo finds the nearest Git workspace boundary and reads applicable
`AGENTS.md` and `CLAUDE.md` files from that root down to the active workspace directory. Root rules
are injected first and deeper rules later, so the most specific scope can refine the repository
defaults. Files above the Git workspace and symlinked instruction files are ignored. A Git worktree
has its own `.git` boundary and receives the same checked-out repository instructions. This context
is assembled by the shared runtime prompt path used by both primary and child agents.
