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

Builtin workflows are cloned through
`POST /api/v1/bamboo/workflow-catalog/<id>/clone`. Bamboo writes the bundle in
the private same-filesystem `.workflow-clone-txn` directory, journals bounded
`prepared -> stage_bound -> staged -> complete` transitions, and publishes it
with an atomic no-replace rename. An exact retry resumes every fully journaled
phase; ambiguous partial state or an identity mismatch fails closed without
mutating it. A concurrent target wins unchanged after Bamboo durably records `aborted`.
If a completed clone directory is later deleted, Bamboo records `retired`
before starting a bounded replacement epoch; a different directory generation
at the public name is never retired, adopted, or deleted.

Migration is an explicit clone through
`POST /api/v1/bamboo/workflow-catalog/<id>/migrate`, using a trusted session id
to select the current workspace. It creates
`<workspace>/.bamboo/skills/<id>/SKILL.md`, never modifies or deletes the
legacy source, never overwrites an existing target, and is idempotent. The
optional request `description` replaces the placeholder; otherwise the
migrated bundle remains manual-only. The catalog reports the canonical winner
as `migration_status: "migrated"` and retains the legacy adapter as a shadowed
diagnostic.

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
