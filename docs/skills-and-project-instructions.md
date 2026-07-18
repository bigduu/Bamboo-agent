# Skills and project instructions

Bamboo rescans skills when its skill store starts or is explicitly refreshed. Sources use this
precedence when the same skill id appears more than once (highest first):

1. `<workspace>/.bamboo/skills[-<mode>]`
2. `~/.bamboo/skills[-<mode>]`
3. `~/.agents/skills/**/SKILL.md`
4. installed plugin skills

Mode-specific Bamboo directories override their generic sibling. Same-tier collisions keep the
first deterministic discovery result and emit a warning. Invalid, unreadable, or missing skills
are logged and skipped without preventing other skills from loading. Discovery never follows
directory symlinks.

For repository instructions Bamboo finds the nearest Git workspace boundary and reads applicable
`AGENTS.md` and `CLAUDE.md` files from that root down to the active workspace directory. Root rules
are injected first and deeper rules later, so the most specific scope can refine the repository
defaults. Files above the Git workspace and symlinked instruction files are ignored. A Git worktree
has its own `.git` boundary and receives the same checked-out repository instructions. This context
is assembled by the shared runtime prompt path used by both primary and child agents.
