# Supervisor current-user boundary (#1968)

The user's 2026-10-27 product correction supersedes the old requirement to
authorize each Project or attach each Session before Supervisor management.
The trusted Supervisor should discover, inspect and coordinate all resources
in the current application's user-data domain. This is not access to another
user, another data directory, an external account or operating-system authority.

## Role execution policy (#2334)

A Supervisor in either Standard or Ultra has its own exact orchestration
allowlist. It does not inherit ordinary Ultra Root's `Read`, `Grep`, `Glob`,
`GetFileInfo` or `ViewImage`. Writes, shell/processes, Skills/Workflows and
external/custom tools are also outside that surface. Use another Session for
workspace/file work; its own permissions and approval requirements still apply.

Provider schemas, capability discovery, native per-call dispatch, agentic
sub-actions and approval replay use this same derived role policy. A stale
catalog or retained permission approval does not override it. It grants no
identity: canonical Supervisor incarnation and concrete Host Session ports
remain required. Ordinary Roots and children keep their own existing policy.

Bounded Session/Work views and exact runtime-owned Plan artifact sections are
business observations, not general filesystem paths. #2304 retains its existing
read-only Planner Child, immutable revision and durable result/auto-resume path.

## Default management and legacy migration

The management/storage migration remains a separate required part of #1968;
the role-policy change alone does not implement default access. Canonical Host
authorization must use the current application user-data domain and live
Supervisor/target identity rather than filling an ACL from UI clicks.
New Projects and Sessions must be discoverable without another grant.

Preserve old scope/link history and operation receipts, accepted input,
Supervisor incarnation, target birth/run and notification deduplication. The
old Project allowlist must stop restricting default Supervisor management.
Worker and Observer target delegation remains explicit and narrow; those
Sessions do not inherit Supervisor's whole-domain management role. Do not
change a target's permissions, automatically answer its pending approval or
cancel its running work to perform the migration.

Do not tell users to authorize one Project at a time while the old Host path
is still installed. Report that actual Host capability/migration blocker.
No live user configuration, security setting, company data or release asset
is modified by this implementation or its isolated test fixtures.
