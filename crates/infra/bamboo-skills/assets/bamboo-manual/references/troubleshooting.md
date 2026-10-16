# Safe troubleshooting

Start with a symptom, expected result, installed package/source identity and
the smallest authorized observation. Keep product knowledge separate from
instance evidence. If time, scope, revision or coverage is missing, say what is
unknown. This chapter does not collect diagnostic data automatically.

## A setting did not take effect

Check whether the key refers to the compatibility view or the typed section
authority. Consider an in-memory environment or command-specific CLI override.
Distinguish persisted, facade-observed desired and consumer-applied state.
Use the existing provider/MCP application receipt only within its stated
coverage. A GET does not force a reload; an in-sync client is not proof of a
successful remote request. Do not restart, reload, migrate or rewrite files as
an unrequested “diagnostic”.

## Provider or MCP appears healthy but a task fails

Check exactly what the observation means. Configuration validation, local
client construction, tool-index publication, current connection health and
remote completion are separate outcomes. Request the narrow existing error
code/status needed to distinguish them; don't assume live-health tests a model
or tool. Avoid copying raw endpoint addresses, credential references, secrets
or arbitrary error text into model context.

## Memory is missing or surprising

Check current Project scope and injection/recall flags before reasoning about
index quality. Lexical recall, optional model rerank, Dream freshness and
prompt budgets are distinct. Counts can establish whether candidates or
injected items existed without exposing their bodies. Never move Project facts
to Global merely to bypass unavailable scope. Dream is derived context, not
proof. Read `memory.md`; don't create a fallback memory file or run reindexing
without a separate authorized need.

## A hook did not run

First distinguish request hooks, native lifecycle hooks and portable plugin
hooks. Inspect the appropriate enablement/review report. Portable bundles may
need exact-byte review, be disabled or unsupported; installation alone is not
execution consent. Check timeout units. A dry-run can execute arbitrary
commands and is not read-only inspection. Never enable a hook to preview it.

## The manual or a chapter cannot be read

Check Skill eligibility/disablement and the package identity returned by the
current catalog. A same-named user skill may shadow a builtin; don't claim
user-edited bytes are the immutable embedded manual. Prefer source= builtin
when verifying this bundle's provenance. Respect denial; do not bypass it by
reading private filesystem paths. A missing resource, stale cursor or truncated
legacy page is incomplete evidence; repeat an authorized read with a fresh
cursor or smaller page, or report the gap. Never silently replace it with docs
for another version.

## Existing command limits

`bamboo doctor` checks configuration/provider credential availability and has a
server-reachability probe. It is not a comprehensive runtime diagnosis and has
no general JSON diagnostic-output contract in this version. `healthz`/`readyz`
report basic health/version, not downstream provider/MCP functional validation.
Check the installed `--help` before suggesting command flags.

Default diagnostic advice should use allowlisted static labels, counts,
booleans and non-secret revisions. Raw config, logs, memory bodies, private
paths, endpoints and identifiers are not safe merely because obvious keys were
masked. Any runtime adapter should remain an explicitly scoped read-only
projection over existing observations; this manual introduces none.

Source: `src/setup_cli.rs`, `src/bin/bamboo.rs`,
`crates/app/bamboo-server/src/handlers/agent/health.rs`,
`docs/guides/configuration-application-state.md`, `docs/plugin-command-hooks.md`,
and the implementation paths cited by the other chapters.
