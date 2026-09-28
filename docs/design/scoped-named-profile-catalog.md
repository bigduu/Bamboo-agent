# Session-scoped named-profile catalog

This read-only host catalog discovers schema-v1 definitions from Global and
the durable Session's exact active Project. It does not apply profiles, create
Children, expose built-in role bodies, choose models, or grant tools. Those
execution boundaries remain #909/#1315; #912 tracks other discovery sources.

## Authority and API

`GET /api/v1/sessions/{session_id}/named-agent-profiles` requires existing host
authentication or LocalBypass. Remote Open and Codex response-only tokens are
not catalog authority. Query selectors are rejected. The host loads the exact
Session through `AppState.storage`, classifies its durable Project identity,
and requires the exact `ProjectStore.get` manifest to be Active. Invalid,
missing, foreign, archived, or unrecoverable authority fails closed with a
static error. A truly unassigned Session discovers Global only.

Global comes from this AppState's data home. Project comes only from
`project_store.paths().project_home(id)`, independent of workspace configuration.
Session cache, request Project/path/workspace, and prompt content have no role.
Existing Storage acquisition remains unchanged. Existing ProjectStore.get may
create lock files, migrate/normalize, quarantine corrupt manifests, and recover
a valid backup. Successfully recovered exact Active authority is accepted;
the catalog itself adds no writer, directory creation, repair, or recovery.

## One immutable observation

The same scan yields public metadata and a private retained definition map.
Identity is `{name, source: global|project, project_id: null|exact_id, revision}`;
revision is the original file SHA256. Exact lookup compares every field and
never reopens a file. A later file replacement cannot change an old selected
body. This is an observation, not an atomic filesystem snapshot or live grant.
Definitions are immutable, non-Serialize, and Debug displays safe metadata only.

A valid Project name shadows the same Global name. Known Project duplicates
block that name's Global fallback; unrelated valid names remain selectable.
An anonymous invalid Project candidate or rejected Project scan makes the whole
catalog unavailable: private lookup returns nothing and no public row remains
selectable. There is no LKG, cache generation, alias, or filename identity.
Global-only anonymous invalid rows retain the existing redacted behavior.

## Bounds and platform

Both layers share ceilings: 128 candidates, 1,024 directory entries including
non-Markdown files, 64 KiB per file, 48 KiB prompt, 1 MiB actual aggregate reads,
and 1 MiB retained definition/identity plus serialized metadata publication.
Callers may only tighten these ceilings. Invalid reads and growth probes count;
aggregate exhaustion closes the whole publication without a partial winner.
An impossibly small response budget returns a static rejection, not oversized
metadata. Public rows contain identity, safe description, status and static
diagnostics only, never prompt, filename/path, routing hint or tool declarations.
Credential rejection retains v1 raw and decoded scalar checks; it does not
claim complete secret recognition in arbitrary prose.

The existing retained-FD reader supports macOS/Linux, rejecting symlinks in
every configured ancestor and final file. The actual configured physical roots
must satisfy that boundary; production does not canonicalize an unsafe root.
Other platforms report UnsupportedPlatform and cannot synthesize an active
catalog. Windows remains #1336. Execution evidence must distinguish actual
macOS gates from unexecuted Linux/Windows paths.

## Verification

Focused real-file fixtures cover precedence, known/anonymous conflicts, exact
identity, replacement, redaction, shared invalid/candidate/entry/publication
budgets, oversize files and ancestor/final symlinks. Existing Global tests retain
FD replacement and bounded growth-probe checks. Actual AppState/HTTP fixtures
cover durable authority despite foreign cache, no workspace prerequisite,
missing/invalid/archived/foreign/corrupt Project, existing backup recovery,
independent host homes, authentication and Codex scope. No new catalog writer
or agents directory is created by discovery. Runtime application, remote/UI,
watchers, plugin precedence, and Child provisioning remain outside this slice.
