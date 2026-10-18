# Project Work board: first vertical slice (#2041)

This slice extends the existing TicketService. Work remains the single execution
and delivery authority. It does not enable a feature flag, change a release cut,
start a scheduler, or grant workspace access.

## Explicit membership

Use the existing owner-authenticated `POST /api/v1/tickets/update` CAS envelope
(`operation_id`, `binding`, `expected_seq`, `expected_epoch`, `operations`).
`associate_project` takes `ticket_id` and a stable `project_id`. The server verifies
that the Project exists and is active; the Ticket service validates scope,
containment/dependencies, and immutable membership atomically with the command.

Create and associate a Goal before creating its children in the same batch;
children inherit that explicit membership. Independent Work can be created and
associated in one batch. Legacy unexecuted leaf Draft/Ready Tickets can be
explicitly linked. Legacy histories and existing trees are not guessed or moved;
active/historical Work reassignment and bulk tree migration are intentionally
unsupported. All containment and dependency edges must remain within one Project
(or entirely in the legacy unlinked graph). Cross-repository Goals use separate
Work and Assignment records.

## Bounded context and provenance

`set_project_context` takes `work_id` and a `context` object:

- `project_id`, `project_revision`, `resource_revision`
- one `workspace_binding`: `path`, optional `label` / `git_common_dir`
- exact `contract_revision` and `contract`
- `sources`: at most 32 nonempty source identity -> immutable version/hash pairs

Canonical context is limited to 16 KiB; hard constraints are never silently
truncated. The server checks current Project revisions and selected registration
when adopting. The primary project path uses a binding with no auxiliary metadata.
Context content is hash-addressed in the existing Ticket snapshot, and Assignment
adoption is immutable. DispatchSpec hashes bind the adopted context hash. A
Submission traces provenance through its immutable Assignment identity. The same
context is included in the existing bounded WorkContextPacket.

This first slice adopts only at a stopped Draft/Ready boundary. Changes during
execution or submission review are rejected. Contract changes require explicit
fresh adoption before another Start; old context/evidence is retained. Fresh host
admission rechecks resource revision and binding, including semantic and recovered
pending-intent paths. A cosmetic Project name revision alone does not invalidate
an already-adopted context. Existing runtime receipts keep their historical adopted
version and are not silently relaunched.

A registration is metadata, not a grant. Start still requires the exact selected
repository, canonical worktree, existing independent host permission, and exclusive
resource claims. Adding a directory does not change an Assignment or authorize it.

Schema 5 prevents schema 1–4 writers from dropping Project references. Legacy
snapshots remain readable and are upgraded only when Project data is written.
File operations and offline transfer must not downgrade schema 5.

## Read API

`POST /api/v1/tickets/project-board` uses the same owner authentication as the
existing Ticket routes. Request:

```json
{"project_id":"stable-id","include_cancelled":false,"include_archived":false,
 "limit":50,"cursor":null,"fixed_commit":null}
```

The response is the existing `ReadEnvelope`, with `data`:

```text
{
  project_id,
  counts: {todo, doing, testing, done, blocked, cancelled, total},
  rows: [{ticket, stage, context, adopted_context}]
}
```

`ticket` is the existing full Ticket DTO. `context` and `adopted_context` are the
snapshot objects above or explicit JSON null. Stage is `todo`, `doing`, `testing`,
`done`, or null. Draft/Ready map to todo, Active to doing, Submitted to testing,
and Accepted to done. Blocked uses `resume_state`, with its reason and paused flag
retained on the Ticket. Cancelled has null stage and is never counted as done.

Counts cover all matching Project Work at the fixed snapshot after the archive
filter, including cancelled Work even when rows hide it. `blocked` is an extra
flag count, not a fifth exclusive stage. A malformed/unknown blocked resume state
has null stage rather than fabricated progress. `total` includes cancelled Work.
Every page repeats the same exact counts. A cursor binds the Project and filters;
concurrent writes do not change later pages. Missing snapshots or changed cursor
filters return `410 resync_required`. Coverage is `authoritative_scope`. The service board port is Supervisor/User-only;
Workers and Runtime cannot read the private board projection. Worker dependency
reads must continue to use the existing redacted inspect/context APIs.

Use existing `tickets/inspect` pinned to the board commit for Assignment, Runtime,
request, and Submission evidence. Use existing `tickets/changes` for invalidation,
then fetch a new board snapshot; cursor gaps require resynchronization. Never
apply a local status patch or treat a Worker completion claim as Accepted. Semantic
acceptance remains the existing exact Submission/evidence/revision command and
required User acceptance cannot be bypassed.

## Explicit remaining work

This is not the whole #2041 checklist. Running-safe pending/adopted revision UX,
relevance-aware external source invalidation, broader explicit legacy tree
migration, and richer server-side board filters remain follow-on slices. Resource
metadata is read from ProjectStore and recorded in TicketStore; this is not a new
cross-store atomic commit protocol. Existing grants and claims remain the safety
boundary. Lotus consumes the read-only endpoint in its separate PR. Durable
claim selection (#2042) and Sprint planning (#2043) are separate work.
