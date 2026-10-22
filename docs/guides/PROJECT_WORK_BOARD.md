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

New execution adopts only at a stopped Draft/Ready Start boundary. While an
Assignment is active (including dispatch-pending or blocked), `set_project_context`
may queue newer Project/resource metadata revisions only when the selected binding,
contract, and source versions are identical to the immutable adopted context.
Counters cannot regress. This permits a Project rename or adding another registered
directory without changing the running workspace, grants, tools, claims, or inputs.
Changes to those inputs still require a confirmed stop and explicit fresh context;
updates during submission review or after acceptance remain rejected. Contract changes require explicit
fresh adoption before another Start; old context/evidence is retained. Fresh host
admission rechecks resource revision and binding, including semantic and recovered
pending-intent paths. A cosmetic Project name revision alone does not invalidate
an already-adopted context. Existing runtime receipts keep their historical adopted
version and are not silently relaunched.

A registration is metadata, not a grant. Start still requires the exact selected
repository, canonical worktree, existing independent host permission, and exclusive
resource claims. Adding a directory does not change an Assignment or authorize it.

Recovery reads the original Assignment workspace/claims, immutable DispatchSpec
context hash, and RuntimeReceipt. The selected workspace binding remains required;
pending metadata is neither a recovery checkpoint nor an authorization grant.
Jiandu summaries and prompt summaries do not reconstruct execution authority.

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
  rows: [{ticket, stage, context, adopted_context, pending_context}]
}
```

`ticket` is the existing full Ticket DTO. `context` and `adopted_context` are the
snapshot objects above or explicit JSON null. `context` is the current candidate.
`adopted_context` is the most recent actual Assignment adoption, including after
confirmed stop. Additive `pending_context` is the candidate when its hash differs
from that adoption, otherwise null; absence on older servers also means unknown/no
pending projection. A pending value is never evidence of runtime adoption.
The next Start fixes that candidate into a new immutable Assignment. Metadata-only
pending revisions do not invalidate an otherwise current Submission. Fresh/recovered
pending-intent admission validates its original DispatchSpec hash, so a newer Work
candidate cannot authorize an old resource revision. Stage is `todo`, `doing`, `testing`,
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

This is not the whole #2041 checklist. Live adoption of changed inputs,
automatic source relevance/invalidation, broader explicit legacy tree
migration, and richer server-side board filters remain follow-on slices. Resource
metadata is read from ProjectStore and recorded in TicketStore; this is not a new
cross-store atomic commit protocol. Existing grants and claims remain the safety
boundary. Lotus consumes the read-only endpoint in its separate PR. Durable
claim selection (#2042) and Sprint planning (#2043) are separate work.

## Isolated loopback integration fixture

`cargo test -p bamboo-server --locked --test project_work_board_http` checks
owner HTTP routes, fixed adoption, and original pending-intent revalidation. No
provider is executed. Both tests explicitly isolate Jiandu under their temp root.

For a bounded Lotus acceptance session, set
`BAMBOO_PROJECT_BOARD_FIXTURE_RECEIPT` to a new local JSON output path and run only
`pending_context_live_routes_preserve_adoption_and_revalidate_original_dispatch`
with `--exact --nocapture`. The fixture binds loopback, defaults to a free port,
serves actual application routes, and writes synthetic IDs, source identity,
binary path, and control-marker paths into the receipt. Optional
`BAMBOO_PROJECT_BOARD_FRONTEND` points to a local built frontend for same-origin
hosting. `BAMBOO_PROJECT_BOARD_FIXTURE_ADDR` must parse as a loopback address.
`BAMBOO_FIXTURE_SOURCE` records the separately verified source SHA in the receipt.

Creating the receipt's `pause_stop_marker` applies Pause and synthetic Runtime
stop confirmation through the Ticket service; the original adopted version
remains visible. Creating `shutdown_marker` stops the fixture; it also expires
after 15 minutes. Dispatch is disabled before serving. All runtime records are
synthetic test evidence, not a completed model run or real Worker acceptance.
