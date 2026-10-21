# Sprint HTTP and Project board filtering

This #2043 follow-up stacks on the Sprint foundation in Draft #2047. It adds
planning routes and a filter on the existing Project board. TicketService remains
the only durable Work/Sprint authority. Planning does not dispatch, stop, accept,
copy Work, reset budgets, change generation or grant access.

## Owner routes

All paths below are POST under `/api/v1/tickets`. They inherit the existing
access-password middleware and verified owner check, including the rejection of
run-scoped Worker credentials. Body fields cannot select an authority role.
Every route revalidates the canonical Supervisor binding. Host reads validate
that the Project exists; a new planning command also requires an active Project.
Archived Projects retain read access and exact committed retries. Disabling
`features.ticket_mutation` keeps reads available and rejects all commands,
including retries, just like the existing Work update port.

| Path | Request | Response |
| --- | --- | --- |
| `/sprints/list` | `SprintListRequest` | `ReadEnvelope<Vec<SprintSummary>>` |
| `/sprints/inspect` | `SprintInspectRequest` | `ReadEnvelope<SprintView>` |
| `/sprints/command` | `SprintCommand` | `SprintReceipt` |
| `/project-board` | `ProjectBoardRequest` with optional `sprint_filter` | Existing `ReadEnvelope<ProjectBoard>` |

List example:

```json
{"project_id":"project-id","status":"planned","limit":20,"cursor":null,"fixed_commit":null}
```

Omit `status`, or use null, to list every lifecycle state. A list summary contains
`id`, `project_id`, `definition`, `status`, `revision`, `created_seq`, `updated_seq`,
`member_count` and nullable `closure_counts`. It omits member IDs and history.
`closure_counts` is frozen at close; it is not a current Work completion metric.
Rows sort by stable opaque Sprint ID. Limit is 1–100. The existing cursor,
truncation and omitted-count fields describe the remainder of that fixed read.
Changing Project/status while reusing a cursor requires resync. Limit may change
between pages.

Inspect example:

```json
{"project_id":"project-id","sprint_id":"sprint-id","fixed_commit":null}
```

Inspect returns `data.sprint` (including membership history and frozen closure),
`data.current_members` and `data.current_counts`. Current means the envelope's
snapshot. A closed Sprint's immutable `closure` is distinct from those current
observations. Existing bounds are 128 members and 2048 membership events.

## CAS commands

Use the latest read's scope `snapshot.seq`, `snapshot.authority_epoch` and the
verified binding returned by `/scope`. Every mutation requires a unique
`operation_id`, Project ID, stable Sprint ID, and bounded source provenance.
Source text confers no authority.

```json
{
  "operation_id":"owner-plan-1",
  "binding":{"scope_id":"scope-id","supervisor_session_id":"supervisor-id","binding_revision":1},
  "expected_seq":12,
  "expected_epoch":1,
  "project_id":"project-id",
  "sprint_id":"sprint-id",
  "source":"Owner planning request",
  "action":{"op":"create","definition":{"name":"Sprint 1","goal":"Ship reviewed Work","starts_at":null,"ends_at":null}}
}
```

Keep the original full command for exact retry. Successful retries return the
same operation/closure even if later planning or Project metadata changes.
Changed payloads collide. New commands check scope sequence/epoch and the Sprint
revision atomically. `create` starts at revision 1; `edit`, `start`, `add`,
`remove` and `close` require `expected_revision` from inspect/list. Dates use
optional Unix seconds and do not trigger runtime actions.

Close example (inside `action`):

```json
{
  "op":"close",
  "expected_revision":4,
  "dispositions":{
    "work-one":{"destination":"backlog"},
    "work-two":{"destination":"sprint","sprint_id":"next-sprint-id"}
  },
  "target_revisions":{"next-sprint-id":2}
}
```

Supply exactly the unfinished Work IDs, including Active, Submitted and Blocked
members. Current accepted evidence and cancelled members are classified by the
service; do not put them in the input dispositions. Include exactly the distinct
target Sprint revisions. The result's `closure` records source commit/sequence,
frozen versioned members, counts and every disposition atomically. A running
Work may carry over with its existing Assignment and budget intact.

The first foundation still requires a scope-local Project Ticket link before
creating that Project's first Sprint. This HTTP slice does not invent a new
registration or Work to satisfy that condition. For lifecycle details and shared
receipt/storage guarantees, see [Sprint foundation](TICKET_SPRINTS.md).

## Existing board with optional membership filter

```json
{"project_id":"project-id","limit":20,"sprint_filter":{"kind":"sprint","sprint_id":"sprint-id"}}
```

```json
{"project_id":"project-id","limit":20,"sprint_filter":{"kind":"backlog"}}
```

Omitted or null `sprint_filter` preserves the prior unfiltered behavior, response
shape and cursor hash. The explicit filter is part of a separate query hash.
Every page resolves membership at the same snapshot as the Work rows, before
counts and pagination:

- `sprint`: members of that Sprint in the requested snapshot. For a closed
  Sprint this is its retained closing member set; Work state/context remains the
  state at the board envelope's snapshot. Use inspect for closing facts.
- `backlog`: Project Work outside every planned/active Sprint at that snapshot.
  Membership in a closed Sprint does not exclude Work from backlog.

`data.counts` covers the entire filtered snapshot, before pagination. Existing
`include_archived` controls the counting scope. As before, cancelled Work counts
toward total/cancelled even when `include_cancelled=false` hides its rows.
Blocked remains an overlay on its resume column. Submitted remains testing;
planning never moves a Work to Done. There is no second Sprint board projection.

Cursor reuse with a different membership filter, Project or include flag returns
410 `resync_required`. An explicit `fixed_commit` must agree with the cursor.
A missing historical commit also requires resync. Foreign-Project Sprint IDs are
403; an absent Sprint is 422. Invalid limits/transitions are 422, stale CAS or
changed retry payload is 409, malformed/unknown JSON fields are 400, and disabled
mutation or unavailable authority is 503, through existing Ticket errors.

## Remaining scope

No Sprint UI or scheduler selection policy is included. The Lotus adapter can
consume these additive payloads in its own follow-up; omitted-filter clients
continue using their current MVP contract. Project pending context and new Work
priority/expected-completion/manual-control fields remain owned by their
respective slices. Neither requires a copied field on Sprint members.
