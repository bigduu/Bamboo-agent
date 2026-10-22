# Sprint planning foundation

This bounded #2043 slice builds on #2045 commit
`2373fe8ef41af668cc874ba55b3f23371f6ccd16` and the existing TicketService.
It supplies a trusted-host Rust port and deterministic storage tests. HTTP
commands, Sprint/backlog board filters, frontend controls, scheduling policy and
cross-run budget integration remain separate slices of #2040/#2041/#2042/#2043.

## Authority and identity

`SprintId` is a distinct opaque stable ID. Project identity is the existing
`bamboo_domain::ProjectId`; a member is the original Work's `String` ID. The
authoritative Project association remains `snapshot.project.links`. A Project
must already have a verified Ticket link in this scope before creating a Sprint.
The first host adapter for otherwise empty Projects must explicitly establish
that link or introduce a verified Project registration contract.

`TicketService::execute_sprint(authority, command)` and
`TicketService::inspect_sprint(authority, request)` require current
Supervisor/User authority and the exact ScopeBinding. Worker and Runtime are
rejected, including for receipt replay and fixed reads. The embedding host must
continue validating Project existence and permissions; request JSON does not
construct Authority or create grants.

## Commands

`SprintCommand` carries operation ID, binding, expected scope sequence and
authority epoch, Project ID, Sprint ID, bounded source provenance, and one
`SprintAction`:

- `Create { definition }`: planned, revision 1. Definition contains name, goal
  and optional start/end Unix seconds. Dates are metadata.
- `Edit { expected_revision, definition }`: edits an open Sprint.
- `Start { expected_revision }`: planned to active. At most one active Sprint
  per Project is permitted; multiple planned Sprints are allowed.
- `Add/Remove { expected_revision, work_id }`: appends provenance-bearing member
  history. A Work belongs to at most one open Sprint.
- `Close { expected_revision, dispositions, target_revisions }`: active to
  closed. The dispositions map must contain exactly the unfinished members,
  each choosing `Backlog` or `Sprint { sprint_id }`. The target revision map must
  contain exactly the distinct targets, which must be open and in the same
  Project. Accepted and cancelled members receive explicit terminal results in
  the returned closure; they must not appear in the input dispositions.

Every new command checks scope sequence/epoch and relevant Sprint revisions
under the existing TicketService lock. Work changes or membership changes after
the caller's snapshot produce a conflict. Successful commands use the existing
shared operation receipt namespace. An exact request replay returns the same
receipt, even after restart; changed payload or a collision with a Work/file/
migration operation is rejected. Preserve the full original command for replay.
If a pre-publication attempt is confirmed absent after restart, obtain a fresh
CAS basis before retrying that unpublished operation.

The close result records the source content-addressed commit and sequence,
original member IDs, record/contract revisions, generation, state, adopted Work
context reference, submission/assignment IDs, counts and every disposition.
Only current, exact, non-stale accepted Submission evidence counts as accepted.
Submitted is pending acceptance; blocked and cancelled are never completed.

## Atomicity and history

Sprint planning is a defaultable field in the same canonical Ticket snapshot,
published through the same FileStore HEAD protocol. Schema 6 fences schema 1–5
writers from discarding Sprint data. Legacy snapshots still open without a
Sprint field; ordinary Ticket operations and stopped offline transfer preserve
the newer schema and all planning history. No destructive history deletion is
provided.

Close commits source closure, outgoing history, incoming target memberships and
the shared receipt together. Tests terminate a real child process immediately
before and after HEAD rename and verify complete old/new recovery. A lost ACK
replays the original closure without duplicate membership or a new Work.

Planning commands change only Sprint metadata, scope sequence/schema and the
operation receipt. They preserve all Ticket/Assignment/intent/request/submission
bytes. An already-running Worker retains its same identity, generation,
worktree/claims and permit and can submit after carry-over. Sprint close does
not call Stop, release claims, Accept a Submission, change permissions, promote
priority or reset Work accounting. Actual cumulative runtime-budget acceptance
remains dependent on #2042 and is not claimed by this fixture.

## Read boundary and bounds

`inspect_sprint` returns the existing `ReadEnvelope<SprintView>` at the current
or an explicit fixed commit. `Sprint.closure` is the immutable closing
observation; `current_members` and `current_counts` are observations at the
envelope's snapshot. Later accept/reopen/archive operations do not rewrite the
closure. A missing fixed commit returns `resync_required`.

This first planning slice allows at most 128 members and 2048 membership history
events per Sprint. Ordinary add/remove and incoming carry-over reserve room for
a complete close. Sources are limited to 512 UTF-8 bytes; names to 256 bytes;
goals to 4096 bytes; command JSON to 256 KiB. These are bounded service limits,
not an automatic rollover or destructive compaction policy. Existing Project
board and HTTP response shapes are unchanged.
