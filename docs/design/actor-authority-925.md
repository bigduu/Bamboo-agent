# Actor authority foundation (#925)

This is the first local implementation slice of #925 under the #791 actor runtime epic.

## Durable identities and attempts

`ActorId` is exactly `Session.id`. The existing `session.json` and `runtime.json`
remain the authority for transcript, parent, root, Project, depth, and Session
lifetime. `ActorSession` is a versioned projection of that identity, plus logical
lifecycle, an optional policy revision, placement intent, and a monotonic attempt
counter. Policy revision remains absent until effective-policy authority is
connected; zero is never used as a fabricated policy revision.
It records the Session birth timestamp so an explicitly deleted and recreated
Session with the same public id cannot inherit an older activation lease.

`SessionStoreV2` persists `actor-authority.json` beside the Session. A claim
first proves the Session is durably present and its main/runtime identity pair
is consistent, then durably publishes a Cold authority record if missing. Only
after that publication and an independent durable
`actor-authority.initialized.json` marker may it publish a Reserved
`ActorActivation`. A missing sidecar with an existing marker is corrupt, never
an invitation to restart at attempt zero. A crash between the Cold record and
marker can complete initialization only while the record is still inert.
An independent Store with a stale process index scans the durable Session
directory tree under the same actor lock. The scan rejects two physical
Sessions with the same ActorId, even when different Stores cache different
index hints. A Child claim also checks its complete saved parent chain up to
the Root, including current Project identity and adjacent depth. Each saved
ancestor birth and metadata revision is retained in the Child actor record,
so a previously observed ancestor re-creation cannot revive an old activation
fence. Root Project A-to-B-to-A changes are fenced when each Project write
advances `metadata_version`; Child Project identity is immutable after its first
durable V2 save (#1317). A deleted middle Child therefore cannot leave a
claimable grandchild behind. This
first-slice safety check scans Root directories on each authority operation;
a future durable unique-id registry could replace that cost. The sidecar is
not a second Session store and contains no transcript, broker endpoint,
credential, PID, container id, or worker mailbox.

Root Project identity may first bind or change after Session creation. While
the actor is Cold or terminal, its Project projection is updated durably with
a higher authority revision before another activation can be claimed. A live
activation retains the Project at claim time. If the Session's Project changes
while it is live, all authority operations return a Project transition conflict
without modifying the old sidecar or lease. The authority also records the
last observed own and ancestor `metadata_version` values: an unseen gap of two
or more revisions while active is blocked even if the Project now matches,
because the Root Project could have changed away and back. `metadata_version`
also covers title and pin changes, so two unrelated UI updates can
conservatively block a live activation. The V2 Child full and runtime save
boundaries reject Project changes under the per-Session writer lock, including
stale writers in another Store. First full saves also scan physical Root trees
under that same lock to reject a reused Child id in a different tree before
the global index can move. The current
runtime has no integrated cancellation/reconciliation caller for a blocked
live activation yet.

This writer guard prevents new cross-tree Child id collisions. It does not
repair histories that already contain two physical Children with the same id:
their ordinary full saves can still move the global index between paths.
ActorDirectory rejects activation for that ambiguous id until those on-disk
Sessions are repaired. The guard scans Root trees only on first Child save,
not on every high-frequency runtime checkpoint.

Every authority operation holds the existing lifecycle, runtime sidecar, and
exact Session maintenance locks in that order. The maintenance lock includes a
cross-process file lock. Each mutation reads the current record, checks the
exact activation fence, and durably replaces it with an incremented revision.
The fence includes actor, activation id, attempt, run, owner, and lease epoch.
An expired owner is replaced by a higher attempt and lease epoch; stale start,
checkpoint, finish, and fence checks fail closed. Retirement fences a live
owner and preserves the Session and its history.

## Filesystem job lifetime (#1349)

The lifecycle shared, Task shared, and exact Session writer guards are owned by
one Arc holder. Every authority or initialization-marker replacement clones
that holder into a single `spawn_blocking` job. That job uses synchronous
filesystem operations for temp creation, write, file sync, replace, directory
sync and error cleanup. A started job retains all physical locks until it
terminates, even if its async caller is aborted or its Tokio runtime stops
waiting during shutdown. Windows preserves replace-existing/write-through
semantics; the native barrier evidence is for the platform running the tests.

This protects each started filesystem job, not the whole async actor operation.
Cancellation can occur after a Cold entry commits but before its separate
marker job starts, or after observation refresh but before activation CAS.
Existing repair accepts only an inert Cold entry without a marker. A queued
blocking job that never starts may be cancelled. A replace can commit before a
directory-sync error or before the caller receives confirmation; an error or
cancelled caller therefore does not prove rollback. Independent Stores reopen
actual state under the same physical locks before repair or successor CAS.

Native tests pause inside the replacement job, probe all three physical locks,
abort callers and shut down runtimes, then release the job and reopen actual
files. They cover separate Cold and marker jobs, missing-marker repair,
observation refresh, successor attempt/epoch/revision, and failures before and
after replacement. This adds no journal, mandatory executor or runtime caller.

The domain `ActorDirectoryPort` is the narrow runtime/storage seam. It exposes
ensure/inspect, claim/start/renew/checkpoint/finish/retire, and exact fence
validation. The supplied clock values must come from the trusted host runtime,
not an untrusted worker frame.

## Follow-on integration required for full #925 acceptance

The current `SessionActivationRouter` and legacy Session write/Inbox ack callers
do not yet use this port. A fence check followed by a separate write is not an
atomic transcript or Inbox guarantee. The next slice must carry the exact
fence into the final durable transcript checkpoint and Inbox ack boundaries,
and route every activation entry point through the same claim. Only then can
the runtime claim one transcript writer and reject stale events/cancel/ack
across all paths. Legacy `deploy_agent` convergence and scheduling belong to
#926/#927 and separate placement work.

Focused tests cover Session-before-activation, restart continuity, competing
independent store owners, expired retry and stale fences, retirement, malformed
or mismatched authority, stale index recovery, duplicate physical IDs, orphaned
descendants, observed ancestor re-creation, Project lineage and reassignment,
missing sidecar/marker recovery, and invalid state-machine records. A Child
first inspected only after its parent was deleted and recreated with the same
id has no earlier ancestor birth record; this slice rejects a later-born
parent by timestamp, while #1318 will put an exact durable incarnation binding
in the Child Session creation record before remote clocks are in scope.
