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
An independent Store with a stale process index reads the current disk index or
scans the exact Session directory tree under the same Session lock. The sidecar
is not a second Session store and contains no transcript, broker endpoint,
credential, PID, container id, or worker mailbox.

Root Project identity may first bind or change after Session creation. While
the actor is Cold or terminal, its Project projection is updated durably with
a higher authority revision before another activation can be claimed. A live
activation retains the Project at claim time. If the Session's Project changes
while it is live, all authority operations return a Project transition conflict
without modifying the old sidecar or lease. The authority also records the
last observed Root `metadata_version`: an unseen gap of two or more revisions
while active is blocked even if the Project now matches, because the Project
could have changed away and back. `metadata_version` also covers title and
pin changes, so two unrelated UI updates can conservatively block a live
activation. This currently affects only the new authority seam, which has no
production activation caller. A Project-specific epoch or guarded Project
write boundary is required before that caller is connected. The current
runtime has no integrated cancellation/reconciliation caller for a blocked
live activation yet.

Every authority operation holds the existing lifecycle, runtime sidecar, and
exact Session maintenance locks in that order. The maintenance lock includes a
cross-process file lock. Each mutation reads the current record, checks the
exact activation fence, and durably replaces it with an incremented revision.
The fence includes actor, activation id, attempt, run, owner, and lease epoch.
An expired owner is replaced by a higher attempt and lease epoch; stale start,
checkpoint, finish, and fence checks fail closed. Retirement fences a live
owner and preserves the Session and its history.

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
or mismatched authority, stale index recovery, Project reassignment, missing
sidecar/marker recovery, and invalid state-machine records.
