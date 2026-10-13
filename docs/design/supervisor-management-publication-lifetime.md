# Supervisor management publication lifetime

`SessionStoreV2::management_mutate` retains its existing lifecycle shared,
Task shared and sorted exact Session locks through each started filesystem
publication job. Attach includes the target Session; Configure and Detach lock
only the verified Supervisor. Detach continues to use the persisted relation
when the target is damaged or deleted.

One private Arc holder owns these actual locks. Each Prepared proof, runtime
sidecar and Committed proof replacement receives a clone. The complete std
job creates a unique same-directory temp, writes and synchronizes it, performs
true replacement, synchronizes the directory where supported and finishes
error cleanup before releasing its clone. Caller cancellation, timeout and
shutdown of the originating Tokio runtime cannot release locks held by an
already-started blocking job. Sessions are released in reverse sorted order,
then Task and lifecycle.

## Existing proof protocol

The three publication stages and their fault boundaries remain separate:

| Completed stage before caller/runtime loss | Allowed durable state |
| --- | --- |
| Prepared proof | New Prepared proof and old runtime; operational reads reject. |
| Runtime sidecar | Prepared proof and new runtime; operational reads reject. |
| Committed proof | Matching new runtime and Committed proof; authoritative readback succeeds. |

No job completion promises that the next stage will start. Cancellation between
stages can leave the existing pending state, which is never repaired or treated
as a successful receipt. An I/O or join error after replacement can leave new
bytes: the result is unconfirmed, not evidence of rollback. Reload through
existing authority readers; a pending or contradictory proof still fails closed.

Successful mutations preserve the canonical current summary, compression
events, model-context state and identity in the complete runtime projection.
Main transcript/native data and target files are not rewritten. Management
revision/link/tombstone checks, incarnation/birth/Project verification and the
existing bounded proof schema remain unchanged. An explicit current-revision
later mutation can publish only after every earlier started job releases the
same physical boundary.

## Validation boundary

The focused native fixtures park inside actual std replacement jobs at each
stage. They use independently opened exclusive FileExt probes against lifecycle,
Task and Session locks, precreated independent Stores, caller abort, timeout,
whole-runtime shutdown, before/after replacement errors and actual raw-file
reopen. Pending cases assert rejection without repair; Committed cases exercise
a real later changed mutation and verify its durable proof cannot be overwritten.
The deleted-target Detach fixture additionally holds the target Session guard
while revocation completes under Supervisor-only authority.

Execution evidence records the actual tested platform and exact source/artifact
identity. Independent Stores run within one OS process; these are not separate
process kill or power-loss experiments. Windows retains the existing
`MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)` helper, but runtime validation on
Windows requires a separate platform run. Injected after-replace errors are not
proof of a real failing filesystem sync.

This slice does not change Inbox followup locks, default full-save proof writes,
Supervisor bootstrap/migration, Task recovery, provider/runner/cache callers or
any public API. It adds no journal, recovery/repair protocol or global guarantee
for arbitrary external writers, and does not make the whole asynchronous
management mutation finish after cancellation.
