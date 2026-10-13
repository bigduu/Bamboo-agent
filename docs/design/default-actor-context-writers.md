# Default Session writers after Actor initialization

This slice (#1350, prerequisite of #1341/#925/#791) fences only default V2
`save_session`, `save_runtime_state` including its full-save fallback, and
`clear_session`. It does not enable production Actor activation.

## Final authority and protected publications

The final lifecycle → Task → exact Session locks cover observational reads of
Actor record/initialized marker and actual durable Session birth. Both files
absent retain legacy compatibility. A valid matching Cold attempt-zero pair
permits default context writes. Every record with current_attempt > 0 stays
protected after finish, failure, cancellation or retirement. Attempt-zero
Retired and missing-one-file/malformed/nonregular/mismatched authority are
protected too. Reads never initialize, repair or refresh Actor authority.

Full saves compare complete ordered messages, provider-native transcript and
Inbox admission from actual main; summaries, compression events and model
context from actual runtime. Runtime-only compares only what its sidecar
publishes: its discarded incoming messages/native/admission do not cause false
rejection. Missing protected runtime cannot be reconstructed from embedded
main. Equality requires actual durable values; history reads can scale with
transcript size. Existing Root/Task/Project/birth and Supervisor checks remain.

A protected delta returns Unsupported with direct SessionAuthorityConflict,
so existing merge/runner rejection paths suppress cache publication. It is not
a Task retry. Exact-context control-plane saves remain independently validated.
Clear rejects protected/ambiguous authority before attachment deletion.

## Physical job lifetime

Each started directory preparation, proof/sidecar/main/search replacement and
complete clear cleanup/rebuild runs in one std filesystem job owning the actual
locks. Fields release Session, then Task, then lifecycle. Caller abort and Tokio
runtime shutdown cannot let successor activation overtake a started job.
Root mode passes its existing holder; no shared fair-lock re-entry is added.

Full-save still sequences Prepared → runtime → main → Committed, preserving
fault boundaries. Cancellation can stop later async stages from starting;
started-job ownership does not promise completion of the whole transaction.
An error after rename may leave changed durable bytes with no confirmation.
Windows retains existing MoveFileExW replace/write-through semantics.

## Boundaries and evidence

Task CAS/undo/recovery (#1354), SupervisorManagement (#1355), and independently
invoked startup migration (#1356) remain separate writers. Fenced append #1351,
Inbox, attachments creation, Copy/delete/recreate, runtime opt-in, global cache
ownership, remote and exactly-once guarantees are excluded. Arbitrary external
removal of both Actor files while retaining Session is indistinguishable from
legacy; no tombstone or new journal is introduced.

Focused tests exercise real durable files, final activation after merge read,
zero rejection callbacks, independent pre-created Stores, actual blocking-job
barriers, FileExt lock probes, caller abort and whole-runtime shutdown. Platform
execution evidence is recorded separately in the acceptance receipt.
