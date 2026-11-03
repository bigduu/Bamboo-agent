# Read-only native runtime observations

Wait slice #2469; live Task-entry slice #2506; parent #2049/#2050.
Original wait source base: accepted dev
`3d3f60b6085a3ff255b8a0ca3a3059b949ca328c`.

The existing zero-model program patrol accepts prepared runtime wait and live-start
projection. The original ledger-only `ProgramPatrol::tick_project` remains
available. The new factory pass retains the same fixed-snapshot, chunked
authorization, exact Scope, journal CAS and Ready admission fences. Runtime I/O
happens before the synchronous pass and outside Ticket transaction locks.

The server's public `TicketApplication::patrol_observation` and existing Ready
program assembly consume the same compact canonical source. No model, runtime
writer, worker dispatch, cancellation, acceptance or executable permission is
added by observation.

## Supported facts and independent identity boundaries

- Ticket Work/Assignment/generation/contract, immutable DispatchSpec/hash and
  RuntimeReceipt must agree. Sources are fenced to their original Ticket commit.
- Canonical Child dispatch, birth, native Host Run, LocalPlan assignment and
  typed runtime Run must match. Pipeline `execution_id` is a different UUID.
  The canonical parent's birth/root/depth and inherited optional Project remain
  exact. A Work Project is independent of the Supervisor/Child tree Project.
- Current typed child/Bash waits require the matching resumable suspension
  reason, a nonempty bounded target list and no conflicting User/approval wait.
- Native Task Questions use their actual one-shot yield contract, rather than
  a fabricated typed suspension. A unique current stopped Assignment may be
  selected after `RuntimeStopped` clears `active_assignment`; its awaiting
  Open Question must retain exact work/assignment/generation/contract, prompt
  and packet identity. Canonical saved yield and completion reason must agree.
  The matching native task instance, birth, Run and owned-stop receipt are
  independently checked. A completed display state never supplies stop proof.
- Runtime reads use the existing compact control-plane reader and repeated
  identity checks. They do not load transcript or call ActorDirectory
  `inspect_actor`/`ensure_actor`, which can initialize or refresh authority.

## Diagnostic allowance, not a business deadline

The Question has no business timeout. Its patrol allowance begins at the exact
canonical native stopped observation (`runtime.local_task.observed_at`), after
the independent owned-stop and current Question checks. Repeated polls and a
cold Host reopen retain that original anchor. Child/Bash waits use their actual
registration time. The existing finite `Policy::max_expected_wait_ms` bounds
the diagnostic allowance; a real earlier runtime deadline tightens it.

Allowance expiry may produce `WaitExpired` for patrol diagnosis. It never
answers/expires the Question, cancels work, proves an execution stopped, releases
claims or authorizes a retry. An answered/replaced/foreign request cannot keep
an expected wait merely because old Child metadata remains on disk.

A new supported current wait reports `ExpectedWait` even when an earlier
`EvidenceUnknown` incident exists. Its historical incident, escalation and
accounting receipts are retained; waiting is not meaningful progress and does
not resolve the old incident. An outstanding diagnostic or settlement still
requires its original adapter and takes precedence over a normal wait report.

## Live native Task entry

Engine owns one non-persisted, once-only entry timestamp on the existing native
`Execution`. The actual owned Tokio Task sets it immediately before Agent
execution, after native capacity and admission. `ExternalChildRunner` and
`SpawnScheduler` expose a read-only sample bound to session birth, Run and the
physical resource-owner instance. The composite runner forwards it only for its
existing sole-native backend. Other backends provide no sample.

Orchestration's `RuntimeFacts` projects the start after canonical Scope, commit
and receipt checks. The original wait-only `RuntimeObservation` API remains
compatible. Server joins the Engine sample to its existing fresh Project,
Ticket, parent and Child proof; it rechecks the exact live sample after the last
asynchronous read and again when the program factory consumes a prepared batch.
The batch keeps a weak adapter reference; a vanished or replaced sample clears
only start, with the independently checked wait and canonical fences preserved.
These observations add no execution authority or writer.

Queued/preparing tasks have no entry sample. Stop, shutdown and cold reopen
provide no live sample; files do not restore it or an execution grant. This
slice supplies no durable historical start time. Actor activation start and the
running/preparation `runtime.local_task.observed_at` observation alone do not
prove actual Task entry. Its verified stopped observation remains the Question
diagnostic anchor described above.

## Live native heartbeat

#2525 exposes the latest successfully acknowledged Actor renewal from the
existing heartbeat loop. The loop captures Task entry before requesting a
renewal and records the renewal's request time only after success. A queued
renewal that finishes after entry still supplies no heartbeat. The timestamp is
conservative when storage is slow and remains with the same live native
execution instance. Reopening a Host cannot reconstruct it from a lease.

Host preparation fences the immutable Task entry. At projection the program
re-reads the heartbeat of the same instance, so an intervening renewal can
advance the sample. The Ready pass has a fixed clock. A heartbeat later than
that pass's cutoff is unavailable for that pass. Closure or replacement clears
live start and heartbeat while preserving independent wait evidence and the
original Ticket fences.

A fresh heartbeat with unavailable progress can still yield `EvidenceUnknown`.
A measured stale heartbeat can yield `MissingHeartbeat` under the existing
policy. An unavailable heartbeat remains unknown.

## Verified workspace progress

#2527 supplies a progress observation from a nonempty new artifact created
through the current Assignment's guarded workspace file port. Ticket records
the Host completion time on the original success receipt after the physical
create. The receipt's committed sequence identifies the progress boundary.
Existing receipt metadata is preserved when the same request is replayed.

The Host projects this evidence only for the exact live native Task and
current Ticket Scope and receipt. The observation uses the latest qualifying
receipt from Task entry through the fixed patrol clock. Legacy receipts without
the completion time, duplicate artifact content for the same Assignment, file
reads, Task-list updates, model output and lease renewals supply no new progress.
Unknown outcomes keep their reconciliation status. User reconciliation of
bytes on disk supplies no physical completion timestamp.

When no qualifying evidence is available, progress remains `None`. The sample
does not survive a stopped Task or cold Host reopen as live progress. The
existing patrol can diagnose stale measured progress or resolve an incident
after a later observed boundary. Submission and acceptance retain their
existing independent authority.

## Acceptance ownership

Ordinary Cargo integration target `patrol_runtime_observation` owns the real
Store/Inbox/Host Question/stop/read/reopen/answer scenario. Its child-process
fixture runs the real native worker once, writes only a test receipt after
canonical evidence exists, and exits before reopening that same disposable
root. It verifies stable allowance identity, retained earlier incident, zero
observer/provider calls, exact fencing and byte-identical Actor authority.
The byte baseline follows exact runner/router finalization and a pure-read
terminal Actor check; a canonical owned stop alone precedes that writer tail.
The existing orchestration `patrol` target owns policy transition regressions;
the server default library owns compact identity-helper negatives.

The existing `ready_work_scheduler` target owns native capacity, entry, queued
renewal and admitted heartbeat controls. The Question scenario checks the
absence of live facts after stop and reopen. The server default library
advances a real renewal during final Host sampling, then closes the source
during another read. The in-flight read conflicts on closure and the prepared
batch loses live start and heartbeat without a Ticket write. Pure patrol
cases check projection fences, API compatibility and the distinction between
unavailable progress and a measured stale heartbeat.

These are controlled-provider/native mechanisms, not paid-model, installed-app,
all-platform or complete #2049 acceptance. Exact executed results belong in the
Issue/PR and must not be inferred from the presence of these tests.
