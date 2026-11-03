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
asynchronous read. These observations add no execution authority or writer.

Queued/preparing tasks have no entry sample. Stop, shutdown and cold reopen
provide no live sample; files do not restore it or an execution grant. This
slice supplies no durable historical start time. Actor activation start and the
running/preparation `runtime.local_task.observed_at` observation alone do not
prove actual Task entry. Its verified stopped observation remains the Question
diagnostic anchor described above.

## Measurements that remain unavailable

Actual heartbeat and meaningful progress remain `None`. Round/token counters,
metadata revisions, polling time and lease expiry do not supply either fact.
Their producers remain separate work.

Policy no longer substitutes a known start for a missing heartbeat/progress
measurement. Unsupported measurements yield `EvidenceUnknown`, unless a valid
supported wait or authoritative terminal state applies.

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

The existing `ready_work_scheduler` target owns real native capacity/entry,
repeat-read and wrong-identity controls. The Question scenario additionally
checks live start and its absence after stop/reopen. The server default library
owns a controlled shutdown at the last Host read: loss of the real live sample
must conflict without a Ticket write. Pure `patrol_restart` cases check projection
fences and wait-only API compatibility; start alone still cannot prove health.

These are controlled-provider/native mechanisms, not paid-model, installed-app,
all-platform or complete #2049 acceptance. Exact executed results belong in the
Issue/PR and must not be inferred from the presence of these tests.
