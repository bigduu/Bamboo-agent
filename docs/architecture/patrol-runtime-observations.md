# Read-only native wait observations

Issue #2469, parent #2049/#2050. Source base: accepted dev
`3d3f60b6085a3ff255b8a0ca3a3059b949ca328c`.

The existing zero-model program patrol now accepts a prepared runtime wait
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

## Measurements that remain unavailable

Production start, actual heartbeat and meaningful progress remain `None`.
There is no existing safe private Actor reader exposing exact run/start timing;
the public snapshot deliberately redacts those fields. Local task observation
times, round/token counters, metadata revisions, polling time and lease expiry
are not substituted for these measurements. Adding a start/heartbeat/progress
producer or a lifecycle read port is separate work.

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
The existing orchestration `patrol` target owns policy transition regressions;
the server default library owns compact identity-helper negatives.

These are controlled-provider/native mechanisms, not paid-model, installed-app,
all-platform or complete #2049 acceptance. Exact executed results belong in the
Issue/PR and must not be inferred from the presence of these tests.
