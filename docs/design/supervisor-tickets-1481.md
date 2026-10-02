# Supervisor tickets implementation ledger (#1481)

## P0 baseline (2026-10-02)

Isolated checkout: `/Users/bigduu/Documents/Codex/2026-10-02/task/bamboo-1481`.
Local branch: `bamboo/feat/1481-supervisor-tickets`.
Base: `8d7e675502f6643d2fe17af002413eb8045ed726`, also the GitHub `dev`
commit resolved during this audit. No pre-existing #1481 branch, PR or
implementation task was found. Zenith and the #791 worktrees are dirty and
are deliberately not used as sources of uncommitted implementation.

The #1479/#1480 owner uses `bamboo/fix/1479-791-child-result-continuity`.
Its committed base is still `8d7e6755`; its implementation is uncommitted.
Both issues are open and locked. Do not copy or reimplement those patches.
#1479 remains a hard gate for live dispatch and P5/P9 acceptance. #1480 is
only a gate for later same-Actor continuation. #1478 is also open/locked;
its macOS unwind repair is not in this base and must not be duplicated.

## Slice boundaries and interfaces

This tracker is executed as independently testable local slices. The first
slice is P1/P2/P4: a file-authoritative TicketService and a deterministic
create/dispatch/submit/accept fixture. P3 changes the existing child Task
boundary. Runtime integration (P5), bounded reads (P6), semantic resolution
(P7), Lotus (P8), and live acceptance (P9) have separate evidence. A passing
domain test is never recorded as end-to-end completion.

- Keep `SupervisorSessionService` and its Storage-owned scope proof intact.
  A trusted host adapter must verify that proof before constructing service
  authority; a model/client cannot submit a trusted identity in JSON.
- Add an independent `bamboo-tickets` infrastructure crate. It stores work
  contracts, not Jiandu memory or Session transcripts. A single process lock
  and unique HEAD govern the scope. No second per-ticket HEAD or index writer.
- `child_session/actions.rs` currently clones the entire parent TaskList;
  `runner/tool_execution/task/taskwrite.rs` routes Child writes to the root.
  Replace both together, preserving old Root Task compatibility.
- Runtime has PlacementScheduler/FileHostRegistry and fenced Actor admission,
  but no persistent `ensure_dispatch(key, spec)` / `query_dispatch(key)` port.
  The bridge must fail closed until admission can prove stable key identity.
  A fake Runtime is only a deterministic contract test.
- Inbox `SessionMessageEnvelope` already has thread_id/in_reply_to/
  correlation_id. ChatRequest does not yet carry them. Those routing fields
  never confer approval. Pending approval binds the full action fingerprint
  and exact request/prompt/assignment/generation.
- Supervisor model calls occur outside the TicketService lock. Ingress and
  resolutions are durable; one short decision turn at a time; Workers do not
  hold the Supervisor's turn or a transaction lock.
- The store is excluded from Worker write roots. Same-UID arbitrary shell
  cannot be represented as a proven security boundary. Live code execution
  stays disabled until the runtime adapter proves filesystem enforcement.

## Verification and rollback

Use deterministic service/fake-worker tests separately from real model
semantic evaluation, Host/four-Worker fixtures and Lotus browser evidence.
Test process lock, complete manifest/hash checks, CAS/idempotency, every
publication fault boundary, late generations, dependency invalidation and
unknown effects. Local APFS is the initial filesystem target; do not infer
NFS, sync-drive or Windows durability support from it.

Mutation and dispatch are independent, default-off host capabilities. Closing
dispatch does not discard submissions. An unsupported schema or damaged HEAD
opens no writable authority. Rollback preserves immutable history/receipts.
No push, PR, merge, deployment, production migration, permission or credential
changes are authorized in this task.

## Evidence status

P0 baseline and interface audit recorded. P1–P9 and A1–A12 remain unaccepted
until individual test and runtime evidence is recorded below.

### Local slice 1: domain and file authority

The foundational `bamboo-tickets` crate contains the separate version axes and
content-addressed full revisions/manifest/commit/HEAD, a lifetime OS writer
lock, fixed commit reads and verified readonly exports. Its focused storage
suite has four passing tests, including every publication I/O boundary.
`cargo clippy -p bamboo-tickets --offline --locked --all-targets -- -D warnings`
passes. Only local APFS process-crash/flush behavior is evidenced here; power
loss and other filesystem types are not accepted.

Ticket command implementation is a subsequent local branch/worktree. During
its initial focused verification, twelve tests passed, including 48 actual
process-exit boundaries and a second-process writer-lock rejection. That
evidence does not accept live dispatch, OS Worker isolation, model routing or UI.

### Local slice 2: application commands

Worktree: `bamboo-1481-commands`; branch: `bamboo/feat/1481-ticket-commands`,
based on foundational commit `dc442538`. TicketService owns atomic typed
batches, receipt deduplication before CAS, trusted-host identity checks,
private LocalPlans, exact approval consumption, retained stale submissions,
dependency/Goal invalidation and explicit legacy import. It checks canonical
worktree claims and excludes its authority directory from declared write roots.
These checks are not OS sandbox enforcement and are not presented as such.

Focused suite: 4 file-authority tests plus 15 service tests, including one
subprocess helper exercised by the real process-crash test. The crash test
checks 48 actual exit windows plus a competing OS writer. The deterministic
entry point `cargo run -p bamboo-tickets --offline --locked --example single_work`
computes 2+3 in a fixture thread, submits its hash and accepts the exact
submission only after fixture-user evidence; output is labelled
`deterministic_service_fixture`. No live Runtime or provider is used.

P3 is still open: ordinary/native Task execution is not yet wired to this
service LocalPlan. P5 is still gated by #1479 and the missing real admission
key/query port. P7 model evaluation, P8 Lotus, and P9 real acceptance remain
unrun. Import currently produces a blocked record requiring review; it does
not migrate running Session ownership or create acceptance for legacy output.
Artifact URI/hash syntax is checked, but artifact bytes need a trusted host
artifact resolver before automated quality acceptance can be enabled.

### Recovery and stable dependency handoff (2026-10-02)

The executor reconnected. Original commits `7c0c7703`, `dc442538`, and
`7e2f04ea` and their worktrees were preserved. The interrupted P6 patch left
no partial files; its worktree was clean. P6's isolated branch was rebased
onto `685328ec6114f13a0a5fda27b380b37dbff62dc4`, the accepted #1483 squash
commit, without changing the original branches. Replayed local commits are
`7dd333c6`, `c194affd`, and `7c792963` respectively.

GitHub confirms #1483 merged at 17:06:53 UTC, closes #1479/#1480/#1478, and
its exact source head is `0520ad4bd9fef15ac06697cd1964f34c48e9c11e`.
CI run `37031388437` reports success; required Test job `110932382202`
reports success. Its downloaded log contains
`actual_four_children_two_rounds_collect_parent_results ... ok`. Other
skipped CI jobs remain skipped, not locally accepted #1481 coverage. These
stable repairs are consumed without copying the owner's former WIP or
reimplementing their Child completion/continuation/linker changes.

### Local slice 3: bounded reads and context

Six focused query tests pass: snapshot-stable pagination across publication,
query-bound/missing cursor resync, inspect/context budget refusal, Worker
private-read isolation, fixed changes high watermark with intermediate
revisions, and full-manifest measurement. Reads directly scan immutable
authoritative revisions: `index_seq` equals the fixed snapshot sequence; no
second authoritative stream/index is introduced. Changes derive from commit
ancestry, with a 256-commit scan bound and explicit `resync_required`.
Inspect is bounded to 32 IDs/depth 4/64 KiB and never truncates a contract.

Local APFS sample: 5/50/200 records produced full manifests of
842/5972/23332 bytes. One-record update elapsed 314/68/105 ms in this run,
including flushes. These small samples are not throughput guarantees or
evidence for 200 concurrent Workers. `work_dispatch` currently accepts
start/steer/cancel/retry typed operations; pause belongs to the next Runtime
slice. Model tool registration, true dispatch and Lotus are not accepted by
these pure service reads.

### Local slice 4: trusted Task / LocalPlan adapter

Worktree `bamboo-1481-plans`; branch `bamboo/feat/1481-worker-local-plan`.
The trusted work-child creation entry omits the inherited Root TaskList and
injects a readonly, bounded contract packet. Legacy child/root entry points
retain their old behavior. A marked Ticket child without a Runtime capability
fails closed; successful writes update only the Assignment LocalPlan and its
private display projection, with no Session/root task-authority patch.
Immutable receipts retain the canonical request for generated adapter retries;
reuse preserves the original expected revision and payload checks. An older
call replay after a later plan revision cannot roll the projection back.

Verification: four new adapter/identity/replay tests pass; all ten affected Task
tests pass, including old Root/child behavior. All 25 TicketService tests pass;
TicketService strict clippy, formatting and engine compilation pass. Engine
clippy completes with existing warnings in the Actor/legacy child code and its
dependencies; it is not a strict-warning pass. This is the in-process adapter
slice only. The native Worker HostBridge, real dispatch, filesystem sandbox and
true #1481 Runtime acceptance still require subsequent slices. Synthetic
receipts in these tests are not real admission evidence.

### Local slice 5: opt-in native Task ceiling

Worktree `bamboo-1481-task-ceiling`; branch
`bamboo/feat/1481-native-task-ceiling`. The actual framework Task instance is
eligible for an explicit native ceiling. Host selects Task alone only for a
Ticket child; ordinary/native legacy sessions keep their five-name surface.
Custom same-name owners and disabled Task do not acquire this ceiling. Four
tool-entry/ceiling tests and three Host ownership/compatibility tests pass.
The macOS server-test linker emits its existing large unwind-section warning;
the test executable starts and all selected tests pass. No linker repair added.

The P3 follow-up canonical compatibility test passes: absent optional Step
status and legacy receipt request bytes retain their original canonical form.
This prevents a schema-compatible read from changing old request hashes.
Native plan callbacks and dispatch remain a subsequent slice.

### Local slice 6: native HostBridge / private plan

Worktree `bamboo-1481-native`; branch `bamboo/feat/1481-native-local-plan`,
based on `0495353f`. The fresh required Child packet carries only the bounded
Work contract/inputs. Native bootstrap compares its complete canonical hash
with the Host's verified packet; equal revision numbers cannot authorize
modified constraints. Each Task callback reloads the exact live Child birth
and Run, then commits through TicketService. Oversized plans fail before
publication. Successful Worker Task events require the exact Host callback
payload/receipt; Worker plan-update events cannot publish Root changes.

The real AgentRuntime Worker loop executes Task over this callback and retains
the Host Run ID throughout startup. Its provider context excludes the Root
private plan, and Root Task/messages remain unchanged. Legacy Task evaluation
does not write a second authority. This test uses a scripted provider and
synthetic admission; it is not real-model semantic evaluation or P5 dispatch.
The initial opt-in native ceiling remains Task-only, with no arbitrary shell
or filesystem tools. Same-UID arbitrary shell isolation is not claimed.

Evidence: seven focused Ticket/creation/capability/replay tests, ten affected
Task tests, three local-transcript tests, all 26 TicketService tests and 32
Worker tests pass. The Worker suite uses CI's `RUST_MIN_STACK=8388608`; its two
loopback fixtures require local socket access outside the execution sandbox.
Without those test permissions it reports 30 pass / 2 permission failures;
the default thread stack also aborts an existing deep Worker fixture.
Formatting and whitespace checks pass. Engine clippy completes with 19 known
legacy warnings, not a strict-warning pass. The publication fixture, scripted
provider loop, Host callback tests and real-model evaluation remain distinct.

P5 stable dispatch/admission receipts, actual terminal submission, P7 semantic
resolution, P8 Lotus and P9 acceptance are still unrun. Generated Task IDs and
the exact raw tool payload need a separate receipt-replay follow-up before
claiming arbitrary Task-call replay; current replay evidence uses explicit IDs.

## P3 original Task-call replay follow-up (2026-10-02)

The `bamboo/feat/1481-worker-call-replay` slice starts from `d874dcca`.
Optional CommandSource retains canonical original Worker Task arguments together
with the immutable Command receipt. The Task adapter reads this input before
regenerating a plan projection, so a call with generated IDs can replay after a
later rename. A changed raw payload conflicts even when its projected Steps
would be the same. Replays project the current plan and cannot roll it back.
Source metadata is bounded and confers no identity or capability; typed operation
checks still apply. Missing source in legacy adapter receipts requires the
original Command rather than reconstructing a potentially different request.

Validation: bamboo-tickets 27/27 (6 queries + 5 authority + 16 service), engine
TicketWorkerPlan 5/5 including generated-ID/rename/restart replay, strict tickets
all-targets clippy, workspace format and diff checks passed. One initial new-test
assertion expected seq zero despite writer initialization; corrected to compare
against the pre-operation seq. Restart replay only reads a prior receipt: the
old Worker remains fenced and cannot resume Task execution. This does not count
as P5 Runtime admission/restart evidence or real-model semantic evaluation.
