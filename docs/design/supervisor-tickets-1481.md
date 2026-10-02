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

## P5 canonical dispatch/admission boundary (2026-10-02)

The `bamboo/feat/1481-runtime-dispatch` slice starts from `4c26c72a`.
Server ensure/query maps an immutable dispatch key to one deterministic Child.
It verifies the canonical Supervisor proof, incarnation and orchestration-mode
revision. A changed spec hash is rejected. Creation uses the existing one-shot
Child factory, exact launch generation and scheduler; the Composite runner now
forwards the Host-only TicketService port. No extra LLM/placement scheduler was
introduced. The API is Host-only and dispatch policy defaults off.

After canonical Inbox activation registration and before RunSpec is sent, the
Actor driver persists the exact key/spec/run/session receipt and Child birth in
its existing control plane, then commits Ticket admission/running. A failed or
uncertain Ticket publication does not grant Worker permission. Queries identify
prepared-but-uncommitted receipts as unknown. Old pending/running permissions
are fenced at writer restart. The generic legacy pending-child boot pass skips
Ticket dispatches; unknown dispatches are never automatically re-enqueued.
Runtime metadata merging preserves this Host receipt across a stale final save.

Validation: server check passed; engine Ticket/admission 10/10; storage metadata
compatibility 14/14; TicketService 27/27; format/diff checks passed. Server clippy
completed with the existing engine 19 and server 6 warnings, with no warnings in
the new modules. The two new tests use real V2 storage and canonical activation
registration with controlled failures. They do not launch a Worker or prove
process kill recovery. P5 terminal submission, fresh-process create-to-accept,
actual intent/admission/receipt kill windows, P6 model tools, P7 semantics,
Lotus and P9 remain outstanding. Dispatch remains disabled by default.
### Local slice 9: canonical Runtime result checkpoint

Worktree `bamboo-1481-runtime-results`; branch
`bamboo/feat/1481-runtime-results`, based on `0609cc3a`. A completed Child's
canonical saved checkpoint, exact birth/run, immutable dispatch spec, and
trusted Supervisor proof must match before Host postprocessing creates a
Submission. Unsaved output and a forged run are refused. Publication failure
keeps the existing durable-delivery acknowledgement pending. Duplicate
checkpoint processing returns the same receipt; cancellation preserves the
late Submission as stale. Submitted Work still requires explicit acceptance.

Final UTF-8 output is stored as a content-addressed Artifact under the scope
writer; the full manifest and verified fixed backup include its bytes. Reads
require a referenced Submission and Worker scope/input checks. Missing or
corrupt Artifact bytes cannot become a published authority snapshot.

Validation: engine `ticket_` tests 12/12; TicketService 29/29 (one unit, six
query, five authority, seventeen contract tests); server check; TicketService
all-target Clippy with `-D warnings`; formatting and whitespace checks pass.
The initial complete TicketService run failed one immediate reopen with a
retained OS lock. A descriptor-duplication regression reproduced that failure
deterministically. Explicit writer teardown now unlocks before closing so a
subprocess's temporary inherited descriptor cannot extend authority lifetime;
the regression and the actual second-process writer-rejection test both pass.
This does not bypass a live writer lock.

The two new engine tests use real canonical storage and activation control
planes with controlled saved completion checkpoints. They do not launch a
Worker process and do not satisfy the P5 actual fresh one-shot exit criterion.
Application scope/feature-flag wiring, dispatch recovery, confirmed-stop
resource release, real model semantics, and Lotus acceptance remain pending.

### Local slice 10: opt-in application scope and actual single-Work Runtime

Worktree `bamboo-1481-application`; branch
`bamboo/feat/1481-ticket-application`, based on `c02c2da9`. AppState now owns
one TicketService bound to the canonical Supervisor incarnation and Root tool
authority revision. Independent `features.ticket_mutation` and
`features.ticket_dispatch` flags default off. Only a fresh atomic Supervisor
bootstrap initializes a scope; existing or interrupted incomplete bootstraps
require explicit recovery/attach instead of automatic takeover. Turning flags
off preserves readable records and the result postprocessing service.

Authenticated owner endpoints under `/api/v1/tickets` expose scope, overview,
bounded search/inspect/changes, typed update/dispatch, immutable-key dispatch
query, and referenced Artifact reads. Client Authority/role/approved/private
source fields are refused. Run-scoped Worker credentials cannot become User
authority. The existing Child adapter/scheduler admits committed intents;
responses distinguish accepted-for-dispatch from Worker completion.

Validation: application tests 3/3 and HTTP wire test 1/1 pass. Server Clippy
retains the baseline engine/storage/server warnings; no new warnings remain.
The actual-process fixture `tests/ticket_runtime_mvp.rs` launches the compiled
local Host and native Worker against a controlled loopback HTTP provider. It
checks real admission, exact private Task callback, canonical Submission and
Artifact bytes, explicit User acceptance, then Host restart and same dispatch
receipt/run with no additional Worker provider calls. Two successive diagnostic
runs pass (14.33s and 14.58s). Run with `RUST_MIN_STACK=8388608 cargo test
-p bamboo-agent --test ticket_runtime_mvp --locked --offline -- --nocapture`.

The initial fixture run timed out waiting for submitted after 90 seconds;
its temporary Child state was not retained, so the cause remains unconfirmed.
Subsequent fixtures retain an identified `/tmp/bamboo-1481-ticket-runtime-*`
directory on failure and report seq, Work/Assignment state, private plan
revision, dispatch receipt, provider call count, and Host log. Successful
fixtures clean only their own stopped test data. These are controlled-provider
Runtime passes, not real-model semantic evaluation or full P5/P9 acceptance.
Confirmed-stop/cancel resource release, crash-window result reconciliation,
model-facing tools, P7 semantics, Lotus integration, and the full A1–A12
matrix remain subsequent work. No remote publication or production changes.
### Local slice 11: cancellation and completion reconciliation

Worktree `bamboo-1481-runtime-recovery`, branch
`bamboo/feat/1481-runtime-recovery`, based on `e4374e6d`.
Host-owned native process reaping now persists an independent stop checkpoint
bound to the exact Child birth and run receipt. Canonical completion recovery
publishes stop/submission before the existing broker receipt ACK. Recovery
never resumes a stopped Worker or grants its tools. Cancellation commits first
and interrupts the existing Child asynchronously. Started/unknown external
effects retain claims even after process stop; no external exactly-once claim.

Actual isolated Host/native HTTP-provider fixtures pass: complete private
Task/Submission/user acceptance/restart; cancellation while the provider is
held; and abrupt Host loss followed by original-receipt replay, unknown
quarantine and zero additional Worker provider calls. These are real process
tests with controlled provider responses, not semantic model evaluation.

The new native regression initially failed before any provider call: the
bootstrap read control overtook its permission-posture event on distinct
transport lanes. Only that exact read now retries the explicit pending-posture
response, bounded to 16 retries. Permission fences and mutation failures are
unchanged. Actual Host-loss tests also exposed a missing Work blocked reason
and a Session orphan `error` display masking Ticket outcome_unknown; both are
fixed and covered. Failed fixture directories/logs remain under the unique
`/tmp/bamboo-1481-ticket-*` prefixes. The earlier slice-10 90-second timeout
still has no retained cause and is not retrospectively declared resolved.

Focused service suite: 32 passing tests; native plan bridge: 5 passing tests;
owned-process reaping regression: 1 passing test. Runtime tests and precise
actual admission/result publication exits are tracked separately. A7/P5 is
not fully accepted until those exits and the existing owner barrier regression
pass. Clippy completes with existing engine/storage/server warnings; no
unrelated lint repair is included. P6 model tools, P7/P8/P9 remain in progress.

### Local slice 12: real publication exits and owner gates

Worktree `bamboo-1481-runtime-fault-acceptance`, branch
`bamboo/feat/1481-runtime-fault-acceptance`, based on `a125a2f9`.
The `ticket-runtime-fixtures` build feature installs an operation-scoped exit
hook only for the isolated fixture Host. Normal builds do not read the fixture
environment variables. Existing publication fault boundaries are reused; no
additional scheduling, crash-retry framework or production environment changes.

`cargo test -p bamboo-agent --test ticket_runtime_publication --features
ticket-runtime-fixtures --locked --offline -- --nocapture`: PASS, 3 cases / 6
real process exits, 25.68s. HEAD-before/after intent exits show zero Worker
calls. Admission exits retain the prepared exact run receipt, remain unknown
and never send a RunSpec. Result exits recover one nonstale exact-byte
Submission before broker ACK with exactly the original two Worker calls.

The unchanged #1483 owner entry point `ordinary_actor_mvp` /
`actual_four_children_two_rounds_collect_parent_results` passes on this stack:
4 actual Workers, 2 rounds, 8 unique outcomes, 26.73s. The existing
`session_app::child_completion_coordinator::tests` suite passes 49/49,
including callback/index barriers, partial admission and Any/FirstError short
circuit behavior. No duplicate #1479/#1480 implementation is included.
TicketService fixture-feature Clippy passes with `-D warnings`; normal server
build passes. These tests accept P5's local fresh-worker recovery bridge and
A7's local publication/owner integration gates. They do not accept P7 model
semantics, Lotus UI, arbitrary shell isolation or external exactly-once effects.

Runnable commands use `RUST_MIN_STACK=8388608`, `CARGO_INCREMENTAL=0` and the
shared build target documented above. Mutation and dispatch remain default-off.
No remote publication or production migration has been performed.

### Local slice 13: bounded Supervisor model tools and short rounds

Worktree `bamboo-1481-supervisor-tools`, branch
`bamboo/feat/1481-supervisor-tools`, based on `499f416a`.
Six model functions are conditionally installed when this same Host's
TicketApplication is available. HTTP and model tools share one authority and
published service. The original executing Supervisor incarnation is checked;
arguments cannot provide scope authority, User approval, ingress provenance,
or Worker/Runtime outcomes. Plan mode cannot mutate. Queries and commands
retain bounded schemas, typed operations, CAS and exact-input receipts.
Ticket argument repair warnings redact private content.

The first actual Host fixture failed because the ordinary SubAgent orphan
wait included Ticket-owned children, preventing a short Supervisor reply.
The retained fixture is `/tmp/bamboo-1481-ticket-lifecycle-ZZhWuY/host` and
`/tmp/1481-tools-actual.log`. The narrow correction inspects canonical Host
child/parent binding and Assignment metadata. It excludes only independent
Ticket children from a newly inferred wait; explicit waits and ordinary
children retain their existing behavior. Malformed/old/sibling records cannot
confer the exemption. No #1479/#1480 coordinator implementation is duplicated.

PASS: 3 tool authority/receipt/schema tests; 19 capability loading tests;
6 Ticket Runtime tests; 4 existing orphan/Bash wait tests; 1 argument privacy
test. The actual Host + controlled provider fixture passes in 14.95s: the
Native Worker remains held while the Supervisor returns, receives a second
human message and completes a second short reply, with no Root TaskList write.
The unchanged owner four-Worker/two-round entry point passes again in 18.57s,
with 8 unique outcomes. These controlled-provider checks are wiring evidence,
not real-model semantic evaluation. Server Clippy passes in 55.67s with
the existing engine/storage/server warnings and no unrelated lint repairs.

P6 is not fully accepted yet: complete accepted artifact input bytes,
inspect section selection, pause and safe explicit retry remain in the next
focused local slice. P7/P8/P9 and default-enable gates remain outstanding.

### Local slice 14: complete dependency context and execution controls

Worktree `bamboo-1481-context-controls`, branch
`bamboo/feat/1481-context-controls`, based on `603c38d3`.
The Child packet now carries every accepted managed UTF-8 Artifact's complete
bytes, content hash and exact source Work/submission/contract revisions.
The current generation, contract and accepted inputs are revalidated. The
whole encoded packet must fit 64 KiB: hard inputs are never truncated; missing,
external-without-resolver, corrupt and unsupported binary inputs fail closed.
Empty optional packet fields preserve canonical historical fingerprints.
`work_inspect` accepts declared history sections; omitted sections cannot be
mistaken for an empty authoritative history. Legacy requests select all.

Explicit pause commits blocked state and revokes execution before requesting
Runtime interruption. Confirmed stop releases a clean attempt, while pause
remains until explicit ready/reopen. Substantial contract steering interrupts
the old run. Reopen can detach only an owned-process-confirmed stopped attempt
with reconciled effects; lease expiry and unknown effects cannot authorize
retry. Cancellation/steering preserve an already unknown disposition.

PASS: service 37 tests (1 storage unit, 9 bounded queries, 5 file authority,
22 command contracts); native plan bridge 5; server Ticket integration 8;
Ticket all-target Clippy with `-D warnings`. The actual Host/native controlled
provider tests pass 3/3 in 19.93s: pause/stop/fresh-generation resume,
steer/stop/explicit retry, and accepted input bytes reaching a downstream model
request twice. Full immutable history remains verified by the publication
fault tests. No general continuation, shell access or effect-exactly-once claim.
Server Clippy passes in 55.97s with the existing engine/storage/server
warnings. P6's local bounded query/context/control gate is accepted;
P7/P8/P9 remain.

### Local slice 15: native questions and fresh answer-bound attempts

Worktree `bamboo-1481-worker-questions`, branch
`bamboo/feat/1481-worker-questions`, based on `15a0d46d`.
A Host-bound native Task callback can atomically save its own LocalPlan and
one bounded question, then end that one-shot Run. The Assignment remains
blocked while the Host independently reaps its process. An answer received
before that stop is saved without releasing execution. After exact stop and
all answers, explicit dispatch creates a fresh generation and empty private
plan. Its context carries only this Work's answered question records with
request, prompt, contract and source-attempt versions. No resident/native
continuation or new tool permission is introduced.

The initial actual fixture retained questions but failed resume with
`local_tool_history_unsupported` (`/tmp/1481-questions-actual2.log`, retained
`/private/tmp/bamboo-1481-ticket-lifecycle-iShy7m/host`). The local transcript
validator had required an assistant report. Only an exact Host question Task
receipt now permits a tool-ending transcript; arbitrary Worker observations
cannot grant it. A subsequent 28.14s controlled fixture passed, but additional
canonical checks found its final broker save still blocked: `completed Child
has no terminal transcript reply`. The retained diagnostic is
`/private/tmp/bamboo-1481-ticket-lifecycle-VpL8KF/host/host.log`; that earlier
pass alone does not accept the terminal lifecycle. The existing broker receipt
ledger now consumes a Rust-only Host proof over the exact Child birth, run,
Task source receipt and full transcript. Ordinary completion APIs still reject
a metadata-only exception. The same prefix/hash/save/ACK protocol is reused.

PASS: service 39 (1 unit, 9 query, 5 file authority, 24 command contracts),
native plan 6, existing local transcript 3, exact question transcript guard 1,
Ticket Runtime result/recovery 6, broker receipt/recovery 12, Ticket all-target
Clippy with `-D warnings`. The final actual controlled-provider entry point
`ticket_worker_questions` passes in 28.98s: five canonical completed/yielded
children and owned stops, Host restart, E/B/D/A/C exact answers, five fresh
generation-2 submissions, retained unanswered items and no sibling answers.
Questions create no fake Submission, and each initial worker uses one model
request. Real-model semantic evaluation and Lotus remain outstanding; this
fixture is deterministic routing/lifecycle evidence, not semantic quality.
The unchanged existing child-completion coordinator passes 49/49 (3.31s).
Actual pause/steer/accepted-input regressions pass 3/3 (19.70s). Server
all-target Clippy completes in 3m06s with the existing engine 19, storage 1
and server 6 library warnings; this slice introduces no new warnings there.
