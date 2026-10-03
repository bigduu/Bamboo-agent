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

### Local slice 16: durable semantic proposal and atomic groups

Isolated `bamboo-1481-semantic-resolution` / `bamboo/feat/1481-semantic-resolution`
starts at a526a0ab. Human ingress, processing-time fixed basis, complete
zero-to-many proposal and per-item group outcomes use the existing full
manifest. Admission can queue ahead; pinning happens when processing starts.
Each indivisible group publishes typed domain changes, temporary-ID mappings
and its original User receipt together. Independent failures are retained.
File locking covers short transactions only; model evaluation is external.
Restart replays saved receipts/proposals, with old-epoch pending proposals
stale rather than regenerated. Optional fields preserve old snapshot bytes.

Host-only ingress capabilities cannot be deserialized. Model operations are
an explicit semantic allowlist; no Runtime result, role, effect, patch or
provenance operation is admitted. Exact target revisions, request identity,
generation, contract and action fingerprint are checked against authoritative
records. The approval guard conservatively requires an explicit current Human
clause identifying the action; questions, vague consent, negation, conditional
consent, modified amounts and external/tool authority text cannot approve.
This guard does not replace semantic target selection by the Supervisor.

PASS: Ticket service 54 tests (previous 39 plus 15 deterministic proposal,
order, replay, concurrency, ambiguity, exact binding and publication tests),
all-target Ticket Clippy with `-D warnings`. Logs:
`/tmp/1481-resolution-service3.log`, `/tmp/1481-resolution-clippy3.log`.
An initial queued-input test needed `ResolutionBasis` equality derived;
the final run passes. This is domain evidence only. Host semantic tools,
real-model Chinese evaluation, multi-pending Lotus and P9 remain pending.

### Local slice 17: authenticated Human delivery and precise response wire

Isolated `bamboo-1481-user-ingress` / `bamboo/feat/1481-user-ingress` starts at
88995e8d. Chat/client DTOs add optional message ID, thread, citation and trace
fields; old clients keep their prior path. Ticket Supervisor inputs require
verified owner access, reject Worker run credentials and reserved activation
trace values, and persist original Human text before delivery. Ticket ingress
order and Inbox delivery generation are independent axes. Hooks/documents do
not replace this approval source. Native SDK envelopes already carry these
fields; committed events and the sanitized history projection now retain them.

The existing Messenger/SDK checkpoint, receipt and ACK protocol admits each
User turn once. An initial actual test exposed HTTP execute preparation's
last-User gate: a queued first turn had not yet reached the transcript. A
narrow adapter invokes the same SDK boundary before preparation, skips live
runners, and refreshes the existing startup handoff. Typed activation intent
supports an active runner's successor. Exact cold replay does not reset a
completed turn to pending. Failed fixture/log retained at
`/private/tmp/bamboo-1481-ticket-lifecycle-V1VoJU/host` and
`/tmp/1481-ingress-actual.log`; it is not counted as a pass.

Capability-negotiated request responses bind scope, request, Work, Assignment,
generation, contract and prompt revisions, decision kind and action fingerprint.
The User service validates current versions/status under CAS. Exact successful
retries return the original receipt. No pending singleton or client role is
used to select approval authority.

PASS: actual Host reference/Inbox/SDK/history/cold replay and precise A-only
approval tests 2/2 (8.17s); actual short Supervisor/held Native Worker regression
1/1 (16.38s), `/tmp/1481-ingress-actual3.log`; existing chat tests 67/67 (37.71s).
Server/client/TUI all-target Clippy completes with only the existing engine 19,
storage 1, server library 6/test 10 warnings after two new needless borrows
were removed. Authenticated live model catalog readiness passes for configured
`gpt-6-sol` using existing encrypted credentials read-only (0.11s),
`/tmp/1481-model-readiness.log`; no extra credential/permission is required.
The opt-in `ticket_model_semantics` target is excluded from ordinary tests.
Readiness is not semantic evaluation; P7 model routing and Lotus remain open.

## Slice 18 — bounded Supervisor Human resolution (local P7)

Worktree `bamboo-1481-supervisor-resolution`, branch
`bamboo/feat/1481-supervisor-resolution`, base `758f505c`.
The six existing tools keep their original Supervisor incarnation/Plan gate.
`work_overview` reads the oldest unresolved canonical Human input and its fixed
contracts/requests; `work_update` saves the entire bounded proposal and settles
independent groups. The model supplies no User/source/scope or operation ID.
Zero groups durably resolve chatter. A saved proposal is replayed verbatim;
input order blocks bypass through ordinary model mutations. Plan reads and a
disabled mutation flag do not pin/publish resolution metadata.

The Host reuses its existing post-receipt dispatch and cancellation path. It
replays only still-current pending/admitted attempts; stopped, superseded and
unknown runs cannot be revived. Precise User decisions notify the existing
Inbox/activation machinery after commit, without conveying an approval grant.
Derived message/group IDs cannot overwrite pre-existing legacy receipts.

PASS: semantic domain 16/16 (11.23s), `/tmp/1481-resolution-service4.log`;
Host model tools 5/5 including whole proposal, per-group results, chatter replay,
Plan read-only and JSON authority rejection;
actual short Supervisor plus held Worker 1/1 (29.80s),
`/tmp/1481-supervisor-resolution-actual.log`; actual one-message A answer/B steer/
C atomic create-ready-start/D cancel plus Host restart 1/1 (17.00s),
`/tmp/1481-supervisor-resolution-multi.log`. These actual Host/Runtime tests use
a controlled provider and do not establish natural-language model quality.
All-target server/Ticket Clippy completes with the existing server 6 library/
10 test warnings (engine 19/storage 1); the added needless borrow was removed.

Real model evaluation: existing configured `gpt-6-sol`, encrypted credential
read/decrypted only in the test process, 2026-10-03. Twelve Chinese fixtures
PASS in 122.85s: chatter, vague yes, E answer, same-name ambiguity, cross-topic,
A/B/C/D multi-intent, explicit A approval, B denial, changed amount, conditional
approval, quoted malicious tool text and optional reference without a grant.
Wrong approvals: zero. Production proposal schema/guidance and local service
validation are shared. This is real-model **proposal-only synthetic** evidence;
no payment/email/shell or other external action runs. One pass is a bounded
acceptance sample, not a statistical correctness or exactly-once guarantee.

Reproduce with `BAMBOO_TICKET_MODEL_CONFIG_ROOT=<existing read-only config root>`
and `cargo test -p bamboo-agent --test ticket_model_semantics
live_chinese_ticket_semantics -- --ignored --nocapture`.
Raw synthetic report: `/Users/bigduu/Documents/Codex/2026-10-02/task/1481-evidence/p7-live-semantics.json`,
SHA-256 `bc6620736eec3cddfbd27114c80e585d462537a77652bacc07a9402726e0bdfa`;
log `/tmp/1481-live-semantics.log`. Missing configuration fails rather than using
a fake provider. Initial live-test compilation failed from a shadowed Result
alias and was corrected before this run. No production config/data is modified.

P7 is locally covered by the above bounded proposal/Host tests. P8 real browser
and P9 migration/full acceptance remain separate gates; flags remain default off.
The parent thread outgoing relay tool is unavailable in this environment;
progress continues in the current thread and this ledger, not remote comments.

## Slice 19 — P8 current Lotus / real Host browser acceptance

The isolated Lotus worktree is `lotus-1481-ticket-overview` on
`lotus-next/feat/1481-ticket-overview`, baseline `1131c275`. It negotiates the
canonical Supervisor scope and uses one normal composer with optional bounded
message references. The panel reads a fixed authoritative snapshot plus bounded
changes, renders all questions/approvals and submitted artifacts, rejects stale
generation/contract/request versions, and keeps exact uncertain-ACK retries.
Only an ordinary question's definite `revision_conflict` receives one bounded
rebase after the same request is revalidated; approvals never auto-rebase.

`tests/ticket_browser_fixture.rs` starts the actual local Host/native workers,
waits for five durable questions and owned stop facts, restarts the Host, serves
the current Lotus build, and checks the browser's five fresh generation-2
Submissions and exact answers. Chromium answers E/B/D/A/C through real cards,
reloads mid-flow, uses an optional reference in the ordinary composer, approves
A while B stays open, changes B's material contract, and proves old buttons and
requests cannot succeed or revive after reload. Workers remain submitted until
explicit acceptance. Controlled provider results here are separate from the
12-case real configured-model proposal evaluation in Slice 18.

PASS: Chromium 1/1, 28.6s (`/tmp/1481-lotus-browser4.log`); actual Host fixture
1/1, 111.68s including browser wait (`/tmp/1481-browser-fixture-run4.log`).
Screenshots and full Runtime checks are retained in `../1481-evidence`, prefixed
`p8-ui-fixture4`. Earlier failed browser attempts are retained too; they exposed
coverage naming and an ordinary answer CAS race, and then a controlled-provider
fixture wake omission, all corrected before this pass.

Reproduce by building Lotus, then setting `BAMBOO_TICKET_FIXTURE_STATIC_DIR` to
its `dist` and `BAMBOO_TICKET_FIXTURE_INFO` to a fresh evidence JSON path while
running the ignored `current_lotus_five_native_questions_browser_fixture` test.
After `TICKET_BROWSER_READY`, set `LOTUS_TICKET_FIXTURE_INFO` to that path and run
`npm exec --offline -- playwright test --config=playwright.tickets.config.ts`.
No production configuration or data is used. The Host fixture expires after
360s and retains temporary state on failure.

P9 explicit legacy attach/import and offline migration remain separate.
Shared #791 integration contracts changed by the stack are optional Chat API
message IDs/references, durable Inbox Human-source sequencing and replay,
original-Supervisor tool proof, exact existing dispatch/query and result ports,
and the Ticket-only independent-child wait exemption. No owned Inbox lease
expiry or generic Root/runtime ownership is enabled; #1334/#1341 retain their
existing implementation owner. Other worktrees are untouched.

## Slice 20 — explicit offline legacy import and authority transfer (P9)

Worktree `bamboo-1481-offline-migration`, branch
`bamboo/feat/1481-offline-transfer`, based on `e3b1029d`. The P8 Lotus work is
locally committed as `f5fc47f`; its final startup CSS is 105971/106000 bytes,
with the original budget unchanged. No remote writes or production migration
are performed.

`bamboo tickets` exposes read-only `preview`, explicit `attach`/`import`, fixed
read-only `backup`, `migration-plan` and stopped-host `migrate`. Preview copies
and validates a bounded complete source Session tree in a temporary directory;
it never initializes the source. Import verifies the reviewed canonical source
hash, selected exact Task IDs and canonical Supervisor incarnation. A managed
immutable source Artifact preserves the whole original Session, Task states and
evidence. Old completed Tasks become blocked needs-review, with no fabricated
Submission, acceptance or transferred execution. Ordinary HTTP/model mutations
cannot provide an import source. Exact retries reuse the original receipt,
including recovery after the Ticket commit precedes the canonical Root flag.
Active, previously executed or ownership-unknown legacy Roots stay preview-only
pending the existing Root ownership reconciliation boundary.

Schema 1 bytes remain compatible for existing records. Full source imports and
offline transfers publish schema 2; a schema-1 binary cannot downgrade or write
that HEAD. Migration holds both local writer OS locks, checks the current fixed
commit/CAS, canonical binding, stopped Root/known children and reconciled effect
ledger, then publishes original retirement before copying. Every reachable full
revision, manifest, commit and managed blob is verified. The copy retains a
durable read-only marker while its new epoch is published; original HEAD then
consumes that exact activation before the marker is removed and directory
flushed. Original authority remains retired. Incomplete staging resumes only
the same bound request; corruption is never silently repaired. The immutable
OperationReceipt stays identical through all stages and retries; transfer stage
is a separate observation. Old approvals expire, and old pending intents cannot
acquire new run authority. Existing terminal keys remain queryable with the same
run receipt. The canonical Supervisor tree and Inbox receipts are preserved;
the existing Session index rebuild recovers only derived indexes and grants no
new activation permission. Provider configuration/credentials do not migrate.

Internal decision wake envelopes use the existing `hidden_from_ui` metadata.
Semantic user-required accept additionally needs an explicit complete current
Human clause naming the Work/submission, so a model proposal cannot promote
chatter, negation, conditions or quoted tool text into user confirmation.

PASS: Ticket domain 61 unique tests; `/tmp/1481-p9-domain-final1.log` and final
acceptance-source regression `/tmp/1481-p9-semantic-final2.log` (17/17, 10.34s).
The additional errno recovery test passes ten injected ENOSPC/EACCES cases
across write, file flush, HEAD replacement and the following directory flush
(`/tmp/1481-p9-errno-final.log`, 1/1, 2.73s), bringing distinct domain checks to
62. It never fills a physical volume or changes permissions; those physical
failure mechanisms remain untested.
The offline suite injects failures at 40 retirement, 234 export and 76 activation/
marker-release I/O boundaries (306.51s); existing storage tests cover 32 I/O and
48 actual process-exit publication boundaries. These are local filesystem /
injected I/O observations, not a power-loss or external-effect guarantee.

PASS: actual Host/native migration and flag rollback 2/2 (27.75s),
`/tmp/1481-offline-host-actual9.log`; final related native regression 19/19,
`/tmp/1481-p9-native-regression1.log`. It includes six actual exit-71 windows,
short Supervisor/held worker, same-message A/B/C/D operations, five questions,
owned cancellation, unknown execution quarantine, accepted inputs, CLI migration
and generation 2, and in-flight result preservation with both flags disabled.
Earlier failed test runs are retained: malformed legacy fixture data, missing
derived index, staged Inbox activation expectation, terminal lookup expectation,
and a destination fixture credential reference. The fixes passed the above
runs; no production key/configuration was changed.

PASS: final explicit offline Host checks 4/4 (4.42s), including interrupted
attach recovery with unchanged Ticket HEAD (`/tmp/1481-p9-offline-host-final.log`);
server Ticket library checks 11/11 (3.36s,
`/tmp/1481-p9-server-ticket2.log`). These are targeted checks, not a claim that
the entire server/workspace suite was rerun.

PASS: final current Lotus Chromium 1/1, 45.5s
(`/tmp/1481-p9-lotus-browser4.log`); actual Host cross-check 1/1, 94.87s including
browser wait (`/tmp/1481-p9-browser-fixture4.log`). Five exact own answers produce
five generation-2 Submissions with stopped native processes, all still awaiting
acceptance. The UI retains definite decision conflicts through read refresh;
explicit ordinary-question retry is bounded to three user clicks. Approval and
uncertain-ACK behavior is unchanged. Internal decision wake messages are hidden
after replay. Screenshots were inspected; full Runtime evidence is retained as
`../1481-evidence/p9-ui-final4*`. The first final attempt exposed an error being
cleared by refresh and failed; attempt 2 was cancelled before the browser to
rebuild; attempt 3 was interrupted by exec-server transport recovery with no
test completion. These reports remain alongside the successful final run.

PASS: complete Lotus 2049/2049 tests (123 files), type-check, lint, architecture,
build and package budgets (`/tmp/1481-p9-lotus-final-verify.log`); CSS remains
105971/106000 bytes, startup JS 1422851 raw / 432706 gzip bytes. Rust all-target
Clippy for Ticket/server/agent passes with existing unrelated warnings
(`/tmp/1481-p9-clippy-final.log`); Ticket all-target strict `-D warnings` passes
(`/tmp/1481-p9-ticket-strict-clippy.log`). No baseline warnings were suppressed.
Final formatting and both repository diff checks pass. The unchanged existing
#1479 coordinator passes 49/49 on this final stack in 3.62s
(`/tmp/1481-p9-coordinator-final.log`); the original owner and runtime protocol
boundaries are preserved.

### Runnable local entry points

Build the local CLI with `cargo build -p bamboo-agent --locked --offline`.
Stop both relevant Hosts before attach/import or migration. Example commands use
operator-chosen offline data directories, not the user's production store:

```sh
bamboo tickets preview --data-dir <source> --source-session <exact-session-id>
bamboo tickets attach --data-dir <source> --expected-snapshot <preview-hash> --operation-id <stable-id> --task-id <exact-task-id>
bamboo tickets import --data-dir <source> --source-session <exact-session-id> --expected-snapshot <preview-hash> --operation-id <stable-id> --task-id <exact-task-id>
bamboo tickets backup --data-dir <source> --destination <new-backup-directory>
bamboo tickets migration-plan --data-dir <source> --destination <existing-empty-data-directory> --operation-id <stable-id>
bamboo tickets migrate --data-dir <source> --request <reviewed-request.json>
```

Save only the `migration-plan` result's `.request` object in the reviewed JSON
request file. `migrate` requires that exact fixed source commit and destination;
after interruption, reuse the identical request/operation ID. Ordinary backups
remain read-only. Configure a destination provider independently through its
already-authorized setup before enabling new execution.

The complete encoded Supervisor tree is capped at 1 MiB, 512 regular UTF-8
files and depth 8; symlinks/unsafe paths are rejected. Larger histories are
refused, never truncated. Mutation/dispatch remain independently default off.
The real native ceiling is Task-only. Arbitrary same-UID shell/coding filesystem
isolation, remote lease expiry with a physically surviving coding process,
other filesystem platforms, sudden power loss and real irreversible provider
effects are **UNRUN**, not inferred from these tests. #1334/#1341 retain the
existing #791 owner; this stack does not activate owned Inbox lease expiry or
claim completion of the general Root/runtime writer/ACK/release protocol.

### A1–A12 evidence status in the supported opt-in local scope

| Item | Status | Evidence and limit |
| --- | --- | --- |
| A1 | PASS | One short Supervisor plus held native worker; five independent actual Work/Assignment/plans. |
| A2 | PASS | Five native questions E/B/D/A/C, restart and exact own-answer context; real-model E/no-reference proposals evaluated separately. |
| A3 | PASS | Actual same Human A-answer/B-steer/C-create-ready-start/D-cancel and restart receipt; atomic fake/domain negative cases. |
| A4 | PASS | Exact A-only buttons, stale revisions/generations/fingerprints and malformed references; 12-case live proposal evaluation has zero wrong approvals. Synthetic actions never execute. |
| A5 | PASS, bounded | Ordinary/native private Task plans and sibling/root denials; native tool ceiling is Task-only, no arbitrary shell claim. |
| A6 | PASS | Concurrent CAS/receipt and ingress/proposal replay; all publication/process-exit windows and frozen migration receipt. |
| A7 | PASS | Six actual Host exit windows plus reused #1479 coordinator 49/49 and four-Worker barriers; no duplicate owner fixes. |
| A8 | PASS | Late generation domain archive; actual pause/steer/cancel/owned stop and Host-loss quarantine without automatic redispatch. |
| A9 | PASS | Explicit exact submission acceptance, accepted input bytes, downstream invalidation and independent Goal evidence. |
| A10 | PASS | Fixed pagination, coverage/omissions, cursor resync and stale event/request state; actual current Lotus browser evidence. |
| A11 | PARTIAL | Canonical worktree claims and unconfirmed/unknown-effect ownership are tested; actual local cancellation/unknown runs are tested. Physical arbitrary coding writes and remote lease-expiry survivor case are UNRUN. |
| A12 | PASS, bounded | Full reachable hashes/readonly backup, fixed stopped-authority transfer, original retirement/new epoch, Inbox receipt preservation and import replay; active/unknown legacy import stays read-only. |

P0–P8 have the local evidence recorded above. The Task-only P9 local rollout is
validated by the recorded bounded tests; the full A11 coding/remote lease gate
and independent current-head review remain open. The tracker is not marked
fully complete or default-enabled. No push, PR, merge or deployment is authorized.
