# Live self-managed Session loops (#1991)

This is the first model-control slice of #896, built on the #898 counted runner.
A host can authorize an ordinary Root execution to start and manage one finite
loop in its current Session. The host capability does not start a generation.
The model chooses the objective through `loop_control` during that execution.
There is one execution owner, provider context, cancellation path and usage
ledger throughout the run.

## Host admission

Pass `LoopControlConfig::new(max_iterations, max_total_rounds)` to
`ExecuteRequestBuilder::loop_control`. Execute through the existing Engine
`Agent` (re-exported as SDK `RuntimeAgent`) registered/direct lease path. A raw request without that execution's private
owner proof fails before startup hooks or transcript writes. There must be
durable Session checkpoint support. This capability cannot be combined with a
host-prestarted `CountedLoopConfig`, enabled Gold, or a Ticket worker plan.

```rust,ignore
use bamboo_sdk::{ExecuteRequestBuilder, LoopControlConfig, RuntimeAgent};

// agent: &RuntimeAgent, with the normal host dependencies and activation router.

let control = LoopControlConfig::new(3, 12);
let request = ExecuteRequestBuilder::new(prompt, event_tx, user_cancel)
    .loop_control(control.clone())
    .build();
agent.execute_direct(&mut session, request).await?;
// A retained host handle can revoke this capability: control.stop().
```

Iteration ceilings are 1–100. The absolute activation ceiling is 1–1000 parent
provider rounds and must be at least the host's iteration ceiling. A stricter
existing `RunBudget.max_rounds` remains stricter. Rounds spent deciding whether
to start, inspecting status, or requesting changes remain charged. A start
must leave at least one round for each requested iteration. This is a finite
execution boundary, not a monetary or wall-clock guarantee: existing provider
retries, tools and auxiliary-model policies retain their own limits.

The ergonomic SDK `Agent::run` facade does not implicitly opt in; advanced hosts
use the typed `RuntimeAgent` request path above.

The tool is present only with host opt-in and remains subject to exact callable
identity, disabled-tool, permission and Session authority checks. The supported
model caller is a Standard ordinary Root. Child, Supervisor, and Ultra
orchestration-only executions gain no grant. The existing Plan executor policy
currently blocks the whole tool; its read-only `status` classification does not
override that gate. Host code can call `inspect_loop` on a Session value it is
already authorized to read. There is no target lookup or read grant in that API.

## Interaction boundary

A started managed generation stays within its original live owner. Before Start,
ordinary permission prompts, questions, child work and native hook suspensions
retain their existing behavior. Opt-in alone does not disable interaction.

The owner observes generic `ToolOutcome::Running` during this admitted activation,
including a wrapper that collapses it into an ordinary result. Before Start this
changes no result, permission, acknowledgement or cleanup authority. An observed
Running conservatively refuses a later Start in that activation, even when a
parallel timeout dropped the inner invocation. This is volatile host observation,
not persisted recovery state or proof that prior detached work has stopped.

Start refuses an enabled Guardian, an existing pending question/wait, active
children or background Bash. It also refuses native control hooks that may
suspend at a future Before/AfterRound, Before/AfterToolExecution or BeforeFinalize
seam. Unknown native hooks conservatively may suspend. A programmatic host may
override `AgentHook::may_suspend` to `false` for a handler that never suspends;
the declaration is frozen when registered. Returning Suspend despite that
declaration aborts under the ordinary failure contract, preserving injected
context instead of creating a wait or ignoring a trusted control decision.
Observer hooks and already completed setup seams keep their existing behavior.

After Start, each tool dispatch derives a host-only, invocation-local restriction
from the actual active generation, including siblings after Start in the same
batch. An actual permission Ask is rejected before a proxy, prompt or pending
request; it never becomes approval. Typed Allow/Deny, existing explicit Auto or
Bypass, Plan, exact identity and authority checks remain in force. Configless
custom checkers are conservatively refused before a potentially interactive
confirmation method; a cached grant is not inferred by replacing their gate.

Plan and SubAgent operations that create, update, launch, message or wait for
child work are refused before their effects, including auto_run=false variants.
Existing read/cancel authority paths remain. Explicit background/interactive
legacy Bash is refused before spawn; auto Bash stays foreground. Typed human
questions, legacy plan/approval payloads and agentic child/question results are
refused before any question/wait checkpoint or notification. Ordinary failures
remain paired once and count through the existing tool policy. The owner must
still receive a real terminal answer to complete an admitted iteration.

An unexpected Running after Start is a terminal tool failure, including custom
tools that ignore the restriction. The owner requests its cooperative kill once
and drops a Driven completion without polling or delivering it. It pairs one
false result, skips post-tool work and later siblings/provider admissions, and
uses the existing failed-activation checkpoint. It never publishes a successful
Running acknowledgement or Loop Complete. A unit-returning or panicking kill
callback is not a physical termination/join receipt: the existing resource domain
retains stop-unconfirmed, and unfinished counted input fences remain intact.
Native cleanup callbacks must cooperate; this adds no cleanup worker or join API.

Inline Workflow execution inherits this host scope. A Running refusal cannot
be converted to success by `ContinueWithError`; later tool calls, Agent retries
and nested workflow entries recheck the shared report after their admission
awaits. Already admitted branches keep the existing drain protocol. Independent
background Workflow Start/Restart retains its existing owned runtime.

The public direct/registered entry also retains its exact logical owner using
the existing registration quarantine. Its stored opaque host observation is
checked on ordinary return and synchronously on future Drop; a completed kill
callback never releases that owner. A new genuine User or queued wake is not
physical-stop evidence and cannot start a successor over the retained owner.
The host must explicitly reconcile it under the existing finalization protocol.
This adds no durable marker, automatic release, restart or recovery path. Loop
Failed describes the loop outcome and is not a confirmed-stop receipt.

Active managed tool calls use the existing sequential apply boundary. Calls
previously classified ParallelSafe retain a per-call deadline bounded by the
smaller existing per-tool and parallel-batch timeout. This is a per-call limit;
a serialized collection does not have the old parallel batch's total elapsed
time. Ordinary execution before Start keeps its existing parallel behavior.

The no-progress and terminal safety nets fail an unexpected wait without
creating a suspension. They do not pretend an unfinished iteration completed.
Interactive work must occur in a later ordinary host activation outside the
managed generation; Stop still drains its admitted batch and grants no next
round. The existing failed-iteration and persisted-crash replay fences remain
unchanged. This prevention contract adds no pause/resume, automatic recovery,
new execution owner or replenished budget.

## Commands and versions

`loop_control` implicitly addresses the executing Session. Every action rejects
unknown fields. There is no Session-id argument. All actions are serial and
cannot be promoted to another runner; only `status` is read-only.

| Action | Required command fields | Result |
| --- | --- | --- |
| `status` | `action` | Current state, accepted/applied revisions, objective, pending objective, consumption and host ceilings. |
| `start` | `operation_id`, `expected_revision`, `objective`, `max_iterations`, `max_total_rounds`; optional `expected_generation` | One new generation and a durable receipt. An absent prior loop uses revision 0 and omitted/null expected generation. A retired prior generation must be named explicitly. |
| `update` | `operation_id`, `generation`, `expected_revision`, at least one of `objective`, `max_iterations`, `max_total_rounds` | A durable accepted edit. Bounds can only tighten and cannot fall below consumption. |
| `stop` | `operation_id`, `generation`, `expected_revision` | A sticky stop for this generation. No later provider round or iteration is admitted. |

Identifiers are UUID strings. Objectives are nonblank and at most 16 KiB of
UTF-8. The serialized LoopState management record, including escaping, a possible
pending objective and receipts, must also fit the existing 128 KiB read bound.
An oversized edit is rejected while the prior state remains readable.

`state_revision` changes for ordinary checkpoints. `control_revision` changes
only for a newly accepted control mutation, so a status observation can be used
for a later-round CAS. `objective_revision` identifies the applied objective;
the admitted iteration separately retains its objective revision. A receipt
reports `accepted_revision`, `applied_revision_at_acceptance`, the requested
`objective_revision`, `effective_iteration` and `applied` at acceptance. A status
observation reports subsequent application; the original receipt is immutable.

Within the current activation's generation, repeating an operation UUID with
the same typed payload returns its original receipt before CAS validation.
Changed payloads conflict. A retired generation's receipts do not authorize or
acknowledge a new activation. At most 64 compact receipts are retained per
generation. At that bound, new model mutations are rejected, including a new
stop operation; duplicate receipts and independent host/user cancellation still
work. The finite bounds continue to apply.

## Iteration and stop boundaries

The round that requests start finishes its already-admitted tool batch. It is
not an objective iteration. Before the next provider call, the existing runner
checkpoints the chosen objective as a counted User input and charges the first
iteration. No extra judge or final summary call is introduced.

An objective edit is accepted and durable immediately, with at most one pending
edit. The current iteration keeps its admitted objective through its remaining
tool rounds and terminal answer. Only the next iteration admission installs the
new text and objective revision. An older iteration's completion cannot count
as completion of the new objective. A pending objective cannot be overwritten
by another objective edit; stop can discard it. Bound-only edits may still
constrain iteration and round admission immediately while that objective is
pending, but they cannot remove its required future iteration or round. Neither
a new objective edit nor a bound-only edit retaining an existing pending
objective is accepted without that future budget. While an iteration is active,
this minimum is two remaining rounds: one for its later terminal answer and one
for the pending objective's admission/answer. Before the first iteration, one
remaining round suffices. These are minimum admissible paths; they do not grant
extra rounds or guarantee success if the current iteration needs more work.

A bound-only edit does not advance the aggregate `applied_revision` past an
unapplied objective. Its immutable acceptance receipt retains `applied: false`
and the next effective iteration; status advances the applied revision only
when the next iteration admits that objective. Prior acceptance receipts remain
unchanged after admission.

Stop drains the already-admitted round/tool batch, then prevents the next
provider admission. It does not cancel a tool that has already started. It
cannot be cleared by update or another start. There is at most one generation
per activation, including a generation whose checkpoint failed. The host's
retained capability stop token and the user's independent cancellation token
are sticky. User cancellation has priority over command acknowledgement and
the next provider admission.

## Checkpoints and failure behavior

The stateless tool validates only. The owning runner applies accepted commands
after existing execution gates. It checkpoints the proposed loop state,
operation receipt and exactly one tool-call/result pair together before
emitting a successful ToolComplete. Approved permission replay cannot mutate a
loop; it requires a fresh owner command. Status and rejections also retain the
normal complete tool transcript pairing.

The owner uses the exact output cap admitted for that tool dispatch, before
checkpointing any result. Large status projections may omit only current and
pending objective text, reporting their omitted UTF-8 byte counts while keeping
canonical goals and all version/budget fields. A mutation is accepted only if
its complete immutable receipt fits; a cap too small for that receipt rejects
before installing generation, revision, receipt or stop effects. Zero means the
existing unlimited policy. A budget-delivery failure does not claim that an
already accepted duplicate operation was unapplied; very small positive caps
can require an empty failure result.

AfterToolUse receives the bounded, checkpointed owner result. Optional native
feedback is correlated by tool-call ID and projected through the existing
volatile lifecycle-hook context. The result and complete added context,
including its inter-context separator, share the captured tool-output cap.
Canonical receipts and the unique paired result remain byte-for-byte unchanged;
ordinary append-safe checkpoints never rewrite existing transcript messages.
Before ToolComplete, a second checkpoint saves any added hook context through
the existing runtime state and verifies that the original pair is intact. A
failed feedback checkpoint keeps the accepted operation and conservative state,
stops execution, and publishes no ToolComplete for that call. Cancellation wins
during hooks, either checkpoint, and a blocked acknowledgement publisher. The
existing policy circuit observes the final owner result exactly once, including
stale or budget rejections.

| Boundary | Behavior |
| --- | --- |
| Invalid/stale command, receipt quota or oversized edit | Rejection result; no loop state, budget or stop-token mutation. |
| Objective accepted | Receipt identifies the next effective iteration; current objective remains admitted. |
| Checkpoint fails before write or after an uncertain commit | Preserve the checkpoint error, reserve the generation conservatively, stop the execution, publish no success event and make no further provider call. Later error persistence keeps complete pairing and a failed/cancelled generation. |
| User cancellation at command/checkpoint boundary | Cancellation outcome wins; no successful completion is published. |
| Unexpected detached Running after Start | Request cooperative cleanup, pair failure and fail the activation; keep stop-unconfirmed and recovery fences, with no later tool/provider admission. |
| Interrupted persisted live/admitted state | Existing #898 recovery refusal remains in force. No replay, wakeup, receipt-driven restart or budget replenishment. |
| Terminal/finalizing generation | Edits are rejected. Existing finalization and one-Complete fences remain authoritative. |

## Remaining parent scope

#896 remains open for HTTP/index endpoints, generic stop/delete/cleanup
integration, cross-activation pause/resume and cadence. Those actions are not
accepted by this tool. #893/#897 retain their broader continuation contracts.

#1976 can reuse the typed command/receipt vocabulary only after separately
proving trusted caller identity, target lifetime, attachment, scope, permission
and execution ownership. This module's private owner apply function is not a
cross-Session API. No Supervisor grant, second executor, scheduler, timer,
recovery manager or persistence store is added here. This slice is excluded from
the current Nova release and makes no real-model or paid calls in validation.
