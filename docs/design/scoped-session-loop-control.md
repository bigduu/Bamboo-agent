# Scoped Supervisor Session management (#1976)

## Product goal and present delivery status

The Supervisor exists to manage **other Sessions**. It must eventually assign
and update their goals, observe progress, pause/resume or stop work, receive
results, and decide which authorized work to schedule next. Executing the
task itself or enabling an Ordinary Root's self-loop is not completion of
that product goal.

This branch is the first **static implementation candidate**, based on actual
protected dev `bf6b20bab2832b45d4b0927e971cea64866f2c77`. The original pushed
design `305735f980e4b4316924216077930e644fb69bdc` and its 15 uncompiled synthetic
contract tests were inspected and preserved separately. #1992 is now accepted
in dev; its old test receipts are not acceptance evidence for this candidate.

The current slice connects a strict executing Supervisor to finite
`start/status/update/stop` on **one already attached and separately host-granted,
currently running Standard Ordinary Root**. It is not the whole #1976 feature.
No Cargo, Rust compiler, Clippy, runtime test, real provider, or paid model call
has been run for this new candidate. Nova owns the sole Rust validation window.
Standalone formatting and static source review are not type checking or
runtime acceptance. #1976 remains open; current frozen Nova/release sources
remain independent.

## Reused interface and authority

Keep `loop_control` self-only. Extend the existing Supervisor management tool:

```json
{
  "action": "loop",
  "target_session_id": "attached-root-a",
  "expected_link_revision": 7,
  "command": {
    "action": "update",
    "operation_id": "a4b85b5b-ba44-488d-9900-20dd7e016e88",
    "generation": "b23bc011-5424-4b5e-8fc9-3c861b89874b",
    "expected_revision": 4,
    "objective": "Verify the revised finite objective"
  }
}
```

`status` may omit `expected_link_revision` to bootstrap its current value,
but still requires the same current host grant, strict identity, enabled link,
Project and target-lifetime checks. Mutations require a nonzero exact link
revision. A status query may additionally supply `receipt_operation_id` to
read one actual target-side control outcome. This does not grant write access.
Unknown fields and JSON claims of caller, grant, Project, owner, or permission
are rejected. The shared command DTO/parser moves down to Domain; the existing
`bamboo_tools::tools::loop_control` public path re-exports it unchanged.

The tool retains the original `ToolCtx.executing_supervisor` observation.
Plan, disabled-tool, ordinary permission and exact identity gates remain.
Loop actions are serial and nonpromotable. Permission replay, including a
legacy replay with no generation, cannot submit a fresh Loop command; old
Followup replay behavior is preserved. Ordinary Roots, ordinary Ultra Roots,
reserved-id impostors and children acquire no cross-Session capability.
Thinking posture does not replace strict Supervisor identity.

Attachment alone, the target's self-loop opt-in, schema registration and an
orchestration whitelist entry are each insufficient. A separate host-only
`SupervisorLoopControlGrant` bounds the exact original Supervisor incarnation,
source tool-authority revision, link revision, permitted actions and finite
limits. The target host installs it through `LoopControlConfig::supervisor_control`.
At the real execution boundary the runtime binds target id/birth/metadata
version/Project, router registration id and pipeline execution id. These two
execution ids are distinct. The existing ActiveOwner holds the private slot;
there is no second registry or durable capability store. Installation and host
revocation use the existing target publication fence. Execution cancellation
and host stop are checked again by the actual physical publication guard.

The initialized target runtime frame is checkpointed through its existing
owner before exposing this slot. Finishing/finalizing/replacing the owner
expires it. A persisted LoopState or Inbox envelope never reconstructs it.

## Structured delivery and target application

The model tool calls the existing `SessionMessenger`, which obtains only an
observation from the live slot and revalidates canonical authority in Storage.
The mutation travels as a registered typed RuntimeInstruction,
`supervisor_loop_control_v1`, through the **existing Session Inbox**. It uses
the existing expected-run QueueOnly intent and live-owner fence. It never
calls generic `request_activation`, reserves a successor, or creates a Root.

Generic delivery cannot publish this control carrier. Ordinary and selected
input consumers exclude it; `to_provider_message` rejects it. It is not a
genuine User, free-text followup, Skill input, or fabricated target ToolCall /
ToolResult. Existing #2000 delivery retries, selected-input fences and input /
output attribution semantics remain separate.

The existing target owner drains only this typed control before counted
`before_round`, objective injection and budget charge. It uses the same
proposal/CAS/finite-bound algorithm as the self tool. Callers explicitly
supply already charged rounds: self-tool `round + 1`, remote boundary
`turn_counter`, including zero before the first provider round. No budget is
reset or fictitiously charged. Self and remote changes contend on one
generation and control revision. A pending objective still needs the existing
next-iteration admission and enough remaining rounds for the active terminal
answer plus that admission.

Canonical validation occurs again at the actual target checkpoint, retaining
the existing lifecycle → Task → sorted Session locks and exact live-owner
fence through the physical write. This does not call a public checkpoint while
holding a guard that would reacquire its target lock. The existing full writer,
Root Actor proof and Inbox ACK protocol are reused for Root-owned and legacy
paths. Backend defaults fail closed rather than falling back to an unrelated
ordinary save.

LoopState, source-bound immutable outcome and typed consumption evidence are
staged in the same target checkpoint. The Main admission cursor must confirm
that typed input before ACK or a configured readback. Runtime-sidecar bytes
alone are not proof of a confirmed Main checkpoint. The operation identity
includes initiator incarnation, exact target lifetime, live grant and owner;
the payload fingerprint includes the typed command. Identical authorized
retries recover the original receipt before CAS, while changed payloads or a
different origin conflict. The model cannot borrow another initiator's receipt.

Invalid CAS or effective bounds are checkpointed as explicit rejected outcomes
under still-valid authority, without Loop mutation. Missing/revoked authority,
owner replacement or an unsupported backend fail closed. A physical checkpoint
or ACK error stops further provider admission and preserves unknown/partial
publication evidence; it does not assert rollback. A strict canonical reload
may recover observable bytes. If that reload also fails, a local volatile
no-save fence prevents later generic finalization from overwriting canonical
state with the proposal or an older snapshot. That marker is never persisted
and grants no recovery. Started filesystem jobs retain their real guards even
when the requesting future is dropped.

Only a confirmed target checkpoint installs local Start/Stop effects. An
observed partially committed generation can be bound solely for existing
error/cancellation retirement, never to continue work. Independent user stop
takes priority over acknowledgements and subsequent provider admission.

## Truthful progress and receipts

A mutation response confirms Inbox admission and active notification only.
`configured: false` there means target application has not been observed in
that delivery response, not proof that the target cannot already have raced
ahead. The caller uses `status` with `receipt_operation_id` for actual readback.

Status returns bounded canonical Loop progress, finite/remaining budgets,
generation and separate state/control/objective revisions. A confirmed
`operation_receipt` comes from the target's typed checkpoint plus Main cursor.
`objective_applied_actual` additionally requires the matching real
`counted_loop_iteration` User message in canonical history and the target's
admitted objective/control revision. An accepted pending update, a partial
runtime publication or the old iteration's terminal answer cannot claim this
proof. Long objective previews are explicitly truncated; canonical goal bytes
and the initiating Supervisor's normal output cap are preserved.

Stop cancels this live grant, including its read retainer. The first slice can
therefore prove a stopped checkpoint from strict host/canonical evidence, but
the initiating Supervisor cannot use live status to query that Stop receipt
afterwards. Owner finalization has the same read limit. Terminal readback needs
a separately designed read capability; this branch does not weaken the shared
write/admission retainer to provide it.

The target retains its provider/model, permissions, workspace, transcript,
context, usage and sole execution owner. A control application receipt is not
a model task result or an input/output attribution claim. Current status is
live-owner scoped; broader post-run result association is follow-up work.

## Future admission and finite lifetime

Active and pending remote objectives retain their original binding. Every
later running admission checkpoint revalidates both through the same retained
canonical/runtime boundary. A self objective edit inherits this origin; the
target's self opt-in cannot silently turn revoked remote-directed work into
self-authorized work. Detach, scope removal, a stale incarnation, permission or
host-grant withdrawal, target replacement, and owner changes prevent further
admission. Existing target error/cancellation finalization may retire already
admitted work; external effects are not rolled back.

The existing hard ceilings remain 100 iterations and 1000 absolute parent
provider rounds, with stricter host and RunBudget limits, one generation per
activation, nonincreasing edits, no consumption reset, 64 bounded operation
receipts and the existing 128 KiB LoopState bound. The new typed consumption
projection is also bounded to 64 entries / 64 KiB in the existing checkpoint.
Stop is sticky and never restarts a generation or answers a human wait.

## Required deterministic acceptance before admission

- Actual ToolCtx identity, strict DTO, Plan/permission/disabled and typed/legacy
  replay denials; unchanged Followup and self-loop contracts.
- Real V2 Inbox/Repository/router: explicit host grant, target owner and attach;
  admission → configured checkpoint → real iteration proof; no provider
  translation, new execution, successor or context replacement.
- Bootstrap status; finite Start including zero consumed rounds; pending
  objective Update; immutable same-operation retries, changed payload and
  stale CAS; self/remote revision contention; sticky Stop and cancellation.
- Complete effective bounds when Update omits fields; no reset or extra round;
  malformed origins and expired owner/grant/lifetime fail closed.
- Retained-lock races for detach, Project/scope/permission/incarnation changes,
  host revoke/cancel after a guard is acquired, target recreation and owner
  finalization/replacement at publication and later objective admission.
- Root-owned and legacy control consumption; Main-before/after failure and
  ACK failure, dropped callers, canonical reload failure, partial/unknown
  observations and no later generic write of an unconfirmed proposal.
- A fake-provider public runner combination with controlled barriers, proving
  the Supervisor commands another live Root and observes its real target-side
  goal admission while provider concurrency remains one.

All new runtime acceptance above is currently **UNRUN**. Existing #898/#1992
green receipts do not satisfy these new gates. Exact source-bound focused
commands and original-CI lint flags accompany the static candidate handoff.

## Specific follow-up product slices (remain open)

1. **Pause/resume and terminal readback:** define a durable paused status,
   a distinct terminal read capability, remaining-budget policy,
   safe owner handoff and fresh authority for resumption. Pause must not cancel
   or answer a human wait; resume must not undo user Stop or reuse a dead grant.
2. **Progress and results:** reuse #2000 P3's input/run/output ownership. Cover
   multiple accepted inputs per run, successor-consumed inputs, partial writes,
   restart and eviction with explicit partial/unknown result coverage. Do not
   relabel these configuration receipts as execution outputs.
3. **Continue scheduling:** consume authorized completion/result evidence and
   explicitly decide the next delegated goal/Session using existing Followup
   and attach scope. Recheck permission at that next admission. The Supervisor
   remains the orchestrator rather than executing delegated work itself.
4. **Dormant activation, interval/sleep/future wakes:** separate #893/#896/#897
   lifecycle work, with bounded wakes and fresh scope/lifetime checks. This
   live-only implementation does not implement timers or dormant startup.
5. **Restart recovery / authority transfer:** require an explicit product and
   persistence contract before adopting any saved live/paused generation. No
   grant, owner or continuation is automatically restored in this slice.
