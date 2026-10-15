# Scoped Supervisor Loop control preparation (#1976)

## Delivery boundary

This is a design and `cfg(test)` prototype, not an enabled control surface.
It depends on unmerged #1992, exactly
`3ca496b0107b8e8f6eaa4a3e2b3e43fafd619793`. That candidate remains frozen and
has delivery priority after the Nova cut. Its test receipts do not validate
this new source. This preparation is excluded from the current Nova release.

The first production slice should permit a real Supervisor to read, start,
update and stop a finite loop in an already active, explicitly authorized
Ordinary Root. #1976 stays open. Dormant startup, successor activation,
pause/resume, interval changes, future wakes and restart recovery require
separate lifecycle services; #1992 does not provide them.

## Proposed command adapter

Keep `loop_control` implicitly self-only. Extend the exact existing
`session_control` management tool at a later implementation step, rather than
adding a target field to the self tool or widening an orchestration whitelist.
The proposed action is a strict wrapper around the existing command DTO:

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

The prototype parses the wrapper payload without the outer `action`, because
the existing `SessionControlArgs` adapter is deliberately unchanged. It reuses
`parse_loop_control_request` for command validation. Unknown fields and caller,
Project, owner or capability claims in model JSON are refused. No model-facing
schema or registered action changes in this preparation.

Existing Supervisor `list`/`get_meta` responses do not expose a model-readable
link revision, and `inspect_link` is a host service. The future `status` action
must therefore allow `expected_link_revision` to be omitted for bootstrap. It
may reveal the current revision only after fresh canonical checks of the
original identity, Project scope, enabled link, target lifetime, separate host
grant, allowed action, permission and exact active owner all succeed. The grant
must still bind the exact current link revision. A supplied expected revision
must be nonzero and match exactly; `start`, `update` and `stop` always require it
and cannot obtain authority by guessing. Omission for `status` grants no write
capability and does not skip any later fresh boundary check.

This bootstrap is a production wiring requirement. The current test-only
prototype observes synthetic snapshots; it adds no actual status-reading tool,
model-facing schema or public action.

All actions remain serial and nonpromotable. Future `status` classification can
be read-only, but it must not override existing Plan denial, disabled-tool,
permission or exact-identity gates. Approval replay is not new command authority.

## Two independent host authorizations

An attached relationship is necessary and insufficient. The target's #1992
self-loop opt-in and finite ceilings are also insufficient for remote control.
A separate, explicit host grant must bind the original Supervisor incarnation,
target id/birth/Project, enabled link revision, current execution id and allowed
operations. Grant bounds cannot exceed the target's existing opt-in ceilings.
No model command creates, broadens or renews any of these grants.

Capture the initiator from the original trusted `ToolCtx`, never a replacement
Session loaded during delivery or approval replay. Standard and Ultra thinking
postures have the same management semantics only for this strict Supervisor
identity. Ordinary Roots, ordinary Ultra Roots, reserved-id impostors and
children do not acquire it. The target remains a Standard Ordinary Root as
required by the #1992 dependency; this preparation grants no new target posture.

Canonical authority must be checked while retaining the existing lifecycle →
Task → sorted Session lock set at both admission and the target commit boundary.
The check includes current incarnation, current Project scope, enabled link and
revision, target birth and metadata version, explicit grant, permission and the
exact active owner. A returned observation or this prototype's preflight is not
a retained grant. Do not hold `SupervisorFollowupGuard` and then call a public
checkpoint method that reacquires those locks. Use the existing integrated
transaction seam and preserve its lock order through the linearization point.

The live command is bound to the admitted target execution and loop generation.
Ending that owner, recreating the target or changing the generation expires the
unapplied command. Never call generic `request_activation` as a substitute: it
can reserve a successor when no owner exists. A human wait is preserved; this
first slice may reject/defer mutations there, and never answers it.

An applied remote objective and its pending/active objective revision must also
retain the original initiator, link, host-grant and target-lifetime binding.
Before every later provider-round or iteration admission for that remote-directed
work, recheck the same canonical authority through the admission linearization
point. Do not retain storage locks across a model call. Revocation or failed
revalidation forbids further admission for that remote objective and records an
explicit reason. The target's existing self opt-in cannot silently convert it
to self-authorized work. Preserve already-admitted work boundaries and do not
roll back external effects. Exact terminal status, explicit authority transfer
and recovery semantics remain later design decisions, not defaults added here.

## Target owner and persistence ordering

The target retains its context, provider/model, workspace, permissions,
transcript, usage ledger and sole execution owner. The control is structured
host input, not instructions to ask the target model to call a tool.
`supervisor_followup` admission/activation is not proof of a Loop change.

On frozen 3ca, `pipeline.rs` calls counted `before_round` before
`round_prelude.rs` drains ordinary inbox messages. The new typed control drain
must precede counted iteration admission, objective injection and budget charge,
under that same target owner. Do not sniff JSON after provider-message
translation. Preserve ordinary input and human-wait ordering. Cover both the
Root-owned `admit_root_inbox` path and the legacy fallback.

Extract an internal shared state-transition proposal from #1992 when wiring the
production path. Preserve its private ownership fences. Do not make
`apply_current` an arbitrary cross-Session API or fabricate a target ToolCall /
ToolResult to reuse its self-tool checkpoint acknowledgement.

LoopState, immutable operation outcome and typed consumption proof must commit
together before ACK. Before-write or uncertain-commit failure retains truthful
failure/unknown status and forbids another provider admission. Retry must not
reapply an already committed mutation. Independent user cancellation and
revoked authority take priority over acknowledgement and the next provider call.
Already performed tool effects are not rolled back.

## Versions, receipts and finite bounds

Self and Supervisor commands contend on the same generation/control revision.
Keep state/checkpoint revision separate from control and objective revisions.
The old iteration retains its admitted objective; an accepted update becomes
effective only at the next iteration boundary. The old completion cannot
overwrite a newer objective or completion state.

The shared owner's dedupe must bind original initiator, exact target lifetime,
operation UUID and typed payload. An authorized identical retry returns the
immutable prior receipt before ordinary CAS validation; changed payloads
conflict. A different initiator must not borrow that receipt by choosing the same
UUID. This origin binding is an explicit production integration requirement;
the prototype implements no receipt store and does not claim it solved.

All #1992 finite bounds still apply: at most 100 iterations, at most 1000 absolute
parent-provider rounds for the activation, stricter existing RunBudget,
nonincreasing edits, no consumption reset, one generation per activation,
bounded receipts and the 128 KiB checkpoint bound. Stop is sticky. No request
clears user stop, grants extra rounds, restarts a generation or creates an
unbounded continuation. No timer, second executor, queue/store, broker, machine
routing or automatic attachment is added.

For an `update` that omits a bound, the future shared owner must resolve the
effective proposal from the current target state. It must check that complete
proposal against the remote grant, target self-loop ceilings, existing RunBudget
and already consumed iterations/rounds together, at the retained commit boundary.
This prototype checks only explicitly supplied Start/Update bounds against the
grant's upper bounds; it does not resolve omitted fields or validate the full
effective proposal. Those checks remain part of the shared owner's existing
transition path, without copying the local executor here.

## Preparation acceptance and remaining gates

The `cfg(test)` prototype uses synthetic authority snapshots and deterministic
owner boundaries. It parses the real command DTO and captures the real original
Supervisor observation. It checks separate grants, target/link lifetime,
explicit Start/Update bound fields, owner/generation changes, Plan/replay/permission denial,
human waits and user-stop priority. It is read-only and returns only
`RequiresRetainedCommit`, never durable `Accepted`, `Applied` or completion.
CAS fields and typed field values are preserved for the future shared owner
service, including UUID spelling and whitespace within objective text. Original
JSON bytes, key order and formatting are not preserved. The prototype is not an
alternative implementation of that service.

Production completion additionally requires the actual retained authority port,
typed Inbox admission, pre-counted owner drain, shared state transition,
source-bound idempotency, same-checkpoint ACK, real public tool/host wiring and
fault/race tests through both storage paths. These remain unimplemented here.
There are no real-model or paid-model tests. New checks must identify this new
commit/tree and disclose unrun integration gates.

## Source hotspots (frozen dependency)

| Path | Reason |
| --- | --- |
| `crates/app/bamboo-server-tools/src/session_control.rs` | Future exact action/schema/permission adapter; currently only Followup. |
| `crates/core/bamboo-agent-core/src/tools/context.rs` | Original executing Supervisor observation; no model-provided identity. |
| `crates/engine/bamboo-engine/src/session_app/supervisor.rs` | Canonical host scope/link seam; observation is not authority for a later write. |
| `crates/infra/bamboo-storage/src/v2/supervisor_management.rs` | Retained incarnation/link/Project/birth/metadata lock set. |
| `crates/core/bamboo-domain/src/session/inbox.rs` | Future explicit typed-control payload; current kinds are provider messages. |
| `crates/infra/bamboo-storage/src/session_inbox.rs` | Durable admission/idempotency under authority; no second protocol. |
| `crates/engine/bamboo-engine/src/runtime/runner/loop_execution/pipeline.rs` | Control must precede counted `before_round`. |
| `crates/engine/bamboo-engine/src/runtime/runner/round_prelude.rs` | Ordinary inbox drain currently occurs later. |
| `crates/engine/bamboo-engine/src/session_repository.rs` | Root-owned and legacy consumption paths both need coverage. |
| `crates/infra/bamboo-storage/src/v2/root_actor_input.rs` | Existing atomic checkpoint → ACK integration seam. |
| `crates/engine/bamboo-engine/src/runtime/loop_control.rs` | Shared local semantics; keep transport acknowledgement and ownership distinct. |
| `crates/engine/bamboo-engine/src/session_activation.rs` | Existing owner fences; generic activation must not launch a successor for this live-only slice. |

This preparation changes only this document, one `cfg(test)` module declaration
and its contract prototype. No production hotspot above is rewired.
