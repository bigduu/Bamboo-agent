# Session message delivery intent

This contract adds opt-in scheduling to `session_message` send/reply over the
existing durable SessionInbox and SessionActivationRouter. It does not create
another runtime, broker, task record or permission system. Task is a shared task
list; SubAgent executes delegated work. Tracking issue: #2000; related #1997 / #1968.

## Explicit scheduling

```json
{
  "action": "send",
  "target_session_id": "logical-session-id",
  "operation_id": "one-stable-operation",
  "message": "Additional findings for your next safe point.",
  "delivery_intent": "queue_only"
}
```

| Intent | Active receiver | Idle or finalizing receiver |
| --- | --- | --- |
| `queue_only` | Notify its current owner; admit at an existing safe point | Keep durable input; do not start a run or request a successor |
| `trigger_turn` | Notify/coalesce through the existing router | Permit the existing router to activate a turn, subject to current waits and authority |
| Omitted | Preserve historical peer steering behavior | Preserve historical peer activation behavior |

`reply` accepts the same optional field. Its recipient and thread are derived
from the canonical received peer message; arguments cannot supply sender or
relationship authority. Both explicit intents respect human, children, Bash and
other specific waits. They do not answer a question, approve a tool, clear a wait,
change a Child task, stop a run, or broaden permissions.

QueueOnly is available to the current consumer even though activation scans
exclude it. It can also be consumed during a later independently started turn.
A trigger authorizes its own delivery, not earlier staged child/Bash outcomes.
An unrelated coordinator prefix or steering grant cannot upgrade an explicit
peer delivery's wait policy or turn QueueOnly into a scheduling trigger.

## Active-run steer

`action: "steer"` requires `target_session_id`, `expected_run_id`, `operation_id`
and `message`. It is an active-run-only QueueOnly delivery. A new operation
must match the current registered, non-finalizing owner at the actual durable
publication boundary, including the canonical Session birth. Missing, replaced,
finalizing, lifetime-unbound or unsupported owners return a structured conflict
with `accepted: false`. It never cancels or starts a run.

The expected run is retained in versioned v3 intent and its semantic digest.
Reuse the exact operation and arguments after an uncertain result. An authorized
existing receipt replays without signalling any owner, including after ACK or
run replacement; changing the expected run under the same operation conflicts.
Accepted does not prove context consumption or run completion. Unconsumed input
remains durable and restricted to that run; restart or a successor cannot
silently retarget it. It cannot schedule a successor by itself.

The actual started filesystem job retains the original peer authority and exact
run through publication and notification even after caller cancellation. The
existing generation reservation/format preparation may survive a rejected late
publication, as with other cancelled admission; it is not a message receipt.
The existing strict zero-tool Child cold-continuation boundary is unchanged.

## Receipt boundaries

The tool retains `admitted`, `message_id`, `inbox_generation`, `activation` and
`activation_error` for compatibility, and adds:

- `accepted: true`: the exact message is durably in the Inbox (or was already
  acknowledged on an exact retry). It does not prove model context consumption.
- `activation: "not_requested"`: QueueOnly requested no turn, or an exact explicit
  retry no longer has pending delivery. Other activation values report routing
  disposition, not a terminal result.
- `active_delivery_notified`: QueueOnly signalled a currently active owner. This
  in-memory observation is not durable processing or completion evidence.
- `terminal: "not_observed"`: this delivery receipt does not observe run terminal
  evidence. It carries no inferred run ID or input-exclusive output association.

An activation failure can coexist with accepted input. Reuse the same
`operation_id`, content, recipient/reply and intent after uncertainty; do not
invent a second logical message. Changing intent under an existing key fails
before activation. Intent is persisted with the delivery and bound into its
semantic digest after ACK. Existing birth/operation identity checks still apply.
Explicit intent uses versioned records that older readers reject safely.
The Inbox observes pending state under its admission locks. An acknowledged
explicit retry cannot activate or notify a new owner; ACK is a transport fact,
not proof of model/run completion. Explicit per-message promotion applies the
same policy before and after ACK.

## Receiver capability boundary

TriggerTurn grants scheduling permission, subject to the existing receiver route;
it does not certify that every Child can resume every backlog shape. The strict
owned zero-tool Actor Child continuation currently admits one fresh pending
input. A queued input plus a later followup exceeds that existing cold-continuation
shape. This slice does not widen its admission, lease or completion protocol;
that route's multi-input continuation remains P1 follow-up acceptance in #2000.
Other supported consumers can admit queued input at their existing safe points.

## Compatibility and later phases

`SubAgent.send_message` is unchanged: `auto_run` still defaults to true; idle
`auto_run=false` still writes a Child draft. A draft is not QueueOnly. Existing
legacy tools and callers keep their old behavior when the new field is absent.

Steer adds no cancellation API.
Cancel accepted never means stopped: exact-run stop and Bamboo's durable terminal
validation keep their existing contracts. Supervisor SSE/output remains session
scoped; no input-exclusive output attribution is inferred. Input/output attribution remains the later
P3 acceptance phase in #2000; P2 acceptance also requires its documented regressions.
