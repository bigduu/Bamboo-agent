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

This slice provides no `expected_run_id` compare-and-steer or cancellation API.
Cancel accepted never means stopped: exact-run stop and Bamboo's durable terminal
validation keep their existing contracts. Supervisor SSE/output remains session
scoped; no input-exclusive output attribution is inferred. These are the later
P2 and P3 acceptance phases in #2000.
