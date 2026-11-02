# Terminal runtime consistency

Tracking: [#2448](https://github.com/bigduu/Bamboo-agent/issues/2448).

Session owns identity and context. An ActorActivation owns one fenced attempt.
A Worker supplies reclaimable execution capacity. Neither an absent local runner
nor a cancel acknowledgement proves that a Worker process stopped.

## Trusted outcomes

| Trusted outcome | `last_run_status` | typed runtime status |
| --- | --- | --- |
| Activation succeeded / Host completed | `completed` | `Completed` |
| Activation cancelled / Host cancelled | `cancelled` | `Cancelled` |
| Activation failed / Host error or timeout | `error` / `timeout` | `Failed` |
| Suspended, children/Bash wait, suspension, pending question, plan approval | retain the wait | retain the nonterminal control plane |

Finishing an activation can hand execution to a durable wait. It must not clear
that wait or turn the Session into completed. A typed terminal status alone never
authorizes finishing a live activation. Confirmed terminal projection retains
round/accounting fields and updates both the typed state and its legacy mirror.
Active tool counts become zero only with the terminal projection.

## Identity and persistence

Host startup binds `runtime.host_run_id` through `Storage::begin_host_run` after
reserving the actual Run. Storage compares the prior Host identity and Session
incarnation under the physical Session lock, and refuses a different live Actor
owner. This dedicated transition preserves canonical content. Ordinary full and
runtime saves cannot change that Host identity. The pipeline/Ticket runtime
`run_id` can be a stable Session identity, so it is insufficient for this check.
Copying a Session removes the Host Run identity.

`finish_activation` validates the full activation fence. It commits authority
before projecting the terminal read model while retaining the same lifecycle,
Task, Session and tree locks. Repeating the identical fence/outcome retries an
interrupted projection without changing the original finish time or authority
revision. A different outcome or successor attempt rejects the old finish.
Final-save and recovery cannot overwrite a later Host Run, even when both
pipeline runtimes use the same Session ID and omit `execution_id`.

Strict broker delivery keeps its existing pending source/receipt boundary.
Pending publication is not repaired into an exposed completion. Committed
receipt publication synchronizes typed status in its existing exact-snapshot CAS.

## Recovery boundary

The child-wait watchdog scans existing terminal records as well as running,
initializing and pending candidates. Its fixed boot cutoff excludes fresh Runs;
old candidates whose leases remain live are revisited by the watchdog. Storage
rechecks the exact control-plane snapshot, incarnation, Inbox admission, Host Run
and current activation while holding its physical locks. It never initializes a
missing Actor authority record during repair.

An already-terminal current activation supplies its original result. An expired
live activation can be failed logically; physical stop remains unconfirmed. An
unexpired activation, pending receipt or durable wait is left to its owner.
Legacy records without a Host UUID are repaired only against the currently
validated canonical pair/activation, under the same locks and snapshot CAS. A
newer known Host UUID prevents the old activation from projecting over that Run.

`Storage::reconcile_child_runtime` also provides a targeted repair entry point.
These repairs update runtime/index state, preserve Main/transcript bytes and
respect the current Session contract. They run independently of Session-version
migration. Filesystem failures propagate; a failed repair does not publish a
synthetic completion. Settled repair is idempotent.

## Separate continuation slice

[#1055](https://github.com/bigduu/Bamboo-agent/issues/1055) remains the independent
checkpoint/non-interrupting continuation protocol: busy targets use Inbox,
idle targets activate, restart retains pending input/checkpoint with source and
permission identity. Legacy resident reset/accumulate still cancels an active
Run before replacement. Its migration needs separate acceptance. Warm-worker
idle reclamation and pool capacity remain resource policy.
