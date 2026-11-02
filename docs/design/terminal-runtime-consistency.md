# Terminal runtime consistency

Tracking: [#2448](https://github.com/bigduu/Bamboo-agent/issues/2448).
Storage is [#2454](https://github.com/bigduu/Bamboo-agent/issues/2454);
Host final-save is [#2460](https://github.com/bigduu/Bamboo-agent/issues/2460).
Receipt replay monotonicity, Root Host/recovery and Child notification remain the independent
[#2468](https://github.com/bigduu/Bamboo-agent/issues/2468) and
[#2470](https://github.com/bigduu/Bamboo-agent/issues/2470) and
[#2455](https://github.com/bigduu/Bamboo-agent/issues/2455) acceptance slices.

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
that wait or turn the Session into completed. A marked in-flight Host run retains its nonterminal projection until the existing final-save/receipt boundary. A typed terminal status alone never
authorizes finishing a live activation. Confirmed terminal projection retains
round/accounting fields and updates both the typed state and its legacy mirror.
Active tool counts become zero only with the terminal projection.
The Host binding port materializes runtime identity before sealing a new result. A legacy
sealed result that bound an absent runtime retains that absence; adding a new
identity afterward would invalidate its proof. It has no nonterminal typed state
to repair. Ordinary saves validate identity without rewriting sealed snapshots.
After a completed child wait explicitly opens the next input gate, `Idle` is a
valid later control state. Replaying the preceding terminal outcome preserves
that gate; it repairs stale `Initializing`/`Running` state instead.

## Identity and persistence

The `Storage::begin_host_run` port binds `runtime.host_run_id` to a reserved Run.
[Child Host/SDK entrypoints](https://github.com/bigduu/Bamboo-agent/issues/2460)
bind the actual reserved Run UUID before execution. Native Child backend
preparation must succeed before this binding; a preparation refusal leaves the
canonical Session unchanged. A refused binding or an explicit startup identity
conflict never falls through into legacy execution. Storage compares the prior Host identity and Session
incarnation under the physical Session lock, and refuses a different live Actor
owner. This dedicated transition preserves canonical content. Ordinary full and
runtime saves cannot change that Host identity. The captured child launch generation prevents an old activation from consuming a newer pending retry/reset/accumulate. The pipeline/Ticket runtime
`run_id` can be a stable Session identity, so it is insufficient for this check.
Copying a Session removes the Host Run identity.
Ordinary legacy writers with neither incoming nor persisted Host UUID retain
their existing contract until the Host integration binds a run. They receive no
new activation or physical-stop proof. The first binding and automatic orphan
notifications remain the explicit #2460/#2455 admission boundaries; this slice
does not retrofit identity by guessing from an unmarked legacy snapshot.

A legacy backend can claim its Actor during execution. At binding, Storage
captures the exact terminal predecessor fence so the new Host can save startup
before that claim. Ordinary saves retain the canonical captured fence. A new
Actor attempt invalidates it even before the next Host UUID is installed.
This observation grants no execution or physical-stop authority.

Host final-save synchronizes the typed state and legacy mirror before sealing
the result. Deferred broker publication retains a nonterminal projection until
the existing receipt proof commits. An explicit queued rerun consumes its old
typed question handoff along with the old question metadata; child/Bash waits,
plan approval and waits created by the current execution remain authoritative.
An unstarted cancelled or failed queued launch closes its exact generation
tombstone and retains the predecessor Run marker. It cannot borrow that Run's
sealed result or finish. Merge-save must not promote this terminal write to a
newer launch, including before the first Host UUID exists. Terminal generations
are stale to launch-intent and scheduler consumers.

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
The separate #2468 slice must reject replay of an older pending checkpoint
after a terminal source was already published, including after receipt ACK.
Host integration alone does not establish that same-run replay guarantee.

## Recovery boundary

[Startup integration and recovery notification fencing](https://github.com/bigduu/Bamboo-agent/issues/2455) are the next dependent slice. They must validate the recovered child at the actual parent wait commit under the shared root tree writer, preserve fresh/unexpired owners and waits, and avoid stale cache publication. This slice does not enable that automatic reconciliation.
Root entrypoint binding and Root final-save fault recovery are kept together in
#2470; enabling only its binding would suppress the existing unmarked terminal
projection before the replacement recovery path is ready. The Child Host slice
leaves that Root entrypoint unchanged.

`Storage::reconcile_child_runtime` also provides a targeted repair entry point.
Targeted repairs compare the exact control-plane, birth, current activation, Host Run, Inbox admission and launch generation under the same physical locks; pending broker delivery and new pending launches are excluded. These repairs update runtime/index state, preserve Main/transcript bytes and
respect the current Session contract. They run independently of Session-version
migration. Filesystem failures propagate; a failed repair does not publish a
synthetic completion. Settled repair is idempotent.
An old nonterminal record without an Actor or terminal Host fact remains unknown.
Expired logical authority cannot override a conflicting committed Host outcome;
recovery checks that conflict before any authority or runtime mutation.

## Separate continuation slice

[#1055](https://github.com/bigduu/Bamboo-agent/issues/1055) remains the independent
checkpoint/non-interrupting continuation protocol: busy targets use Inbox,
idle targets activate, restart retains pending input/checkpoint with source and
permission identity. Legacy resident reset/accumulate still cancels an active
Run before replacement. Its migration needs separate acceptance. Warm-worker
idle reclamation and pool capacity remain resource policy.
