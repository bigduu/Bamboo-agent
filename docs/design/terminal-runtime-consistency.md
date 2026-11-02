# Terminal runtime consistency

Tracking: [#2448](https://github.com/bigduu/Bamboo-agent/issues/2448).
Storage projection is [#2454](https://github.com/bigduu/Bamboo-agent/issues/2454);
Child final-save and terminal consumers are [#2460](https://github.com/bigduu/Bamboo-agent/issues/2460).
Exact Child admission is [#2481](https://github.com/bigduu/Bamboo-agent/issues/2481),
receipt replay [#2468](https://github.com/bigduu/Bamboo-agent/issues/2468),
Root outcome/recovery [#2470](https://github.com/bigduu/Bamboo-agent/issues/2470),
and Child startup/parent proof [#2455](https://github.com/bigduu/Bamboo-agent/issues/2455).

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
[Child Host/SDK entrypoints](https://github.com/bigduu/Bamboo-agent/issues/2481)
bind the actual reserved Run UUID before execution. Native backend preparation
must succeed first; a preparation refusal preserves canonical Session state.
A binding conflict never falls through into legacy execution. Storage compares
the prior Host identity and Session
incarnation under the physical Session lock, and refuses a different live Actor
owner. This dedicated transition preserves canonical content. Ordinary full and
runtime saves cannot change that Host identity. The captured child launch generation prevents an old activation from consuming a newer pending retry/reset/accumulate. The pipeline/Ticket runtime
`run_id` can be a stable Session identity, so it is insufficient for this check.
Copying a Session removes the Host Run identity.
Ordinary legacy writers with neither incoming nor persisted Host UUID retain
their existing contract until the Host integration binds a run. They receive no
new activation or physical-stop proof. Automatic orphan notification remains
#2455; unmarked legacy snapshots do not acquire inferred execution authority.

A legacy backend can claim its Actor during execution. Binding records its exact
terminal predecessor fence; a new Actor invalidates startup writes even before
another Host UUID is installed. An expired Child owner is revoked under those
same locks only without a current pending/published/unknown completion source.
Its own authority and runtime failure commit in one retained filesystem job
before the successor binds; renewal first makes binding refuse. This is logical
revocation, not proof of a stopped Worker. Root expiry is not handled this way.

An owned pre-execution closure retains the reserved UUID, original birth/input,
launch generation and event publication fence. If a bound Child cannot save
startup, cleanup conditionally commits Failed and cache under physical guards;
caller cancellation retains that cleanup before runner/event retirement. A live
Actor, changed input, a new completion source or successor invalidates it. Native
cleanup continues through its existing safe-stop/unknown-quarantine boundary.
An unstarted cancelled/failed queued launch closes its own generation tombstone
while retaining its predecessor marker; it cannot borrow that Run's sealed
result. Terminal merge writers never adopt a successor's launch generation.

`finish_activation` validates the full activation fence. It commits authority
before projecting the terminal read model while retaining the same lifecycle,
Task, Session and tree locks. Repeating the identical fence/outcome retries an
interrupted projection without changing the original finish time or authority
revision. A different outcome or successor attempt rejects the old finish.
Final-save and recovery cannot overwrite a later Host Run, even when both
pipeline runtimes use the same Session ID and omit `execution_id`.

Child SDK and resumed Child final-save synchronize typed state and the legacy
mirror before sealing the terminal source. Genuine typed waits are honored even
without a legacy suspend-reason string. An explicit queued rerun consumes its old
question/approval handoff just as the existing metadata contract does; independently
owned child/Bash/plan waits and waits created by the current run remain intact.
Native safe prelaunch cleanup synchronizes its owned failure before publishing
the cache. Unknown physical cleanup remains quarantined by its existing owner.

Strict broker delivery keeps its existing pending source/receipt boundary.
Pending publication is not repaired into an exposed completion. Committed
receipt publication synchronizes typed status in its existing exact-snapshot CAS.
The SDK temporarily withholds terminal typed state with the pending source,
then restores the intended typed state and mirror for the existing proof commit.
For the same Run, a published terminal source cannot return to the older
pending checkpoint, including after receipt ACK and Store reopen. The normal
full/runtime writer rejects that replay under its existing physical locks;
the first pending stage remains nonterminal. A new Run still requires its own
identity transition and receipt rather than borrowing its predecessor's proof.
Duplicate source publication repeats the guarded full commit. Runtime or Main
bytes readable after an uncertain rename are not a durable acknowledgement:
the retry must complete the actual file and parent-directory syncs and repair
a Main left behind the leading sidecar. Receipt/anchor checks and the complete
expected Session CAS remain in force, including on an identical retry.
Queued launch consumers treat completed/cancelled/error as stale; only
running/suspended means started.

## Recovery boundary

[Startup integration and recovery notification fencing](https://github.com/bigduu/Bamboo-agent/issues/2455) are the next dependent slice. They must validate the recovered child at the actual parent wait commit under the shared root tree writer, preserve fresh/unexpired owners and waits, and avoid stale cache publication. This slice does not enable that automatic reconciliation. Root claims with
original reset identity are #2482; owner-attributed Root outcome recovery is
#2470. Child admission does not enable Root Host UUID binding.

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
