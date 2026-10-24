# Root idle reset witness: bounded implementation plan after C3b4

This is an implementation design based on source
`7a6fba6531509f60fe88ebdeb6649926e5664663`, together with the C3b4
scoped-generation patch. The APIs and states below are proposals, not installed
runtime capabilities. No activated-idle reset writer or public reset endpoint
exists in this freeze. C1 `/clear` and the pure C2 transform do not supply one.

## Evidence that must change

| Existing source | Observation and required seam |
| --- | --- |
| Engine `runtime/execution/runner_state.rs::AgentRunner` | Stores status, cancellation, event publication and run identity, but no retained actual execution JoinHandle or resource-drain receipt. Keep visible status semantics; add separately owned physical lifecycle evidence. |
| Engine `runtime/execution/agent_spawn.rs::spawn_session_execution_with_inputs` | Discards the outer `tokio::spawn` handle. `finalize_runner_exact` makes summaries idle before `root_actor.finish`, activation registration finalization, cache publication and completion callbacks finish. A registry badge cannot issue reset authority. Retain and join that exact outer task outside the task itself. |
| Engine `runtime/execution/runner_lifecycle.rs` | Raw reservation may replace a terminal runner. Every raw, registered and router reservation must honor the same reset admission reservation before replacing a slot. Entry points without the issuer remain unsupported for reset. |
| App `handlers/agent/execute/runtime/execution.rs` | Native and ordinary HTTP launch converge on the Engine spawn path. App assembly must provide one physical Root owner and shutdown join, rather than a second server-only proof. Resume and router-spawner paths must use it too. |
| Engine `session_activation.rs::ActiveOwner` | Logical registrations, finalizing state and scoped grants do not contain an actual task join. Reuse the target publication fence for short admission transitions; do not turn absence from this map into idleness. |
| Engine `external_agents/local_task.rs` | `Execution` retains its task; private `ConfirmedTaskStop` is issued after real join and owned resources drain. It is Child-specific and cannot be replayed as a Root reset receipt. Reuse its existing ownership, shutdown and failure discipline without modifying or borrowing the Child proof. |
| Tools `local_task_resources.rs` | Owned resource tasks are actually joined and admissions close before drain succeeds. The mechanism explicitly cannot cover arbitrary third-party spawned tasks. A Root owner needs the equivalent complete resource domain, with unsupported/untracked effects preventing a closure receipt. |
| Storage `v2/default_actor_context.rs` and `v2/scoped_loop_control.rs` | Physical default jobs retain lifecycle/Task/Session guards, then scoped retain takes the activation target publication mutex. Holding that publication mutex while awaiting Storage locks would reverse the existing order. |

`Complete`, successful final save, `is_running: false`, cancellation requested,
terminal Actor status, expired lease and missing/local-empty owner maps are all
insufficient. A panic, lost handoff, failed drain or unknown external effect
must leave reset unavailable. Joining a Tokio task alone does not prove an
untracked process or detached physical filesystem job has stopped.

## C4a: real Root physical lifecycle, without a reset writer

Give this slice one Engine owner and an isolated branch. Keep changes within
the existing reservation/spawn/finalization architecture and at most these
bounded seams: a new `runtime/execution/root_execution_lifetime.rs` module and
fixture module; execution module registration; `runner_state.rs`,
`runner_lifecycle.rs`, `agent_spawn.rs`; and App launch/assembly/shutdown
adapters. If resource binding or raw reservation admission exceeds that bound,
freeze retention/closure first and admit no idle reset capability until the
remaining entry points are fenced. Do not modify `external_agents/local_task.rs`,
ticket ownership or independent Child lifecycle in this slice.

The new Host-private owner keeps the exact actual outer JoinHandle, birth/run/
instance identity, original full conversation observation, handoff completion
and result of the Root resource domain's close-and-drain. A separate retained
coordinator joins the outer task; the task cannot join itself. It also joins or
accounts for execution-owned event/heartbeat/cleanup work and waits for owned
physical filesystem jobs. Do not merely move the visible terminal status to
claim this work is complete. No `is_finished()` shortcut or cancellation token
can mint closure.

Reuse the established weak binding and per-invocation resource acquisition:
tools do not retain a strong owner cycle or acquire resources after closure.
Host shutdown first closes admission, waits for in-flight launch handoff, joins
the retained coordinators/tasks, drains resources and preserves any failure.
An aborted HTTP future must not drop the only JoinHandle or shorten an already
admitted physical job's lifetime. A dropped/reset request is independent from
the Root execution owner.

A proposed private `ConfirmedRootExecutionClosed` value is non-deserializable,
issued only by that actual coordinator after every required join/drain, and
bound to exact Root birth/run/instance and original conversation observation.
It is not a caller-supplied boolean, timestamp, Actor record or a public
constructor on `AgentRunner`. Storage may validate it only through a
Host-installed issuer/read port, using the existing trusted composition pattern;
unknown/custom SDK runtimes get default Unsupported and cannot synthesize it
from serialized status. The issuer/port signature needs a separate source
freeze before sharing an API with frontend code.

Old closure evidence cannot authorize reset after a successor was reserved.
Keep a monotonic Host admission epoch and exact registered owner identity;
successor reservation invalidates prior closure evidence even if its caller
reuses a Session id. Do not bind stable permission/scope intent to conversation
generation: the old operation observation is frozen, while the retained scope
itself may survive a later reset.

Required source fixtures for this freeze use the actual Root spawn/reserve path:
pause after visible terminal status but before Actor finish, block a completion
callback and a physical write, then prove no closed receipt appears. Observe
the retained real JoinHandle and successful resource drain before issuing one.
Cover handoff cancellation, panic, drain failure, stale owner/successor, unknown
runtime, shutdown and request abort. Never create the receipt directly in a
fixture or infer it from an empty runner registry.

## C4b: short admission reservation, without lock inversion

All launch paths must consult one Host reset admission reservation. The proposed
sequence is:

1. Under the target's existing publication fence, check exact birth/Host epoch
   and supported owner state. A live/stopping/finalizing owner, unconfirmed
   handoff, or unknown runtime refuses; reset never automatically stops it.
   Install one operation-owned reset reservation and release the publication
   fence. Do not acquire Storage locks or wait for a task join while holding it.
2. Obtain/validate the exact already-closed coordinator evidence outside that
   fence. A reservation may wait for already-known cleanup only if that wait
   does not convert an active execution into an implicitly authorized stop.
   Any uncertainty leaves a recoverable refusal, not an idle assertion.
3. Acquire existing Storage lifecycle, Task, canonically ordered Session and
   Actor-tree boundaries in their existing order. Read canonical pairs/proofs,
   pending operations and Inbox. Compare the operation's original birth,
   ledger and full Session digest. No canonical read upgrades the observation.
4. At each final physical replacement, check the installed reservation and
   closed evidence using a nonblocking, immutable or atomic token check. It
   must not acquire the target publication mutex or await the Host states map
   while Storage boundaries are held. Every owned filesystem job retains both
   its physical guards and the reservation token until completion.
5. Release Storage boundaries before reacquiring the publication fence for
   final scheduling cleanup. An old completion/release must not clear a newer
   operation's reservation. Use exact operation/instance compare, not target id
   alone. Preserve queued work; new execution needs fresh canonical admission.

An admitted delivery/inbox enqueue must either participate in the same exact
reservation protocol or make the final full-snapshot/pending-input check fail.
It may not be ACKed or discarded to make reset pass. Legacy raw reservation,
custom tool or SDK launch paths that cannot check this reservation block reset
for that runtime. This is a prerequisite inventory, not a claim that C3b4
already fences every custom Host or mixed-runtime path.

## C4c: private transaction and recovery, after both prerequisites

Storage is the single writer for a new `v2/conversation_reset.rs` plus exact
reader/recovery hooks. Do not remove #1350 or call the old default clear path.
The private `IdleConversationResetWrite` is minted from canonical authority and
the supported Host reservation/closed evidence. It cannot be obtained from
`RootActorRuntimeWrite`, a new lease, a cold census, role name or deserialized
claim. Ordinary and Supervisor Roots use the same mechanism; their existing
identity and management proofs are checked and preserved.

Bind the transaction to the C2 request's original birth, full canonical snapshot
digest, full original ledger and epoch-prefixed operation id. Refuse active
Actor/Inbox ownership, unresolved approval/replay/question/Plan/Bash work,
unknown external effects, and retained Task/copy/reset journals before staging.
An unfinished independent Child/managed Root remains untouched; if its pending
handoff makes the target conversation unsafe, refuse instead of deleting it.

Stage exact new Main/runtime and any changed Root proof bytes plus their hashes.
The prepared transaction marker must be recognized by every operational reader
and final writer before any canonical file is replaced, including startup,
raw Actor and retained-operation paths. Operational access to a prepared or
partial pair refuses. Integrate with the existing cross-process recovery gate;
do not introduce a competing journal lock or treat a cache/index as authority.
An older runtime does not know a new marker name: the prepared marker alone
cannot establish mixed-runtime exclusion. Before admitting this writer, prove
the exact publication order under existing cross-process locks and the old
runtime's rejection of the versioned Main/Root proof at every readable partial
state. If that cannot be established, keep reset unsupported for shared or
mixed-runtime data rather than claiming the marker closes that gap.
Verify real-file, birth, witness and staged-byte ownership at every recovery
step. A hash-verified exact operation may roll forward once; contradictory or
unavailable data stays quarantined/unknown. Do not undo by restoring old
conversation bytes or retry under a new operation id.

Abort before prepared publication can remove only its own private staging after
proving no physical job remains. After prepared publication, request abort must
leave one recoverable exact operation. Startup recovery cannot invent a Root
closure receipt from local absence: the preceding owner's physical death or
closure must be established by the supported Host protocol; otherwise remain
blocked. Cross-process ownership and crashes require dedicated tests before
the recovery path can be admitted.

Only a committed, agreeing Main/runtime/proof pair with canonical readback may
publish reset success, cache replacement and a generation-tagged account event.
Unknown IO, committed-but-unreadable state or lost confirmation reports
`outcome_unknown` with the original operation id. A replay returns its immutable
receipt with fresh observation/refetch semantics; it never clears newer
messages. Preserve identity, Project/workspace, stable scopes, management
links, independent Roots, Tasks, run budgets and unfinished work. Reuse the C2
transform for conversation/native/model-context cleanup and its monotonic
epochs; no delete/recreate or implicit stop is allowed.

## API and acceptance gate

Freeze the actual transaction, operation-status and public history/detail/feed
generation contract only after source integration exists. Existing C1 200/409/
unknown behavior remains separate. Frontend local response fences do not prove
cross-client exclusion and must not claim activated reset has become available.
The original Lotus owner should connect only the frozen real API.

Required later tests include actual Root join/closure races, all admission
paths, staged-write faults and recovery, exact replay and argument conflicts,
cross-process/mixed-runtime refusal, and real server delayed-history generation
fences. Use an in-process capturing provider on the next actual runner request
after reset/reopen, including native PromptIR/Responses: cleared sentinels,
native System-anchored payload groups, old summary/compression/prompt snapshots,
hook context and previous-response links must be absent, while fresh authorized
Project/Task context and independent work remain. A transformed Session or fake
read-port payload is insufficient.

No code or tests for these C4 capabilities run in C3b4. Cargo/typechecking,
controlled actual provider payload, lifecycle/recovery and cross-client
acceptance are **UNRUN**. The next Engine and Storage owners must freeze these
bounded implementation slices separately before any ready/merge claim.
