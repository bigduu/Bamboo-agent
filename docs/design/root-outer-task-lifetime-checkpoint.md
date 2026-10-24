# Root outer-task ownership checkpoint (C4a)

This local bounded checkpoint stacks on C3b4
`680a402531b0f2a87cfbd4097fefa75c9413e46c`. It implements real outer
task retention and Host-local admission prerequisites. It supplies no reset
writer, HTTP/API capability, actual clear or physical resource closure issuer.
The reset admission token's `require_physical_closure` explicitly returns
Unsupported. Root history reset is not ready, including Supervisor history.

## Implemented source and ownership

The conversation_reset owner writes only Engine execution modules
`root_execution_lifetime.rs`, its dedicated test module, `mod.rs`,
`runner_state.rs`, `runner_lifecycle.rs`, `agent_spawn.rs`, and the explicitly
handed-off five-line retain guard in Server `app_state/init.rs`. No changes to
Session messaging/Host first-input, Domain, Storage, Child resources/tickets,
tool scope/permissions, or the #2144 resource lifecycle are included.

Common raw runner reservation increments a Host admission epoch before the
runner slot is replaced. Registered execution carries the same private
admission through its real reservation, including router activation handoff.
The epoch is process-local ownership, not persisted conversation revision.
The complete original conversation ledger is captured separately and cannot
be refreshed by a current lease, callback, new scope or canonical readback.

For a Root, the actual common spawn function attaches that exact admission,
retains its real outer JoinHandle in a coordinator and stores that coordinator
in the runner lineage. The coordinator holds a weak lineage reference and
records join only after `task.await`. The producer's tail marker alone is not
join. The observation waits past final save, visible runner terminalization,
Actor renewal/finish, registration finalization, cache publication and callback
return. `RootActorExecution::finish_observed` additionally preserves renewal
join/Actor finish errors; existing cleanup callers retain their unit-returning
wrapper. A caught provider-loop panic is still not resource quiescence, and an
outer panic or unfinished startup/tail poisons the join observation.

Observations are `Unknown`, `Pending`, `Unconfirmed`, or
`JoinedResourcesUnavailable { actor_finished }`. None is an idle grant. Missing
legacy handoffs, Child/SDK/direct execution without this owner, lost lineage,
unknown physical effects and unconfirmed Actor closure cannot mint a reset
writer. There is no migration from terminal/expired/missing state to a receipt.

The private `close_and_join` closes admission and actually awaits retained join
coordinators in an owned job. A cancelled waiter does not drop those handles.
It sends no cancellation to a Root, Bash process, independent task or Child.
The caller must release runner, publication and Storage locks before awaiting.
App-wide shutdown assembly is still a later integration; this method alone
does not stop or close all Host resources.

## Reset admission reservation and current limits

After a real supported outer join, the installed runner can reserve one exact
Host epoch and operation UUID. Snapshot clones are observation-only and cannot
reserve. The future Host adapter must borrow the actual installed runner under
its registry guard while reserving. The token retains original birth, Project
and full ledger; a same-revision receipt change still refuses. It contains no
Role, permission, stop flag or deserializable idle assertion.

Common raw/registered entry, preparation's existing checks and shared runner
removal deny an already reserved slot. Idle maintenance keeps it. Holding the
token through `Arc` lets a physical job outlive its HTTP waiter without opening
new admission. Only the last token holder clears its exact nonce. The final
token check uses atomics, without the activation publication mutex, Host states
map, or Storage locks.

One unique runner-slot owner is transferred on ordinary replacement. Cloned
runners do not own that slot. Arbitrary slot deletion atomically invalidates
old tokens; **this invalidation alone does not prevent cross-slot admission**.
The shared remove and maintenance guards are separately required, and custom
code which removes/replaces maps outside them remains unsupported for reset.
Destructive dev reset and deletion still require the eventual Storage lifecycle
protocol; this checkpoint does not change those operations or run them.

No actual reset producer can consume this token yet. The activation router's
logical registration/preparation races, alternate custom/SDK admissions,
cross-process ownership and canonical pending/Actor checks must join the C4b
short publication reservation before a real idle writer exists. Early reserved
checks do not by themselves establish that full handshake. Keep the order:
short target publication transition, release it, then acquire Storage guards;
the final job must not acquire publication while holding Storage boundaries.

## Minimal next Root resource issuer: exact seams

The next owner must implement this as a separate bounded slice, reusing #2144
instead of wrapping Roots in Child close-and-drain. No file below was edited
in C4a.

| Required source seam | Existing behavior and smallest supported change |
| --- | --- |
| Tools `local_task_resources.rs` | The current `close_and_drain` closes admission, sends stop and aborts cancellation-safe helpers. Add a separate quiescence-only operation on the same registry, with a private Root owner identity/issuer. It must actually poll/join ready retained handles, return Busy with state unchanged if any producer remains pending, preserve poison/failures, and seal admission atomically only when all tasks have joined. It must never send stop, abort a helper, kill a process or manufacture a drain receipt. |
| Engine `runtime/runner/loop_execution/startup.rs` | The current scope validation explicitly requires `SessionKind::Child` and uses `child_birth`. Add a separate trusted Root resource binding, exact birth/run/Host instance and original ledger; preserve the existing Child guard. A boolean or reusing a Child receipt is insufficient. |
| Tools `executor.rs`; Engine `runtime/runner/tool_execution.rs`, `per_call.rs` | Only provenance-checked framework Write/Edit/Bash/exec_command invocations use the existing owned path when a scope exists. Register all Root invocations that can retain physical work; preserve per-invocation acquisition and weak bindings. Unknown custom/MCP/plugin tools or interrupted inline invocations deny the exhaustive issuer until their own real completion contract exists. Matching a tool name is not provenance. |
| Tools `tools/bash_runtime.rs`, `exec_command.rs`, Bash input/output | A background process, pumps, completion supervisor, retained pull handle and future stdin/callback can outlive the outer Agent. The existing shell/status lists are observations, not physical receipts. Root ownership must retain their actual completion tasks and exact Session birth/run, allow natural completion, and reject reset while any required producer/pending capture remains. Do not remove the shell or consume its result to pass reset. |
| Engine `runtime/hooks/local_resources.rs`; loop `pipeline.rs`, `gold.rs`; output compressor `tee.rs` | Known hook commands, auxiliary helpers and filesystem cleanup already use the task-local registry when present; otherwise work may detach. Thread the explicit Root scope through supported producers and reject unknown/custom hook effects. Ready-join only; no cancellation of independent work. |
| App execute `runtime/events.rs`; Engine `execution/event_forwarder.rs` | The forwarder is a separate actual task; a history-barrier receipt is not its join. Retain that handle or an exact closed publication/resource receipt in the Root owner. Parent owns this overlap with first-input assembly; coordinate before edits. |
| Host assembly/shutdown | Keep one registered Root issuer, weak/per-invocation tool acquisition, closed admission and retained shutdown joins. A custom direct SDK or old/mixed runtime with no issuer remains Unsupported. Do not infer physical death from local absence after restart. |

Safe quiescence sequence: hold the operation's short Root admission reservation,
observe canonical pending/independent-work handoffs, then ask the complete Root
resource registry to try joining all ready tasks. If any registered task is
pending, return Busy and release the request's reservation; its background work
continues naturally. Do not seal a partly active registry, since its normal
completion may still need to register cleanup. Once all actual producers have
joined, seal under the same registry lock so no late acquisition can enter.
Only then can a non-deserializable Root resource receipt combine with the real
outer join and canonical authority to issue `IdleConversationResetWrite`.

This gives a concrete supported path for newly bound, provenance-covered Root
runtimes. Known legacy/unknown SDK/custom runtimes need an explicit fenced Host
quiescence/recovery transition before they can participate; do not retrofit a
missing proof from empty maps or silently discard a prior failure. Independent
Roots/Children, managed Tasks and explicitly handed-off background work retain
their identities and lifetimes. An unresolved handoff blocks the target reset.

## Synthetic source and validation

Six new test functions cover real JoinHandle ownership; original ledger/birth/
Project and same-revision receipt comparison; raw admission/shared remove
refusal while a retained token survives its request owner; snapshot versus
unique-slot ownership; panic, Actor failure, unobserved or incomplete tail;
cancelled shutdown waiter and untouched independent work; and the actual Root
spawn/Actor binding path paused after the terminal badge but before Actor finish.
The last fixture uses a fresh V2 store and no provider/tools, then observes real
outer join and real Actor finish, still refusing physical closure. Other
fixtures use synthetic private tail observations and do not issue a write grant.
Maintenance's new deny-only consumer is a static boundary check here, not a
claimed complete App cleanup runtime test.

Direct rustfmt/diff and an exact-source independent review are the allowed
checks. All six new tests, Cargo in every mode, typechecking, mixed-runtime,
actual Root resource/physical-write races, provider payload and cross-client
acceptance remain **UNRUN** under the parent window restriction. This is a local
checkpoint for the single integration owner, with no new Draft or merge/Ready
claim. Actual resource issuer, C4b handshake, private transaction/recovery and
public generation/API/readback/feed integration remain required.
