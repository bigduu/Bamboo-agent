# Bounded counted Session loop (#898)

This engine primitive repeats the current admitted User prompt in one live
Session. It reuses the existing activation owner, runner, provider/context,
tool policy, cancellation and Session checkpoint. It creates no Session, timer,
worker, queue or scheduler. A caller must use its existing registered execution
owner; `Agent::execute_direct` obtains that owner when the shared activation
router is installed. Its consumed registration privately binds the exact router,
Session lifetime and run to that request. Earlier config/stop clones acquire no
ownership. Raw execution rejects this opt-in feature even while another caller's
run owns the same Session; the mere presence of an owner grants no execution.

`ExecuteRequestBuilder::counted_loop(CountedLoopConfig::new(k, rounds))` opts in.
The same config type is exported by `bamboo_engine`, `bamboo_sdk` (also under
`agent`), and `bamboo_agent::agent` / `bamboo_agent::CountedLoopConfig`;
facade callers need no direct engine dependency to construct the request.
Both bounds are mandatory: 1–100 iterations, at most 1,000 total runner rounds,
and an initial prompt of 1–16,384 UTF-8 bytes with non-whitespace content. The
effective round bound is the minimum of the request and existing host run
budget. It must permit at least k one-round iterations. Existing token, tool
and subagent limits still apply and cannot be raised by the request. A counted
loop receives no extra summary round beyond those bounds. Provider retry and
tool timeouts retain their existing finite policies; rounds are not a promise
of a hard monetary or elapsed-time cap.

The first iteration uses the already admitted User. Each subsequent iteration
is admitted serially only after the previous no-tool terminal, keeping the
accumulated transcript. The next ordinary runtime User is appended at the next
round boundary, after checking stop and remaining budget. It carries generation,
objective revision and iteration metadata and grants no new tool/skill authority.
The counted gate emits no intermediate `Complete`; ordinary finalization emits
one completion event for a successful live execution. Counted publication waits
until all post-round hooks/finalizers and the original generation's terminal
checkpoint succeed. A failed completion checkpoint records failure for the
host's later error save; it cannot publish success first. The event carries this
run's cumulative actual token usage, including retries and final-evidence work.

Clones of `CountedLoopConfig` share an idempotent `stop()` signal. Stop drains
the current admitted round/tool batch and prevents any later round or iteration;
it does not undo external effects or inject a new prompt. Session cancellation
retains the normal cancellation/error path and does not fabricate a `Complete`.
Until a completion has been published, cancellation wins concurrent stop/budget
early exits, including cancellation during their checkpoint awaits.
No counted-termination model judge is called. Enabled Gold plus counted mode is
rejected before either executes: objective binding and combination semantics
belong to #897. Existing Guardian/final-evidence/Stop-hook gates run only after
the counted target is reached, subject to the same total-round cap. A Guardian
or human/hook suspension retains its existing wait and stops counted execution.

`runtime.counted_loop` holds the versioned `LoopState` through normal Session
persistence. A generation has an immutable objective revision of 1, an execution
owner, monotonically increasing state revision, iteration admissions/completions,
conservative round consumption, and explicit status. Each admission is
checkpointed before dispatch. Checkpoint failure and malformed/unknown state
fail closed. A failed initial checkpoint marks that exact generation Failed,
including in memory if its failure checkpoint also fails, so the host's later
error save cannot leave a phantom Running loop or an Initializing runtime:
the exact runtime run/round identity is captured before that first save, and
only that execution's unfinished/success snapshot is corrected on failure.
The new round identity is also mirrored before a fallible prelude, so an
admission followed by pre-dispatch cancellation retains the same failure fence. Only explicit budget/round/cap
reasons map to BudgetExhausted. Ticket question yields and other non-budget
early exits retain their diagnostic and record recovery_unsupported; they do
not imply a completed iteration or grant another admission.
Persistence does not imply automatic recovery. Before preparation, tool replay,
hooks or generic final saves, an existing direct/spawn registration checks the
presented Session against fresh durable birth and complete LoopState (including
generation/revision). A mismatch only refuses; it cannot erase newer state even
when the clone predates the first loop. A raw caller has no ownership and only
gets a read-only refusal. Registrations never replace the existing Actor writer:
repair uses the already-bound execution persistence, and the native Child route
refuses before backend preparation supplies that writer.

For a matched interrupted Running/Finalizing generation, a proven replacement
owner stages from the fresh canonical Session, records `recovery_unsupported`,
and marks only the corresponding unfinished runtime Failed in one checkpoint.
The optional typed `AgentRuntimeState.execution_id` records the actual pipeline
UUID before the first startup mirror/checkpoint; ordinary new runtimes omit it.
It matches LoopState.owner_execution_id without changing the stable run_id or
the sealed SDK execution UUID. Legacy snapshots may match the complete round ID
only when the typed execution ID is absent. Missing first-round identity,
foreign runtimes, Suspended status and existing waits are preserved. The rejected
caller clone's unadmitted input, older permissions or waits are never saved.
All refusal paths return before normal execution finalizers; they never advance
the inbox's executed watermark. Subsequent ordinary attempts also refuse.
Repeated Users are staged and published into the live Session only after
their admission checkpoint succeeds. A failed checkpoint retains conservative
consumption and the original failure. If the write committed before returning
an error, append-safe history may retain the generated User as a fact. That
exact unfinished generation/objective-revision/iteration token cannot become a
new external admission, regardless of the terminal reason. System resume and
other counted messages do not clear it. Registered entry checks both the live
and fresh canonical history; a pending canonical token requires explicit host
reconciliation or an already-persisted genuine new User. This slice does not
add a typed-preappend recovery grant. Public raw execution also reads fresh
canonical state for unfinished terminal snapshots before any hooks or generic
error save; an absent/stale durable snapshot refuses. This covers a committed
repeat followed by failing finish and host saves, even when the returned live
clone has no repeat. Both admission checks use the durable admission snapshot
port, bypassing SessionRepository's ordinary runtime cache while retaining
bound-reader checks and leaving cache publication alone. Custom cached
persisters must supply that durable read or fail closed. This read does not
mint a writer or lease. Older completed
iteration tokens remain ordinary history. The host must explicitly reconcile ambiguous state; this
slice provides no reconciliation or auto-resume API. It does not claim
exactly-once external effects across crashes or independent processes.

#896 owns start/status/update/pause/resume/stop application APIs, revisions and
idempotency for concurrent management requests. #893 owns pacing and any future
recovery protocol; #1976 owns authorized attached-Session Supervisor control.
This slice adds no HTTP/model tool, global gate, automatic attach, Workflow
unlock, production deployment or publication.

Deterministic tests use synthetic providers, temporary storage, controlled
barriers and existing activation leases. They cover exact repetition/context,
one terminal event, persisted admissions/terminal state, stop and cancel races,
round limits, invalid bounds/state, recovery refusal, owner collision, and final
Guardian suspension. No real model or broker is needed.

## Final outcome and failure matrix

One generation-fenced `finish` match resolves the execution Result. Stopped,
BudgetExhausted and Completed may have been written at an earlier boundary;
they cannot hide a later execution error. The existing cancellation-error
precedence is retained. Every resolved outcome, including an unchanged status,
checkpoints the final LoopState **and** Runtime mirror before Complete can be
published. A checkpoint error prevents publication and preserves the original
execution error when one already exists. Counters never roll back. This is a
live-owner boundary, not a restart/reconciliation API.

The table is the shared contract for outcome, persistence and admission tests.
`derive` retains existing suspension, completion-reason and target checks;
`preserve` retains the current status and still checkpoints the final mirror.
Cancellation in the table means the returned `AgentError::Cancelled`, rather
than replacing an unrelated original error merely because a signal also fired.

| State / fault boundary | Successful Result | Error Result | Cancellation Result | Persistence / subsequent admission | Deterministic coverage |
| --- | --- | --- | --- | --- | --- |
| Running | derive | Failed | Cancelled | Same generation; conservative counters; final mirror checkpoint | `counted_finish_outcome_matrix_preserves_counters_and_generation_fence` and live-loop tests |
| Finalizing | derive | Failed | Cancelled | No Complete before the final snapshot succeeds | Same outcome matrix; terminal publication barrier |
| Stopped | preserve | Failed | Cancelled | An earlier stop checkpoint cannot hide a later hook/finalizer error | Outcome matrix; stopped hook barrier |
| BudgetExhausted | preserve | Failed | Cancelled | The final Runtime mirror is saved even when the budget status is unchanged | Outcome matrix; bounded checkpoint matrix |
| Completed | preserve | Failed | Cancelled | A failed write/publication never leaves live success for the host save | Outcome matrix; completion checkpoint tests |
| Cancelled | preserve | preserve | Cancelled | Existing cancellation semantics and wait/identity fences remain | Outcome matrix; cancellation races |
| Failed | preserve | preserve | Cancelled | No new admission is implied by terminal status | Outcome matrix; pending-repeat refusal |
| RecoveryUnsupported | preserve | preserve | Cancelled | Ordinary entry still refuses this recovery fence; waits are not cleared | Outcome matrix; recovery and suspension matrices |
| Foreign generation / malformed state | refuse | refuse | refuse | No LoopState write across the generation fence; no Complete | Outcome matrix; invalid-terminal-state tests |
| Initial checkpoint fails, including failed failure-save | — | Failed | As above | Zero provider/Complete; later host save cannot persist phantom Running/Initializing | `counted_initial_checkpoint_failure_cannot_be_resaved_as_live` |
| Third repeat checkpoint fails before write | — | Failed | As above | Provider=2, Complete=0, counters=3/2/3; no iteration-3 User; completed iteration-2 token is history | `counted_repeat_checkpoint_failure_stages_input_and_fences_ambiguous_writes` |
| Repeat write commits then errors, including failed finish/host saves | — | Failed in live memory | As above | Durable state may remain at its last successful checkpoint; exact pending token / stale canonical snapshot refuses raw and registered re-entry without another provider call or error-save | Same repeat fault test; raw read matrix; SDK replay refusal |
| Stopped / Budget / Completed checkpoint fails before or after write | — | Failed | As above | Both early exit and final Runtime-mirror boundaries; original error, one provider, zero Complete, later host save retains both failures | `counted_bounded_checkpoint_failure_matrix_never_publishes_success` (10 controlled faults) |
| Stop wins during provider; AfterRound hook then aborts | — | Failed | As above | Hook first observes durable Stopped; returned/live/durable Runtime and Loop both Failed; no extra dispatch or Complete | `counted_stopped_after_round_failure_persists_both_failure_snapshots` |
| Pending token followed by system wake / another counted User / unsaved external input | refuse reuse | — | — | Only a genuine already-canonical new User clears the exact unfinished token; typed preappend is not recovery authority | `counted_pending_repeat_requires_canonical_external_input`; SDK replay refusal |
| Repository cache is stale or hides a newer token/generation | read durable | read durable | read durable | Admission read bypasses cache without publishing rejected data; canonical new User works | `counted_admission_bypasses_stale_repository_cache_without_publishing` |
| Missing / failed / stale durable read; stale birth/revision | refuse | refuse | refuse | Read-only failure before hooks/final save; no cached clone substitutes for canonical state | `counted_raw_unfinished_terminal_read_fails_closed_without_writing`; stale registered-clone matrix |
| Current / missing / expired / foreign lease, or missing Actor writer | existing proof only | refuse invalid proof | existing cancellation | Registration never mints writer authority; repair stages canonical data and fences only the matching runtime; foreign/Suspended waits remain | `counted_recovery_requires_real_current_owner_and_existing_actor_writer`; recovery identity matrix |

These tests share synthetic providers, real temporary SessionStore/Repository
adapters and controlled barriers. They do not claim crash-safe exactly-once
external effects or implement objective edits, automatic recovery, or a new gate.
