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
error save cannot leave a phantom Running loop. Only explicit budget/round/cap
reasons map to BudgetExhausted. Ticket question yields and other non-budget
early exits retain their diagnostic and record recovery_unsupported; they do
not imply a completed iteration or grant another admission.
Persistence does not imply automatic recovery: a new activation
finding Running/Finalizing state records `recovery_unsupported` and refuses to
replay it, including on subsequent ordinary resume attempts. A suspended live
loop also records this status. The host must explicitly reconcile that state;
this slice provides no reconciliation or auto-resume API. It does not claim
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
