# Bounded goal timeout recovery

Goal recovery is disabled by default. A session's `gold_config` can enable it:

```json
{
  "enabled": true,
  "auto_continue_enabled": true,
  "goal": "Finish and verify the requested change",
  "recovery": {
    "max_attempts": 3,
    "max_elapsed_seconds": 900
  }
}
```

Lotus Next exposes these choices in the goal editor. Recovery requires an active goal, no pending completion declaration, no input/child/Bash suspension, available run budget, and coordinated persistence.

After the ordinary three turn attempts, only structured replay-safe stream timeouts can reserve an extra attempt. A timeout after any semantic output or a partially emitted tool call is ineligible. Formatted provider error strings never qualify for these extra attempts.

A reservation records its counter, goal identity, start time and next attempt time in session metadata before waiting. Extra attempts use exponential backoff from five seconds up to sixty seconds. The policy caps attempts at ten and the recovery window at one hour even if a larger value is supplied. The run's accumulated token/tool/subagent limits still apply; recovery does not replenish them. The current run retains ownership, so no second execution is launched. Stop interrupts both ordinary retry waits and recovery waits.

Counters persist across reloads and are scoped to the goal. A resumed goal respects any remaining recorded backoff. A timeout retry reservation alone does not start an idle session after a process restart; an explicit resume retains the existing retry budget. Terminal goals, corrupt recovery records and elapsed recovery windows do not receive extra retries. No account routing, provider-specific message matching, or unrestricted watchdog is introduced.

## Completion and restart

A terminal Gold review can confirm achievement even after the continuation
allowance is spent. An unconfirmed goal keeps its review history and stops
with a reason such as `continuation_budget`, `no_progress`, or
`verification_unavailable`. A model's `update_goal` declaration is evidence
for review and does not determine the final status.

A session can explicitly set `gold_config.resume_after_restart` to `true`
to allow one startup continuation of that Goal. This applies to an ordinary
locally owned Root when Bamboo saved the continuation and had yet
to admit its next model round. The running owner withdraws the checkpoint
before starting that round. Startup verifies the Session lifetime, the Goal,
the last committed message, and the expired Actor owner before taking the
checkpoint. Pending waits, newer input, a separate Loop, an execution-specific
budget or deadline, and process loss after the next round starts require
ordinary recovery.

Use `/goal resume` to explicitly continue a stopped or interrupted Goal
with the same saved history and continuation count. Resolve outstanding waits
and verify any tool call with an unknown result before resuming.
Chat clients can submit the command through
`POST /api/v1/sessions/{session_id}/goal/resume`. Bamboo starts that run under
its Root owner as part of accepting the request.
