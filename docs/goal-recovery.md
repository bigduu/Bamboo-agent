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

Counters persist across reloads and are scoped to the goal. A resumed goal respects any remaining recorded backoff. Recovery does not start an idle session after a process restart; an explicit resume retains the existing retry budget. Terminal goals, corrupt recovery records and elapsed recovery windows do not receive extra retries. No account routing, provider-specific message matching, or unrestricted watchdog is introduced.
