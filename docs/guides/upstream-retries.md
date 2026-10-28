# Initial upstream request retries

Bamboo retries establishment of the initial model HTTP response. Once a successful
response is returned, its stream is consumed once. This policy never restarts an
agent turn, re-appends a user message or re-executes a tool. A timeout before
headers means that the upstream outcome is unknown; it does not prove the POST
was unreceived.

## Finite automatic default

The default is six total HTTP attempts (one initial request and five retries).
Anthropic shares a 120-second total budget across its compatibility fallbacks;
each headers attempt has a 20-second deadline. This doubles the previous
60-second allowance while keeping ordinary interactive execution finite. The
count remains a ceiling: elapsed time or a long server hint may stop it sooner.
Other built-in providers using the shared HTTP driver apply finite headers and per-send time limits. Copilot model POSTs use the same policy while model discovery retains its existing transport contract. Successful streams retain their independent stream watchdog.

Operator environment settings are read once at first use, so changing them
requires a later normal process start. This feature does not edit live settings.

| Setting | Default | Meaning |
| --- | --- | --- |
| `BAMBOO_LLM_MAX_RETRIES` | `5` | Retries after the initial attempt; `0` means one attempt. Anthropic no longer silently clamps this setting to four total attempts. |
| `BAMBOO_LLM_RETRY_TOTAL_BUDGET_MS` | `120000` | Managed initial-request budget, shared across Anthropic fallbacks and per send for other providers; positive values up to 3600000 ms. |
| `BAMBOO_LLM_RETRY_ATTEMPT_TIMEOUT_MS` | `20000` | Initial headers deadline; positive values up to 60000 ms. |
| `BAMBOO_LLM_RETRY_BASE_DELAY_MS` | `500` | Exponential backoff base. |
| `BAMBOO_LLM_RETRY_MAX_DELAY_MS` | `30000` | Cap on computed backoff, with jitter. |

Invalid new duration settings retain finite defaults. Retry-After supports
seconds and HTTP dates. It is a minimum wait and is never shortened to the
computed backoff cap or to fit a remaining total budget. Finite mode stops when
the required delay cannot fit; explicit continuous mode waits cancellably.

## Explicit per-execution continuous mode

First negotiate `GET /api/v1/execute/defaults`. The additive `upstream_retry`
object advertises `supported_modes: ["bounded", "until_cancelled"]`,
`default_mode: "bounded"`, `max_attempts`, `total_budget_ms`, and
`attempt_timeout_ms`. Clients must not infer support from a version string or
send an unnegotiated mode to an older server that could ignore unknown fields.

Continue the existing pending session using
`POST /api/v1/sessions/{session_id}/execute` (legacy alias:
`POST /api/v1/execute/{session_id}`):

```json
{"upstream_retry":{"mode":"until_cancelled"}}
```

Omit the field, or send `{"upstream_retry":{"mode":"bounded"}}`, to retain
finite automatic retries. Unknown modes fail request decoding. The option is
carried through the existing execution reservation into main-model bootstrap;
it is not written to Session metadata and does not propagate to auxiliary calls,
children, later executions or restart. Do not send another Chat request to
implement the button.

Use the existing `POST /api/v1/sessions/{session_id}/stop` or legacy
`POST /api/v1/stop/{session_id}`. For an exact stop, send `Bamboo-Run-Id` with the
opaque `run_id` returned by Execute; the diagnostic receipt's `run_id` is not a
replacement. A malformed header returns 400. A stale, replaced or completed run
returns 409 and never cancels a newer run. Repeating an exact stop succeeds while
that cancelled runner remains current in the registry; removal or replacement
returns 409. Headerless Stop retains its legacy current-owner behavior.
Cancellation drops both sends and backoff immediately. Repeated Execute while an
owner is running keeps the existing already-running semantics; it neither
starts another retry loop nor upgrades that owner's mode.

There is no per-retry AgentEvent/SSE progress event or next-attempt timestamp in
this contract. Clients can show a cancellable waiting state. The content-free
retry diagnostics are evidence, not a live attempt counter or countdown.

Only transient network/connect failures, headers timeouts, 429 and
500/502/503/504 are retried. Ordinary 401, 403, parameter errors and other terminal
statuses stop. The existing exact structured connector-database-recovery 401
exception remains narrow; it is not a general authentication retry rule.

A managed headers policy owns its initial deadline. On successful establishment,
the independent first-semantic/transport/semantic stream watchdog starts, and a
stream failure remains terminal rather than replaying successful output or tools.
Custom providers using public `send_provider_request` opt into the managed headers contract. The legacy public `send_with_retry` helper remains finite and does not inherit continuous mode; those custom providers retain the owner's headers timeout. Their retry receipt still forbids resetting a long server hint through an engine turn replay.

SDK callers can use `agent.execute_with_upstream_retry(session, request, UpstreamRetry::UntilCancelled)`, importing the enum from `bamboo_sdk`. Existing requests, builders and execute entrypoints retain their bounded contract.

Continuous mode does not override Work's one-physical-send-per-Host-reservation
rule, run/token/tool budgets, approvals or permissions. A Work retry must still
return through its authoritative Host reservation path. Do not promise unlimited
requests across those guardrails. The separate finite diagnostic budget is
unchanged.

For the negotiated v1 live-bootstrap continuation, a run admits at most one
offer. Concurrent confirmations of that exact offer share its committed
result; they never create another run or replay tools. If confirmation is still
unresolved after a bounded wait, or the run expires, stops or changes, the
client receives an uncertain result and should follow the original run. A
later bootstrap in the same run cannot consume a delayed confirmation for the
earlier offer. Multiple separately confirmable offers would require a protocol
that identifies each offer. The Host caps outstanding registered offer
operations at 32 and tracks at most 1024 run markers; capacity exhaustion
leaves the bounded request failure intact and does not create a continuation.

## Diagnostic compatibility

Finite receipts preserve the existing error text and numeric limits. They now
accept the configured count and up to the configured one-hour time budget.
Continuous cancellation/classification receipts have
`retry_mode: "until_cancelled"` and null `attempt_limit`, `total_budget_ms`, and
`remaining_budget_ms`; they never claim a finite retry limit was exhausted.
Allowlisted fields, bounded receipt size and exact session/error binding remain.

## Validation ownership

Policy and controlled HTTP/SSE cases belong to the existing bamboo-llm library
catalog; real execute, tool non-replay and watchdog consumers belong to the
existing engine/server library catalogs. No additional workflow or test producer
is introduced. Use synthetic content and local mock endpoints only. A prepared
or formatted test is not evidence that it executed.
