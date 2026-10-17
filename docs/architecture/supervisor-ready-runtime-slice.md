# Explicit native Ready Work runtime slice

Part of #2042, workspace/failure isolation in #2046, and the first bounded crate
extraction in #2050, after #2044 and #2045. Semantic patrol remains in #2049.
This is a trusted Rust Host embedding port, not an HTTP auto-run switch. Normal
server startup installs no scheduling policy. No release branch is changed.

Policy, typed observations, failure accounting and the finite reconciler live in
the independent `bamboo-orchestration` crate. It depends on Domain/Tickets and
Tokio through a `ReadyWorkHost` trait; it has no server, engine, provider or memory
adapter dependency and creates no new durable authority. Server supplies only
current authorization, preflight and the existing dispatch adapter. Existing
placement scheduling and user Cron services retain their separate responsibilities.

`TicketApplication::install_ready_work_policy` accepts one Project, 1–64 explicit
Work grants, a scope capacity of 1–3, a finite round ceiling, and a lifetime up to
one hour. Each grant binds its exact contract revision, immutable Project context
hash and isolated `ExecutionWorkspace`. Only initial generation-zero Ready Work
can receive a new Assignment. A policy is process-local and must be explicitly
installed again after revocation, expiry or Host restart; it cannot be recovered
from Ticket records, prompt text or Jiandu summaries.

The real AppState starts one managed Tokio reconciler after binding its existing
Child adapter. Successful Host Work updates notify it. A five-second interval
repairs missed notifications and runtime-side changes; missed timer ticks are
skipped and event wakes are coalesced with a 100ms minimum spacing. Each pass
reconciles at most one existing pending dispatch or commits one new Start.
Shutdown interrupts an in-flight admission wait and joins the reconciler; an
already committed intent remains in the authoritative Ticket ledger.

## Admission and recovery

Every pass revalidates the canonical Supervisor binding and epoch, feature flags,
current Project resource/binding metadata, exact context and contract, local
worktree identity, scope capacity, dependencies and resource ownership. A
malformed or missing workspace is omitted before Start and retains an explicit
blocked diagnostic. Other eligible grants can still proceed. No fallback calls
Supervisor execution, installs more tools, chooses a broker or selects a process
or remote backend. Existing #1986 tool-catalog/final-execution enforcement and
#1490 finite delegation failure protection remain authoritative.

Selection reuses `prepare_ready_claim` and its whole-scope sequence/epoch CAS.
Dispatch reuses `work_dispatch`, immutable DispatchIntent, canonical Child
identity, Inbox admission and RuntimeReceipt. A pending retry queries/ensures
the same key; an unknown outcome is quarantined, never turned into a new Start.
Unconfirmed stop and unknown effects continue to retain resources. Submission
remains testing/Submitted until the existing explicit acceptance protocol runs.

`ReadyReconcile` is the program inspection seam: disabled, idle, capacity blocked,
typed blocked diagnostic, or committed/enqueued dispatch identity. `Dispatched`
does not claim the provider is running. Configuration/permission/unknown-outcome
failures stop automatic retries for that policy. Transient admission failures
receive at most three attempts including the initial post-commit failure.
Resource waits are rechecked within the policy lifetime. Reinstalling a policy
does not reset Ticket generation, reclaim resources, or authorize another run.

The actual assembled backend must advertise native Ticket-budget support. Mixed
process/remote routers conservatively reject this automatic path; their existing
explicit operations are unchanged. The Host cap is persisted in the canonical
Child's `TicketDispatchBinding.max_rounds` and carried into the native
`ExecuteRequest` as a tighten-only `run_budget` override. A pending Child cannot
lose its cap when global config is later relaxed or when the same key is retried.
The existing round limit permits one final summary turn. Rounds are neither a
precise token allowance nor a financial budget.
An exhausted Host-limited Ticket follows the existing budget-error delivery
path after its owned task and resources stop. Its Assignment becomes failed and
the Work blocked; a summary or more tool calls cannot masquerade as a completed
Submission. The same logical run must finish delivery before activation is released.

## Boundaries still open

This slice does not implement cross-run cumulative Work budgets, background aging
or foreground fairness, durable next-check/backoff/queue diagnostics, automatic
workspace repair or replacement workers, dormant grant recovery, safe preemption,
or semantic/model-based patrol. There is no automatic continuation/retry from
generation one to a new Assignment. Provider retries and user-authorized input
continuations retain their existing protocols; no claim of cumulative accounting
across those paths is made. Complete #2042 and #2046 remain open.

Future low-cost inspection can consume bounded incremental observations and
escalate exceptions through a separate policy. This reconciler makes zero LLM
calls to poll an empty or blocked queue. No inspection model is selected or called.
Execution handoff continues to reference Work/Assignment/run/generation, immutable
context and canonical checkpoint/receipt identities. Jiandu #90 may hold derived
experience references; it cannot confer permissions or replace those facts.

## Verification

`ready_work_scheduler` is a standalone integration binary with an injected,
in-process controlled provider and explicit temporary Jiandu storage. It exercises
real AppState/Child/Task/submission, deduplication, capacity, stale metadata,
missing-workspace isolation, existing Supervisor tool authority, immutable queued
round ceilings, and virtual-clock policy expiry/revocation/restart. No paid
provider, user data, broker, deployment or release acceptance is required.
