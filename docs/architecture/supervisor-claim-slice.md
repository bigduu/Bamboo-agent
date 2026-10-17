# Supervisor claim selection: initial #2042 slice

This slice is a trusted-host library primitive, not an enabled background worker
or completion of #2042. It reuses TicketService's existing durable Start,
DispatchIntent, operation receipt and restart fencing. It adds no lease store,
broker, polling loop, provider call or restored permission grant.

## Host integration contract

1. Persist Work through TicketService before scheduling it.
2. Read the published snapshot and retain its commit, sequence and authority epoch.
3. Verify current Project association/context revision, current permissions,
   remaining cumulative Work budget, and resource availability. Supply only known
   eligible Work to `prepare_ready_claim`. An absent/unknown budget or permission
   is not permission to run. This API deliberately does not deserialize eligibility
   assertions from model/worker input.
4. Supply at most 64 distinct candidates and the host's scope-wide capacity ceiling.
   The deterministic choice is oldest `updated_seq`, with Work ID as tie breaker.
   This ordering is not the later foreground/background aging policy.
5. Submit the returned exact Command to `execute`. The whole-scope seq/epoch CAS
   fences both selection and capacity: competing claims for different Work cannot
   both consume the last slot from the same snapshot. Any conflict requires a new
   read and fresh eligibility checks; do not just refresh the command's CAS fields.
6. Keep/recover the original command for ACK-loss retry. Successful execution stores
   its canonical request and operation receipt. Re-executing it returns that same
   receipt, Assignment and stable dispatch key. Reusing its logical operation ID
   for a newly selected Work is rejected.
7. Runtime admission must still validate fresh host authorization. Reconcile an
   unknown dispatch against its existing key/receipt rather than starting another.

Capacity counts assignments that still hold resources, including pending dispatch,
uncertain outcome and unconfirmed cancellation/pause. A timeout, heartbeat, or
request to pause never frees a slot. The existing Runtime stop/effect reconciliation
protocol releases it. Restart fences active assignments as OutcomeUnknown and
retains their reservations. A fourth Work remains persisted and Ready when three
slots are occupied; selection does not mutate or discard it.

## Deliberately remaining work

The host loop is not wired to this primitive yet. #2041 owns Project/context
persistence and Start validation. #2042's subsequent slices must supply host
eligibility and cumulative-budget policy, durable wake/reconcile/backoff state,
foreground/background fairness, runtime lease-owner reconciliation and controlled
provider acceptance. This slice does not claim exact-once arbitrary side effects,
real-provider execution, durable queue-reason projection, or Supervisor evidence
acceptance. Submitted remains distinct from Accepted through the existing service.

## Local deterministic coverage

`cargo test -p bamboo-tickets --locked` includes `supervisor_claims` tests for:

- competing consumers claiming the same Work and different Work for a last slot;
- lost claim ACK replay before/after store restart, preserving one intent/key;
- three pending dispatches and a fourth persisted Ready Work;
- stale host snapshot rejection, unaccepted dependencies and idle selection;
- pause retaining capacity until a trusted stop/effect reconciliation ACK;
- rejection of Runtime claim authority, duplicate inputs and oversized batches.

The existing full crate suite additionally exercises file publication fault
boundaries, resource fencing, admission receipts and submission/acceptance rules.
These are library tests; they do not stand in for controlled-provider loop tests.
