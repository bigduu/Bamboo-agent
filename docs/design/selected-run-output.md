# Selected Native run output snapshots

This bounded #1595 / #1953 slice follows the real receipt/run binding in PR1952.
A compatible activation must observe output from the run it actually started.
The shared Session SSE and message WS follow Session generations, so using them
as a request result could join a successor. Full durable event replay is not a
prerequisite for a first bounded adapter. The existing runner-owned
`VisibleMessageStream` and publication fence already provide the neutral core.

## Read contract

After a selected Native execute returns its activation receipt, use:

```http
GET /api/v1/sessions/{session_id}/runs/{run_id}/output
Bamboo-Session-Created-At: {existing Root birth used for execute}
```

The GET uses the existing route authentication and supervisor-owner gate, checks
the canonical ordinary Root lifetime, matches the exact retained runner and its
Host-captured output identity, and returns `Cache-Control: no-store`.
The existing per-session persistence lock covers owner validation, Root birth
and the retained-run snapshot together, in the same lock order as delete/create.
A waiting read validates the replacement Root after its mutation completes;
retained old bytes cannot authorize a read across Root recreation or a new
Supervisor identity.
The expected birth header must occur once and parse as RFC3339. Missing/invalid
birth is 400; wrong Root lifetime/kind is 409. Missing/replaced/unretained run,
evicted buffer, absent Root or process restart is 410. Storage verification
failure is 503. No branch selects a current successor, session transcript,
previous ACK, checkpoint or account journal as fallback output.

The 200 JSON is a complete replacement snapshot:

```json
{
  "session_id": "root",
  "run_id": "actual run from the activation receipt",
  "session_created_at": "canonical expected Root birth",
  "input": {"message_id": "accepted selected input", "ingress_seq": 7},
  "output_scope": "visible_messages",
  "runner_status": "completed",
  "output": {
    "version": 4,
    "messages": [{"id": "assistant id", "content": "visible text", "created_at": "timestamp"}],
    "terminal": "complete",
    "history_committed": true
  }
}
```

Version belongs only to this exact run. Repeated reads replace the previous
snapshot; clients must not append the whole returned text on every poll. Message
IDs, explicit discard/retry replacement, empty filtering and raw visible text
come from the existing producer. Reasoning, tool arguments/results, diagnostics,
source text and full AgentEvents are not added to this message-only interface.
It is neither a vendor completion response nor an event cursor/replay log.

`runner_status` reads the existing exact AgentRunner status, with no new state
or raw diagnostic string. It can acknowledge cancellation before producer
cleanup ends. Visible `version` does not version this registry observation.
Native currently represents cancellation with a generic Error terminal; the
runner status disambiguates it without changing the old event contract.

`terminal` is the last actual visible-stream disposition (`complete`, `error`,
`cancelled` or null). It does not prove history was saved. `history_committed`
is true only after the original fenced history barrier; any later text/start or
actual discard makes that snapshot uncommitted again. Error/cancellation can
retain a visible prefix without inventing successful completion or persistence.
A future vendor adapter must define its own completion/usage/error conversion.

## Same buffer, existing lifetime

Only a selected execution carrying the actual Root writer and bound receipt may
opt in before auxiliary title work and the existing forwarder/spawn.
All fallible/cancellable output binding finishes before the title task is
launched, so aborted Ready startup cannot trigger auxiliary model work. Identity is copied
from that capability and exact runner, never supplied as authority by HTTP.
Ordinary reservations are a no-op. Existing event producers, physical/Actor
fences, history publication/ACK, cancellation, permissions and Ready handoff
remain the owners of execution and output publication.

Selected output retains the same visible-message vector after history commit.
A private per-message committed marker hides that prefix from the original
message WS snapshot, preserving its existing reconciliation/clearing behavior.
Later segments remain distinct; reusing an already committed ID replaces its
value instead of appending the old bytes. There is no parallel buffer, event
store, grant/index, lifecycle machine or independent TTL.

Retention follows the existing AgentRunner entry. Normal terminal idle cleanup
has its existing 300-second grace and receiver/producer conditions; replacement
can end lookup earlier, even within that grace. The snapshot endpoint does not
pin runners or reset that idle clock. Old response receipts in the process-local
idempotency cache may therefore outlive output lookup. Eviction, replacement and
restart return explicit unavailability rather than reconstructing an answer.
A read is a point-in-time snapshot; it does not promise retention after return.

## Acceptance and remaining work

Real HTTP/fake-provider tests cover current input/run identity, live and late
post-history reads, exact retries, an actual successor, sibling and reasoning
isolation, cancellation/error prefixes, missing/nonselected runs, trusted Root
recreation with deliberately retained old runner bytes, eviction and restart.
A concurrent recreation fixture verifies the read waits for the existing Host
lock, then rejects a new ordinary birth or unauthenticated Supervisor access.
A real reserved-token cancellation fixture also verifies that aborting output
binding starts neither title nor primary provider work. Shared-core tests
preserve default WS clear-on-history and cover binding,
post-barrier segments, reused IDs, explicit discard and commit invalidation.
Keep the prior Native/Skills/current-input/cancel/idempotency/CORS regressions,
Root publication fences, message WS, and actual OpenAI Chat/Responses, Anthropic
and Gemini converter boundary checks.

This closes the finite exact-run visible-output Host gap. Compatible HTTP
activation adapters, vendor completion/usage/stream mapping, isolated full event
streams, durable restart replay and configured-provider/full-platform acceptance
remain open. No vendor endpoint is activated here. No paid model or deployment.

Actor-mode Root Chat returns the exact durable Inbox receipt from its existing
`commit_selected_input` transaction in the existing `message_id`/`ingress_seq`
response fields. The successor proof uses the actual Root Chat POST response,
then selected HTTP execute and exact-run output; direct Inbox acceptance cannot
substitute for that entry boundary. This receipt remains input identity, not a
writer, a reservation or an execution promise. A legitimate consumer can already
have admitted it before execute. Original activation policy, detached commit,
owner/lifetime/lock, cancellation, bounded process-local idempotency and output
retention contracts remain unchanged; disconnects do not create durable response
replay or exactly-once HTTP submission guarantees.
