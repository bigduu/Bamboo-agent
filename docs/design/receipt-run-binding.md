# Native receipt bound execution

This slice under #1595 connects the exact owned claim from #1949 / PR1950 to a
real Native ordinary Root execution. It supplies the neutral Host seam needed by
future OpenAI Chat/Responses, Anthropic and Gemini activation adapters. Existing
compatible forwarding, conversion and streaming routes retain their behavior.

## Opt-in contract

Call either existing Native execute alias with all four headers:

```http
POST /api/v1/sessions/{session_id}/execute
Bamboo-Input-Id: {message_id from Native chat}
Bamboo-Ingress-Seq: {ingress_seq from Native chat}
Bamboo-Session-Created-At: {created_at of the existing Root, known before submission}
Idempotency-Key: {stable key for this exact request}
Content-Type: application/json

{}
```

All selector headers must occur once. The expected Root birth must match the canonical Session before reservation; deletion/recreation with reused input IDs and generations is rejected. Obtain the existing Root birth from its Session creation/read before submitting chat. This bounded opt-in does not cover an unknown newly-created Root birth. The generation must be a positive u64;
the ID uses the existing canonical parser and HTTP header text restrictions.
Ordinary execute without either selector header keeps its existing contract.
This does not alter the public `ExecuteRequest` or `SessionExecutionArgs` layout.

The handler reserves the real runner and binds its Root Actor before installing
the selector in that reservation's execution-private persistence. No existing
owner is joined. The existing owned Root consumer matches ID and generation in
the lease transaction, checkpoints the canonical typed User and ACKs it under
the actual Actor writer. Only a newly committed ordinary User and the existing
fresh startup observation can proceed to Ready. Stored transcript, previous ACK,
recovered checkpoint or request metadata cannot reconstruct current invocation.

The same private persistence moves through original Ready and spawn, including
its normal permission, Source and Skills producer. It retains the selector at
every provider boundary, so a later round cannot claim another pending input.
Unselected staged/eligible inputs remain for their legitimate owner. This mode
does not migrate a legacy pending source or promote its queue prefix. Specific
waits and queued/legacy ingress are excluded from this bounded opt-in.

A selected reservation also retains an unread observer of the registration's
existing watch signal. At finish, abandon and Drop, the original router owner
lock atomically releases an unnotified selected owner without scanning and
launching unrelated backlog. Explicit independent producer notifications,
including coordinator wakeups, retain the original successor path. A producer
arriving after release sees no owner and reserves its own activation. Cancellation
while awaiting finish keeps this restricted Drop behavior. Ordinary reservations,
quarantine, durable eligibility, generation counters and recovery stay unchanged.

This is an immutable request restriction in an existing capability, with no new
grant, lifetime proof, persistent map, index, lease or alternate state machine.
All original Actor/lifetime/physical writer guards, activation eligibility,
checkpoint/ACK, permission ceilings, cancellation and finalization remain.

## Output ownership and cancellation

The existing 202 response is an **activation receipt**, with the exact newly
reserved `run_id`. It echoes `Bamboo-Input-Id` and `Bamboo-Ingress-Seq`, plus
`Bamboo-Output-Scope: activation-receipt`. It is not an LLM completion payload.
The existing `events_url` still names the Native shared Session stream; this
slice does not claim an isolated run stream, output replay or protocol response
conversion. Those are subsequent adapter work, never inferred by joining the
current runner or returning another execution's result.

To cancel only that execution, use either existing stop alias with:

```http
POST /api/v1/sessions/{session_id}/stop
Bamboo-Run-Id: {run_id from the activation receipt}
```

The wrapper checks the exact current runner under the registry read lock and
cancels only its retained token, then applies the existing exact-id terminal
update. A wrong, stale or completed run returns 409. An already-cancelled matching
run is acknowledged idempotently. There is no legacy token-table fallback and a
delayed cancel cannot stop a successor. Stop without the header and WS session
stop retain their existing behavior and source entry point.

Dropping a request before spawn retains original reservation RAII and exact
Actor cleanup. After spawn the Native background run continues if HTTP disconnects;
explicit cancellation is the control operation. The original physical checkpoint
and ACK jobs retain their guards through cancellation. A lost response after ACK
may therefore leave an uncertain client outcome; never assume it did not execute.

## Idempotency and restart

The existing process-local idempotency store fingerprints the execute request
and canonical selector together. Concurrent/same-key retries replay the original
small response within its configured window; changing the payload or selector
with that key conflicts. An error response is also replayed in that window.

The store is not durable. After restart, cache expiry, eviction or a lost
post-spawn cache commit, this path cannot promise to recreate the original 202.
An ACKed/missing/unavailable selected receipt fails closed, without starting or
joining another run, cancelling a successor, or granting Fresh from history.
A preparation failure after ACK likewise needs an explicit new accepted input
for a future attempt. There is no durable exactly-once execution claim.

## Acceptance boundary

Real local HTTP and fake-provider fixtures cover selected current User/Skills
over multiple rounds, sibling isolation, actual run/fence identity, active-owner
rejection, idempotency, wrong/staged selectors, startup failure/Drop cleanup,
restart and exact cancellation. Existing Native Main/Ultra, execute/stop/owned
Inbox families and actual OpenAI Chat/Responses/Anthropic/Gemini Reader envelope
and EOF conversion checks remain required. No paid provider or deployment is used.

Compatible HTTP activation adapters, isolated exact-run output/replay, broader
wait/legacy surfaces and configured-provider/full platform acceptance remain open.
The previous optional all-targets Clippy baseline failures remain separately
preserved; passing original-contract library Clippy does not erase them.
