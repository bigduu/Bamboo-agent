# Compatibility receipt activation, first adapter

This is Bamboo's asynchronous receipt-activation extension, not a vendor
completion response. The shared contract is independent of OpenAI, Anthropic,
Gemini, the selected provider, and the model. The first adapter is the smallest
non-streaming Chat Completions entry. Responses, Anthropic and Gemini activation
remain open; ordinary forwarding remains supported at all four protocol surfaces.

## Shared receipt, run and output contract

An input receipt identifies an already accepted input by message id, positive
ingress sequence, session id and expected Root birth. It does not reserve a run.
Native Chat keeps its existing scheduling: a legitimate coordinator can consume
the input first. Explicit activation then returns 409; it cannot join that
coordinator's run, claim a sibling, or opt the background run into selected output.

Successful activation must return the run actually started by the existing Root
reservation, owned Inbox CAS/claim/ACK, shared Skills preparation and Ready/spawn.
Only server-loaded input and authority reach that pipeline. Vendor transcript,
model, tool declarations and flattened parameters cannot grant authority, supply
new input, alter Skills selection, enlarge a budget or bypass approval.

The existing Native HTTP execute handler owns one idempotency namespace across
Native and compatibility requests. An identical selected receipt and operation
key replay the same operation/run; another receipt with that key conflicts.
There is no second cache, reservation, owner, cancellation controller or state
machine. The cache is process-local. It does not promise durable exactly-once
delivery after process loss or disconnect. Losing the request after accepted202
does not cancel the actual run; explicit cancellation requires the exact run id.
Existing startup Drop/rollback and execution cancellation remain authoritative.

Output is the finite selected runner's existing visible-message snapshot at
`GET /api/v1/sessions/{session_id}/runs/{run_id}/output`, with the expected
`Bamboo-Session-Created-At` header. It excludes reasoning, unrelated inputs and
successors, and has no history, other-run or vendor-output fallback. See
[selected-run output](selected-run-output.md) for retention and terminal details.
A cached202 can outlive this output: replaced, unretained, evicted or restarted
runners return 410. A mismatched canonical Root birth/kind returns 409.
Neither replay nor reading output pins a runner or starts another run.

Cancel uses the existing `POST /api/v1/sessions/{session_id}/stop` with
`Bamboo-Run-Id: <run_id>`. A stale run id conflicts and never cancels a successor.
Cancellation retains the selected run's visible prefix while retained. Native's
existing output terminal and `runner_status` distinguish cancelled from failed.

## First entry: Chat Completions

`POST /openai/v1/chat/completions` opts in only with these six single headers:

| Header | Value |
| --- | --- |
| `Bamboo-Activation` | `receipt` |
| `Bamboo-Session-Id` | Existing Root id, 1–128 ASCII letters, digits, `_` or `-` |
| `Bamboo-Input-Id` | Actual Native Chat `message_id` |
| `Bamboo-Ingress-Seq` | Actual Native Chat positive `ingress_seq` |
| `Bamboo-Session-Created-At` | Expected canonical Root birth, RFC3339 |
| `Idempotency-Key` | Existing Native mutation operation key |

The body is `{"model":"bamboo-agent","messages":[]}`. `stream:false` and empty
`tools:[]` are equivalent optional spellings. Generation parameters, nonempty
transcripts/tools, tool choice, streaming and stream options return 400 before
provider forwarding or Root admission. `bamboo-agent` is a virtual adapter
selector; the existing server-owned session/model route remains authoritative.
It does not select the OpenAI provider.

The compatibility route retains access middleware and rate/CORS policy. The
shared adapter additionally calls the canonical verified User gate, preserving
the original HTTP request and any scoped-credential context. A Worker/model-proxy
credential cannot be promoted to Root execution authority. Native's canonical
Supervisor/Root ownership, birth, current-input, permission-ceiling and Actor
fence checks still run.

The adapter invokes `agent::execute::handler` with the original HTTP request and
the ordinary empty ExecuteRequest. It does not use the selector-free in-process
execute port. Native errors/statuses pass through. A successful Native202 is
projected into this explicit extension envelope, with `Cache-Control: no-store`:

```json
{
  "object": "bamboo.activation",
  "status": "started",
  "session_id": "root-id",
  "session_created_at": "2026-10-14T00:00:00Z",
  "run_id": "actual-run-id",
  "input": {"message_id": "accepted-input-id", "ingress_seq": 1},
  "output_scope": "visible_messages",
  "output_url": "/api/v1/sessions/root-id/runs/actual-run-id/output",
  "cancel_url": "/api/v1/sessions/root-id/stop",
  "cancel_header": {"Bamboo-Run-Id": "actual-run-id"}
}
```

The four existing Native receipt headers remain browser-exposed; request headers
already pass the existing permissive-header, restricted-origin CORS policy.
The Native `Bamboo-Output-Scope: activation-receipt` header describes the202
receipt; the envelope's `output_scope: visible_messages` describes its linked
GET output. This response deliberately has no vendor `choices`, `usage`, vendor
completion id or shared session event URL. It is not consumable as a standard
ChatCompletion by an unaware SDK. A client must opt in and follow the explicit
Bamboo receipt/output/cancel contract.

## Protocol status and bounded remaining work

| Entry | Receipt activation in this slice | Existing path without activation selectors |
| --- | --- | --- |
| OpenAI Chat, non-streaming | Actual selected Root run, explicit202 | Existing stateless completion forwarding |
| OpenAI Chat, streaming | 400 | Existing SSE forwarding |
| OpenAI Responses | 501 | Existing Responses forwarding, scoped Codex model-proxy behavior |
| Anthropic Messages / legacy Complete | 501 | Existing Anthropic forwarding |
| Gemini Generate / StreamGenerate | 501 | Existing Gemini forwarding |

Any mode/session/receipt selector activates fail-closed detection, so incomplete
activation cannot quietly turn into stateless forwarding. `Idempotency-Key`
alone remains an ordinary vendor header. Unsupported entries reject the explicit
extension before conversion/provider/admission; this is a declared gap, not
multi-protocol activation delivery. Future vendor adapters can share this neutral
contract without changing the Root core. Full vendor completion/stream/tool-event
mapping, token usage accounting, durable event replay, reconnect cursors and
distributed execution are outside this slice.

## Acceptance evidence

Tests cross the actual configured HTTP routes and default Root scheduler, using
synthetic providers without paid model calls. They cover actual Native Chat
acceptance to compatibility202 to Root Actor run and isolated output; shared
Skills paging/EOF followed by a real action; Native/compatibility idempotency;
cancel and retained prefix; coordinator-first409/background-output410 without
adoption; cached202 versus replaced/evicted output; stale-cancel isolation;
invalid/duplicate/partial/foreign selectors and rejected authority options;
verified owner ceiling; declared unsupported protocol entries; original four
protocol forwarding and original CORS origin/header policy. Existing Native
ABA/owner/claim/cancellation/Skills and protocol regressions remain required.
