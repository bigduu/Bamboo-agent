# Compatibility receipt activation

This is Bamboo's asynchronous receipt-activation extension, not a vendor
completion response. The shared contract is independent of OpenAI, Anthropic,
Gemini, the selected provider, and the model. Non-streaming Chat Completions, Responses,
Anthropic Messages and Gemini generateContent have explicit adapters. Each validates its own HTTP schema before the
unchanged neutral Native activation port; Messages does not construct an OpenAI
request or pass through its ordinary conversion layer. Anthropic legacy Complete
and Gemini streaming activation remain open. Ordinary forwarding remains supported
at all four protocol surfaces.

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

## Shared headers and Chat Completions entry

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

## Anthropic Messages entry

`POST /anthropic/v1/messages` uses the same six single headers, neutral202
envelope, actual selected run, Native execute idempotency namespace, exact finite
output and exact cancellation. Its strict Messages receipt schema validates
`{"model":"bamboo-agent","messages":[]}`, optionally `stream:false` and empty
`tools:[]`. `system` (text or blocks), `max_tokens`, sampling/stop parameters,
nonempty transcript/tools, tool choice, streaming and every flattened extra option
return400 before ordinary conversion, provider forwarding or Root admission.
Only the four allowed keys may be present, with their exact allowed values;
explicit null is rejected too (including `stream:null` and `tools:null`). The HTTP
extractor selects either this strict schema or the original ordinary
`Json<AnthropicMessagesRequest>` extractor. Both retain the existing JSON
content-type, compression and payload-limit handling. Ordinary null/default and
duplicate-field semantics remain the original DTO's behavior.
These fields cannot replace accepted input, add authority, select a backend or
override the server's budget. The shared activation module and all Native owners
remain unchanged.

This is explicitly a **Bamboo202 extension**, not a standard Anthropic
`type:message` response. It has no fabricated content, stop reason or usage.
The extension omits `max_tokens`; ordinary Anthropic SDKs that require it need a
custom Bamboo extension request/client and are not claimed to work out of the
box. Original ordinary Messages generation, including its existing SSE path,
still uses the unchanged conversion/provider pipeline without receipt selectors.
Streaming activation returns400; standard Messages completion/usage/stream
mapping is outside this slice.

Messages proofs use a real configured HTTP AppState with **only an Anthropic
synthetic provider registered**, no OpenAI provider/fallback. Actual Native Chat
acceptance reaches Root reservation/owned claim/ACK/Ready/SDK execution, shared
Skills main/reference paging to EOF, an actual Read action and retained exact
output. Native execute, Chat and Messages replay the same key to the same actual
run. Default coordinator competition,409/410, stale cancellation, source/input
isolation and the verified User ceiling remain unchanged and tested.

## Responses entry

`POST /openai/v1/responses` uses the same six single receipt headers, unchanged
neutral Native idempotency/lease/owned claim/ACK/Ready/spawn, actual-run202 envelope,
finite exact output and exact cancellation. Its strict four-field receipt body is
`{"model":"bamboo-agent","input":[]}`, optionally `stream:false` and empty
`tools:[]` using the own Responses tool schema. Input must be a present empty
array; text/nonempty transcript, missing/null input, null model/stream/tools and
every unknown field reject400 before admission/provider. This includes null or
present instructions, max_output_tokens, previous_response_id, conversation,
store, tool_choice, reasoning, messages or authority/budget options. The extension
does not construct a ChatCompletion request or let caller input become authority.

Without receipt selectors, the original `Json<ResponsesCreateRequest>` extractor,
preparation, conversion, output/stream and scoped request_session_id propagation
remain unchanged. JSON content-type/compression/size/default/duplicate semantics
stay on the framework's original extractor; `Idempotency-Key` alone is ordinary
forwarding. Existing scoped model-proxy credentials are allowed to forward
ordinary Responses with their actual session context. They **cannot activate a
Root**, including replay of a cached202: the original HTTP extensions reach the
unchanged neutral verified User gate, returning403 before Native execution/cache
lookup. Revocation keeps401 even on loopback.

This is explicit **Bamboo202**, not a standard Responses object, ChatCompletion,
SDK out-of-box response, stream/usage mapping or durable replay. Standard
Responses activation completion/error/stream/usage remains open. Tests use actual
HTTP/Root/Skills EOF/Read/output with synthetic providers, four-entry same-key
Native/Chat/Messages/Responses, proxy-context and cached-replay denial, strict
null/owner/input validation, default coordinator409 and finite output410.

## Gemini generateContent entry

`POST /gemini/v1beta/models/bamboo-agent:generateContent` uses the same six single
receipt headers and the unchanged neutral Native202/run/input/output/cancel
contract. The virtual URL model must be exactly `bamboo-agent`; it does not pick
the Gemini backend or change the server-owned Root session model. The strict
native schema requires `{"contents":[]}`, optionally empty `tools:[]` using Gemini
content/tool types. Only these two keys may be present. Missing/null/nonempty
contents, null/nonempty tools and every unknown key reject400 before conversion,
model mapping, provider forwarding or Root admission. This includes explicit
null systemInstruction/system_instruction, generationConfig/generation_config,
model, stream, safetySettings, toolConfig, cachedContent, transcripts, sampling,
usage or authority/budget options. Other URL models also reject400 in receipt
mode. No vendor DTO can replace accepted input or grant Root authority.

Without receipt selectors the exact original `Json<GeminiRequest>` extractor,
native aliases/defaults/unknown-field/duplicate/content-type/size handling, URL
model mapping and ordinary conversion/output/SSE remain. `Idempotency-Key` alone
is ordinary forwarding. Existing nonstream endpoint routing follows the original
provider_model_ref flag; streaming continues to use the original default provider.
No routing repair is included. Valid or revoked Responses run-tokens retain401
for Gemini paths, even on loopback. Original HTTP extensions still reach the
neutral verified User gate; a verified Worker context cannot replay cached202.

This is an explicit **Bamboo202 extension**, not a standard Gemini
generateContent response: it fabricates no candidates, finishReason or usage.
An unaware Gemini SDK cannot consume it as a normal completion. Standard vendor
completion/error/stream/tool-event/usage mapping and durable replay remain open.
`streamGenerateContent` receipt activation stays501; legacy Complete stays501.

Actual configured HTTP/Root SDK/Skills main+reference EOF then Read proofs run
against only an Anthropic synthetic provider, with no OpenAI prerequisite. Native
execute, Chat, Messages, Responses and Gemini share one operation key/run; exact
cancel/prefix, cached202/finite410, actual coordinator-first409/background410 and
stale cancel remain unchanged. Strict body/path/header/owner refusal and ordinary
Gemini model/provider routing, JSON/SSE/native aliases/duplicates/null semantics
and authentic token path denial are covered separately.

## Protocol status and bounded remaining work

| Entry | Receipt activation in this slice | Existing path without activation selectors |
| --- | --- | --- |
| OpenAI Chat, non-streaming | Actual selected Root run, explicit202 | Existing stateless completion forwarding |
| OpenAI Chat, streaming | 400 | Existing SSE forwarding |
| OpenAI Responses, non-streaming | Actual selected Root run, explicit202 | Existing Responses forwarding and scoped model-proxy context |
| OpenAI Responses, streaming | 400 | Existing Responses SSE forwarding |
| Anthropic Messages, non-streaming | Actual selected Root run, explicit202 | Existing Messages forwarding |
| Anthropic Messages, streaming | 400 | Existing Messages SSE forwarding |
| Anthropic legacy Complete | 501 | Existing legacy forwarding |
| Gemini generateContent, non-streaming | Actual selected Root run, explicit202 | Existing Gemini DTO/model/endpoint-provider forwarding |
| Gemini streamGenerateContent | 501 | Existing Gemini SSE/default-provider forwarding |

Any mode/session/receipt selector activates fail-closed detection, so incomplete
activation cannot quietly turn into stateless forwarding. `Idempotency-Key`
alone remains an ordinary vendor header. Unsupported entries reject the explicit
extension before conversion/provider/admission; this is a declared gap, not
multi-protocol activation delivery. The Chat, Responses, Messages and Gemini generateContent adapters
share this neutral contract without changing the Root core; remaining adapters stay open. Full vendor completion/stream/tool-event
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
protocol forwarding, ordinary Anthropic Messages SSE and original CORS origin/header
policy. Messages additionally proves execution and Skills with an Anthropic-only
provider registry plus Native/Chat/Messages idempotency. Responses adds strict input/null
validation, four-entry replay, and authentic scoped-proxy session preservation
with activation/cache-replay denial. Gemini adds strict contents/tools and URL
selector validation, five-entry replay, preserved ordinary native DTO/model/provider
routing/SSE, and authentic credential path denial. Existing Native
ABA/owner/claim/cancellation/Skills and protocol regressions remain required.
