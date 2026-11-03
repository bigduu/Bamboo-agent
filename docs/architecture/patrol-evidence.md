# Unmetered Work patrol evidence

`AppState::read_work_patrol_evidence(scope, expected_ticket_commit, since_cursor)`
reads a bounded evidence snapshot for one current Ticket Assignment. The public
Host Rust API returns `UnmeteredWorkPatrolEvidence`. It obtains current Host
authority independently; the supplied Scope, commit and cursor are selectors.
No HTTP route or model tool exposes this API.

This reader is a prerequisite for the staged semantic pass. The program-only
pass still uses zero model calls.
The output has no provider, model, token estimate or budget reservation, and no
automatic conversion into `DiagnosticInput`. The separate preparation API below
requires an explicit selected model and a trusted local protocol meter.

## Crate ownership

`bamboo-orchestration::patrol::evidence` owns the unmetered data contract,
deterministic adopted-context projection, resource validation and byte/reference
limits. Its evidence plan consumes Ticket/domain values and performs no I/O or
authority acquisition. Native reference/status payloads remain opaque data; the
policy crate does not depend on Jiandu, MemoryStore, AppState or the server.

`bamboo-server` obtains fresh Host authority and performs concrete Project/Jiandu
I/O. It validates native reference identity and uses the core evidence plan;
the public Host API and its existing type paths remain available through aliases.
`bamboo-projects` owns the physical read-only Project proof. Ticket state,
contracts, adopted contexts and cumulative budget facts remain in
`bamboo-tickets`. Pure policy tests run without starting a server; actual
Host/Task/native-source/reopen tests stay with the concrete server adapter.

## Snapshot and adopted sources

Coverage is explicitly `bounded_snapshot`, at the returned Ticket commit, cursor
and execution-facts revision. `since_cursor` is a requested boundary, not proof
of complete delta/event coverage. A future cursor or changed expected commit is
rejected. The reader does not fetch transcripts or infer start, heartbeat,
meaningful progress, workspace availability or tool execution from declarations.

Jiandu references come only from the Assignment's adopted Project context. Its
hash must match the immutable DispatchSpec. A newer Work candidate context does
not replace the context used by that Assignment. The reader resolves only exact
Project source pins through the configured native read-only MemoryStore API.
It does not discover additional records or fall back to Session or Global memory.

An exact active version match may return source content. Missing, changed,
inactive, oversized, unavailable and unsupported sources remain explicit and
prevent an all-sources-matched result. Content pins identify bytes, not verified
truth, authority or confidence. Memory content remains untrusted evidence data.
An all-sources-matched result covers the selected adopted references only; it
does not establish complete evidence, health, recovery or Work acceptance.

Eight references include the two fixed Ticket/context references, leaving at
most six adopted source references. The complete serialized response is bounded
to 16 KiB. The core reserves overflow-marker metadata for every source on each update,
so a later oversized source preserves earlier usable partial content.
Oversized content is refused rather than clipped into a misleading
exact-source claim. Jiandu content versions remain strings, distinct from numeric
Ticket execution revisions. Unsupported source kinds are not retrieved through
an alternative resolver.

## Authority and side effects

Before and after native I/O the Host checks authority, the same TicketService,
exact HEAD/Scope/current Assignment and active Project context/resources. Project
validation uses the assembled read-only store. No Ticket lock spans asynchronous
I/O. Any changed authority or source boundary fails closed. A returned snapshot
is an observation, not a retained read or execution lease; native source outcomes
describe the time of their individual exact-version reads.

`ProjectStore::get_read_only` accepts only an existing current-schema primary,
existing nonzero matching revision floor and existing shared lock. It uses
bounded descriptor-relative no-follow reads on Unix and rejects unsupported
platforms. It does not call the normal `get` migration/recovery path or `open`
initialization path. Missing, busy or repair-dependent Project data is an error.

Reads do not reserve or settle tokens, write Ticket/Actor/checkpoint/Inbox/Jiandu
state, run reconcile, dispatch a worker, stage an escalation, activate a Session
or call a provider. The existing checkpoint codecs and Inbox bodies retain their
original identity and retry behavior.

## Metered diagnostic input preparation

`AppState::read_work_patrol_diagnostic_input` accepts the current patrol
observation, expected Ticket commit, since cursor, selected patrol model and
trusted local meter. It uses the same native evidence reader. Orchestration
checks the exact Scope, source revision and cursor window, then builds bounded
canonical JSON and opaque references for the Ticket snapshot, adopted context
and selected source pins. Each selected key must be unique and have matched its
pinned version at the read. An empty selection still has the two fixed references.

The meter receives the selected model and exact summary and reference payload.
It accounts for the complete model-visible protocol envelope, including
instructions and schema, without submitting a provider request. A missing,
zero or over-cap count fails preparation. The Host checks current authority,
Ticket HEAD, Assignment and Project proof again after measurement. Native
content retains the read-time exact-version semantics of the evidence snapshot.

### Configured native prompt

The optional `patrol_model` key belongs to the existing Model Policy section.
An absent selection disables configured preparation. For example, the section
data can contain:

```json
{
  "patrol_model": {
    "provider": "patrol-local",
    "model": "gpt-oss-20b",
    "protocol": "llama_cpp_raw_v1",
    "endpoint": "127.0.0.1:8080",
    "max_input_tokens": 4096,
    "max_output_tokens": 256,
    "max_evidence_bytes": 16384,
    "max_evidence_refs": 16
  }
}
```

The provider is an explicit routing identifier for patrol admission. The
profile requires the pinned model name and uses the bundled Harmony
vocabulary. Orchestration renders the read-only instructions, response schema
and exact summary/reference data. The LLM layer adds the Harmony role framing
and encodes ordinary text separately from control tokens. The Host meters
outside the async executor and Ticket locks, rechecks the evidence fences and
selection, and returns the model, input and immutable numeric prompt together.

The LLM layer offers a one-use native `/completion` transport for an explicitly
trusted numeric loopback address. It checks the single gpt-oss-20b alias and
model metadata, slot context and a literal Harmony tokenizer probe before
submitting any evidence. The checks identify the deployment's compatible
protocol profile. Only raw output tokens in a completed final-channel frame
can yield advisory text. Native final input and output counters are returned
separately, including observed excess output. A transport failure or missing
usage retains unknown status. The optional endpoint enables the existing Ready
worker to use this transport for an eligible anomaly. Omit it to keep the
program-only pass and the read-only preparation API.

Orchestration stages the input and reserves against the canonical Work,
UTC-day and total token ceilings before publishing the exact diagnostic
checkpoint. Only a live staged submission can schedule the provider call.
The Host checks the selected configuration, Ready fence, current Ticket,
Project and unsettled receipt before sending. The provider task reports usage
against the original receipt even if the diagnostic result has become stale.
Unknown usage retains the original caps. An observed overrun halts admission
when recorded, and exact results awaiting accounting use the original receipt.
Graceful shutdown stops new provider calls, waits for bounded accounting
writes, and attempts a final drain. An unresolved drain logs a warning. If
the process exits before usage reaches the ledger or checkpoint, the original
reservation remains charged at its caps. The operator supplies and trusts the
loopback deployment.

[Harmony format](https://developers.openai.com/cookbook/articles/openai-harmony) ·
[llama.cpp raw completion](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md#post-completion-given-a-prompt-it-returns-the-predicted-completion)

## Validation boundary

Focused tests exercise the public Host reader against a disposable canonical
Ticket/Project/worker and native Jiandu store, plus bounded collector negatives.
The fixture distinguishes adopted and candidate contexts, Project isolation,
source-resolution outcomes, repeated reads, cold reopen and authority/source
changes. Persistent-byte comparisons and provider call counts cover the reads;
fixture setup deliberately creates its own disposable authoritative state.
Runtime counts belong to the exact tested source, platform and configuration.

The native fixture also exercises metered preparation with a controlled local
meter, exact payload retention, over-cap refusal and a Ticket HEAD change
during measurement. Configured preparation is also tested with the local native
profile, exact-cap acceptance, over-cap refusal, and a stale model selection.
A controlled loopback provider fixture also exercises the raw request, the
native Work ledger and stale configuration refusal. Live model and
installed-application measurements remain separate under
[#2049](https://github.com/bigduu/Bamboo-agent/issues/2049) and
[#2050](https://github.com/bigduu/Bamboo-agent/issues/2050).
