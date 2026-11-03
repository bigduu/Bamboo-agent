# Unmetered Work patrol evidence

`AppState::read_work_patrol_evidence(scope, expected_ticket_commit, since_cursor)`
reads a bounded evidence snapshot for one current Ticket Assignment. The public
Host Rust API returns `UnmeteredWorkPatrolEvidence`. It obtains current Host
authority independently; the supplied Scope, commit and cursor are selectors.
No HTTP route or model tool exposes this API.

This reader is a prerequisite for semantic patrol. The existing program patrol
remains zero-model, and its semantic `diagnostic_input` port remains unavailable.
The output has no provider, model, token estimate or budget reservation, and no
automatic conversion into `DiagnosticInput`. A later protocol adapter must meter
its complete request envelope and obtain canonical cumulative Work budget
admission before submitting anything to a provider.

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
to 16 KiB. Oversized content is refused rather than clipped into a misleading
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

## Validation boundary

Focused tests exercise the public Host reader against a disposable canonical
Ticket/Project/worker and native Jiandu store, plus bounded collector negatives.
The fixture distinguishes adopted and candidate contexts, Project isolation,
source-resolution outcomes, repeated reads, cold reopen and authority/source
changes. Persistent-byte comparisons and provider call counts cover the reads;
fixture setup deliberately creates its own disposable authoritative state.
Runtime counts belong to the exact tested source, platform and configuration.

This proves the evidence reader only. Automatic semantic patrol consumption,
independent provider configuration, protocol-specific metering, usage/Work
budget assembly and remaining runtime measurements are separate slices under
[#2049](https://github.com/bigduu/Bamboo-agent/issues/2049) and
[#2050](https://github.com/bigduu/Bamboo-agent/issues/2050). No paid-provider or
installed-application acceptance is implied.
