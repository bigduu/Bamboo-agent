# Local model usage and trace inspection (schema 1)

The Bamboo executable records a bounded, in-memory projection of its own OTel
agent, primary model-call and tool-dispatch spans by default. It does not run a
collector and does not accept arbitrary OTLP data. Records are lost on restart;
the process keeps at most 2,048 completed spans across all sessions. The local
store uses a nonblocking insertion path, so it is best effort and is not a billing
ledger. Set `BAMBOO_LOCAL_TRACES_ENABLED=false` to disable and clear it. Library
hosts retain control of their own telemetry initialization.

Only fixed operation names, validated identifiers, timestamps, fixed outcome
categories and provider-reported token counts are projected. No prompts,
responses, tool arguments/results, paths, credentials, span events or raw error
messages are stored. As with the existing OTel mapping, operators must not place
secrets in syntactically valid session, model or tool identifiers. For a session
ID outside that policy, local inspection uses a process-keyed HMAC reference
instead of retaining the original in spans; request the API with the original,
URL-encoded session ID. Completed children can appear before their parent;
`partial` is conservative when a parent is absent, retention has evicted data,
or insertion contention has caused a drop. It cannot prove an entire trace was
observed, and remote or separately managed spans may be missing.

The OpenAI-compatible Chat and Responses, Anthropic Messages and Gemini
GenerateContent paths supply optional per-call snapshots. Anthropic reports
fresh input, cache creation and cache read as disjoint components; the snapshot
forms input total only when all three are present and the sum fits. It emits the
snapshot at `message_stop`, so an interrupted stream can have unknown usage.
Gemini reports prompt and cached content separately from candidate and thought
tokens. Its snapshot combines candidate and thought tokens once for output
only when both are present; otherwise output remains unknown. When reported,
tool-use prompt tokens are included in input. The current adapter sends
client-side function declarations, not Google-managed server tools. These
rules follow the [Anthropic prompt-cache fields](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
and [Gemini usage metadata](https://ai.google.dev/api/generate-content).
Legacy session counters may have different availability; do not infer a
missing snapshot value from them.

External OTLP still requires an explicit endpoint and opt-in. Local recording
remains enabled when exporter setup fails. When export sampling drops a span,
that span remains locally recorded but is not sent to the exporter. See
[OpenTelemetry](OPENTELEMETRY.md) for build and export configuration.

## Read-only HTTP API

Both endpoints use the existing HostOwner authorization boundary, the process's
data-authority scope and the exact persisted session lifetime. An opaque,
process-local key separates data directories without retaining their paths in
trace records or sending that key to OTLP. They return `Cache-Control: private, no-store`, do
not accept Codex run tokens, and never mix another session's spans. A missing
session or trace returns 404. The list limit is 1–100 (default 50); `cursor` is
opaque, and a client should deduplicate by `trace_id` when refreshing. A cursor
can become stale as the bounded store changes.

`GET /api/v1/sessions/{session_id}/traces?limit=50&cursor=...`:

```json
{
  "schema_version": 1,
  "session_id": "session-1",
  "collection_state": "enabled",
  "coverage": "best_effort",
  "traces": [{
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "started_at": "2026-10-29T02:00:00Z",
    "ended_at": "2026-10-29T02:00:01Z",
    "status": "success",
    "span_count": 2,
    "partial": false
  }],
  "next_cursor": null,
  "retention": {"max_spans": 2048, "evicted_spans": 0}
}
```

`collection_state` is `enabled` or `disabled`. `evicted_spans` is scoped to the
session lifetime; `null` means its bounded eviction counter was retired and the
number is unknown. `GET /api/v1/sessions/{session_id}/traces/{trace_id}` returns:

```json
{
  "schema_version": 1,
  "session_id": "session-1",
  "trace": {
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "partial": false,
    "spans": [{
      "span_id": "00f067aa0ba902b7",
      "parent_span_id": null,
      "kind": "llm",
      "name": "chat",
      "started_at": "2026-10-29T02:00:00Z",
      "ended_at": "2026-10-29T02:00:01Z",
      "status": "success",
      "error_type": null,
      "model": "synthetic-model",
      "provider": "openai",
      "run_id": "run-1",
      "call_id": "call-1",
      "usage": {
        "input_tokens": 100, "output_tokens": 20, "reasoning_tokens": 3,
        "cache_read_input_tokens": 0, "cache_creation_input_tokens": null,
        "cache_write_input_tokens": 12
      }
    }]
  }
}
```

The detail example is schematic: an actual child retains its actual parent ID.
`kind` is `agent`, `llm`, or `tool`; status is `success`, `error`, or
`cancelled`. Missing identifiers, usage and individual counters are `null`.
A `null` counter is unknown; `0` is an explicit provider-reported zero. Input
is the provider's total input; cache read/creation are input subsets, cache
write is separate provider metadata, and reasoning is an output subset. Do not
add subsets to the totals. A cache-read ratio is meaningful only when both
input and read are known, input is positive, and read does not exceed input.

## Read-only hook event

Configured native hooks and reviewed portable plugins may subscribe to the
Bamboo extension `BambooModelCallUsage`. It is emitted once per dispatched
primary model call after success or a reported error. A `session_id` that meets
the identifier policy is unchanged. Other nonempty session IDs are represented
by `~` followed by a 64-digit lowercase HMAC reference. That reference is
stable within one process only, changes after restart, is not intended to be
reversed or used as an authorization credential, and must not be treated as
the persisted session ID.
Dropping an in-flight call emits a best-effort cancelled/error observation with
unknown usage. Internal transport retries share a call ID; a new outer dispatch
gets a new ID. If a stream fails after reporting usage, its error observation
retains the reported counters. Repeated cumulative snapshots update known
counters instead of adding them. Hook execution is best effort: a process-wide
maximum of 32 queued/running observers and a two-second deadline can drop work.
Return values never control the model call. Up to four usage-plugin registry
and file reads, path resolutions, bundle hash checks and data-directory
creations may remain active after a timeout; further such work is dropped until
a slot becomes free. For this event, registry and manifest reads are limited to
64 MiB and hook configuration reads to 64 KiB; exceeding a limit drops that
portable delivery. Native handlers receive the existing envelope with a
`payload` of type `model_call_usage`; portable handlers receive `observation`
at top level. Both contain the same schema-1 snapshot and no workspace path in
the event payload.
Programmatic native observers receive a deliberately minimal Session: its id and
root id follow the same reference policy, and its birth identifies the original
lifetime. Other Session fields are not a snapshot of runtime state. Consume the
usage observation itself.
Deduplicate on `event_id`; retain IDs according to your plugin's own policy.

A reviewed, runnable portable example is in
[`crates/infra/bamboo-plugin/examples/model-usage`](../../crates/infra/bamboo-plugin/examples/model-usage).
It reads JSON from stdin and keeps the last 1,000 distinct event IDs in the
plugin data directory. This is an example, not a billing or durable audit system.
