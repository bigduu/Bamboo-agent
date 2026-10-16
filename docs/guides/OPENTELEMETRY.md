# Optional OpenTelemetry traces

This source increment adds a small, **disabled-by-default trace foundation** for
Bamboo's engine. Existing logs, `metrics.db`, token-usage journals and runtime
identities retain their current behavior and authority. It does not export OTel
metrics or logs, install a `tracing` subscriber bridge, or change permissions.

## Enable a local collector

Build with the optional transport feature, then explicitly select a collector:

```sh
BAMBOO_FRONTEND_BUILD_MODE=api-only cargo build --locked --features otel-otlp --bin bamboo
BAMBOO_OTEL_ENABLED=true \
BAMBOO_OTEL_ENDPOINT=http://127.0.0.1:4318/v1/traces \
BAMBOO_OTEL_SAMPLE_RATIO=1 \
  target/debug/bamboo serve
```

The API-only example has no bundled browser UI. Use the normal locked frontend
staging procedure for a UI build. An OTLP/HTTP protobuf receiver must already be
running at the chosen endpoint; Bamboo does not install or launch a collector.
No tests call a paid model or an external telemetry service.

- `BAMBOO_OTEL_ENABLED`: `true`/`1` enables; absent, `false` or `0` disables.
- `BAMBOO_OTEL_ENDPOINT`: complete trace URL ending in `/v1/traces`, optionally
  after a reverse-proxy prefix. Required when enabled; a collector base URL alone
  is rejected. HTTPS is required except for loopback HTTP. User information,
  passwords, query strings and fragments are rejected. Maximum 2,048 bytes.
- `BAMBOO_OTEL_SAMPLE_RATIO`: finite number from 0 to 1, default 1. This applies
  to new roots. A valid remote parent's sampling decision is honored.

Without `otel-otlp`, enabling export returns `FeatureUnavailable`. Invalid
configuration or exporter initialization fails open in the CLI, with a fixed
warning and ordinary agent execution continuing. Disabled initialization does
not inspect exporter settings or start exporter tasks/network work. `RUST_LOG`
and standard `OTEL_*` variables cannot enable Bamboo export.

The configured endpoint and sampling policy override ambient OTel values.
Arbitrary `OTEL_*` HTTP headers, including credentials, are **not forwarded**.
This first slice has no credential configuration. Use an unauthenticated local
collector and configure any downstream credentials there. Ambient exporter
compression settings can make initialization fail if unsupported; they cannot
redirect export. HTTP proxies and redirects are disabled.

## Recorded lifecycle and fields

Each actual agent-loop invocation gets one finite `invoke_agent` INTERNAL span.
Resuming the same persisted session creates another invocation; a session is not
an indefinitely open span. Existing execution/run IDs are attached only after
the runtime establishes them.

Each primary provider dispatch gets a `chat {model}` CLIENT span, starting after
prompt/preflight validation and ending after stream consumption or failure. It
includes provider-internal HTTP retry time. An outer engine retry is another
request span. Auxiliary model calls (such as compression or task evaluation) are
not covered by this first slice. This is not per-HTTP-attempt instrumentation.

Each admitted tool-executor dispatch gets an `execute_tool {name}` INTERNAL
span. Denied calls before dispatch do not fabricate tool execution spans. A
background/suspended tool's span describes dispatch and its acknowledgement,
not eventual background completion. Both parallel tools remain children of the
correct invocation. No tool-call arguments or results are recorded.

The application attribute allowlist is:

- `gen_ai.operation.name`, `gen_ai.conversation.id`
- `gen_ai.provider.name`, `gen_ai.request.model`
- `gen_ai.tool.name`, `gen_ai.tool.call.id`
- `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`
- `bamboo.root_session.id`, `bamboo.execution.id`, `bamboo.run.id`, `bamboo.round.id`
- `bamboo.tool.observation` (`dispatch`), `bamboo.outcome`, `error.type`

Identifiers must be 1–128 ASCII letters, digits, underscores, hyphens or periods;
other values are omitted, and span names fall back to the operation alone. The
provider family is restricted to OpenAI, Anthropic, Gemini/Google and Copilot;
free-form provider instance names are omitted. These are correlation fields,
not a place to put sensitive data. Even syntactically valid IDs/model/tool names
must not contain secrets. IDs are high-cardinality trace metadata, not metric
labels. Resource attributes are limited to `service.name=bamboo` and the build's
`service.version`; there are no host, process, environment or filesystem
resource detectors.

Token attributes use authoritative provider totals, preserving omitted versus
explicit zero. Cached input and reasoning output subsets are not added again;
local estimates are not labeled as provider usage. An interrupted stream can
report usage already observed before the failure. Fixed error categories are
used instead of error messages, stack traces or panic payloads. Dropped futures
end spans as cancelled; an elapsed tool/batch deadline ends unfinished dispatch
spans as timeout, while completed siblings keep their recorded outcome. Unwinding
ends spans as panic. Ordinary CLI error exits
explicitly flush within the same shutdown budget; normal completion flushes
before Tokio runtime teardown. The dedicated subagent worker's fatal hard-exit
remains immediate to preserve its termination guarantee. That exit, signals and
other abrupt termination cannot guarantee span completion or export.

There is no prompt, response, thinking, memory, workflow content, tool schema,
argument/result, private filesystem path, credential or arbitrary event capture
API in this layer. Ordinary existing logging remains separate and may have its
own data handling requirements.

## Convention version

GenAI conventions are **Development**, not a stable schema contract. Mapping
version `1` follows the official
[GenAI conventions at f3c6d0c9bb66ebd474668a3a0be25c61226f02c2](https://github.com/open-telemetry/semantic-conventions-genai/tree/f3c6d0c9bb66ebd474668a3a0be25c61226f02c2),
including [agent spans](https://github.com/open-telemetry/semantic-conventions-genai/blob/f3c6d0c9bb66ebd474668a3a0be25c61226f02c2/docs/gen-ai/gen-ai-agent-spans.md)
and [tool spans](https://github.com/open-telemetry/semantic-conventions-genai/blob/f3c6d0c9bb66ebd474668a3a0be25c61226f02c2/docs/gen-ai/gen-ai-spans.md).
The `bamboo.runtime` instrumentation scope includes the build version,
`bamboo.gen_ai.conventions_revision`, and `bamboo.telemetry.mapping_version`.
No official schema URL is invented. This privacy-limited subset intentionally
omits available free-form descriptions and unknown provider names; it does not
claim complete GenAI convention conformance. Future mapping changes need a new
mapping version and regression evidence.

## Rust hosts and propagation

Embedding hosts can call `bamboo_agent::telemetry::init` or `init_from_env` once
at startup and keep the returned guard alive until work has ended. Explicit
`guard.shutdown()` gives a fixed error if the shutdown budget expires. Enabled
initialization installs a global tracer provider; disabled initialization leaves
an embedding host's existing provider unchanged. A host that supplies its own
provider controls its own export behavior.

`TraceContext` validates, normalizes and serializes only W3C `traceparent` and
optional `tracestate`, each bounded to 512 input bytes. Unknown carrier fields
are rejected. Optional invalid tracestate is discarded according to W3C
parsing. Tracestate is opaque trace metadata; do not put business data or secrets
in it. No baggage is accepted. `TraceContext::scope(future)` restores context
per poll, and `with_current_context(future)` captures it for an explicit task
spawn; neither holds a thread-local guard across `await`.

These helpers are not automatic HTTP/MCP/remote-worker wiring. Incoming context
is untrusted correlation metadata and must be used only after transport
authentication. It is never evidence of actor identity, execution ownership,
authorization, fencing or attempt authority. That protocol is separate (#886).

## Failure budget and checks

Bamboo uses a nonblocking batch queue of 256 ended spans, batches of at most 64,
a 500 ms scheduled delay, no export retry, one-second connection/request
timeouts and a two-second shutdown wait. Overflow or collector outage drops
telemetry rather than waiting in the execution path. At most 16 attributes and
zero events/links are retained per span. Collector response bodies are not
buffered or logged by Bamboo's transport; HTTP success is not proof of durable
collector storage. A shutdown timeout can leave the bounded exporter worker
finishing in the background; it cannot delay the caller beyond its budget.

Focused checks (run with the repository's Rust 1.95 toolchain):

```sh
cargo +1.95.0 test --locked -p bamboo-infrastructure telemetry
cargo +1.95.0 test --locked -p bamboo-infrastructure --all-features telemetry
cargo +1.95.0 test --locked -p bamboo-engine --lib runtime::runner::tests
```

Tests use isolated in-memory providers and loopback TCP collectors: no process-
global test provider is installed, no remote collector is contacted. They cover
parentage, concurrent/spawned futures, finite repeat invocations, cancellation,
panic, usage presence, allowlist/path/secret sentinels, W3C validation, local
protobuf export, sampling and a stalled collector. Existing runner metrics
assertions verify trace observation does not replace durable metrics.
