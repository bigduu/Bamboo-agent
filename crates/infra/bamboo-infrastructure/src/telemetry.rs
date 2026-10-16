//! Opt-in, content-free OpenTelemetry traces, independent of ordinary logs.
//!
//! No `tracing` subscriber bridge is installed: existing log events can contain
//! payloads and must never become exported span events. Each agent invocation is
//! finite; SQLite metrics and durable journals remain independent authorities.

use std::borrow::Cow;
use std::future::Future;

use opentelemetry::context::FutureExt;
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{global, Context, InstrumentationScope, KeyValue};

mod config;
mod propagation;
mod timeout;

pub use config::{init, init_from_env, TelemetryConfig, TelemetryError, TelemetryGuard};
pub use propagation::TraceContext;
pub use timeout::with_timeout;

/// GenAI conventions are Development. This source revision, not a floating
/// schema URL, defines our deliberately small attribute mapping.
pub const GEN_AI_CONVENTIONS_REVISION: &str = "f3c6d0c9bb66ebd474668a3a0be25c61226f02c2";
pub const INSTRUMENTATION_NAME: &str = "bamboo.runtime";
pub const MAPPING_VERSION: &str = "1";

fn scope() -> InstrumentationScope {
    InstrumentationScope::builder(INSTRUMENTATION_NAME)
        .with_version(env!("CARGO_PKG_VERSION"))
        .with_attributes([
            KeyValue::new(
                "bamboo.gen_ai.conventions_revision",
                GEN_AI_CONVENTIONS_REVISION,
            ),
            KeyValue::new("bamboo.telemetry.mapping_version", MAPPING_VERSION),
        ])
        .build()
}

/// Low-cardinality outcomes only. Never put a raw error or panic payload here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Cancelled,
    ProviderError,
    ToolError,
    Timeout,
    ContextOverflow,
    EmptyResponse,
    ExecutionError,
}

impl Outcome {
    fn error_type(self) -> Option<&'static str> {
        match self {
            Self::Success => None,
            Self::Cancelled => Some("cancelled"),
            Self::ProviderError => Some("provider_error"),
            Self::ToolError => Some("tool_error"),
            Self::Timeout => Some("timeout"),
            Self::ContextOverflow => Some("context_overflow"),
            Self::EmptyResponse => Some("empty_response"),
            Self::ExecutionError => Some("execution_error"),
        }
    }
}

/// An explicitly allowlisted span. It ends on success, failure, cancellation or
/// dropped futures, and never holds a thread-local context guard across await.
#[must_use]
pub struct Observation {
    context: Context,
    ended: bool,
}

impl Observation {
    fn start(
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
        attributes: Vec<KeyValue>,
    ) -> Self {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(tracer) = Context::current().get::<test_support::ScopedTracer>() {
            return Self::start_with(&tracer.0, name, kind, attributes);
        }
        let tracer = global::tracer_with_scope(scope());
        Self::start_with(&tracer, name, kind, attributes)
    }

    fn start_with<T: Tracer>(
        tracer: &T,
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
        attributes: Vec<KeyValue>,
    ) -> Self
    where
        T::Span: Send + Sync + 'static,
    {
        let parent = Context::current();
        let span = tracer.build_with_context(
            tracer
                .span_builder(name)
                .with_kind(kind)
                .with_attributes(attributes),
            &parent,
        );
        Self {
            context: parent.with_span(span),
            ended: false,
        }
    }

    /// One actual agent-loop invocation, not the lifetime of a persisted Session.
    pub fn agent(session_id: &str, root_session_id: &str) -> Self {
        let mut attributes = vec![KeyValue::new("gen_ai.operation.name", "invoke_agent")];
        add_identifier(&mut attributes, "gen_ai.conversation.id", session_id);
        add_identifier(&mut attributes, "bamboo.root_session.id", root_session_id);
        Self::start("invoke_agent", SpanKind::Internal, attributes)
    }

    /// One provider request, including stream consumption and internal retry.
    pub fn llm(provider_type: Option<&str>, model: &str, session_id: &str) -> Self {
        let mut attributes = vec![KeyValue::new("gen_ai.operation.name", "chat")];
        add_identifier(&mut attributes, "gen_ai.conversation.id", session_id);
        add_identifier(&mut attributes, "gen_ai.request.model", model);
        // Configured provider names are free text. Export only known families.
        if let Some(provider) = provider_type.and_then(provider_family) {
            attributes.push(KeyValue::new("gen_ai.provider.name", provider));
        }
        let name = if valid_identifier(model) {
            format!("chat {model}")
        } else {
            "chat".into()
        };
        Self::start(name, SpanKind::Client, attributes)
    }

    /// Observe only the admitted, resolved executor name, never model arguments.
    pub fn tool(name: &str, call_id: &str, session_id: &str, round_id: &str) -> Self {
        let mut attributes = vec![KeyValue::new("gen_ai.operation.name", "execute_tool")];
        add_identifier(&mut attributes, "gen_ai.tool.name", name);
        add_identifier(&mut attributes, "gen_ai.tool.call.id", call_id);
        add_identifier(&mut attributes, "gen_ai.conversation.id", session_id);
        add_identifier(&mut attributes, "bamboo.round.id", round_id);
        attributes.push(KeyValue::new("bamboo.tool.observation", "dispatch"));
        let name = if valid_identifier(name) {
            format!("execute_tool {name}")
        } else {
            "execute_tool".into()
        };
        Self::start(name, SpanKind::Internal, attributes)
    }

    /// Poll `future` with this span current. Nested and parallel futures restore
    /// the previous context after each poll; spawned tasks need explicit capture.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        future.with_context(self.context.clone()).await
    }

    /// Authoritative provider totals only. Unknown counters stay absent. Input
    /// includes cache hits; cache/reasoning subsets must not be added again.
    pub fn usage(&self, input: Option<u64>, output: Option<u64>) {
        for (key, value) in [
            ("gen_ai.usage.input_tokens", input),
            ("gen_ai.usage.output_tokens", output),
        ] {
            if let Some(value) = value.and_then(|value| i64::try_from(value).ok()) {
                self.context.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }

    pub fn finish(mut self, outcome: Outcome) {
        let span = self.context.span();
        if let Some(error_type) = outcome.error_type() {
            span.set_attribute(KeyValue::new("error.type", error_type));
            span.set_status(Status::error(error_type));
        }
        span.set_attribute(KeyValue::new(
            "bamboo.outcome",
            match outcome {
                Outcome::Success => "success",
                Outcome::Cancelled => "cancelled",
                _ => "error",
            },
        ));
        span.end();
        self.ended = true;
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if !self.ended {
            let span = self.context.span();
            let error_type = if std::thread::panicking() {
                "panic"
            } else if timeout::is_timeout_drop() {
                "timeout"
            } else {
                "cancelled"
            };
            span.set_attribute(KeyValue::new("error.type", error_type));
            span.set_attribute(KeyValue::new(
                "bamboo.outcome",
                if error_type == "timeout" {
                    "error"
                } else {
                    error_type
                },
            ));
            span.set_status(Status::error(error_type));
            span.end();
        }
    }
}

fn provider_family(value: &str) -> Option<&'static str> {
    match value {
        "openai" => Some("openai"),
        "anthropic" => Some("anthropic"),
        "gemini" | "google" => Some("gcp.gen_ai"),
        "copilot" => Some("github.copilot"),
        _ => None,
    }
}

// Reject, rather than truncate, path/URL/content-shaped metadata. These are
// identifiers, not a mechanism for exporting arbitrary user-supplied strings.
fn add_identifier(attributes: &mut Vec<KeyValue>, key: &'static str, value: &str) {
    if valid_identifier(value) {
        attributes.push(KeyValue::new(key, value.to_owned()));
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Correlate the existing runtime identity with the current invocation span.
/// These attributes report an already established identity, never create one.
pub fn record_execution_identity(execution_id: &str, run_id: &str) {
    let mut attributes = Vec::new();
    add_identifier(&mut attributes, "bamboo.execution.id", execution_id);
    add_identifier(&mut attributes, "bamboo.run.id", run_id);
    Context::current().span().set_attributes(attributes);
}

/// Carry the current context across a task-spawn boundary. This is explicit and
/// does not turn tracing data into authorization or logical actor identity.
pub fn with_current_context<F: Future>(future: F) -> opentelemetry::context::WithContext<F> {
    future.with_current_context()
}

#[cfg(test)]
mod tests;

/// Per-future test instrumentation without mutating the global provider. The
/// context is isolated across concurrently polled tests and tasks.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support {
    use super::*;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracer, SdkTracerProvider, SpanData};

    #[derive(Clone)]
    pub(super) struct ScopedTracer(pub SdkTracer);

    pub struct TraceTest {
        provider: SdkTracerProvider,
        exporter: InMemorySpanExporter,
    }

    impl Default for TraceTest {
        fn default() -> Self {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                // Tests must not inherit a machine's OTEL_TRACES_SAMPLER.
                .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                .with_simple_exporter(exporter.clone())
                .with_resource(opentelemetry_sdk::Resource::builder_empty().build())
                .build();
            Self { provider, exporter }
        }
    }

    impl TraceTest {
        pub fn scope<F: Future>(&self, future: F) -> opentelemetry::context::WithContext<F> {
            let tracer = self.provider.tracer_with_scope(super::scope());
            future.with_context(Context::current().with_value(ScopedTracer(tracer)))
        }

        pub fn spans(&self) -> Vec<SpanData> {
            self.exporter
                .get_finished_spans()
                .expect("in-memory exporter")
        }
    }
}
