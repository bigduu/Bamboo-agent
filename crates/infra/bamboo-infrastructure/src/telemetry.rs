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
pub mod local;
mod propagation;
mod timeout;

pub use config::{
    init, init_from_env, init_local_from_env, TelemetryConfig, TelemetryError, TelemetryGuard,
};
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

#[derive(Clone)]
struct SessionLifetime {
    session_id: String,
    birth: String,
    local_scope: Option<String>,
    run_id: std::sync::Arc<std::sync::Mutex<Option<String>>>,
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
        mut attributes: Vec<KeyValue>,
    ) -> Self
    where
        T::Span: Send + Sync + 'static,
    {
        let parent = Context::current();
        // Only inherit the lifetime when the child identifies the same session.
        // A propagated trace context alone is never an authorization boundary.
        if let Some(lifetime) = parent.get::<SessionLifetime>() {
            let same_session = attributes.iter().any(|attribute| {
                attribute.key.as_str() == "gen_ai.conversation.id"
                    && attribute.value == opentelemetry::Value::from(lifetime.session_id.clone())
            });
            let is_new_agent = attributes.iter().any(|attribute| {
                attribute.key.as_str() == "gen_ai.operation.name"
                    && attribute.value == opentelemetry::Value::from("invoke_agent")
            });
            // An invocation must bind its own data authority and birth; an
            // ambient same-id parent does not establish either one.
            if same_session && !is_new_agent {
                attributes.push(KeyValue::new(
                    "bamboo.session.birth",
                    lifetime.birth.clone(),
                ));
                if let Some(scope) = &lifetime.local_scope {
                    attributes.push(KeyValue::new("bamboo.local.scope", scope.clone()));
                }
            }
        }
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

    /// Bind local inspection to the exact persisted session lifetime. Child
    /// observations inherit this only when their own session id matches.
    pub fn bind_session_lifetime(
        mut self,
        session_id: &str,
        birth: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        if valid_identifier(session_id) {
            let lifetime = SessionLifetime {
                session_id: session_id.to_owned(),
                birth: birth.to_rfc3339(),
                local_scope: None,
                run_id: std::sync::Arc::new(std::sync::Mutex::new(None)),
            };
            self.context.span().set_attribute(KeyValue::new(
                "bamboo.session.birth",
                lifetime.birth.clone(),
            ));
            self.context = self.context.with_value(lifetime);
        }
        self
    }

    /// Bind local inspection to the data authority without recording its path.
    pub fn bind_local_scope(mut self, path: &std::path::Path) -> Self {
        if let Some(mut lifetime) = self.context.get::<SessionLifetime>().cloned() {
            let scope = local::scope_for_path(path);
            self.context
                .span()
                .set_attribute(KeyValue::new("bamboo.local.scope", scope.clone()));
            lifetime.local_scope = Some(scope);
            self.context = self.context.with_value(lifetime);
        }
        self
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

    /// Record the same authoritative snapshot exposed to usage observers.
    pub fn model_usage(&self, usage: &bamboo_domain::ModelTokenUsage) {
        for (key, value) in [
            ("gen_ai.usage.input_tokens", usage.input_tokens),
            ("gen_ai.usage.output_tokens", usage.output_tokens),
            ("bamboo.usage.reasoning_tokens", usage.reasoning_tokens),
            (
                "bamboo.usage.cache_read_input_tokens",
                usage.cache_read_input_tokens,
            ),
            (
                "bamboo.usage.cache_creation_input_tokens",
                usage.cache_creation_input_tokens,
            ),
            (
                "bamboo.usage.cache_write_input_tokens",
                usage.cache_write_input_tokens,
            ),
        ] {
            if let Some(value) = value.and_then(|value| i64::try_from(value).ok()) {
                self.context.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }

    pub fn model_call_id(&self, call_id: &str) {
        let mut attributes = Vec::new();
        add_identifier(&mut attributes, "bamboo.model_call.id", call_id);
        self.context.span().set_attributes(attributes);
    }

    /// Actual SDK context, absent when a non-recording/no-op tracer is in use.
    pub fn trace_ids(&self) -> Option<(String, String)> {
        let span = self.context.span();
        let context = span.span_context();
        (span.is_recording() && context.is_valid()).then(|| {
            (
                context.trace_id().to_string(),
                context.span_id().to_string(),
            )
        })
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
    if valid_identifier(run_id) {
        if let Some(lifetime) = Context::current().get::<SessionLifetime>() {
            if let Ok(mut current) = lifetime.run_id.lock() {
                *current = Some(run_id.to_owned());
            }
        }
    }
}

/// The real invocation's run identifier, when its runtime has established one.
pub fn current_run_id(session_id: &str) -> Option<String> {
    let context = Context::current();
    let lifetime = context.get::<SessionLifetime>()?;
    if lifetime.session_id != session_id {
        return None;
    }
    let run_id = lifetime.run_id.lock().ok()?.clone();
    run_id
}

/// Reject arbitrary path/content-shaped metadata at observer boundaries.
pub fn safe_identifier(value: &str) -> Option<String> {
    valid_identifier(value).then(|| value.to_owned())
}

pub fn safe_provider(value: &str) -> Option<String> {
    provider_family(value).map(str::to_owned)
}

/// Classification used when a model-call future is dropped before returning.
pub fn dropped_model_call_status() -> bamboo_domain::ModelCallStatus {
    if std::thread::panicking() || timeout::is_timeout_drop() {
        bamboo_domain::ModelCallStatus::Error
    } else {
        bamboo_domain::ModelCallStatus::Cancelled
    }
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

    /// Uses a scoped SDK tracer and the production local processor. `global`
    /// lets a server handler read the same store without changing the global
    /// OpenTelemetry provider used by unrelated tests.
    pub struct LocalTraceTest {
        provider: SdkTracerProvider,
        pub store: std::sync::Arc<super::local::LocalTraceStore>,
    }

    impl LocalTraceTest {
        pub fn isolated() -> Self {
            Self::with_store(std::sync::Arc::new(super::local::LocalTraceStore::default()))
        }

        pub fn global() -> Self {
            Self::with_store(super::local::store())
        }

        fn with_store(store: std::sync::Arc<super::local::LocalTraceStore>) -> Self {
            store.set_enabled(true);
            let provider = SdkTracerProvider::builder()
                .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                .with_span_processor(super::local::LocalProcessor(store.clone()))
                .with_resource(opentelemetry_sdk::Resource::builder_empty().build())
                .build();
            Self { provider, store }
        }

        pub fn scope<F: Future>(&self, future: F) -> opentelemetry::context::WithContext<F> {
            let tracer = self.provider.tracer_with_scope(super::scope());
            future.with_context(Context::current().with_value(ScopedTracer(tracer)))
        }
    }
}
