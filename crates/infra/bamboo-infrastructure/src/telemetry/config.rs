use std::time::Duration;

use opentelemetry_sdk::trace::SdkTracerProvider;

/// Preserve the exporter sampling decision while recording unsampled spans
/// locally. The batch processor below receives sampled spans only.
#[derive(Clone, Debug)]
struct LocalSampler(opentelemetry_sdk::trace::Sampler);

impl opentelemetry_sdk::trace::ShouldSample for LocalSampler {
    fn should_sample(
        &self,
        parent: Option<&opentelemetry::Context>,
        trace_id: opentelemetry::trace::TraceId,
        name: &str,
        kind: &opentelemetry::trace::SpanKind,
        attributes: &[opentelemetry::KeyValue],
        links: &[opentelemetry::trace::Link],
    ) -> opentelemetry_sdk::trace::SamplingResult {
        use opentelemetry_sdk::trace::SamplingDecision;
        let mut result = self
            .0
            .should_sample(parent, trace_id, name, kind, attributes, links);
        if result.decision == SamplingDecision::Drop {
            result.decision = SamplingDecision::RecordOnly;
        }
        result
    }
}

#[cfg(feature = "otel-otlp")]
#[derive(Debug)]
struct SampledOnly<P>(P);

/// Process-scoped pseudonyms preserve equality without exporting free-form IDs.
/// Domain separation prevents unrelated kinds of equal input from being linked.
#[cfg(feature = "otel-otlp")]
fn export_reference(domain: &'static str, value: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use std::sync::OnceLock;

    static KEY: OnceLock<[u8; 32]> = OnceLock::new();
    let key = KEY.get_or_init(|| {
        let mut key = [0; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key
    });
    let mut digest = Hmac::<sha2::Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    digest.update(b"bamboo/otlp-reference/v1\0");
    digest.update(domain.as_bytes());
    digest.update(b"\0");
    digest.update(value.as_bytes());
    format!("ref-{}", hex::encode(digest.finalize().into_bytes()))
}

/// Build a privacy-limited export copy. Local processors receive the original
/// SpanData, as each SDK processor is called with a separate owned copy.
#[cfg(feature = "otel-otlp")]
fn sanitize_export_span(
    mut span: opentelemetry_sdk::trace::SpanData,
) -> Option<opentelemetry_sdk::trace::SpanData> {
    use opentelemetry::trace::{SpanContext, Status, TraceState};
    use opentelemetry::{KeyValue, Value};

    if span.instrumentation_scope.name() != super::INSTRUMENTATION_NAME {
        return None;
    }
    let operation: &'static str = span.attributes.iter().take(16).find_map(|attribute| {
        if attribute.key.as_str() != "gen_ai.operation.name" {
            return None;
        }
        match &attribute.value {
            Value::String(value) => match value.as_str() {
                "invoke_agent" => Some("invoke_agent"),
                "chat" => Some("chat"),
                "execute_tool" => Some("execute_tool"),
                _ => None,
            },
            _ => None,
        }
    })?;
    let mut attributes = vec![KeyValue::new("gen_ai.operation.name", operation)];
    for attribute in span.attributes.into_iter().take(16) {
        let key = attribute.key.as_str();
        let reference = match key {
            "gen_ai.conversation.id" => Some(("bamboo.session.ref", "session")),
            "bamboo.root_session.id" => Some(("bamboo.root_session.ref", "session")),
            "gen_ai.tool.call.id" => Some(("bamboo.tool_call.ref", "tool_call")),
            "bamboo.execution.id" => Some(("bamboo.execution.ref", "execution")),
            "bamboo.run.id" => Some(("bamboo.run.ref", "run")),
            "bamboo.round.id" => Some(("bamboo.round.ref", "round")),
            "bamboo.model_call.id" => Some(("bamboo.model_call.ref", "model_call")),
            "gen_ai.request.model" => Some(("bamboo.model.ref", "model")),
            "gen_ai.tool.name" => Some(("bamboo.tool.ref", "tool")),
            _ => None,
        };
        if let Some((output_key, domain)) = reference {
            if let Value::String(value) = &attribute.value {
                if super::valid_identifier(value.as_str()) {
                    attributes.push(KeyValue::new(
                        output_key,
                        export_reference(domain, value.as_str()),
                    ));
                }
            }
            continue;
        }
        let allowed = match (key, &attribute.value) {
            ("gen_ai.provider.name", Value::String(value)) => matches!(
                value.as_str(),
                "openai" | "anthropic" | "gcp.gen_ai" | "github.copilot"
            ),
            ("bamboo.tool.observation", Value::String(value)) => value.as_str() == "dispatch",
            ("bamboo.outcome", Value::String(value)) => {
                matches!(value.as_str(), "success" | "cancelled" | "error")
            }
            ("error.type", Value::String(value)) => matches!(
                value.as_str(),
                "cancelled"
                    | "provider_error"
                    | "tool_error"
                    | "timeout"
                    | "context_overflow"
                    | "empty_response"
                    | "execution_error"
                    | "panic"
            ),
            ("bamboo.session.birth", Value::String(value)) => {
                value.as_str().len() <= 64
                    && chrono::DateTime::parse_from_rfc3339(value.as_str()).is_ok()
            }
            (
                "gen_ai.usage.input_tokens"
                | "gen_ai.usage.output_tokens"
                | "bamboo.usage.reasoning_tokens"
                | "bamboo.usage.cache_read_input_tokens"
                | "bamboo.usage.cache_creation_input_tokens"
                | "bamboo.usage.cache_write_input_tokens",
                Value::I64(value),
            ) => *value >= 0,
            _ => false,
        };
        if allowed {
            attributes.push(attribute);
        }
    }
    span.attributes = attributes;
    span.name = operation.into();
    span.instrumentation_scope = super::scope();
    span.events = Default::default();
    span.links = Default::default();
    if matches!(&span.status, Status::Error { .. }) {
        span.status = Status::error("error");
    }
    let context = &span.span_context;
    span.span_context = SpanContext::new(
        context.trace_id(),
        context.span_id(),
        context.trace_flags(),
        context.is_remote(),
        TraceState::NONE,
    );
    Some(span)
}

#[cfg(feature = "otel-otlp")]
impl<P: opentelemetry_sdk::trace::SpanProcessor> opentelemetry_sdk::trace::SpanProcessor
    for SampledOnly<P>
{
    fn on_start(
        &self,
        span: &mut opentelemetry_sdk::trace::Span,
        context: &opentelemetry::Context,
    ) {
        self.0.on_start(span, context);
    }
    fn on_end(&self, span: opentelemetry_sdk::trace::SpanData) {
        if span.span_context.is_sampled() {
            if let Some(span) = sanitize_export_span(span) {
                self.0.on_end(span);
            }
        }
    }
    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.0.set_resource(resource);
    }
}

/// Explicit opt-in. Neither `RUST_LOG` nor standard `OTEL_*` variables enable
/// Bamboo export. Disabled initialization does not replace a host's provider.
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    pub enabled: bool,
    /// Full OTLP/HTTP trace URL, including `/v1/traces`. HTTPS is required except
    /// for loopback collectors. Credentials, query and fragment are forbidden.
    pub endpoint: Option<String>,
    /// Sampling probability for new roots; remote parent decisions are honored.
    pub sample_ratio: f64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            sample_ratio: 1.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TelemetryError {
    #[error("invalid Bamboo telemetry configuration")]
    InvalidConfiguration,
    #[error("OTLP export requires the otel-otlp build feature")]
    FeatureUnavailable,
    #[error("could not initialize the OTLP exporter")]
    ExporterInitialization,
    #[error("telemetry shutdown did not complete within its budget")]
    Shutdown,
}

/// Bounded best-effort lifecycle. Keep this alive until application work ends.
/// Drop flushes for at most two seconds. Abrupt process exit cannot flush.
#[derive(Default)]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    pub fn enabled(&self) -> bool {
        self.provider.is_some()
    }

    pub fn shutdown(&mut self) -> Result<(), TelemetryError> {
        if let Some(provider) = self.provider.take() {
            provider
                .shutdown_with_timeout(Duration::from_secs(2))
                .map_err(|_| TelemetryError::Shutdown)?;
        }
        Ok(())
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl TelemetryConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, TelemetryError> {
        let enabled = match get("BAMBOO_OTEL_ENABLED").as_deref() {
            None | Some("false" | "0") => false,
            Some("true" | "1") => true,
            _ => return Err(TelemetryError::InvalidConfiguration),
        };
        if !enabled {
            return Ok(Self::default());
        }
        let config = Self {
            enabled,
            endpoint: get("BAMBOO_OTEL_ENDPOINT"),
            sample_ratio: get("BAMBOO_OTEL_SAMPLE_RATIO")
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| TelemetryError::InvalidConfiguration)
                })
                .transpose()?
                .unwrap_or(1.0),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), TelemetryError> {
        if !self.sample_ratio.is_finite() || !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(TelemetryError::InvalidConfiguration);
        }
        let endpoint = self
            .endpoint
            .as_deref()
            .ok_or(TelemetryError::InvalidConfiguration)?;
        if endpoint.len() > 2048 {
            return Err(TelemetryError::InvalidConfiguration);
        }
        let url = url::Url::parse(endpoint).map_err(|_| TelemetryError::InvalidConfiguration)?;
        let loopback = match url.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host().is_none()
            || !url.path().ends_with("/v1/traces")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TelemetryError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Call once at the host's entry point. This does not install/replace a logging
/// subscriber and never enables payload recording. Initialization errors have
/// fixed text: endpoint strings and exporter internals are not reported.
pub fn init(config: &TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    if !config.enabled {
        return Ok(TelemetryGuard::default());
    }
    config.validate()?;
    #[cfg(feature = "otel-otlp")]
    {
        let provider = build_provider(config)?;
        opentelemetry::global::set_tracer_provider(provider.clone());
        Ok(TelemetryGuard {
            provider: Some(provider),
        })
    }
    #[cfg(not(feature = "otel-otlp"))]
    Err(TelemetryError::FeatureUnavailable)
}

pub fn init_from_env() -> Result<TelemetryGuard, TelemetryError> {
    let local_enabled = local_enabled_from_env()?;
    let config = TelemetryConfig::from_lookup(|key| std::env::var(key).ok())?;
    if !local_enabled {
        super::local::store().set_enabled(false);
        return init(&config);
    }
    let store = super::local::store();
    store.set_enabled(true);
    let provider = if config.enabled {
        #[cfg(feature = "otel-otlp")]
        {
            build_provider_with_local(&config, Some(store))?
        }
        #[cfg(not(feature = "otel-otlp"))]
        {
            return Err(TelemetryError::FeatureUnavailable);
        }
    } else {
        build_local_provider(store)
    };
    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(TelemetryGuard {
        provider: Some(provider),
    })
}

fn local_enabled_from_env() -> Result<bool, TelemetryError> {
    match std::env::var("BAMBOO_LOCAL_TRACES_ENABLED").ok().as_deref() {
        None | Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        _ => Err(TelemetryError::InvalidConfiguration),
    }
}

/// Preserve local inspection when optional external exporter setup fails.
pub fn init_local_from_env() -> Result<TelemetryGuard, TelemetryError> {
    if !local_enabled_from_env()? {
        super::local::store().set_enabled(false);
        return Ok(TelemetryGuard::default());
    }
    let store = super::local::store();
    store.set_enabled(true);
    let provider = build_local_provider(store);
    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(TelemetryGuard {
        provider: Some(provider),
    })
}

fn build_local_provider(store: std::sync::Arc<super::local::LocalTraceStore>) -> SdkTracerProvider {
    use opentelemetry_sdk::trace::Sampler;
    SdkTracerProvider::builder()
        .with_span_processor(super::local::LocalProcessor(store))
        .with_sampler(LocalSampler(Sampler::ParentBased(Box::new(
            Sampler::AlwaysOn,
        ))))
        .with_max_attributes_per_span(16)
        .with_max_events_per_span(0)
        .with_max_links_per_span(0)
        .with_resource(opentelemetry_sdk::Resource::builder_empty().build())
        .build()
}

#[cfg(feature = "otel-otlp")]
pub(super) fn build_provider(
    config: &TelemetryConfig,
) -> Result<SdkTracerProvider, TelemetryError> {
    build_provider_with_local(config, None)
}

#[cfg(feature = "otel-otlp")]
fn build_provider_with_local(
    config: &TelemetryConfig,
    local: Option<std::sync::Arc<super::local::LocalTraceStore>>,
) -> Result<SdkTracerProvider, TelemetryError> {
    use opentelemetry_otlp::{Protocol, RetryPolicy, WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler};
    use opentelemetry_sdk::Resource;

    config.validate()?;
    // Build the blocking client off-runtime, without the exporter's unbounded
    // fallback client. Never redirect trace bytes to another destination.
    let client = std::thread::Builder::new()
        .name("bamboo-otel-init".into())
        .spawn(|| {
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(1))
                .connect_timeout(Duration::from_secs(1))
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
        })
        .map_err(|_| TelemetryError::ExporterInitialization)?
        .join()
        .map_err(|_| TelemetryError::ExporterInitialization)?
        .map_err(|_| TelemetryError::ExporterInitialization)?;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_http_client(AllowlistedHttpClient { client })
        .with_endpoint(
            config
                .endpoint
                .as_deref()
                .ok_or(TelemetryError::InvalidConfiguration)?,
        )
        .with_timeout(Duration::from_secs(1))
        .with_retry_policy(RetryPolicy::disabled())
        .build()
        .map_err(|_| TelemetryError::ExporterInitialization)?;
    // Retries are disabled explicitly: one bounded attempt, then drop. Collector
    // outages must not backpressure execution or grow an unbounded queue.
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(256)
                .with_max_export_batch_size(64)
                .with_scheduled_delay(Duration::from_millis(500))
                .build(),
        )
        .build();
    let local_enabled = local.is_some();
    let mut builder = SdkTracerProvider::builder().with_span_processor(SampledOnly(processor));
    if let Some(store) = local {
        builder = builder.with_span_processor(super::local::LocalProcessor(store));
    }
    let sampling = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(config.sample_ratio)));
    let sampler: Box<dyn opentelemetry_sdk::trace::ShouldSample> = if local_enabled {
        Box::new(LocalSampler(sampling))
    } else {
        Box::new(sampling)
    };
    Ok(builder
        .with_sampler(sampler)
        .with_max_attributes_per_span(16)
        .with_max_events_per_span(0)
        .with_max_links_per_span(0)
        // No environment/process/host detector: resource metadata is allowlisted.
        .with_resource(
            Resource::builder_empty()
                .with_service_name("bamboo")
                .with_attributes([opentelemetry::KeyValue::new(
                    "service.version",
                    env!("CARGO_PKG_VERSION"),
                )])
                .build(),
        )
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "otel-otlp")]
    #[derive(Clone, Debug, Default)]
    struct CapturedSpans(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);

    #[cfg(feature = "otel-otlp")]
    impl opentelemetry_sdk::trace::SpanProcessor for CapturedSpans {
        fn on_start(
            &self,
            _span: &mut opentelemetry_sdk::trace::Span,
            _context: &opentelemetry::Context,
        ) {
        }

        fn on_end(&self, span: opentelemetry_sdk::trace::SpanData) {
            self.0.lock().unwrap().push(span);
        }

        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(
            &self,
            _timeout: Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
    }

    #[cfg(feature = "otel-otlp")]
    #[test]
    fn exported_references_hide_untrusted_ids_and_preserve_local_correlation() {
        use opentelemetry::trace::{
            SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
            TraceState, TracerProvider,
        };
        use opentelemetry::{Context, InstrumentationScope, KeyValue, Value};
        use std::sync::Arc;

        let local_capture = CapturedSpans::default();
        let export_capture = CapturedSpans::default();
        let store = Arc::new(super::super::local::LocalTraceStore::default());
        store.set_enabled(true);
        let provider = SdkTracerProvider::builder()
            .with_span_processor(SampledOnly(export_capture.clone()))
            .with_span_processor(local_capture.clone())
            .with_span_processor(super::super::local::LocalProcessor(store.clone()))
            .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
            .build();
        let scope = InstrumentationScope::builder(super::super::INSTRUMENTATION_NAME)
            .with_attributes([KeyValue::new("untrusted.scope", "sk_SYNTHETIC_SCOPE")])
            .build();
        let tracer = provider.tracer_with_scope(scope);
        let session = "sk_SYNTHETIC_SESSION";
        let model = "sk_SYNTHETIC_MODEL";
        let tool = "sk_SYNTHETIC_TOOL";
        let call = "sk_SYNTHETIC_CALL";
        let birth = chrono::Utc::now();
        let remote_parent = SpanContext::new(
            TraceId::from_hex("0123456789abcdef0123456789abcdef").unwrap(),
            SpanId::from_hex("0123456789abcdef").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::from_key_value([("vendor", "sk_SYNTHETIC_STATE")]).unwrap(),
        );
        let remote_guard = Context::new()
            .with_remote_span_context(remote_parent)
            .attach();
        let root = super::super::Observation::start_with(
            &tracer,
            "invoke_agent",
            SpanKind::Internal,
            vec![
                KeyValue::new("gen_ai.operation.name", "invoke_agent"),
                KeyValue::new("gen_ai.conversation.id", session),
                KeyValue::new("bamboo.root_session.id", session),
                KeyValue::new("bamboo.execution.id", "sk_SYNTHETIC_EXECUTION"),
                KeyValue::new("bamboo.run.id", "sk_SYNTHETIC_RUN"),
            ],
        )
        .bind_session_lifetime(session, birth);
        let root_ids = root.trace_ids().unwrap();
        let root_guard = root.context.clone().attach();
        let llm = super::super::Observation::start_with(
            &tracer,
            format!("chat {model}"),
            SpanKind::Client,
            vec![
                KeyValue::new("gen_ai.operation.name", "chat"),
                KeyValue::new("gen_ai.conversation.id", session),
                KeyValue::new("gen_ai.request.model", model),
                KeyValue::new("bamboo.model_call.id", "sk_SYNTHETIC_MODEL_CALL"),
                KeyValue::new("gen_ai.usage.input_tokens", 100_i64),
                KeyValue::new("bamboo.usage.cache_read_input_tokens", 0_i64),
            ],
        );
        llm.context
            .span()
            .set_attribute(KeyValue::new("arbitrary.secret", "sk_SYNTHETIC_EXTRA"));
        llm.context
            .span()
            .set_attribute(KeyValue::new("gen_ai.tool.call.id", 123_i64));
        llm.context
            .span()
            .set_attribute(KeyValue::new("gen_ai.request.model", "x".repeat(129)));
        llm.context.span().add_event("sk_SYNTHETIC_EVENT", vec![]);
        llm.context
            .span()
            .set_status(Status::error("sk_SYNTHETIC_ERROR"));
        llm.finish(super::super::Outcome::Success);
        for _ in 0..2 {
            super::super::Observation::start_with(
                &tracer,
                format!("execute_tool {tool}"),
                SpanKind::Internal,
                vec![
                    KeyValue::new("gen_ai.operation.name", "execute_tool"),
                    KeyValue::new("gen_ai.conversation.id", session),
                    KeyValue::new("gen_ai.tool.name", tool),
                    KeyValue::new("gen_ai.tool.call.id", call),
                    KeyValue::new("bamboo.round.id", "sk_SYNTHETIC_ROUND"),
                ],
            )
            .finish(super::super::Outcome::Success);
        }
        drop(root_guard);
        root.finish(super::super::Outcome::Success);
        drop(remote_guard);
        let foreign = provider.tracer("foreign.tracer");
        super::super::Observation::start_with(
            &foreign,
            "sk_SYNTHETIC_FOREIGN",
            SpanKind::Internal,
            vec![KeyValue::new("gen_ai.operation.name", "chat")],
        )
        .finish(super::super::Outcome::Success);
        provider.force_flush().unwrap();
        let original = local_capture.0.lock().unwrap().clone();
        let exported = export_capture.0.lock().unwrap().clone();
        assert_eq!(original.len(), 5);
        assert_eq!(exported.len(), 4);
        assert!(format!("{original:?}").contains(session));
        assert!(!format!("{exported:?}").contains("sk_SYNTHETIC_"));
        let get = |span: &opentelemetry_sdk::trace::SpanData, key: &str| {
            span.attributes
                .iter()
                .find(|item| item.key.as_str() == key)
                .map(|item| item.value.clone())
        };
        let root = exported
            .iter()
            .find(|span| span.name == "invoke_agent")
            .unwrap();
        let session_ref = get(root, "bamboo.session.ref").unwrap();
        assert_eq!(
            get(root, "bamboo.root_session.ref"),
            Some(session_ref.clone())
        );
        let tools: Vec<_> = exported
            .iter()
            .filter(|span| span.name == "execute_tool")
            .collect();
        assert_eq!(tools.len(), 2);
        assert_eq!(
            get(tools[0], "bamboo.session.ref"),
            Some(session_ref.clone())
        );
        assert_eq!(
            get(tools[0], "bamboo.tool_call.ref"),
            get(tools[1], "bamboo.tool_call.ref")
        );
        assert_ne!(get(tools[0], "bamboo.tool_call.ref"), Some(session_ref));
        let model_span = exported.iter().find(|span| span.name == "chat").unwrap();
        assert_eq!(
            get(model_span, "bamboo.usage.cache_read_input_tokens"),
            Some(Value::I64(0))
        );
        assert!(get(model_span, "gen_ai.request.model").is_none());
        assert!(get(model_span, "bamboo.tool_call.ref").is_none());
        assert_eq!(
            model_span
                .attributes
                .iter()
                .filter(|item| item.key.as_str() == "bamboo.model.ref")
                .count(),
            1
        );
        assert!(get(model_span, "arbitrary.secret").is_none());
        assert!(model_span.events.is_empty());
        assert_eq!(model_span.status, Status::error("error"));
        for span in &exported {
            let raw = original
                .iter()
                .find(|item| item.span_context.span_id() == span.span_context.span_id())
                .unwrap();
            assert_eq!(span.span_context.trace_id(), raw.span_context.trace_id());
            assert_eq!(
                span.span_context.trace_flags(),
                raw.span_context.trace_flags()
            );
            assert_eq!(span.span_context.is_remote(), raw.span_context.is_remote());
            assert_eq!(span.parent_span_id, raw.parent_span_id);
            assert_eq!(span.parent_span_is_remote, raw.parent_span_is_remote);
            assert!(!raw.span_context.trace_state().header().is_empty());
            assert!(span.span_context.trace_state().header().is_empty());
        }
        let page = store.page(session, birth, 10, None);
        assert_eq!(page.traces.len(), 1);
        assert_eq!(page.traces[0].trace_id, root_ids.0);
        let detail = store.detail(session, birth, &root_ids.0).unwrap();
        assert!(detail
            .spans
            .iter()
            .any(|span| span.model.as_deref() == Some(model)));
        assert!(detail.spans.iter().any(|span| span.name == tool));
    }

    #[cfg(feature = "otel-otlp")]
    #[test]
    fn export_references_are_concurrent_and_domain_separated() {
        let value = "sk_SYNTHETIC_CORRELATION";
        let expected = export_reference("session", value);
        assert_ne!(expected, value);
        assert_ne!(expected, export_reference("tool_call", value));
        assert_ne!(expected, export_reference("session", "sk_SYNTHETIC_OTHER"));
        let threads: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(move || export_reference("session", value)))
            .collect();
        for thread in threads {
            assert_eq!(thread.join().unwrap(), expected);
        }
    }

    #[cfg(feature = "otel-otlp")]
    #[test]
    fn zero_export_sampling_keeps_local_spans_without_sending_them() {
        use opentelemetry::trace::{SpanKind, TracerProvider};
        use opentelemetry::KeyValue;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let store = std::sync::Arc::new(super::super::local::LocalTraceStore::default());
        store.set_enabled(true);
        let birth = chrono::Utc::now();
        let provider = build_provider_with_local(
            &TelemetryConfig {
                enabled: true,
                endpoint: Some(format!(
                    "http://{}/v1/traces",
                    listener.local_addr().unwrap()
                )),
                sample_ratio: 0.0,
            },
            Some(store.clone()),
        )
        .unwrap();
        let tracer = provider.tracer_with_scope(super::super::scope());
        super::super::Observation::start_with(
            &tracer,
            "invoke_agent",
            SpanKind::Internal,
            vec![
                KeyValue::new("gen_ai.operation.name", "invoke_agent"),
                KeyValue::new("gen_ai.conversation.id", "sample-zero"),
            ],
        )
        .bind_session_lifetime("sample-zero", birth)
        .finish(super::super::Outcome::Success);
        assert_eq!(store.page("sample-zero", birth, 10, None).traces.len(), 1);
        provider.force_flush().unwrap();
        provider
            .shutdown_with_timeout(Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn disabled_ignores_endpoint_sampler_and_does_not_start_provider() {
        let config = TelemetryConfig::from_lookup(|key| match key {
            "BAMBOO_OTEL_ENABLED" => None,
            _ => panic!("disabled must not inspect exporter settings"),
        })
        .unwrap();
        assert!(!init(&config).unwrap().enabled());
        let config = TelemetryConfig {
            endpoint: Some("secret".into()),
            sample_ratio: f64::NAN,
            ..config
        };
        assert!(!init(&config).unwrap().enabled());
    }

    #[test]
    fn env_enable_is_explicit_and_sample_ratio_is_validated() {
        for enable in ["yes", "TRUE", "", "on"] {
            assert_eq!(
                TelemetryConfig::from_lookup(
                    |key| (key == "BAMBOO_OTEL_ENABLED").then(|| enable.to_owned())
                )
                .unwrap_err(),
                TelemetryError::InvalidConfiguration
            );
        }
        let config = TelemetryConfig::from_lookup(|key| match key {
            "BAMBOO_OTEL_ENABLED" => Some("true".into()),
            "BAMBOO_OTEL_ENDPOINT" => Some("http://localhost:4318/v1/traces".into()),
            "BAMBOO_OTEL_SAMPLE_RATIO" => Some("0.25".into()),
            _ => None,
        })
        .unwrap();
        assert!(config.enabled);
        assert_eq!(config.sample_ratio, 0.25);
    }

    #[cfg(not(feature = "otel-otlp"))]
    #[test]
    fn explicit_enable_without_transport_feature_fails_without_starting_exporter() {
        let config = TelemetryConfig {
            enabled: true,
            endpoint: Some("http://127.0.0.1:4318/v1/traces".into()),
            ..Default::default()
        };
        assert!(matches!(
            init(&config),
            Err(TelemetryError::FeatureUnavailable)
        ));
    }

    #[test]
    fn explicit_enable_requires_safe_endpoint_and_bounded_sampling() {
        for endpoint in [
            "",
            "file:///tmp/secret",
            "http://collector.example/v1/traces",
            "https://user:secret@example.com/v1/traces",
            "https://example.com/v1/traces?key=secret",
            "https://example.com/#secret",
        ] {
            let config = TelemetryConfig {
                enabled: true,
                endpoint: Some(endpoint.into()),
                ..Default::default()
            };
            assert_eq!(config.validate(), Err(TelemetryError::InvalidConfiguration));
        }
        for endpoint in [
            "http://127.0.0.1:4318/v1/traces",
            "http://[::1]:4318/v1/traces",
            "https://collector.example/v1/traces",
        ] {
            let mut config = TelemetryConfig {
                enabled: true,
                endpoint: Some(endpoint.into()),
                ..Default::default()
            };
            assert!(config.validate().is_ok());
            for ratio in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
                config.sample_ratio = ratio;
                assert!(config.validate().is_err());
            }
        }
    }

    #[test]
    fn endpoint_requires_trace_path_and_allows_reverse_proxy_prefix() {
        for endpoint in [
            "https://collector.example",
            "https://collector.example/",
            "http://127.0.0.1:4318",
            "https://collector.example/v1/metrics",
            "https://collector.example/v1/traces/",
            "https://collector.example/v1/traces/extra",
            "https://collector.example/prefixv1/traces",
            "https://collector.example/v1%2Ftraces",
        ] {
            let config = TelemetryConfig {
                enabled: true,
                endpoint: Some(endpoint.into()),
                ..Default::default()
            };
            assert_eq!(config.validate(), Err(TelemetryError::InvalidConfiguration));
        }
        for endpoint in [
            "https://collector.example/v1/traces",
            "https://collector.example/otel/tenant/v1/traces",
            "http://127.0.0.1:4318/otel/v1/traces",
        ] {
            let config = TelemetryConfig {
                enabled: true,
                endpoint: Some(endpoint.into()),
                ..Default::default()
            };
            assert_eq!(config.validate(), Ok(()));
        }
    }

    #[cfg(feature = "otel-otlp")]
    #[test]
    fn explicit_guard_shutdown_is_bounded_and_idempotent() {
        use opentelemetry::trace::{SpanKind, TracerProvider};
        use opentelemetry::KeyValue;

        // Keep the collector socket open without responding. Exercise the
        // exact guard API used before CLI process::exit, without installing a
        // process-global provider in this parallel test suite.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let provider = build_provider(&TelemetryConfig {
            enabled: true,
            endpoint: Some(format!(
                "http://{}/v1/traces",
                listener.local_addr().unwrap()
            )),
            sample_ratio: 1.0,
        })
        .unwrap();
        let tracer = provider.tracer_with_scope(super::super::scope());
        super::super::Observation::start_with(
            &tracer,
            "invoke_agent",
            SpanKind::Internal,
            vec![KeyValue::new("gen_ai.operation.name", "invoke_agent")],
        )
        .finish(super::super::Outcome::Success);
        let mut guard = TelemetryGuard {
            provider: Some(provider),
        };
        assert!(guard.enabled());
        let started = std::time::Instant::now();
        let _ = guard.shutdown();
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!guard.enabled());
        listener.set_nonblocking(true).unwrap();
        assert!(
            listener.accept().is_ok(),
            "shutdown never attempted the queued export"
        );
        // Drop after explicit shutdown must not wait on the exporter again.
        guard.shutdown().unwrap();
        drop(guard);
        assert!(started.elapsed() < Duration::from_secs(3));
    }
}

/// The upstream exporter merges OTEL_* headers even after programmatic config.
/// Keep that ambient credential channel out of this deliberately unauthenticated
/// transport. Only the protobuf payload crosses the configured destination.
#[cfg(feature = "otel-otlp")]
#[derive(Debug)]
struct AllowlistedHttpClient {
    client: reqwest::blocking::Client,
}

#[cfg(feature = "otel-otlp")]
#[async_trait::async_trait]
impl opentelemetry_http::HttpClient for AllowlistedHttpClient {
    async fn send_bytes(
        &self,
        request: opentelemetry_http::Request<opentelemetry_http::Bytes>,
    ) -> Result<
        opentelemetry_http::Response<opentelemetry_http::Bytes>,
        opentelemetry_http::HttpError,
    > {
        // SDK calls this only on its dedicated batch-export thread. Do not
        // forward arbitrary headers, including authentication or trace baggage.
        let response = self
            .client
            .post(request.uri().to_string())
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .body(request.into_body())
            .send()
            .map_err(|_| std::io::Error::other("telemetry transport failed"))?;
        let status = response.status();
        // A collector response is not application data. The exporter only
        // needs the status for delivery; never buffer/echo an unbounded body.
        Ok(opentelemetry_http::Response::builder()
            .status(status)
            .body(opentelemetry_http::Bytes::new())?)
    }
}
