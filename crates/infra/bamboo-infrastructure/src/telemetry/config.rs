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
            self.0.on_end(span);
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
        use opentelemetry::trace::{Span, Tracer, TracerProvider};

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
        provider.tracer("guard-test").start("invoke_agent").end();
        let mut guard = TelemetryGuard {
            provider: Some(provider),
        };
        assert!(guard.enabled());
        let started = std::time::Instant::now();
        let _ = guard.shutdown();
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!guard.enabled());
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
