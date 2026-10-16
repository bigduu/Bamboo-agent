use std::time::Duration;

use opentelemetry_sdk::trace::SdkTracerProvider;

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
    init(&TelemetryConfig::from_lookup(|key| {
        std::env::var(key).ok()
    })?)
}

#[cfg(feature = "otel-otlp")]
pub(super) fn build_provider(
    config: &TelemetryConfig,
) -> Result<SdkTracerProvider, TelemetryError> {
    use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
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
        .build()
        .map_err(|_| TelemetryError::ExporterInitialization)?;
    // No retry feature: one bounded attempt, then drop. A collector outage
    // must not backpressure execution or grow an unbounded queue.
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(256)
                .with_max_export_batch_size(64)
                .with_scheduled_delay(Duration::from_millis(500))
                .build(),
        )
        .build();
    Ok(SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
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
