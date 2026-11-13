//! Shared HTTP client construction for LLM providers.
//!
//! We centralize proxy handling here so all code paths (server handlers,
//! provider factory, auth flows) consistently respect `Config` proxy settings.

use std::time::Duration;

use crate::provider::LLMError;
use bamboo_config::Config;
use reqwest::{header::HeaderMap, Client, ClientBuilder, NoProxy, Proxy, Response};

const BAMBOO_USER_AGENT: &str = concat!("Bamboo/", env!("CARGO_PKG_VERSION"));

pub fn build_proxy(config: &Config) -> Result<Option<Proxy>, LLMError> {
    let http_proxy = config.http_proxy.trim();
    let https_proxy = config.https_proxy.trim();

    // User requested: no need to distinguish between HTTP/HTTPS. Pick a single proxy URL.
    let proxy_url = if !http_proxy.is_empty() {
        http_proxy
    } else if !https_proxy.is_empty() {
        https_proxy
    } else {
        return Ok(None);
    };

    let mut proxy = Proxy::all(proxy_url)?;
    if let Some(auth) = config.proxy_auth.as_ref() {
        proxy = proxy.basic_auth(&auth.username, &auth.password);
    }

    // Safety: never proxy loopback requests. This avoids surprising failures when users
    // run local OpenAI-compatible servers (e.g. localhost base_url) while having a
    // corporate proxy configured.
    proxy = proxy.no_proxy(NoProxy::from_string("localhost,127.0.0.1,::1"));

    Ok(Some(proxy))
}

pub fn build_http_client(config: &Config) -> Result<Client, LLMError> {
    Ok(client_builder(config)?.build()?)
}

pub(crate) fn default_http_client() -> Client {
    provider_client_builder()
        .build()
        .expect("failed to initialize the provider HTTP client")
}

pub(crate) fn build_work_text_client(config: &Config) -> Result<Client, LLMError> {
    Ok(client_builder(config)?
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?)
}

fn client_builder(config: &Config) -> Result<ClientBuilder, LLMError> {
    let mut builder = provider_client_builder();
    if let Some(proxy) = build_proxy(config)? {
        builder = builder.proxy(proxy);
    }
    Ok(builder)
}

fn provider_client_builder() -> ClientBuilder {
    // `connect_timeout` bounds only TCP/TLS connection establishment. That is
    // safe for streaming: it never kills an in-flight streaming response. We
    // deliberately do NOT set an overall `.timeout()` here — this client is
    // shared between streaming and non-streaming calls, and an overall timeout
    // would abort legitimately long streaming responses (thinking models can
    // stream for minutes). Mid-stream stalls — where the connection stays open
    // but no chunks arrive — are instead bounded by the inter-chunk idle
    // timeout in `consume_llm_stream_internal` (issue #28).
    Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .user_agent(BAMBOO_USER_AGENT)
}

fn correlation_value<'a>(headers: &'a HeaderMap, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 512)
    })
}

/// Keep upstream and gateway IDs distinct and correlate without logging raw headers.
pub(crate) fn observe_provider_response(response: &Response) {
    use crate::providers::common::log_identity;

    let headers = response.headers();
    let request_id = correlation_value(
        headers,
        &[
            "llm_provider-request-id",
            "llm_provider-x-request-id",
            "request-id",
            "x-request-id",
            "x-github-request-id",
            "x-goog-request-id",
        ],
    );
    let gateway_call_id = correlation_value(headers, &["x-litellm-call-id"]);
    if request_id.is_some() || gateway_call_id.is_some() {
        tracing::debug!(
            status = response.status().as_u16(),
            upstream_request_id_hash = request_id.map(log_identity),
            gateway_call_id_hash = gateway_call_id.map(log_identity),
            "LLM provider response headers received"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_client_sends_bamboo_user_agent() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .and(header("user-agent", BAMBOO_USER_AGENT))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let response = build_http_client(&Config::default())
            .unwrap()
            .get(format!("{}/models", server.uri()))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }

    #[test]
    fn proxy_attaches_no_proxy_loopback_list() {
        let mut cfg = Config::default();
        cfg.http_proxy = "http://proxy.example.com:8080".to_string();

        let proxy = build_proxy(&cfg).unwrap().expect("proxy");
        let dbg = format!("{proxy:?}");

        // We rely on reqwest's Debug output here because there is no public getter for the
        // internal no-proxy matcher. This still guards against accidentally dropping the
        // loopback bypass list during refactors.
        assert!(dbg.contains("localhost"));
        assert!(dbg.contains("127.0.0.1"));
        assert!(dbg.contains("::1"));
    }
}
