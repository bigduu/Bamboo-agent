//! Shared HTTP client construction for LLM providers.
//!
//! We centralize proxy handling here so all code paths (server handlers,
//! provider factory, auth flows) consistently respect `Config` proxy settings.

use std::time::Duration;

use crate::provider::LLMError;
use bamboo_config::Config;
use reqwest::{Client, NoProxy, Proxy};

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
    // `connect_timeout` bounds only TCP/TLS connection establishment. That is
    // safe for streaming: it never kills an in-flight streaming response. We
    // deliberately do NOT set an overall `.timeout()` here — this client is
    // shared between streaming and non-streaming calls, and an overall timeout
    // would abort legitimately long streaming responses (thinking models can
    // stream for minutes). Mid-stream stalls — where the connection stays open
    // but no chunks arrive — are instead bounded by the inter-chunk idle
    // timeout in `consume_llm_stream_internal` (issue #28).
    let mut builder = Client::builder().connect_timeout(Duration::from_secs(30));
    if let Some(proxy) = build_proxy(config)? {
        builder = builder.proxy(proxy);
    }
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

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
