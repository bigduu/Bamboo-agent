//! Shared HTTP client construction for LLM providers.
//!
//! We centralize proxy handling here so all code paths (server handlers,
//! provider factory, auth flows) consistently respect `Config` proxy settings.

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
    let mut builder = Client::builder();
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
        let cfg = Config {
            http_proxy: "http://proxy.example.com:8080".to_string(),
            ..Default::default()
        };

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
