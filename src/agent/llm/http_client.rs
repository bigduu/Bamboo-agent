//! Shared HTTP client construction for LLM providers.
//!
//! We centralize proxy handling here so all code paths (server handlers,
//! provider factory, auth flows) consistently respect `Config` proxy settings.

use crate::agent::llm::provider::LLMError;
use crate::core::Config;
use reqwest::{Client, Proxy};

pub(crate) fn build_proxy(config: &Config) -> Result<Option<Proxy>, LLMError> {
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

    Ok(Some(proxy))
}

pub(crate) fn build_http_client(config: &Config) -> Result<Client, LLMError> {
    let mut builder = Client::builder();
    if let Some(proxy) = build_proxy(config)? {
        builder = builder.proxy(proxy);
    }
    Ok(builder.build()?)
}
