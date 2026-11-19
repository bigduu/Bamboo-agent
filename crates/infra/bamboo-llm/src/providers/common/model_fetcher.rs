//! Shared helper for fetching model lists from provider APIs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{header::HeaderMap, Client};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::provider::{LLMError, ProviderModelInfo, Result};

const MAX_MODEL_CATALOG_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) enum ModelListFormat {
    Compatible,
    Messages,
    Gemini,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ModelInfoCache(Arc<Mutex<Option<CachedModels>>>);

#[derive(Debug)]
struct CachedModels {
    route: [u8; 32],
    expires_at: Instant,
    models: Option<Vec<ProviderModelInfo>>,
}

impl ModelInfoCache {
    pub(crate) async fn get(
        &self,
        client: &Client,
        url: &str,
        headers: HeaderMap,
        provider_name: &str,
        format: ModelListFormat,
    ) -> Result<Vec<ProviderModelInfo>> {
        let mut hash = Sha256::new();
        hash.update(url.len().to_le_bytes());
        hash.update(url.as_bytes());
        let mut ordered_headers: Vec<_> = headers.iter().collect();
        ordered_headers.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        for (name, value) in ordered_headers {
            for bytes in [name.as_str().as_bytes(), value.as_bytes()] {
                hash.update(bytes.len().to_le_bytes());
                hash.update(bytes);
            }
        }
        let route = hash.finalize().into();
        let mut cached = self.0.lock().await;
        if let Some(entry) = cached.as_ref() {
            if entry.route == route && entry.expires_at > Instant::now() {
                return entry.models.clone().ok_or_else(|| {
                    LLMError::Api("Provider model metadata is temporarily unavailable".into())
                });
            }
        }
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            fetch_models(client, url, headers, provider_name, format),
        )
        .await
        .unwrap_or_else(|_| {
            Err(LLMError::Api(
                "Provider model metadata lookup timed out".into(),
            ))
        });
        let ttl = Duration::from_secs(if result.is_ok() { 300 } else { 30 });
        *cached = Some(CachedModels {
            route,
            expires_at: Instant::now() + ttl,
            models: result.as_ref().ok().cloned(),
        });
        result
    }
}

/// Fetch a model list from a JSON endpoint.
///
/// Supports common response formats:
/// - `{ data: [{ id: "..." }, ...] }` — OpenAI, Anthropic
/// - `{ models: [{ name: "..." }, ...] }` — Gemini (strips `models/` prefix)
/// - `{ models: [{ id: "..." }, ...] }` — Generic fallback
/// - `["id1", "id2"]` — Plain string array
pub async fn fetch_model_list(
    client: &Client,
    url: &str,
    headers: HeaderMap,
    provider_name: &str,
) -> Result<Vec<String>> {
    Ok(fetch_models(
        client,
        url,
        headers,
        provider_name,
        ModelListFormat::Compatible,
    )
    .await?
    .into_iter()
    .map(|model| model.id)
    .collect())
}

async fn fetch_models(
    client: &Client,
    url: &str,
    headers: HeaderMap,
    provider_name: &str,
    format: ModelListFormat,
) -> Result<Vec<ProviderModelInfo>> {
    let mut response = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(LLMError::Http)?;
    crate::http_client::observe_provider_response(&response);

    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(LLMError::Http)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_MODEL_CATALOG_BYTES {
            return Err(LLMError::Api(
                "Provider model catalog exceeds the response limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes);
        return Err(LLMError::Api(format!(
            "{} models API error: HTTP {}: {}",
            provider_name, status, text
        )));
    }

    let json: Value = serde_json::from_slice(&bytes)?;
    Ok(parse_models(&json, format))
}

fn parse_models(json: &Value, format: ModelListFormat) -> Vec<ProviderModelInfo> {
    if let Some(data) = json.get("data").and_then(Value::as_array) {
        extract_models(data, "id", format)
    } else if let Some(models_arr) = json.get("models").and_then(|m| m.as_array()) {
        let models = extract_models(models_arr, "name", format);
        if models.is_empty() {
            extract_models(models_arr, "id", format)
        } else {
            models
        }
    } else if let Some(arr) = json.as_array() {
        arr.iter()
            .filter_map(|value| value.as_str().map(ProviderModelInfo::from_id))
            .collect()
    } else {
        vec![]
    }
}

fn extract_models(items: &[Value], key: &str, format: ModelListFormat) -> Vec<ProviderModelInfo> {
    fn limit(item: &Value, name: &str) -> Option<u32> {
        item.get(name)?
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
    }
    items
        .iter()
        .filter_map(|item| {
            let id = item.get(key)?.as_str()?;
            let id = if key == "name" {
                id.strip_prefix("models/").unwrap_or(id)
            } else {
                id
            };
            let (native_input, native_output) = match format {
                ModelListFormat::Messages => (None, limit(item, "max_tokens")),
                ModelListFormat::Gemini => (
                    limit(item, "inputTokenLimit"),
                    limit(item, "outputTokenLimit"),
                ),
                ModelListFormat::Compatible => (None, None),
            };
            Some(ProviderModelInfo {
                id: id.into(),
                max_context_tokens: limit(item, "max_context_tokens"),
                max_input_tokens: limit(item, "max_input_tokens").or(native_input),
                max_output_tokens: limit(item, "max_output_tokens").or(native_output),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn model_catalog_keeps_independent_limits_and_unknown_values() {
        let messages = parse_models(
            &json!({"data":[{"id":"messages","max_input_tokens":120_000,"max_tokens":64_000}]}),
            ModelListFormat::Messages,
        );
        assert_eq!(messages[0].max_input_tokens, Some(120_000));
        assert_eq!(messages[0].max_output_tokens, Some(64_000));
        assert_eq!(messages[0].max_context_tokens, None);

        let gemini = parse_models(
            &json!({"models":[{"name":"models/gemini","inputTokenLimit":1_000_000,
                "outputTokenLimit":65_536}]}),
            ModelListFormat::Gemini,
        );
        assert_eq!(gemini[0].id, "gemini");
        assert_eq!(gemini[0].max_input_tokens, Some(1_000_000));
        assert_eq!(gemini[0].max_output_tokens, Some(65_536));
        assert_eq!(gemini[0].max_context_tokens, None);

        let compatible = parse_models(
            &json!({"data":[{"id":"alias","max_input_tokens":null,"max_output_tokens":4294967296_u64,
                "max_context_tokens":0}]}),
            ModelListFormat::Compatible,
        );
        assert_eq!(compatible, vec![ProviderModelInfo::from_id("alias")]);
    }

    #[tokio::test]
    async fn model_catalog_cache_coalesces_reads_and_keeps_route_headers_distinct() {
        let server = MockServer::start().await;
        for (route, limit) in [("one", 4000), ("two", 8000)] {
            Mock::given(method("GET"))
                .and(path("/models"))
                .and(header("x-route", route))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data":[{"id":"same-alias","max_context_tokens":limit}]
                })))
                .expect(1)
                .mount(&server)
                .await;
        }
        let url = format!("{}/models", server.uri());
        let client = Client::builder().no_proxy().build().unwrap();
        let cache = ModelInfoCache::default();
        let fetch = |route: &'static str| {
            let mut headers = HeaderMap::new();
            headers.insert("x-route", reqwest::header::HeaderValue::from_static(route));
            cache.get(
                &client,
                &url,
                headers,
                "fixture",
                ModelListFormat::Compatible,
            )
        };
        let (first, second) = tokio::join!(fetch("one"), fetch("one"));
        assert_eq!(first.unwrap()[0].max_context_tokens, Some(4000));
        assert_eq!(second.unwrap()[0].max_context_tokens, Some(4000));
        assert_eq!(
            fetch("two").await.unwrap()[0].max_context_tokens,
            Some(8000)
        );
    }
}
