//! Shared helper for fetching model lists from provider APIs.

use reqwest::{header::HeaderMap, Client};
use serde_json::Value;

use crate::provider::{LLMError, Result};

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
    let response = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .map_err(LLMError::Http)?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.map_err(LLMError::Http)?;
        return Err(LLMError::Api(format!(
            "{} models API error: HTTP {}: {}",
            provider_name, status, text
        )));
    }

    let json: Value = response.json().await.map_err(LLMError::Http)?;

    let models: Vec<String> = if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
        // OpenAI / Anthropic format: { data: [{ id: "..." }] }
        extract_string_field(data, "id")
    } else if let Some(models_arr) = json.get("models").and_then(|m| m.as_array()) {
        // Gemini format: { models: [{ name: "models/..." }] }
        // Try "name" first, then fall back to "id"
        let ids = extract_string_field(models_arr, "name");
        if ids.is_empty() {
            extract_string_field(models_arr, "id")
        } else {
            ids.into_iter()
                .map(|s| s.strip_prefix("models/").unwrap_or(&s).to_string())
                .collect()
        }
    } else if let Some(arr) = json.as_array() {
        // Plain array of strings
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    } else {
        vec![]
    };

    Ok(models)
}

fn extract_string_field(items: &[Value], key: &str) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| {
            item.get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}
