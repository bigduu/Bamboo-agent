use serde_json::Value;

use bamboo_infrastructure_llm::providers::common::request_overrides;
use bamboo_infrastructure_config::RequestOverridesConfig;
use crate::error::AppError;

pub(super) async fn fetch_models_from_api(
    client: &reqwest::Client,
    provider: &str,
    api_key: &str,
    base_url: Option<&str>,
    request_overrides_cfg: Option<&RequestOverridesConfig>,
) -> Result<Vec<String>, AppError> {
    let request = build_provider_models_request(provider, api_key, base_url)?;

    tracing::info!("Fetching models from: {}", request.url);

    let mut headers = reqwest::header::HeaderMap::new();
    if let Some((header_name, header_value)) = request.auth_header {
        let normalized_name = header_name.to_ascii_lowercase();
        let parsed_name = reqwest::header::HeaderName::from_bytes(normalized_name.as_bytes())
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "Invalid models auth header name '{}': {}",
                    header_name,
                    error
                ))
            })?;
        let parsed_value =
            reqwest::header::HeaderValue::from_str(&header_value).map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "Invalid models auth header value for '{}': {}",
                    header_name,
                    error
                ))
            })?;
        headers.insert(parsed_name, parsed_value);
    }
    request_overrides::apply_overrides_to_header_map(
        &mut headers,
        request_overrides_cfg,
        request_overrides::ENDPOINT_MODELS,
        None,
    );

    let response = client
        .get(&request.url)
        .headers(headers)
        .send()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!("Request failed: {}", error)))?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalError(anyhow::anyhow!(
            "API request failed: {} - {}",
            status,
            error_text
        )));
    }

    let json: Value = response.json().await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to parse JSON: {}", error))
    })?;

    let models = extract_model_ids(json)?;
    tracing::info!("Fetched {} models", models.len());
    Ok(models)
}

#[derive(Debug, Clone)]
pub(super) struct ProviderModelsRequest {
    pub(super) url: String,
    pub(super) auth_header: Option<(&'static str, String)>,
}

pub(super) fn build_provider_models_request(
    provider: &str,
    api_key: &str,
    base_url: Option<&str>,
) -> Result<ProviderModelsRequest, AppError> {
    match provider {
        "openai" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}/models", base)
            } else {
                "https://api.openai.com/v1/models".to_string()
            };
            Ok(ProviderModelsRequest {
                url,
                auth_header: Some(("Authorization", format!("Bearer {}", api_key))),
            })
        }
        "anthropic" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}/models", base)
            } else {
                "https://api.anthropic.com/v1/models".to_string()
            };
            Ok(ProviderModelsRequest {
                url,
                auth_header: Some(("x-api-key", api_key.to_string())),
            })
        }
        "gemini" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}?key={}", base, api_key)
            } else {
                format!(
                    "https://generativelanguage.googleapis.com/v1beta/models?key={}",
                    api_key
                )
            };
            Ok(ProviderModelsRequest {
                url,
                auth_header: None,
            })
        }
        _ => Err(AppError::BadRequest(format!(
            "Unsupported provider: {}",
            provider
        ))),
    }
}

pub(super) fn extract_model_ids(json: Value) -> Result<Vec<String>, AppError> {
    if let Some(data) = json.get("data").and_then(|value| value.as_array()) {
        // Standard OpenAI format.
        return Ok(data
            .iter()
            .filter_map(|model| {
                model
                    .get("id")
                    .and_then(|id| id.as_str())
                    .map(|id| id.to_string())
            })
            .collect());
    }

    if let Some(models) = json.get("models").and_then(|value| value.as_array()) {
        // Alternative format used by Gemini.
        return Ok(models
            .iter()
            .filter_map(|model| {
                if let Some(name) = model.get("name").and_then(|name| name.as_str()) {
                    return Some(name.to_string());
                }
                if let Some(id) = model.get("id").and_then(|id| id.as_str()) {
                    return Some(id.to_string());
                }
                model.as_str().map(|value| value.to_string())
            })
            .collect());
    }

    if let Some(array) = json.as_array() {
        return Ok(array
            .iter()
            .filter_map(|value| value.as_str().map(|entry| entry.to_string()))
            .collect());
    }

    Err(AppError::InternalError(anyhow::anyhow!(
        "Unexpected response format"
    )))
}
