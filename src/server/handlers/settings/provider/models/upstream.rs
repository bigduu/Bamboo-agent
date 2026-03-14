use serde_json::Value;

use crate::server::error::AppError;

pub(super) async fn fetch_models_from_api(
    client: &reqwest::Client,
    provider: &str,
    api_key: &str,
    base_url: Option<&str>,
) -> Result<Vec<String>, AppError> {
    let request = build_provider_models_request(provider, api_key, base_url)?;

    log::info!("Fetching models from: {}", request.url);

    let mut http_request = client.get(&request.url);
    if let Some((header_name, header_value)) = request.auth_header {
        http_request = http_request.header(header_name, header_value);
    }

    let response = http_request
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
    log::info!("Fetched {} models", models.len());
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
