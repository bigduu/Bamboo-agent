use actix_web::{web, HttpResponse};
use anyhow::anyhow;
use serde_json::json;

use crate::agent::metrics::types::ForwardStatus;
use crate::server::{app_state::AppState, error::AppError};

/// List available models.
pub async fn list_models(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "gemini.models",
        "models",
        false,
        chrono::Utc::now(),
    );

    let provider = state.get_provider().await;
    let models = match provider.list_models().await {
        Ok(models) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                None,
                None,
            );
            models
        }
        Err(error) => {
            state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(format!("Failed to list models: {error}")),
            );
            return Err(AppError::InternalError(anyhow!(
                "Failed to list models: {error}"
            )));
        }
    };

    let gemini_models: Vec<_> = models
        .into_iter()
        .map(|name| {
            json!({
                "name": format!("models/{}", name),
                "displayName": name,
                "supportedGenerationMethods": [
                    "generateContent",
                    "streamGenerateContent"
                ],
            })
        })
        .collect();

    Ok(HttpResponse::Ok().json(json!({
        "models": gemini_models
    })))
}
