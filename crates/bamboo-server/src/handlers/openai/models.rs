use actix_web::{web, HttpResponse};

use bamboo_engine::metrics::types::ForwardStatus;
use crate::{app_state::AppState, error::AppError};

use super::types::{ListModelsResponse, Model};

pub async fn get_models(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.models",
        "models",
        false,
        chrono::Utc::now(),
    );

    // Get provider and fetch models.
    let provider = app_state.get_provider().await;
    let model_ids = match provider.list_models().await {
        Ok(model_ids) => {
            app_state.metrics_service.collector().forward_completed(
                forward_id.clone(),
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                None,
                None,
            );
            model_ids
        }
        Err(error) => {
            app_state.metrics_service.collector().forward_completed(
                forward_id,
                chrono::Utc::now(),
                None,
                ForwardStatus::Error,
                None,
                Some(error.to_string()),
            );
            // Check if error is related to proxy auth.
            let err_msg = error.to_string();
            if err_msg.contains("proxy") || err_msg.contains("407") {
                return Err(AppError::ProxyAuthRequired);
            }
            // Keep this endpoint best-effort so UIs/SDKs can call it even when the provider
            // is not configured yet.
            tracing::warn!("Failed to fetch models (returning empty list): {}", error);
            return Ok(HttpResponse::Ok().json(ListModelsResponse {
                success: true,
                object: "list".to_string(),
                data: vec![],
            }));
        }
    };

    // Convert model IDs to OpenAI-compatible format.
    let models: Vec<Model> = model_ids
        .into_iter()
        .map(|id| Model {
            id,
            object: "model".to_string(),
            created: 1626777600, // Stable timestamp for UI compatibility.
            owned_by: "custom".to_string(),
            supported_endpoint_types: vec!["openai".to_string(), "anthropic".to_string()],
        })
        .collect();

    let response = ListModelsResponse {
        success: true,
        object: "list".to_string(),
        data: models,
    };

    Ok(HttpResponse::Ok().json(response))
}
