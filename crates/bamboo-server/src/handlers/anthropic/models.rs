use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_engine::metrics::types::ForwardStatus;
use bamboo_infrastructure::providers::anthropic::{
    api_types::{AnthropicListModelsResponse, AnthropicModel},
    format_model_display_name,
};

pub async fn get_models(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let forward_id = uuid::Uuid::new_v4().to_string();
    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "anthropic.models",
        "models",
        false,
        chrono::Utc::now(),
    );

    // Get Anthropic provider and fetch models.
    let provider = app_state.get_provider_for_endpoint("anthropic").await?;
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
            return Err(AppError::InternalError(anyhow::anyhow!(
                "Failed to fetch models: {}",
                error
            )));
        }
    };

    // Convert model IDs to Anthropic-compatible format.
    let models: Vec<AnthropicModel> = model_ids
        .into_iter()
        .map(|id| {
            let display_name = format_model_display_name(&id);
            AnthropicModel {
                model_type: "model".to_string(),
                id,
                display_name,
                created_at: "2024-01-01T00:00:00Z".to_string(), // Use a fixed timestamp.
            }
        })
        .collect();

    let first_id = models.first().map(|model| model.id.clone());
    let last_id = models.last().map(|model| model.id.clone());

    let response = AnthropicListModelsResponse {
        data: models,
        has_more: false,
        first_id,
        last_id,
    };

    Ok(HttpResponse::Ok().json(response))
}
