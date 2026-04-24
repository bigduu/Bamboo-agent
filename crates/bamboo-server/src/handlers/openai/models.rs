use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_engine::metrics::types::ForwardStatus;

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

    // Return all catalog models from all providers in provider/model format.
    let catalog = app_state.model_catalog.get_catalog().await;
    let models: Vec<Model> = catalog
        .models
        .iter()
        .map(|m| Model {
            id: format!("{}/{}", m.reference.provider, m.reference.model),
            object: "model".to_string(),
            created: 1626777600,
            owned_by: m.reference.provider.clone(),
            supported_endpoint_types: vec!["openai".to_string()],
        })
        .collect();

    app_state.metrics_service.collector().forward_completed(
        forward_id,
        chrono::Utc::now(),
        Some(200),
        ForwardStatus::Success,
        None,
        None,
    );

    Ok(HttpResponse::Ok().json(ListModelsResponse {
        success: true,
        object: "list".to_string(),
        data: models,
    }))
}
