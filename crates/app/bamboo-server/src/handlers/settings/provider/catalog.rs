//! Provider catalog API — aggregated view of all providers and their models.

use actix_web::{web, HttpResponse};

use bamboo_llm::model_catalog::ProviderFetchResult;

use crate::{app_state::AppState, error::AppError};

/// Return the aggregated provider catalog.
pub async fn get_provider_catalog(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let catalog = app_state.model_catalog.get_catalog().await;
    Ok(HttpResponse::Ok().json(catalog))
}

/// Fetch model lists from one or all providers.
///
/// If `provider` is specified, fetches models for that single provider.
/// If omitted, fetches models from all configured providers.
#[derive(Debug, serde::Deserialize)]
pub struct FetchModelsRequest {
    pub provider: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct FetchModelsResponse {
    pub fetched: Vec<ProviderFetchResult>,
}

pub async fn fetch_catalog_models(
    app_state: web::Data<AppState>,
    body: web::Json<FetchModelsRequest>,
) -> Result<HttpResponse, AppError> {
    let results = match &body.provider {
        Some(provider_name) => {
            let models = app_state
                .model_catalog
                .list_models_for_provider(provider_name)
                .await
                .map_err(|e: String| AppError::BadRequest(e))?;
            vec![ProviderFetchResult {
                provider: provider_name.clone(),
                models: Some(models),
                error: None,
            }]
        }
        None => {
            app_state
                .model_catalog
                .fetch_models_for_all_providers()
                .await
        }
    };

    Ok(HttpResponse::Ok().json(FetchModelsResponse { fetched: results }))
}
