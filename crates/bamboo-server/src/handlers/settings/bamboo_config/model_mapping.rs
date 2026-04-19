use actix_web::{web, HttpResponse};

use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

pub async fn get_anthropic_model_mapping(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    Ok(HttpResponse::Ok().json(config.anthropic_model_mapping.clone()))
}

pub async fn set_anthropic_model_mapping(
    app_state: web::Data<AppState>,
    payload: web::Json<bamboo_infrastructure::model_mapping::AnthropicModelMapping>,
) -> Result<HttpResponse, AppError> {
    let mapping = payload.into_inner();
    app_state
        .update_config(
            |config| {
                config.anthropic_model_mapping = mapping.clone();
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;
    Ok(HttpResponse::Ok().json(mapping))
}
