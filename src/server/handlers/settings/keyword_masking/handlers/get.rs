use crate::server::{app_state::AppState, error::AppError};
use actix_web::{web, HttpResponse};

use super::super::types::KeywordMaskingResponse;

/// Gets keyword masking configuration.
pub async fn get_keyword_masking_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    Ok(HttpResponse::Ok().json(KeywordMaskingResponse::new(
        config.keyword_masking.entries.clone(),
    )))
}
