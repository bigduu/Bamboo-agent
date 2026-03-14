use crate::{
    core::keyword_masking::KeywordEntry,
    server::{
        app_state::{AppState, ConfigUpdateEffects},
        error::AppError,
    },
};
use actix_web::{web, HttpResponse};

use super::super::{types::KeywordMaskingResponse, validation::build_validated_config};

/// Updates keyword masking configuration.
pub async fn update_keyword_masking_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Vec<KeywordEntry>>,
) -> Result<HttpResponse, AppError> {
    let config = build_validated_config(payload.into_inner())?;

    app_state
        .update_config(
            |current| {
                current.keyword_masking = config.clone();
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: keyword masking is a UX feature and should remain configurable
                // even when the provider is not yet configured.
                reload_provider: false,
                reconcile_mcp: false,
            },
        )
        .await?;

    if let Err(error) = app_state.reload_provider().await {
        log::warn!(
            "Keyword masking updated but provider reload failed: {}",
            error
        );
    }

    Ok(HttpResponse::Ok().json(KeywordMaskingResponse::new(config.entries)))
}
