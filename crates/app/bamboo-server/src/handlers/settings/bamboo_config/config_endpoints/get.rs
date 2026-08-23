use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::common::{config_file_path, redacted_config_json};

pub async fn get_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_file_path(&app_state.app_data_dir);
    if !path.exists()
        && !bamboo_config::modular_authority_boundary_present(&app_state.app_data_dir)
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error.to_string())))?
    {
        return Ok(HttpResponse::Ok().json(serde_json::json!({})));
    }

    let config = app_state.config.read().await.clone();
    Ok(HttpResponse::Ok().json(redacted_config_json(&config, &app_state.app_data_dir).await?))
}
