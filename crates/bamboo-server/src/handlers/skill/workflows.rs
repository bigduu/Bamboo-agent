use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use crate::error::AppError;

use super::types::AvailableWorkflowsResponse;

/// GET /skills/available-workflows - Get available workflows
pub async fn get_available_workflows(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let workflows = crate::services::skill_service::list_workflows(&state.app_data_dir)
        .await
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!("Failed to list workflows: {}", error))
        })?;

    Ok(HttpResponse::Ok().json(AvailableWorkflowsResponse { workflows }))
}
