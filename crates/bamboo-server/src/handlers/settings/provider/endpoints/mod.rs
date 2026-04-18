mod get;
mod reload;
mod update;

use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::types::UpdateProviderRequest;

pub async fn get_provider_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    get::handle_get_provider_config(app_state).await
}

pub async fn update_provider_config(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateProviderRequest>,
) -> Result<HttpResponse, AppError> {
    update::handle_update_provider_config(app_state, payload).await
}

pub async fn reload_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    reload::handle_reload_provider_config(app_state).await
}
