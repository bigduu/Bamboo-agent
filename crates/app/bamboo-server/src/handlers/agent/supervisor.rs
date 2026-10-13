//! Owner entrypoint for the persistent default Supervisor, independent of Tickets.

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;

use crate::{app_state::AppState, handlers::agent::tickets};

/// `POST /api/v1/supervisor/default` opens the same authority on every click.
/// The Storage bootstrap port verifies existing identity and preserves history;
/// this endpoint does not enable Ticket mutation, attach a scope, or start a run.
pub async fn open_default(
    state: web::Data<AppState>,
    request: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let model = {
        let config = state.config.read().await;
        // Identity can be opened before provider setup. No model is invoked;
        // normal chat admission binds a model before the first execution.
        bamboo_engine::model_config_helper::get_default_model_from_config(&config)
            .unwrap_or_default()
    };
    match SupervisorSessionService::new(state.storage.clone())
        .get_or_create_default(&model)
        .await
    {
        Ok(receipt) => Ok(HttpResponse::Ok().json(receipt)),
        Err(error) => {
            let (status, message) = match error.kind() {
                std::io::ErrorKind::AlreadyExists => (
                    StatusCode::CONFLICT,
                    "Default Supervisor identity is occupied by another session",
                ),
                _ => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Default Supervisor authority could not be verified",
                ),
            };
            tracing::warn!(%error, "Default Supervisor could not be opened");
            Ok(crate::error::json_error(status, message))
        }
    }
}

#[cfg(test)]
mod tests;
