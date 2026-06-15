//! Notification preference handlers.
//!
//! The backend owns notification policy (see `bamboo-notification`). These
//! endpoints expose the user's notification preferences so the frontend can read
//! and update them instead of keeping them in browser-local storage.

use actix_web::{web, HttpResponse, Responder};

use crate::app_state::AppState;
use bamboo_notification::NotificationPreferences;

/// `GET /api/v1/notifications/preferences` — current notification preferences.
pub async fn get_preferences(state: web::Data<AppState>) -> impl Responder {
    HttpResponse::Ok().json(state.notification_service.preferences())
}

/// `PUT /api/v1/notifications/preferences` — replace notification preferences.
pub async fn update_preferences(
    state: web::Data<AppState>,
    body: web::Json<NotificationPreferences>,
) -> impl Responder {
    let prefs = body.into_inner();
    match state.notification_service.set_preferences(prefs.clone()) {
        Ok(()) => HttpResponse::Ok().json(prefs),
        Err(error) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to persist notification preferences: {error}")
        })),
    }
}
