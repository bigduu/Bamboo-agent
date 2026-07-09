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

/// `POST /api/v1/notifications/test` — setup-UX endpoint: fires a test
/// notification through every currently ENABLED delivery channel
/// (desktop/ntfy/bark) so a user can confirm a topic/token actually works.
///
/// Deliberately bypasses `NotificationService` entirely (no master-switch or
/// per-category preference check, no dedup) — a test must fire even while the
/// user is still setting notifications up with everything toggled off — but
/// still honors per-channel enablement in `Config.notifications`: an unwired
/// channel has nothing to test. `category: "custom"` + `has_watcher: false`
/// (a manual test has no associated live session to suppress the desktop
/// popup against) means every enabled channel always fires, so `attempted`
/// exactly reflects which channels were exercised.
pub async fn send_test_notification(state: web::Data<AppState>) -> impl Responder {
    let config_snapshot = state.config.read().await.clone();
    let attempted = crate::notify_sinks::attempted_channels(&config_snapshot, false, "custom");

    let body = if attempted.is_empty() {
        "No notification channels are currently enabled in settings.".to_string()
    } else {
        format!("Enabled channels: {}.", attempted.join(", "))
    };
    let notification = crate::notify_sinks::SinkNotification {
        title: "Bamboo test notification".to_string(),
        body,
        category: "custom".to_string(),
        priority: "normal".to_string(),
        session_id: "test".to_string(),
        click_url: None,
    };
    crate::notify_sinks::dispatch_to_sinks(&config_snapshot, false, &notification);

    HttpResponse::Ok().json(serde_json::json!({ "attempted": attempted }))
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;
    use bamboo_notification::NotificationPreferences;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
    }

    #[actix_web::test]
    async fn preferences_round_trip_via_http_includes_run_fields() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let updated = NotificationPreferences {
            enabled: true,
            on_clarification: true,
            on_tool_approval: true,
            on_context_pressure: true,
            on_subagent_complete: true,
            on_background_task_complete: true,
            on_run_complete: false,
            on_run_failed: true,
        };
        let put_resp = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/api/v1/notifications/preferences")
                .set_json(&updated)
                .to_request(),
        )
        .await;
        assert_eq!(put_resp.status(), StatusCode::OK);

        let get_resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/notifications/preferences")
                .to_request(),
        )
        .await;
        assert_eq!(get_resp.status(), StatusCode::OK);
        let fetched: NotificationPreferences = test::read_body_json(get_resp).await;
        assert_eq!(fetched, updated);
        assert!(!fetched.on_run_complete);
        assert!(fetched.on_run_failed);
    }

    #[actix_web::test]
    async fn test_endpoint_reports_attempted_channels_matching_config() {
        let state = new_state().await;
        {
            // Pin every channel explicitly so this test never depends on the
            // process-wide sidecar-mode static (other tests in this binary
            // toggle it concurrently — see `notify_sinks::desktop`).
            let mut config = state.config.write().await;
            config.notifications.desktop.enabled = Some(true);
            config.notifications.ntfy.enabled = true;
            config.notifications.ntfy.topic = "t".to_string();
            config.notifications.bark.enabled = false;
        }
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/notifications/test")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(resp).await;
        let attempted: Vec<&str> = body["attempted"]
            .as_array()
            .expect("attempted should be an array")
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(attempted, vec!["desktop", "ntfy"]);
    }

    #[actix_web::test]
    async fn test_endpoint_reports_no_channels_when_everything_disabled() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.notifications.desktop.enabled = Some(false);
            config.notifications.ntfy.enabled = false;
            config.notifications.bark.enabled = false;
        }
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/notifications/test")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["attempted"].as_array().unwrap().len(), 0);
    }
}
