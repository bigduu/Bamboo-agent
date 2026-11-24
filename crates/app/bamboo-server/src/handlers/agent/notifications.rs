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
            "error": crate::error::error_value(format!(
                "Failed to persist notification preferences: {error}"
            ))
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
        id: None,
        title: "Bamboo test notification".to_string(),
        body,
        category: "custom".to_string(),
        priority: "normal".to_string(),
        session_id: "test".to_string(),
        dedup_key: None,
        created_at: None,
        click_url: None,
    };
    crate::notify_sinks::dispatch_with_hook_admission(
        &config_snapshot,
        false,
        &notification,
        state.managed_lifecycle_hook_admission(),
    );

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

    #[cfg(unix)]
    #[actix_web::test]
    async fn managed_update_refuses_after_notification_hook_is_enabled_run_and_disabled() {
        use crate::app_state::{planned_update::HostUpdateReadiness, ConfigUpdateEffects};
        use bamboo_config::{LifecycleHookGroup, LifecycleHookHandler};
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let state = web::Data::new(
            AppState::new_for_managed_update(dir.path().to_owned())
                .await
                .unwrap(),
        );
        state.wait_for_boot_reconcile_services().await;
        assert!(!state.config.read().await.lifecycle_hooks.enabled);
        let marker = dir.path().join("notification-hook-executed");
        let command = format!("printf 'notification hook ran' > '{}'", marker.display());
        state
            .update_config(
                |config| {
                    config.notifications.desktop.enabled = Some(false);
                    config.notifications.ntfy.enabled = false;
                    config.notifications.bark.enabled = false;
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();
        state
            .update_config(
                |config| {
                    config.lifecycle_hooks.enabled = true;
                    config.lifecycle_hooks.notification = vec![LifecycleHookGroup {
                        enabled: true,
                        matcher: None,
                        hooks: vec![LifecycleHookHandler::command(command, 2_000)],
                    }];
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();
        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/api/v1/notifications/test",
            web::post().to(super::send_test_notification),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/notifications/test")
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["attempted"], serde_json::json!(["command"]));
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if std::fs::read_to_string(&marker)
                    .is_ok_and(|content| content == "notification hook ran")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        state
            .update_config(
                |config| {
                    config.lifecycle_hooks.enabled = false;
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();
        assert!(!state.config.read().await.lifecycle_hooks.enabled);
        let mut preparation = state.begin_planned_update().await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match preparation.observe().await {
                    Err(error) => break error,
                    Ok(HostUpdateReadiness::Busy { .. }) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(HostUpdateReadiness::Ready) => {
                        panic!("executed notification hook has no closure proof")
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("dispatched a lifecycle hook"));
        drop(preparation);
        state.shutdown().await;
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
