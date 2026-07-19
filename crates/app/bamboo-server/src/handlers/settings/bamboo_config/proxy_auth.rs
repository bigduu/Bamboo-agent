use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};

use super::types::ProxyAuthPayload;

/// Sets proxy authentication credentials
///
/// # HTTP Route
/// `POST /bamboo/proxy-auth`
///
/// # Request Body
/// ```json
/// {
///   "expected_revision": 0,
///   "username": "user",
///   "password": "pass"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Proxy auth saved and provider reloaded
/// - `500 Internal Server Error`: Failed to save or reload
///
/// # Security
/// Credentials are encrypted in the isolated credential store. `config.json`
/// retains only the stable `proxy.default.auth` reference.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/proxy-auth \
///   -H "Content-Type: application/json" \
///   -d '{"expected_revision": 0, "username": "user", "password": "pass"}'
/// ```
pub async fn set_proxy_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<ProxyAuthPayload>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    let expected_revision = payload.expected_revision();
    let auth = payload.into_proxy_auth();

    let (_, status, health) = app_state
        .update_proxy_auth_credential(
            auth,
            expected_revision,
            ConfigUpdateEffects {
                // Best-effort inside the detached post-commit convergence task:
                // setup flows often set proxy auth before provider config is complete.
                reload_provider: true,
                // Proxy auth can affect SSE-based MCP servers too.
                reconcile_mcp: true,
            },
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "credential_ref": status.credential_ref,
        "configured": status.configured,
        "source": status.source,
        "updated_at": status.updated_at,
        "revision": health.revision,
        "status": health.status,
        "source_kind": health.source,
        "last_error": health.last_error,
    })))
}

/// Gets proxy authentication status
///
/// # HTTP Route
/// `GET /bamboo/proxy-auth/status`
///
/// # Response Format
/// ```json
/// {
///   "credential_ref": "proxy.default.auth",
///   "configured": true,
///   "revision": 1,
///   "status": "healthy"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Note
/// Neither username nor password is returned; only credential and section metadata.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/proxy-auth/status
/// ```
pub async fn get_proxy_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let reference = app_state
        .config
        .read()
        .await
        .proxy_auth_credential_ref
        .clone();
    let Some(reference) = reference else {
        let (_, health) = app_state
            .credential_store
            .statuses_with_health()
            .map_err(super::credentials::map_store_read_error)?;
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "credential_ref": serde_json::Value::Null,
            "configured": false,
            "source": serde_json::Value::Null,
            "updated_at": serde_json::Value::Null,
            "revision": health.revision,
            "status": health.status,
            "source_kind": health.source,
            "last_error": health.last_error,
        })));
    };
    let (status, health) = app_state
        .credential_store
        .status_with_health(&reference)
        .map_err(super::credentials::map_store_read_error)?;
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "credential_ref": status.credential_ref,
        "configured": status.configured,
        "source": status.source,
        "updated_at": status.updated_at,
        "revision": health.revision,
        "status": health.status,
        "source_kind": health.source,
        "last_error": health.last_error,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_config::{credential_ref, CredentialSource};

    #[actix_web::test]
    async fn status_does_not_treat_an_orphan_canonical_credential_as_active_proxy_auth() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0xa4; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .credential_store
            .replace(
                credential_ref("proxy", "default", "auth").unwrap(),
                r#"{"username":"orphan","password":"secret"}"#,
                CredentialSource::User,
                0,
            )
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/proxy-auth/status", web::get().to(get_proxy_auth_status)),
        )
        .await;

        let request = test::TestRequest::get()
            .uri("/proxy-auth/status")
            .to_request();
        let response: serde_json::Value = test::call_and_read_body_json(&app, request).await;
        assert_eq!(response["credential_ref"], serde_json::Value::Null);
        assert_eq!(response["configured"], false);
        assert_eq!(response["revision"], 1);
    }

    #[actix_web::test]
    async fn proxy_auth_update_requires_revision_and_stale_write_is_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/proxy-auth", web::post().to(set_proxy_auth)),
        )
        .await;

        let set = test::TestRequest::post()
            .uri("/proxy-auth")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "username": "proxy-user",
                "password": "proxy-secret"
            }))
            .to_request();
        let response = test::call_service(&app, set).await;
        assert!(response.status().is_success());
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["revision"], 1);
        assert_eq!(body["configured"], true);
        assert_eq!(body["credential_ref"], "proxy.default.auth");
        assert!(!body.to_string().contains("proxy-secret"));
        let (committed_ref, committed_auth) = {
            let config = state.config.read().await;
            (
                config.proxy_auth_credential_ref.clone(),
                config.proxy_auth.clone(),
            )
        };
        let committed_credentials = std::fs::read(dir.path().join("credentials.json")).unwrap();

        let stale = test::TestRequest::post()
            .uri("/proxy-auth")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "username": "stale-user",
                "password": "stale-secret"
            }))
            .to_request();
        let stale = test::call_service(&app, stale).await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
        let stale_body = String::from_utf8(test::read_body(stale).await.to_vec()).unwrap();
        assert!(!stale_body.contains("stale-secret"));
        {
            let config = state.config.read().await;
            assert_eq!(config.proxy_auth_credential_ref, committed_ref);
            assert_eq!(
                config
                    .proxy_auth
                    .as_ref()
                    .map(|auth| auth.username.as_str()),
                committed_auth.as_ref().map(|auth| auth.username.as_str())
            );
            assert!(
                config
                    .proxy_auth
                    .as_ref()
                    .zip(committed_auth.as_ref())
                    .is_some_and(|(current, committed)| current.password == committed.password),
                "stale conflict changed the committed proxy password"
            );
        }
        assert_eq!(
            std::fs::read(dir.path().join("credentials.json")).unwrap(),
            committed_credentials,
            "stale conflict changed the committed credential document"
        );

        let clear = test::TestRequest::post()
            .uri("/proxy-auth")
            .set_json(serde_json::json!({"expected_revision": 1}))
            .to_request();
        let clear = test::call_service(&app, clear).await;
        assert!(clear.status().is_success());
        let body: serde_json::Value = test::read_body_json(clear).await;
        assert_eq!(body["revision"], 2);
        assert_eq!(body["configured"], false);
        assert!(state.config.read().await.proxy_auth.is_none());
    }
}
