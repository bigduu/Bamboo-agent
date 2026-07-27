use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};

use super::{super::credential_action::credential_status_view, types::ProxyAuthPayload};

/// Sets proxy authentication credentials
///
/// # HTTP Route
/// `POST /bamboo/proxy-auth`
///
/// # Request Body
/// ```json
/// {
///   "expected_revision": 0,
///   "action": "replace",
///   "username": "user",
///   "password": "pass"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "revision": 1,
///   "section": { "revision": 1, "data": {} },
///   "configured": true
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
///   -d '{"expected_revision":0,"action":"replace","username":"user","password":"pass"}'
/// ```
pub async fn set_proxy_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<ProxyAuthPayload>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    let expected_revision = payload.expected_revision();
    let auth = payload.into_proxy_auth().map_err(AppError::BadRequest)?;

    let (config, revision, status, credential_health, section) = app_state
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
    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "proxy mutation completed without a typed Core section envelope"
        ))
    })?;
    let credential = credential_status_view(
        config.proxy_auth_credential_ref.as_ref(),
        config.proxy_auth_credential_ref.is_some(),
        Some(&status),
        &credential_health,
    );

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "section": section,
        "credential_ref": credential.credential_ref,
        "state": credential.state,
        "configured": credential.configured,
        "source": credential.source,
        "updated_at": credential.updated_at,
        "revision": revision,
        "credential_health": credential_health.clone(),
        "credential_revision": credential_health.revision,
        "credential_status": credential_health.status,
        "credential_source_kind": credential_health.source,
        "credential_last_error": credential_health.last_error,
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
    let exact = app_state
        .read_exact_credential_section(bamboo_config::SectionId::Core)
        .await?;
    let section = exact.section;
    let reference = exact.config.proxy_auth_credential_ref.clone();
    let Some(reference) = reference else {
        let health = exact.metadata.credential_health;
        let credential = credential_status_view(None, false, None, &health);
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "section": section.clone(),
            "credential_ref": credential.credential_ref,
            "state": credential.state,
            "configured": credential.configured,
            "source": credential.source,
            "updated_at": credential.updated_at,
            "revision": section.revision,
            "status": section.status,
            "source_kind": section.source_kind,
            "source_path": section.source_path,
            "loaded_at": section.loaded_at,
            "last_error": section.last_error,
            "credential_health": health.clone(),
            "credential_revision": health.revision,
            "credential_status": health.status,
            "credential_source_kind": health.source,
            "credential_last_error": health.last_error,
        })));
    };
    let health = exact.metadata.credential_health;
    let status = exact
        .metadata
        .credential_statuses
        .iter()
        .find(|status| status.credential_ref == reference);
    let credential = credential_status_view(Some(&reference), true, status, &health);
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "section": section.clone(),
        "credential_ref": credential.credential_ref,
        "state": credential.state,
        "configured": credential.configured,
        "source": credential.source,
        "updated_at": credential.updated_at,
        "revision": section.revision,
        "status": section.status,
        "source_kind": section.source_kind,
        "source_path": section.source_path,
        "loaded_at": section.loaded_at,
        "last_error": section.last_error,
        "credential_health": health.clone(),
        "credential_revision": health.revision,
        "credential_status": health.status,
        "credential_source_kind": health.source,
        "credential_last_error": health.last_error,
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
        assert_eq!(response["state"], "missing");
        assert_eq!(response["revision"], 0);
        assert_eq!(response["credential_revision"], 1);
    }

    #[actix_web::test]
    async fn status_reports_valid_ciphertext_with_invalid_proxy_payload_as_error() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .update_proxy_auth_credential(
                Some(bamboo_config::ProxyAuth {
                    username: "proxy-user".to_string(),
                    password: "proxy-secret".to_string(),
                }),
                0,
                Default::default(),
            )
            .await
            .unwrap();
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        state
            .credential_store
            .replace(
                reference.clone(),
                r#"{"username":"","password":"still-encrypted"}"#,
                CredentialSource::User,
                1,
            )
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/proxy-auth/status", web::get().to(get_proxy_auth_status)),
        )
        .await;
        let response: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/proxy-auth/status")
                .to_request(),
        )
        .await;
        assert_eq!(response["revision"], 1);
        assert_eq!(response["credential_revision"], 2);
        assert_eq!(response["configured"], false);
        assert_eq!(response["state"], "error");

        state
            .credential_store
            .replace(
                reference,
                r#"{"username":"****...****","password":"still-encrypted"}"#,
                CredentialSource::User,
                2,
            )
            .unwrap();
        let masked: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/proxy-auth/status")
                .to_request(),
        )
        .await;
        assert_eq!(masked["credential_revision"], 3);
        assert_eq!(masked["configured"], false);
        assert_eq!(masked["state"], "error");
    }

    #[actix_web::test]
    async fn exact_status_reports_wrong_encryption_key_as_error() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_proxy_auth_credential(
                Some(bamboo_config::ProxyAuth {
                    username: "proxy-user".to_string(),
                    password: "proxy-secret".to_string(),
                }),
                0,
                Default::default(),
            )
            .await
            .unwrap();
        let data_dir = dir.path().to_path_buf();
        let exact = tokio::task::spawn_blocking(move || {
            let _wrong_key = bamboo_config::encryption::set_test_encryption_key([0xa7; 32]);
            bamboo_config::read_exact_credential_section_snapshot(
                data_dir,
                bamboo_config::SectionId::Core,
                None,
            )
        })
        .await;
        let exact = exact.unwrap().unwrap();
        assert_eq!(exact.section.revision, 1);
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        let status = exact
            .credential_statuses
            .iter()
            .find(|status| status.credential_ref == reference)
            .unwrap();
        assert!(!status.configured);
    }

    #[actix_web::test]
    async fn proxy_auth_update_requires_revision_and_stale_write_is_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let mut events = state.account_sink.subscribe();
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
                "action": "replace",
                "username": "proxy-user",
                "password": "proxy-secret"
            }))
            .to_request();
        let response = test::call_service(&app, set).await;
        assert!(response.status().is_success());
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["revision"], 1);
        assert_eq!(body["configured"], true);
        assert_eq!(body["state"], "configured");
        assert_eq!(body["credential_ref"], "proxy.default.auth");
        assert!(!body.to_string().contains("proxy-secret"));
        assert_eq!(body["section"]["revision"], 1);
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision: 1 }
                        if section == "core"
                ) {
                    break event;
                }
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            event.event,
            bamboo_agent_core::AgentEvent::ConfigChanged { ref section, revision: 1 }
                if section == "core"
        ));
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
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "action": "clear"
            }))
            .to_request();
        let clear = test::call_service(&app, clear).await;
        assert!(clear.status().is_success());
        let body: serde_json::Value = test::read_body_json(clear).await;
        assert_eq!(body["revision"], 2);
        assert_eq!(body["configured"], false);
        assert_eq!(body["state"], "missing");
        assert!(state.config.read().await.proxy_auth.is_none());
    }
}
