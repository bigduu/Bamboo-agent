use actix_web::{web, HttpResponse};
use bamboo_config::{
    ConfigStoreError, CredentialRef, CredentialSource, CredentialStoreHealth, SectionSourceKind,
    SectionStatus,
};
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, error::AppError};
use bamboo_agent_core::AgentEvent;

#[derive(Debug, Serialize)]
struct CredentialEnvelope<T> {
    data: T,
    revision: u64,
    status: SectionStatus,
    source: SectionSourceKind,
    last_error: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceCredentialRequest {
    pub expected_revision: u64,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct ClearCredentialRequest {
    pub expected_revision: u64,
}

pub async fn list_credentials(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let (statuses, health) = app_state
        .credential_store
        .statuses_with_health()
        .map_err(map_store_read_error)?;
    Ok(HttpResponse::Ok().json(envelope(statuses, health)))
}

pub async fn get_credential_status(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = parse_credential_ref(path.into_inner())?;
    let (status, health) = app_state
        .credential_store
        .status_with_health(&credential_ref)
        .map_err(map_store_read_error)?;
    Ok(HttpResponse::Ok().json(envelope(status, health)))
}

pub async fn replace_credential(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ReplaceCredentialRequest>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = parse_credential_ref(path.into_inner())?;
    let _io = app_state.config_io_lock.lock().await;
    reject_managed_credential_ref(&app_state, &credential_ref).await?;
    let (revision, status) = app_state
        .credential_store
        .replace(
            credential_ref,
            &payload.value,
            CredentialSource::User,
            payload.expected_revision,
        )
        .map_err(map_store_mutation_error)?;
    publish_credential_event(&app_state, revision);
    Ok(HttpResponse::Ok().json(envelope(status, CredentialStoreHealth::committed(revision))))
}

pub async fn clear_credential(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ClearCredentialRequest>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = parse_credential_ref(path.into_inner())?;
    let _io = app_state.config_io_lock.lock().await;
    reject_managed_credential_ref(&app_state, &credential_ref).await?;
    let (revision, status) = app_state
        .credential_store
        .clear(&credential_ref, payload.expected_revision)
        .map_err(map_store_mutation_error)?;
    publish_credential_event(&app_state, revision);
    Ok(HttpResponse::Ok().json(envelope(status, CredentialStoreHealth::committed(revision))))
}

pub async fn get_live_config_health(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let providers = app_state
        .config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let mcp = app_state
        .mcp_config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "providers": providers,
        "mcp": mcp,
    })))
}

fn envelope<T>(data: T, health: CredentialStoreHealth) -> CredentialEnvelope<T> {
    CredentialEnvelope {
        data,
        revision: health.revision,
        status: health.status,
        source: health.source,
        last_error: health.last_error,
    }
}

fn publish_credential_event(app_state: &AppState, revision: u64) {
    app_state.account_sink.record(
        None,
        &AgentEvent::ConfigChanged {
            section: "credentials".to_string(),
            revision,
        },
    );
}

fn parse_credential_ref(value: String) -> Result<CredentialRef, AppError> {
    CredentialRef::parse(value)
        .map_err(|_| AppError::BadRequest("invalid credential reference".to_string()))
}

async fn reject_managed_credential_ref(
    app_state: &AppState,
    credential_ref: &CredentialRef,
) -> Result<(), AppError> {
    let config = app_state.config.read().await;
    if config.proxy_auth_credential_ref.as_ref() == Some(credential_ref) {
        return Err(AppError::BadRequest(
            "active proxy credentials must be changed through the revisioned proxy-auth API"
                .to_string(),
        ));
    }
    if config
        .env_vars
        .iter()
        .any(|entry| entry.credential_ref.as_ref() == Some(credential_ref))
    {
        return Err(AppError::BadRequest(
            "env credentials must be changed through the revisioned env-vars API".to_string(),
        ));
    }
    if config.notifications.ntfy.credential_ref.as_ref() == Some(credential_ref)
        || config.notifications.bark.credential_ref.as_ref() == Some(credential_ref)
    {
        return Err(AppError::BadRequest(
            "notification credentials must be changed through the revisioned notification config API"
                .to_string(),
        ));
    }
    Ok(())
}

fn map_store_mutation_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        ConfigStoreError::Validation(message) if message.starts_with("credential value ") => {
            AppError::BadRequest(message)
        }
        other => map_store_read_error(other),
    }
}

pub(super) fn map_store_read_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        ConfigStoreError::Validation(_) => {
            AppError::InternalError(anyhow::anyhow!("credential store validation failed"))
        }
        ConfigStoreError::Json(_) => {
            AppError::InternalError(anyhow::anyhow!("credential store document is invalid"))
        }
        ConfigStoreError::Io(error) => AppError::StorageError(error),
        ConfigStoreError::Watch(error) => {
            AppError::InternalError(anyhow::anyhow!("credential store watch failed: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_config::CredentialStatus;
    use std::time::Duration;

    #[::core::prelude::v1::test]
    fn envelope_serialization_contains_metadata_but_no_secret_slot() {
        let status = CredentialStatus {
            credential_ref: CredentialRef::parse("provider.openai.api_key").unwrap(),
            configured: true,
            source: CredentialSource::User,
            updated_at: None,
        };
        let value =
            serde_json::to_value(envelope(status, CredentialStoreHealth::committed(4))).unwrap();
        assert_eq!(value["revision"], 4);
        assert_eq!(value["status"], "healthy");
        assert!(value["data"].get("value").is_none());
        assert!(value["data"].get("secret").is_none());
    }

    #[actix_web::test]
    async fn replace_is_redacted_stale_cas_is_409_and_feed_receives_change() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x41; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route(
                    "/credentials/{credential_ref}",
                    web::put().to(replace_credential),
                )
                .route(
                    "/credentials/{credential_ref}",
                    web::get().to(get_credential_status),
                ),
        )
        .await;

        let replace = test::TestRequest::put()
            .uri("/credentials/provider.openai.api_key")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "value": "sk-never-return-this"
            }))
            .to_request();
        let response = test::call_service(&app, replace).await;
        assert!(response.status().is_success());
        let bytes = test::read_body(response).await;
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!body.contains("sk-never-return-this"));
        assert!(body.contains("\"revision\":1"));

        let changed = tokio::time::timeout(Duration::from_secs(2), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            changed.event,
            AgentEvent::ConfigChanged { ref section, revision: 1 }
                if section == "credentials"
        ));

        let stale = test::TestRequest::put()
            .uri("/credentials/provider.openai.api_key")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "value": "stale"
            }))
            .to_request();
        let stale_response = test::call_service(&app, stale).await;
        assert_eq!(
            stale_response.status(),
            actix_web::http::StatusCode::CONFLICT
        );
        let stale_body: serde_json::Value = test::read_body_json(stale_response).await;
        assert_eq!(stale_body["error"]["code"], "config_revision_conflict");

        let forged_source = test::TestRequest::put()
            .uri("/credentials/provider.openai.api_key")
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "value": "replacement",
                "source": "migrated"
            }))
            .to_request();
        let forged_response = test::call_service(&app, forged_source).await;
        assert_eq!(
            forged_response.status(),
            actix_web::http::StatusCode::BAD_REQUEST
        );

        let get = test::TestRequest::get()
            .uri("/credentials/provider.openai.api_key")
            .to_request();
        let response = test::call_service(&app, get).await;
        let bytes = test::read_body(response).await;
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!body.contains("sk-never-return-this"));
        assert!(body.contains("\"configured\":true"));
    }

    #[actix_web::test]
    async fn status_api_reports_missing_and_backup_recovery_truthfully() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x42; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/credentials/{credential_ref}",
            web::get().to(get_credential_status),
        ))
        .await;

        let missing = test::TestRequest::get()
            .uri("/credentials/provider.openai.api_key")
            .to_request();
        let missing: serde_json::Value = test::call_and_read_body_json(&app, missing).await;
        assert_eq!(missing["revision"], 0);
        assert_eq!(missing["status"], "missing");
        assert_eq!(missing["source"], "default");

        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        state
            .credential_store
            .replace(reference.clone(), "first", CredentialSource::User, 0)
            .unwrap();
        state
            .credential_store
            .replace(reference, "second", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(state.credential_store.path(), b"{corrupt").unwrap();

        let recovered = test::TestRequest::get()
            .uri("/credentials/provider.openai.api_key")
            .to_request();
        let recovered: serde_json::Value = test::call_and_read_body_json(&app, recovered).await;
        assert_eq!(recovered["revision"], 1);
        assert_eq!(recovered["status"], "degraded");
        assert_eq!(recovered["source"], "backup");
        assert_eq!(
            recovered["last_error"],
            "primary credential document invalid; using last-known-good backup"
        );
        let serialized = recovered.to_string();
        assert!(!serialized.contains("first"));
        assert!(!serialized.contains("second"));
    }

    #[actix_web::test]
    async fn corrupt_store_without_backup_is_redacted_server_error() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        std::fs::write(state.credential_store.path(), b"{private-corrupt-bytes").unwrap();
        let app = test::init_service(App::new().app_data(state).route(
            "/credentials/{credential_ref}",
            web::get().to(get_credential_status),
        ))
        .await;
        let request = test::TestRequest::get()
            .uri("/credentials/provider.openai.api_key")
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(!body.contains("private-corrupt-bytes"));
        assert!(!body.contains(dir.path().to_string_lossy().as_ref()));
    }

    #[actix_web::test]
    async fn generic_mutations_reject_the_active_proxy_credential_reference() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x43; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let reference = CredentialRef::parse("proxy.default.auth").unwrap();
        state
            .credential_store
            .replace(
                reference.clone(),
                r#"{"username":"active","password":"secret"}"#,
                CredentialSource::User,
                0,
            )
            .unwrap();
        {
            let mut config = state.config.write().await;
            config.proxy_auth_credential_ref = Some(reference.clone());
            config.proxy_auth = Some(bamboo_config::ProxyAuth {
                username: "active".to_string(),
                password: "secret".to_string(),
            });
        }
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route(
                    "/credentials/{credential_ref}",
                    web::put().to(replace_credential),
                )
                .route(
                    "/credentials/{credential_ref}",
                    web::delete().to(clear_credential),
                ),
        )
        .await;

        let replace = test::TestRequest::put()
            .uri("/credentials/proxy.default.auth")
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "value": "replacement"
            }))
            .to_request();
        let replace = test::call_service(&app, replace).await;
        assert_eq!(replace.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let clear = test::TestRequest::delete()
            .uri("/credentials/proxy.default.auth")
            .set_json(serde_json::json!({"expected_revision": 1}))
            .to_request();
        let clear = test::call_service(&app, clear).await;
        assert_eq!(clear.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            state
                .credential_store
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            r#"{"username":"active","password":"secret"}"#
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .proxy_auth
                .as_ref()
                .map(|auth| auth.username.as_str()),
            Some("active")
        );
    }

    #[actix_web::test]
    async fn generic_mutations_reject_env_refs_without_changing_any_layer() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let reference = CredentialRef::parse("env.TOKEN.value").unwrap();
        state
            .credential_store
            .replace(reference.clone(), "env-secret", CredentialSource::User, 0)
            .unwrap();
        {
            let mut config = state.config.write().await;
            config.env_vars.push(bamboo_config::EnvVarEntry {
                name: "TOKEN".to_string(),
                value: "env-secret".to_string(),
                secret: true,
                value_encrypted: None,
                credential_ref: Some(reference.clone()),
                configured: true,
                description: None,
            });
            config.publish_env_vars();
        }
        let before_store = std::fs::read(state.credential_store.path()).unwrap();
        let before_runtime = bamboo_config::Config::current_env_vars();
        let before_config = state.config.read().await.clone();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route(
                    "/credentials/{credential_ref}",
                    web::put().to(replace_credential),
                )
                .route(
                    "/credentials/{credential_ref}",
                    web::delete().to(clear_credential),
                ),
        )
        .await;
        for request in [
            test::TestRequest::put()
                .uri("/credentials/env.TOKEN.value")
                .set_json(serde_json::json!({"expected_revision": 1, "value": "bad"}))
                .to_request(),
            test::TestRequest::delete()
                .uri("/credentials/env.TOKEN.value")
                .set_json(serde_json::json!({"expected_revision": 1}))
                .to_request(),
        ] {
            assert_eq!(
                test::call_service(&app, request).await.status(),
                actix_web::http::StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            std::fs::read(state.credential_store.path()).unwrap(),
            before_store
        );
        assert_eq!(bamboo_config::Config::current_env_vars(), before_runtime);
        assert_eq!(state.config.read().await.env_vars, before_config.env_vars);
    }
}
