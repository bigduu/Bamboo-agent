use actix_web::{web, HttpResponse};
use bamboo_config::{
    ConfigStoreError, CredentialRef, CredentialSource, SectionSourceKind, SectionStatus,
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

#[derive(Debug, Deserialize)]
pub struct ReplaceCredentialRequest {
    pub expected_revision: u64,
    pub value: String,
    #[serde(default = "user_source")]
    pub source: CredentialSource,
}

#[derive(Debug, Deserialize)]
pub struct ClearCredentialRequest {
    pub expected_revision: u64,
}

fn user_source() -> CredentialSource {
    CredentialSource::User
}

pub async fn list_credentials(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let (revision, statuses) = app_state
        .credential_store
        .statuses_with_revision()
        .map_err(map_store_error)?;
    Ok(HttpResponse::Ok().json(envelope(statuses, revision)))
}

pub async fn get_credential_status(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = CredentialRef::parse(path.into_inner()).map_err(map_store_error)?;
    let (revision, status) = app_state
        .credential_store
        .status_with_revision(&credential_ref)
        .map_err(map_store_error)?;
    Ok(HttpResponse::Ok().json(envelope(status, revision)))
}

pub async fn replace_credential(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ReplaceCredentialRequest>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = CredentialRef::parse(path.into_inner()).map_err(map_store_error)?;
    let (revision, status) = app_state
        .credential_store
        .replace(
            credential_ref,
            &payload.value,
            payload.source,
            payload.expected_revision,
        )
        .map_err(map_store_error)?;
    publish_credential_event(&app_state, revision);
    Ok(HttpResponse::Ok().json(envelope(status, revision)))
}

pub async fn clear_credential(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ClearCredentialRequest>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = CredentialRef::parse(path.into_inner()).map_err(map_store_error)?;
    let (revision, status) = app_state
        .credential_store
        .clear(&credential_ref, payload.expected_revision)
        .map_err(map_store_error)?;
    publish_credential_event(&app_state, revision);
    Ok(HttpResponse::Ok().json(envelope(status, revision)))
}

pub async fn get_live_config_health(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let health = app_state
        .config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    Ok(HttpResponse::Ok().json(health))
}

fn envelope<T>(data: T, revision: u64) -> CredentialEnvelope<T> {
    CredentialEnvelope {
        data,
        revision,
        status: SectionStatus::Healthy,
        source: SectionSourceKind::File,
        last_error: None,
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

fn map_store_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        ConfigStoreError::Validation(message) => AppError::BadRequest(message),
        ConfigStoreError::Json(_) => {
            AppError::BadRequest("invalid credential document".to_string())
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
        let value = serde_json::to_value(envelope(status, 4)).unwrap();
        assert_eq!(value["revision"], 4);
        assert_eq!(value["status"], "healthy");
        assert!(value["data"].get("value").is_none());
        assert!(value["data"].get("secret").is_none());
    }

    #[actix_web::test]
    async fn replace_is_redacted_stale_cas_is_409_and_feed_receives_change() {
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

        let get = test::TestRequest::get()
            .uri("/credentials/provider.openai.api_key")
            .to_request();
        let response = test::call_service(&app, get).await;
        let bytes = test::read_body(response).await;
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!body.contains("sk-never-return-this"));
        assert!(body.contains("\"configured\":true"));
    }
}
