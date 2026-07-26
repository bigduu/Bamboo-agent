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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetCredentialsRequest {
    pub expected_revision: u64,
}

pub async fn list_credentials(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let (mut statuses, health) = app_state
        .credential_store
        .statuses_with_health()
        .map_err(map_store_read_error)?;
    statuses.retain(|status| !is_cluster_credential_ref(&status.credential_ref));
    Ok(HttpResponse::Ok().json(envelope(statuses, health)))
}

pub async fn get_credential_status(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let credential_ref = parse_credential_ref(path.into_inner())?;
    reject_cluster_credential_ref(&credential_ref)?;
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

pub async fn reset_credentials(
    app_state: web::Data<AppState>,
    payload: web::Json<ResetCredentialsRequest>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let config = app_state.config.read().await.clone();
    let (revision, statuses) = app_state
        .credential_store
        .clear_all_unreferenced(&config, payload.expected_revision)
        .map_err(|error| match error {
            ConfigStoreError::Validation(message) => AppError::BadRequest(message),
            other => map_store_mutation_error(other),
        })?;
    publish_credential_event(&app_state, revision);
    Ok(HttpResponse::Ok().json(envelope(
        statuses,
        CredentialStoreHealth::committed(revision),
    )))
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
    reject_cluster_credential_ref(credential_ref)?;
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
    if config
        .cluster_fabric
        .credential_refs
        .values()
        .any(|metadata| {
            metadata
                .references()
                .any(|reference| reference == credential_ref)
        })
    {
        return Err(AppError::BadRequest(
            "active cluster credentials must be changed through the dedicated revisioned cluster-fabric API"
                .to_string(),
        ));
    }
    Ok(())
}

fn is_cluster_credential_ref(credential_ref: &CredentialRef) -> bool {
    credential_ref.as_str().starts_with("cluster.")
}

fn reject_cluster_credential_ref(credential_ref: &CredentialRef) -> Result<(), AppError> {
    if is_cluster_credential_ref(credential_ref) {
        return Err(AppError::BadRequest(
            "cluster credential references are reserved for the dedicated revisioned cluster-fabric API"
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
    use bamboo_config::{
        cluster_fabric::{
            ClusterCredentialAction, ClusterNodeCredentialIntents, DeployProfile, Node,
            NodePlacement, SshAuth, SshTarget, TrustLevel,
        },
        CredentialStatus,
    };
    use std::collections::BTreeMap;
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
        assert_eq!(missing["status"], "healthy");
        assert_eq!(missing["source"], "file");

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
    async fn cluster_credential_namespace_is_hidden_and_reserved_without_an_owner() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x66; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let cluster_ref = CredentialRef::parse("cluster.unowned.password").unwrap();
        let ordinary_ref = CredentialRef::parse("custom.visible.token").unwrap();
        state
            .credential_store
            .replace(
                cluster_ref.clone(),
                "unowned-cluster-secret",
                CredentialSource::User,
                0,
            )
            .unwrap();
        state
            .credential_store
            .replace(
                ordinary_ref.clone(),
                "ordinary-secret",
                CredentialSource::User,
                1,
            )
            .unwrap();
        assert!(state
            .config
            .read()
            .await
            .cluster_fabric
            .credential_refs
            .is_empty());

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/credentials", web::get().to(list_credentials))
                .route(
                    "/credentials/{credential_ref}",
                    web::get().to(get_credential_status),
                )
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

        let list: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/credentials").to_request(),
        )
        .await;
        assert_eq!(list["revision"], 2);
        assert_eq!(list["data"].as_array().unwrap().len(), 1);
        assert!(list.to_string().contains(ordinary_ref.as_str()));
        assert!(!list.to_string().contains(cluster_ref.as_str()));

        let uri = format!("/credentials/{}", cluster_ref.as_str());
        for request in [
            test::TestRequest::get().uri(&uri).to_request(),
            test::TestRequest::put()
                .uri(&uri)
                .set_json(serde_json::json!({
                    "expected_revision": 2,
                    "value": "generic-replacement"
                }))
                .to_request(),
            test::TestRequest::delete()
                .uri(&uri)
                .set_json(serde_json::json!({"expected_revision": 2}))
                .to_request(),
        ] {
            let response = test::call_service(&app, request).await;
            assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("reserved"));
        }
        assert_eq!(state.credential_store.revision().unwrap(), 2);
        assert_eq!(
            state
                .credential_store
                .resolve(&cluster_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "unowned-cluster-secret"
        );
    }

    #[actix_web::test]
    async fn generic_replace_clear_and_reset_preserve_active_cluster_credentials() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let password_ref = bamboo_config::cluster_password_credential_ref("password-node").unwrap();
        let private_key_ref =
            bamboo_config::cluster_private_key_credential_ref("key-node").unwrap();
        let passphrase_ref = bamboo_config::cluster_passphrase_credential_ref("key-node").unwrap();
        let intents = BTreeMap::from([
            (
                "password-node".to_string(),
                ClusterNodeCredentialIntents {
                    password: ClusterCredentialAction::Replace("password-secret".to_string()),
                    private_key: ClusterCredentialAction::Clear,
                    passphrase: ClusterCredentialAction::Clear,
                },
            ),
            (
                "key-node".to_string(),
                ClusterNodeCredentialIntents {
                    password: ClusterCredentialAction::Clear,
                    private_key: ClusterCredentialAction::Replace("private-key-secret".to_string()),
                    passphrase: ClusterCredentialAction::Replace("passphrase-secret".to_string()),
                },
            ),
        ]);
        state
            .update_cluster_fabric_credentials(0, intents, |config| {
                config.cluster_fabric.nodes = vec![
                    Node {
                        id: "password-node".to_string(),
                        label: "password-node".to_string(),
                        placement: NodePlacement::Ssh(SshTarget {
                            host: "password.example".to_string(),
                            port: 22,
                            username: "deploy".to_string(),
                            auth: SshAuth::Password {
                                password: String::new(),
                                password_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: TrustLevel::Trusted,
                        deploy: DeployProfile::default(),
                        state: None,
                        enabled: true,
                    },
                    Node {
                        id: "key-node".to_string(),
                        label: "key-node".to_string(),
                        placement: NodePlacement::Ssh(SshTarget {
                            host: "key.example".to_string(),
                            port: 22,
                            username: "deploy".to_string(),
                            auth: SshAuth::PrivateKey {
                                private_key: String::new(),
                                private_key_encrypted: None,
                                private_key_path: None,
                                passphrase: String::new(),
                                passphrase_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: TrustLevel::Trusted,
                        deploy: DeployProfile::default(),
                        state: None,
                        enabled: true,
                    },
                ];
                Ok(())
            })
            .await
            .unwrap();

        let orphan = CredentialRef::parse("custom.cluster-reset.orphan").unwrap();
        let before_orphan_revision = state.credential_store.revision().unwrap();
        state
            .credential_store
            .replace(
                orphan.clone(),
                "unrelated-orphan",
                CredentialSource::User,
                before_orphan_revision,
            )
            .unwrap();
        let credential_revision = state.credential_store.revision().unwrap();
        let cluster_revision = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .cluster_fabric
            .snapshot()
            .revision;
        let before_store = std::fs::read(state.credential_store.path()).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/credentials/reset", web::post().to(reset_credentials))
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

        for reference in [&password_ref, &private_key_ref, &passphrase_ref] {
            let uri = format!("/credentials/{}", reference.as_str());
            let replace = test::call_service(
                &app,
                test::TestRequest::put()
                    .uri(&uri)
                    .set_json(serde_json::json!({
                        "expected_revision": credential_revision,
                        "value": "generic-replacement-must-not-win"
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(replace.status(), actix_web::http::StatusCode::BAD_REQUEST);
            let replace: serde_json::Value = test::read_body_json(replace).await;
            assert!(replace["error"]["message"]
                .as_str()
                .unwrap()
                .contains("dedicated revisioned cluster-fabric API"));

            let clear = test::call_service(
                &app,
                test::TestRequest::delete()
                    .uri(&uri)
                    .set_json(serde_json::json!({
                        "expected_revision": credential_revision
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(clear.status(), actix_web::http::StatusCode::BAD_REQUEST);
            let clear: serde_json::Value = test::read_body_json(clear).await;
            assert!(clear["error"]["message"]
                .as_str()
                .unwrap()
                .contains("dedicated revisioned cluster-fabric API"));
        }

        let reset = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/credentials/reset")
                .set_json(serde_json::json!({
                    "expected_revision": credential_revision
                }))
                .to_request(),
        )
        .await;
        assert_eq!(reset.status(), actix_web::http::StatusCode::BAD_REQUEST);
        let reset: serde_json::Value = test::read_body_json(reset).await;
        assert!(reset["error"]["message"]
            .as_str()
            .unwrap()
            .contains("credentials still referenced by configuration"));

        assert_eq!(
            std::fs::read(state.credential_store.path()).unwrap(),
            before_store
        );
        assert_eq!(
            state.credential_store.revision().unwrap(),
            credential_revision
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            cluster_revision
        );
        for reference in [&password_ref, &private_key_ref, &passphrase_ref] {
            assert!(
                state.credential_store.status(reference).unwrap().configured,
                "{} must remain configured",
                reference.as_str()
            );
        }
        assert!(state.credential_store.status(&orphan).unwrap().configured);
    }

    #[cfg(feature = "test-utils")]
    #[actix_web::test]
    async fn generic_mutations_reject_env_refs_without_changing_any_layer() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        // Keep both runtime-cache publications and reads in this Actix
        // current-thread scope. Rejected handlers can still mutate the scoped
        // cache (and fail the assertion), while parallel AppState tests cannot
        // replace it from another test-harness thread.
        let _env_cache = bamboo_config::test_support::isolate_env_vars_cache();
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

    #[actix_web::test]
    async fn reset_clears_all_orphans_in_one_cas_but_rejects_live_references() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x63; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let orphan = CredentialRef::parse("custom.orphan.token").unwrap();
        let active = CredentialRef::parse("env.ACTIVE.value").unwrap();
        state
            .credential_store
            .replace(orphan, "orphan-secret", CredentialSource::User, 0)
            .unwrap();
        state
            .credential_store
            .replace(active.clone(), "active-secret", CredentialSource::User, 1)
            .unwrap();
        let durable_env = bamboo_config::EnvSection(vec![bamboo_config::EnvVarEntry {
            name: "ACTIVE".to_string(),
            value: String::new(),
            secret: true,
            value_encrypted: None,
            credential_ref: Some(active),
            configured: true,
            description: None,
        }]);
        state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .env
            .commit(0, durable_env)
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/credentials/reset", web::post().to(reset_credentials)),
        )
        .await;

        let rejected = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/credentials/reset")
                .set_json(serde_json::json!({"expected_revision": 2}))
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(state.credential_store.statuses().unwrap().len(), 2);

        state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .env
            .commit(1, bamboo_config::EnvSection::default())
            .unwrap();
        let reset = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/credentials/reset")
                .set_json(serde_json::json!({"expected_revision": 2}))
                .to_request(),
        )
        .await;
        assert!(reset.status().is_success());
        let reset: serde_json::Value = test::read_body_json(reset).await;
        assert_eq!(reset["revision"], 3);
        assert_eq!(reset["data"], serde_json::json!([]));
        assert!(state.credential_store.statuses().unwrap().is_empty());
    }
}
