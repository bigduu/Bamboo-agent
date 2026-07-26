use crate::config_manager;
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

use super::super::super::redaction::redact_config_for_api;
use super::common::{
    read_model_limits_file, redacted_config_json, take_model_limits_patch, write_model_limits_file,
};

/// Updates the Bamboo application configuration.
pub async fn set_bamboo_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let patch = payload.into_inner();
    let mut patch_obj = config_manager::assert_json_object(patch)?;
    if patch_obj.contains_key("env_vars") {
        return Err(AppError::BadRequest(
            "env_vars must be changed through the dedicated revisioned env-vars API".to_string(),
        ));
    }
    remove_unchanged_cluster_fabric_echo(&app_state, &mut patch_obj).await?;
    if patch_obj.contains_key("notifications") {
        let has_other_domain = patch_obj
            .keys()
            .any(|key| !matches!(key.as_str(), "notifications" | "expected_revision"));
        if !has_other_domain || !notification_payload_is_unchanged(&app_state, &patch_obj).await? {
            return set_notification_config(app_state, patch_obj).await;
        }
        // Legacy full-config payloads echo every section. If notification
        // metadata/credentials are unchanged, omit that domain rather than
        // forcing an unrelated provider/etc. update through the notification
        // transaction or letting it rewrite notification state.
        patch_obj.remove("notifications");
    }
    if patch_obj.contains_key("connect") {
        let has_other_domain = patch_obj
            .keys()
            .any(|key| !matches!(key.as_str(), "connect" | "expected_revision"));
        if !has_other_domain || !connect_payload_is_unchanged(&app_state, &patch_obj).await? {
            return set_connect_config(app_state, patch_obj).await;
        }
        patch_obj.remove("connect");
    }
    if patch_obj.remove("expected_revision").is_some() {
        return Err(AppError::BadRequest(
            "expected_revision is only valid for a dedicated revisioned config domain".to_string(),
        ));
    }
    let mut model_limits_patch = take_model_limits_patch(&mut patch_obj);
    config_manager::sanitize_root_patch(&mut patch_obj);
    if model_limits_patch.is_some() && !patch_obj.is_empty() {
        let current = read_model_limits_file(&app_state.app_data_dir).await?;
        if current.as_ref() == model_limits_patch.as_ref() {
            model_limits_patch = None;
        } else {
            return Err(AppError::BadRequest(
                "model_limits changes cannot be combined with another config section; split the request"
                    .to_string(),
            ));
        }
    }
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);
    let effects = config_manager::effects_for_root_patch(&patch_obj);
    let provider_credential_intents = api_key_intents.providers.clone();
    let provider_instance_credential_intents = api_key_intents.provider_instances.clone();
    // Apply the patch under the config write lock to avoid clobbering concurrent updates.
    let new_config = app_state
        .update_config_with_provider_credentials(
            move |config| {
                let current = config.clone();
                let mut patch_obj = patch_obj;
                remove_unchanged_access_control_echo(&current, &mut patch_obj)?;
                config_manager::preserve_masked_provider_api_keys(&mut patch_obj, &current);
                config_manager::preserve_masked_notification_secrets(&mut patch_obj, &current);
                config_manager::preserve_masked_connect_secrets(&mut patch_obj, &current);
                let mut new_config = config_manager::build_merged_config(&current, patch_obj)?;
                // Compatibility serialization intentionally omits access
                // verifier material. This path rejects all access-control
                // mutations, so carry the hydrated runtime verifier records
                // from the lock-time authority instead of losing them in the
                // JSON merge round-trip.
                new_config.access_control = current.access_control.clone();
                new_config.cluster_fabric.prune_orphaned_credential_refs();
                new_config.extra.remove("model_limits");
                config_manager::sync_provider_api_keys_encrypted_for_patch(
                    &mut new_config,
                    &api_key_intents,
                )?;
                *config = new_config;
                Ok(())
            },
            provider_credential_intents,
            provider_instance_credential_intents,
            ConfigUpdateEffects {
                // Best-effort: setup/UX flows must be able to persist partial config even when
                // provider init isn't possible yet.
                reload_provider: false,
                reconcile_mcp: effects.reconcile_mcp,
            },
        )
        .await?;

    // Persist model_limits.json under the config write lock so two concurrent
    // set_bamboo_config calls can't race / clobber each other's writes (the
    // write itself is now atomic too — see common::write_model_limits_file). #42.
    {
        let _config_guard = app_state.config.write().await;
        write_model_limits_file(&app_state.app_data_dir, model_limits_patch.as_ref()).await?;
    }

    if effects.reload_provider == config_manager::ReloadMode::BestEffort {
        if let Err(error) = app_state.reload_provider().await {
            tracing::warn!(
                "Config updated (provider={}, requested_reload=true) but provider reload failed: {}",
                new_config.provider,
                error
            );
        }
    }

    Ok(HttpResponse::Ok().json(redacted_config_json(&new_config, &app_state.app_data_dir).await?))
}

pub(super) fn remove_unchanged_access_control_echo(
    current: &bamboo_llm::Config,
    patch_obj: &mut Map<String, Value>,
) -> Result<(), AppError> {
    let Some(incoming) = patch_obj.get("access_control") else {
        return Ok(());
    };
    if incoming.is_null() {
        return Err(access_control_patch_error());
    }

    let current_value = current.to_compatibility_value()?;
    let redacted_current = redact_config_for_api(current_value, current);
    if redacted_current.get("access_control") != Some(incoming) {
        return Err(access_control_patch_error());
    }

    // This helper is called inside update_config_with_provider_credentials'
    // config write lock. Compatibility clients POST the full redacted GET
    // payload; the echoed metadata is safe to ignore only when it is exactly
    // the lock-time redacted projection. Never merge it because verifier
    // fields are intentionally absent and arrays would otherwise replace the
    // durable device records.
    patch_obj.remove("access_control");
    Ok(())
}

fn access_control_patch_error() -> AppError {
    AppError::BadRequest(
        "access_control must be changed through the dedicated password, pairing, and device APIs"
            .to_string(),
    )
}

async fn remove_unchanged_cluster_fabric_echo(
    app_state: &AppState,
    patch_obj: &mut Map<String, Value>,
) -> Result<(), AppError> {
    let Some(incoming) = patch_obj.get("cluster_fabric") else {
        return Ok(());
    };
    if incoming.is_null() {
        return Err(cluster_fabric_patch_error());
    }

    let current = app_state.config.read().await.clone();
    let current_value = current.to_compatibility_value()?;
    let redacted_current = redact_config_for_api(current_value, &current);
    if redacted_current.get("cluster_fabric") != Some(incoming) {
        return Err(cluster_fabric_patch_error());
    }

    // Compatibility clients POST the complete redacted GET payload. Ignore an
    // exact cluster echo before routing any other dedicated domain, but never
    // merge it: the redacted shape omits server-owned credential references.
    patch_obj.remove("cluster_fabric");
    Ok(())
}

fn cluster_fabric_patch_error() -> AppError {
    AppError::BadRequest(
        "cluster_fabric must be changed through the dedicated revisioned cluster API".to_string(),
    )
}

async fn notification_payload_is_unchanged(
    app_state: &AppState,
    patch_obj: &serde_json::Map<String, Value>,
) -> Result<bool, AppError> {
    if patch_obj.get("notifications").is_some_and(Value::is_null) {
        return Ok(false);
    }
    let current = app_state.config.read().await.clone();
    let mut notification_patch = serde_json::Map::new();
    notification_patch.insert(
        "notifications".to_string(),
        patch_obj
            .get("notifications")
            .cloned()
            .expect("caller checked notifications"),
    );
    config_manager::preserve_masked_notification_secrets(&mut notification_patch, &current);
    let merged = config_manager::build_merged_config(&current, notification_patch)?;
    Ok(merged.notifications == current.notifications)
}

async fn set_notification_config(
    app_state: web::Data<AppState>,
    mut patch_obj: serde_json::Map<String, Value>,
) -> Result<HttpResponse, AppError> {
    let explicit_revision = patch_obj.contains_key("expected_revision");
    let expected_revision = match patch_obj.remove("expected_revision") {
        Some(value) => value.as_u64().ok_or_else(|| {
            AppError::BadRequest(
                "notification expected_revision must be an unsigned integer".to_string(),
            )
        })?,
        None => app_state
            .credential_store
            .revision()
            .map_err(super::super::credentials::map_store_read_error)?,
    };
    if patch_obj.len() != 1 {
        return Err(AppError::BadRequest(
            "notification updates cannot be combined with other config domains; split the request"
                .to_string(),
        ));
    }
    let reset_domain = patch_obj.get("notifications").is_some_and(Value::is_null);
    let mut secret_intents = if reset_domain {
        BTreeSet::from(["ntfy".to_string(), "bark".to_string()])
    } else {
        BTreeSet::new()
    };
    if !reset_domain {
        let notifications = patch_obj
            .get("notifications")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AppError::BadRequest("notifications must be an object or null".to_string())
            })?;
        for (channel, secret_field, encrypted_field) in [
            ("ntfy", "token", "token_encrypted"),
            ("bark", "device_key", "device_key_encrypted"),
        ] {
            let Some(channel_patch) = notifications.get(channel).and_then(Value::as_object) else {
                continue;
            };
            for forbidden in [encrypted_field, "credential_ref", "configured"] {
                if channel_patch.contains_key(forbidden) {
                    return Err(AppError::BadRequest(
                        "notification credential metadata is server-managed".to_string(),
                    ));
                }
            }
            if let Some(value) = channel_patch.get(secret_field) {
                secret_intents.insert(channel.to_string());
                match value {
                    Value::Null => {}
                    Value::String(value) => {
                        if bamboo_config::patch::is_masked_api_key(value) {
                            if explicit_revision {
                                return Err(AppError::BadRequest(
                                    "notification credential value must not be a mask; omit it to keep the existing value"
                                        .to_string(),
                                ));
                            }
                            secret_intents.remove(channel);
                        }
                    }
                    _ => {
                        return Err(AppError::BadRequest(
                            "notification credential value must be a string or null".to_string(),
                        ));
                    }
                }
            }
        }
    }
    let patch_for_update = patch_obj;
    let intents_for_update = secret_intents.clone();
    let (new_config, _) = app_state
        .update_notification_credentials(
            expected_revision,
            secret_intents,
            reset_domain,
            move |config| {
                let current = config.clone();
                let mut merged = config_manager::build_merged_config(&current, patch_for_update)?;
                if reset_domain {
                    merged.notifications = bamboo_config::NotificationsConfig::default();
                }
                for channel in ["ntfy", "bark"] {
                    if intents_for_update.contains(channel) {
                        let value = if channel == "ntfy" {
                            merged.notifications.ntfy.token.clone()
                        } else {
                            merged.notifications.bark.device_key.clone()
                        };
                        let configured = value
                            .as_deref()
                            .is_some_and(|value| !value.trim().is_empty());
                        if channel == "ntfy" {
                            merged.notifications.ntfy.token = value;
                            merged.notifications.ntfy.configured = configured;
                        } else {
                            merged.notifications.bark.device_key = value;
                            merged.notifications.bark.configured = configured;
                        }
                    } else if channel == "ntfy" {
                        merged.notifications.ntfy.token = current.notifications.ntfy.token.clone();
                        merged.notifications.ntfy.credential_ref =
                            current.notifications.ntfy.credential_ref.clone();
                        merged.notifications.ntfy.configured =
                            current.notifications.ntfy.configured;
                    } else {
                        merged.notifications.bark.device_key =
                            current.notifications.bark.device_key.clone();
                        merged.notifications.bark.credential_ref =
                            current.notifications.bark.credential_ref.clone();
                        merged.notifications.bark.configured =
                            current.notifications.bark.configured;
                    }
                }
                *config = merged;
                Ok(())
            },
        )
        .await?;
    Ok(HttpResponse::Ok().json(redacted_config_json(&new_config, &app_state.app_data_dir).await?))
}

async fn connect_payload_is_unchanged(
    app_state: &AppState,
    patch_obj: &serde_json::Map<String, Value>,
) -> Result<bool, AppError> {
    if patch_obj.get("connect").is_some_and(Value::is_null) {
        return Ok(false);
    }
    let current = app_state.config.read().await.clone();
    let mut connect_patch = serde_json::Map::new();
    connect_patch.insert(
        "connect".to_string(),
        patch_obj
            .get("connect")
            .cloned()
            .expect("caller checked connect"),
    );
    config_manager::preserve_masked_connect_secrets(&mut connect_patch, &current);
    let merged = config_manager::build_merged_config(&current, connect_patch)?;
    Ok(merged.connect == current.connect)
}

async fn set_connect_config(
    app_state: web::Data<AppState>,
    mut patch_obj: serde_json::Map<String, Value>,
) -> Result<HttpResponse, AppError> {
    let explicit_revision = patch_obj.contains_key("expected_revision");
    let expected_revision = match patch_obj.remove("expected_revision") {
        Some(value) => value.as_u64().ok_or_else(|| {
            AppError::BadRequest(
                "connect expected_revision must be an unsigned integer".to_string(),
            )
        })?,
        None => app_state
            .credential_store
            .revision()
            .map_err(super::super::credentials::map_store_read_error)?,
    };
    if patch_obj.len() != 1 {
        return Err(AppError::BadRequest(
            "connect updates cannot be combined with other config domains; split the request"
                .to_string(),
        ));
    }
    let reset_domain = patch_obj.get("connect").is_some_and(Value::is_null);
    let mut secret_intents = bamboo_config::patch::ConnectSecretIntents::default();
    if !reset_domain {
        let connect = patch_obj
            .get("connect")
            .and_then(Value::as_object)
            .ok_or_else(|| AppError::BadRequest("connect must be an object or null".to_string()))?;
        let platforms = connect
            .get("platforms")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AppError::BadRequest("connect.platforms must be an array".to_string())
            })?;
        for (index, platform) in platforms.iter().enumerate() {
            let platform = platform.as_object().ok_or_else(|| {
                AppError::BadRequest("connect platform must be an object".to_string())
            })?;
            for forbidden in [
                "token_encrypted",
                "token_credential_ref",
                "token_configured",
                "app_secret_encrypted",
                "app_secret_credential_ref",
                "app_secret_configured",
            ] {
                if platform.contains_key(forbidden) {
                    return Err(AppError::BadRequest(
                        "connect credential metadata is server-managed".to_string(),
                    ));
                }
            }
            for (field, intents) in [
                ("token", &mut secret_intents.token),
                ("app_secret", &mut secret_intents.app_secret),
            ] {
                let Some(value) = platform.get(field) else {
                    continue;
                };
                match value {
                    Value::Null => {
                        intents.insert(index);
                    }
                    Value::String(value) => {
                        if bamboo_config::patch::is_masked_api_key(value) {
                            if explicit_revision {
                                return Err(AppError::BadRequest(
                                    "connect credential value must not be a mask; omit it to keep the existing value"
                                        .to_string(),
                                ));
                            }
                        } else {
                            intents.insert(index);
                        }
                    }
                    _ => {
                        return Err(AppError::BadRequest(
                            "connect credential value must be a string or null".to_string(),
                        ));
                    }
                }
            }
        }
    }

    config_manager::sanitize_root_patch(&mut patch_obj);
    let patch_for_update = patch_obj;
    let intents_for_update = secret_intents.clone();
    let (new_config, _) = app_state
        .update_connect_credentials(expected_revision, secret_intents, move |config| {
            let current = config.clone();
            let mut patch = patch_for_update;
            config_manager::preserve_masked_connect_secrets(&mut patch, &current);
            let mut merged = config_manager::build_merged_config(&current, patch)?;
            if reset_domain {
                merged.connect = bamboo_config::ConnectConfig::default();
            }
            preserve_connect_runtime_authority(&current, &mut merged, &intents_for_update);
            *config = merged;
            Ok(())
        })
        .await?;
    Ok(HttpResponse::Ok().json(redacted_config_json(&new_config, &app_state.app_data_dir).await?))
}

fn preserve_connect_runtime_authority(
    current: &bamboo_llm::Config,
    candidate: &mut bamboo_llm::Config,
    intents: &bamboo_config::patch::ConnectSecretIntents,
) {
    for (index, platform) in candidate.connect.platforms.iter_mut().enumerate() {
        let current_platform = platform
            .id
            .as_deref()
            .and_then(|id| {
                current
                    .connect
                    .platforms
                    .iter()
                    .find(|existing| existing.id.as_deref() == Some(id))
            })
            .or_else(|| {
                current
                    .connect
                    .platforms
                    .get(index)
                    .filter(|existing| existing.platform_type == platform.platform_type)
            })
            .or_else(|| {
                let mut matches = current
                    .connect
                    .platforms
                    .iter()
                    .filter(|existing| existing.platform_type == platform.platform_type);
                let first = matches.next();
                first.filter(|_| matches.next().is_none())
            });
        let Some(existing) = current_platform else {
            continue;
        };
        if platform.id.is_none() {
            platform.id = existing.id.clone();
        }
        if !intents.token.contains(&index) {
            platform.token = existing.token.clone();
            platform.token_credential_ref = existing.token_credential_ref.clone();
            platform.token_configured = existing.token_configured;
        }
        if !intents.app_secret.contains(&index) {
            platform.app_secret = existing.app_secret.clone();
            platform.app_secret_credential_ref = existing.app_secret_credential_ref.clone();
            platform.app_secret_configured = existing.app_secret_configured;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};

    #[actix_web::test]
    async fn root_patch_rejects_env_vars_instead_of_silently_dropping_them() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/config", web::post().to(set_bamboo_config))
                .route(
                    "/config/validate",
                    web::post().to(crate::handlers::settings::validate_bamboo_config_patch),
                ),
        )
        .await;
        for uri in ["/config", "/config/validate"] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(uri)
                    .set_json(serde_json::json!({
                        "env_vars": [{"name": "TOKEN", "value": "secret", "secret": true}]
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
            assert!(body.contains("revisioned env-vars API"));
        }
    }

    #[actix_web::test]
    async fn root_patch_and_validation_reject_cluster_fabric_bypass() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let cluster_path = dir.path().join("cluster-fabric.json");
        let before = std::fs::read(&cluster_path).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/config", web::post().to(set_bamboo_config))
                .route(
                    "/config/validate",
                    web::post().to(crate::handlers::settings::validate_bamboo_config_patch),
                ),
        )
        .await;
        for uri in ["/config", "/config/validate"] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(uri)
                    .set_json(serde_json::json!({
                        "cluster_fabric": {
                            "nodes": [{
                                "id": "bypass",
                                "label": "bypass",
                                "placement": {"type": "local"}
                            }]
                        }
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
            assert!(body.contains("revisioned cluster API"));
        }
        assert_eq!(std::fs::read(cluster_path).unwrap(), before);
        assert!(state.config.read().await.cluster_fabric.nodes.is_empty());
    }

    #[actix_web::test]
    async fn legacy_root_get_post_accepts_only_an_unchanged_redacted_cluster_echo() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x65; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let reference = bamboo_config::cluster_password_credential_ref("echo-node").unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                std::collections::BTreeMap::from([(
                    "echo-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents {
                        password: bamboo_config::ClusterCredentialAction::Replace(
                            "echo-secret".to_string(),
                        ),
                        private_key: bamboo_config::ClusterCredentialAction::Clear,
                        passphrase: bamboo_config::ClusterCredentialAction::Clear,
                    },
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "echo-node".to_string(),
                        label: "echo-node".to_string(),
                        placement: bamboo_config::NodePlacement::Ssh(bamboo_config::SshTarget {
                            host: "echo.example".to_string(),
                            port: 22,
                            username: "deploy".to_string(),
                            auth: bamboo_config::SshAuth::Password {
                                password: String::new(),
                                password_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        let cluster_path = dir.path().join("cluster-fabric.json");
        let cluster_before = std::fs::read(&cluster_path).unwrap();
        let credentials_before = std::fs::read(state.credential_store.path()).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route(
                    "/config",
                    web::get().to(crate::handlers::settings::get_bamboo_config),
                )
                .route("/config", web::post().to(set_bamboo_config)),
        )
        .await;

        let mut full_config: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/config").to_request(),
        )
        .await;
        let cluster_echo = full_config["cluster_fabric"].clone();
        assert!(cluster_echo.get("credential_refs").is_none());
        assert!(!cluster_echo.to_string().contains(reference.as_str()));
        assert!(!cluster_echo.to_string().contains("echo-secret"));

        full_config["server"]["port"] = serde_json::json!(19_999);
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/config")
                .set_json(&full_config)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response: Value = test::read_body_json(response).await;
        assert_eq!(response["cluster_fabric"], cluster_echo);
        assert_eq!(response["server"]["port"], 19_999);
        assert_eq!(std::fs::read(cluster_path).unwrap(), cluster_before);
        assert_eq!(
            std::fs::read(state.credential_store.path()).unwrap(),
            credentials_before
        );
    }

    #[actix_web::test]
    async fn notification_patch_is_revisioned_redacted_and_supports_keep_clear_replace() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0xb1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/config", web::post().to(set_bamboo_config))
                .route(
                    "/notifications",
                    web::get().to(crate::handlers::settings::get_notification_config),
                ),
        )
        .await;

        let set = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "notifications": {
                    "ntfy": {"enabled": true, "topic": "alerts", "token": "ntfy-api-secret"},
                    "bark": {"enabled": true, "device_key": "bark-api-secret"}
                }
            }))
            .to_request();
        let response = test::call_service(&app, set).await;
        assert!(response.status().is_success());
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(!body.contains("ntfy-api-secret"));
        assert!(!body.contains("bark-api-secret"));
        assert!(!body.contains("token_encrypted"));
        assert!(!body.contains("device_key_encrypted"));
        let root = std::fs::read_to_string(dir.path().join("notifications.json")).unwrap();
        assert!(!root.contains("ntfy-api-secret"));
        assert!(!root.contains("bark-api-secret"));
        assert!(!root.contains("token_encrypted"));
        let credentials = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
        assert!(!credentials.contains("ntfy-api-secret"));
        assert!(!credentials.contains("bark-api-secret"));

        let metadata: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/notifications").to_request(),
        )
        .await;
        assert_eq!(metadata["revision"], 1);
        assert_eq!(metadata["data"]["ntfy"]["credential"]["configured"], true);
        assert_eq!(metadata["data"]["bark"]["credential"]["configured"], true);
        assert!(!metadata.to_string().contains("api-secret"));

        let keep = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "notifications": {"ntfy": {"topic": "renamed"}}
            }))
            .to_request();
        assert!(test::call_service(&app, keep).await.status().is_success());
        let metadata: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/notifications").to_request(),
        )
        .await;
        assert_eq!(metadata["revision"], 2);
        assert_eq!(metadata["data"]["ntfy"]["topic"], "renamed");
        assert_eq!(
            state
                .config
                .read()
                .await
                .notifications
                .ntfy
                .token
                .as_deref(),
            Some("ntfy-api-secret")
        );

        let stale = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "notifications": {"ntfy": {"token": "stale-secret"}}
            }))
            .to_request();
        let stale = test::call_service(&app, stale).await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(!String::from_utf8(test::read_body(stale).await.to_vec())
            .unwrap()
            .contains("stale-secret"));

        for notifications in [
            serde_json::json!({"ntfy": {"token": "****...****"}}),
            serde_json::json!({"bark": {"credential_ref": "attacker.ref"}}),
            serde_json::json!({"ntfy": {"token_encrypted": "attacker-cipher"}}),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/config")
                    .set_json(serde_json::json!({
                        "expected_revision": 2,
                        "notifications": notifications
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        let clear = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 2,
                "notifications": {"ntfy": {"token": null}}
            }))
            .to_request();
        assert!(test::call_service(&app, clear).await.status().is_success());
        let metadata: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/notifications").to_request(),
        )
        .await;
        assert_eq!(metadata["revision"], 3);
        assert_eq!(metadata["data"]["ntfy"]["credential"]["configured"], false);
        assert!(state.config.read().await.notifications.ntfy.token.is_none());
    }

    #[actix_web::test]
    async fn notification_null_reset_clears_both_credentials_and_restores_defaults() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0xb2; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/config", web::post().to(set_bamboo_config))
                .route(
                    "/notifications",
                    web::get().to(crate::handlers::settings::get_notification_config),
                ),
        )
        .await;
        let set = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 0,
                "notifications": {
                    "desktop": {"enabled": true},
                    "ntfy": {"enabled": true, "topic": "alerts", "token": "reset-ntfy-secret"},
                    "bark": {"enabled": true, "device_key": "reset-bark-secret"}
                }
            }))
            .to_request();
        assert!(test::call_service(&app, set).await.status().is_success());

        let reset = test::TestRequest::post()
            .uri("/config")
            .set_json(serde_json::json!({
                "expected_revision": 1,
                "notifications": null
            }))
            .to_request();
        let reset = test::call_service(&app, reset).await;
        let reset_status = reset.status();
        let reset_body = String::from_utf8(test::read_body(reset).await.to_vec()).unwrap();
        assert!(
            reset_status.is_success(),
            "reset failed with {reset_status}: {reset_body}"
        );

        let metadata: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/notifications").to_request(),
        )
        .await;
        assert_eq!(metadata["revision"], 2);
        assert_eq!(metadata["data"]["desktop"]["enabled"], Value::Null);
        assert_eq!(metadata["data"]["ntfy"]["enabled"], false);
        assert_eq!(metadata["data"]["ntfy"]["topic"], "");
        assert_eq!(metadata["data"]["ntfy"]["credential"]["configured"], false);
        assert_eq!(
            metadata["data"]["ntfy"]["credential"]["credential_ref"],
            Value::Null
        );
        assert_eq!(metadata["data"]["bark"]["enabled"], false);
        assert_eq!(metadata["data"]["bark"]["credential"]["configured"], false);
        assert_eq!(
            metadata["data"]["bark"]["credential"]["credential_ref"],
            Value::Null
        );
        let store = bamboo_config::CredentialStore::open(dir.path());
        assert!(store
            .resolve(&bamboo_config::credential_ref("notification", "ntfy", "token").unwrap())
            .unwrap()
            .is_none());
        assert!(store
            .resolve(&bamboo_config::credential_ref("notification", "bark", "device_key").unwrap())
            .unwrap()
            .is_none());
        let loaded =
            bamboo_config::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.notifications,
            bamboo_config::NotificationsConfig::default()
        );
        let disk = std::fs::read_to_string(dir.path().join("notifications.json")).unwrap();
        assert!(!disk.contains("reset-ntfy-secret"));
        assert!(!disk.contains("reset-bark-secret"));
    }
}
