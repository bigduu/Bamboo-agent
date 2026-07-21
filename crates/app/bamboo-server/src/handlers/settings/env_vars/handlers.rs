use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_config::EnvVarEntry;

use super::{
    types::{
        DeleteEnvVarQuery, EnvVarResponse, EnvVarsListResponse, ReplaceEnvVarsRequest,
        UpsertEnvVarRequest,
    },
    validation::{check_duplicate_names, validate_env_var_name, validate_env_var_value},
};

/// `GET /bamboo/env-vars` – list all env vars (secrets masked).
pub async fn list_env_vars(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let revision = app_state.credential_store.revision().map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "env credential status unavailable: {error}"
        ))
    })?;
    let config = app_state.config.read().await;
    let entries: Vec<EnvVarResponse> = config
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { revision, entries }))
}

/// `POST /bamboo/env-vars` – create or update a single env var.
pub async fn upsert_env_var(
    app_state: web::Data<AppState>,
    payload: web::Json<UpsertEnvVarRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    let expected_revision = req.expected_revision;
    let req = req.entry;
    validate_env_var_name(&req.name)?;
    if let Some(value) = req.value.as_deref() {
        validate_env_var_value(value)?;
        if bamboo_config::patch::is_masked_api_key(value) {
            return Err(AppError::BadRequest(
                "environment variable value must not be a mask".to_string(),
            ));
        }
    }
    let name = req.name.clone();

    let (updated, revision) = app_state
        .update_env_var_credentials(
            expected_revision,
            std::collections::BTreeSet::from([name]),
            move |cfg| {
                // Replace existing or push new.
                if let Some(existing) = cfg.env_vars.iter_mut().find(|e| e.name == req.name) {
                    let was_secret = existing.secret;
                    if let Some(value) = req.value.as_ref() {
                        // Empty is an explicit clear. Missing is the only keep
                        // operation, and masks are rejected above.
                        existing.value = value.clone();
                        existing.configured = !value.is_empty();
                    } else if was_secret && req.secret {
                        // Preserve is represented to the transaction by
                        // configured metadata without re-supplying plaintext.
                        existing.value.clear();
                    }
                    existing.secret = req.secret;
                    existing.value_encrypted = None;
                    existing.description = req.description.clone();
                } else {
                    if req.secret && req.value.is_none() {
                        return Err(AppError::BadRequest(
                            "a new secret environment variable requires a value".to_string(),
                        ));
                    }
                    let value = req.value.clone().unwrap_or_default();
                    cfg.env_vars.push(EnvVarEntry {
                        name: req.name.clone(),
                        value: value.clone(),
                        secret: req.secret,
                        value_encrypted: None,
                        credential_ref: None,
                        configured: !value.is_empty(),
                        description: req.description.clone(),
                    });
                }
                Ok(())
            },
        )
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { revision, entries }))
}

/// `PUT /bamboo/env-vars` – replace the entire env vars list.
pub async fn replace_env_vars(
    app_state: web::Data<AppState>,
    payload: web::Json<ReplaceEnvVarsRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    let expected_revision = req.expected_revision;

    // Validate all entries up-front.
    let names: Vec<&str> = req.entries.iter().map(|e| e.name.as_str()).collect();
    check_duplicate_names(&names)?;
    for entry in &req.entries {
        validate_env_var_name(&entry.name)?;
        if let Some(value) = entry.value.as_deref() {
            validate_env_var_value(value)?;
            if bamboo_config::patch::is_masked_api_key(value) {
                return Err(AppError::BadRequest(
                    "environment variable value must not be a mask".to_string(),
                ));
            }
        }
    }
    let requested_entries = req.entries;

    let mut intents = {
        let cfg = app_state.config.read().await;
        cfg.env_vars
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<std::collections::BTreeSet<_>>()
    };
    intents.extend(requested_entries.iter().map(|entry| entry.name.clone()));
    let (updated, revision) = app_state
        .update_env_var_credentials(expected_revision, intents, move |cfg| {
            let mut new_entries = Vec::with_capacity(requested_entries.len());
            for entry in &requested_entries {
                let existing = cfg
                    .env_vars
                    .iter()
                    .find(|current| current.name == entry.name);
                if entry.secret && entry.value.is_none() && existing.is_none() {
                    return Err(AppError::BadRequest(
                        "a new secret environment variable requires a value".to_string(),
                    ));
                }
                let value = entry
                    .value
                    .clone()
                    .or_else(|| {
                        existing.and_then(|current| {
                            (!(current.secret && entry.secret)).then(|| current.value.clone())
                        })
                    })
                    .unwrap_or_default();
                new_entries.push(EnvVarEntry {
                    name: entry.name.clone(),
                    value: value.clone(),
                    secret: entry.secret,
                    value_encrypted: None,
                    credential_ref: existing.and_then(|current| current.credential_ref.clone()),
                    configured: if entry.secret {
                        if entry.value.is_some() {
                            !value.is_empty()
                        } else {
                            existing.is_some_and(|current| current.configured) || !value.is_empty()
                        }
                    } else {
                        !value.is_empty()
                    },
                    description: entry.description.clone(),
                });
            }
            cfg.env_vars = new_entries;
            Ok(())
        })
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { revision, entries }))
}

/// `DELETE /bamboo/env-vars/{name}` – delete a single env var.
pub async fn delete_env_var(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<DeleteEnvVarQuery>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();
    let expected_revision = query.expected_revision;
    let intent = name.clone();

    let (updated, revision) = app_state
        .update_env_var_credentials(
            expected_revision,
            std::collections::BTreeSet::from([intent]),
            move |cfg| {
                let before = cfg.env_vars.len();
                cfg.env_vars.retain(|e| e.name != name);
                if cfg.env_vars.len() == before {
                    return Err(AppError::NotFound(format!(
                        "Environment variable '{}' not found",
                        name
                    )));
                }
                Ok(())
            },
        )
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { revision, entries }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};

    fn encryption_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[actix_web::test]
    async fn api_redacts_secret_rejects_masks_and_returns_stale_cas_as_conflict() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/env-vars", web::post().to(upsert_env_var))
                .route("/env-vars/replace", web::post().to(replace_env_vars))
                .route("/env-vars", web::get().to(list_env_vars)),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 0,
                    "name": "TOKEN",
                    "value": "super-secret-value",
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["revision"], 1);
        assert_eq!(body["entries"][0]["value"], "****...****");
        assert_eq!(body["entries"][0]["configured"], true);
        let rendered = body.to_string();
        assert!(!rendered.contains("super-secret-value"));
        assert!(!rendered.contains("credential_ref"));

        let mut metadata_feed = state.account_sink.subscribe();
        let metadata = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 1,
                    "name": "TOKEN",
                    "secret": true,
                    "description": "metadata only"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(metadata.status(), StatusCode::OK);
        let metadata: serde_json::Value = test::read_body_json(metadata).await;
        assert_eq!(metadata["revision"], 2);
        assert_eq!(metadata["entries"][0]["configured"], true);
        let metadata_event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = metadata_feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision: 2 }
                        if section == "env"
                ) {
                    break event;
                }
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            metadata_event.event,
            bamboo_agent_core::AgentEvent::ConfigChanged { ref section, revision: 2 }
                if section == "env"
        ));

        let mut no_op_feed = state.account_sink.subscribe();
        let no_op = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 2,
                    "name": "TOKEN",
                    "secret": true,
                    "description": "metadata only"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(no_op.status(), StatusCode::OK);
        let no_op: serde_json::Value = test::read_body_json(no_op).await;
        assert_eq!(no_op["revision"], 2);
        let duplicate_env = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                let event = no_op_feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigInvalid { section, .. }
                        if section == "env"
                ) {
                    break;
                }
            }
        })
        .await;
        assert!(
            duplicate_env.is_err(),
            "a semantic no-op must not emit an env ConfigChanged"
        );

        for value in [None, Some("plain loser")] {
            let stale = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/env-vars")
                    .set_json(serde_json::json!({
                        "expected_revision": 1,
                        "name": "TOKEN",
                        "value": value,
                        "secret": value.is_none(),
                        "description": "stale metadata"
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(stale.status(), StatusCode::CONFLICT);
        }

        let bulk = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars/replace")
                .set_json(serde_json::json!({
                    "expected_revision": 2,
                    "entries": [{
                        "name": "TOKEN", "secret": true,
                        "description": "bulk metadata"
                    }]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(bulk.status(), StatusCode::OK);
        let bulk: serde_json::Value = test::read_body_json(bulk).await;
        assert_eq!(bulk["revision"], 3);
        assert_eq!(bulk["entries"][0]["configured"], true);

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 1,
                    "name": "TOKEN",
                    "value": "loser",
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);

        let missing_new = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 3,
                    "name": "NEW_TOKEN",
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(missing_new.status(), StatusCode::BAD_REQUEST);

        for field in ["credential_ref", "configured", "value_encrypted"] {
            let spoof = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/env-vars")
                    .set_json(serde_json::json!({
                        "expected_revision": 3,
                        "name": "TOKEN",
                        "secret": true,
                        (field): "spoof"
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(spoof.status(), StatusCode::BAD_REQUEST, "{field}");
        }

        let masked = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 3,
                    "name": "TOKEN",
                    "value": "****...****",
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(masked.status(), StatusCode::BAD_REQUEST);

        let clear = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 3,
                    "name": "TOKEN",
                    "value": "",
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(clear.status(), StatusCode::OK);
        let clear: serde_json::Value = test::read_body_json(clear).await;
        assert_eq!(clear["revision"], 4);
        assert_eq!(clear["entries"][0]["configured"], false);

        let non_secret_create = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 4,
                    "name": "PUBLIC_VALUE",
                    "value": "one",
                    "secret": false
                }))
                .to_request(),
        )
        .await;
        let non_secret_status = non_secret_create.status();
        let non_secret_create: serde_json::Value = test::read_body_json(non_secret_create).await;
        assert_eq!(
            non_secret_status,
            StatusCode::OK,
            "unexpected response: {non_secret_create}"
        );
        assert_eq!(non_secret_create["revision"], 5);

        let non_secret_update = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 5,
                    "name": "PUBLIC_VALUE",
                    "value": "two",
                    "secret": false
                }))
                .to_request(),
        )
        .await;
        assert_eq!(non_secret_update.status(), StatusCode::OK);
        let non_secret_update: serde_json::Value = test::read_body_json(non_secret_update).await;
        assert_eq!(non_secret_update["revision"], 6);

        let root = std::fs::read_to_string(dir.path().join("env.json")).unwrap();
        assert!(!root.contains("super-secret-value"));
        assert!(!root.contains("value_encrypted"));
        assert!(!root.contains("****...****"));
    }

    #[actix_web::test]
    async fn cancelled_caller_finishes_once_and_old_revision_retry_cannot_replay() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let mut feed = state.account_sink.subscribe();
        let worker_state = state.clone();
        let caller = tokio::spawn(async move {
            worker_state
                .update_env_var_credentials(
                    0,
                    std::collections::BTreeSet::from(["TOKEN".to_string()]),
                    |config| {
                        config.env_vars.push(EnvVarEntry {
                            name: "TOKEN".to_string(),
                            value: "cancel-safe".to_string(),
                            secret: true,
                            value_encrypted: None,
                            credential_ref: None,
                            configured: true,
                            description: None,
                        });
                        Ok(())
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        caller.abort();

        for _ in 0..100 {
            if state.credential_store.revision().unwrap() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(state.credential_store.revision().unwrap(), 1);
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision: 1 }
                        if section == "env"
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
                if section == "env"
        ));
        let reference = bamboo_config::credential_ref("env", "TOKEN", "value").unwrap();
        assert_eq!(
            state
                .credential_store
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "cancel-safe"
        );
        let retry = state
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                |_| Ok(()),
            )
            .await;
        assert!(matches!(
            retry,
            Err(AppError::ConfigConflict { actual: 1, .. })
        ));
        assert_eq!(state.credential_store.revision().unwrap(), 1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), feed.recv())
                .await
                .is_err()
        );
    }

    #[actix_web::test]
    async fn committed_reload_preserves_external_root_rebase_in_live_snapshot() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        bamboo_config::set_env_transaction_test_hook(|data_dir| {
            let path = data_dir.join("config.json");
            let mut root: serde_json::Value = std::fs::read(&path)
                .ok()
                .map(|bytes| serde_json::from_slice(&bytes).unwrap())
                .unwrap_or_else(|| serde_json::json!({}));
            root.as_object_mut().unwrap().insert(
                "external_root_marker".to_string(),
                serde_json::Value::String("preserved".to_string()),
            );
            std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        });
        let (committed, revision) = state
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                |config| {
                    config.env_vars.push(EnvVarEntry {
                        name: "TOKEN".to_string(),
                        value: "secret".to_string(),
                        secret: true,
                        value_encrypted: None,
                        credential_ref: None,
                        configured: true,
                        description: None,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(revision, 1);
        assert_eq!(committed.extra["external_root_marker"], "preserved");
        assert_eq!(
            state.config.read().await.extra["external_root_marker"],
            "preserved"
        );
        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                .unwrap();
        assert_eq!(root["external_root_marker"], "preserved");
    }
}
