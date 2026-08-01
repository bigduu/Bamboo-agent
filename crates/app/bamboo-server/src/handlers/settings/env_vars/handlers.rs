use actix_web::{web, HttpResponse};
use std::collections::BTreeMap;

use crate::{app_state::AppState, error::AppError};
use bamboo_config::EnvVarEntry;

use super::{
    types::{
        DeleteEnvVarQuery, EnvVarResponse, EnvVarsListResponse, ReplaceEnvVarsRequest,
        UpsertEnvVarRequest,
    },
    validation::{check_duplicate_names, validate_env_var_name, validate_env_var_value},
};
use crate::handlers::settings::credential_action::CredentialAction;

#[derive(Clone)]
enum EnvSecretChange {
    Keep,
    Replace(String),
    Clear,
}

fn resolve_env_secret_change(
    entry: &super::types::EnvVarInput,
) -> Result<Option<EnvSecretChange>, AppError> {
    if !entry.secret {
        if entry.credential_change.is_some() {
            return Err(AppError::BadRequest(
                "credential_change is valid only for secret environment variables".to_string(),
            ));
        }
        return Ok(None);
    }
    if entry.credential_change.is_some() && entry.value.is_some() {
        return Err(AppError::BadRequest(
            "secret environment variables must use either credential_change or the legacy value field, not both"
                .to_string(),
        ));
    }
    let change = match entry.credential_change.as_ref() {
        Some(action) => {
            action.validate("environment credential")?;
            match action {
                CredentialAction::Keep => EnvSecretChange::Keep,
                CredentialAction::Replace { value } => EnvSecretChange::Replace(value.clone()),
                CredentialAction::Clear => EnvSecretChange::Clear,
            }
        }
        None => match entry.value.as_ref() {
            Some(value) if value.is_empty() => EnvSecretChange::Clear,
            Some(value) => EnvSecretChange::Replace(value.clone()),
            None => EnvSecretChange::Keep,
        },
    };
    Ok(Some(change))
}

fn env_response(
    config: &bamboo_config::Config,
    revision: u64,
    statuses: &[bamboo_config::CredentialStatus],
    section: bamboo_config::SectionEnvelope<serde_json::Value>,
    credential_health: bamboo_config::CredentialStoreHealth,
) -> EnvVarsListResponse {
    let statuses = statuses
        .iter()
        .map(|status| (status.credential_ref.clone(), status))
        .collect::<BTreeMap<_, _>>();
    let entries = config
        .env_vars
        .iter()
        .map(|entry| {
            EnvVarResponse::from_entry(
                entry,
                entry
                    .credential_ref
                    .as_ref()
                    .and_then(|reference| statuses.get(reference).copied()),
                &credential_health,
            )
        })
        .collect();
    EnvVarsListResponse {
        revision,
        entries,
        section,
        credential_health,
    }
}

/// `GET /bamboo/env-vars` – list all env vars without secret values or masks.
pub async fn list_env_vars(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let exact = app_state
        .read_exact_credential_section(bamboo_config::SectionId::Env)
        .await?;
    let section = exact.section;
    let revision = section.revision;
    Ok(HttpResponse::Ok().json(env_response(
        &exact.config,
        revision,
        &exact.metadata.credential_statuses,
        section,
        exact.metadata.credential_health,
    )))
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
    let secret_change = resolve_env_secret_change(&req)?;
    let name = req.name.clone();

    let (updated, revision, metadata, section) = app_state
        .update_env_var_credentials(
            expected_revision,
            std::collections::BTreeSet::from([name]),
            false,
            move |cfg| {
                // Replace existing or push new.
                if let Some(existing) = cfg.env_vars.iter_mut().find(|e| e.name == req.name) {
                    let was_secret = existing.secret;
                    if req.secret {
                        match secret_change
                            .as_ref()
                            .expect("secret input has a resolved credential action")
                        {
                            EnvSecretChange::Keep => {
                                if !was_secret {
                                    return Err(AppError::BadRequest(
                                        "converting a plain environment variable to secret requires replace"
                                            .to_string(),
                                    ));
                                }
                                // Preserve is represented to the transaction by
                                // configured metadata without re-supplying plaintext.
                                existing.value.clear();
                            }
                            EnvSecretChange::Replace(value) => {
                                existing.value = value.clone();
                                existing.configured = true;
                            }
                            EnvSecretChange::Clear => {
                                existing.value.clear();
                                existing.configured = false;
                            }
                        }
                    } else {
                        if was_secret && req.value.is_none() {
                            return Err(AppError::BadRequest(
                                "converting a secret environment variable to plain requires an explicit value"
                                    .to_string(),
                            ));
                        }
                        if let Some(value) = req.value.as_ref() {
                            existing.value = value.clone();
                            existing.configured = !value.is_empty();
                        }
                    }
                    existing.secret = req.secret;
                    existing.value_encrypted = None;
                    existing.description = req.description.clone();
                } else {
                    let value = if req.secret {
                        match secret_change
                            .as_ref()
                            .expect("secret input has a resolved credential action")
                        {
                            EnvSecretChange::Replace(value) => value.clone(),
                            EnvSecretChange::Keep | EnvSecretChange::Clear => {
                                return Err(AppError::BadRequest(
                                    "a new secret environment variable requires replace"
                                        .to_string(),
                                ));
                            }
                        }
                    } else {
                        req.value.clone().unwrap_or_default()
                    };
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

    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "env mutation completed without a typed section envelope"
        ))
    })?;
    Ok(HttpResponse::Ok().json(env_response(
        &updated,
        revision,
        &metadata.credential_statuses,
        section,
        metadata.credential_health,
    )))
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
    let requested_entries = req
        .entries
        .into_iter()
        .map(|entry| {
            let secret_change = resolve_env_secret_change(&entry)?;
            Ok::<_, AppError>((entry, secret_change))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let intents = requested_entries
        .iter()
        .map(|(entry, _)| entry.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let (updated, revision, metadata, section) = app_state
        .update_env_var_credentials(expected_revision, intents, true, move |cfg| {
            let mut new_entries = Vec::with_capacity(requested_entries.len());
            for (entry, secret_change) in &requested_entries {
                let existing = cfg
                    .env_vars
                    .iter()
                    .find(|current| current.name == entry.name);
                let value = if entry.secret {
                    match secret_change
                        .as_ref()
                        .expect("secret input has a resolved credential action")
                    {
                        EnvSecretChange::Keep => {
                            let Some(existing) = existing.filter(|current| current.secret) else {
                                return Err(AppError::BadRequest(
                                    "a new or newly-secret environment variable requires replace"
                                        .to_string(),
                                ));
                            };
                            let _ = existing;
                            String::new()
                        }
                        EnvSecretChange::Replace(value) => value.clone(),
                        EnvSecretChange::Clear => {
                            if existing.is_none() {
                                return Err(AppError::BadRequest(
                                    "a new secret environment variable requires replace"
                                        .to_string(),
                                ));
                            }
                            String::new()
                        }
                    }
                } else {
                    if existing.is_some_and(|current| current.secret) && entry.value.is_none() {
                        return Err(AppError::BadRequest(
                            "converting a secret environment variable to plain requires an explicit value"
                                .to_string(),
                        ));
                    }
                    entry
                        .value
                        .clone()
                        .or_else(|| existing.map(|current| current.value.clone()))
                        .unwrap_or_default()
                };
                new_entries.push(EnvVarEntry {
                    name: entry.name.clone(),
                    value: value.clone(),
                    secret: entry.secret,
                    value_encrypted: None,
                    credential_ref: existing.and_then(|current| current.credential_ref.clone()),
                    configured: if entry.secret {
                        match secret_change
                            .as_ref()
                            .expect("secret input has a resolved credential action")
                        {
                            EnvSecretChange::Keep => {
                                existing.is_some_and(|current| current.configured)
                            }
                            EnvSecretChange::Replace(_) => true,
                            EnvSecretChange::Clear => false,
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

    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "env mutation completed without a typed section envelope"
        ))
    })?;
    Ok(HttpResponse::Ok().json(env_response(
        &updated,
        revision,
        &metadata.credential_statuses,
        section,
        metadata.credential_health,
    )))
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

    let (updated, revision, metadata, section) = app_state
        .update_env_var_credentials(
            expected_revision,
            std::collections::BTreeSet::from([intent]),
            false,
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

    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "env mutation completed without a typed section envelope"
        ))
    })?;
    Ok(HttpResponse::Ok().json(env_response(
        &updated,
        revision,
        &metadata.credential_statuses,
        section,
        metadata.credential_health,
    )))
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
                    "credential_change": {
                        "action": "replace",
                        "value": "super-secret-value"
                    },
                    "secret": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["revision"], 1);
        assert_eq!(body["section"]["revision"], 1);
        assert_eq!(body["credential_health"]["revision"], 1);
        assert!(body["entries"][0].get("value").is_none());
        assert_eq!(body["entries"][0]["configured"], true);
        assert_eq!(body["entries"][0]["credential_state"], "configured");
        assert_eq!(body["entries"][0]["credential_ref"], "env.TOKEN.value");
        assert_eq!(body["entries"][0]["source"], "user");
        let rendered = body.to_string();
        assert!(!rendered.contains("super-secret-value"));
        assert!(!rendered.contains("****"));

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
                    "credential_change": {"action": "clear"},
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

        let invalid_plain_to_secret_keep = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 6,
                    "name": "PUBLIC_VALUE",
                    "secret": true,
                    "credential_change": {"action": "keep"}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            invalid_plain_to_secret_keep.status(),
            StatusCode::BAD_REQUEST
        );

        let convert_to_secret = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 6,
                    "name": "PUBLIC_VALUE",
                    "secret": true,
                    "credential_change": {
                        "action": "replace",
                        "value": "converted-secret"
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(convert_to_secret.status(), StatusCode::OK);
        let convert_to_secret: serde_json::Value = test::read_body_json(convert_to_secret).await;
        assert_eq!(convert_to_secret["revision"], 7);
        assert!(convert_to_secret["entries"][1].get("value").is_none());
        assert!(!convert_to_secret.to_string().contains("converted-secret"));

        let unsafe_secret_to_plain = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 7,
                    "name": "PUBLIC_VALUE",
                    "secret": false
                }))
                .to_request(),
        )
        .await;
        assert_eq!(unsafe_secret_to_plain.status(), StatusCode::BAD_REQUEST);

        let convert_to_plain = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars")
                .set_json(serde_json::json!({
                    "expected_revision": 7,
                    "name": "PUBLIC_VALUE",
                    "secret": false,
                    "value": "public-after-secret"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(convert_to_plain.status(), StatusCode::OK);
        let convert_to_plain: serde_json::Value = test::read_body_json(convert_to_plain).await;
        assert_eq!(convert_to_plain["revision"], 8);
        assert_eq!(
            convert_to_plain["entries"][1]["value"],
            "public-after-secret"
        );

        let root = std::fs::read_to_string(dir.path().join("env.json")).unwrap();
        assert!(!root.contains("super-secret-value"));
        assert!(!root.contains("converted-secret"));
        assert!(!root.contains("value_encrypted"));
        assert!(!root.contains("****...****"));
    }

    #[actix_web::test]
    async fn stale_full_replace_clears_omitted_durable_secret_and_credential_record() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let mut stale = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stale.stop_config_watcher_for_test();
        let writer = AppState::new(dir.path().to_path_buf()).await.unwrap();
        writer
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["EXTERNAL_SECRET".to_string()]),
                false,
                |config| {
                    config.env_vars.push(EnvVarEntry {
                        name: "EXTERNAL_SECRET".to_string(),
                        value: "external-secret-value".to_string(),
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
        assert!(stale.config.read().await.env_vars.is_empty());

        let stale_store = stale.credential_store.clone();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(stale))
                .route("/env-vars/replace", web::post().to(replace_env_vars)),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/env-vars/replace")
                .set_json(serde_json::json!({
                    "expected_revision": 1,
                    "entries": [{
                        "name": "PUBLIC_VALUE",
                        "value": "public",
                        "secret": false
                    }]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["revision"], 2);
        assert_eq!(body["entries"].as_array().unwrap().len(), 1);
        assert_eq!(body["entries"][0]["name"], "PUBLIC_VALUE");

        let reference = bamboo_config::credential_ref("env", "EXTERNAL_SECRET", "value").unwrap();
        assert!(!stale_store.status(&reference).unwrap().configured);
        assert!(stale_store.resolve(&reference).unwrap().is_none());
        let durable: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("env.json")).unwrap()).unwrap();
        assert_eq!(durable["revision"], 2);
        assert_eq!(durable["data"].as_array().unwrap().len(), 1);
        assert!(
            !std::fs::read_to_string(dir.path().join("credentials.json"))
                .unwrap()
                .contains("EXTERNAL_SECRET")
        );
    }

    #[actix_web::test]
    async fn stale_process_get_pairs_latest_env_section_with_same_credential_generation() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let writer = AppState::new(dir.path().to_path_buf()).await.unwrap();
        writer
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                false,
                |config| {
                    config.env_vars.push(EnvVarEntry {
                        name: "TOKEN".to_string(),
                        value: "first-generation-secret".to_string(),
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
        let mut stale = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stale.stop_config_watcher_for_test();
        assert_eq!(stale.config.read().await.env_vars.len(), 1);

        writer
            .update_env_var_credentials(
                1,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                false,
                |config| {
                    config.env_vars.clear();
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(stale.config.read().await.env_vars.len(), 1);

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(stale))
                .route("/env-vars", web::get().to(list_env_vars)),
        )
        .await;
        let body: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/env-vars").to_request(),
        )
        .await;
        assert_eq!(body["revision"], 2);
        assert_eq!(body["section"]["revision"], 2);
        assert_eq!(body["credential_health"]["revision"], 2);
        assert!(body["entries"].as_array().unwrap().is_empty());
        assert!(!body.to_string().contains("env.TOKEN.value"));
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
                    false,
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
                false,
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
    async fn committed_env_write_preserves_external_root_bytes_without_cross_section_adoption() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let baseline_seq = state.account_sink.latest_seq();
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
        let (committed, revision, _, _) = state
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                false,
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
        assert!(
            !committed.extra.contains_key("external_root_marker"),
            "an unrevisioned external root field must not be installed with the env commit"
        );
        assert!(
            !state
                .config
                .read()
                .await
                .extra
                .contains_key("external_root_marker"),
            "the live process must advance only the owned env section"
        );
        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                .unwrap();
        assert_eq!(root["external_root_marker"], "preserved");
        let facade = state.config_facade.as_ref().unwrap();
        assert_eq!(facade.registry().core.snapshot().revision, 0);
        assert_eq!(facade.registry().env.snapshot().revision, 1);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert!(!events.iter().any(|event| matches!(
            &event.event,
            bamboo_agent_core::AgentEvent::ConfigChanged { section, .. } if section == "core"
        )));
    }

    #[actix_web::test]
    async fn post_manifest_section_rebase_returns_and_publishes_actual_revision() {
        let _serial = encryption_test_lock().lock().await;
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let baseline_seq = state.account_sink.latest_seq();
        bamboo_config::set_env_transaction_test_hook(|data_dir| {
            let path = data_dir.join("env.json");
            let mut envelope: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(envelope["revision"], 0);
            envelope["revision"] = serde_json::Value::from(1);
            envelope["data"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "EXTERNAL",
                    "value": "external-winner",
                    "secret": false,
                    "configured": true,
                    "description": "post-manifest generation"
                }));
            std::fs::write(path, serde_json::to_vec_pretty(&envelope).unwrap()).unwrap();
        });

        let (committed, revision, _, section) = state
            .update_env_var_credentials(
                0,
                std::collections::BTreeSet::from(["TOKEN".to_string()]),
                false,
                |config| {
                    config.env_vars.push(EnvVarEntry {
                        name: "TOKEN".to_string(),
                        value: "transaction-secret".to_string(),
                        secret: true,
                        value_encrypted: None,
                        credential_ref: None,
                        configured: true,
                        description: Some("transaction generation".to_string()),
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(revision, 2);
        let section = section.expect("exact committed envelope");
        assert_eq!(section.revision, revision);
        let returned_names = section
            .data
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["name"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            returned_names,
            std::collections::BTreeSet::from(["EXTERNAL", "TOKEN"])
        );
        let live = state.config.read().await.clone();
        for config in [&committed, &live] {
            assert!(config.env_vars.iter().any(|entry| {
                entry.name == "EXTERNAL" && entry.value == "external-winner" && !entry.secret
            }));
            assert!(config
                .env_vars
                .iter()
                .any(|entry| entry.name == "TOKEN" && entry.value == "transaction-secret"));
        }

        let facade = state.config_facade.as_ref().unwrap();
        assert_eq!(facade.registry().env.snapshot().revision, revision);
        let durable: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("env.json")).unwrap()).unwrap();
        assert_eq!(durable["revision"], revision);
        assert_eq!(
            durable["data"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|entry| entry["name"].as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["EXTERNAL", "TOKEN"])
        );
        let credentials = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
        assert!(!credentials.contains("transaction-secret"));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let env_revisions = events
            .iter()
            .filter_map(|event| match &event.event {
                bamboo_agent_core::AgentEvent::ConfigChanged { section, revision }
                    if section == "env" =>
                {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(env_revisions, vec![revision]);
    }
}
