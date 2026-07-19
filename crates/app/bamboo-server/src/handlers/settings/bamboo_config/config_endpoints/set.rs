use crate::config_manager;
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};
use serde_json::Value;

use super::common::{redacted_config_json, take_model_limits_patch, write_model_limits_file};

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
    let model_limits_patch = take_model_limits_patch(&mut patch_obj);
    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);
    let effects = config_manager::effects_for_root_patch(&patch_obj);
    let provider_credential_intents = api_key_intents.providers.clone();
    let provider_instance_credential_intents = api_key_intents.provider_instances.clone();
    if (!provider_credential_intents.is_empty() || !provider_instance_credential_intents.is_empty())
        && model_limits_patch.is_some()
    {
        return Err(AppError::BadRequest(
            "provider credential updates cannot be combined with model_limits changes; split the request"
                .to_string(),
        ));
    }

    // Apply the patch under the config write lock to avoid clobbering concurrent updates.
    let new_config = app_state
        .update_config_with_provider_credentials(
            move |config| {
                let current = config.clone();
                let mut patch_obj = patch_obj;
                config_manager::preserve_masked_provider_api_keys(&mut patch_obj, &current);
                config_manager::preserve_masked_notification_secrets(&mut patch_obj, &current);
                config_manager::preserve_masked_connect_secrets(&mut patch_obj, &current);
                let mut new_config = config_manager::build_merged_config(&current, patch_obj)?;
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
}
