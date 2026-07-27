use actix_web::{web, HttpResponse};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    app_state::AppState,
    error::AppError,
    handlers::settings::credential_action::{credential_status_view, CredentialAction},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectMutationRequest {
    expected_revision: u64,
    data: ConnectMutationData,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectMutationData {
    platforms: Vec<ConnectPlatformMutation>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectPlatformMutation {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    project_id: Option<bamboo_domain::ProjectId>,
    #[serde(rename = "type")]
    platform_type: String,
    #[serde(default)]
    app_id: Option<String>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    allow_from: Vec<String>,
    #[serde(default)]
    admin_from: Vec<String>,
    #[serde(default)]
    token_change: Option<CredentialAction>,
    #[serde(default)]
    app_secret_change: Option<CredentialAction>,
}

fn existing_platform<'a>(
    current: &'a bamboo_config::ConnectConfig,
    candidate: &ConnectPlatformMutation,
    index: usize,
) -> Option<&'a bamboo_config::ConnectPlatformConfig> {
    candidate
        .id
        .as_deref()
        .and_then(|id| {
            current
                .platforms
                .iter()
                .find(|platform| platform.id.as_deref() == Some(id))
        })
        .or_else(|| {
            current
                .platforms
                .get(index)
                .filter(|platform| platform.platform_type == candidate.platform_type)
        })
        .or_else(|| {
            let mut matches = current
                .platforms
                .iter()
                .filter(|platform| platform.platform_type == candidate.platform_type);
            let first = matches.next();
            first.filter(|_| matches.next().is_none())
        })
}

fn apply_action(
    action: Option<&CredentialAction>,
    current_value: Option<String>,
    current_ref: Option<bamboo_config::CredentialRef>,
    current_configured: bool,
) -> (Option<String>, Option<bamboo_config::CredentialRef>, bool) {
    match action {
        None | Some(CredentialAction::Keep) => (current_value, current_ref, current_configured),
        Some(CredentialAction::Replace { value }) => (Some(value.clone()), current_ref, true),
        Some(CredentialAction::Clear) => (None, current_ref, false),
    }
}

fn connect_response(
    section: bamboo_config::SectionEnvelope<serde_json::Value>,
    config: &bamboo_config::Config,
    statuses: Vec<bamboo_config::CredentialStatus>,
    credential_health: bamboo_config::CredentialStoreHealth,
) -> serde_json::Value {
    let statuses = statuses
        .into_iter()
        .map(|status| (status.credential_ref.clone(), status))
        .collect::<BTreeMap<_, _>>();
    let active_refs = config
        .connect
        .platforms
        .iter()
        .flat_map(|platform| {
            [
                platform.token_credential_ref.clone(),
                platform.app_secret_credential_ref.clone(),
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let credentials = active_refs
        .into_iter()
        .map(|reference| {
            credential_status_view(
                Some(&reference),
                true,
                statuses.get(&reference),
                &credential_health,
            )
        })
        .collect::<Vec<_>>();
    let credential_status = config
        .connect
        .platforms
        .iter()
        .enumerate()
        .map(|(index, platform)| {
            let key = platform
                .id
                .clone()
                .unwrap_or_else(|| format!("index-{index}"));
            let token_status = platform
                .token_credential_ref
                .as_ref()
                .and_then(|reference| statuses.get(reference));
            let app_secret_status = platform
                .app_secret_credential_ref
                .as_ref()
                .and_then(|reference| statuses.get(reference));
            (
                key,
                serde_json::json!({
                    "token": credential_status_view(
                        platform.token_credential_ref.as_ref(),
                        platform.token_configured,
                        token_status,
                        &credential_health,
                    ),
                    "app_secret": credential_status_view(
                        platform.app_secret_credential_ref.as_ref(),
                        platform.app_secret_configured,
                        app_secret_status,
                        &credential_health,
                    ),
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    serde_json::json!({
        "revision": section.revision,
        "section": section,
        "credentials": credentials,
        "credential_status": credential_status,
        "credential_health": credential_health,
    })
}

/// Return the Connect section and one secret-free credential status snapshot.
pub async fn get_connect_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let exact = app_state
        .read_exact_credential_section(bamboo_config::SectionId::Connect)
        .await?;
    Ok(HttpResponse::Ok().json(connect_response(
        exact.section,
        &exact.config,
        exact.metadata.credential_statuses,
        exact.metadata.credential_health,
    )))
}

/// Replace Connect metadata and explicitly keep, replace, or clear platform
/// secrets using only the Connect section revision as the public CAS token.
pub async fn put_connect_config(
    app_state: web::Data<AppState>,
    payload: web::Json<ConnectMutationRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    let mut ids = BTreeSet::new();
    let mut intents = bamboo_config::patch::ConnectSecretIntents::default();
    for (index, platform) in payload.data.platforms.iter().enumerate() {
        if platform
            .id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty() || !ids.insert(id.to_string()))
        {
            return Err(AppError::BadRequest(
                "connect platform ids must be nonempty and unique".to_string(),
            ));
        }
        if platform.platform_type.trim().is_empty() {
            return Err(AppError::BadRequest(
                "connect platform type must be nonempty".to_string(),
            ));
        }
        for (label, action, target) in [
            (
                "connect token",
                platform.token_change.as_ref(),
                &mut intents.token,
            ),
            (
                "connect app secret",
                platform.app_secret_change.as_ref(),
                &mut intents.app_secret,
            ),
        ] {
            if let Some(action) = action {
                action.validate(label)?;
                if !action.is_keep() {
                    target.insert(index);
                }
            }
        }
    }

    let expected_revision = payload.expected_revision;
    let data = payload.data;
    let (config, _, metadata, section) = app_state
        .update_connect_credentials(expected_revision, intents, move |config| {
            let current = config.connect.clone();
            let mut platforms = Vec::with_capacity(data.platforms.len());
            for (index, input) in data.platforms.into_iter().enumerate() {
                let existing = existing_platform(&current, &input, index);
                let (token, token_credential_ref, token_configured) = apply_action(
                    input.token_change.as_ref(),
                    existing.and_then(|platform| platform.token.clone()),
                    existing.and_then(|platform| platform.token_credential_ref.clone()),
                    existing.is_some_and(|platform| platform.token_configured),
                );
                let (app_secret, app_secret_credential_ref, app_secret_configured) = apply_action(
                    input.app_secret_change.as_ref(),
                    existing.and_then(|platform| platform.app_secret.clone()),
                    existing.and_then(|platform| platform.app_secret_credential_ref.clone()),
                    existing.is_some_and(|platform| platform.app_secret_configured),
                );
                platforms.push(bamboo_config::ConnectPlatformConfig {
                    id: input.id,
                    project_id: input.project_id,
                    platform_type: input.platform_type,
                    token,
                    token_encrypted: None,
                    token_credential_ref,
                    token_configured,
                    app_id: input.app_id,
                    app_secret,
                    app_secret_encrypted: None,
                    app_secret_credential_ref,
                    app_secret_configured,
                    domain: input.domain,
                    allow_from: input.allow_from,
                    admin_from: input.admin_from,
                });
            }
            config.connect = bamboo_config::ConnectConfig { platforms };
            Ok(())
        })
        .await?;
    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "connect mutation completed without a typed section envelope"
        ))
    })?;
    Ok(HttpResponse::Ok().json(connect_response(
        section,
        &config,
        metadata.credential_statuses,
        metadata.credential_health,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};

    #[actix_web::test]
    async fn action_debug_never_exposes_connect_secrets() {
        let payload: ConnectMutationRequest = serde_json::from_value(serde_json::json!({
            "expected_revision": 0,
            "data": {
                "platforms": [{
                    "type": "telegram",
                    "token_change": {"action": "replace", "value": "telegram-secret"}
                }]
            }
        }))
        .unwrap();
        let debug = format!("{payload:?}");
        assert!(!debug.contains("telegram-secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[actix_web::test]
    async fn dedicated_connect_mutation_uses_section_cas_and_explicit_actions() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0xc5; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let mut events = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/connect", web::put().to(put_connect_config)),
        )
        .await;

        let replace = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/connect")
                .set_json(serde_json::json!({
                    "expected_revision": 0,
                    "data": {
                        "platforms": [{
                            "type": "telegram",
                            "allow_from": ["owner"],
                            "token_change": {
                                "action": "replace",
                                "value": "telegram-token-secret"
                            }
                        }]
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(replace.status(), StatusCode::OK);
        let replace: serde_json::Value = test::read_body_json(replace).await;
        assert_eq!(replace["revision"], 1);
        assert_eq!(replace["section"]["revision"], 1);
        assert_eq!(replace["credentials"][0]["configured"], true);
        assert_eq!(replace["credentials"][0]["state"], "configured");
        assert!(!replace.to_string().contains("telegram-token-secret"));
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision: 1 }
                        if section == "connect"
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
                if section == "connect"
        ));
        let platform_id = replace["section"]["data"]["platforms"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/connect")
                .set_json(serde_json::json!({
                    "expected_revision": 0,
                    "data": {
                        "platforms": [{
                            "id": platform_id,
                            "type": "telegram",
                            "allow_from": ["stale"],
                            "token_change": {
                                "action": "replace",
                                "value": "stale-telegram-secret"
                            }
                        }]
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(!String::from_utf8(test::read_body(stale).await.to_vec())
            .unwrap()
            .contains("stale-telegram-secret"));

        let keep = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/connect")
                .set_json(serde_json::json!({
                    "expected_revision": 1,
                    "data": {
                        "platforms": [{
                            "id": platform_id,
                            "type": "telegram",
                            "allow_from": ["owner", "maintainer"],
                            "token_change": {"action": "keep"}
                        }]
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(keep.status(), StatusCode::OK);
        let keep: serde_json::Value = test::read_body_json(keep).await;
        assert_eq!(keep["section"]["revision"], 2);
        assert_eq!(
            state.config.read().await.connect.platforms[0]
                .token
                .as_deref(),
            Some("telegram-token-secret")
        );

        let clear = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/connect")
                .set_json(serde_json::json!({
                    "expected_revision": 2,
                    "data": {
                        "platforms": [{
                            "id": platform_id,
                            "type": "telegram",
                            "allow_from": ["owner", "maintainer"],
                            "token_change": {"action": "clear"}
                        }]
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(clear.status(), StatusCode::OK);
        let clear: serde_json::Value = test::read_body_json(clear).await;
        assert_eq!(clear["section"]["revision"], 3);
        assert!(clear["credentials"].as_array().unwrap().is_empty());
        assert_eq!(
            clear["credential_status"][&platform_id]["token"]["state"],
            "missing"
        );
        assert!(state.config.read().await.connect.platforms[0]
            .token
            .is_none());

        let masked = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/connect")
                .set_json(serde_json::json!({
                    "expected_revision": 3,
                    "data": {
                        "platforms": [{
                            "id": platform_id,
                            "type": "telegram",
                            "token_change": {
                                "action": "replace",
                                "value": "********"
                            }
                        }]
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(masked.status(), StatusCode::BAD_REQUEST);

        let connect_file = std::fs::read_to_string(dir.path().join("connect.json")).unwrap();
        let credential_file = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
        for secret in ["telegram-token-secret", "stale-telegram-secret", "********"] {
            assert!(!connect_file.contains(secret));
            assert!(!credential_file.contains(secret));
        }
    }
}
