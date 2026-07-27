use actix_web::{web, HttpResponse};
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::handlers::settings::credential_action::{
    credential_status_view, CredentialAction, CredentialStatusView,
};
use crate::{app_state::AppState, error::AppError};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationMutationRequest {
    expected_revision: u64,
    data: NotificationMutationData,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationMutationData {
    desktop: DesktopMutationData,
    ntfy: NtfyMutationData,
    bark: BarkMutationData,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesktopMutationData {
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NtfyMutationData {
    enabled: bool,
    base_url: String,
    topic: String,
    #[serde(default)]
    credential_change: Option<CredentialAction>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarkMutationData {
    enabled: bool,
    base_url: String,
    #[serde(default)]
    credential_change: Option<CredentialAction>,
}

fn channel_status_from_snapshot(
    reference: Option<bamboo_config::CredentialRef>,
    expected_configured: bool,
    statuses: &BTreeMap<bamboo_config::CredentialRef, bamboo_config::CredentialStatus>,
    health: &bamboo_config::CredentialStoreHealth,
) -> CredentialStatusView {
    let status = reference
        .as_ref()
        .and_then(|reference| statuses.get(reference));
    credential_status_view(reference.as_ref(), expected_configured, status, health)
}

fn notification_response(
    section: bamboo_config::SectionEnvelope<serde_json::Value>,
    config: &bamboo_config::Config,
    statuses: Vec<bamboo_config::CredentialStatus>,
    credential_health: bamboo_config::CredentialStoreHealth,
) -> serde_json::Value {
    let statuses = statuses
        .into_iter()
        .map(|status| (status.credential_ref.clone(), status))
        .collect::<BTreeMap<_, _>>();
    let ntfy = channel_status_from_snapshot(
        config.notifications.ntfy.credential_ref.clone(),
        config.notifications.ntfy.configured,
        &statuses,
        &credential_health,
    );
    let bark = channel_status_from_snapshot(
        config.notifications.bark.credential_ref.clone(),
        config.notifications.bark.configured,
        &statuses,
        &credential_health,
    );
    serde_json::json!({
        "revision": section.revision,
        "status": section.status,
        "source": section.source_kind,
        "source_path": section.source_path,
        "loaded_at": section.loaded_at,
        "last_error": section.last_error,
        "section": section.clone(),
        "credential_revision": credential_health.revision,
        "credential_status": credential_health.status,
        "credential_source": credential_health.source,
        "credential_last_error": credential_health.last_error,
        "credential_health": credential_health,
        "data": {
            "desktop": { "enabled": config.notifications.desktop.enabled },
            "ntfy": {
                "enabled": config.notifications.ntfy.enabled,
                "base_url": config.notifications.ntfy.base_url,
                "topic": config.notifications.ntfy.topic,
                "credential": ntfy,
            },
            "bark": {
                "enabled": config.notifications.bark.enabled,
                "base_url": config.notifications.bark.base_url,
                "credential": bark,
            }
        }
    })
}

/// Metadata-only notification configuration and credential status. Secret
/// values, ciphertext and masks are never returned.
pub async fn get_notification_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let facade = app_state.config_facade.as_ref().ok_or_else(|| {
        AppError::BadRequest(
            "notification settings require the modular configuration facade".to_string(),
        )
    })?;
    let section = facade
        .registry()
        .envelope_value(bamboo_config::SectionId::Notifications)
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "notification section envelope unavailable: {error}"
            ))
        })?;
    let config = app_state.config.read().await.clone();
    let (statuses, credential_health) = app_state
        .credential_store
        .statuses_with_health()
        .map_err(super::credentials::map_store_read_error)?;
    Ok(HttpResponse::Ok().json(notification_response(
        section,
        &config,
        statuses,
        credential_health,
    )))
}

/// Replace notification metadata and explicitly keep, replace, or clear each
/// channel credential using the Notifications section revision as the only
/// public precondition.
pub async fn put_notification_config(
    app_state: web::Data<AppState>,
    payload: web::Json<NotificationMutationRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    for (label, action) in [
        (
            "ntfy credential",
            payload.data.ntfy.credential_change.as_ref(),
        ),
        (
            "Bark credential",
            payload.data.bark.credential_change.as_ref(),
        ),
    ] {
        if let Some(action) = action {
            action.validate(label)?;
        }
    }

    let mut intents = std::collections::BTreeSet::new();
    if payload
        .data
        .ntfy
        .credential_change
        .as_ref()
        .is_some_and(|action| !action.is_keep())
    {
        intents.insert("ntfy".to_string());
    }
    if payload
        .data
        .bark
        .credential_change
        .as_ref()
        .is_some_and(|action| !action.is_keep())
    {
        intents.insert("bark".to_string());
    }

    let expected_revision = payload.expected_revision;
    let data = payload.data;
    let (config, _, metadata, section) = app_state
        .update_notification_credentials(expected_revision, intents, false, move |config| {
            let current = config.notifications.clone();
            let mut candidate = bamboo_config::NotificationsConfig {
                desktop: bamboo_config::DesktopChannelConfig {
                    enabled: data.desktop.enabled,
                },
                ntfy: bamboo_config::NtfyChannelConfig {
                    enabled: data.ntfy.enabled,
                    base_url: data.ntfy.base_url,
                    topic: data.ntfy.topic,
                    ..Default::default()
                },
                bark: bamboo_config::BarkChannelConfig {
                    enabled: data.bark.enabled,
                    base_url: data.bark.base_url,
                    ..Default::default()
                },
            };
            match data.ntfy.credential_change.as_ref() {
                None | Some(CredentialAction::Keep) => {
                    candidate.ntfy.token = current.ntfy.token;
                    candidate.ntfy.credential_ref = current.ntfy.credential_ref;
                    candidate.ntfy.configured = current.ntfy.configured;
                }
                Some(CredentialAction::Replace { value }) => {
                    candidate.ntfy.token = Some(value.clone());
                    candidate.ntfy.credential_ref = current.ntfy.credential_ref;
                    candidate.ntfy.configured = true;
                }
                Some(CredentialAction::Clear) => {
                    candidate.ntfy.credential_ref = current.ntfy.credential_ref;
                }
            }
            match data.bark.credential_change.as_ref() {
                None | Some(CredentialAction::Keep) => {
                    candidate.bark.device_key = current.bark.device_key;
                    candidate.bark.credential_ref = current.bark.credential_ref;
                    candidate.bark.configured = current.bark.configured;
                }
                Some(CredentialAction::Replace { value }) => {
                    candidate.bark.device_key = Some(value.clone());
                    candidate.bark.credential_ref = current.bark.credential_ref;
                    candidate.bark.configured = true;
                }
                Some(CredentialAction::Clear) => {
                    candidate.bark.credential_ref = current.bark.credential_ref;
                }
            }
            config.notifications = candidate;
            Ok(())
        })
        .await?;
    let section = section.ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "notification mutation completed without a typed section envelope"
        ))
    })?;
    Ok(HttpResponse::Ok().json(notification_response(
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
    async fn channel_status_uses_only_the_supplied_snapshot() {
        let reference = bamboo_config::CredentialRef::parse("notification.ntfy.token").unwrap();
        let status = bamboo_config::CredentialStatus {
            credential_ref: reference.clone(),
            configured: true,
            source: bamboo_config::CredentialSource::Migrated,
            updated_at: None,
        };
        let snapshot = BTreeMap::from([(reference.clone(), status)]);
        let health = bamboo_config::CredentialStoreHealth::committed(1);
        let configured = serde_json::to_value(channel_status_from_snapshot(
            Some(reference.clone()),
            true,
            &snapshot,
            &health,
        ))
        .unwrap();
        assert_eq!(configured["configured"], true);
        assert_eq!(configured["state"], "configured");
        assert_eq!(configured["source"], "migrated");

        let incoherent = serde_json::to_value(channel_status_from_snapshot(
            Some(reference),
            true,
            &BTreeMap::new(),
            &health,
        ))
        .unwrap();
        assert_eq!(incoherent["configured"], false);
        assert_eq!(incoherent["state"], "error");
        assert!(incoherent.get("source").is_none());
    }

    #[actix_web::test]
    async fn dedicated_mutation_returns_exact_section_and_explicit_secret_status() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0xc4; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let mut events = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/notifications", web::put().to(put_notification_config)),
        )
        .await;

        let replace = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/notifications")
                .set_json(serde_json::json!({
                    "expected_revision": 0,
                    "data": {
                        "desktop": {"enabled": null},
                        "ntfy": {
                            "enabled": true,
                            "base_url": "https://ntfy.sh",
                            "topic": "alerts",
                            "credential_change": {
                                "action": "replace",
                                "value": "notification-secret"
                            }
                        },
                        "bark": {
                            "enabled": false,
                            "base_url": "https://api.day.app"
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(replace.status(), StatusCode::OK);
        let replace: serde_json::Value = test::read_body_json(replace).await;
        assert_eq!(replace["revision"], 1);
        assert_eq!(replace["section"]["revision"], 1);
        assert_eq!(replace["data"]["ntfy"]["credential"]["configured"], true);
        assert_eq!(replace["data"]["ntfy"]["credential"]["state"], "configured");
        assert!(!replace.to_string().contains("notification-secret"));
        assert!(!replace.to_string().contains("********"));
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision: 1 }
                        if section == "notifications"
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
                if section == "notifications"
        ));

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/notifications")
                .set_json(serde_json::json!({
                    "expected_revision": 0,
                    "data": {
                        "desktop": {"enabled": true},
                        "ntfy": {
                            "enabled": true,
                            "base_url": "https://ntfy.sh",
                            "topic": "stale",
                            "credential_change": {
                                "action": "replace",
                                "value": "stale-notification-secret"
                            }
                        },
                        "bark": {
                            "enabled": false,
                            "base_url": "https://api.day.app"
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(!String::from_utf8(test::read_body(stale).await.to_vec())
            .unwrap()
            .contains("stale-notification-secret"));

        let keep = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/notifications")
                .set_json(serde_json::json!({
                    "expected_revision": 1,
                    "data": {
                        "desktop": {"enabled": true},
                        "ntfy": {
                            "enabled": true,
                            "base_url": "https://ntfy.sh",
                            "topic": "renamed",
                            "credential_change": {"action": "keep"}
                        },
                        "bark": {
                            "enabled": false,
                            "base_url": "https://api.day.app"
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(keep.status(), StatusCode::OK);
        let keep: serde_json::Value = test::read_body_json(keep).await;
        assert_eq!(keep["section"]["revision"], 2);
        assert_eq!(keep["data"]["ntfy"]["topic"], "renamed");
        assert_eq!(
            state
                .config
                .read()
                .await
                .notifications
                .ntfy
                .token
                .as_deref(),
            Some("notification-secret")
        );

        let clear = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/notifications")
                .set_json(serde_json::json!({
                    "expected_revision": 2,
                    "data": {
                        "desktop": {"enabled": true},
                        "ntfy": {
                            "enabled": false,
                            "base_url": "https://ntfy.sh",
                            "topic": "renamed",
                            "credential_change": {"action": "clear"}
                        },
                        "bark": {
                            "enabled": false,
                            "base_url": "https://api.day.app"
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(clear.status(), StatusCode::OK);
        let clear: serde_json::Value = test::read_body_json(clear).await;
        assert_eq!(clear["section"]["revision"], 3);
        assert_eq!(clear["data"]["ntfy"]["credential"]["configured"], false);
        assert_eq!(clear["data"]["ntfy"]["credential"]["state"], "missing");

        let masked = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/notifications")
                .set_json(serde_json::json!({
                    "expected_revision": 3,
                    "data": {
                        "desktop": {"enabled": true},
                        "ntfy": {
                            "enabled": false,
                            "base_url": "https://ntfy.sh",
                            "topic": "renamed",
                            "credential_change": {
                                "action": "replace",
                                "value": "********"
                            }
                        },
                        "bark": {
                            "enabled": false,
                            "base_url": "https://api.day.app"
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(masked.status(), StatusCode::BAD_REQUEST);

        let notification_file =
            std::fs::read_to_string(dir.path().join("notifications.json")).unwrap();
        let credential_file = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
        for secret in [
            "notification-secret",
            "stale-notification-secret",
            "********",
        ] {
            assert!(!notification_file.contains(secret));
            assert!(!credential_file.contains(secret));
        }
    }
}
