use actix_web::{web, HttpResponse};
use std::collections::BTreeMap;

use crate::{app_state::AppState, error::AppError};

fn channel_status_from_snapshot(
    reference: Option<bamboo_config::CredentialRef>,
    statuses: &BTreeMap<bamboo_config::CredentialRef, bamboo_config::CredentialStatus>,
) -> serde_json::Value {
    let Some(reference) = reference else {
        return serde_json::json!({
            "credential_ref": serde_json::Value::Null,
            "configured": false,
            "source": serde_json::Value::Null,
            "updated_at": serde_json::Value::Null,
        });
    };
    match statuses.get(&reference) {
        Some(status) => serde_json::json!({
            "credential_ref": status.credential_ref,
            "configured": status.configured,
            "source": status.source,
            "updated_at": status.updated_at,
        }),
        None => serde_json::json!({
            "credential_ref": reference,
            "configured": false,
            "source": bamboo_config::CredentialSource::User,
            "updated_at": serde_json::Value::Null,
        }),
    }
}

/// Metadata-only notification configuration and credential status. Secret
/// values, ciphertext and masks are never returned.
pub async fn get_notification_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let config = app_state.config.read().await.clone();
    let (statuses, health) = app_state
        .credential_store
        .statuses_with_health()
        .map_err(super::credentials::map_store_read_error)?;
    let statuses = statuses
        .into_iter()
        .map(|status| (status.credential_ref.clone(), status))
        .collect::<BTreeMap<_, _>>();
    let ntfy =
        channel_status_from_snapshot(config.notifications.ntfy.credential_ref.clone(), &statuses);
    let bark =
        channel_status_from_snapshot(config.notifications.bark.credential_ref.clone(), &statuses);
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "revision": health.revision,
        "status": health.status,
        "source": health.source,
        "last_error": health.last_error,
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
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_status_uses_only_the_supplied_snapshot() {
        let reference = bamboo_config::CredentialRef::parse("notification.ntfy.token").unwrap();
        let status = bamboo_config::CredentialStatus {
            credential_ref: reference.clone(),
            configured: true,
            source: bamboo_config::CredentialSource::Migrated,
            updated_at: None,
        };
        let snapshot = BTreeMap::from([(reference.clone(), status)]);
        let configured = channel_status_from_snapshot(Some(reference.clone()), &snapshot);
        assert_eq!(configured["configured"], true);
        assert_eq!(configured["source"], "migrated");

        let missing = channel_status_from_snapshot(Some(reference), &BTreeMap::new());
        assert_eq!(missing["configured"], false);
        assert_eq!(missing["source"], "user");
    }
}
