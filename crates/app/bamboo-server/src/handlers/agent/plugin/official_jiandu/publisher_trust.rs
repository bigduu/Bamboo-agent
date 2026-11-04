//! Explicit adoption of one reviewed publisher through the existing config authority.
use super::*;
use crate::{app_state::ConfigSectionMutationError, error::AppError};
use bamboo_config::{
    official_plugin_publishers, publisher_key_id, ConfigStoreError, PublisherMode, SectionId,
    SectionStatus, TrustedKey,
};

fn catalog_identity() -> BTreeMap<String, String> {
    official_plugin_publishers()
        .into_iter()
        .map(|(id, key)| (id.into(), publisher_key_id(&key)))
        .collect()
}

fn policy_report(policy: &PluginTrustConfig, revision: u64) -> serde_json::Value {
    let effective = policy.effective_trusted_keys();
    let catalog = catalog_identity();
    serde_json::json!({
        "expected_revision": revision,
        "publisher_mode": policy.publisher_mode,
        "enforcement": policy.enforcement,
        "custom_key_count": policy.trusted_keys.iter().filter(|key| !catalog.values().any(|id| *id == publisher_key_id(key))).count(),
        "publishers": official_plugin_publishers().into_iter().map(|(id, key)| {
            serde_json::json!({"id":id,
                "trusted":effective.iter().any(|trusted| publisher_key_id(trusted) == publisher_key_id(&key)),
                "revoked":policy.revoked_publishers.contains_key(id), "key":key})
        }).collect::<Vec<_>>()
    })
}

/// Observe the shipped catalog and one healthy revision without changing it.
pub async fn publisher_policy(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let _io = state.config_io_lock.lock().await;
    let Some(facade) = &state.config_facade else {
        return Ok(unavailable("configuration_unavailable"));
    };
    let snapshot = facade.registry().tools_skills.snapshot();
    if snapshot.status != SectionStatus::Healthy
        || snapshot.data.plugin_trust != state.config.read().await.plugin_trust
    {
        return Ok(unavailable("configuration_unavailable"));
    }
    Ok(HttpResponse::Ok().json(policy_report(
        &snapshot.data.plugin_trust,
        snapshot.revision,
    )))
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublisherPolicyChange {
    SetMode { publisher_mode: PublisherMode },
    Revoke { publisher_id: String },
    Trust { publisher_id: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherPolicyRequest {
    expected_revision: u64,
    expected_publishers: BTreeMap<String, String>,
    change: PublisherPolicyChange,
}

/// Every grant, revoke and mode change binds the reviewed catalog and existing
/// tools-skills CAS. No source hosts, enforcement, installs or sessions change.
pub async fn change_publisher_policy(
    state: web::Data<AppState>,
    body: web::Json<PublisherPolicyRequest>,
) -> Result<HttpResponse, AppError> {
    if body.expected_publishers != catalog_identity() {
        return Err(AppError::BadRequest(
            "reviewed publisher catalog changed; refresh policy".into(),
        ));
    }
    let candidate = {
        let _io = state.config_io_lock.lock().await;
        let Some(facade) = &state.config_facade else {
            return Ok(unavailable("configuration_unavailable"));
        };
        let snapshot = facade.registry().tools_skills.snapshot();
        if snapshot.status != SectionStatus::Healthy
            || snapshot.data.plugin_trust != state.config.read().await.plugin_trust
        {
            return Ok(unavailable("configuration_unavailable"));
        }
        if snapshot.revision != body.expected_revision {
            return Err(AppError::ConfigConflict {
                expected: body.expected_revision,
                actual: snapshot.revision,
            });
        }
        let mut candidate = snapshot.data.as_ref().clone();
        let policy = &mut candidate.plugin_trust;
        let valid = match &body.change {
            PublisherPolicyChange::SetMode { publisher_mode } => {
                policy.set_publisher_mode(*publisher_mode);
                true
            }
            PublisherPolicyChange::Revoke { publisher_id } => {
                policy.revoke_official_publisher(publisher_id)
            }
            PublisherPolicyChange::Trust { publisher_id } => {
                policy.trust_official_publisher(publisher_id)
            }
        };
        if !valid {
            return Err(AppError::BadRequest("unknown official publisher".into()));
        }
        serde_json::to_value(candidate)?
    };
    state
        .put_ordinary_section(SectionId::ToolsSkills, body.expected_revision, candidate)
        .await
        .map_err(map_commit_error)?;
    Ok(HttpResponse::NoContent().finish())
}

fn map_commit_error(error: ConfigSectionMutationError) -> AppError {
    match error {
        ConfigSectionMutationError::Store(ConfigStoreError::Conflict { expected, actual }) => {
            AppError::ConfigConflict { expected, actual }
        }
        error => {
            tracing::warn!(?error, "publisher policy commit could not be confirmed");
            AppError::InternalError(anyhow::anyhow!(
                "publisher policy could not be confirmed; refresh settings"
            ))
        }
    }
}

#[derive(Serialize)]
pub(super) struct PublisherTrustAction {
    available: bool,
    reason: Option<&'static str>,
    expected_revision: u64,
    publisher: TrustedKey,
}

fn publisher() -> TrustedKey {
    TrustedKey {
        label: "jiandu (bigduu official)".into(),
        algorithm: "ed25519".into(),
        public_key: JIANDU_PLUGIN_PUBLIC_KEY.into(),
    }
}

pub(super) async fn observe_trust(
    state: &AppState,
    host: &Host,
    release: Option<&Release>,
    memory_reason: Option<&'static str>,
    installing: bool,
) -> (PluginTrustConfig, Option<PublisherTrustAction>) {
    // Bind the policy used by setup and the offered revision to the same
    // process-owned observation. Cross-process changes still meet disk CAS.
    let _io = state.config_io_lock.lock().await;
    let trust = state.config.read().await.plugin_trust.clone();
    let action = state.config_facade.as_ref().and_then(|facade| {
        let snapshot = facade.registry().tools_skills.snapshot();
        if snapshot.status != SectionStatus::Healthy || snapshot.data.plugin_trust != trust {
            return None;
        }
        let reason = host
            .reason
            .or(memory_reason)
            .or_else(|| release.is_none().then_some("artifact_unavailable"))
            .or_else(|| {
                release
                    .filter(|r| !trust.is_host_trusted(&r.manifest_url))
                    .map(|_| "source_not_trusted")
            })
            .or_else(|| installing.then_some("incomplete_install"))
            .or_else(|| {
                (trust_reason(&trust, release) != Some("publisher_not_trusted"))
                    .then_some("publisher_already_trusted")
            });
        Some(PublisherTrustAction {
            available: reason.is_none(),
            reason,
            expected_revision: snapshot.revision,
            publisher: publisher(),
        })
    });
    (trust, action)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherTrustRequest {
    expected_revision: u64,
    expected_public_key: String,
}

fn unavailable(reason: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "error": {"type": "official_publisher_trust_unavailable", "reason": reason}
    }))
}

/// Trust the displayed official publisher only. Never installs a plugin,
/// adds a source host, changes enforcement, or accepts caller-supplied keys.
pub async fn trust_official_jiandu(
    state: web::Data<AppState>,
    body: web::Json<PublisherTrustRequest>,
) -> Result<HttpResponse, AppError> {
    if !body
        .expected_public_key
        .eq_ignore_ascii_case(JIANDU_PLUGIN_PUBLIC_KEY)
    {
        return Err(AppError::BadRequest(
            "reviewed Jiandu publisher identity changed; refresh setup".into(),
        ));
    }
    let report = match setup(&state, Host::detect(), accepted_release()).await {
        Ok(report) => report,
        Err(error) => return Ok(plugin_error_response(&error)),
    };
    let Some(action) = &report.trust_action else {
        return Ok(unavailable("configuration_unavailable"));
    };
    if action.expected_revision != body.expected_revision {
        return Err(AppError::ConfigConflict {
            expected: body.expected_revision,
            actual: action.expected_revision,
        });
    }
    if action.reason == Some("publisher_already_trusted") {
        return Ok(HttpResponse::Ok().json(report));
    }
    if !action.available {
        return Ok(unavailable(
            action.reason.unwrap_or("configuration_unavailable"),
        ));
    }
    let candidate = {
        let _io = state.config_io_lock.lock().await;
        let Some(facade) = &state.config_facade else {
            return Ok(unavailable("configuration_unavailable"));
        };
        let snapshot = facade.registry().tools_skills.snapshot();
        if snapshot.status != SectionStatus::Healthy {
            return Ok(unavailable("configuration_unavailable"));
        }
        if snapshot.revision != body.expected_revision {
            return Err(AppError::ConfigConflict {
                expected: body.expected_revision,
                actual: snapshot.revision,
            });
        }
        let mut candidate = snapshot.data.as_ref().clone();
        candidate.plugin_trust.trust_official_publisher("jiandu");
        serde_json::to_value(candidate)?
    };
    // This writer owns atomic persistence, cross-process CAS, runtime adoption
    // and config-change events. It rechecks the revision after the read lock.
    state
        .put_ordinary_section(SectionId::ToolsSkills, body.expected_revision, candidate)
        .await
        .map_err(map_commit_error)?;
    // A fresh GET, rather than this mutation response, authorizes the UI's
    // next action. A lost response is reconciled without repeating the POST.
    Ok(HttpResponse::NoContent().finish())
}

#[cfg(test)]
mod tests;
