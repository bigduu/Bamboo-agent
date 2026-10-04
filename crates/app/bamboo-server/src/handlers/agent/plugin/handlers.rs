//! `/api/v1/plugins` handlers: install / update / list / remove.
//!
//! Every handler constructs a fresh `ServerPluginInstaller::new(state.clone())`
//! per request — the same pattern every other handler in this crate uses for
//! `web::Data<AppState>` (it's `Arc`-backed, so cloning is cheap) — and either
//! calls it directly (`list`/`uninstall`) or drives the prepared-source
//! transaction (`install`/`update`): prepare privately, acquire the global
//! operation guard, audit ownership, activate, install, commit/rollback.

use std::path::PathBuf;

use actix_web::{web, HttpResponse, Responder};
use bamboo_plugin::{InstallDisposition, PluginInstaller, PluginSource};

use crate::app_state::AppState;
use crate::plugin_installer::ServerPluginInstaller;
use crate::plugin_source::{
    install_server_plugin_from_source_with_event_sink_grants, PluginSourceInput,
};

use super::api_types::{to_view, InstallPluginRequest, PluginListResponse};
use super::errors::plugin_error_response;

fn plugins_root(state: &AppState) -> PathBuf {
    state.app_data_dir.join("plugins")
}

/// The wire `SourceSpec` reuses `bamboo_plugin::PluginSource`'s own serde
/// shape (see `api_types` module docs) directly as the request body — a
/// `url` source's `sha256`/`allow_unverified`/`allow_untrusted_host`/
/// `allow_unsigned`/`insecure` flow straight through to
/// `PluginSourceInput::Url`, which `plugin_source::fetch_manifest_bundle`
/// enforces (the three-layer trust model: host allowlist, signature,
/// checksum — plus the `insecure`/`plugin_trust.enforcement` aggregate
/// escape hatch over all three — see that module's docs). `signed_by` is a
/// RESULT of staging (which key verified), never an input, so it's dropped
/// here — the request-side `PluginSource::Url` field is meaningless on the
/// way in and `fetch_manifest_bundle` recomputes it fresh. `insecure`, by
/// contrast, genuinely IS an input here (the caller's `--insecure` /
/// `"insecure": true` opt-in) — this authenticated/local-only HTTP surface
/// is already behind the access-password middleware (see `routes`), same as
/// every other `/api/v1/plugins` route.
fn to_source_input(source: PluginSource) -> PluginSourceInput {
    match source {
        PluginSource::LocalDir { path } => PluginSourceInput::LocalDir(path),
        PluginSource::LocalArchive { path } => PluginSourceInput::LocalArchive(path),
        PluginSource::Url {
            url,
            sha256,
            allow_unverified,
            allow_untrusted_host,
            allow_unsigned,
            signed_by: _,
            insecure,
        } => PluginSourceInput::Url {
            url,
            sha256,
            allow_unverified,
            allow_untrusted_host,
            allow_unsigned,
            insecure,
        },
    }
}

/// `GET /api/v1/plugins`
pub async fn list_plugins(state: web::Data<AppState>) -> impl Responder {
    let installer = ServerPluginInstaller::new(state.clone());
    match installer.list().await {
        Ok(entries) => {
            let mut plugins = Vec::with_capacity(entries.len());
            for entry in entries {
                plugins
                    .push(to_view(entry, &state.service_manager, &state.tool_event_router).await);
            }
            HttpResponse::Ok().json(PluginListResponse { plugins })
        }
        Err(error) => plugin_error_response(&error),
    }
}

/// `POST /api/v1/plugins/install` — always `InstallDisposition::FailIfInstalled`
/// (surfaces `PluginError::AlreadyInstalled` as 409 if the id is already
/// registered; retry via `POST /{id}/update` instead).
pub async fn install_plugin(
    state: web::Data<AppState>,
    body: web::Json<InstallPluginRequest>,
) -> impl Responder {
    let installer = ServerPluginInstaller::new(state.clone());
    let root = plugins_root(&state);
    let request = body.into_inner();
    let grants = request.event_sink_grants;
    let input = to_source_input(request.source);
    let trust = state.config.read().await.plugin_trust.clone();

    match install_server_plugin_from_source_with_event_sink_grants(
        &installer,
        input,
        &root,
        &trust,
        InstallDisposition::FailIfInstalled,
        None,
        grants.as_deref(),
    )
    .await
    {
        Ok(entry) => HttpResponse::Created()
            .json(to_view(entry, &state.service_manager, &state.tool_event_router).await),
        Err(error) => plugin_error_response(&error),
    }
}

/// `POST /api/v1/plugins/{id}/update` — `InstallDisposition::Upgrade`.
///
/// Unlike `install`, this route's URL names the target id up front, so —
/// before handing off to the installer — the staged source's OWN manifest id
/// (the id `install()` will actually key the upgrade by) is checked against
/// the path segment. A mismatch is refused as a 400 rather than silently
/// upgrading whatever id the body's source happens to declare, which would
/// otherwise be README-legible but genuinely confusing (a request the URL
/// promises operates on `foo` silently upgrading `bar`).
pub async fn update_plugin(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<InstallPluginRequest>,
) -> impl Responder {
    let path_id = path.into_inner();
    let installer = ServerPluginInstaller::new(state.clone());
    let root = plugins_root(&state);
    let request = body.into_inner();
    let grants = request.event_sink_grants;
    let input = to_source_input(request.source);
    let trust = state.config.read().await.plugin_trust.clone();

    match install_server_plugin_from_source_with_event_sink_grants(
        &installer,
        input,
        &root,
        &trust,
        InstallDisposition::Upgrade,
        Some(&path_id),
        grants.as_deref(),
    )
    .await
    {
        Ok(entry) => HttpResponse::Ok()
            .json(to_view(entry, &state.service_manager, &state.tool_event_router).await),
        Err(error) => plugin_error_response(&error),
    }
}

/// `DELETE /api/v1/plugins/{id}`
pub async fn remove_plugin(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let id = path.into_inner();
    let installer = ServerPluginInstaller::new(state.clone());
    match installer.uninstall(&id).await {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({ "id": id, "removed": true })),
        Err(error) => plugin_error_response(&error),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookReviewRequest {
    pub config: String,
    pub digest: String,
    pub enabled: bool,
    /// Required explicit acknowledgment before granting execution trust.
    pub confirm_execution: bool,
}

/// Read-only review report: receipt, current bytes, strict compatibility result.
pub async fn plugin_hooks(state: web::Data<AppState>, id: web::Path<String>) -> impl Responder {
    let installer = ServerPluginInstaller::new(state.clone());
    let _guard = installer.begin_operation().await;
    match hook_review_report(&state, &id).await {
        Ok(report) => HttpResponse::Ok().json(report),
        Err(error) => plugin_error_response(&error),
    }
}
async fn hook_review_report(
    state: &AppState,
    id: &str,
) -> bamboo_plugin::PluginResult<serde_json::Value> {
    let store =
        bamboo_plugin::InstalledPlugins::load(&plugins_root(state).join("installed.json")).await?;
    let entry = store.get_unique(id)?.ok_or_else(|| {
        bamboo_plugin::PluginError::InvalidManifest("plugin not installed".into())
    })?;
    let manifest: bamboo_plugin::PluginManifest = serde_json::from_slice(
        &tokio::fs::read(entry.plugin_dir.join("plugin.json"))
            .await
            .map_err(|e| bamboo_plugin::PluginError::InvalidManifest(e.to_string()))?,
    )
    .map_err(|e| bamboo_plugin::PluginError::InvalidManifest(e.to_string()))?;
    match bamboo_plugin::hooks::registrations(&manifest, &entry.plugin_dir) {
        Ok(current) => Ok(serde_json::json!({"hooks": current.iter().map(|now| {
            let stored=entry.registered.hooks.iter().find(|r| r.config==now.config);
            serde_json::json!({"config":now.config,"digest":now.digest,"state": if entry.status==bamboo_plugin::PluginInstallStatus::Installed { stored.map(|r|r.state(&now.digest)).unwrap_or(bamboo_plugin::hooks::HookState::NeedsReview) } else { bamboo_plugin::hooks::HookState::Disabled }})
        }).collect::<Vec<_>>() })),
        Err(error) => {
            Ok(serde_json::json!({"state":"unsupported","compatibility_error":error.to_string()}))
        }
    }
}

/// An authenticated explicit review of exact bytes; install/update never calls it.
pub async fn review_plugin_hooks(
    state: web::Data<AppState>,
    id: web::Path<String>,
    body: web::Json<HookReviewRequest>,
) -> impl Responder {
    let installer = ServerPluginInstaller::new(state.clone());
    let _guard = installer.begin_operation().await;
    let result: bamboo_plugin::PluginResult<()> = async {
        let path = plugins_root(&state).join("installed.json");
        let mut store = bamboo_plugin::InstalledPlugins::load(&path).await?;
        let mut entry = store.get_unique(&id)?.cloned().ok_or_else(|| {
            bamboo_plugin::PluginError::InvalidManifest("plugin not installed".into())
        })?;
        if entry.status != bamboo_plugin::PluginInstallStatus::Installed {
            return Err(bamboo_plugin::PluginError::InvalidManifest(
                "install incomplete".into(),
            ));
        }
        let manifest: bamboo_plugin::PluginManifest = serde_json::from_slice(
            &tokio::fs::read(entry.plugin_dir.join("plugin.json"))
                .await
                .map_err(|e| bamboo_plugin::PluginError::InvalidManifest(e.to_string()))?,
        )
        .map_err(|e| bamboo_plugin::PluginError::InvalidManifest(e.to_string()))?;
        if manifest.id != entry.id || manifest.version != entry.version {
            return Err(bamboo_plugin::PluginError::InvalidManifest(
                "manifest identity changed".into(),
            ));
        }
        let current = bamboo_plugin::hooks::registrations(&manifest, &entry.plugin_dir)?;
        let now = current
            .iter()
            .find(|r| r.config == body.config)
            .ok_or_else(|| {
                bamboo_plugin::PluginError::InvalidManifest("unknown hook config".into())
            })?;
        let registered = entry
            .registered
            .hooks
            .iter_mut()
            .find(|r| r.config == body.config)
            .ok_or_else(|| {
                bamboo_plugin::PluginError::InvalidManifest("hook is not registered".into())
            })?;
        if now.digest != body.digest {
            return Err(bamboo_plugin::PluginError::InvalidManifest(
                "review digest changed".into(),
            ));
        }
        registered.digest = now.digest.clone();
        if body.enabled {
            if !body.confirm_execution {
                return Err(bamboo_plugin::PluginError::InvalidManifest(
                    "explicit execution confirmation required".into(),
                ));
            }
            registered.confirm_review(&body.digest)?;
        } else {
            registered.enabled = false;
        }
        store.add(entry);
        store.save(&path).await
    }
    .await;
    match result {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({"ok":true})),
        Err(error) => plugin_error_response(&error),
    }
}
