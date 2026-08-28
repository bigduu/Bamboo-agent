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
