//! `/api/v1/plugins` handlers: install / update / list / remove.
//!
//! Every handler constructs a fresh `ServerPluginInstaller::new(state.clone())`
//! per request — the same pattern every other handler in this crate uses for
//! `web::Data<AppState>` (it's `Arc`-backed, so cloning is cheap) — and either
//! calls it directly (`list`/`uninstall`) or drives it through
//! `crate::plugin_source`'s stage-then-install seam (`install`/`update`),
//! matching `examples/install_plugin.rs`'s reference usage.

use std::path::PathBuf;

use actix_web::{web, HttpResponse, Responder};
use bamboo_plugin::{InstallDisposition, PluginError, PluginInstaller, PluginSource};

use crate::app_state::AppState;
use crate::plugin_installer::ServerPluginInstaller;
use crate::plugin_source::{install_plugin_from_source, stage_plugin_source, PluginSourceInput};

use super::api_types::{to_view, InstallPluginRequest, PluginListResponse};
use super::errors::plugin_error_response;

fn plugins_root(state: &AppState) -> PathBuf {
    state.app_data_dir.join("plugins")
}

/// The wire `SourceSpec` (reusing `bamboo_plugin::PluginSource`'s own serde
/// shape — see `api_types` module docs) carries an optional `sha256` on the
/// `Url` variant that `PluginSourceInput`/`stage_plugin_source` do not yet
/// consume (see that module's "Known follow-ups"); dropped here, not lost —
/// it was never wired to anything to lose.
fn to_source_input(source: PluginSource) -> PluginSourceInput {
    match source {
        PluginSource::LocalDir { path } => PluginSourceInput::LocalDir(path),
        PluginSource::LocalArchive { path } => PluginSourceInput::LocalArchive(path),
        PluginSource::Url { url, sha256: _ } => PluginSourceInput::Url(url),
    }
}

/// `GET /api/v1/plugins`
pub async fn list_plugins(state: web::Data<AppState>) -> impl Responder {
    let installer = ServerPluginInstaller::new(state.clone());
    match installer.list().await {
        Ok(entries) => {
            let mut plugins = Vec::with_capacity(entries.len());
            for entry in entries {
                plugins.push(to_view(entry).await);
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
    let input = to_source_input(body.into_inner().source);

    match install_plugin_from_source(
        &installer,
        input,
        &root,
        InstallDisposition::FailIfInstalled,
    )
    .await
    {
        Ok(entry) => HttpResponse::Created().json(to_view(entry).await),
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
    let input = to_source_input(body.into_inner().source);

    let staged = match stage_plugin_source(input, &root).await {
        Ok(staged) => staged,
        Err(error) => return plugin_error_response(&error),
    };
    if staged.manifest.id != path_id {
        let manifest_id = staged.manifest.id.clone();
        staged.rollback().await;
        return plugin_error_response(&PluginError::InvalidManifest(format!(
            "path id '{path_id}' does not match the source's manifest id '{manifest_id}'"
        )));
    }

    let manifest = staged.manifest.clone();
    let plugin_dir = staged.plugin_dir.clone();
    let source = staged.source.clone();
    match installer
        .install(
            &manifest,
            &plugin_dir,
            source,
            InstallDisposition::Upgrade,
            chrono::Utc::now(),
        )
        .await
    {
        Ok(entry) => {
            staged.commit().await;
            HttpResponse::Ok().json(to_view(entry).await)
        }
        Err(error) => {
            staged.rollback().await;
            plugin_error_response(&error)
        }
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
