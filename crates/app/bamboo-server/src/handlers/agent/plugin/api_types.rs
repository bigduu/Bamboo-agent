//! Wire types for `/api/v1/plugins` (Wave 2 § HTTP agent, `PLUGIN_PLAN.md`).
//!
//! # Frozen contract
//!
//! Coded jointly with a parallel `bamboo plugin ...` CLI agent against the
//! same HTTP surface — do not change field names/shapes here without
//! re-syncing both sides:
//!
//! - `GET /api/v1/plugins` -> `200 { "plugins": [InstalledPluginView, ...] }`
//! - `POST /api/v1/plugins/install` body `{ "source": SourceSpec }` -> `201 InstalledPluginView`
//! - `POST /api/v1/plugins/{id}/update` body `{ "source": SourceSpec }` -> `200 InstalledPluginView`
//! - `DELETE /api/v1/plugins/{id}` -> `200 { "id", "removed": true }`
//!
//! `SourceSpec` reuses [`bamboo_plugin::PluginSource`]'s own
//! `#[serde(tag = "type", rename_all = "snake_case")]` wire shape directly —
//! `{"type":"local_dir","path":...}` / `{"type":"local_archive","path":...}` /
//! `{"type":"url","url":...,"sha256":...?}` — rather than inventing a
//! parallel request enum: it is already exactly the shape both the CLI and
//! this HTTP layer need, and it doubles as the provenance record `install()`
//! persists (see `bamboo_plugin::registry::PluginSource`'s doc comment).
//!
//! Known no-op: a client-supplied `sha256` on a `url` source is accepted (for
//! forward-compat / symmetry with the provenance shape) but not currently
//! enforced — `plugin_source::stage_plugin_source` only pins the
//! per-platform *binary artifact*'s sha256 (declared inside the manifest
//! itself), not the top-level manifest/content bundle fetched from `url`. See
//! that module's "Known follow-ups" doc comment.

use serde::{Deserialize, Serialize};

use bamboo_plugin::{
    InstalledPlugin, PluginInstallStatus, PluginManifest, PluginSource, RegisteredCapabilities,
};

/// Shared body for `POST /install` and `POST /{id}/update`.
#[derive(Debug, Deserialize)]
pub struct InstallPluginRequest {
    pub source: PluginSource,
}

/// `GET /plugins` element, and the body of a successful install/update
/// response.
#[derive(Debug, Clone, Serialize)]
pub struct InstalledPluginView {
    pub id: String,
    /// Best-effort — see [`to_view`]. Omitted (not `null`) when unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub version: String,
    pub source: PluginSource,
    pub status: PluginInstallStatus,
    pub registered: RegisteredCapabilities,
}

/// `GET /api/v1/plugins` response body.
#[derive(Debug, Serialize)]
pub struct PluginListResponse {
    pub plugins: Vec<InstalledPluginView>,
}

/// Project an [`InstalledPlugin`] provenance row into the wire view.
///
/// `InstalledPlugin` (the `installed.json` provenance record) does not carry
/// the manifest's display `name` — only `id`/`version`/`source`/`plugin_dir`/
/// `status`/`registered` (see `bamboo_plugin::registry::InstalledPlugin`).
/// This best-effort re-reads `<plugin_dir>/plugin.json` (still on disk after
/// a successful install/update) to recover it; `None` if that file is
/// missing or fails to parse (e.g. hand-deleted out from under bamboo),
/// matching the contract's optional `name?`.
pub async fn to_view(entry: InstalledPlugin) -> InstalledPluginView {
    let name = read_manifest_name(&entry.plugin_dir).await;
    InstalledPluginView {
        id: entry.id,
        name,
        version: entry.version,
        source: entry.source,
        status: entry.status,
        registered: entry.registered,
    }
}

async fn read_manifest_name(plugin_dir: &std::path::Path) -> Option<String> {
    let raw = tokio::fs::read_to_string(plugin_dir.join("plugin.json"))
        .await
        .ok()?;
    let manifest = PluginManifest::parse_str(&raw).ok()?;
    Some(manifest.name)
}
