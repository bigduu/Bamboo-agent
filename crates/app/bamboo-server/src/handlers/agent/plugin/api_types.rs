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
//! `{"type":"url","url":...,"sha256":...?,"allow_unverified":...?,
//! "allow_untrusted_host":...?,"allow_unsigned":...?,"insecure":...?}` —
//! rather than inventing a parallel request enum: it is already exactly the
//! shape both the CLI and this HTTP layer need, and it doubles as the
//! provenance record `install()` persists (see
//! `bamboo_plugin::registry::PluginSource`'s doc comment). `signed_by` is
//! also part of that same wire shape but is response-only in practice — a
//! request-supplied value is ignored (see `to_source_input`). `insecure`, by
//! contrast, IS a real request-side input (see below).
//!
//! **Three trust layers (`url` sources), enforced together in
//! `plugin_source::fetch_manifest_bundle`** (see that module's docs for the
//! full precedence):
//!
//! 1. **Host allowlist.** The URL's host+path must match one of
//!    `plugin_trust.trusted_hosts` (config.json) unless
//!    `allow_untrusted_host: true` is set — refused with
//!    `PluginError::UntrustedHost` (`403`) BEFORE any fetch.
//! 2. **Signature.** The bundle's `<url>.sig` must verify against one of
//!    `plugin_trust.trusted_keys` unless `allow_unsigned: true` is set —
//!    refused with `PluginError::UnsignedOrUntrustedSignature` (`403`).
//! 3. **Checksum.** `sha256`, when given, pins the downloaded BUNDLE's exact
//!    bytes — verified BEFORE anything is extracted/parsed, distinct from
//!    (and in addition to) the per-platform *binary artifact*'s own sha256
//!    declared inside the manifest itself (always verified, regardless of
//!    this field). Without `sha256`, a `url` install is REFUSED
//!    (`PluginError::ChecksumRequired`, mapped to `400`) unless
//!    `allow_unverified: true` is set OR the bundle was verified in step 2
//!    (a valid signature satisfies the checksum requirement on its own — see
//!    `plugin_source.rs`'s module docs).
//!
//! So `POST /install` with a bare `{"type":"url","url":"..."}` body no
//! longer just downloads and trusts any tar.gz from any host.
//!
//! **`insecure` — the aggregate escape hatch.** `"insecure": true` on a `url`
//! source is shorthand for setting `allow_untrusted_host`, `allow_unsigned`
//! AND `allow_unverified` all at once for THIS request — see
//! `plugin_source.rs`'s "`--insecure` / `plugin_trust.enforcement`" module
//! docs section. A supplied `sha256` is still verified even with
//! `insecure: true` (the aggregate only turns default-required checks OFF;
//! it never turns off a check the caller explicitly opted into). The same
//! aggregate also applies server-wide, with no per-request field needed, when
//! `plugin_trust.enforcement` is `"off"` in `config.json`. This route sits
//! behind the same `enforce_access_password_middleware` wrap as every other
//! `/api/v1/plugins` route (see `routes::agent::plugin_scope`), so `insecure`
//! is not an additional unauthenticated surface.

use serde::{Deserialize, Serialize};

use bamboo_plugin::{
    InstalledPlugin, PluginInstallStatus, PluginManifest, PluginSource, RegisteredCapabilities,
};

use crate::service_manager::{ServiceManager, ServiceState};

/// Shared body for `POST /install` and `POST /{id}/update`.
#[derive(Debug, Deserialize)]
pub struct InstallPluginRequest {
    pub source: PluginSource,
}

/// `GET /plugins` element, and the body of a successful install/update
/// response.
///
/// Known, accepted gap: `source` (a [`PluginSource`]) echoes back the
/// caller-supplied `LocalDir`/`LocalArchive` path VERBATIM, including its
/// absolute filesystem path, to any caller of this authenticated/local-only
/// HTTP surface. That's a minor local-path disclosure, not a vulnerability on
/// its own here — this API has no remote/multi-tenant exposure today — but
/// worth remembering if this surface is ever opened up further (at which
/// point `to_view` would need to redact/omit `source`'s path for non-owners).
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
    /// Live `ServiceManager` status for each id in `registered.service_ids`
    /// (issue #479). Populated from the SAME `ServiceManager` snapshot the
    /// list handler reads — see [`to_view`]. Empty for a plugin with no
    /// services (the common case), same shape/emptiness convention as
    /// `registered`'s other `Vec` fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub service_status: Vec<ServiceStatusView>,
}

/// Wire projection of [`crate::service_manager::ServiceStatusSnapshot`] —
/// kept as a separate type (rather than reusing the internal snapshot
/// directly) so this HTTP contract doesn't silently change shape if the
/// internal one grows a field later.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceStatusView {
    pub id: String,
    pub state: ServiceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub restart_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
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
pub async fn to_view(
    entry: InstalledPlugin,
    service_manager: &ServiceManager,
) -> InstalledPluginView {
    let name = read_manifest_name(&entry.plugin_dir).await;
    let mut service_status = Vec::with_capacity(entry.registered.service_ids.len());
    for service_id in &entry.registered.service_ids {
        let view = match service_manager.status(service_id).await {
            Some(snapshot) => ServiceStatusView {
                id: snapshot.id,
                state: snapshot.state,
                pid: snapshot.pid,
                restart_count: snapshot.restart_count,
                last_error: snapshot.last_error,
            },
            // Not currently supervised (disabled in the manifest, or its
            // supervisor task already unwound e.g. after `stop_service`) —
            // still surface the id as `Stopped` rather than silently
            // dropping it, so a caller sees every service this plugin owns.
            None => ServiceStatusView {
                id: service_id.clone(),
                state: ServiceState::Stopped,
                pid: None,
                restart_count: 0,
                last_error: None,
            },
        };
        service_status.push(view);
    }
    InstalledPluginView {
        id: entry.id,
        name,
        version: entry.version,
        source: entry.source,
        status: entry.status,
        registered: entry.registered,
        service_status,
    }
}

async fn read_manifest_name(plugin_dir: &std::path::Path) -> Option<String> {
    let raw = tokio::fs::read_to_string(plugin_dir.join("plugin.json"))
        .await
        .ok()?;
    let manifest = PluginManifest::parse_str(&raw).ok()?;
    Some(manifest.name)
}
