//! `ServerPluginInstaller` — the `AppState`-backed implementation of
//! `bamboo_plugin::PluginInstaller` (Wave 2 § Installer-core agent,
//! `PLUGIN_PLAN.md`).
//!
//! `bamboo-plugin` is an `infra`-layer crate with no access to `AppState`, so
//! its `LocalPluginInstaller` reference skeleton stops at
//! `PluginError::NotImplemented` exactly where capability registration needs
//! `config.json`, `mcp_manager`, `prompt-presets.json`, and plugin discovery
//! roots. This type is the real implementation: an ordinary
//! downstream `impl PluginInstaller for ServerPluginInstaller` (the trait is
//! foreign, the type is local — no orphan-rule issue).
//!
//! # Why a borrowed `web::Data<AppState>`
//!
//! `ServerPluginInstaller` holds a `web::Data<AppState>` clone — the exact
//! handle every HTTP handler in this crate already receives as an argument
//! (`web::Data` is `Arc`-backed, so cloning it is cheap). An HTTP handler
//! constructs one per request: `ServerPluginInstaller::new(state.clone())`.
//! The installer coordinates the AppState-owned service manager and ToolEvent
//! router, so runtime registration and revocation share the same lifecycle
//! boundary as durable plugin provenance.
//!
//! # Path derivation: `state.app_data_dir`, not the `bamboo_config::paths` globals
//!
//! `bamboo_config::paths::{plugins_dir, workflows_dir, plugins_installed_json_path, ...}`
//! all resolve through a process-wide `OnceLock` that `AppState::new` seeds
//! ONCE per process (first caller wins — see its doc comment). That is
//! correct for the single production `AppState` per process, but this
//! crate's own test suite already builds many `AppState`s over different
//! `tempfile::tempdir()`s in the same test binary (e.g.
//! `app_state::tests::test_app_state_creation` and friends) — if this type
//! read the global helpers, every one of those `AppState`s would silently
//! share whichever tempdir happened to construct the first one. Every path
//! below is instead derived from the borrowed `state.app_data_dir` field
//! directly, exactly the pattern `handlers::settings::workflows` and
//! `prompt_presets::storage::store_file_path` already use. In production,
//! where there is exactly one `AppState`, this resolves to the identical
//! path the global helpers would have produced.
//!
//! # Concurrency
//!
//! Every `install`/`uninstall` runs under a single process-wide async lock
//! ([`PLUGIN_OP_LOCK`]), held for the ENTIRE operation including rollback, so
//! the reconcile→mutate→provenance sequence is atomic w.r.t. any other plugin
//! op. This closes three concurrency gaps at once: the `installed.json` and
//! `prompt-presets.json` load/modify/save lost-update races, and the MCP
//! reconcile→config-write TOCTOU. As additional defense against a concurrent
//! NON-plugin config write (which does not take this lock), the MCP step also
//! RE-runs its ownership pre-check INSIDE the `update_config` closure, under
//! `config_io_lock`, and aborts rather than clobbering if a foreign entry
//! appeared. Lock ordering is `PLUGIN_OP_LOCK` → `config_io_lock` (never the
//! reverse) — see [`PLUGIN_OP_LOCK`].
//!
//! Plugin workflow markdown is never copied into the user's global workflow
//! directory. It remains inside the plugin bundle and is discovered in place
//! by the SkillStore, so plugin install cannot overwrite a same-named user
//! source and needs no shared workflow-file lock.
//!
//! # Crash safety (process killed mid-install)
//!
//! In-process rollback (below) only fires on an `Err`. A HARD kill after the
//! MCP step wrote to `config.json` but before provenance is committed would,
//! without a journal, leave: `reconcile_exclusive` seeing the orphaned mcp id
//! as existing-but-not-owned → a false `Conflict` on the retry, AND
//! `uninstall` returning `NotFound` (no provenance) → the user stuck
//! hand-editing `config.json`. To prevent that, `install` writes a provenance
//! row with status [`PluginInstallStatus::Installing`] — recording the
//! INTENDED ownership set — BEFORE steps 1-4, and flips it to
//! [`PluginInstallStatus::Installed`] only after step 5 succeeds. On the next
//! install/upgrade of an id whose row is still `Installing` (a prior crash),
//! [`load_previous_for_disposition`] returns it as `previous` (it does NOT
//! trip `AlreadyInstalled`), so its intended set is treated as
//! this-plugin-owned — the leftover reads as an `OwnedReinstall`, not a
//! foreign conflict — and is cleaned up as an upgrade-over-incomplete.
//! `uninstall` works on an `Installing` row too.
//!
//! # Atomicity / rollback semantics
//!
//! `install()` follows `PLUGIN_PLAN.md`'s numbered sequence exactly:
//!
//! 0. **Upgrade drop-diff** (only when upgrading an already-installed id):
//!    de-register whatever the new manifest no longer declares, computed via
//!    [`bamboo_plugin::registry::RegisteredCapabilities::removed_since`],
//!    BEFORE registering anything new. De-registration is idempotent/
//!    best-effort (see [`ServerPluginInstaller::deregister_capabilities`]) —
//!    an entry a user already removed by hand never blocks an upgrade.
//! 1. **MCP** — ownership-checked (REFUSE on a foreign conflict, via
//!    [`bamboo_plugin::registry::reconcile_exclusive`]), merged into
//!    `config.json`, started.
//! 2. **Prompts** — rename-on-collision (never refuse), appended to
//!    `prompt-presets.json`.
//! 3. **Workflows** — validated for safe in-place discovery; no shared-store
//!    copy or ownership mutation.
//! 4. **Skills** — nothing to register (discovered in place); just recorded.
//! 5. **Provenance commit** — `installed.json` is only ever upserted after
//!    steps 0-4 all succeed.
//!
//! Steps 1-2 are real, sequential mutations (config then prompt-store writes)
//! — NOT a dry-run computed up front — because `PLUGIN_PLAN.md`
//! requires the ownership pre-checks to run in that exact order against the
//! LIVE state each step leaves behind. That means a HARD failure at step 2
//! or the workflow validation in step 3 can happen after step 1 already wrote
//! real entries into `config.json`. [`ServerPluginInstaller::install`] tracks
//! every already-applied mutation in an [`InstallRollback`] and, on any hard
//! failure from steps 1-3, best-effort UNDOES them (removes the mcp entries
//! it just added and stops any it started, removes the presets it just
//! appended, and removes services it started) before returning the
//! error — so a caller's retry starts from a clean slate. Provenance is
//! never written on a failed path (step 5 is the only place `installed.json`
//! is touched on success), which is the minimum safety bar even if a rollback
//! step itself only partially succeeds (rollback operations are themselves
//! idempotent/log-and-continue, so a second rollback attempt via a plain
//! retry can never fail louder than the first).
//!
//! The production HTTP path prepares source bytes in an isolated directory,
//! then retains [`PluginOperationGuard`] across global ownership preflight,
//! old-service shutdown, bundle activation, registration, and rollback. The
//! on-disk swap therefore shares the same serialization boundary as the
//! provenance/config mutations. Standalone callers of the lower-level trait
//! remain responsible for staging serialization; see `crate::plugin_source`.
//!
//! Prompt-preset drop-diff caveat: the upgrade drop-diff compares the NEW
//! manifest's nominal preset ids against the OLD install's ACTUAL (possibly
//! renamed-on-collision) registered ids. A preset that got renamed at its
//! original install time and is still declared under its original nominal id
//! in the new manifest will look "dropped" (the nominal id is absent from the
//! actual old set) and get re-appended (possibly renamed again). This is
//! harmless — preset content is just refreshed under a fresh id — and not
//! worth a stable-id-mapping schema change for what `RegisteredCapabilities`
//! already documents as the one rename (not refuse) exception.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::fs;

use bamboo_domain::mcp_config::McpServerConfig;
use bamboo_plugin::installer::{load_previous_for_disposition, preflight_install};
use bamboo_plugin::manifest::{Platform, ServiceManifestEntry};
use bamboo_plugin::registry::{
    reconcile_event_sinks, reconcile_exclusive, reconcile_plugin_boot, PluginBootCandidate,
    RegisteredCapabilities,
};
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, InstalledPlugins, PluginError, PluginInstallStatus,
    PluginInstaller, PluginManifest, PluginResult, PluginSource,
};

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::error::AppError;
use crate::handlers::agent::mcp::upsert_server_by_id;
use crate::handlers::agent::prompt_presets::{
    ensure_unique_preset_id, load_store, save_store, store_file_path, StoredPromptPreset,
};
use crate::service_manager::{ServiceManager, ServiceRuntimeConfig};
use crate::tool_event_router::ToolEventRouter;

/// Process-wide serialization of plugin install/uninstall operations.
///
/// The whole ownership/upgrade machinery is a read-modify-write over shared
/// stores (`config.json`, `prompt-presets.json`, `installed.json`) with the
/// ownership pre-check and the eventual mutation in separate steps. Under
/// CONCURRENT plugin ops (the HTTP agent will expose exactly that) those
/// interleave badly: two installs of different ids race `installed.json`'s
/// load/add/save (last save drops the other's row), `prompt-presets.json`'s
/// load/save (lost update), and the MCP reconcile→write window (a foreign
/// entry landing mid-window gets clobbered AND recorded as plugin-owned,
/// re-opening BLOCKER-1). Plugin installs are rare and not perf-sensitive, so
/// one coarse process-wide lock held across the ENTIRE `install`/`uninstall`
/// (including rollback) is the right call — it makes each op's
/// reconcile→mutate→provenance sequence atomic w.r.t. every other plugin op.
///
/// Lock ordering: this lock is acquired at the TOP of `install`/`uninstall`,
/// OUTSIDE any `AppState::update_config` call (which internally takes
/// `config_io_lock`). So the order is always `PLUGIN_OP_LOCK` →
/// `config_io_lock`, never the reverse — no deadlock. Nothing acquires
/// `PLUGIN_OP_LOCK` while holding `config_io_lock`.
///
/// # Single-process assumption (deferred: no cross-process lock)
///
/// This is a `tokio::sync::Mutex` — IN-PROCESS only. It serializes plugin ops
/// within one `bamboo serve` process, but two SEPARATE `bamboo serve`
/// processes pointed at the same `~/.bamboo` data dir would each get their
/// own independent `PLUGIN_OP_LOCK` and could race each other's
/// reconcile→mutate→provenance sequence exactly the way this lock exists to
/// prevent for concurrent ops WITHIN one process. The plugin system assumes
/// the normal deployment: a SINGLE `bamboo serve` per data directory. True
/// multi-process safety would need an OS-level file lock (e.g. `flock` on a
/// lockfile under `plugins_dir()`) instead of/in addition to this `Mutex`;
/// that's a documented follow-up, not implemented here.
static PLUGIN_OP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Proof that the caller holds the process-wide plugin-operation boundary.
/// HTTP source preparation uses this guard across ownership preflight, old
/// service shutdown, bundle activation, installer mutation, and rollback.
pub(crate) struct PluginOperationGuard {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

/// AppState-backed [`PluginInstaller`]. See the module docs for the full
/// design rationale (borrowing, path derivation, atomicity).
pub struct ServerPluginInstaller {
    state: actix_web::web::Data<AppState>,
}

/// Mutations already applied by a not-yet-committed `install()`, so a hard
/// failure partway through steps 1-3 can best-effort undo exactly what has
/// been done so far. See the module docs' "Atomicity / rollback semantics".
#[derive(Default)]
struct InstallRollback {
    mcp_ids_added: Vec<String>,
    preset_ids_added: Vec<String>,
    /// Service ids this install claimed ownership of (whether or not the
    /// actual `start_service` call succeeded — best-effort).
    service_ids_added: Vec<String>,
    /// Subset of `service_ids_added` that actually got a running
    /// `ServiceManager` runtime started — only these need `stop_service` on
    /// rollback.
    service_ids_started: Vec<String>,
}

#[derive(Default)]
struct InstallFailureInjection {
    before_service_replacement: bool,
    final_provenance_commit: bool,
}

impl ServerPluginInstaller {
    pub fn new(state: actix_web::web::Data<AppState>) -> Self {
        Self { state }
    }

    pub(crate) async fn begin_operation(&self) -> PluginOperationGuard {
        PluginOperationGuard {
            _guard: PLUGIN_OP_LOCK.lock().await,
        }
    }

    async fn preflight_provenance_ownership(
        &self,
        manifest: &PluginManifest,
    ) -> PluginResult<bamboo_plugin::registry::ExclusiveReconciliation> {
        let declared_event_sink_ids: Vec<String> = manifest
            .provides
            .event_sinks
            .iter()
            .map(|sink| sink.id.clone())
            .collect();
        // `existing_*` contains ONLY other plugin rows. Any hit is therefore
        // foreign even when a corrupt current row also claims the same id;
        // current previous ownership must never override it.
        let event_sinks = reconcile_exclusive(
            &declared_event_sink_ids,
            &self.existing_event_sink_ids(&manifest.id).await?,
            &[],
        );
        if !event_sinks.foreign_conflicts.is_empty() {
            return Err(PluginError::Conflict {
                kind: "event sink",
                name: event_sinks.foreign_conflicts.join(", "),
                plugin_id: manifest.id.clone(),
            });
        }

        let declared_service_ids: Vec<String> = manifest
            .provides
            .services
            .iter()
            .map(|service| service.id.clone())
            .collect();
        let services = reconcile_exclusive(
            &declared_service_ids,
            &self.existing_service_ids(&manifest.id).await?,
            &[],
        );
        if !services.foreign_conflicts.is_empty() {
            return Err(PluginError::Conflict {
                kind: if manifest.provides.event_sinks.iter().any(|sink| {
                    services
                        .foreign_conflicts
                        .iter()
                        .any(|id| id == &sink.service_id)
                }) {
                    "event sink service"
                } else {
                    "service"
                },
                name: services.foreign_conflicts.join(", "),
                plugin_id: manifest.id.clone(),
            });
        }
        Ok(event_sinks)
    }

    /// Validate an isolated candidate while holding the same operation lock
    /// that will cover activation and install. No live bundle, service,
    /// config, or provenance state is mutated here.
    pub(crate) async fn preflight_prepared_candidate(
        &self,
        manifest: &PluginManifest,
        prepared_dir: &Path,
        disposition: InstallDisposition,
        _guard: &PluginOperationGuard,
    ) -> PluginResult<Option<InstalledPlugin>> {
        let previous =
            load_previous_for_disposition(&self.installed_json_path(), &manifest.id, disposition)
                .await?;
        preflight_install(manifest, prepared_dir).await?;
        self.preflight_provenance_ownership(manifest).await?;
        Ok(previous)
    }

    fn plugins_dir(&self) -> PathBuf {
        self.state.app_data_dir.join("plugins")
    }

    fn installed_json_path(&self) -> PathBuf {
        self.plugins_dir().join("installed.json")
    }

    fn workflows_dir(&self) -> PathBuf {
        self.state.app_data_dir.join("workflows")
    }

    fn prompt_presets_path(&self) -> PathBuf {
        store_file_path(&self.state.app_data_dir)
    }

    /// `<data_dir>/plugin_service_config/<plugin_id>/config.json` — the
    /// per-service user config path passed to services as
    /// `BAMBOO_PLUGIN_SERVICE_CONFIG` (issue #479 open question 2).
    ///
    /// Deliberately NOT under `plugins_dir()/<plugin_id>/` (the
    /// swap-managed `plugin_dir`): plugin-source activation upgrades a plugin
    /// by renaming the ENTIRE old `plugin_dir` aside and swapping a prepared
    /// directory into its place — any file living
    /// inside `plugin_dir` would be swept away with the old bundle on
    /// upgrade (or deleted outright on uninstall) unless bamboo specifically
    /// carried it forward, which it does not. A sibling directory, named
    /// only by `plugin_id`, is untouched by that swap and by
    /// `uninstall`'s `remove_dir_all(plugin_dir)` — so a service's own
    /// config (which may carry tokens/secrets a connector needs) survives
    /// both an upgrade and an uninstall. bamboo only ever creates the PARENT
    /// directory here (see [`Self::ensure_service_config_parent_dir`]) —
    /// never writes or deletes `config.json` itself; on uninstall it is
    /// deliberately left in place (not part of `remove_dir_all`), so a
    /// later re-install of the same plugin id picks its old config back up
    /// automatically.
    fn service_config_path(&self, plugin_id: &str) -> PathBuf {
        service_config_path_under(&self.state.app_data_dir, plugin_id)
    }

    async fn ensure_service_config_parent_dir(&self, plugin_id: &str) -> PluginResult<()> {
        let path = self.service_config_path(plugin_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// Every service id currently owned by any OTHER installed plugin — the
    /// "existing" side of [`reconcile_exclusive`] for services. There is no
    /// single shared document analogous to `config.json` for services, so
    /// provenance itself is the source of truth for "who owns this id".
    ///
    /// `exclude_plugin_id` MUST be the id of the plugin currently being
    /// installed/upgraded and is always excluded — NOT an optional
    /// nicety: by the time [`Self::register_services`] calls this,
    /// `install()`'s crash-safety journal write has ALREADY upserted THIS
    /// plugin's `Installing` row into `installed.json` with its full
    /// INTENDED `service_ids` (see `install()`'s "Crash-safety journal"
    /// step, which runs before every registration step). Without excluding
    /// it, a plain fresh install would see its own not-yet-committed row as
    /// a foreign owner of its own declared ids and refuse itself.
    /// Because this query excludes the current row, every returned id is
    /// unambiguously foreign. Re-declared ids from the current plugin remain
    /// absent here and are recorded from the new manifest after preflight.
    async fn existing_service_ids(&self, exclude_plugin_id: &str) -> PluginResult<Vec<String>> {
        let store = InstalledPlugins::load(&self.installed_json_path()).await?;
        store.get_unique(exclude_plugin_id)?;
        Ok(store
            .list()
            .iter()
            .filter(|plugin| plugin.id != exclude_plugin_id)
            .flat_map(|plugin| plugin.registered.service_ids.iter().cloned())
            .collect())
    }

    /// Event-sink ids are process/AppState registration keys. Installed
    /// provenance remains the authoritative global ownership index; the live
    /// router only activates a reconciliation plan after this check has
    /// rejected cross-plugin borrowing before any install mutation.
    async fn existing_event_sink_ids(&self, exclude_plugin_id: &str) -> PluginResult<Vec<String>> {
        let store = InstalledPlugins::load(&self.installed_json_path()).await?;
        store.get_unique(exclude_plugin_id)?;
        Ok(store
            .list()
            .iter()
            .filter(|plugin| plugin.id != exclude_plugin_id)
            .flat_map(|plugin| plugin.registered.event_sink_ids.iter().cloned())
            .collect())
    }

    /// Resolve one manifest-declared service entry into a
    /// [`ServiceRuntimeConfig`] ready for `ServiceManager::start_service`.
    fn resolve_service_config(
        &self,
        plugin_id: &str,
        entry: &ServiceManifestEntry,
        plugin_dir: &Path,
        platform: Platform,
    ) -> ServiceRuntimeConfig {
        resolve_service_config_under(
            &self.state.app_data_dir,
            plugin_id,
            entry,
            plugin_dir,
            platform,
        )
    }

    // --- De-registration primitives (shared by upgrade drop-diff, install
    // rollback, and `uninstall`). Each is individually idempotent/tolerant —
    // an entry that is already gone (e.g. a user manually deleted it) is
    // logged and skipped, never a hard failure — matching the requirement
    // that de-registration never blocks an uninstall/upgrade retry. ---

    async fn remove_mcp_server(&self, id: &str) {
        let owned_id = id.to_string();
        let result = self
            .state
            .update_config(
                move |config| {
                    config.mcp.servers.retain(|server| server.id != owned_id);
                    Ok(())
                },
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::None,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::BestEffort,
                },
            )
            .await;
        if let Err(error) = result {
            tracing::warn!(
                mcp_server_id = %id,
                %error,
                "failed to remove plugin-owned mcp server from config.json; continuing"
            );
        }
    }

    async fn remove_prompt_preset(&self, preset_id: &str) {
        let path = self.prompt_presets_path();
        match load_store(&path).await {
            Ok(mut store) => {
                let before = store.prompts.len();
                store.prompts.retain(|preset| preset.id != preset_id);
                if store.prompts.len() != before {
                    if let Err(error) = save_store(&path, &store).await {
                        tracing::warn!(
                            %preset_id,
                            %error,
                            "failed to persist prompt-presets.json after removing plugin-owned preset; continuing"
                        );
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    %preset_id,
                    %error,
                    "failed to load prompt-presets.json while removing plugin-owned preset; continuing"
                );
            }
        }
    }

    async fn remove_service(&self, id: &str) {
        if let Err(error) = self.state.service_manager.stop_service(id).await {
            tracing::warn!(
                service_id = %id,
                %error,
                "failed to stop plugin-owned service; continuing"
            );
        }
    }

    async fn remove_workflow_file(&self, filename: &str) {
        let path = self.workflows_dir().join(filename);
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    %filename,
                    %error,
                    "failed to remove plugin-owned workflow file; continuing"
                );
            }
        }
    }

    /// De-register a whole [`RegisteredCapabilities`] set (used for the
    /// upgrade drop-diff and for `uninstall`). Skill dirs need no shared-store
    /// action — they are only ever removed by deleting `plugin_dir` itself.
    async fn deregister_capabilities(&self, registered: &RegisteredCapabilities) {
        // #903's removal contract is authority-bearing: revoke the hot routing
        // snapshot and await every exact-generation worker before the backing
        // service can be stopped by the loop below.
        self.state
            .tool_event_router
            .unregister_sinks(&registered.removal_order().event_sink_ids_before_services)
            .await;
        for mcp_id in &registered.mcp_server_ids {
            self.remove_mcp_server(mcp_id).await;
        }
        for preset_id in &registered.preset_ids {
            self.remove_prompt_preset(preset_id).await;
        }
        for workflow_filename in &registered.workflow_filenames {
            self.remove_workflow_file(workflow_filename).await;
        }
        for service_id in &registered.service_ids {
            self.remove_service(service_id).await;
        }
    }

    /// Apply an upgrade's id-level drop-diff without ever stopping a service
    /// beneath a still-live sink generation. A retained sink id may change
    /// its backing service id, which `RegisteredCapabilities::removed_since`
    /// cannot express because provenance stores capability ids rather than
    /// sink-to-service edges. Before stopping dropped services, revoke only
    /// prior sinks whose current router declaration is actually backed by one
    /// of those services. Unrelated retained routes must survive failures
    /// before the later full service-replacement seam.
    async fn deregister_upgrade_drop_diff(
        &self,
        plugin_id: &str,
        previous: &RegisteredCapabilities,
        dropped: &RegisteredCapabilities,
    ) {
        self.state
            .tool_event_router
            .unregister_plugin_sinks_backed_by_services(
                plugin_id,
                &previous.event_sink_ids,
                &dropped.service_ids,
            )
            .await;
        self.deregister_capabilities(dropped).await;
    }

    /// Best-effort undo of an `install()` that failed partway through steps
    /// 1-3. See the module docs.
    async fn rollback_partial_install(&self, rollback: &InstallRollback) {
        for id in &rollback.mcp_ids_added {
            self.remove_mcp_server(id).await;
        }
        for id in &rollback.preset_ids_added {
            self.remove_prompt_preset(id).await;
        }
        for id in &rollback.service_ids_started {
            let _ = self.state.service_manager.stop_service(id).await;
        }
    }

    /// Upsert one provenance row into `installed.json`. Used both for the
    /// pre-registration `Installing` journal row and the final `Installed`
    /// commit — the ONLY two writers of `installed.json` in `install`. Both
    /// run under [`PLUGIN_OP_LOCK`], so the load/add/save is race-free.
    async fn upsert_provenance(&self, entry: InstalledPlugin, path: &Path) -> PluginResult<()> {
        let mut store = InstalledPlugins::load(path).await?;
        store.get_unique(&entry.id)?;
        store.add(entry);
        store.save(path).await?;
        Ok(())
    }

    /// Abort an in-process `install` failure: best-effort undo of the partial
    /// registration (steps 1-3), then restore `installed.json` to its
    /// pre-install state — re-writing the original `previous` row on an
    /// upgrade/recovery, or removing the id's row entirely on a fresh install
    /// — so the `Installing` journal row we wrote up front never lingers after
    /// a clean in-process failure. (A HARD kill is the only path that
    /// intentionally leaves an `Installing` row, for the next op to recover.)
    async fn abort_install(
        &self,
        rollback: &InstallRollback,
        previous: &Option<InstalledPlugin>,
        plugin_id: &str,
        path: &Path,
    ) {
        self.rollback_partial_install(rollback).await;
        let restore = match previous {
            Some(prev) => self.upsert_provenance(prev.clone(), path).await,
            None => match InstalledPlugins::load(path).await {
                Ok(mut store) => {
                    store.remove(plugin_id);
                    store.save(path).await
                }
                Err(error) => Err(error),
            },
        };
        if let Err(error) = restore {
            tracing::warn!(
                %plugin_id,
                %error,
                "failed to restore provenance after aborting a failed install; a stale \
                 `installing` row may remain (recoverable by a retry)"
            );
        }
    }

    /// Step 1: MCP. Returns the ids actually (re-)registered
    /// (`reconciliation.to_register`), which — once past the conflict gate —
    /// is exactly the declared id set.
    async fn register_mcp(
        &self,
        manifest: &PluginManifest,
        resolved_mcp_servers: Vec<McpServerConfig>,
        previously_owned: &[String],
        rollback: &mut InstallRollback,
    ) -> PluginResult<Vec<String>> {
        if resolved_mcp_servers.is_empty() {
            return Ok(Vec::new());
        }

        let declared_ids: Vec<String> = manifest
            .provides
            .mcp_servers
            .iter()
            .map(|entry| entry.id.clone())
            .collect();
        let existing_ids: Vec<String> = {
            let config = self.state.config.read().await;
            config.mcp.servers.iter().map(|s| s.id.clone()).collect()
        };

        let reconciliation = reconcile_exclusive(&declared_ids, &existing_ids, previously_owned);
        if !reconciliation.foreign_conflicts.is_empty() {
            return Err(PluginError::Conflict {
                kind: "mcp server",
                name: reconciliation.foreign_conflicts.join(", "),
                plugin_id: manifest.id.clone(),
            });
        }

        let to_register: HashSet<&str> = reconciliation
            .to_register
            .iter()
            .map(String::as_str)
            .collect();
        let configs_to_register: Vec<McpServerConfig> = resolved_mcp_servers
            .into_iter()
            .filter(|config| to_register.contains(config.id.as_str()))
            .collect();

        let owned_configs = configs_to_register.clone();
        let declared_for_recheck = declared_ids.clone();
        let owned_for_recheck: Vec<String> = previously_owned.to_vec();
        let plugin_id_for_recheck = manifest.id.clone();
        let forced_mcp_replacements = reconciliation.to_register.iter().cloned().collect();
        self.state
            .update_config_with_forced_mcp_replacements(
                move |config| {
                    // TOCTOU guard: re-run the ownership pre-check against the
                    // LIVE config while holding config_io_lock, so a foreign
                    // entry that landed between our earlier read and now can't
                    // be silently clobbered (and then recorded as
                    // plugin-owned, re-opening BLOCKER-1 under a race).
                    // Concurrent PLUGIN ops are already excluded by
                    // PLUGIN_OP_LOCK; this closes the residual window against a
                    // concurrent NON-plugin config write.
                    let live_existing: Vec<String> =
                        config.mcp.servers.iter().map(|s| s.id.clone()).collect();
                    let live = reconcile_exclusive(
                        &declared_for_recheck,
                        &live_existing,
                        &owned_for_recheck,
                    );
                    if !live.foreign_conflicts.is_empty() {
                        return Err(AppError::BadRequest(format!(
                            "mcp server(s) '{}' now conflict with a non-plugin entry (a concurrent \
                             change landed mid-install); refusing to overwrite for plugin '{}'",
                            live.foreign_conflicts.join(", "),
                            plugin_id_for_recheck
                        )));
                    }
                    // Shared by-id merge (same helper import_servers uses).
                    for server in &owned_configs {
                        upsert_server_by_id(&mut config.mcp.servers, server.clone());
                    }
                    Ok(())
                },
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::None,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::BestEffort,
                },
                forced_mcp_replacements,
            )
            .await
            .map_err(|error| {
                PluginError::Registration(format!("failed to write mcp servers to config: {error}"))
            })?;
        // The config generation is committed and live before ownership is
        // claimed. Runtime activation is generation-serialized and reports
        // degraded MCP health on failure, preserving the installer's existing
        // best-effort activation contract without an out-of-lock start.
        rollback.mcp_ids_added = reconciliation.to_register.clone();

        Ok(reconciliation.to_register)
    }

    /// Step 1b: Services (issue #479, prereq for epic #477). Same
    /// REFUSE-on-conflict + best-effort-start shape as [`Self::register_mcp`],
    /// against [`Self::existing_service_ids`] instead of `config.json` (there
    /// is no shared config document for services — provenance itself is the
    /// ownership store, see that method's doc comment).
    async fn register_services(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        rollback: &mut InstallRollback,
    ) -> PluginResult<Vec<String>> {
        if manifest.provides.services.is_empty() {
            return Ok(Vec::new());
        }

        let declared_ids: Vec<String> = manifest
            .provides
            .services
            .iter()
            .map(|entry| entry.id.clone())
            .collect();
        let existing_ids = self.existing_service_ids(&manifest.id).await?;
        // `existing_ids` excludes this plugin row, so every collision is
        // foreign even if corrupt provenance also records it under self.
        let reconciliation = reconcile_exclusive(&declared_ids, &existing_ids, &[]);
        if !reconciliation.foreign_conflicts.is_empty() {
            return Err(PluginError::Conflict {
                kind: "service",
                name: reconciliation.foreign_conflicts.join(", "),
                plugin_id: manifest.id.clone(),
            });
        }

        // Ownership is claimed regardless of individual start outcomes below
        // (matches `register_mcp`'s "config write succeeded, so record
        // ownership" contract — here there is no config write, so this is
        // simply claimed up front).
        rollback.service_ids_added = reconciliation.to_register.clone();

        self.ensure_service_config_parent_dir(&manifest.id).await?;
        let platform = Platform::current().unwrap_or(Platform::Linux);
        let to_register: HashSet<&str> = reconciliation
            .to_register
            .iter()
            .map(String::as_str)
            .collect();

        for entry in &manifest.provides.services {
            if !to_register.contains(entry.id.as_str()) {
                continue;
            }
            // Stop any stale running instance first — covers a leftover
            // from a crashed install/upgrade recovery (a genuine same-id
            // upgrade already had its old service stopped BEFORE activation by
            // `stop_services_for_upgrade`, see the module docs' "Same-id
            // upgrade ordering").
            let _ = self.state.service_manager.stop_service(&entry.id).await;
            if !entry.enabled {
                continue;
            }
            let config = self.resolve_service_config(&manifest.id, entry, plugin_dir, platform);
            match self.state.service_manager.start_service(config).await {
                Ok(()) => rollback.service_ids_started.push(entry.id.clone()),
                Err(error) => tracing::warn!(
                    service_id = %entry.id,
                    %error,
                    "plugin-registered service failed to start; ownership kept (best-effort, matches mcp)"
                ),
            }
        }

        Ok(reconciliation.to_register)
    }

    /// Step 2: Prompts. Rename-on-collision (never refuses) — returns the
    /// ACTUAL ids used (after any rename), which is what provenance must
    /// record.
    async fn register_prompts(&self, manifest: &PluginManifest) -> PluginResult<Vec<String>> {
        if manifest.provides.prompts.is_empty() {
            return Ok(Vec::new());
        }

        let path = self.prompt_presets_path();
        let mut store = load_store(&path).await.map_err(|error| {
            PluginError::Registration(format!("failed to load prompt-presets.json: {error}"))
        })?;

        let mut existing_ids: HashSet<String> = store
            .prompts
            .iter()
            .map(|preset| preset.id.clone())
            .collect();
        // `general_assistant` (bamboo-server's DEFAULT_PRESET_ID) is never a
        // row in the store, so it wouldn't otherwise appear in `existing_ids`
        // — but manifest validation already rejects any plugin declaring it
        // (RESERVED_PRESET_IDS), so no extra guard is needed here.

        let mut actual_ids = Vec::with_capacity(manifest.provides.prompts.len());
        for preset in &manifest.provides.prompts {
            let actual_id = ensure_unique_preset_id(&preset.id, &existing_ids);
            store.prompts.push(StoredPromptPreset {
                id: actual_id.clone(),
                name: preset.name.clone(),
                description: preset.description.clone(),
                content: preset.content.clone(),
            });
            existing_ids.insert(actual_id.clone());
            actual_ids.push(actual_id);
        }

        save_store(&path, &store).await.map_err(|error| {
            PluginError::Registration(format!("failed to persist prompt-presets.json: {error}"))
        })?;

        Ok(actual_ids)
    }

    /// Step 3: validate legacy plugin workflows for in-place discovery.
    ///
    /// Workflow markdown stays under `<plugin_dir>/workflows`; the SkillStore
    /// discovers it as a read-only legacy adapter. Nothing is copied into the
    /// shared global workflows directory, so a plugin filename can never
    /// conflict with or overwrite a user's own legacy workflow.
    async fn validate_workflows_in_place(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
    ) -> PluginResult<()> {
        if manifest.provides.workflows.is_empty() {
            return Ok(());
        }

        let workflows_dir = plugin_dir.join("workflows");
        let directory_metadata = fs::symlink_metadata(&workflows_dir).await?;
        if !directory_metadata.is_dir() || directory_metadata.file_type().is_symlink() {
            return Err(PluginError::InvalidManifest(
                "plugin workflows must live in a real workflows directory".to_string(),
            ));
        }
        let declared: HashSet<&str> = manifest
            .provides
            .workflows
            .iter()
            .map(String::as_str)
            .collect();
        let mut actual = HashSet::new();
        let mut entries = fs::read_dir(&workflows_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("md") {
                continue;
            }
            let file_type = entry.file_type().await?;
            let Some(filename) = entry.file_name().to_str().map(str::to_string) else {
                return Err(PluginError::InvalidManifest(
                    "plugin workflow filename must be UTF-8".to_string(),
                ));
            };
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(PluginError::InvalidManifest(format!(
                    "workflow '{filename}' must be a regular in-place markdown file"
                )));
            }
            actual.insert(filename);
        }
        if actual.len() != declared.len()
            || !actual.iter().all(|name| declared.contains(name.as_str()))
        {
            return Err(PluginError::InvalidManifest(
                "provides.workflows must declare every workflows/*.md file exactly once"
                    .to_string(),
            ));
        }

        for filename in &manifest.provides.workflows {
            let stem = filename.strip_suffix(".md").unwrap_or(filename);
            if !bamboo_config::paths::is_safe_workflow_name(stem) {
                return Err(PluginError::InvalidManifest(format!(
                    "workflow filename '{filename}' is not a safe workflow name"
                )));
            }
            let source_path = workflows_dir.join(filename);
            let metadata = fs::symlink_metadata(&source_path).await?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(PluginError::InvalidManifest(format!(
                    "workflow '{filename}' must be a regular in-place markdown file"
                )));
            }
        }
        Ok(())
    }

    /// **Same-id upgrade ordering** (issue #479): after a source candidate is
    /// prepared and its global ownership audit succeeds, the HTTP update path
    /// calls this while retaining [`PluginOperationGuard`], then activates the
    /// bundle and invokes [`Self::install_with_operation`]. A still-running old
    /// process can therefore neither hold the replaced binary open nor run
    /// stale code after the swap. Net effect: preflight → stop old binary →
    /// swap → start new binary, all in one serialized operation boundary.
    ///
    /// Returns exactly the ids that were actually running and got stopped
    /// (not e.g. an already-stopped or unknown id). If any later source
    /// transaction step fails, those services deliberately remain stopped
    /// for explicit operator recovery. A plugin with no prior install (or no
    /// services) returns an empty vec and stops nothing.
    pub(crate) async fn stop_services_for_upgrade(&self, plugin_id: &str) -> Vec<String> {
        let store = match InstalledPlugins::load(&self.installed_json_path()).await {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(
                    %plugin_id,
                    %error,
                    "stop_services_for_upgrade: failed to load installed.json; skipping"
                );
                return Vec::new();
            }
        };
        let entry = match store.get_unique(plugin_id) {
            Ok(Some(entry)) => entry,
            Ok(None) => return Vec::new(),
            Err(error) => {
                tracing::warn!(
                    %plugin_id,
                    %error,
                    "stop_services_for_upgrade: ambiguous plugin provenance; skipping"
                );
                return Vec::new();
            }
        };
        let mut stopped = Vec::with_capacity(entry.registered.service_ids.len());
        self.state
            .tool_event_router
            .unregister_sinks(
                &entry
                    .registered
                    .removal_order()
                    .event_sink_ids_before_services,
            )
            .await;
        for service_id in &entry.registered.service_ids {
            match self.state.service_manager.stop_service(service_id).await {
                Ok(()) => stopped.push(service_id.clone()),
                Err(error) => tracing::debug!(
                    service_id = %service_id,
                    %error,
                    "stop_services_for_upgrade: service was not running; nothing to stop"
                ),
            }
        }
        stopped
    }

    pub(crate) async fn install_with_operation(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
        _guard: &PluginOperationGuard,
    ) -> PluginResult<InstalledPlugin> {
        self.install_with_operation_inner(
            manifest,
            plugin_dir,
            source,
            disposition,
            installed_at,
            _guard,
            InstallFailureInjection::default(),
        )
        .await
    }

    /// Deterministic test seam immediately after the Step-0 drop-diff and
    /// crash-safety journal, but before every prior sink is revoked for
    /// possible same-id service replacement.
    #[cfg(all(test, unix))]
    pub(crate) async fn install_with_operation_failing_before_service_replacement(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
        guard: &PluginOperationGuard,
    ) -> PluginResult<InstalledPlugin> {
        self.install_with_operation_inner(
            manifest,
            plugin_dir,
            source,
            disposition,
            installed_at,
            guard,
            InstallFailureInjection {
                before_service_replacement: true,
                ..InstallFailureInjection::default()
            },
        )
        .await
    }

    /// Deterministic test seam for the final `Installing` -> `Installed`
    /// provenance commit. It exercises the complete registration and abort
    /// path without depending on platform-specific chmod/locking behavior.
    #[cfg(test)]
    pub(crate) async fn install_with_operation_failing_final_commit(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
        guard: &PluginOperationGuard,
    ) -> PluginResult<InstalledPlugin> {
        self.install_with_operation_inner(
            manifest,
            plugin_dir,
            source,
            disposition,
            installed_at,
            guard,
            InstallFailureInjection {
                final_provenance_commit: true,
                ..InstallFailureInjection::default()
            },
        )
        .await
    }

    async fn install_with_operation_inner(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
        _guard: &PluginOperationGuard,
        failure_injection: InstallFailureInjection,
    ) -> PluginResult<InstalledPlugin> {
        let installed_json_path = self.installed_json_path();

        // Disposition gate (AlreadyInstalled only for a COMPLETED prior
        // install; an `Installing` leftover is returned for recovery) + the
        // rest of the pure, AppState-free validation this crate can already
        // do (manifest shape, platform gate, on-disk skill/workflow
        // existence, `provides.skills` authoritativeness).
        let previous =
            load_previous_for_disposition(&installed_json_path, &manifest.id, disposition).await?;
        let resolved_mcp_servers = preflight_install(manifest, plugin_dir).await?;

        // Re-run under the held operation guard as defense in depth. The HTTP
        // prepared-source path performs the same audit before bundle swap or
        // old-service shutdown; direct trait callers still get this gate
        // before installer-owned provenance/config/runtime mutation.
        let event_sink_reconciliation = self.preflight_provenance_ownership(manifest).await?;
        let declared_event_sink_ids = event_sink_reconciliation.to_register.clone();

        // The set this install INTENDS to own, by declaration order. Used both
        // for the crash-safety journal row (below) and the step-0 drop-diff.
        let intended = RegisteredCapabilities {
            mcp_server_ids: manifest
                .provides
                .mcp_servers
                .iter()
                .map(|entry| entry.id.clone())
                .collect(),
            skill_dirs: manifest.provides.skills.clone(),
            preset_ids: manifest
                .provides
                .prompts
                .iter()
                .map(|preset| preset.id.clone())
                .collect(),
            // New workflow publications are discovered in place. Keeping this
            // legacy copied-file field empty also makes an upgrade clean up
            // files copied by pre-#561 installs through the drop-diff below.
            workflow_filenames: Vec::new(),
            service_ids: manifest
                .provides
                .services
                .iter()
                .map(|entry| entry.id.clone())
                .collect(),
            event_sink_ids: declared_event_sink_ids,
        };

        // Step 0: upgrade drop-diff. Computed from the NEW manifest's plain
        // declared ids (see module docs re: the preset-rename caveat) vs the
        // OLD install's registered set — de-register whatever the new version
        // no longer declares BEFORE registering anything new (BLOCKER 2). Also
        // fires for a recovery over an `Installing` leftover: its intended set
        // is diffed the same way, so a crashed attempt's extra ids get cleaned.
        if let Some(previous) = &previous {
            let dropped = intended.removed_since(&previous.registered);
            if !dropped.is_empty() {
                tracing::info!(
                    plugin_id = %manifest.id,
                    recovering = previous.status == PluginInstallStatus::Installing,
                    dropped_mcp = ?dropped.mcp_server_ids,
                    dropped_presets = ?dropped.preset_ids,
                    dropped_workflows = ?dropped.workflow_filenames,
                    dropped_services = ?dropped.service_ids,
                    dropped_event_sinks = ?dropped.event_sink_ids,
                    "install drop-diff: de-registering capabilities the new/completed version no longer declares"
                );
                self.deregister_upgrade_drop_diff(&manifest.id, &previous.registered, &dropped)
                    .await;
            }
        }

        let previously_owned_mcp = previous
            .as_ref()
            .map(|p| p.registered.mcp_server_ids.clone())
            .unwrap_or_default();
        // Crash-safety journal: write an `Installing` provenance row recording
        // the INTENDED ownership set BEFORE mutating any shared store, so a
        // hard kill mid-install leaves a recoverable marker (see module docs
        // "Crash safety"). On a fresh install this creates the row; on an
        // upgrade/recovery it overwrites the prior row.
        self.upsert_provenance(
            InstalledPlugin {
                id: manifest.id.clone(),
                version: manifest.version.clone(),
                source: source.clone(),
                plugin_dir: plugin_dir.to_path_buf(),
                installed_at,
                status: PluginInstallStatus::Installing,
                registered: intended.clone(),
            },
            &installed_json_path,
        )
        .await?;

        let mut rollback = InstallRollback::default();

        // Step 1: MCP.
        let mcp_server_ids = match self
            .register_mcp(
                manifest,
                resolved_mcp_servers,
                &previously_owned_mcp,
                &mut rollback,
            )
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                    .await;
                return Err(error);
            }
        };

        if failure_injection.before_service_replacement {
            let error = PluginError::Registration(
                "injected failure before service replacement".to_string(),
            );
            self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                .await;
            return Err(error);
        }

        // Direct `PluginInstaller::install(..., Upgrade, ...)` callers do not
        // pass through plugin_source's pre-swap stop hook. Revoke every prior
        // sink generation at the last common point before `register_services`
        // can stop/replace a same-id service. When Step 0 did not drop an old
        // service, keeping this after the journal and MCP step means an
        // earlier failure leaves that still-running service and route intact.
        // Step 0 revoked only sinks backed by services it actually dropped;
        // this full revoke is therefore always required before retained
        // same-id service replacement. The source path may also have revoked
        // the set already; unregister is intentionally idempotent.
        if let Some(previous) = &previous {
            self.state
                .tool_event_router
                .unregister_sinks(&previous.registered.event_sink_ids)
                .await;
        }

        // Step 1b: Services (issue #479). Runs right after MCP, before the
        // never-refusing Prompts step, so a services conflict fails the
        // install as early as the other REFUSE-on-conflict kinds do. Note:
        // for a same-id UPGRADE, the OLD service (if any) was already stopped
        // after prepared-candidate ownership preflight and before bundle
        // activation — see `stop_services_for_upgrade`'s ordering contract.
        let service_ids = match self
            .register_services(manifest, plugin_dir, &mut rollback)
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                    .await;
                return Err(error);
            }
        };

        // Step 2: Prompts.
        let preset_ids = match self.register_prompts(manifest).await {
            Ok(ids) => {
                rollback.preset_ids_added = ids.clone();
                ids
            }
            Err(error) => {
                self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                    .await;
                return Err(error);
            }
        };

        // Step 3: Workflows remain inside the plugin bundle and are discovered
        // by the shared SkillStore as read-only legacy adapters.
        let workflow_filenames = match self.validate_workflows_in_place(manifest, plugin_dir).await
        {
            Ok(()) => Vec::new(),
            Err(error) => {
                self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                    .await;
                return Err(error);
            }
        };

        // Step 4: Skills — nothing to register, just record the
        // declared+validated dir names (preflight_install already confirmed
        // every declared dir exists and that no undeclared dir is present).
        let skill_dirs = manifest.provides.skills.clone();

        // Step 5: commit provenance — flip the journal row to `Installed` with
        // the ACTUAL registered set (renamed preset ids, the to_register mcp/
        // workflow subsets). Only reached once 0-4 all succeeded.
        let registered = RegisteredCapabilities {
            mcp_server_ids,
            skill_dirs,
            preset_ids,
            workflow_filenames,
            service_ids,
            event_sink_ids: event_sink_reconciliation.to_register,
        };
        let runtime_sink_plan = reconcile_event_sinks(
            manifest,
            &registered,
            PluginInstallStatus::Installed,
            Platform::current(),
        )?;
        let entry = InstalledPlugin {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            source,
            plugin_dir: plugin_dir.to_path_buf(),
            installed_at,
            status: PluginInstallStatus::Installed,
            registered,
        };
        let final_commit = if failure_injection.final_provenance_commit {
            Err(PluginError::Registration(
                "injected final Installed provenance commit failure".to_string(),
            ))
        } else {
            self.upsert_provenance(entry.clone(), &installed_json_path)
                .await
        };
        if let Err(error) = final_commit {
            self.abort_install(&rollback, &previous, &manifest.id, &installed_json_path)
                .await;
            return Err(error);
        }

        // Provenance is committed before runtime publication. The router
        // records eligible/inactive declarations now, but only creates a live
        // queue after ServiceManager exposes a Ready exact-generation sender.
        self.state
            .tool_event_router
            .apply_plugin_plan(&manifest.id, manifest, &runtime_sink_plan)
            .await;

        Ok(entry)
    }
}

#[async_trait]
impl PluginInstaller for ServerPluginInstaller {
    async fn install(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin> {
        let guard = self.begin_operation().await;
        self.install_with_operation(
            manifest,
            plugin_dir,
            source,
            disposition,
            installed_at,
            &guard,
        )
        .await
    }

    async fn uninstall(&self, id: &str) -> PluginResult<()> {
        // Serialize against every other plugin op (see module docs).
        let _op_guard = PLUGIN_OP_LOCK.lock().await;

        let installed_json_path = self.installed_json_path();
        let mut store = InstalledPlugins::load(&installed_json_path).await?;
        // Works on an `Installing` (crash-leftover) row too, so a crashed
        // install is never un-uninstallable.
        let Some(entry) = store.get_unique(id)?.cloned() else {
            return Err(PluginError::NotFound(id.to_string()));
        };

        // De-register everything this plugin's `registered` set names — by
        // construction (see bamboo-plugin's ownership contract) this can
        // only ever be entries the plugin itself created. Idempotent: a
        // manually-removed entry is logged and skipped, never a hard error.
        self.deregister_capabilities(&entry.registered).await;

        // Remove the plugin's own files BEFORE clearing provenance: if this
        // fails (e.g. a permission error), provenance is left intact so a
        // retry is safe (the de-registration above is idempotent, so
        // re-running it is a harmless no-op) rather than leaving an
        // unregistered-but-still-on-disk `skills/` dir that discovery would
        // keep picking up despite `uninstall` having "succeeded".
        match fs::remove_dir_all(&entry.plugin_dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(PluginError::Io(error)),
        }

        store.remove(id);
        store.save(&installed_json_path).await?;
        Ok(())
    }

    async fn list(&self) -> PluginResult<Vec<InstalledPlugin>> {
        let store = InstalledPlugins::load(&self.installed_json_path()).await?;
        Ok(store.plugins)
    }
}

// ---------------------------------------------------------------------
// Free helpers shared between `ServerPluginInstaller` (instance methods
// above, which delegate here) and `boot_reconcile_services` (which has no
// `ServerPluginInstaller`/`AppState` handle to call instance methods on —
// see `app_state::builder`, which calls it before `AppState` finishes
// constructing).
// ---------------------------------------------------------------------

/// See `ServerPluginInstaller::service_config_path`'s doc comment for the
/// full rationale (kept there since that's the reader's first encounter).
fn service_config_path_under(app_data_dir: &Path, plugin_id: &str) -> PathBuf {
    app_data_dir
        .join("plugin_service_config")
        .join(plugin_id)
        .join("config.json")
}

fn resolve_service_config_under(
    app_data_dir: &Path,
    plugin_id: &str,
    entry: &ServiceManifestEntry,
    plugin_dir: &Path,
    platform: Platform,
) -> ServiceRuntimeConfig {
    let resolved = entry.resolve(plugin_dir, plugin_id, platform);
    ServiceRuntimeConfig {
        id: resolved.id,
        plugin_id: plugin_id.to_string(),
        name: resolved.name,
        command: resolved.command,
        args: resolved.args,
        cwd: resolved.cwd,
        env: resolved.env,
        health_check: resolved.health_check,
        restart_policy: resolved.restart_policy,
        graceful_shutdown: resolved.graceful_shutdown,
        input_protocol: resolved.input_protocol,
        user_config_path: service_config_path_under(app_data_dir, plugin_id),
    }
}

/// Boot-time reconcile (issue #479): start every ENABLED, plugin-owned
/// service that `installed.json` says should be running but has no live
/// [`ServiceManager`] runtime — the previous `bamboo serve` process (if any)
/// died along with every service it supervised (child processes are spawned
/// `kill_on_drop`, and nothing about a running service persists
/// cross-process). Called from `app_state::builder` the same way
/// `app_state::init::init_mcp_manager` kicks off its background MCP
/// bootstrap — the caller is expected to `tokio::spawn` this, NOT await it
/// inline, so server startup is never blocked on plugin service spawns.
///
/// Deliberately reads `installed.json` + each plugin's on-disk
/// `plugin.json` directly rather than going through `ServerPluginInstaller`
/// (which needs a fully-built `web::Data<AppState>` this runs before).
pub async fn boot_reconcile_services(
    app_data_dir: &Path,
    service_manager: &ServiceManager,
    tool_event_router: &std::sync::Arc<ToolEventRouter>,
) {
    // Boot reads a provenance snapshot and then mutates both service and sink
    // generations from that plan. Serialize the whole pass with install,
    // update, and uninstall so an old boot plan can never unregister or
    // overwrite a route those operations just committed.
    let _op_guard = PLUGIN_OP_LOCK.lock().await;
    let installed_json_path = app_data_dir.join("plugins").join("installed.json");
    let store = match InstalledPlugins::load(&installed_json_path).await {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!(
                %error,
                "service boot-reconcile: failed to load installed.json; skipping"
            );
            return;
        }
    };

    let mut candidates = Vec::with_capacity(store.list().len());
    for plugin in store.list() {
        let manifest_path = plugin.plugin_dir.join("plugin.json");
        let manifest = fs::read_to_string(&manifest_path)
            .await
            .ok()
            .and_then(|raw| PluginManifest::parse_str(&raw).ok());
        candidates.push(PluginBootCandidate {
            installed: plugin.clone(),
            manifest,
        });
    }

    let platform = Platform::current();
    let plans = reconcile_plugin_boot(&candidates, platform);
    let Some(platform) = platform else {
        tracing::warn!(
            host_os = std::env::consts::OS,
            "service boot-reconcile: unknown host platform; all plugin services remain stopped"
        );
        return;
    };

    for (candidate, plan) in candidates.iter().zip(plans) {
        let plugin = &candidate.installed;
        tool_event_router
            .unregister_sinks(&plan.event_sinks.deactivate_before_services)
            .await;
        for issue in &plan.issues {
            tracing::warn!(
                plugin_id = %plugin.id,
                ?issue,
                "service boot-reconcile: provenance audit kept capabilities inactive"
            );
        }
        let Some(manifest) = candidate.manifest.as_ref() else {
            continue;
        };

        let to_start: HashSet<&str> = plan
            .service_ids_to_start
            .iter()
            .map(String::as_str)
            .collect();
        for entry in &manifest.provides.services {
            if !to_start.contains(entry.id.as_str()) {
                continue;
            }
            if service_manager.is_running(&entry.id) {
                continue;
            }
            let config = resolve_service_config_under(
                app_data_dir,
                &plugin.id,
                entry,
                &plugin.plugin_dir,
                platform,
            );
            if let Some(parent) = config.user_config_path.parent() {
                if let Err(error) = fs::create_dir_all(parent).await {
                    tracing::warn!(
                        service_id = %entry.id,
                        plugin_id = %plugin.id,
                        %error,
                        "service boot-reconcile: failed to create service config parent dir"
                    );
                }
            }
            match service_manager.start_service(config).await {
                Ok(()) => tracing::info!(
                    service_id = %entry.id,
                    plugin_id = %plugin.id,
                    "service boot-reconcile: started"
                ),
                Err(error) => tracing::warn!(
                    service_id = %entry.id,
                    plugin_id = %plugin.id,
                    %error,
                    "service boot-reconcile: failed to start"
                ),
            }
        }
        tool_event_router
            .apply_plugin_plan(&plugin.id, manifest, &plan.event_sinks)
            .await;
    }
}

#[cfg(test)]
mod tests;
