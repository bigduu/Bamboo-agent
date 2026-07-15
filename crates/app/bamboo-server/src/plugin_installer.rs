//! `ServerPluginInstaller` — the `AppState`-backed implementation of
//! `bamboo_plugin::PluginInstaller` (Wave 2 § Installer-core agent,
//! `PLUGIN_PLAN.md`).
//!
//! `bamboo-plugin` is an `infra`-layer crate with no access to `AppState`, so
//! its `LocalPluginInstaller` reference skeleton stops at
//! `PluginError::NotImplemented` exactly where capability registration needs
//! `config.json`, `mcp_manager`, `prompt-presets.json`, and
//! `workflows_dir()`. This type is the real implementation: an ordinary
//! downstream `impl PluginInstaller for ServerPluginInstaller` (the trait is
//! foreign, the type is local — no orphan-rule issue).
//!
//! # Why a borrowed `web::Data<AppState>` and no `AppState` struct change
//!
//! `ServerPluginInstaller` holds a `web::Data<AppState>` clone — the exact
//! handle every HTTP handler in this crate already receives as an argument
//! (`web::Data` is `Arc`-backed, so cloning it is cheap). An HTTP handler
//! constructs one per request: `ServerPluginInstaller::new(state.clone())`.
//! `AppState` itself is intentionally untouched — no new field, no
//! coordinated append to `app_state/mod.rs` / `app_state/builder.rs` — so
//! this branch can never conflict with the other Wave-2 branches that also
//! stack on `feat/plugin-framework`.
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
//! Narrower, accepted gap: [`ServerPluginInstaller::register_workflows`] does
//! NOT get the equivalent live re-check that MCP's step does — there is no
//! `workflows_io_lock` analogous to `config_io_lock` to re-run
//! `reconcile_exclusive` inside, because workflow files are plain files under
//! `workflows_dir()`, not a single locked/rewritten document like
//! `config.json`. So a concurrent NON-plugin write of a same-named workflow
//! file landing in the window between `existing_workflow_filenames()` and the
//! actual `fs::write` below could still be clobbered (and then recorded as
//! plugin-owned, which a later uninstall would incorrectly delete). Deferred:
//! this is the same class of race the MCP re-check closes, just for a store
//! that has no equivalent lock to hook into yet.
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
//! 3. **Workflows** — ownership-checked exactly like MCP, copied into
//!    `workflows_dir()`.
//! 4. **Skills** — nothing to register (discovered in place); just recorded.
//! 5. **Provenance commit** — `installed.json` is only ever upserted after
//!    steps 0-4 all succeed.
//!
//! Steps 1-3 are real, sequential mutations (config write, then file
//! writes) — NOT a dry-run computed up front — because `PLUGIN_PLAN.md`
//! requires the ownership pre-checks to run in that exact order against the
//! LIVE state each step leaves behind. That means a HARD failure at step 2
//! or 3 (e.g. an MCP conflict is already past, but the workflow conflict
//! check at step 3 fails) can happen after step 1 already wrote real
//! entries into `config.json`. [`ServerPluginInstaller::install`] tracks
//! every already-applied mutation in an [`InstallRollback`] and, on any hard
//! failure from steps 1-3, best-effort UNDOES them (removes the mcp entries
//! it just added and stops any it started, removes the presets it just
//! appended, deletes the workflow files it just copied) before returning the
//! error — so a caller's retry starts from a clean slate. Provenance is
//! never written on a failed path (step 5 is the only place `installed.json`
//! is touched on success), which is the minimum safety bar even if a rollback
//! step itself only partially succeeds (rollback operations are themselves
//! idempotent/log-and-continue, so a second rollback attempt via a plain
//! retry can never fail louder than the first).
//!
//! One known, accepted gap: `stage_plugin_source`/`install_plugin_from_source`
//! in [`crate::plugin_source`] additionally guard the ON-DISK `plugin_dir`
//! swap itself (an upgrade's new bundle replaces the old one's files at a
//! fixed path) by moving the previous bundle aside instead of deleting it, and
//! restoring it if `install()` subsequently fails — see that module's docs.
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
use bamboo_plugin::registry::{reconcile_exclusive, RegisteredCapabilities};
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
    mcp_ids_started: Vec<String>,
    preset_ids_added: Vec<String>,
    workflow_files_added: Vec<String>,
    /// Service ids this install claimed ownership of (whether or not the
    /// actual `start_service` call succeeded — best-effort, same contract as
    /// `mcp_ids_added`/`mcp_ids_started`).
    service_ids_added: Vec<String>,
    /// Subset of `service_ids_added` that actually got a running
    /// `ServiceManager` runtime started — only these need `stop_service` on
    /// rollback.
    service_ids_started: Vec<String>,
}

impl ServerPluginInstaller {
    pub fn new(state: actix_web::web::Data<AppState>) -> Self {
        Self { state }
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
    /// swap-managed `plugin_dir`): `plugin_source::stage_plugin_source`
    /// upgrades a plugin by renaming the ENTIRE old `plugin_dir` aside and
    /// swapping a freshly-staged directory into its place — any file living
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
    /// `previously_owned` (computed separately, from `previous` BEFORE that
    /// journal write) is what still correctly classifies an upgrade's
    /// re-declared ids as `OwnedReinstall` rather than `New`.
    async fn existing_service_ids(&self, exclude_plugin_id: &str) -> PluginResult<Vec<String>> {
        let store = InstalledPlugins::load(&self.installed_json_path()).await?;
        Ok(store
            .list()
            .iter()
            .filter(|plugin| plugin.id != exclude_plugin_id)
            .flat_map(|plugin| plugin.registered.service_ids.iter().cloned())
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

    /// Every `.md` filename directly under `workflows_dir()` (created if
    /// missing). Mirrors `handlers::settings::workflows::list_workflows`'s
    /// listing logic but returns bare filenames for
    /// [`bamboo_plugin::registry::reconcile_exclusive`].
    async fn existing_workflow_filenames(&self) -> PluginResult<Vec<String>> {
        let dir = self.workflows_dir();
        fs::create_dir_all(&dir).await?;
        let mut entries = fs::read_dir(&dir).await?;
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let is_file = entry
                .file_type()
                .await
                .map(|file_type| file_type.is_file())
                .unwrap_or(false);
            if !is_file {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".md") {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
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
                move |cfg| {
                    cfg.mcp.servers.retain(|server| server.id != owned_id);
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await;
        if let Err(error) = result {
            tracing::warn!(
                mcp_server_id = %id,
                %error,
                "failed to remove plugin-owned mcp server from config.json; continuing"
            );
        }
        if let Err(error) = self.state.mcp_manager.stop_server(id).await {
            tracing::warn!(
                mcp_server_id = %id,
                %error,
                "failed to stop plugin-owned mcp server; continuing"
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

    /// Best-effort undo of an `install()` that failed partway through steps
    /// 1-3. See the module docs.
    async fn rollback_partial_install(&self, rollback: &InstallRollback) {
        for id in &rollback.mcp_ids_started {
            let _ = self.state.mcp_manager.stop_server(id).await;
        }
        for id in &rollback.mcp_ids_added {
            self.remove_mcp_server(id).await;
        }
        for id in &rollback.preset_ids_added {
            self.remove_prompt_preset(id).await;
        }
        for file in &rollback.workflow_files_added {
            self.remove_workflow_file(file).await;
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
        self.state
            .update_config(
                move |cfg| {
                    // TOCTOU guard: re-run the ownership pre-check against the
                    // LIVE config while holding config_io_lock, so a foreign
                    // entry that landed between our earlier read and now can't
                    // be silently clobbered (and then recorded as
                    // plugin-owned, re-opening BLOCKER-1 under a race).
                    // Concurrent PLUGIN ops are already excluded by
                    // PLUGIN_OP_LOCK; this closes the residual window against a
                    // concurrent NON-plugin config write.
                    let live_existing: Vec<String> =
                        cfg.mcp.servers.iter().map(|s| s.id.clone()).collect();
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
                        upsert_server_by_id(&mut cfg.mcp.servers, server.clone());
                    }
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .map_err(|error| {
                PluginError::Registration(format!("failed to write mcp servers to config: {error}"))
            })?;
        // Config write for the whole batch succeeded — record ownership now,
        // regardless of whether individual `start_server` calls below
        // succeed (matches `import_servers`' best-effort start semantics: a
        // config entry that fails to start is still a real, plugin-owned
        // registration a user/CLI can retry starting later).
        rollback.mcp_ids_added = reconciliation.to_register.clone();

        for server in &configs_to_register {
            // Stop any stale running instance first, matching the
            // update/import handlers' pattern.
            let _ = self.state.mcp_manager.stop_server(&server.id).await;
            if server.enabled {
                match self.state.mcp_manager.start_server(server.clone()).await {
                    Ok(()) => rollback.mcp_ids_started.push(server.id.clone()),
                    Err(error) => tracing::warn!(
                        mcp_server_id = %server.id,
                        %error,
                        "plugin-registered mcp server failed to start; config entry kept (best-effort)"
                    ),
                }
            }
        }

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
        previously_owned: &[String],
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
        let reconciliation = reconcile_exclusive(&declared_ids, &existing_ids, previously_owned);
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
            // upgrade already had its old service stopped BEFORE staging by
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

    /// Step 3: Workflows. Same REFUSE-on-conflict shape as MCP.
    ///
    /// Takes `rollback` directly (unlike [`Self::register_prompts`], which
    /// commits in one atomic file write) because each workflow file is
    /// copied with a SEPARATE `fs::write` call: if copying the Nth file
    /// fails, files 1..N-1 are already really on disk, and `rollback` must
    /// know about them even though this function returns `Err` — recording
    /// the whole `to_register` list only on a successful `Ok` return (the
    /// pattern the caller uses for [`Self::register_mcp`]/
    /// [`Self::register_prompts`]) would lose that partial progress.
    async fn register_workflows(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        previously_owned: &[String],
        rollback: &mut InstallRollback,
    ) -> PluginResult<Vec<String>> {
        if manifest.provides.workflows.is_empty() {
            return Ok(Vec::new());
        }

        let declared: Vec<String> = manifest.provides.workflows.clone();
        let existing = self.existing_workflow_filenames().await?;
        let reconciliation = reconcile_exclusive(&declared, &existing, previously_owned);
        if !reconciliation.foreign_conflicts.is_empty() {
            return Err(PluginError::Conflict {
                kind: "workflow",
                name: reconciliation.foreign_conflicts.join(", "),
                plugin_id: manifest.id.clone(),
            });
        }

        let dest_dir = self.workflows_dir();
        for filename in &reconciliation.to_register {
            let stem = filename.strip_suffix(".md").unwrap_or(filename);
            if !bamboo_config::paths::is_safe_workflow_name(stem) {
                return Err(PluginError::InvalidManifest(format!(
                    "workflow filename '{filename}' is not a safe workflow name"
                )));
            }
            let source_path = plugin_dir.join("workflows").join(filename);
            let content = fs::read_to_string(&source_path).await?;
            fs::write(dest_dir.join(filename), content).await?;
            // Recorded immediately, not after the whole loop: if a LATER
            // file in this same call fails, this one is already really on
            // disk and rollback must know to remove it.
            rollback.workflow_files_added.push(filename.clone());
        }

        Ok(reconciliation.to_register)
    }

    /// **Same-id upgrade ordering** (issue #479 "Install-flow deltas" /
    /// "Same-id upgrade ordering bug risk"): `plugin_source::stage_plugin_source`
    /// swaps `plugin_dir`'s ENTIRE contents (old bundle moved to a
    /// `.backup-*` dir, staged bundle renamed into place) BEFORE
    /// `install()` — and therefore [`Self::register_services`] — ever runs.
    /// A still-running old service process holding the old binary open
    /// during that swap is at best running stale code post-swap and at
    /// worst (Windows) blocks the rename outright. The minimal seam that
    /// fixes the ordering without restructuring `stage_plugin_source`/
    /// `install()`: the HTTP `update_plugin` handler calls this BEFORE
    /// `stage_plugin_source`, using the URL path's target id (known up
    /// front for an upgrade, unlike a fresh `install`) to look up the
    /// CURRENTLY-installed row's `registered.service_ids` and stop each one.
    /// Net effect: stop (old binary) → swap (new binary) → start (new
    /// binary, via `register_services` inside `install()`), exactly the
    /// sequencing the issue calls for.
    ///
    /// Best-effort: returns exactly the ids that were actually running and
    /// got stopped (not e.g. an already-stopped or unknown id), so a
    /// subsequently-failed upgrade can restart precisely those — see
    /// [`Self::restart_services_after_failed_upgrade`]. A plugin with no
    /// prior install (or no services) returns an empty vec and stops
    /// nothing.
    pub async fn stop_services_for_upgrade(&self, plugin_id: &str) -> Vec<String> {
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
        let Some(entry) = store.get(plugin_id) else {
            return Vec::new();
        };
        let mut stopped = Vec::with_capacity(entry.registered.service_ids.len());
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

    /// Counterpart to [`Self::stop_services_for_upgrade`]: called after a
    /// FAILED upgrade whose `StagedPlugin::rollback()` already restored
    /// `plugin_dir` to the pre-upgrade bundle's bytes — re-reads that
    /// (now-restored) OLD `plugin.json` and restarts exactly the services in
    /// `stopped` that it still declares as `enabled`. Best-effort/
    /// log-and-continue: a failure here leaves the affected service stopped
    /// (a degraded-but-safe outcome — never silently double-runs an old and
    /// a new instance) rather than panicking the request.
    pub async fn restart_services_after_failed_upgrade(&self, plugin_id: &str, stopped: &[String]) {
        if stopped.is_empty() {
            return;
        }
        let store = match InstalledPlugins::load(&self.installed_json_path()).await {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(
                    %plugin_id,
                    %error,
                    "restart_services_after_failed_upgrade: failed to load installed.json"
                );
                return;
            }
        };
        let Some(entry) = store.get(plugin_id) else {
            return;
        };
        let manifest_path = entry.plugin_dir.join("plugin.json");
        let manifest = match fs::read_to_string(&manifest_path)
            .await
            .ok()
            .and_then(|raw| PluginManifest::parse_str(&raw).ok())
        {
            Some(manifest) => manifest,
            None => {
                tracing::warn!(
                    %plugin_id,
                    path = %manifest_path.display(),
                    "restart_services_after_failed_upgrade: failed to read/parse the \
                     rolled-back plugin.json; affected service(s) remain stopped"
                );
                return;
            }
        };
        let platform = Platform::current().unwrap_or(Platform::Linux);
        for svc in &manifest.provides.services {
            if !stopped.contains(&svc.id) || !svc.enabled {
                continue;
            }
            let config = self.resolve_service_config(plugin_id, svc, &entry.plugin_dir, platform);
            if let Err(error) = self.state.service_manager.start_service(config).await {
                tracing::warn!(
                    service_id = %svc.id,
                    %plugin_id,
                    %error,
                    "failed to restart service after a failed upgrade rolled back to the \
                     previous plugin bundle; service remains stopped"
                );
            }
        }
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
        // Serialize the whole op against every other plugin install/uninstall
        // (process-wide) — held across all steps AND rollback. See module docs
        // "Concurrency".
        let _op_guard = PLUGIN_OP_LOCK.lock().await;

        let installed_json_path = self.installed_json_path();

        // Disposition gate (AlreadyInstalled only for a COMPLETED prior
        // install; an `Installing` leftover is returned for recovery) + the
        // rest of the pure, AppState-free validation this crate can already
        // do (manifest shape, platform gate, on-disk skill/workflow
        // existence, `provides.skills` authoritativeness).
        let previous =
            load_previous_for_disposition(&installed_json_path, &manifest.id, disposition).await?;
        let resolved_mcp_servers = preflight_install(manifest, plugin_dir).await?;

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
            workflow_filenames: manifest.provides.workflows.clone(),
            service_ids: manifest
                .provides
                .services
                .iter()
                .map(|entry| entry.id.clone())
                .collect(),
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
                    "install drop-diff: de-registering capabilities the new/completed version no longer declares"
                );
                self.deregister_capabilities(&dropped).await;
            }
        }

        let previously_owned_mcp = previous
            .as_ref()
            .map(|p| p.registered.mcp_server_ids.clone())
            .unwrap_or_default();
        let previously_owned_workflows = previous
            .as_ref()
            .map(|p| p.registered.workflow_filenames.clone())
            .unwrap_or_default();
        let previously_owned_services = previous
            .as_ref()
            .map(|p| p.registered.service_ids.clone())
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

        // Step 1b: Services (issue #479). Runs right after MCP, before the
        // never-refusing Prompts step, so a services conflict fails the
        // install as early as the other REFUSE-on-conflict kinds do. Note:
        // for a same-id UPGRADE, the OLD service (if any) was already
        // stopped by `stop_services_for_upgrade` before `stage_plugin_source`
        // swapped `plugin_dir` — see that method's doc comment on the
        // stop→swap→start sequencing.
        let service_ids = match self
            .register_services(
                manifest,
                plugin_dir,
                &previously_owned_services,
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

        // Step 3: Workflows. `register_workflows` records each copied file
        // into `rollback` itself as it goes (see its doc comment) — a
        // partial failure partway through a multi-file copy is still fully
        // rolled back.
        let workflow_filenames = match self
            .register_workflows(
                manifest,
                plugin_dir,
                &previously_owned_workflows,
                &mut rollback,
            )
            .await
        {
            Ok(files) => files,
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
        };
        let entry = InstalledPlugin {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            source,
            plugin_dir: plugin_dir.to_path_buf(),
            installed_at,
            status: PluginInstallStatus::Installed,
            registered,
        };
        self.upsert_provenance(entry.clone(), &installed_json_path)
            .await?;

        Ok(entry)
    }

    async fn uninstall(&self, id: &str) -> PluginResult<()> {
        // Serialize against every other plugin op (see module docs).
        let _op_guard = PLUGIN_OP_LOCK.lock().await;

        let installed_json_path = self.installed_json_path();
        let mut store = InstalledPlugins::load(&installed_json_path).await?;
        // Works on an `Installing` (crash-leftover) row too, so a crashed
        // install is never un-uninstallable.
        let Some(entry) = store.get(id).cloned() else {
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
pub async fn boot_reconcile_services(app_data_dir: &Path, service_manager: &ServiceManager) {
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

    let platform = Platform::current().unwrap_or(Platform::Linux);
    for plugin in store.list() {
        if plugin.registered.service_ids.is_empty() {
            continue;
        }
        let manifest_path = plugin.plugin_dir.join("plugin.json");
        let manifest = match fs::read_to_string(&manifest_path)
            .await
            .ok()
            .and_then(|raw| PluginManifest::parse_str(&raw).ok())
        {
            Some(manifest) => manifest,
            None => {
                tracing::warn!(
                    plugin_id = %plugin.id,
                    path = %manifest_path.display(),
                    "service boot-reconcile: failed to read/parse plugin.json; skipping this \
                     plugin's services"
                );
                continue;
            }
        };

        let owned: HashSet<&str> = plugin
            .registered
            .service_ids
            .iter()
            .map(String::as_str)
            .collect();
        for entry in &manifest.provides.services {
            if !entry.enabled || !owned.contains(entry.id.as_str()) {
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
    }
}

#[cfg(test)]
mod tests;
