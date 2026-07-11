//! The `PluginInstaller` trait — the method surface later agents implement.
//!
//! This crate lives at the `infra` layer and has no access to `AppState`
//! (`bamboo-server`, an `app`-layer crate). Actually registering capabilities
//! — merging into `config.json`, calling `mcp_manager.start_server`, appending
//! to `prompt-presets.json`, writing workflow files — all need `AppState` (or
//! equivalent handles), so a REAL implementation of this trait has to live in
//! `bamboo-server` (or a sibling crate that depends on it), implemented by a
//! later agent (see `PLUGIN_PLAN.md` § Installer-core agent). Because the
//! trait is foreign there and the implementing type is local, that's a
//! perfectly ordinary downstream `impl` — no orphan-rule issue.
//!
//! [`LocalPluginInstaller`] below is a reference skeleton that implements
//! everything this crate CAN implement without `AppState` (manifest
//! validation, platform gating, MCP-entry token resolution, the
//! disposition/upgrade decision, the `provides.skills`-authoritative check,
//! provenance listing) and returns [`PluginError::NotImplemented`] at the
//! exact points that need capability-registration wiring, with a comment
//! enumerating what goes there and citing the exact files. It is a
//! reference/example, not a requirement to reuse verbatim — Wave-2's
//! installer-core agent may replace it entirely with a type that holds an
//! `AppState` handle.
//!
//! # Ownership + upgrade contract (why uninstall is provably safe)
//!
//! Two invariants make uninstall/upgrade never touch a user's own entries:
//!
//! 1. **Only plugin-created entries are ever recorded as removable.** For the
//!    REFUSE-on-conflict capabilities (MCP servers, workflow files) the
//!    installer MUST run [`crate::registry::reconcile_exclusive`] against the
//!    live shared store before touching anything: a declared id/filename that
//!    already exists and is not owned by this plugin lands in
//!    `foreign_conflicts` and the install is REFUSED
//!    ([`PluginError::Conflict`]) — it is never registered and never written
//!    into [`crate::registry::RegisteredCapabilities`]. So the `registered`
//!    set an [`InstalledPlugin`] carries contains ONLY entries this plugin
//!    genuinely created; `uninstall` iterating that set can only ever delete
//!    the plugin's own entries. (Prompt presets are the one exception: they
//!    rename on collision via bamboo-server's `ensure_unique_preset_id`
//!    instead of refusing, and the RENAMED id is what gets recorded.)
//! 2. **Upgrade de-registers what the new version dropped.** `install` with
//!    [`InstallDisposition::Upgrade`] loads the prior [`InstalledPlugin`],
//!    computes [`crate::registry::RegisteredCapabilities::removed_since`] (old
//!    minus new), de-registers those dropped capabilities, THEN registers the
//!    new set and upserts provenance — so a capability the old version had and
//!    the new one dropped can't leak as an orphan.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::{PluginError, PluginResult};
use crate::manifest::{Platform, PluginManifest};
use crate::registry::{InstalledPlugin, InstalledPlugins, PluginSource};

/// How [`PluginInstaller::install`] must treat a plugin id that is ALREADY
/// installed. Maps directly to the CLI verbs: `bamboo plugin install` uses
/// [`Self::FailIfInstalled`], `bamboo plugin update` (or `install --force`)
/// uses [`Self::Upgrade`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallDisposition {
    /// Refuse a re-install of an already-installed id with
    /// [`PluginError::AlreadyInstalled`]. A first-time `install` should not
    /// silently replace an existing plugin.
    FailIfInstalled,
    /// Upgrade in place: de-register the capabilities the new version dropped
    /// (via [`crate::registry::RegisteredCapabilities::removed_since`]),
    /// register the new set, then upsert provenance.
    Upgrade,
}

/// The installer method surface. `install`/`uninstall`/`list` are the verbs
/// the CLI (`bamboo plugin install/list/remove/update`) and the HTTP routes
/// (`/api/v1/plugins`) both call through.
#[async_trait]
pub trait PluginInstaller {
    /// Install (or, with [`InstallDisposition::Upgrade`], upgrade) a plugin
    /// already unpacked at `plugin_dir` (source handling — copying a local
    /// dir, unpacking a `.tar.gz`, fetching+verifying a URL + selecting the
    /// per-platform artifact — happens BEFORE this is called; by the time
    /// `install` runs, `plugin_dir` already contains `plugin.json` plus the
    /// `skills/`/`prompts/`/`workflows/`/`bin/` layout the manifest declares).
    ///
    /// Contract (see the module docs for the full rationale):
    /// - `disposition` decides already-installed handling (fail vs upgrade).
    /// - MCP-server and workflow collisions with NON-plugin entries MUST be
    ///   refused ([`PluginError::Conflict`]) via
    ///   [`crate::registry::reconcile_exclusive`] — never clobbered.
    /// - On upgrade, capabilities the new version dropped MUST be
    ///   de-registered ([`crate::registry::RegisteredCapabilities::removed_since`]).
    /// - The returned [`InstalledPlugin`] must have already been persisted
    ///   into `installed.json`, and its `registered` set must contain ONLY
    ///   entries this plugin genuinely created (so uninstall is safe).
    async fn install(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        disposition: InstallDisposition,
        installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin>;

    /// Reverse everything `install` registered for `id` (stop + remove MCP
    /// servers, remove prompt presets, remove workflow files), then remove
    /// the provenance entry and delete `plugin_dir` from disk. Safe by
    /// construction: the provenance `registered` set only ever names
    /// plugin-created entries (invariant 1 in the module docs).
    async fn uninstall(&self, id: &str) -> PluginResult<()>;

    /// All currently-installed plugins (a thin read of `installed.json`).
    async fn list(&self) -> PluginResult<Vec<InstalledPlugin>>;
}

/// Directory names directly under `<plugin_dir>/skills/` that contain a
/// `SKILL.md` (i.e. what discovery would actually pick up in place). Used
/// by [`preflight_install`] to enforce that `provides.skills` is
/// authoritative (MAJOR 4).
pub async fn on_disk_skill_dirs(plugin_dir: &Path) -> Vec<String> {
    let skills_root = plugin_dir.join("skills");
    let mut found = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(&skills_root).await else {
        return found;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let is_dir = entry
            .file_type()
            .await
            .map(|file_type| file_type.is_dir())
            .unwrap_or(false);
        if !is_dir {
            continue;
        }
        let has_skill_md = tokio::fs::try_exists(entry.path().join("SKILL.md"))
            .await
            .unwrap_or(false);
        if !has_skill_md {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            found.push(name.to_string());
        }
    }
    found
}

/// Everything an `install` can validate/resolve WITHOUT touching `AppState`:
/// manifest validation, platform gating, per-entry MCP resolution (so a
/// malformed entry — e.g. an empty stdio command — fails fast before any
/// registration happens), and the `provides.skills` / `provides.workflows`
/// on-disk existence + authoritative checks (MAJOR 4).
///
/// Shared by [`LocalPluginInstaller`] and any real `AppState`-backed
/// installer (see `PLUGIN_PLAN.md` § Installer-core agent) so the two can
/// never drift apart. Returns the resolved MCP server configs (the caller
/// needs them anyway to register step 1) so a real installer doesn't have to
/// re-resolve them a second time.
pub async fn preflight_install(
    manifest: &PluginManifest,
    plugin_dir: &Path,
) -> PluginResult<Vec<bamboo_domain::mcp_config::McpServerConfig>> {
    manifest.validate()?;

    let current_platform = Platform::current();
    if let Some(platforms) = &manifest.platforms {
        // An unrecognized host OS (`current_platform == None`) fails closed
        // rather than guessing.
        let supported = current_platform.is_some_and(|platform| platforms.contains(&platform));
        if !supported {
            return Err(PluginError::UnsupportedPlatform {
                plugin_id: manifest.id.clone(),
                platform: current_platform
                    .map(|platform| platform.as_str().to_string())
                    .unwrap_or_else(|| std::env::consts::OS.to_string()),
            });
        }
    }

    // Resolve what each declared MCP server WOULD look like once registered.
    // Pure — fails early on an unresolvable entry (e.g. empty stdio command)
    // and hands the caller the resolved configs to register in step 1.
    let platform = current_platform.unwrap_or(Platform::Linux);
    let resolved_mcp_servers = manifest
        .provides
        .mcp_servers
        .iter()
        .map(|entry| entry.resolve(plugin_dir, &manifest.id, platform))
        .collect::<PluginResult<Vec<_>>>()?;

    // Sanity-check declared skill dirs exist on disk. Skills need no further
    // action here — they're discovered in place by bamboo-skills' plugin
    // discovery-dir extension, not copied.
    for skill_dir in &manifest.provides.skills {
        let skill_md = plugin_dir.join("skills").join(skill_dir).join("SKILL.md");
        if !tokio::fs::try_exists(&skill_md).await.unwrap_or(false) {
            return Err(PluginError::InvalidManifest(format!(
                "declared skill '{skill_dir}' has no SKILL.md at {}",
                skill_md.display()
            )));
        }
    }
    // `provides.skills` is AUTHORITATIVE: reject any on-disk skill dir the
    // manifest does not declare. Discovery is a dumb globber that picks up
    // every `<plugin_dir>/skills/*` with a SKILL.md, so without this a
    // bundle could smuggle an undeclared skill live past its own manifest.
    {
        use std::collections::HashSet;
        let declared: HashSet<&str> = manifest
            .provides
            .skills
            .iter()
            .map(String::as_str)
            .collect();
        for on_disk in on_disk_skill_dirs(plugin_dir).await {
            if !declared.contains(on_disk.as_str()) {
                return Err(PluginError::InvalidManifest(format!(
                    "skill directory '{on_disk}' exists under skills/ but is not declared in \
                     provides.skills (a plugin must declare every skill it ships)"
                )));
            }
        }
    }
    for workflow_file in &manifest.provides.workflows {
        let workflow_path = plugin_dir.join("workflows").join(workflow_file);
        if !tokio::fs::try_exists(&workflow_path).await.unwrap_or(false) {
            return Err(PluginError::InvalidManifest(format!(
                "declared workflow '{workflow_file}' not found at {}",
                workflow_path.display()
            )));
        }
    }

    Ok(resolved_mcp_servers)
}

/// Loads prior provenance for `plugin_id` from `installed_json_path` and
/// applies the [`InstallDisposition`] gate: [`InstallDisposition::FailIfInstalled`]
/// errors [`PluginError::AlreadyInstalled`] when an entry already exists;
/// [`InstallDisposition::Upgrade`] passes through either way. Returns the
/// previous entry (`None` for a fresh install), which the caller needs for
/// the upgrade drop-diff (BLOCKER 2).
pub async fn load_previous_for_disposition(
    installed_json_path: &Path,
    plugin_id: &str,
    disposition: InstallDisposition,
) -> PluginResult<Option<InstalledPlugin>> {
    let existing = InstalledPlugins::load(installed_json_path).await?;
    let previous = existing.get(plugin_id).cloned();
    if previous.is_some() && disposition == InstallDisposition::FailIfInstalled {
        return Err(PluginError::AlreadyInstalled(plugin_id.to_string()));
    }
    Ok(previous)
}

/// Reference skeleton — see module docs. Holds only a `bamboo_dir` so tests
/// can point it at a tempdir instead of the real `~/.bamboo`.
pub struct LocalPluginInstaller {
    bamboo_dir: PathBuf,
}

impl LocalPluginInstaller {
    pub fn new(bamboo_dir: PathBuf) -> Self {
        Self { bamboo_dir }
    }

    fn plugins_dir(&self) -> PathBuf {
        self.bamboo_dir.join("plugins")
    }

    fn installed_json_path(&self) -> PathBuf {
        self.plugins_dir().join("installed.json")
    }
}

impl Default for LocalPluginInstaller {
    fn default() -> Self {
        Self::new(bamboo_config::paths::bamboo_dir())
    }
}

#[async_trait]
impl PluginInstaller for LocalPluginInstaller {
    async fn install(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        _source: PluginSource,
        disposition: InstallDisposition,
        _installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin> {
        // --- Disposition / upgrade decision (fully implemented here) ---
        //
        // Load prior provenance FIRST so we can (a) reject a first-time
        // install of an already-installed id, and (b) on upgrade, capture the
        // old registered set for the drop-diff below.
        let previous =
            load_previous_for_disposition(&self.installed_json_path(), &manifest.id, disposition)
                .await?;
        // On upgrade this is the set the installer-core agent must
        // `removed_since`-diff against the new registered set and de-register.
        let _previous_registered = previous.as_ref().map(|plugin| plugin.registered.clone());

        // --- Validation + pure path resolution (fully implemented here,
        // shared with any real AppState-backed installer via
        // `preflight_install`) ---
        let _resolved_mcp_servers = preflight_install(manifest, plugin_dir).await?;

        // --- Capability-registration wiring (TODO: installer-core agent) ---
        //
        // None of this can be implemented in this infra-layer crate — it all
        // needs `AppState` (bamboo-server, an app-layer crate this crate must
        // not depend on). See PLUGIN_PLAN.md § Installer-core agent for the
        // full breakdown. In order:
        //
        //   0. UPGRADE DROP-DIFF (only when `previous` is Some): compute
        //      `new_registered.removed_since(&_previous_registered.unwrap())`
        //      and DE-register those dropped mcp ids / preset ids / workflow
        //      files (same removal ops as `uninstall`) BEFORE registering the
        //      new set, so a capability the old version had and the new one
        //      dropped never leaks (BLOCKER 2).
        //   1. MCP: run `registry::reconcile_exclusive(declared_mcp_ids,
        //      existing_mcp_ids_in_config, previously_owned_mcp_ids)`. If
        //      `foreign_conflicts` is non-empty → return `PluginError::Conflict`
        //      (BLOCKER 1 — do NOT clobber). Otherwise merge only `to_register`
        //      into `Config.mcp.servers` via `AppState::update_config`
        //      (crates/app/bamboo-server/src/app_state/config_runtime.rs),
        //      reusing the merge-by-id logic in
        //      crates/app/bamboo-server/src/handlers/agent/mcp/server_handlers/import.rs
        //      (`import_servers`), then `state.mcp_manager.start_server(..)`
        //      for each enabled one. Record exactly `to_register` as owned.
        //   2. Prompts: append `manifest.provides.prompts` into
        //      `prompt-presets.json`
        //      (crates/app/bamboo-server/src/handlers/agent/prompt_presets/storage.rs),
        //      reusing `validate_preset_id` / `ensure_unique_preset_id` — on an
        //      id collision RENAME (don't refuse), and record the RENAMED id as
        //      owned (not the manifest's nominal one).
        //   3. Workflows: run `reconcile_exclusive` on the workflow filenames
        //      the same way as MCP (refuse foreign collisions), then copy
        //      `<plugin_dir>/workflows/<name>.md` into
        //      `bamboo_config::paths::workflows_dir()/<name>.md` (validate each
        //      name with `bamboo_config::paths::is_safe_workflow_name`).
        //   4. Skills: nothing to register (discovered in place). Record the
        //      declared+validated dir names as owned.
        //   5. Only once 0-4 succeed: build the `RegisteredCapabilities`
        //      reflecting exactly what got registered (renamed preset ids, the
        //      `to_register` mcp/workflow subsets — NOT a blind copy of
        //      `manifest.provides`), then upsert provenance via
        //      `InstalledPlugins::load` + `.add(..)` + `.save(..)` at
        //      `self.installed_json_path()`.
        let _ = self.installed_json_path(); // reserved for step 5 above.
        Err(PluginError::NotImplemented(
            "capability registration wiring — see PLUGIN_PLAN.md \
             \u{a7} Installer-core agent"
                .to_string(),
        ))
    }

    async fn uninstall(&self, id: &str) -> PluginResult<()> {
        let installed = InstalledPlugins::load(&self.installed_json_path()).await?;
        if installed.get(id).is_none() {
            return Err(PluginError::NotFound(id.to_string()));
        }

        // TODO(installer-core agent): using the found entry's `registered`
        // capabilities (which, by construction, name ONLY plugin-created
        // entries — see module docs), stop + remove each `mcp_server_ids`
        // entry from `config.json` (`AppState::update_config` +
        // `mcp_manager.stop_server`), remove each `preset_ids` entry from
        // `prompt-presets.json`, delete each `workflow_filenames` file from
        // `bamboo_config::paths::workflows_dir()`. THEN remove the provenance
        // entry (`InstalledPlugins::remove` + `.save(..)`) and finally
        // `tokio::fs::remove_dir_all(entry.plugin_dir)`.
        Err(PluginError::NotImplemented(
            "capability de-registration wiring — see PLUGIN_PLAN.md \
             \u{a7} Installer-core agent"
                .to_string(),
        ))
    }

    async fn list(&self) -> PluginResult<Vec<InstalledPlugin>> {
        let installed = InstalledPlugins::load(&self.installed_json_path()).await?;
        Ok(installed.plugins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::PluginManifest;
    use crate::registry::RegisteredCapabilities;

    fn manifest_with(skills: Vec<&str>, workflows: Vec<&str>) -> PluginManifest {
        let json = serde_json::json!({
            "id": "hello-plugin",
            "name": "Hello Plugin",
            "version": "0.1.0",
            "provides": {
                "skills": skills,
                "workflows": workflows,
            }
        });
        PluginManifest::parse_str(&json.to_string()).expect("parse manifest")
    }

    /// Create `<plugin_dir>/skills/<id>/SKILL.md` for each id.
    async fn write_skill_dirs(plugin_dir: &Path, ids: &[&str]) {
        for id in ids {
            let skill_dir = plugin_dir.join("skills").join(id);
            tokio::fs::create_dir_all(&skill_dir).await.unwrap();
            tokio::fs::write(
                skill_dir.join("SKILL.md"),
                format!("---\nname: {id}\ndescription: demo\n---\nHi\n"),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn list_on_fresh_bamboo_dir_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().to_path_buf());
        let plugins = installer.list().await.expect("list");
        assert!(plugins.is_empty());
    }

    #[tokio::test]
    async fn uninstall_unknown_plugin_is_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().to_path_buf());
        let error = installer
            .uninstall("does-not-exist")
            .await
            .expect_err("should be not-found");
        assert!(matches!(error, PluginError::NotFound(_)));
    }

    #[tokio::test]
    async fn install_validates_declared_skill_exists_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().join("bamboo-home"));

        let plugin_dir = dir.path().join("plugin-src");
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        // Note: no `skills/hello-world/SKILL.md` created — install should
        // reject the manifest for a missing declared skill before it ever
        // reaches the (currently NotImplemented) registration step.
        let manifest = manifest_with(vec!["hello-world"], vec![]);

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
            .expect_err("missing SKILL.md should fail validation");
        assert!(matches!(error, PluginError::InvalidManifest(_)));
    }

    #[tokio::test]
    async fn install_rejects_undeclared_on_disk_skill_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().join("bamboo-home"));

        let plugin_dir = dir.path().join("plugin-src");
        // Bundle ships TWO skills on disk but declares only one.
        write_skill_dirs(&plugin_dir, &["hello-world", "sneaky-extra"]).await;
        let manifest = manifest_with(vec!["hello-world"], vec![]);

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
            .expect_err("undeclared skill dir should be rejected");
        assert!(matches!(error, PluginError::InvalidManifest(_)));
        assert!(error.to_string().contains("sneaky-extra"));
    }

    #[tokio::test]
    async fn install_reaches_not_implemented_once_declared_files_exist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().join("bamboo-home"));

        let plugin_dir = dir.path().join("plugin-src");
        write_skill_dirs(&plugin_dir, &["hello-world"]).await;
        let manifest = manifest_with(vec!["hello-world"], vec![]);

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
            .expect_err("registration wiring is a later-agent TODO");
        assert!(matches!(error, PluginError::NotImplemented(_)));

        // And it must not have been committed to provenance either, since
        // registration never completed.
        let plugins = installer.list().await.expect("list");
        assert!(plugins.is_empty());
    }

    #[tokio::test]
    async fn install_fails_if_already_installed_under_fail_disposition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bamboo_home = dir.path().join("bamboo-home");
        let installer = LocalPluginInstaller::new(bamboo_home.clone());

        // Seed provenance with an existing install of the same id.
        let mut store = InstalledPlugins::default();
        store.add(InstalledPlugin {
            id: "hello-plugin".to_string(),
            version: "0.0.1".to_string(),
            source: PluginSource::LocalDir {
                path: dir.path().to_path_buf(),
            },
            plugin_dir: bamboo_home.join("plugins").join("hello-plugin"),
            installed_at: Utc::now(),
            registered: RegisteredCapabilities::default(),
        });
        store
            .save(&bamboo_home.join("plugins").join("installed.json"))
            .await
            .unwrap();

        let plugin_dir = dir.path().join("plugin-src");
        write_skill_dirs(&plugin_dir, &["hello-world"]).await;
        let manifest = manifest_with(vec!["hello-world"], vec![]);

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
            .expect_err("already-installed under FailIfInstalled should error");
        assert!(matches!(error, PluginError::AlreadyInstalled(_)));
    }

    #[tokio::test]
    async fn upgrade_disposition_proceeds_past_the_already_installed_gate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bamboo_home = dir.path().join("bamboo-home");
        let installer = LocalPluginInstaller::new(bamboo_home.clone());

        // Same seed as the FailIfInstalled test.
        let mut store = InstalledPlugins::default();
        store.add(InstalledPlugin {
            id: "hello-plugin".to_string(),
            version: "0.0.1".to_string(),
            source: PluginSource::LocalDir {
                path: dir.path().to_path_buf(),
            },
            plugin_dir: bamboo_home.join("plugins").join("hello-plugin"),
            installed_at: Utc::now(),
            registered: RegisteredCapabilities::default(),
        });
        store
            .save(&bamboo_home.join("plugins").join("installed.json"))
            .await
            .unwrap();

        let plugin_dir = dir.path().join("plugin-src");
        write_skill_dirs(&plugin_dir, &["hello-world"]).await;
        let manifest = manifest_with(vec!["hello-world"], vec![]);

        // Upgrade must NOT hit AlreadyInstalled; it proceeds to the (still
        // later-agent) registration TODO.
        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::Upgrade,
                Utc::now(),
            )
            .await
            .expect_err("registration wiring is a later-agent TODO");
        assert!(matches!(error, PluginError::NotImplemented(_)));
    }
}
