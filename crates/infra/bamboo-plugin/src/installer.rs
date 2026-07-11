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
//! validation, platform gating, MCP-entry token resolution, provenance
//! listing) and returns [`PluginError::NotImplemented`] at the exact points
//! that need capability-registration wiring, with a comment enumerating what
//! goes there and citing the exact files. It is a reference/example, not a
//! requirement to reuse verbatim — Wave-2's installer-core agent may replace
//! it entirely with a type that holds an `AppState` handle.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::{PluginError, PluginResult};
use crate::manifest::{Platform, PluginManifest};
use crate::registry::{InstalledPlugin, InstalledPlugins, PluginSource};

/// The installer method surface. `install`/`uninstall`/`list` are the three
/// verbs the CLI (`bamboo plugin install/list/remove/update`) and the HTTP
/// routes (`/api/v1/plugins`) both call through.
#[async_trait]
pub trait PluginInstaller {
    /// Register a plugin already unpacked at `plugin_dir` (source handling —
    /// copying a local dir, unpacking a `.tar.gz`, fetching+verifying a URL —
    /// happens BEFORE this is called; by the time `install` runs, `plugin_dir`
    /// already contains `plugin.json` plus the `skills/`/`prompts/`/
    /// `workflows/`/`bin/` layout the manifest declares).
    ///
    /// On success, the returned [`InstalledPlugin`] must have already been
    /// persisted into `installed.json` (i.e. `install` commits provenance
    /// itself; callers should not need to separately call
    /// `InstalledPlugins::add`).
    async fn install(
        &self,
        manifest: &PluginManifest,
        plugin_dir: &Path,
        source: PluginSource,
        installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin>;

    /// Reverse everything `install` registered for `id` (stop + remove MCP
    /// servers, remove prompt presets, remove workflow files), then remove
    /// the provenance entry and delete `plugin_dir` from disk.
    async fn uninstall(&self, id: &str) -> PluginResult<()>;

    /// All currently-installed plugins (a thin read of `installed.json`).
    async fn list(&self) -> PluginResult<Vec<InstalledPlugin>>;
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
        _installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin> {
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

        // --- Path resolution (trivial, pure — fully implemented here) ---
        //
        // Resolve what each declared MCP server WOULD look like once
        // registered. Kept even though it's unused past this point (`_`) to
        // fail `install` early if a manifest has an unresolvable entry (e.g.
        // empty stdio command), and to demonstrate the resolution call the
        // installer-core agent should reuse.
        let platform = current_platform.unwrap_or(Platform::Linux);
        let _resolved_mcp_servers = manifest
            .provides
            .mcp_servers
            .iter()
            .map(|entry| entry.resolve(plugin_dir, &manifest.id, platform))
            .collect::<PluginResult<Vec<_>>>()?;

        // Sanity-check declared skill dirs / workflow files exist on disk.
        // Skills need no further action here — they're discovered in place by
        // bamboo-skills' plugin discovery-dir extension, not copied.
        for skill_dir in &manifest.provides.skills {
            let skill_md = plugin_dir.join("skills").join(skill_dir).join("SKILL.md");
            if !tokio::fs::try_exists(&skill_md).await.unwrap_or(false) {
                return Err(PluginError::InvalidManifest(format!(
                    "declared skill '{skill_dir}' has no SKILL.md at {}",
                    skill_md.display()
                )));
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

        // --- Capability-registration wiring (TODO: installer-core agent) ---
        //
        // None of this can be implemented in this infra-layer crate — it all
        // needs `AppState` (bamboo-server, an app-layer crate this crate must
        // not depend on). See PLUGIN_PLAN.md § Installer-core agent for the
        // full breakdown. In order:
        //
        //   1. MCP: merge `_resolved_mcp_servers` into `Config.mcp.servers`
        //      via `AppState::update_config`
        //      (crates/app/bamboo-server/src/app_state/config_runtime.rs),
        //      reusing the merge-by-id logic in
        //      crates/app/bamboo-server/src/handlers/agent/mcp/server_handlers/import.rs
        //      (`import_servers`), then call
        //      `state.mcp_manager.start_server(..)` for each enabled one.
        //   2. Prompts: append `manifest.provides.prompts` into
        //      `prompt-presets.json`
        //      (crates/app/bamboo-server/src/handlers/agent/prompt_presets/storage.rs),
        //      reusing `validate_preset_id` / `ensure_unique_preset_id` so a
        //      plugin can never silently clobber an existing user preset id.
        //   3. Workflows: copy `<plugin_dir>/workflows/<name>.md` into
        //      `bamboo_config::paths::workflows_dir()/<name>.md` (validate
        //      each name with `bamboo_config::paths::is_safe_workflow_name`).
        //   4. Only once 1-3 succeed: build the `RegisteredCapabilities`
        //      reflecting exactly what got registered, then commit
        //      provenance via `InstalledPlugins::load` + `.add(..)` +
        //      `.save(..)` at `self.installed_json_path()`.
        let _ = self.installed_json_path(); // reserved for step 4 above.
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
        // capabilities, stop + remove each `mcp_server_ids` entry from
        // `config.json` (`AppState::update_config` + `mcp_manager.stop_server`),
        // remove each `preset_ids` entry from `prompt-presets.json`, delete
        // each `workflow_filenames` file from
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
                Utc::now(),
            )
            .await
            .expect_err("missing SKILL.md should fail validation");
        assert!(matches!(error, PluginError::InvalidManifest(_)));
    }

    #[tokio::test]
    async fn install_reaches_not_implemented_once_declared_files_exist() {
        let dir = tempfile::tempdir().expect("tempdir");
        let installer = LocalPluginInstaller::new(dir.path().join("bamboo-home"));

        let plugin_dir = dir.path().join("plugin-src");
        let skill_dir = plugin_dir.join("skills").join("hello-world");
        tokio::fs::create_dir_all(&skill_dir).await.unwrap();
        tokio::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: hello-world\ndescription: demo\n---\nHi\n",
        )
        .await
        .unwrap();

        let manifest = manifest_with(vec!["hello-world"], vec![]);

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
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
}
