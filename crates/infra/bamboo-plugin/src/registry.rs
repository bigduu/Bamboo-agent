//! Provenance registry: `~/.bamboo/plugins/installed.json`.
//!
//! Records, for each installed plugin, EXACTLY what it registered (which
//! `mcpServers` ids, which skill dir names, which prompt preset ids, which
//! workflow filenames) so uninstall/upgrade can precisely undo only what a
//! given plugin added — never touching a user's own hand-added entries that
//! happen to share a config file with plugin-registered ones.
//!
//! This module only defines the schema + load/save/add/remove helpers. Wiring
//! *when* to call `add`/`remove` relative to actually registering/
//! deregistering capabilities (MCP servers, prompt presets, workflow files)
//! is the installer's job (see [`crate::installer`] and `PLUGIN_PLAN.md`).

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::error::{PluginError, PluginResult};

/// Where a plugin's installed bundle came from. Recorded verbatim so
/// `update`/reinstall can re-fetch from the same place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginSource {
    /// Installed from a local directory (copied or referenced in place — the
    /// installer decides which; either way this records the ORIGINAL path the
    /// user pointed at, not necessarily `plugin_dir`).
    LocalDir { path: PathBuf },
    /// Installed by unpacking a local `.tar.gz` archive.
    LocalArchive { path: PathBuf },
    /// Installed by fetching a URL (optionally sha256-verified).
    Url {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },
}

/// Exactly what an installed plugin registered into Bamboo's shared capability
/// stores. Every id/name here MUST have actually been written by the
/// installer for THIS plugin — never a superset (that would risk clobbering
/// or removing a user's own entries on uninstall) and never a subset
/// (uninstall would leak orphaned registrations).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredCapabilities {
    /// Ids registered into `config.json`'s `mcpServers` map.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_server_ids: Vec<String>,
    /// Directory names under `<plugin_dir>/skills/` that are valid skill
    /// dirs (contain `SKILL.md`) and are therefore discoverable in place.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_dirs: Vec<String>,
    /// Ids appended into `prompt-presets.json`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preset_ids: Vec<String>,
    /// Filenames copied into `bamboo_config::paths::workflows_dir()`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflow_filenames: Vec<String>,
}

impl RegisteredCapabilities {
    pub fn is_empty(&self) -> bool {
        self.mcp_server_ids.is_empty()
            && self.skill_dirs.is_empty()
            && self.preset_ids.is_empty()
            && self.workflow_filenames.is_empty()
    }
}

/// A single installed plugin's provenance record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledPlugin {
    pub id: String,
    /// The manifest `version` at the time of this install/upgrade.
    pub version: String,
    pub source: PluginSource,
    /// `~/.bamboo/plugins/<id>` — where the plugin's own files live.
    pub plugin_dir: PathBuf,
    /// Caller-supplied timestamp (NOT computed internally — see module docs
    /// on why: keeps this crate free of a hidden `Utc::now()` call so tests
    /// and callers stay in full control of "when").
    pub installed_at: DateTime<Utc>,
    #[serde(default)]
    pub registered: RegisteredCapabilities,
}

/// The full `installed.json` document: `{ "plugins": [ ... ] }`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstalledPlugins {
    #[serde(default)]
    pub plugins: Vec<InstalledPlugin>,
}

impl InstalledPlugins {
    /// Load from `path`. A missing file is treated as an empty registry (this
    /// is the state before any plugin has ever been installed) rather than an
    /// error.
    pub async fn load(path: &Path) -> PluginResult<Self> {
        match fs::try_exists(path).await {
            Ok(true) => {}
            Ok(false) => return Ok(Self::default()),
            Err(error) => return Err(PluginError::Io(error)),
        }

        let raw = fs::read_to_string(path).await?;
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }
        let store: Self = serde_json::from_str(&raw)?;
        Ok(store)
    }

    /// Persist to `path`, creating parent directories as needed.
    pub async fn save(&self, path: &Path) -> PluginResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let serialized = serde_json::to_string_pretty(self)?;
        fs::write(path, serialized).await?;
        Ok(())
    }

    /// Look up a plugin by id.
    pub fn get(&self, id: &str) -> Option<&InstalledPlugin> {
        self.plugins.iter().find(|plugin| plugin.id == id)
    }

    /// Insert or replace (by id) — an upgrade re-adds the same id with a new
    /// version/registered set, so this is an upsert rather than an append.
    pub fn add(&mut self, plugin: InstalledPlugin) {
        self.remove(&plugin.id);
        self.plugins.push(plugin);
    }

    /// Remove and return the entry for `id`, if any.
    pub fn remove(&mut self, id: &str) -> Option<InstalledPlugin> {
        let index = self.plugins.iter().position(|plugin| plugin.id == id)?;
        Some(self.plugins.remove(index))
    }

    /// All installed plugins, in insertion order.
    pub fn list(&self) -> &[InstalledPlugin] {
        &self.plugins
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_plugin(id: &str) -> InstalledPlugin {
        InstalledPlugin {
            id: id.to_string(),
            version: "0.1.0".to_string(),
            source: PluginSource::LocalDir {
                path: PathBuf::from("/tmp/source"),
            },
            plugin_dir: PathBuf::from(format!("/home/user/.bamboo/plugins/{id}")),
            installed_at: DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            registered: RegisteredCapabilities {
                mcp_server_ids: vec![],
                skill_dirs: vec!["hello-world".to_string()],
                preset_ids: vec!["hello_preset".to_string()],
                workflow_filenames: vec![],
            },
        }
    }

    #[tokio::test]
    async fn load_missing_file_returns_empty_registry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("plugins").join("installed.json");
        let loaded = InstalledPlugins::load(&path).await.expect("load");
        assert!(loaded.plugins.is_empty());
    }

    #[tokio::test]
    async fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("plugins").join("installed.json");

        let mut store = InstalledPlugins::default();
        store.add(sample_plugin("hello-plugin"));
        store.add(sample_plugin("other-plugin"));
        store.save(&path).await.expect("save");

        let loaded = InstalledPlugins::load(&path).await.expect("load");
        assert_eq!(loaded.plugins.len(), 2);
        let hello = loaded.get("hello-plugin").expect("hello-plugin present");
        assert_eq!(hello.version, "0.1.0");
        assert_eq!(hello.registered.skill_dirs, vec!["hello-world".to_string()]);
        assert_eq!(
            hello.registered.preset_ids,
            vec!["hello_preset".to_string()]
        );
        assert_eq!(
            hello.source,
            PluginSource::LocalDir {
                path: PathBuf::from("/tmp/source")
            }
        );
    }

    #[tokio::test]
    async fn add_upserts_by_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("installed.json");

        let mut store = InstalledPlugins::default();
        store.add(sample_plugin("hello-plugin"));

        let mut upgraded = sample_plugin("hello-plugin");
        upgraded.version = "0.2.0".to_string();
        store.add(upgraded);

        assert_eq!(store.plugins.len(), 1);
        assert_eq!(store.get("hello-plugin").unwrap().version, "0.2.0");

        store.save(&path).await.expect("save");
        let loaded = InstalledPlugins::load(&path).await.expect("load");
        assert_eq!(loaded.plugins.len(), 1);
        assert_eq!(loaded.get("hello-plugin").unwrap().version, "0.2.0");
    }

    #[tokio::test]
    async fn remove_deletes_and_returns_entry() {
        let mut store = InstalledPlugins::default();
        store.add(sample_plugin("hello-plugin"));

        let removed = store.remove("hello-plugin").expect("present before remove");
        assert_eq!(removed.id, "hello-plugin");
        assert!(store.get("hello-plugin").is_none());
        assert!(store.remove("hello-plugin").is_none());
    }

    #[tokio::test]
    async fn load_empty_file_returns_empty_registry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("installed.json");
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, "").await.unwrap();

        let loaded = InstalledPlugins::load(&path).await.expect("load");
        assert!(loaded.plugins.is_empty());
    }
}
