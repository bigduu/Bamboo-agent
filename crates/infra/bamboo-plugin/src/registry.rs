//! Provenance registry: `~/.bamboo/plugins/installed.json`.
//!
//! Records, for each installed plugin, EXACTLY what it registered (which
//! `mcpServers` ids, which skill dir names, which prompt preset ids, and any
//! legacy workflow-copy filenames) so uninstall/upgrade can precisely undo only what a
//! given plugin added — never touching a user's own hand-added entries that
//! happen to share a config file with plugin-registered ones.
//!
//! This module only defines the schema + load/save/add/remove helpers. Wiring
//! *when* to call `add`/`remove` relative to actually registering/
//! deregistering capabilities (MCP servers, prompt presets, workflow files)
//! is the installer's job (see [`crate::installer`] and `PLUGIN_PLAN.md`).

use std::collections::HashSet;
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
    /// Installed by fetching a URL. Three trust layers, all enforced by
    /// `bamboo-server`'s `plugin_source.rs` before this record is written:
    ///
    /// 1. **Host allowlist** (source authorization) — was the URL's host
    ///    fetched from an operator-trusted host (`allow_untrusted_host` opts
    ///    out).
    /// 2. **Signature** (publisher authenticity) — did the bundle's `.sig`
    ///    verify against a trusted ed25519 key (`signed_by`; `allow_unsigned`
    ///    opts out of requiring one).
    /// 3. **Checksum** (integrity) — `sha256` is the user-verified hash of
    ///    the downloaded BUNDLE (the `plugin.json`, or the archive containing
    ///    it) — `Some` in the normal case, confirmed against a
    ///    caller-supplied expected hash BEFORE anything was
    ///    extracted/trusted.
    ///
    /// `sha256` is `None` either when the install explicitly opted out of
    /// checksum verification (`allow_unverified: true`, no hash supplied), OR
    /// when a verified signature (`signed_by: Some(_)`) already established
    /// integrity+authenticity more strongly than a pasted checksum could —
    /// see `plugin_source.rs`'s module docs for why a valid signature
    /// supersedes the checksum requirement. An install refuses outright
    /// rather than silently trusting an unpinned/unsigned download from an
    /// untrusted host, so every `None`/`false` combination here always means
    /// a deliberate, recorded risk acceptance, never an oversight.
    ///
    /// `insecure` is the convenience AGGREGATE over the three per-layer
    /// opt-outs above: `true` when this install waived all three at once,
    /// either via a per-install `--insecure` flag / `"insecure": true` on the
    /// request, or because `plugin_trust.enforcement` was `off` at install
    /// time (see `bamboo-server`'s `plugin_source.rs` module docs). Recorded
    /// separately from the three individual `allow_*` fields so `plugin
    /// list`/audit can tell "an operator deliberately accepted ALL risk for
    /// this source" apart from "these three flags happened to all be set
    /// individually" — functionally identical, but a meaningfully different
    /// signal for review. `#[serde(default)]` so a pre-existing
    /// `installed.json` row (written before this field existed) loads as
    /// `false` rather than failing to deserialize.
    Url {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
        /// Recorded for audit: true when this install ran with
        /// `allow_unverified` and no `sha256`. `#[serde(default)]` so a
        /// pre-existing `installed.json` row (written before this field
        /// existed, back when only the per-platform binary artifact was
        /// pinned) loads as `false` rather than failing to deserialize.
        #[serde(default, skip_serializing_if = "is_false")]
        allow_unverified: bool,
        /// Recorded for audit: true when this install ran with
        /// `allow_untrusted_host` against a host outside
        /// `plugin_trust.trusted_hosts`. `#[serde(default)]` for backward
        /// compat with rows written before this field existed.
        #[serde(default, skip_serializing_if = "is_false")]
        allow_untrusted_host: bool,
        /// Recorded for audit: true when this install ran with
        /// `allow_unsigned` (no valid signature from a trusted key).
        /// `#[serde(default)]` for backward compat.
        #[serde(default, skip_serializing_if = "is_false")]
        allow_unsigned: bool,
        /// The label of the `plugin_trust.trusted_keys` entry the bundle's
        /// `.sig` verified against, or `None` if the install proceeded
        /// unsigned (`allow_unsigned: true`). `#[serde(default)]` for
        /// backward compat with rows written before signing existed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signed_by: Option<String>,
        /// True when this install ran with ALL THREE trust layers waived at
        /// once via the `--insecure` / `"insecure": true` aggregate opt-out,
        /// or because `plugin_trust.enforcement` was `off` — see this
        /// variant's doc comment above. `#[serde(default)]` for backward
        /// compat with rows written before this field existed.
        #[serde(default, skip_serializing_if = "is_false")]
        insecure: bool,
    },
}

/// `skip_serializing_if` helper for a `bool` field that should be omitted
/// from the JSON when `false` (serde has no built-in equivalent of
/// `std::ops::Not::not` that takes a reference).
fn is_false(value: &bool) -> bool {
    !*value
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
    /// Files copied into the global workflow directory by pre-#561 installers.
    /// New plugin workflows remain in place, so new installs leave this empty;
    /// the field remains for backward-compatible cleanup during upgrade/remove.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflow_filenames: Vec<String>,
    /// Ids started via bamboo-server's `ServiceManager` (issue #479, prereq
    /// for epic #477). `#[serde(default)]` so an `installed.json` written
    /// before services existed loads with an empty set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub service_ids: Vec<String>,
}

impl RegisteredCapabilities {
    pub fn is_empty(&self) -> bool {
        self.mcp_server_ids.is_empty()
            && self.skill_dirs.is_empty()
            && self.preset_ids.is_empty()
            && self.workflow_filenames.is_empty()
            && self.service_ids.is_empty()
    }

    /// The capabilities present in `old` (a prior install's registered set)
    /// but ABSENT from `self` (the set the new/upgraded install will register).
    ///
    /// These are exactly the entries an in-place upgrade must DE-register:
    /// their ids/filenames vanish from provenance across the upgrade, so if
    /// they are not actively removed here they leak — orphaned forever,
    /// un-removable because no future uninstall knows they were ours. See the
    /// upgrade sequence in [`crate::installer`] / `PLUGIN_PLAN.md`.
    ///
    /// Order-preserving relative to `old` (stable output for diffing/logging).
    pub fn removed_since(&self, old: &RegisteredCapabilities) -> RegisteredCapabilities {
        RegisteredCapabilities {
            mcp_server_ids: subtract(&old.mcp_server_ids, &self.mcp_server_ids),
            skill_dirs: subtract(&old.skill_dirs, &self.skill_dirs),
            preset_ids: subtract(&old.preset_ids, &self.preset_ids),
            workflow_filenames: subtract(&old.workflow_filenames, &self.workflow_filenames),
            service_ids: subtract(&old.service_ids, &self.service_ids),
        }
    }
}

/// Elements of `from` not present in `remove`, preserving `from`'s order.
fn subtract(from: &[String], remove: &[String]) -> Vec<String> {
    let drop: HashSet<&str> = remove.iter().map(String::as_str).collect();
    from.iter()
        .filter(|value| !drop.contains(value.as_str()))
        .cloned()
        .collect()
}

/// Ownership classification of one declared capability id/filename against a
/// shared store, for the REFUSE-on-conflict capability kinds (MCP servers,
/// workflow files). Prompt presets do NOT use this — they rename on collision
/// via bamboo-server's `ensure_unique_preset_id` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// Not present in the shared store — safe to create AND record as
    /// plugin-owned/removable.
    New,
    /// Present, and registered by THIS plugin's prior install — an upgrade
    /// re-registering its own entry. Safe; stays recorded as plugin-owned.
    OwnedReinstall,
    /// Present and NOT owned by this plugin (a user's own entry, or another
    /// plugin's). Must block the install — never recorded as removable.
    ForeignConflict,
}

/// Classify one id against the shared store's current `existing` ids and the
/// `owned_previously` ids that THIS plugin's prior install registered.
pub fn classify_ownership(
    id: &str,
    existing: &HashSet<&str>,
    owned_previously: &HashSet<&str>,
) -> Ownership {
    if !existing.contains(id) {
        Ownership::New
    } else if owned_previously.contains(id) {
        Ownership::OwnedReinstall
    } else {
        Ownership::ForeignConflict
    }
}

/// Result of reconciling a plugin's declared ids/filenames against a shared
/// store for a REFUSE-on-conflict capability (MCP servers, workflow files).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExclusiveReconciliation {
    /// Genuinely-new plus this-plugin's-own-from-a-prior-install: register
    /// these and record them as plugin-owned/removable in provenance.
    pub to_register: Vec<String>,
    /// Foreign collisions (exist, not owned by this plugin). If this is
    /// non-empty the caller MUST refuse the install (return
    /// [`PluginError::Conflict`]) — do not register or record any of these.
    pub foreign_conflicts: Vec<String>,
}

/// Reconcile `declared` ids against the shared store for a REFUSE-on-conflict
/// capability. `existing` = every id currently in the shared store;
/// `owned_previously` = the ids THIS plugin's prior install recorded (empty
/// for a fresh install). Pure — the caller supplies the store state (which,
/// for MCP/workflows, only the app layer can read).
///
/// This is the pre-check that closes BLOCKER 1: a pre-existing collision with
/// a non-plugin entry lands in `foreign_conflicts`, so it is NEVER registered
/// and NEVER recorded as removable — uninstall can therefore only ever delete
/// entries this plugin genuinely created.
pub fn reconcile_exclusive(
    declared: &[String],
    existing: &[String],
    owned_previously: &[String],
) -> ExclusiveReconciliation {
    let existing_set: HashSet<&str> = existing.iter().map(String::as_str).collect();
    let owned_set: HashSet<&str> = owned_previously.iter().map(String::as_str).collect();

    let mut result = ExclusiveReconciliation::default();
    for id in declared {
        match classify_ownership(id, &existing_set, &owned_set) {
            Ownership::New | Ownership::OwnedReinstall => result.to_register.push(id.clone()),
            Ownership::ForeignConflict => result.foreign_conflicts.push(id.clone()),
        }
    }
    result
}

/// Lifecycle status of a provenance row — the crash-safety journal marker.
///
/// The installer writes a row as [`Self::Installing`] BEFORE it begins
/// registering capabilities (MCP into `config.json`, prompts, workflow files),
/// and flips it to [`Self::Installed`] only after the whole sequence succeeds.
/// A row left [`Self::Installing`] therefore marks an install that was
/// interrupted (a hard process kill mid-install): its `registered` set names
/// what the install INTENDED to own, so on the next install/upgrade of that id
/// the installer can (a) treat the leftover as this-plugin-owned — so the
/// ownership pre-check doesn't false-`Conflict` on the plugin's own
/// half-written entries — and (b) clean it up as an upgrade-over-incomplete.
/// `uninstall` works on an `Installing` row too, so a user is never stranded
/// having to hand-edit `config.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginInstallStatus {
    /// A crash-safety journal marker: capability registration has begun but
    /// not yet completed. `registered` records the INTENDED ownership set.
    Installing,
    /// The steady state: registration completed and provenance is authoritative.
    /// The [`Default`] so a pre-journal `installed.json` (no `status` field)
    /// deserializes as a completed install (backward compat).
    #[default]
    Installed,
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
    /// Crash-safety journal marker (see [`PluginInstallStatus`]). Defaults to
    /// [`PluginInstallStatus::Installed`] so an `installed.json` written before
    /// this field existed loads as a completed install.
    #[serde(default)]
    pub status: PluginInstallStatus,
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
    ///
    /// Writes to a sibling `<path>.tmp` first, then `rename`s it over `path`
    /// — `rename` is atomic on the same filesystem (and `<path>.tmp` sits
    /// right next to `path`, guaranteeing that), so a hard kill mid-write can
    /// only ever leave a stray, harmless `.tmp` file behind, never a
    /// truncated/corrupt `installed.json` a later `load` would choke on.
    pub async fn save(&self, path: &Path) -> PluginResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let serialized = serde_json::to_string_pretty(self)?;
        let tmp_path = tmp_path_for(path);
        fs::write(&tmp_path, serialized).await?;
        fs::rename(&tmp_path, path).await?;
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

/// `<path>` with `.tmp` appended to its file name (e.g. `installed.json` ->
/// `installed.json.tmp`) — a sibling in the SAME directory as `path`, so the
/// `rename` in [`InstalledPlugins::save`] is guaranteed same-filesystem and
/// therefore atomic.
fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    path.with_file_name(tmp_name)
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
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                mcp_server_ids: vec![],
                skill_dirs: vec!["hello-world".to_string()],
                preset_ids: vec!["hello_preset".to_string()],
                workflow_filenames: vec![],
                service_ids: vec![],
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
    async fn save_is_atomic_via_tmp_file_rename() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("installed.json");
        let tmp_path = tmp_path_for(&path);

        let mut store = InstalledPlugins::default();
        store.add(sample_plugin("hello-plugin"));
        store.save(&path).await.expect("save");

        assert!(path.exists(), "installed.json should exist after save");
        assert!(
            !tmp_path.exists(),
            "the .tmp staging file must be renamed over the target, never left behind"
        );

        // A second save (e.g. an upgrade re-persisting the store) must go
        // through the same write-tmp-then-rename path and leave no trace
        // either.
        let mut reloaded = InstalledPlugins::load(&path).await.expect("load");
        reloaded.add(sample_plugin("other-plugin"));
        reloaded.save(&path).await.expect("save again");
        assert!(!tmp_path.exists());

        let loaded = InstalledPlugins::load(&path).await.expect("load");
        assert_eq!(loaded.plugins.len(), 2);
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

    #[test]
    fn reconcile_exclusive_fresh_install_splits_new_from_foreign() {
        // Fresh install (no prior ownership): "a" is new, "b" collides with a
        // user's own entry.
        let declared = vec!["a".to_string(), "b".to_string()];
        let existing = vec!["b".to_string(), "user-thing".to_string()];
        let owned_previously: Vec<String> = vec![];

        let reconciliation = reconcile_exclusive(&declared, &existing, &owned_previously);
        assert_eq!(reconciliation.to_register, vec!["a".to_string()]);
        assert_eq!(reconciliation.foreign_conflicts, vec!["b".to_string()]);
    }

    #[test]
    fn reconcile_exclusive_upgrade_reregisters_own_but_refuses_new_foreign() {
        // Upgrade: "a" was ours last time (owned reinstall, fine); "c" is new;
        // "d" newly collides with a user entry that appeared since → foreign.
        let declared = vec!["a".to_string(), "c".to_string(), "d".to_string()];
        let existing = vec!["a".to_string(), "d".to_string()];
        let owned_previously = vec!["a".to_string()];

        let reconciliation = reconcile_exclusive(&declared, &existing, &owned_previously);
        assert_eq!(
            reconciliation.to_register,
            vec!["a".to_string(), "c".to_string()]
        );
        assert_eq!(reconciliation.foreign_conflicts, vec!["d".to_string()]);
    }

    #[test]
    fn classify_ownership_three_way() {
        let existing: HashSet<&str> = ["x", "y"].into_iter().collect();
        let owned: HashSet<&str> = ["y"].into_iter().collect();
        assert_eq!(classify_ownership("z", &existing, &owned), Ownership::New);
        assert_eq!(
            classify_ownership("y", &existing, &owned),
            Ownership::OwnedReinstall
        );
        assert_eq!(
            classify_ownership("x", &existing, &owned),
            Ownership::ForeignConflict
        );
    }

    #[test]
    fn removed_since_computes_dropped_capabilities_per_kind() {
        let old = RegisteredCapabilities {
            mcp_server_ids: vec!["srv-a".to_string(), "srv-b".to_string()],
            skill_dirs: vec!["skill-a".to_string()],
            preset_ids: vec!["preset-a".to_string(), "preset-b".to_string()],
            workflow_filenames: vec!["wf-a.md".to_string()],
            service_ids: vec!["svc-a".to_string(), "svc-b".to_string()],
        };
        // New version drops srv-b, preset-a, and svc-b; keeps the rest; adds srv-c.
        let new = RegisteredCapabilities {
            mcp_server_ids: vec!["srv-a".to_string(), "srv-c".to_string()],
            skill_dirs: vec!["skill-a".to_string()],
            preset_ids: vec!["preset-b".to_string()],
            workflow_filenames: vec!["wf-a.md".to_string()],
            service_ids: vec!["svc-a".to_string()],
        };

        let removed = new.removed_since(&old);
        assert_eq!(removed.mcp_server_ids, vec!["srv-b".to_string()]);
        assert!(removed.skill_dirs.is_empty());
        assert_eq!(removed.preset_ids, vec!["preset-a".to_string()]);
        assert!(removed.workflow_filenames.is_empty());
        assert_eq!(removed.service_ids, vec!["svc-b".to_string()]);
    }

    #[test]
    fn reconcile_exclusive_covers_service_ids_same_as_other_kinds() {
        // `reconcile_exclusive` is capability-kind-agnostic (plain `Vec<String>`
        // in/out), but exercise it explicitly against service ids since
        // that's a new call site (issue #479's install step).
        let declared = vec!["svc-a".to_string(), "svc-b".to_string()];
        let existing = vec!["svc-b".to_string(), "other-plugins-svc".to_string()];
        let owned_previously: Vec<String> = vec![];

        let reconciliation = reconcile_exclusive(&declared, &existing, &owned_previously);
        assert_eq!(reconciliation.to_register, vec!["svc-a".to_string()]);
        assert_eq!(reconciliation.foreign_conflicts, vec!["svc-b".to_string()]);
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
