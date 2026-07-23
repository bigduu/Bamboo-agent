//! Foundation for Bamboo's local plugin system.
//!
//! A **plugin** is a locally-installed bundle that can provide any of:
//! skills, MCP servers, prompt presets, and (future) workflows. It is
//! installed to `~/.bamboo/plugins/<id>/` (see
//! `bamboo_config::paths::plugin_dir`), keeping the plugin's own files
//! together (manifest, skills, prompts, workflows, optional per-platform
//! binaries), then REGISTERED into Bamboo's existing capability locations:
//!
//! | Capability | Registered into | Discovery model |
//! |---|---|---|
//! | MCP servers | `config.json` (`Config.mcp`) + `mcp_manager.start_server` | copied into shared config |
//! | Skills | N/A — discovered **in place** | `~/.bamboo/plugins/*/skills` is an additional `SkillDiscoveryDir` (see `bamboo-skills`) |
//! | Prompt presets | `prompt-presets.json` | copied into shared store |
//! | Legacy workflows | N/A — discovered **in place** | `~/.bamboo/plugins/*/workflows/*.md` is a read-only Skill adapter source |
//!
//! This crate defines the shared skeleton three things build on:
//!
//! 1. [`manifest::PluginManifest`] — the `plugin.json` schema, with
//!    validation and the `${plugin_dir}`/`${platform_bin}` token-substitution
//!    contract for MCP stdio commands (see the `manifest` module docs for the
//!    full contract).
//! 2. [`registry::InstalledPlugins`] — the `~/.bamboo/plugins/installed.json`
//!    provenance registry (load/save/add/remove), recording exactly what each
//!    plugin registered so uninstall/upgrade is precise.
//! 3. [`installer::PluginInstaller`] — the trait later agents implement to
//!    actually wire capability registration (that wiring needs `AppState`,
//!    which this `infra`-layer crate intentionally does not depend on).
//!
//! See the repo-root `PLUGIN_PLAN.md` (temporary, deleted before final merge)
//! for how the remaining work is split across parallel agents, and
//! `examples/hello-plugin/` for a minimal end-to-end reference plugin
//! (one skill + one prompt preset, no binary, no MCP server).

pub mod error;
pub mod installer;
pub mod manifest;
pub mod registry;

pub use error::{PluginError, PluginResult};
pub use installer::{
    load_previous_for_disposition, on_disk_skill_dirs, preflight_install, InstallDisposition,
    LocalPluginInstaller, PluginInstaller,
};
pub use manifest::{
    platform_bin_path, GracefulShutdown, HealthCheckKind, HealthCheckSpec, McpServerManifestEntry,
    McpTransportManifest, Platform, PluginArtifact, PluginManifest, PluginPromptPreset,
    PluginProvides, ResolvedServiceEntry, ServiceManifestEntry, ShutdownSignal, PLATFORM_BIN_TOKEN,
};
pub use registry::{
    classify_ownership, reconcile_exclusive, ExclusiveReconciliation, InstalledPlugin,
    InstalledPlugins, Ownership, PluginInstallStatus, PluginSource, RegisteredCapabilities,
};
