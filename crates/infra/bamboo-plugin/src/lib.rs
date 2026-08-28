//! Foundation for Bamboo's local plugin system.
//!
//! A **plugin** is a locally-installed bundle that can provide any of:
//! skills, MCP servers, prompt presets, workflows, supervised services, and
//! declarative ToolEvent sinks. It is
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
//! | Services | `ServiceManager` | supervised process owned by exact plugin provenance |
//! | Event sinks | `installed.json` + pure reconciliation plan | `bamboo-server` activates the plan through its AppState-owned `ToolEventRouter` |
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

pub use bamboo_plugin_protocol::ToolEventSubscriptionId;
pub use error::{PluginError, PluginResult};
pub use installer::{
    load_previous_for_disposition, on_disk_skill_dirs, preflight_install, InstallDisposition,
    LocalPluginInstaller, PluginInstaller,
};
pub use manifest::{
    platform_bin_path, EventSinkCapabilityState, EventSinkDeliveryLimits, EventSinkInactiveReason,
    EventSinkManifestEntry, EventSinkProtocolManifest, EventSinkSubscriptionManifest,
    GracefulShutdown, HealthCheckKind, HealthCheckSpec, McpServerManifestEntry,
    McpTransportManifest, ObservationPermissionId, Platform, PluginArtifact, PluginManifest,
    PluginPromptPreset, PluginProvides, ResolvedServiceEntry, ServiceInputProtocol,
    ServiceManifestEntry, ShutdownSignal, DEFAULT_EVENT_SINK_QUEUE_CAPACITY,
    MAX_EVENT_SINKS_PER_PLUGIN, MAX_EVENT_SINK_EVENT_BYTES, MAX_EVENT_SINK_EXTENSION_FIELDS,
    MAX_EVENT_SINK_EXTENSION_KEY_BYTES, MAX_EVENT_SINK_EXTENSION_VALUE_BYTES,
    MAX_EVENT_SINK_ID_BYTES, MAX_EVENT_SINK_MANIFEST_BUFFER_BYTES, MAX_EVENT_SINK_PERMISSIONS,
    MAX_EVENT_SINK_PERMISSION_ID_BYTES, MAX_EVENT_SINK_QUEUE_CAPACITY,
    MAX_EVENT_SINK_SERVICE_ID_BYTES, MAX_EVENT_SINK_SUBSCRIPTIONS, MAX_EVENT_SINK_TOOL_NAMES,
    OBSERVE_CONTENT_PERMISSION, OBSERVE_DIFF_PERMISSION, OBSERVE_METADATA_PERMISSION,
    OBSERVE_PATHS_PERMISSION, OBSERVE_TOOL_NAME_PERMISSION, PLATFORM_BIN_TOKEN,
};
pub use registry::{
    classify_ownership, reconcile_event_sinks, reconcile_exclusive, reconcile_plugin_boot,
    EventSinkReconciliation, EventSinkRemovalOrder, ExclusiveReconciliation, InstalledPlugin,
    InstalledPlugins, Ownership, PluginBootCandidate, PluginBootIssue, PluginBootReconciliation,
    PluginInstallStatus, PluginSource, ReconciledEventSink, RegisteredCapabilities,
};
