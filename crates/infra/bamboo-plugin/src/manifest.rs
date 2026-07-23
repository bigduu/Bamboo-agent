//! `plugin.json` manifest schema.
//!
//! A plugin bundle is a directory (installed at `~/.bamboo/plugins/<id>/`) with a
//! `plugin.json` at its root describing what it *provides*: MCP servers, skills,
//! prompt presets, and (future) workflows. This module defines that schema and a
//! handful of pure, side-effect-free helpers (validation + `${...}` token
//! substitution) that the installer (a later agent) builds on.
//!
//! # Directory layout convention
//!
//! ```text
//! ~/.bamboo/plugins/<id>/
//!   plugin.json          <- this manifest
//!   skills/<skill-dir>/SKILL.md   (one or more, referenced by `provides.skills`)
//!   prompts/              (optional; unused by the inline prompt design, see below)
//!   workflows/<name>.md   (referenced by `provides.workflows`)
//!   bin/<platform>/<id>[.exe]     (optional per-platform binary, see substitution contract)
//! ```
//!
//! # Design decision: inline prompts, not file references
//!
//! `provides.prompts` is a `Vec<PluginPromptPreset>` with the preset content
//! inlined directly in `plugin.json` (mirroring bamboo-server's
//! `StoredPromptPreset { id, name, description?, content }`), rather than a list
//! of filenames under `prompts/`. Rationale: prompt presets are small, and
//! inlining keeps `plugin.json` a single self-contained source of truth the
//! installer can validate and append into `prompt-presets.json` without a second
//! file-read pass or an extra path-traversal surface. A future manifest version
//! could add a file-reference variant if presets grow large enough to want
//! external editing.
//!
//! # Substitution contract for `mcp_servers[].transport.stdio.{command,args,cwd,env}`
//!
//! Stdio MCP server commands may reference two tokens, resolved by the
//! installer at install/registration time (see [`substitute_tokens`]):
//!
//! - `${plugin_dir}` — the absolute path to the installed plugin's root
//!   directory (i.e. the directory containing `plugin.json`).
//! - `${platform_bin}` — the absolute path to this plugin's per-platform
//!   binary, resolved as `<plugin_dir>/bin/<platform>/<plugin id>[.exe on windows]`
//!   where `<platform>` is one of `macos` | `windows` | `linux` (matching
//!   [`Platform::as_str`]). This is a fixed naming convention (binary filename
//!   == manifest `id`, `.exe` suffix only on Windows) so a single manifest
//!   works across platforms without per-OS conditionals in `plugin.json` — if a
//!   plugin needs a different binary name, it can still express that by joining
//!   directly, e.g. `"${plugin_dir}/bin/${platform}/nova"` — but `${platform}`
//!   alone is intentionally NOT provided as a token (see [`substitute_tokens`]
//!   doc) to keep the contract to exactly two tokens.
//!
//! Tokens are substituted in `command`, each element of `args`, `cwd`, and each
//! value in `env` (not env *keys*, and not in `url` for sse/streamable_http —
//! remote endpoints have no plugin-local path to inject).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{PluginError, PluginResult};

/// Target OS gate / per-platform artifact key.
///
/// Kept as a 3-way enum (rather than a free-form string) so `platforms` /
/// `${platform_bin}` resolution / artifact selection all agree on the exact
/// same three spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Macos,
    Windows,
    Linux,
}

impl Platform {
    /// The platform this process is currently running on, if it is one of the
    /// three Bamboo supports. `None` for anything else (e.g. `freebsd`) — a
    /// platform gate should treat that as "not supported" rather than guess.
    pub fn current() -> Option<Platform> {
        Self::parse(std::env::consts::OS)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Platform::Macos => "macos",
            Platform::Windows => "windows",
            Platform::Linux => "linux",
        }
    }

    /// Parse the lowercase spelling used both in `plugin.json` and in
    /// `std::env::consts::OS` (which already yields "macos"/"windows"/"linux").
    pub fn parse(value: &str) -> Option<Platform> {
        match value {
            "macos" => Some(Platform::Macos),
            "windows" => Some(Platform::Windows),
            "linux" => Some(Platform::Linux),
            _ => None,
        }
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single MCP server this plugin wants to register, shaped like
/// [`bamboo_domain::mcp_config::McpServerConfig`] but with `${plugin_dir}` /
/// `${platform_bin}` tokens allowed in the stdio transport's path-shaped
/// fields. See the module docs for the substitution contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerManifestEntry {
    /// Server id — becomes the `mcpServers` map key once registered.
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub transport: McpTransportManifest,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// Transport variants a manifest can declare. Mirrors
/// [`bamboo_domain::mcp_config::TransportConfig`]'s three transports, minus
/// the fields the installer fills in with sensible defaults at registration
/// time (timeouts, reconnect policy) — a manifest author shouldn't need to
/// know Bamboo's default timeout values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpTransportManifest {
    Stdio {
        /// May contain `${plugin_dir}` / `${platform_bin}`.
        command: String,
        #[serde(default)]
        args: Vec<String>,
        /// May contain `${plugin_dir}` / `${platform_bin}`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        /// Values (not keys) may contain `${plugin_dir}` / `${platform_bin}`.
        #[serde(default)]
        env: HashMap<String, String>,
    },
    Sse {
        url: String,
        #[serde(default)]
        headers: Vec<bamboo_domain::mcp_config::HeaderConfig>,
    },
    #[serde(rename = "streamable_http")]
    StreamableHttp {
        url: String,
        #[serde(default)]
        headers: Vec<bamboo_domain::mcp_config::HeaderConfig>,
    },
}

impl McpServerManifestEntry {
    /// Resolve this manifest entry into a real
    /// [`bamboo_domain::mcp_config::McpServerConfig`], substituting
    /// `${plugin_dir}` / `${platform_bin}` tokens and filling in Bamboo's
    /// standard defaults for timeouts/reconnect. Pure — does not touch disk,
    /// does not start anything. The caller (installer) is responsible for
    /// merging the result into `config.json` and calling
    /// `mcp_manager.start_server`.
    pub fn resolve(
        &self,
        plugin_dir: &Path,
        plugin_id: &str,
        platform: Platform,
    ) -> PluginResult<bamboo_domain::mcp_config::McpServerConfig> {
        use bamboo_domain::mcp_config::{
            default_connect_timeout, default_healthcheck_interval, default_request_timeout,
            default_startup_timeout, McpServerConfig, ReconnectConfig, SseConfig, StdioConfig,
            StreamableHttpConfig, TransportConfig,
        };

        let transport = match &self.transport {
            McpTransportManifest::Stdio {
                command,
                args,
                cwd,
                env,
            } => {
                if command.trim().is_empty() {
                    return Err(PluginError::InvalidManifest(format!(
                        "mcp server '{}' has an empty stdio command",
                        self.id
                    )));
                }
                TransportConfig::Stdio(StdioConfig {
                    command: substitute_tokens(command, plugin_dir, plugin_id, platform),
                    args: args
                        .iter()
                        .map(|value| substitute_tokens(value, plugin_dir, plugin_id, platform))
                        .collect(),
                    cwd: cwd
                        .as_deref()
                        .map(|value| substitute_tokens(value, plugin_dir, plugin_id, platform)),
                    env: env
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.clone(),
                                substitute_tokens(value, plugin_dir, plugin_id, platform),
                            )
                        })
                        .collect(),
                    env_encrypted: HashMap::new(),
                    env_credential_refs: std::collections::HashMap::new(),
                    startup_timeout_ms: default_startup_timeout(),
                })
            }
            McpTransportManifest::Sse { url, headers } => TransportConfig::Sse(SseConfig {
                url: url.clone(),
                headers: headers.clone(),
                connect_timeout_ms: default_connect_timeout(),
            }),
            McpTransportManifest::StreamableHttp { url, headers } => {
                TransportConfig::StreamableHttp(StreamableHttpConfig {
                    url: url.clone(),
                    headers: headers.clone(),
                    connect_timeout_ms: default_connect_timeout(),
                })
            }
        };

        Ok(McpServerConfig {
            id: self.id.clone(),
            name: self.name.clone(),
            enabled: self.enabled,
            transport,
            request_timeout_ms: default_request_timeout(),
            healthcheck_interval_ms: default_healthcheck_interval(),
            reconnect: ReconnectConfig::default(),
            allowed_tools: self.allowed_tools.clone(),
            denied_tools: self.denied_tools.clone(),
        })
    }
}

/// Substitute `${plugin_dir}` and `${platform_bin}` in `template`. Unknown
/// `${...}` tokens are left untouched (forward-compatible: a newer manifest
/// using a token an older Bamboo doesn't know about degrades to a literal
/// string rather than failing).
pub fn substitute_tokens(
    template: &str,
    plugin_dir: &Path,
    plugin_id: &str,
    platform: Platform,
) -> String {
    let plugin_dir_str = plugin_dir.to_string_lossy();
    let platform_bin_str = platform_bin_path(plugin_dir, plugin_id, platform)
        .to_string_lossy()
        .into_owned();
    template
        .replace("${plugin_dir}", plugin_dir_str.as_ref())
        .replace("${platform_bin}", &platform_bin_str)
}

/// Resolve the fixed-convention per-platform binary path:
/// `<plugin_dir>/bin/<platform>/<plugin_id>[.exe]`.
pub fn platform_bin_path(plugin_dir: &Path, plugin_id: &str, platform: Platform) -> PathBuf {
    let filename = if matches!(platform, Platform::Windows) {
        format!("{plugin_id}.exe")
    } else {
        plugin_id.to_string()
    };
    plugin_dir
        .join("bin")
        .join(platform.as_str())
        .join(filename)
}

/// Literal token a [`ServiceManifestEntry::command`] must equal EXACTLY (no
/// PATH resolution, no ambient binaries — see [`ServiceManifestEntry`]'s
/// docs and `PluginManifest::validate`). Also used by
/// [`PluginManifest::uses_platform_bin_token`].
pub const PLATFORM_BIN_TOKEN: &str = "${platform_bin}";

/// How [`ServiceManager`](../../bamboo_server/service_manager/index.html)
/// (bamboo-server) should poll a running service for liveness. `ProcessAlive`
/// is the v1 default (no `target`); `Tcp`/`Http` additionally require a
/// `target` (validated in [`PluginManifest::validate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckKind {
    ProcessAlive,
    Tcp,
    Http,
}

fn default_health_interval_ms() -> u64 {
    15_000
}

fn default_health_timeout_ms() -> u64 {
    5_000
}

/// Health-check policy for a [`ServiceManifestEntry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckSpec {
    pub kind: HealthCheckKind,
    /// Required (non-empty) for `Tcp` (`host:port`) / `Http` (a URL);
    /// unused for `ProcessAlive`. Validated in [`PluginManifest::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default = "default_health_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_health_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for HealthCheckSpec {
    fn default() -> Self {
        Self {
            kind: HealthCheckKind::ProcessAlive,
            target: None,
            interval_ms: default_health_interval_ms(),
            timeout_ms: default_health_timeout_ms(),
        }
    }
}

/// Signal a service's graceful shutdown sends before escalating to a hard
/// kill. `Term` (SIGTERM on unix; a best-effort equivalent request on
/// Windows before `TerminateProcess`) is the default; `None` skips the
/// graceful signal entirely and kills immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownSignal {
    #[default]
    Term,
    None,
}

fn default_shutdown_timeout_ms() -> u64 {
    5_000
}

/// Graceful-shutdown policy for a [`ServiceManifestEntry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GracefulShutdown {
    #[serde(default)]
    pub signal: ShutdownSignal,
    /// How long to wait after `signal` before escalating to SIGKILL /
    /// `TerminateProcess`.
    #[serde(default = "default_shutdown_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for GracefulShutdown {
    fn default() -> Self {
        Self {
            signal: ShutdownSignal::default(),
            timeout_ms: default_shutdown_timeout_ms(),
        }
    }
}

/// A long-running service this plugin wants supervised (issue #479, prereq
/// for epic #477 — standalone connectors distributed as plugins). The
/// highest-trust artifact kind a plugin can declare: unlike an MCP stdio
/// server (whose `command` is free-form — see [`McpServerManifestEntry`]), a
/// service's `command` MUST be exactly [`PLATFORM_BIN_TOKEN`] — no PATH
/// resolution, no ambient binaries. It may only ever execute the plugin's
/// own verified, sha256-pinned per-platform binary (see
/// [`PluginArtifact`]'s archive contract) resolved via
/// [`platform_bin_path`].
///
/// `args`/`cwd`/`env` (values only) accept the same `${plugin_dir}`/
/// `${platform_bin}` substitution as MCP stdio entries — see
/// [`substitute_tokens`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceManifestEntry {
    /// Service id — becomes the key bamboo-server's `ServiceManager` and
    /// provenance (`RegisteredCapabilities::service_ids`) key off.
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// MUST validate as exactly [`PLATFORM_BIN_TOKEN`] — see the type docs.
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// May contain `${plugin_dir}` / `${platform_bin}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Values (not keys) may contain `${plugin_dir}` / `${platform_bin}`.
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub health_check: HealthCheckSpec,
    /// Reuses [`bamboo_domain::mcp_config::ReconnectConfig`]'s shape
    /// (enabled/initial_backoff_ms/max_backoff_ms/max_attempts) per the
    /// issue's design.
    #[serde(default)]
    pub restart_policy: bamboo_domain::mcp_config::ReconnectConfig,
    #[serde(default)]
    pub graceful_shutdown: GracefulShutdown,
}

/// A [`ServiceManifestEntry`] with all `${...}` tokens substituted and
/// `command` resolved to the concrete per-platform binary path — pure, ready
/// for bamboo-server's `ServiceManager` to spawn. Analogous to
/// [`McpServerManifestEntry::resolve`]'s `McpServerConfig` output.
#[derive(Debug, Clone)]
pub struct ResolvedServiceEntry {
    pub id: String,
    pub name: Option<String>,
    pub enabled: bool,
    pub command: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub health_check: HealthCheckSpec,
    pub restart_policy: bamboo_domain::mcp_config::ReconnectConfig,
    pub graceful_shutdown: GracefulShutdown,
}

impl ServiceManifestEntry {
    /// Resolve this manifest entry against a concrete `plugin_dir`/platform.
    /// Pure — does not touch disk, does not spawn anything. `command` is
    /// always [`platform_bin_path`] (never `substitute_tokens`'d from
    /// `self.command`) — validation already pins `self.command` to exactly
    /// [`PLATFORM_BIN_TOKEN`], and `platform_bin_path` IS that token's
    /// resolution.
    pub fn resolve(
        &self,
        plugin_dir: &Path,
        plugin_id: &str,
        platform: Platform,
    ) -> ResolvedServiceEntry {
        ResolvedServiceEntry {
            id: self.id.clone(),
            name: self.name.clone(),
            enabled: self.enabled,
            command: platform_bin_path(plugin_dir, plugin_id, platform),
            args: self
                .args
                .iter()
                .map(|value| substitute_tokens(value, plugin_dir, plugin_id, platform))
                .collect(),
            cwd: self.cwd.as_deref().map(|value| {
                PathBuf::from(substitute_tokens(value, plugin_dir, plugin_id, platform))
            }),
            env: self
                .env
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        substitute_tokens(value, plugin_dir, plugin_id, platform),
                    )
                })
                .collect(),
            health_check: self.health_check.clone(),
            restart_policy: self.restart_policy.clone(),
            graceful_shutdown: self.graceful_shutdown.clone(),
        }
    }
}

/// Inline prompt preset, mirroring bamboo-server's
/// `StoredPromptPreset { id, name, description?, content }` (see
/// `crates/app/bamboo-server/src/handlers/agent/prompt_presets/types.rs`).
/// `id` must satisfy the same rule bamboo-server enforces:
/// `[a-z0-9_]`, length <= 80 (see [`is_valid_preset_id`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginPromptPreset {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub content: String,
}

/// Per-platform downloadable artifact for the URL-install source (fetch logic
/// is a later agent's job — this is schema-only).
///
/// # Archive contract (pinned for Wave-2 fetch code + plugin authors)
///
/// `url` points at an **archive**, never a raw executable: a `.zip` **or**
/// `.tar.gz`/`.tgz`. The installer:
/// 1. downloads it, verifies [`Self::sha256`] (lowercase hex, over the raw
///    archive bytes) BEFORE unpacking anything,
/// 2. unpacks it, and expects **exactly one executable at the archive root**
///    named `<plugin id>` (unix) or `<plugin id>.exe` (windows),
/// 3. places that executable at `<plugin_dir>/bin/<platform>/<plugin id>[.exe]`
///    — the exact path [`platform_bin_path`] resolves, so `${platform_bin}`
///    then points at it.
///
/// This matches how real release assets ship (e.g. nova's are
/// `nova-v<ver>-<triple>.zip` with `nova.exe` at the zip root, and a
/// `.tar.gz` with `nova` at the tar root) — a plugin does NOT have to
/// re-layout its release binaries, it just declares the archive URL + hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginArtifact {
    /// Archive URL (`.zip` / `.tar.gz` / `.tgz`) — see the type-level docs.
    pub url: String,
    /// Lowercase hex-encoded sha256 of the raw archive bytes, verified by the
    /// installer after download and BEFORE unpacking.
    pub sha256: String,
}

/// What a plugin provides: any subset of MCP servers, skills, prompt presets,
/// and (future) workflows.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PluginProvides {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_servers: Vec<McpServerManifestEntry>,
    /// Directory names under `<plugin_dir>/skills/`. Each must contain a
    /// `SKILL.md`. These are discovered *in place* (no copy, no symlink) once
    /// the skill-discovery extension picks up the plugin dir — see
    /// `bamboo-skills`' `SkillDirectorySource::Plugin`. Declaring them here is
    /// for provenance/validation, not for making discovery work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prompts: Vec<PluginPromptPreset>,
    /// `.md` filenames under `<plugin_dir>/workflows/`. They remain in place
    /// and are discovered as read-only legacy Skill adapters; installation
    /// never copies them into a user's global workflow directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<String>,
    /// Long-running services this plugin wants supervised — see
    /// [`ServiceManifestEntry`]. Issue #479 (prereq for epic #477).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ServiceManifestEntry>,
}

impl PluginProvides {
    pub fn is_empty(&self) -> bool {
        self.mcp_servers.is_empty()
            && self.skills.is_empty()
            && self.prompts.is_empty()
            && self.workflows.is_empty()
            && self.services.is_empty()
    }
}

/// The `plugin.json` manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Stable identifier, `[a-z0-9_-]`, used as the install directory name
    /// (`~/.bamboo/plugins/<id>/`) and default binary name.
    pub id: String,
    pub name: String,
    /// Semver-shaped string (`major.minor.patch[-pre][+build]`). Validated
    /// structurally by [`PluginManifest::validate`]; actual semver comparison
    /// for upgrade decisions is the installer's job (not depended on here to
    /// avoid pulling in a semver crate for a foundation crate).
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Minimum Bamboo version required, same shape as `version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bamboo_min_version: Option<String>,
    /// Platform gate. `None` means "no restriction" (all platforms). `Some([])`
    /// is rejected by [`PluginManifest::validate`] (an explicit empty gate
    /// would mean "installable nowhere", which is never intended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platforms: Option<Vec<Platform>>,
    #[serde(default)]
    pub provides: PluginProvides,
    /// Per-platform downloadable bundle for the URL-install source. Keys are
    /// the same lowercase strings as [`Platform::as_str`] (kept as `String`
    /// rather than `Platform` here so an unknown/typo'd key surfaces as a
    /// clear validation error instead of a silent serde failure on the whole
    /// map — see [`PluginManifest::validate`]).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub artifacts: HashMap<String, PluginArtifact>,
}

const MAX_PLUGIN_ID_LEN: usize = 64;
const MAX_PRESET_ID_LEN: usize = 80;

/// Preset ids the plugin system must NOT let a plugin claim, because
/// bamboo-server reserves them. `"general_assistant"` is its
/// `DEFAULT_PRESET_ID` (see
/// `crates/app/bamboo-server/src/handlers/agent/prompt_presets/types.rs`);
/// `sanitize_store` there silently STRIPS any stored preset with that id, so a
/// plugin declaring it would pass a naive `[a-z0-9_]` check but then vanish at
/// runtime with no error. Reject it up front at manifest validation instead.
const RESERVED_PRESET_IDS: &[&str] = &["general_assistant"];

/// `[a-z0-9-_]`, non-empty, no leading/trailing separator, no `--`/`__` runs
/// are NOT specifically forbidden (unlike skill ids) since plugin ids may
/// legitimately contain underscores (e.g. ported from an npm-style package
/// name) — only characters and length are constrained.
pub fn is_valid_plugin_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_PLUGIN_ID_LEN
        && id
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_')
}

/// Same rule bamboo-server's prompt-preset store enforces
/// (`validate_preset_id` in `handlers/agent/prompt_presets/storage.rs`):
/// `[a-z0-9_]`, length <= 80 — plus a rejection of ids bamboo-server reserves
/// (see [`RESERVED_PRESET_IDS`]) so a plugin can't declare one that would be
/// silently dropped later.
pub fn is_valid_preset_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_PRESET_ID_LEN
        && !RESERVED_PRESET_IDS.contains(&id)
        && id
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

/// A conservative, dependency-free semver *shape* check: `N.N.N` with
/// optional `-pre` / `+build` suffixes. Doesn't validate pre-release/build
/// identifier grammar precisely — good enough to reject obviously-wrong
/// strings (`"latest"`, `""`, `"1.0"`) without adding a `semver` dependency to
/// a foundation crate. The installer can layer stricter comparison later.
pub fn is_plausible_semver(value: &str) -> bool {
    let core = value.split(['-', '+']).next().unwrap_or_default();
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

/// Rejects path separators, `..` traversal, empty strings, and control
/// characters — used for both `provides.skills` dir names and
/// `provides.workflows` filenames (which additionally must end in `.md`).
fn is_safe_relative_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && !name.chars().any(|ch| ch.is_control())
}

impl PluginManifest {
    /// Parse a manifest from a `plugin.json` file's contents. Does not
    /// validate — call [`Self::validate`] separately (parse vs. validate are
    /// kept distinct so a caller can inspect an invalid-but-parseable
    /// manifest, e.g. to report a precise validation error).
    pub fn parse_str(content: &str) -> PluginResult<Self> {
        serde_json::from_str(content).map_err(PluginError::from)
    }

    /// Structural validation beyond what serde already enforces. Does not
    /// touch disk (skill-dir / workflow-file *existence* checks happen at
    /// install time against a concrete `plugin_dir`, not here).
    pub fn validate(&self) -> PluginResult<()> {
        if !is_valid_plugin_id(&self.id) {
            return Err(PluginError::InvalidManifest(format!(
                "invalid plugin id '{}': must be [a-z0-9-_], <= {} chars",
                self.id, MAX_PLUGIN_ID_LEN
            )));
        }
        if self.name.trim().is_empty() {
            return Err(PluginError::InvalidManifest(
                "plugin name must not be empty".to_string(),
            ));
        }
        if !is_plausible_semver(&self.version) {
            return Err(PluginError::InvalidManifest(format!(
                "invalid plugin version '{}': expected major.minor.patch[-pre][+build]",
                self.version
            )));
        }
        if let Some(min_version) = &self.bamboo_min_version {
            if !is_plausible_semver(min_version) {
                return Err(PluginError::InvalidManifest(format!(
                    "invalid bamboo_min_version '{min_version}'"
                )));
            }
        }
        if let Some(platforms) = &self.platforms {
            if platforms.is_empty() {
                return Err(PluginError::InvalidManifest(
                    "platforms, if present, must not be empty (use `null`/omit for \"all platforms\")"
                        .to_string(),
                ));
            }
        }

        let mut seen_mcp_ids = std::collections::HashSet::new();
        for entry in &self.provides.mcp_servers {
            if entry.id.trim().is_empty() {
                return Err(PluginError::InvalidManifest(
                    "mcp server entries must have a non-empty id".to_string(),
                ));
            }
            if !seen_mcp_ids.insert(entry.id.clone()) {
                return Err(PluginError::InvalidManifest(format!(
                    "duplicate mcp server id '{}' in provides.mcp_servers",
                    entry.id
                )));
            }
            if let McpTransportManifest::Stdio { command, .. } = &entry.transport {
                if command.trim().is_empty() {
                    return Err(PluginError::InvalidManifest(format!(
                        "mcp server '{}' has an empty stdio command",
                        entry.id
                    )));
                }
            }
        }

        let mut seen_service_ids = std::collections::HashSet::new();
        for entry in &self.provides.services {
            if entry.id.trim().is_empty() {
                return Err(PluginError::InvalidManifest(
                    "service entries must have a non-empty id".to_string(),
                ));
            }
            if !seen_service_ids.insert(entry.id.clone()) {
                return Err(PluginError::InvalidManifest(format!(
                    "duplicate service id '{}' in provides.services",
                    entry.id
                )));
            }
            if entry.command.trim().is_empty() {
                return Err(PluginError::InvalidManifest(format!(
                    "service '{}' has an empty command",
                    entry.id
                )));
            }
            // Services are the highest-trust artifact kind: no PATH
            // resolution, no ambient binaries. `command` must be EXACTLY the
            // substitution token, never a literal path or shell command —
            // stricter than MCP's free-form stdio `command`.
            if entry.command != PLATFORM_BIN_TOKEN {
                return Err(PluginError::InvalidManifest(format!(
                    "service '{}' command must be exactly '{PLATFORM_BIN_TOKEN}' — services may \
                     only execute the plugin's own verified per-platform binary, never an \
                     arbitrary command",
                    entry.id
                )));
            }
            match entry.health_check.kind {
                HealthCheckKind::Tcp | HealthCheckKind::Http => {
                    let target_ok = entry
                        .health_check
                        .target
                        .as_deref()
                        .map(|value| !value.trim().is_empty())
                        .unwrap_or(false);
                    if !target_ok {
                        return Err(PluginError::InvalidManifest(format!(
                            "service '{}' health_check.kind={:?} requires a non-empty target",
                            entry.id, entry.health_check.kind
                        )));
                    }
                }
                HealthCheckKind::ProcessAlive => {}
            }
        }

        for skill_dir in &self.provides.skills {
            if !is_safe_relative_name(skill_dir) {
                return Err(PluginError::InvalidManifest(format!(
                    "invalid skill directory name '{skill_dir}' in provides.skills"
                )));
            }
        }

        let mut seen_preset_ids = std::collections::HashSet::new();
        for preset in &self.provides.prompts {
            if !is_valid_preset_id(&preset.id) {
                return Err(PluginError::InvalidManifest(format!(
                    "invalid prompt preset id '{}': must be [a-z0-9_], <= {} chars",
                    preset.id, MAX_PRESET_ID_LEN
                )));
            }
            if !seen_preset_ids.insert(preset.id.clone()) {
                return Err(PluginError::InvalidManifest(format!(
                    "duplicate prompt preset id '{}' in provides.prompts",
                    preset.id
                )));
            }
            if preset.name.trim().is_empty() {
                return Err(PluginError::InvalidManifest(format!(
                    "prompt preset '{}' has an empty name",
                    preset.id
                )));
            }
            if preset.content.trim().is_empty() {
                return Err(PluginError::InvalidManifest(format!(
                    "prompt preset '{}' has empty content",
                    preset.id
                )));
            }
        }

        for workflow_file in &self.provides.workflows {
            if !is_safe_relative_name(workflow_file) || !workflow_file.ends_with(".md") {
                return Err(PluginError::InvalidManifest(format!(
                    "invalid workflow filename '{workflow_file}' in provides.workflows (must be a bare '<name>.md')"
                )));
            }
        }

        for (platform_key, artifact) in &self.artifacts {
            let Some(artifact_platform) = Platform::parse(platform_key) else {
                return Err(PluginError::InvalidManifest(format!(
                    "unknown platform key '{platform_key}' in artifacts (expected macos/windows/linux)"
                )));
            };
            // An artifact for a platform this plugin does not claim to support
            // is dead weight at best and a sign of a mistake at worst — reject
            // it so the manifest can't drift out of sync with its own gate.
            if let Some(gate) = &self.platforms {
                if !gate.contains(&artifact_platform) {
                    return Err(PluginError::InvalidManifest(format!(
                        "artifacts contains platform '{platform_key}' which is not in the \
                         `platforms` gate {:?}",
                        gate.iter()
                            .map(|platform| platform.as_str())
                            .collect::<Vec<_>>()
                    )));
                }
            }
            if artifact.url.trim().is_empty() {
                return Err(PluginError::InvalidManifest(format!(
                    "artifact for platform '{platform_key}' has an empty url"
                )));
            }
            let sha_is_hex64 = artifact.sha256.len() == 64
                && artifact.sha256.chars().all(|ch| ch.is_ascii_hexdigit());
            if !sha_is_hex64 {
                return Err(PluginError::InvalidManifest(format!(
                    "artifact for platform '{platform_key}' has an invalid sha256 (expected 64 lowercase hex chars)"
                )));
            }
        }

        // Binary-backed URL install: if a per-platform binary is needed
        // (`${platform_bin}` is used) AND this manifest ships downloadable
        // `artifacts` (the URL-install path), then every platform the plugin
        // claims to support MUST have an artifact — otherwise the install
        // would fail opaquely on that OS at runtime (no binary to place under
        // `bin/`) instead of here, at manifest validation. Local-dir/archive
        // installs ship `bin/` directly and declare no `artifacts`, so this is
        // (correctly) skipped for them.
        if !self.artifacts.is_empty() && self.uses_platform_bin_token() {
            for platform in self.effective_platforms() {
                if !self.artifacts.contains_key(platform.as_str()) {
                    return Err(PluginError::InvalidManifest(format!(
                        "plugin uses ${{platform_bin}} and ships URL artifacts, but has no \
                         artifact for supported platform '{}' (every supported platform needs a \
                         downloadable binary bundle)",
                        platform.as_str()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Whether this manifest is installable on the given platform (`true` if
    /// `platforms` is unset — no restriction).
    pub fn supports_platform(&self, platform: Platform) -> bool {
        match &self.platforms {
            None => true,
            Some(platforms) => platforms.contains(&platform),
        }
    }

    /// The effective set of platforms this plugin claims to support: the
    /// `platforms` gate if present, otherwise all three (an unset gate means
    /// "all platforms").
    pub fn effective_platforms(&self) -> Vec<Platform> {
        self.platforms
            .clone()
            .unwrap_or_else(|| vec![Platform::Macos, Platform::Windows, Platform::Linux])
    }

    /// Whether any declared MCP stdio server references the `${platform_bin}`
    /// substitution token (in command, args, cwd, or an env value) — i.e.
    /// whether this plugin needs a per-platform binary to run. Drives the
    /// artifacts/platform cross-check in [`Self::validate`].
    pub fn uses_platform_bin_token(&self) -> bool {
        const TOKEN: &str = PLATFORM_BIN_TOKEN;
        let mcp_uses = self.provides.mcp_servers.iter().any(|entry| {
            let McpTransportManifest::Stdio {
                command,
                args,
                cwd,
                env,
            } = &entry.transport
            else {
                return false;
            };
            command.contains(TOKEN)
                || args.iter().any(|value| value.contains(TOKEN))
                || cwd.as_deref().is_some_and(|value| value.contains(TOKEN))
                || env.values().any(|value| value.contains(TOKEN))
        });
        // Services validate to command == PLATFORM_BIN_TOKEN exactly, but
        // this helper must stay correct even against a not-yet-validated
        // manifest (it feeds the artifacts/platform cross-check inside
        // `validate()` itself), so check `.contains` the same way MCP does
        // rather than assuming validity.
        let service_uses = self.provides.services.iter().any(|entry| {
            entry.command.contains(TOKEN)
                || entry.args.iter().any(|value| value.contains(TOKEN))
                || entry
                    .cwd
                    .as_deref()
                    .is_some_and(|value| value.contains(TOKEN))
                || entry.env.values().any(|value| value.contains(TOKEN))
        });
        mcp_uses || service_uses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_manifest_json() -> &'static str {
        r#"{
            "id": "hello-plugin",
            "name": "Hello Plugin",
            "version": "0.1.0",
            "provides": {
                "skills": ["hello-world"],
                "prompts": [
                    {"id": "hello_preset", "name": "Hello Preset", "content": "Say hello."}
                ]
            }
        }"#
    }

    #[test]
    fn parses_minimal_manifest() {
        let manifest = PluginManifest::parse_str(minimal_manifest_json()).expect("parse");
        assert_eq!(manifest.id, "hello-plugin");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(manifest.provides.skills, vec!["hello-world".to_string()]);
        assert_eq!(manifest.provides.prompts.len(), 1);
        assert!(manifest.provides.mcp_servers.is_empty());
        assert!(manifest.artifacts.is_empty());
        manifest.validate().expect("minimal manifest is valid");
    }

    #[test]
    fn parses_full_manifest_with_mcp_and_artifacts() {
        let json = r#"{
            "id": "nova_plugin",
            "name": "Nova",
            "version": "1.2.3-beta+build.7",
            "description": "Desktop control MCP server",
            "bamboo_min_version": "2026.7.0",
            "platforms": ["macos", "windows", "linux"],
            "provides": {
                "mcp_servers": [
                    {
                        "id": "nova",
                        "enabled": true,
                        "transport": {
                            "type": "stdio",
                            "command": "${platform_bin}",
                            "args": ["--serve"],
                            "cwd": "${plugin_dir}",
                            "env": {"NOVA_HOME": "${plugin_dir}/data"}
                        }
                    }
                ],
                "workflows": ["daily-report.md"]
            },
            "artifacts": {
                "macos": {"url": "https://example.com/nova-macos.tar.gz", "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                "windows": {"url": "https://example.com/nova-windows.zip", "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
                "linux": {"url": "https://example.com/nova-linux.tar.gz", "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
            }
        }"#;

        let manifest = PluginManifest::parse_str(json).expect("parse full manifest");
        manifest.validate().expect("full manifest is valid");
        assert!(manifest.supports_platform(Platform::Macos));
        assert!(manifest.supports_platform(Platform::Windows));
        assert!(manifest.supports_platform(Platform::Linux));

        let entry = &manifest.provides.mcp_servers[0];
        let plugin_dir = Path::new("/home/user/.bamboo/plugins/nova_plugin");
        let resolved = entry
            .resolve(plugin_dir, &manifest.id, Platform::Macos)
            .expect("resolve mcp entry");
        match resolved.transport {
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                assert_eq!(
                    stdio.command,
                    "/home/user/.bamboo/plugins/nova_plugin/bin/macos/nova_plugin"
                );
                assert_eq!(stdio.cwd.as_deref(), Some(plugin_dir.to_str().unwrap()));
                assert_eq!(
                    stdio.env.get("NOVA_HOME").map(String::as_str),
                    Some("/home/user/.bamboo/plugins/nova_plugin/data")
                );
            }
            _ => panic!("expected stdio transport"),
        }
    }

    #[test]
    fn platform_bin_path_appends_exe_on_windows_only() {
        let dir = Path::new("/plugins/demo");
        assert_eq!(
            platform_bin_path(dir, "demo", Platform::Macos),
            PathBuf::from("/plugins/demo/bin/macos/demo")
        );
        assert_eq!(
            platform_bin_path(dir, "demo", Platform::Windows),
            PathBuf::from("/plugins/demo/bin/windows/demo.exe")
        );
        assert_eq!(
            platform_bin_path(dir, "demo", Platform::Linux),
            PathBuf::from("/plugins/demo/bin/linux/demo")
        );
    }

    #[test]
    fn rejects_invalid_id() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.id = "Bad Id!".to_string();
        let error = manifest.validate().expect_err("bad id should fail");
        assert!(error.to_string().contains("invalid plugin id"));
    }

    #[test]
    fn rejects_bad_semver() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.version = "latest".to_string();
        let error = manifest.validate().expect_err("bad version should fail");
        assert!(error.to_string().contains("invalid plugin version"));
    }

    #[test]
    fn rejects_empty_platforms_list() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.platforms = Some(vec![]);
        let error = manifest
            .validate()
            .expect_err("empty platforms should fail");
        assert!(error.to_string().contains("platforms"));
    }

    #[test]
    fn rejects_duplicate_mcp_server_ids() {
        let json = r#"{
            "id": "dup",
            "name": "Dup",
            "version": "1.0.0",
            "provides": {
                "mcp_servers": [
                    {"id": "a", "transport": {"type": "stdio", "command": "x"}},
                    {"id": "a", "transport": {"type": "stdio", "command": "y"}}
                ]
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        let error = manifest
            .validate()
            .expect_err("duplicate mcp id should fail");
        assert!(error.to_string().contains("duplicate mcp server id"));
    }

    #[test]
    fn rejects_traversal_in_skill_dir_and_bad_workflow_filename() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.provides.skills = vec!["../escape".to_string()];
        assert!(manifest.validate().is_err());

        let mut manifest2: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest2.provides.skills = vec![];
        manifest2.provides.workflows = vec!["not-markdown.txt".to_string()];
        assert!(manifest2.validate().is_err());
    }

    #[test]
    fn rejects_invalid_artifact_sha256() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.artifacts.insert(
            "macos".to_string(),
            PluginArtifact {
                url: "https://example.com/x.tar.gz".to_string(),
                sha256: "not-hex".to_string(),
            },
        );
        let error = manifest.validate().expect_err("bad sha256 should fail");
        assert!(error.to_string().contains("sha256"));
    }

    #[test]
    fn rejects_platform_bin_plugin_missing_an_artifact_for_a_supported_platform() {
        // Uses ${platform_bin}, supports all three platforms (no gate), ships
        // URL artifacts — but only for macos + windows. Missing linux artifact
        // must be caught at validation, not at install time on a linux host.
        let json = r#"{
            "id": "binbacked",
            "name": "Bin Backed",
            "version": "1.0.0",
            "provides": {
                "mcp_servers": [
                    {"id": "srv", "transport": {"type": "stdio", "command": "${platform_bin}"}}
                ]
            },
            "artifacts": {
                "macos": {"url": "https://example.com/x-macos.tar.gz", "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                "windows": {"url": "https://example.com/x-windows.zip", "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        let error = manifest
            .validate()
            .expect_err("missing linux artifact should fail");
        assert!(error.to_string().contains("linux"));
    }

    #[test]
    fn platform_bin_plugin_is_valid_when_gate_narrows_to_covered_platforms() {
        // Same plugin, but a `platforms` gate narrows support to exactly the
        // platforms that DO have artifacts → valid.
        let json = r#"{
            "id": "binbacked",
            "name": "Bin Backed",
            "version": "1.0.0",
            "platforms": ["macos", "windows"],
            "provides": {
                "mcp_servers": [
                    {"id": "srv", "transport": {"type": "stdio", "command": "${platform_bin}"}}
                ]
            },
            "artifacts": {
                "macos": {"url": "https://example.com/x-macos.tar.gz", "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                "windows": {"url": "https://example.com/x-windows.zip", "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        manifest
            .validate()
            .expect("gate-narrowed binary plugin is valid");
        assert!(manifest.uses_platform_bin_token());
    }

    #[test]
    fn rejects_artifact_for_platform_outside_the_gate() {
        let json = r#"{
            "id": "gated",
            "name": "Gated",
            "version": "1.0.0",
            "platforms": ["macos"],
            "artifacts": {
                "linux": {"url": "https://example.com/x-linux.tar.gz", "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        let error = manifest
            .validate()
            .expect_err("artifact outside gate should fail");
        assert!(error.to_string().contains("not in the `platforms` gate"));
    }

    #[test]
    fn local_install_with_platform_bin_and_no_artifacts_is_valid() {
        // Local-dir/archive install: uses ${platform_bin} but ships bin/
        // directly (no `artifacts`). The cross-check must NOT fire.
        let json = r#"{
            "id": "localbin",
            "name": "Local Bin",
            "version": "1.0.0",
            "provides": {
                "mcp_servers": [
                    {"id": "srv", "transport": {"type": "stdio", "command": "${platform_bin}"}}
                ]
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        manifest
            .validate()
            .expect("local binary plugin without artifacts is valid");
    }

    #[test]
    fn rejects_reserved_preset_id() {
        let json = r#"{
            "id": "reserver",
            "name": "Reserver",
            "version": "1.0.0",
            "provides": {
                "prompts": [
                    {"id": "general_assistant", "name": "Nope", "content": "x"}
                ]
            }
        }"#;
        let manifest = PluginManifest::parse_str(json).unwrap();
        let error = manifest
            .validate()
            .expect_err("reserved preset id should fail");
        assert!(error.to_string().contains("prompt preset id"));
        assert!(!is_valid_preset_id("general_assistant"));
    }

    #[test]
    fn rejects_unknown_artifact_platform_key() {
        let mut manifest: PluginManifest = serde_json::from_str(minimal_manifest_json()).unwrap();
        manifest.artifacts.insert(
            "solaris".to_string(),
            PluginArtifact {
                url: "https://example.com/x.tar.gz".to_string(),
                sha256: "a".repeat(64),
            },
        );
        let error = manifest
            .validate()
            .expect_err("unknown platform key should fail");
        assert!(error.to_string().contains("unknown platform key"));
    }

    #[test]
    fn semver_shape_check() {
        assert!(is_plausible_semver("1.2.3"));
        assert!(is_plausible_semver("1.2.3-beta.1"));
        assert!(is_plausible_semver("1.2.3+build.7"));
        assert!(is_plausible_semver("1.2.3-beta+build"));
        assert!(!is_plausible_semver("1.2"));
        assert!(!is_plausible_semver("latest"));
        assert!(!is_plausible_semver(""));
        assert!(!is_plausible_semver("v1.2.3"));
    }

    fn service_manifest_json(id: &str, command: &str) -> String {
        serde_json::json!({
            "id": "svc-plugin",
            "name": "Svc Plugin",
            "version": "1.0.0",
            "provides": {
                "services": [
                    {"id": id, "command": command}
                ]
            }
        })
        .to_string()
    }

    #[test]
    fn parses_and_validates_minimal_service_entry() {
        let json = service_manifest_json("svc", PLATFORM_BIN_TOKEN);
        let manifest = PluginManifest::parse_str(&json).unwrap();
        manifest.validate().expect("minimal service entry is valid");
        let entry = &manifest.provides.services[0];
        assert!(entry.enabled);
        assert_eq!(entry.health_check.kind, HealthCheckKind::ProcessAlive);
        assert_eq!(entry.graceful_shutdown.signal, ShutdownSignal::Term);
        assert!(manifest.uses_platform_bin_token());
    }

    #[test]
    fn rejects_service_command_that_is_not_exactly_the_platform_bin_token() {
        for bad_command in ["/usr/bin/env", "nova", "${platform_bin} --serve", ""] {
            let json = service_manifest_json("svc", bad_command);
            let manifest = PluginManifest::parse_str(&json).unwrap();
            let error = manifest
                .validate()
                .expect_err("non-token service command must be rejected");
            assert!(matches!(error, PluginError::InvalidManifest(_)));
        }
    }

    #[test]
    fn rejects_duplicate_service_ids() {
        let json = serde_json::json!({
            "id": "svc-plugin",
            "name": "Svc",
            "version": "1.0.0",
            "provides": {
                "services": [
                    {"id": "a", "command": PLATFORM_BIN_TOKEN},
                    {"id": "a", "command": PLATFORM_BIN_TOKEN}
                ]
            }
        })
        .to_string();
        let manifest = PluginManifest::parse_str(&json).unwrap();
        let error = manifest
            .validate()
            .expect_err("duplicate service id should fail");
        assert!(error.to_string().contains("duplicate service id"));
    }

    #[test]
    fn rejects_tcp_and_http_health_check_missing_target() {
        for kind in ["tcp", "http"] {
            let json = serde_json::json!({
                "id": "svc-plugin",
                "name": "Svc",
                "version": "1.0.0",
                "provides": {
                    "services": [
                        {"id": "a", "command": PLATFORM_BIN_TOKEN, "health_check": {"kind": kind}}
                    ]
                }
            })
            .to_string();
            let manifest = PluginManifest::parse_str(&json).unwrap();
            let error = manifest
                .validate()
                .expect_err("tcp/http health_check without a target should fail");
            assert!(error.to_string().contains("target"));
        }
    }

    #[test]
    fn accepts_tcp_health_check_with_target() {
        let json = serde_json::json!({
            "id": "svc-plugin",
            "name": "Svc",
            "version": "1.0.0",
            "provides": {
                "services": [
                    {"id": "a", "command": PLATFORM_BIN_TOKEN, "health_check": {"kind": "tcp", "target": "127.0.0.1:9000"}}
                ]
            }
        })
        .to_string();
        let manifest = PluginManifest::parse_str(&json).unwrap();
        manifest
            .validate()
            .expect("tcp health_check with target is valid");
    }

    #[test]
    fn services_missing_artifact_for_a_supported_platform_is_rejected() {
        // Mirrors `rejects_platform_bin_plugin_missing_an_artifact_for_a_supported_platform`
        // but via `provides.services` instead of `provides.mcp_servers` — the
        // artifacts/platform cross-check must cover services too (issue #479).
        let json = serde_json::json!({
            "id": "svc-plugin",
            "name": "Svc",
            "version": "1.0.0",
            "provides": {
                "services": [
                    {"id": "a", "command": PLATFORM_BIN_TOKEN}
                ]
            },
            "artifacts": {
                "macos": {"url": "https://example.com/x-macos.tar.gz", "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                "windows": {"url": "https://example.com/x-windows.zip", "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
            }
        })
        .to_string();
        let manifest = PluginManifest::parse_str(&json).unwrap();
        let error = manifest
            .validate()
            .expect_err("missing linux artifact for a service-only plugin should fail");
        assert!(error.to_string().contains("linux"));
    }

    #[test]
    fn resolve_service_entry_substitutes_tokens_and_pins_command_to_platform_bin() {
        let json = serde_json::json!({
            "id": "svc-plugin",
            "name": "Svc",
            "version": "1.0.0",
            "provides": {
                "services": [
                    {
                        "id": "a",
                        "command": PLATFORM_BIN_TOKEN,
                        "args": ["--config", "${plugin_dir}/data"],
                        "cwd": "${plugin_dir}",
                        "env": {"HOME_DIR": "${plugin_dir}/home"}
                    }
                ]
            }
        })
        .to_string();
        let manifest = PluginManifest::parse_str(&json).unwrap();
        manifest.validate().expect("valid");
        let entry = &manifest.provides.services[0];
        let plugin_dir = Path::new("/home/user/.bamboo/plugins/svc-plugin");
        let resolved = entry.resolve(plugin_dir, &manifest.id, Platform::Linux);
        assert_eq!(
            resolved.command,
            PathBuf::from("/home/user/.bamboo/plugins/svc-plugin/bin/linux/svc-plugin")
        );
        assert_eq!(
            resolved.args,
            vec![
                "--config".to_string(),
                "/home/user/.bamboo/plugins/svc-plugin/data".to_string()
            ]
        );
        assert_eq!(resolved.cwd, Some(plugin_dir.to_path_buf()));
        assert_eq!(
            resolved.env.get("HOME_DIR").map(String::as_str),
            Some("/home/user/.bamboo/plugins/svc-plugin/home")
        );
    }

    #[test]
    fn plugin_id_rules() {
        assert!(is_valid_plugin_id("hello-plugin"));
        assert!(is_valid_plugin_id("nova_plugin_2"));
        assert!(!is_valid_plugin_id(""));
        assert!(!is_valid_plugin_id("Hello"));
        assert!(!is_valid_plugin_id("hello plugin"));
        assert!(!is_valid_plugin_id(&"a".repeat(MAX_PLUGIN_ID_LEN + 1)));
    }
}
