//! Configuration management for Bamboo agent
//!
//! This module provides unified configuration types and loading logic for the entire
//! Bamboo agent system. It supports multiple LLM providers, proxy settings,
//! and JSON configuration format.
//!
//! # Configuration File
//!
//! Configuration is stored in `config.json` under the unified data directory
//! (defaults to `${HOME}/.bamboo/`). Environment variables can override file values.
//!
//! # Example (JSON)
//!
//! ```json
//! {
//!   "provider": "anthropic",
//!   "server": {
//!     "port": 9562,
//!     "bind": "127.0.0.1"
//!   },
//!   "providers": {
//!     "anthropic": {
//!       "api_key": "sk-ant-...",
//!       "model": "claude-3-5-sonnet-20241022"
//!     },
//!     "openai": {
//!       "api_key": "sk-...",
//!       "base_url": "https://api.openai.com/v1"
//!     }
//!   }
//! }
//! ```
//!
//! # Priority Order
//!
//! Configuration values are loaded in this order (later overrides earlier):
//! 1. Code defaults (hardcoded default values)
//! 2. Config file values (from `${HOME}/.bamboo/config.json`)
//! 3. Environment variables (e.g., `BAMBOO_PORT`)
//! 4. CLI arguments (e.g., `--port 9000`)
//!
//! # Environment Variables
//!
//! - `BAMBOO_DATA_DIR`: Override data directory location
//! - `BAMBOO_PORT`: Override server port
//! - `BAMBOO_BIND`: Override server bind address
//! - `BAMBOO_PROVIDER`: Override default provider
//! - `BAMBOO_HEADLESS`: Enable headless authentication mode
//! - `BAMBOO_OPENAI_API_KEY` / `BAMBOO_ANTHROPIC_API_KEY` / `BAMBOO_GEMINI_API_KEY`:
//!   Supply a provider's API key from the environment (in-memory only, never
//!   persisted) — for 12-factor / secret-manager / CI deploys without a
//!   plaintext key in config.json.

use anyhow::{Context, Result};
use bamboo_domain::poison::PoisonRecover;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{OnceLock, RwLock};

use crate::keyword_masking::KeywordMaskingConfig;
use crate::model_mapping::{AnthropicModelMapping, GeminiModelMapping};
use bamboo_domain::tool_names::normalize_tool_ref;
use bamboo_domain::ReasoningEffort;

/// A user-managed environment variable that is injected into Bash tool processes.
///
/// Secret entries are encrypted at rest: `value` is empty on disk and populated
/// in memory after hydration from `value_encrypted`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvVarEntry {
    /// Variable name (must match `^[A-Za-z_][A-Za-z0-9_]*$`).
    pub name: String,
    /// Plaintext value – populated in memory after hydration.
    /// For `secret=true` entries this field is empty on disk.
    #[serde(default)]
    pub value: String,
    /// Whether this variable contains sensitive data (token, password, etc.).
    #[serde(default)]
    pub secret: bool,
    /// Encrypted ciphertext (only present on disk for secret entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_encrypted: Option<String>,
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Default work area configuration.
///
/// Allows Bamboo to operate without an explicit initial workspace while still
/// providing a stable fallback directory for relative-path tool execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DefaultWorkAreaConfig {
    /// Optional default filesystem path used when a session has no active workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Access control configuration for password-based UI/API gating.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessControlConfig {
    /// Whether password protection is enabled.
    #[serde(default)]
    pub password_enabled: bool,
    /// Password hash (hex-encoded). Never expose via API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_hash: Option<String>,
    /// Salt used for hashing (hex-encoded). Never expose via API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_salt: Option<String>,
    /// Last update timestamp for auditing / debugging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// v2 (#181): issued per-device tokens. Empty = root-password-only mode
    /// (back-compat with old instances). Each entry stores only the token hash;
    /// the plaintext token is returned to the client once at pairing time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<DeviceCredential>,
}

/// A single paired device's credential (v2-P2 per-device token, #181).
///
/// The server stores only `token_hash` (never the plaintext token). The hash is
/// computed with the SAME construction as the access password — `SHA-256(salt ||
/// token)` — so no new crypto dependency is introduced (`docs/api-v2-transport.md`
/// §4.2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceCredential {
    /// Server-generated stable id: `bamboo_<12 hex>`.
    pub device_id: String,
    /// Human-readable label, e.g. "iPhone 15".
    pub label: String,
    /// `SHA-256(hex_decode(token_salt) || token)`, hex-encoded.
    pub token_hash: String,
    /// Per-device salt (hex-encoded).
    pub token_salt: String,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RFC3339 last-used timestamp (deferred stamping; see PR).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
    /// Whether this device's token has been revoked. A revoked token is rejected
    /// at the handshake/middleware immediately.
    #[serde(default)]
    pub revoked: bool,
}

/// Memory and background summarization configuration.
// No `Eq`: `dedup_gardener_min_score` is an f64 (PartialEq only).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryConfig {
    /// Optional dedicated model for memory/session summarization and reflection.
    /// Falls back to the provider fast model when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_model: Option<String>,
    /// Whether lightweight automatic Dream-style consolidation should run in the
    /// background. Default ON (memory redesign L4): each tick no-ops when there is
    /// no background model configured or no new candidate sessions, so it is free
    /// until there is real work + a model. Set false to opt out.
    #[serde(default = "default_true_auto_dream_enabled")]
    pub auto_dream_enabled: bool,
    /// Seconds between background auto-Dream ticks (default 30 minutes).
    /// Each tick still no-ops when there are no new candidate sessions, so raising
    /// this only lowers how often an active user triggers a real consolidation.
    #[serde(default = "default_auto_dream_interval_secs")]
    pub auto_dream_interval_secs: u64,
    /// Whether project durable-memory index injection is enabled for the main prompt.
    #[serde(
        default = "default_true_memory_project_prompt_injection",
        alias = "memory_project_prompt_injection"
    )]
    pub project_prompt_injection: bool,
    /// Whether automatic relevant durable-memory recall is enabled for the main prompt.
    #[serde(
        default = "default_true_memory_relevant_recall",
        alias = "memory_relevant_recall"
    )]
    pub relevant_recall: bool,
    /// Whether relevant durable-memory recall should rerank lexical shortlist candidates
    /// using the configured memory/background model.
    #[serde(default, alias = "memory_relevant_recall_rerank")]
    pub relevant_recall_rerank: bool,
    /// Whether Dream prompt injection should prefer project Dream and only use global Dream as fallback.
    #[serde(
        default = "default_true_memory_project_first_dream",
        alias = "memory_project_first_dream"
    )]
    pub project_first_dream: bool,
    /// DEPRECATED (memory redesign L3): the "Refine" Dream mode — rewriting the
    /// notebook from its own prior prose — was retired because a self-referential
    /// narrative rewrite drifts from durable truth and silently over-merges. The
    /// notebook is now always a grounded VIEW of the durable memory index (Rebuild)
    /// or a session bootstrap (Incremental). This field is IGNORED; it is retained
    /// only so existing config files that set it still deserialize.
    #[serde(default, alias = "memory_dream_refine_mode")]
    pub dream_refine_mode: bool,
    /// Whether the background "gardener" may use the LLM to split/merge "blob" memories.
    /// Default ON (memory redesign L4). The deterministic blob prefilter is cheap
    /// and each run is bounded by `gardener_max_splits_per_run`; a run that finds
    /// nothing, or finds work but has no background model, spends no tokens. Set
    /// false to opt out.
    #[serde(
        default = "default_true_gardener_enabled",
        alias = "memory_gardener_enabled"
    )]
    pub gardener_enabled: bool,
    /// Seconds between gardener time-triggered runs (default daily). A run may also
    /// fire early when the library grows — see `gardener_volume_trigger`.
    #[serde(default = "default_gardener_interval_secs")]
    pub gardener_interval_secs: u64,
    /// Run the gardener maintenance pass early (before the next time tick) once this
    /// many new durable memories have accumulated since the last run, so pileup is
    /// bounded by growth, not only by the clock (memory redesign L4). 0 disables the
    /// volume trigger (time-only). Per-run caps still bound the work done.
    #[serde(default = "default_gardener_volume_trigger")]
    pub gardener_volume_trigger: usize,
    /// Hard cap on LLM-backed splits per gardener run (cost ceiling per run).
    #[serde(default = "default_gardener_max_splits_per_run")]
    pub gardener_max_splits_per_run: usize,
    /// Minimum `---` accretions for a memory to be a gardener split candidate.
    #[serde(default = "default_gardener_min_sections")]
    pub gardener_min_sections: usize,
    /// Whether the background dedup gardener may use the LLM to consolidate
    /// near-duplicate memories. Default ON (memory redesign L4); bounded by
    /// `dedup_gardener_max_merges_per_run` and no-ops without a model. Set false to
    /// opt out.
    #[serde(
        default = "default_true_dedup_gardener_enabled",
        alias = "memory_dedup_gardener_enabled"
    )]
    pub dedup_gardener_enabled: bool,
    /// Minimum content-keyword Jaccard (0.0–1.0) for two active memories to be
    /// flagged as dedup candidates by the deterministic prefilter.
    #[serde(default = "default_dedup_gardener_min_score")]
    pub dedup_gardener_min_score: f64,
    /// Hard cap on LLM-backed consolidations per dedup gardener run (cost ceiling).
    #[serde(default = "default_dedup_gardener_max_merges_per_run")]
    pub dedup_gardener_max_merges_per_run: usize,
    /// Max RECALLABLE (Active/Stale) memories per scope before the capacity gardener
    /// archives the lowest-value overflow OUT of the recall index (memory redesign
    /// L5 — archive, never delete; reversible). 0 = unbounded (feature OFF, the
    /// default): consequential enough to be opt-in, since L4's dedup already curbs
    /// most growth. `Reference`/`User`/`Feedback` memories are always exempt — so
    /// the effective floor is the count of exempt Active memories in a scope; set
    /// this comfortably above that (a capacity below it is a no-op, not a purge).
    #[serde(default)]
    pub memory_active_capacity: usize,
    /// Hard cap on how many memories the capacity gardener archives per run, so a
    /// large overflow drains gradually instead of in one burst.
    #[serde(default = "default_capacity_max_archivals_per_run")]
    pub capacity_max_archivals_per_run: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            background_model: None,
            auto_dream_enabled: default_true_auto_dream_enabled(),
            auto_dream_interval_secs: default_auto_dream_interval_secs(),
            project_prompt_injection: default_true_memory_project_prompt_injection(),
            relevant_recall: default_true_memory_relevant_recall(),
            relevant_recall_rerank: false,
            project_first_dream: default_true_memory_project_first_dream(),
            dream_refine_mode: false,
            gardener_enabled: default_true_gardener_enabled(),
            gardener_interval_secs: default_gardener_interval_secs(),
            gardener_volume_trigger: default_gardener_volume_trigger(),
            gardener_max_splits_per_run: default_gardener_max_splits_per_run(),
            gardener_min_sections: default_gardener_min_sections(),
            dedup_gardener_enabled: default_true_dedup_gardener_enabled(),
            dedup_gardener_min_score: default_dedup_gardener_min_score(),
            dedup_gardener_max_merges_per_run: default_dedup_gardener_max_merges_per_run(),
            memory_active_capacity: 0,
            capacity_max_archivals_per_run: default_capacity_max_archivals_per_run(),
        }
    }
}

fn default_capacity_max_archivals_per_run() -> usize {
    50
}

fn default_true_auto_dream_enabled() -> bool {
    true
}

fn default_true_gardener_enabled() -> bool {
    true
}

fn default_true_dedup_gardener_enabled() -> bool {
    true
}

/// Fire the gardener maintenance pass early once ~this many new memories accumulate
/// since the last run. Conservative: large enough to avoid thrashing on a few
/// writes, small enough to bound pileup well under a full (daily) interval.
fn default_gardener_volume_trigger() -> usize {
    25
}

fn default_gardener_interval_secs() -> u64 {
    86_400
}

fn default_auto_dream_interval_secs() -> u64 {
    60 * 30
}

fn default_gardener_max_splits_per_run() -> usize {
    8
}

fn default_gardener_min_sections() -> usize {
    5
}

fn default_dedup_gardener_min_score() -> f64 {
    0.6
}

fn default_dedup_gardener_max_merges_per_run() -> usize {
    8
}

fn default_true_memory_project_prompt_injection() -> bool {
    true
}

fn default_true_memory_relevant_recall() -> bool {
    true
}

fn default_true_memory_project_first_dream() -> bool {
    true
}

/// Sub-agent execution settings.
///
/// Sub-agents always run as independent **actor** processes — an isolated OS
/// process with its own context (crash isolation, true parallelism, per-child
/// resource limits). The historical in-process runtime was removed, so there is
/// no longer a runtime toggle (a stray `"runtime"`/`"overrides"` key in an old
/// config is ignored). The worker binary, its arguments, and the discovery
/// directory are derived automatically (the current `bamboo` executable +
/// `subagent-worker`); the expert fields below override them only when you run a
/// custom worker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubagentsConfig {
    /// Maximum actor processes running at once; further spawns wait their
    /// turn. Default: 8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<usize>,
    /// Expert: custom worker binary. Default: the current bamboo executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_bin: Option<String>,
    /// Expert: arguments for the custom worker binary. Default for the
    /// built-in worker: `["subagent-worker"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_args: Option<Vec<String>>,
    /// Expert: discovery fabric directory. Default: a per-user temp dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fabric_dir: Option<String>,
    /// Expert: `"echo"` swaps in a dependency-free smoke executor (no LLM)
    /// to verify the actor chain end-to-end; `"claude_code"` drives the
    /// official Claude Code CLI (see the `claude_code_*` fields below).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    /// `executor = "claude_code"` only: override the `claude` executable.
    /// `None` runs `claude` resolved from `PATH`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_binary: Option<String>,
    /// `executor = "claude_code"` only: `--model` override. `None` omits the
    /// flag (CLI default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_model: Option<String>,
    /// `executor = "claude_code"` only: `--permission-mode` override. `None`
    /// still passes an EXPLICIT `default` to the CLI (issue #443 — the
    /// headless stream-json default is `auto`, which self-approves every
    /// tool and never asks); it does not mean "omit the flag".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_permission_mode: Option<String>,
    /// `executor = "claude_code"` only: `true` lets the child inherit the
    /// invoking user's `~/.claude` MCP servers/skills/settings. `false`/unset
    /// (the default) isolates it (`--strict-mcp-config` +
    /// `--setting-sources project`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_inherit_user_config: Option<bool>,
    /// `executor = "claude_code"` only: extra env var NAMES forwarded
    /// verbatim from this process's env to the child, on top of the fixed
    /// HOME/PATH/SHELL/TERM/LANG/LC_*/TMPDIR/USER/LOGNAME allowlist.
    /// Forwarding `ANTHROPIC_API_KEY` here is an explicit opt-in that flips
    /// billing from the CLI's own subscription auth to the API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_forward_env: Option<Vec<String>>,
    /// The active message-broker endpoint the `ask_agent` tool / sub-agent bus
    /// dials. RUNTIME-ONLY (`#[serde(skip)]`): never read from nor written to
    /// `config.json`. It is populated in memory each boot by `maybe_embed_broker`
    /// — either from a user-managed external broker in `<data_dir>/broker.json`,
    /// or from the freshly-embedded in-process broker (whose ephemeral loopback
    /// port must NEVER be persisted, else a later boot dials a dead port).
    #[serde(skip)]
    pub broker: Option<BrokerClientConfig>,
    /// Remote placements: pin specific sub-agent roles to resident workers
    /// reached over `wss://` instead of a locally-spawned subprocess
    /// (remote-actor-plan §3.4 / P1.5, #193). Empty (the default) keeps every
    /// role on the local path — fully back-compatible: an old config with no
    /// `remote_placements` key deserializes to an empty vec.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remote_placements: Vec<RemoteActorPlacement>,
    /// Schedulable placements: route specific sub-agent roles to a LIVE worker
    /// resolved from the agent registry at run time, instead of a locally-spawned
    /// subprocess (remote-actor-plan §3.4 / P2b, #181). Unlike `remote_placements`
    /// (a fixed endpoint), a schedulable placement names a logical `pool` and a
    /// `registry_url`; the engine queries the registry for live workers in that
    /// pool and picks one. Empty (the default) keeps every role on the local path
    /// — fully back-compatible: an old config with no `schedulable_placements` key
    /// deserializes to an empty vec.
    ///
    /// PRECEDENCE: if a role appears in BOTH `remote_placements` and
    /// `schedulable_placements`, the fixed remote placement wins (it is resolved
    /// first in `build_spec`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schedulable_placements: Vec<SchedulablePlacement>,
}

/// Routes a single sub-agent role to a registry-scheduled worker (remote-actor-
/// plan §3.4 / P2b, #181). A child whose `subagent_type` matches `role` is run on
/// a LIVE worker chosen from the agent registry: the engine builds a
/// `RegistryFabric` at `registry_url`, lists live workers (the registry already
/// excludes expired leases), filters to those whose `role` == `pool`, picks one
/// (round-robin), and connects over `wss://` (Bearer-authenticated). If no live
/// worker exists the run ERRORS — a schedulable role NEVER falls back to a local
/// subprocess (that would silently defeat the placement).
///
/// The bearer token is NEVER stored here in the clear: `token_env` names the
/// environment variable that holds it (mirroring `RemoteActorPlacement` /
/// the A2A `auth_ref` pattern), read once at runner-build time and used for BOTH
/// the registry query AND the worker connect. A `token_env` that is set-but-unset
/// at build time fails SAFE — the placement is skipped and the role falls back to
/// Local rather than querying/connecting unauthenticated.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchedulablePlacement {
    /// Sub-agent role this targets (matches the child session's
    /// `metadata["subagent_type"]`).
    pub role: String,
    /// Logical pool name — the registry `role` to query for live workers.
    pub pool: String,
    /// VESTIGIAL (Phase 3 retired the HTTP agent registry — pools are now bus
    /// roles resolved via broker presence). Kept for config back-compat; ignored
    /// by the resolver. Optional so a placement is just `{role, pool}`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub registry_url: String,
    /// Env var holding the bearer token (NOT the raw token — mirrors A2A
    /// `auth_ref`). Used for BOTH the registry query and the worker connect.
    /// `None` ⇒ query/connect without a bearer (trusted link only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    /// PEM file pinning a self-signed worker/registry cert. `None` ⇒ default
    /// webpki roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_file: Option<String>,
}

/// Pins a single sub-agent role to a remote resident worker (remote-actor-plan
/// §3.4 / P1.5). A child whose `subagent_type` matches `role` is connected over
/// `wss://` to `endpoint` (Bearer-authenticated) instead of being spawned as a
/// local subprocess. No role match ⇒ that child stays on the local path.
///
/// The bearer token is NEVER stored here in the clear: `token_env` names the
/// environment variable that holds it (mirroring the A2A `auth_ref` pattern),
/// read once at runner-build time. A `token_env` that is set-but-unset at build
/// time fails SAFE — the placement is skipped and the role falls back to Local
/// rather than connecting unauthenticated.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RemoteActorPlacement {
    /// Sub-agent role this targets (matches the child session's
    /// `metadata["subagent_type"]`).
    pub role: String,
    /// Resident worker endpoint, e.g. `wss://gpu-host:8443` (or `ws://` only on
    /// a trusted/loopback link).
    pub endpoint: String,
    /// Env var holding the bearer token (NOT the raw token — mirrors A2A
    /// `auth_ref`). `None` ⇒ connect without a bearer (trusted link only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    /// PEM file pinning a self-signed worker cert. `None` ⇒ default webpki roots
    /// (or plaintext `ws://`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_file: Option<String>,
}

/// How to reach the central sub-agent message broker (`bamboo broker serve`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BrokerClientConfig {
    /// Broker WebSocket endpoint, e.g. `ws://broker-host:9600`.
    pub endpoint: String,
    /// Bearer token presented in the broker handshake.
    ///
    /// Secret: encrypted at rest in `token_encrypted`; this plaintext field is
    /// empty on disk and hydrated in memory on load (mirrors [`EnvVarEntry`]).
    #[serde(default)]
    pub token: String,
    /// Encrypted ciphertext of `token` (the at-rest representation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_encrypted: Option<String>,
}

/// Native desktop (OS-notification) delivery channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DesktopChannelConfig {
    /// `None` = auto: on when Bamboo runs as a standalone `bamboo serve`
    /// process, off when spawned as a sidecar under `--parent-pid` (a native
    /// shell such as Bodhi owns notification UX in that mode — desktop
    /// notifications from both the sidecar and the shell would double-fire).
    /// `Some(_)` is an explicit user override of that default in either
    /// direction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

/// [ntfy.sh](https://ntfy.sh) push notification channel (self-hostable).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NtfyChannelConfig {
    #[serde(default)]
    pub enabled: bool,
    /// ntfy server base URL (public ntfy.sh or a self-hosted instance).
    #[serde(default = "default_ntfy_base_url")]
    pub base_url: String,
    /// Topic to publish to. Priority mapping from notification category is
    /// left to the delivery sink, not configured here.
    #[serde(default)]
    pub topic: String,
    /// Access token for a protected/self-hosted ntfy instance (public ntfy.sh
    /// topics need none).
    ///
    /// Secret: encrypted at rest in `token_encrypted`; this plaintext field is
    /// never serialized and is hydrated in memory on load (mirrors
    /// [`EnvVarEntry`] / [`BrokerClientConfig::token`]).
    #[serde(default, skip_serializing)]
    pub token: Option<String>,
    /// Encrypted ciphertext of `token` (the at-rest representation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_encrypted: Option<String>,
}

impl Default for NtfyChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: default_ntfy_base_url(),
            topic: String::new(),
            token: None,
            token_encrypted: None,
        }
    }
}

fn default_ntfy_base_url() -> String {
    "https://ntfy.sh".to_string()
}

/// [Bark](https://github.com/Finb/Bark) iOS push notification channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BarkChannelConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Bark server base URL (public api.day.app or a self-hosted instance).
    #[serde(default = "default_bark_base_url")]
    pub base_url: String,
    /// Bark device key identifying the target iOS device.
    ///
    /// Secret: encrypted at rest in `device_key_encrypted`; this plaintext
    /// field is never serialized and is hydrated in memory on load (mirrors
    /// [`NtfyChannelConfig::token`]).
    #[serde(default, skip_serializing)]
    pub device_key: Option<String>,
    /// Encrypted ciphertext of `device_key` (the at-rest representation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_key_encrypted: Option<String>,
}

impl Default for BarkChannelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: default_bark_base_url(),
            device_key: None,
            device_key_encrypted: None,
        }
    }
}

fn default_bark_base_url() -> String {
    "https://api.day.app".to_string()
}

/// Notification delivery channels: native desktop plus push-relay services.
///
/// Additive/back-compat: an absent `notifications` key in `config.json`
/// deserializes to the defaults (desktop auto, ntfy/bark disabled).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NotificationsConfig {
    #[serde(default)]
    pub desktop: DesktopChannelConfig,
    #[serde(default)]
    pub ntfy: NtfyChannelConfig,
    #[serde(default)]
    pub bark: BarkChannelConfig,
}

/// One IM-platform bridge configured under `[[connect.platforms]]` —
/// bamboo-connect (issue #452 / epic #447): drives a bamboo session from an
/// external chat platform (Telegram first).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectPlatformConfig {
    /// Platform adapter selector, e.g. `"telegram"`. Unrecognized values are
    /// skipped (with a startup warning) rather than failing config load —
    /// forward-compatible with future adapters (Feishu/Slack).
    #[serde(rename = "type")]
    pub platform_type: String,
    /// Platform bot/API token.
    ///
    /// Secret: encrypted at rest in `token_encrypted`; this plaintext field is
    /// never serialized and is hydrated in memory on load (mirrors
    /// [`NtfyChannelConfig::token`] / [`BarkChannelConfig::device_key`]).
    #[serde(default, skip_serializing)]
    pub token: Option<String>,
    /// Encrypted ciphertext of `token` (the at-rest representation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_encrypted: Option<String>,
    /// Platform-scoped user ids allowed to drive a session. Deliberately
    /// STRICTER than the general secret-mask precedents: an EMPTY list means
    /// deny-all (every inbound message is rejected), not allow-all — a
    /// startup warning is logged when a platform has no allowed users.
    #[serde(default)]
    pub allow_from: Vec<String>,
    /// User ids allowed to run privileged/admin commands. Parsed from day one
    /// but UNUSED in the MVP (#452) — no admin commands exist yet; reserved
    /// for the approvals/admin phase of epic #447.
    #[serde(default)]
    pub admin_from: Vec<String>,
}

/// bamboo-connect platform bridges: drive bamboo sessions from IM platforms.
/// Additive/back-compat: an absent `connect` key in `config.json`
/// deserializes to an empty platform list — fully inert (#452).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ConnectConfig {
    #[serde(default)]
    pub platforms: Vec<ConnectPlatformConfig>,
}

fn connect_config_is_empty(config: &ConnectConfig) -> bool {
    config.platforms.is_empty()
}

/// One publisher key trusted to sign plugin bundles.
///
/// `algorithm` is a plain string (not an enum) so an unrecognized future value
/// in an old/new config just never matches during verification rather than
/// failing to deserialize — additive/forward-compatible, matching this
/// crate's other config sections. Only `"ed25519"` is understood today.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrustedKey {
    /// Human-readable label (surfaced in logs/CLI output); purely descriptive.
    pub label: String,
    /// Signature algorithm. Only `"ed25519"` is currently verified.
    pub algorithm: String,
    /// Hex-encoded public key (32 raw bytes for ed25519).
    pub public_key: String,
}

/// Nova's official plugin-signing key, trusted by default so an out-of-the-box
/// `bamboo plugin install <official nova release url>` needs no
/// `--allow-unsigned` once nova's release CI signs the bundle.
fn default_trusted_keys() -> Vec<TrustedKey> {
    vec![TrustedKey {
        label: "nova (bigduu official)".to_string(),
        algorithm: "ed25519".to_string(),
        public_key: "e3c429e1be50098b12c6f45737abf457189b668535875b5b3e2b4349be86ea59".to_string(),
    }]
}

/// Default trusted host+path prefix: the `bigduu` GitHub org/user's own repos
/// (e.g. `github.com/bigduu/Nova/releases/...`).
fn default_trusted_hosts() -> Vec<String> {
    vec!["github.com/bigduu/".to_string()]
}

/// Plugin URL-install source-trust policy: a host allowlist (is the SOURCE
/// authorized?) plus ed25519 publisher keys (is the PUBLISHER authentic?).
/// This stacks on top of the checksum layer already enforced in
/// `bamboo_plugin::registry::PluginSource::Url` (are the BYTES what the
/// caller expected?) — see `bamboo-server`'s `plugin_source.rs` for where all
/// three layers are enforced together. A pasted checksum alone cannot
/// establish source trust (an attacker who controls the page a checksum was
/// copied from can just publish a checksum for their own tampered bundle);
/// the host allowlist and signature checks close that gap.
///
/// Both fields are user-editable (`config.json`, or the config-set HTTP/CLI
/// path) so an operator can add their own trusted hosts/keys. Additive/
/// back-compat: an absent `plugin_trust` key deserializes to
/// [`PluginTrustConfig::default`] (the built-in defaults below), not an empty
/// policy — so a fresh install can trust the official nova plugin out of the
/// box.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginTrustConfig {
    /// Host+path prefixes a `url` plugin source may be fetched from without
    /// `--allow-untrusted-host`, e.g. `"github.com/bigduu/"` (a bare host
    /// with no `/`, e.g. `"example.com"`, matches any path on that exact
    /// host). Each entry is split into a host component and a path-prefix
    /// component and matched on PARSED URL components — whole-host equality
    /// plus a `/`-boundary path-prefix check, never a raw string
    /// `starts_with` — see [`is_host_trusted`] for the precise rule and why
    /// (it closes a domain-gluing bypass like `example.com` matching
    /// `example.com.evil.com`, and a sibling-path bypass like
    /// `github.com/bigduu` matching `github.com/bigduu-evil/x`).
    #[serde(default = "default_trusted_hosts")]
    pub trusted_hosts: Vec<String>,
    /// Publisher keys a bundle's `.sig` signature may verify against without
    /// `--allow-unsigned`.
    #[serde(default = "default_trusted_keys")]
    pub trusted_keys: Vec<TrustedKey>,
}

impl Default for PluginTrustConfig {
    fn default() -> Self {
        Self {
            trusted_hosts: default_trusted_hosts(),
            trusted_keys: default_trusted_keys(),
        }
    }
}

impl PluginTrustConfig {
    /// True when `url` is `https` and its host+path match one of
    /// `trusted_hosts` on parsed URL components (host compared
    /// case-insensitively as a WHOLE string, path matched on a `/` boundary
    /// — see the free function [`is_host_trusted`] for the precise rule; an
    /// unparseable URL or a non-`https` scheme is never trusted).
    pub fn is_host_trusted(&self, url: &str) -> bool {
        is_host_trusted(url, &self.trusted_hosts)
    }
}

/// One `trusted_hosts` entry, split into its host and path-prefix
/// components (see [`is_host_trusted`]). `path_prefix` is empty for a
/// bare-host entry (e.g. `"example.com"`, meaning "any path on this exact
/// host") or starts with `/` (e.g. `"/bigduu/"` from `"github.com/bigduu/"`).
struct TrustedHostEntry<'a> {
    host: &'a str,
    path_prefix: &'a str,
}

/// Split a raw `trusted_hosts` entry at its first `/` into host + path
/// components. Entries are compared against ALREADY-lowercased input by the
/// caller ([`is_host_trusted`]), so this does no case normalization itself.
fn parse_trusted_host_entry(entry: &str) -> TrustedHostEntry<'_> {
    match entry.find('/') {
        Some(index) => TrustedHostEntry {
            host: &entry[..index],
            path_prefix: &entry[index..],
        },
        None => TrustedHostEntry {
            host: entry,
            path_prefix: "",
        },
    }
}

/// True when `path` matches `prefix` on a `/` path-component boundary, never
/// on a raw byte prefix: exactly equal to `prefix`, or `prefix` ends in `/`
/// and `path` starts with it, or the character in `path` immediately
/// following `prefix` is `/`. An empty `prefix` (a bare-host trusted_hosts
/// entry) matches any path.
///
/// This is what stops a sibling path from passing as a prefix match — e.g.
/// entry `github.com/bigduu` (no trailing slash) must NOT match
/// `github.com/bigduu-evil/x`: `"/bigduu-evil/x"` starts with `"/bigduu"` as
/// raw bytes, but the character right after the prefix is `-`, not `/`, so
/// this correctly refuses it.
fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() || path == prefix {
        return true;
    }
    if prefix.ends_with('/') {
        return path.starts_with(prefix);
    }
    path.starts_with(prefix) && path.as_bytes().get(prefix.len()) == Some(&b'/')
}

/// Free function backing [`PluginTrustConfig::is_host_trusted`] — exposed
/// separately so callers (and tests) can check a candidate host list without
/// constructing a full [`PluginTrustConfig`]/[`Config`].
///
/// Matches on PARSED URL COMPONENTS, not a raw string prefix: `url` must be
/// `https`, its `host_str()` (already correct for userinfo — `user@host` or
/// `host@evil.com`-style tricks resolve to the real host, not a decoy — and
/// for an explicit port, which `host_str()` excludes) must EQUAL a
/// `trusted_hosts` entry's host component (case-insensitively, WHOLE host —
/// never a `starts_with`), and its (already dot-segment-normalized by
/// `Url::parse`) path must match that entry's path-prefix component on a `/`
/// boundary (see [`path_matches_prefix`]). A bare-host entry (no `/` in it)
/// has an empty path-prefix, so it matches any path but ONLY on that exact
/// host.
///
/// A prior raw-`starts_with` implementation was defeated by (1) gluing a
/// trusted bare host into a longer attacker-controlled one, e.g.
/// `trusted.example.com` matching `trusted.example.com.evil.com` /
/// `trusted.example.comevil.com`, and (2) a sibling path prefix, e.g.
/// `github.com/bigduu` (no trailing slash) matching
/// `github.com/bigduu-evil/x`. Component-wise matching closes both: host
/// comparison is whole-string equality (no gluing possible), and the path
/// check enforces a `/` boundary (no sibling-prefix bypass possible).
pub fn is_host_trusted(url: &str, trusted_hosts: &[String]) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    let path = parsed.path();

    trusted_hosts.iter().any(|raw_entry| {
        let entry = raw_entry.trim().to_ascii_lowercase();
        let parsed_entry = parse_trusted_host_entry(&entry);
        host == parsed_entry.host && path_matches_prefix(path, parsed_entry.path_prefix)
    })
}

/// Main configuration structure for Bamboo agent
///
/// Contains all settings needed to run the agent, including provider credentials,
/// proxy settings, model selection, and server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP proxy URL (e.g., `http://proxy.example.com:8080`)
    #[serde(default)]
    pub http_proxy: String,
    /// HTTPS proxy URL (e.g., `https://proxy.example.com:8080`)
    #[serde(default)]
    pub https_proxy: String,
    /// Proxy authentication credentials
    ///
    /// Note: this is kept in-memory only. On disk we store `proxy_auth_encrypted`.
    #[serde(skip_serializing)]
    pub proxy_auth: Option<ProxyAuth>,
    /// Encrypted proxy authentication credentials (nonce:ciphertext)
    ///
    /// This is the at-rest storage representation. When present, Bamboo will
    /// decrypt it into `proxy_auth` at load time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_auth_encrypted: Option<String>,
    /// Deprecated: Use `providers.copilot.headless_auth` instead
    #[serde(default)]
    pub headless_auth: bool,

    /// Default LLM provider to use (e.g., "anthropic", "openai", "gemini", "copilot")
    #[serde(default = "default_provider")]
    pub provider: String,

    /// Default model assignments (used when features.provider_model_ref is enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<DefaultsConfig>,

    /// Provider-specific configurations (legacy, single-instance per type).
    #[serde(default)]
    pub providers: ProviderConfigs,

    /// Multi-instance provider configurations keyed by instance id.
    ///
    /// When `provider_instances` is non-empty, the registry and router prefer
    /// instance ids as routing keys. Legacy `providers` / `provider` fields are
    /// still supported for backward compatibility; see
    /// [`Config::synthesize_legacy_instances`].
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub provider_instances: HashMap<String, ProviderInstanceConfig>,

    /// The default provider instance id used when a request does not specify one.
    ///
    /// When set, this takes precedence over the legacy `provider` field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider_instance: Option<String>,

    /// HTTP server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Global keyword masking configuration.
    ///
    /// Previously persisted in `keyword_masking.json` (now unified into `config.json`).
    #[serde(default)]
    pub keyword_masking: KeywordMaskingConfig,

    /// Anthropic model mapping configuration.
    ///
    /// Previously persisted in `anthropic-model-mapping.json` (now unified into `config.json`).
    #[serde(default)]
    pub anthropic_model_mapping: AnthropicModelMapping,

    /// Gemini model mapping configuration.
    ///
    /// Previously persisted in `gemini-model-mapping.json` (now unified into `config.json`).
    #[serde(default)]
    pub gemini_model_mapping: GeminiModelMapping,

    /// Request preflight hooks.
    ///
    /// These hooks can inspect and rewrite outgoing requests before they are sent upstream
    /// (e.g. image fallback behavior for text-only models).
    #[serde(default)]
    pub hooks: HooksConfig,

    /// Global tool toggles.
    ///
    /// Any tool listed in `disabled` is omitted from the tool schemas sent to the LLM.
    #[serde(default, skip_serializing_if = "ToolsConfig::is_empty")]
    pub tools: ToolsConfig,

    /// Global skill toggles.
    ///
    /// Any skill listed in `disabled` is excluded from skill context construction and
    /// cannot be loaded through the skill runtime tools.
    #[serde(default, skip_serializing_if = "SkillsConfig::is_empty")]
    pub skills: SkillsConfig,

    /// User-managed environment variables injected into Bash tool processes.
    ///
    /// Secret entries are encrypted at rest; plaintext values are hydrated in memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_vars: Vec<EnvVarEntry>,

    /// Default work area used when a session has no explicit active workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_work_area: Option<DefaultWorkAreaConfig>,

    /// Access control / password gate configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_control: Option<AccessControlConfig>,

    /// Feature flags for incremental rollout.
    #[serde(default)]
    pub features: FeatureFlags,

    /// Memory/background summarization settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryConfig>,

    /// Sub-agent execution settings.
    ///
    /// Sub-agents ALWAYS run as independent actor subprocesses (crash isolation,
    /// true parallelism) — the in-process runtime was removed, so there is no
    /// `runtime` toggle (a stray `runtime`/`overrides` key in an old config is
    /// silently ignored). Most users need nothing here; the fields below
    /// (`max_concurrent`, `broker`, remote/schedulable placements) are advanced.
    #[serde(default)]
    pub subagents: SubagentsConfig,

    /// Remote Cluster Fabric: operator-managed nodes & clusters for deploying
    /// `broker-agent` workers locally or over SSH. Additive/back-compat: absent
    /// ⇒ empty. SSH secrets are encrypted at rest (see [`crate::cluster_fabric`]).
    #[serde(
        default,
        skip_serializing_if = "crate::cluster_fabric::ClusterFabricConfig::is_empty"
    )]
    pub cluster_fabric: crate::cluster_fabric::ClusterFabricConfig,

    /// MCP server configuration.
    ///
    /// Previously persisted in `mcp.json` (now unified into `config.json`).
    // On disk we use the mainstream `mcpServers` key (matching Claude Desktop / MCP ecosystem
    // conventions). We still accept the legacy `mcp` key for backward compatibility.
    #[serde(default, rename = "mcpServers", alias = "mcp")]
    pub mcp: bamboo_domain::mcp_config::McpConfig,

    /// Notification delivery channels (desktop + push-relay services).
    /// Secrets (ntfy token, Bark device key) are encrypted at rest — see
    /// [`Config::hydrate_notifications_from_encrypted`] /
    /// [`Config::refresh_notifications_encrypted`].
    #[serde(default)]
    pub notifications: NotificationsConfig,

    /// bamboo-connect IM-platform bridges (Telegram first, #452 / epic #447).
    /// Secrets (each platform's `token`) are encrypted at rest — see
    /// [`Config::hydrate_connect_platform_tokens_from_encrypted`] /
    /// [`Config::refresh_connect_platform_tokens_encrypted`].
    #[serde(default, skip_serializing_if = "connect_config_is_empty")]
    pub connect: ConnectConfig,

    /// Plugin URL-install source-trust policy (host allowlist + ed25519
    /// publisher keys). See [`PluginTrustConfig`]'s docs for the three-layer
    /// model this stacks with the checksum layer.
    #[serde(default)]
    pub plugin_trust: PluginTrustConfig,

    /// Extension fields stored at the root of `config.json`.
    ///
    /// This keeps the config forward-compatible and allows unrelated subsystems
    /// (e.g. setup UI state) to persist their own keys without getting dropped by
    /// typed (de)serialization.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Container for provider-specific configurations
///
/// Each field is optional, allowing users to configure only the providers they need.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderConfigs {
    /// OpenAI provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai: Option<OpenAIConfig>,
    /// Anthropic provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic: Option<AnthropicConfig>,
    /// Google Gemini provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gemini: Option<GeminiConfig>,
    /// GitHub Copilot provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copilot: Option<CopilotConfig>,
    /// Bodhi proxy provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bodhi: Option<BodhiConfig>,

    /// Preserve unknown provider keys (forward compatibility).
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Feature flags for incremental rollout of new subsystems.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureFlags {
    /// Enable the ProviderModelRef system (multi-provider + unified model selection).
    #[serde(default)]
    pub provider_model_ref: bool,
    /// Enable MiniLoop-based complexity evaluation and dynamic per-round model switching.
    #[serde(default)]
    pub dynamic_model_routing: bool,
}

/// Default model assignments for specific capabilities.
///
/// Used when `features.provider_model_ref` is enabled.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DefaultsConfig {
    pub chat: bamboo_domain::ProviderModelRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast: Option<bamboo_domain::ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_summary: Option<bamboo_domain::ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision: Option<bamboo_domain::ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_background: Option<bamboo_domain::ProviderModelRef>,
    /// Model for planning/coordination tasks (task decomposition, architecture).
    /// Falls back to `chat` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning: Option<bamboo_domain::ProviderModelRef>,
    /// Model for search/navigation tasks (grep, file listing, symbol resolution).
    /// Falls back to `fast` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<bamboo_domain::ProviderModelRef>,
    /// Model for code review tasks.
    /// Falls back to `chat` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_review: Option<bamboo_domain::ProviderModelRef>,
    /// Default model for child SubAgent runs.
    /// Falls back to `fast`, then `chat` when unset.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "sub_session"
    )]
    pub sub_agent: Option<bamboo_domain::ProviderModelRef>,
    /// Per-subagent-type model overrides.
    /// Key = subagent_type (e.g. "researcher", "coder"), Value = ProviderModelRef.
    /// Falls back to `chat` when no match is found for a given type.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub subagent_models: HashMap<String, bamboo_domain::ProviderModelRef>,
}

/// Request hook configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksConfig {
    /// Image fallback behavior for OpenAI-compatible requests (chat/responses).
    #[serde(default)]
    pub image_fallback: ImageFallbackHookConfig,
}

/// Request override configuration for provider-specific HTTP behavior.
///
/// Overrides are merged in this order (later wins):
/// 1. `common`
/// 2. `endpoints[endpoint]`
/// 3. matching `rules` (sorted by specificity)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RequestOverridesConfig {
    /// Overrides applied to all endpoints.
    #[serde(default, skip_serializing_if = "RequestScopeOverride::is_empty")]
    pub common: RequestScopeOverride,
    /// Endpoint-specific overrides (`chat_completions`, `responses`, `messages`, etc.).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub endpoints: BTreeMap<String, RequestScopeOverride>,
    /// Model-conditional overrides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ModelRequestRule>,
}

/// A conditional override rule matching a model pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRequestRule {
    /// Model pattern (exact: `gpt-4o`, prefix wildcard: `gpt-5*`).
    pub model_pattern: String,
    /// Optional endpoint constraint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Overrides applied when this rule matches.
    #[serde(default, skip_serializing_if = "RequestScopeOverride::is_empty")]
    pub scope: RequestScopeOverride,
}

/// Request overrides applied in a specific scope.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RequestScopeOverride {
    /// Extra or overridden HTTP headers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, TemplateExpr>,
    /// JSON body patch operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body_patch: Vec<BodyPatch>,
}

impl RequestScopeOverride {
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty() && self.body_patch.is_empty()
    }
}

/// Body patch operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BodyPatch {
    /// Target path (`foo.bar.0` or `/foo/bar/0`).
    pub path: String,
    /// Operation type.
    #[serde(default)]
    pub op: BodyPatchOp,
    /// Value for `set` operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<PatchValue>,
}

/// Supported body patch operations.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyPatchOp {
    #[default]
    Set,
    Remove,
}

/// Body patch value: either a template expression or a raw JSON value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PatchValue {
    Template(TemplateExpr),
    Json(Value),
}

/// String template expression used by headers/body patch values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TemplateExpr {
    /// Shorthand literal value.
    Literal(String),
    /// Structured template expression.
    Structured(TemplateExprSpec),
}

/// Structured template expression.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TemplateExprSpec {
    /// Literal string value.
    Literal { value: String },
    /// Reference a value from Bamboo env vars.
    EnvRef {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fallback: Option<String>,
    },
    /// Generate a runtime value.
    Generated { generator: GeneratedValue },
    /// Format string with placeholders (`{env:NAME}`, `{uuid}`, `{unix_ms}`).
    Format { template: String },
}

/// Supported generated value kinds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeneratedValue {
    Uuid,
    UnixMs,
}

/// Global tool toggle configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolsConfig {
    /// Tool names that are disabled globally.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<String>,
}

impl ToolsConfig {
    fn is_empty(&self) -> bool {
        self.disabled.is_empty()
    }
}

/// Global skill toggle configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SkillsConfig {
    /// Skill IDs that are disabled globally.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<String>,
}

impl SkillsConfig {
    fn is_empty(&self) -> bool {
        self.disabled.is_empty()
    }
}

/// When a request contains image parts but the effective provider path is text-only,
/// we can either:
/// - error fast (preferred for strict setups), or
/// - degrade gracefully by replacing images with a placeholder text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageFallbackHookConfig {
    #[serde(default = "default_true_hooks")]
    pub enabled: bool,

    /// "placeholder" (default) or "error"
    #[serde(default = "default_image_fallback_mode")]
    pub mode: String,
}

impl Default for ImageFallbackHookConfig {
    fn default() -> Self {
        Self {
            enabled: default_true_hooks(),
            mode: default_image_fallback_mode(),
        }
    }
}

fn default_image_fallback_mode() -> String {
    "placeholder".to_string()
}

fn default_true_hooks() -> bool {
    // Default to disabled so image inputs are preserved unless the user explicitly
    // opts into fallback rewriting (placeholder/error/ocr).
    false
}

/// OpenAI provider configuration
///
/// # Example
///
/// ```json
/// "openai": {
///   "api_key": "sk-...",
///   "base_url": "https://api.openai.com/v1",
///   "model": "gpt-4"
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenAIConfig {
    /// OpenAI API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted OpenAI API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// True when `api_key` was supplied via a `BAMBOO_*_API_KEY` env var.
    /// Such keys are runtime-only and MUST NOT be re-encrypted into
    /// `api_key_encrypted` on save (that would bake the secret into
    /// config.json). Not (de)serialized. (#253)
    #[serde(skip)]
    pub api_key_from_env: bool,
    /// Custom API base URL (for Azure or self-hosted deployments)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gpt-4", "gpt-3.5-turbo")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation and summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for OpenAI requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Models that must use the OpenAI Responses API upstream (instead of chat/completions).
    ///
    /// Example:
    /// ```json
    /// "responses_only_models": ["gpt-5.3-codex", "gpt-5*"]
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.openai`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Anthropic provider configuration
///
/// # Example
///
/// ```json
/// "anthropic": {
///   "api_key": "sk-ant-...",
///   "model": "claude-3-5-sonnet-20241022",
///   "max_tokens": 4096
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnthropicConfig {
    /// Anthropic API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted Anthropic API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// True when `api_key` was supplied via a `BAMBOO_*_API_KEY` env var.
    /// Such keys are runtime-only and MUST NOT be re-encrypted into
    /// `api_key_encrypted` on save (that would bake the secret into
    /// config.json). Not (de)serialized. (#253)
    #[serde(skip)]
    pub api_key_from_env: bool,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "claude-3-5-sonnet-20241022")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Maximum tokens in model response
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Default reasoning effort for Anthropic requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.anthropic`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Google Gemini provider configuration
///
/// # Example
///
/// ```json
/// "gemini": {
///   "api_key": "AIza...",
///   "model": "gemini-2.0-flash-exp"
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeminiConfig {
    /// Google AI API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted Google AI API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// True when `api_key` was supplied via a `BAMBOO_*_API_KEY` env var.
    /// Such keys are runtime-only and MUST NOT be re-encrypted into
    /// `api_key_encrypted` on save (that would bake the secret into
    /// config.json). Not (de)serialized. (#253)
    #[serde(skip)]
    pub api_key_from_env: bool,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gemini-2.0-flash-exp")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for Gemini requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.gemini`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GitHub Copilot provider configuration
///
/// # Example
///
/// ```json
/// "copilot": {
///   "enabled": true,
///   "headless_auth": false,
///   "model": "gpt-4o"
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CopilotConfig {
    /// Whether Copilot provider is enabled
    #[serde(default)]
    pub enabled: bool,
    /// Print login URL to console instead of opening browser
    #[serde(default)]
    pub headless_auth: bool,
    /// Default model to use for Copilot (used when clients request the "default" model)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for Copilot requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Models that must use the OpenAI Responses API upstream (instead of chat/completions).
    ///
    /// This is useful for newer Copilot models that only support Responses-style requests.
    ///
    /// Example:
    /// ```json
    /// "responses_only_models": ["gpt-5.3-codex", "gpt-5*"]
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.copilot`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Bodhi proxy provider configuration.
///
/// Routes LLM requests through a bodhi-server instance so that raw provider
/// API keys never reach the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodhiConfig {
    /// Bodhi server API key (e.g. "bhi_sk_xxx").  In-memory only.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted form of the API key stored on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// Bodhi server base URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Which upstream provider to route through bodhi ("openai", "anthropic", "gemini").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_provider: Option<String>,
    /// Default reasoning effort.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Preserve unknown keys.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Returns the default provider name ("anthropic")
fn default_provider() -> String {
    "anthropic".to_string()
}

// ─── Provider Instance Configuration ──────────────────────────────────

/// Configuration for a single provider instance.
///
/// Multiple instances of the same provider type (e.g. two OpenAI accounts)
/// can coexist. Each instance is identified by a stable `instance_id` that
/// is used as the routing key in [`ProviderModelRef::provider`] and the
/// provider registry.
///
/// # Example (config.json)
///
/// ```json
/// {
///   "provider_instances": {
///     "openai-work": {
///       "provider_type": "openai",
///       "label": "OpenAI (Work)",
///       "api_key": "sk-...",
///       "model": "gpt-4o"
///     },
///     "openai-personal": {
///       "provider_type": "openai",
///       "label": "OpenAI (Personal)",
///       "api_key": "sk-...",
///       "base_url": "https://api.openai.com/v1",
///       "model": "gpt-4o-mini"
///     }
///   },
///   "default_provider_instance": "openai-work"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderInstanceConfig {
    /// Which provider backend this instance targets.
    ///
    /// Must be one of [`AVAILABLE_PROVIDERS`]: `"openai"`, `"anthropic"`,
    /// `"gemini"`, `"copilot"`, `"bodhi"`.
    pub provider_type: String,

    /// Human-readable label shown in the UI / catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// API key (plaintext in memory, encrypted at rest via `api_key_encrypted`).
    #[serde(default, skip_serializing)]
    pub api_key: String,

    /// Encrypted API key (nonce:ciphertext). Written to disk; decrypted into
    /// `api_key` on load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,

    /// Custom base URL override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    /// Default chat model for this instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Fast/cheap model for lightweight tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,

    /// Vision-capable model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,

    /// Default reasoning effort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<bamboo_domain::ReasoningEffort>,

    /// Models that must use the Responses API upstream (OpenAI only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,

    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Whether this instance is enabled. Disabled instances are skipped
    /// during registry construction.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Provider-type-specific extra fields preserved through (de)serialization.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn default_true() -> bool {
    true
}

/// Returns the default server port (9562)
fn default_port() -> u16 {
    9562
}

/// Returns the default bind address (127.0.0.1)
fn default_bind() -> String {
    "127.0.0.1".to_string()
}

/// Returns the default worker count (10)
fn default_workers() -> usize {
    10
}

/// Returns the default data directory (`BAMBOO_DATA_DIR` or `${HOME}/.bamboo`)
fn default_data_dir() -> PathBuf {
    super::paths::bamboo_dir()
}

/// HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Port to listen on
    #[serde(default = "default_port")]
    pub port: u16,

    /// Bind address (127.0.0.1, 0.0.0.0, etc.)
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Static files directory (for Docker mode)
    pub static_dir: Option<PathBuf>,

    /// Worker count for Actix-web
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// v2 (API v2 transport, #181): optional TLS termination config. When both
    /// `cert_file` and `key_file` are given, bamboo terminates TLS itself
    /// (rustls, no reverse proxy) and serves `https://` — intended for the
    /// public `0.0.0.0` face. When absent, the server keeps the plain `.bind()`
    /// / `.listen()` path unchanged (desktop loopback stays plaintext). Missing
    /// or unparseable cert/key files are fail-fast at startup, never a silent
    /// downgrade to plaintext.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    /// Preserve unknown keys under `server`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            bind: default_bind(),
            static_dir: None,
            workers: default_workers(),
            tls: None,
            extra: BTreeMap::new(),
        }
    }
}

/// Manual TLS certificate configuration (current stage; ACME deferred).
///
/// Both fields point at PEM files: `cert_file` is the full certificate chain
/// (leaf → intermediates → root), `key_file` is the matching private key
/// (PKCS#8 or RSA). See `docs/api-v2-transport.md` §3.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    /// PEM certificate chain (leaf → intermediates → root).
    pub cert_file: PathBuf,
    /// PEM private key (PKCS#8 or RSA).
    pub key_file: PathBuf,
}

/// Proxy authentication credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyAuth {
    /// Proxy username
    pub username: String,
    /// Proxy password
    pub password: String,
}

/// Parse a boolean value from environment variable strings
///
/// Accepts: "1", "true", "yes", "y", "on" (case-insensitive)
fn parse_bool_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

fn expand_user_path(value: &str) -> PathBuf {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(trimmed)
}

impl Default for Config {
    fn default() -> Self {
        // In-memory defaults ONLY. `default()` must not touch the filesystem or
        // environment: it was delegating to `new()` → `from_data_dir(None)`,
        // which read config.json from disk, applied BAMBOO_* env overrides, and
        // published to the global env-var cache. That made every `..Default::
        // default()` struct-update and every test silently disk-dependent and
        // let non-server callers clobber the server's in-memory config cache.
        // `create_default()` is the pure in-memory constructor; disk loading is
        // the explicit job of `new()` / `from_data_dir()`. #38.
        Self::create_default()
    }
}

/// Prompt-safe snapshot of configured env vars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSafeEnvVarEntry {
    pub name: String,
    pub secret: bool,
    pub description: Option<String>,
}

/// Global cache of user-managed env vars for injection into child processes.
///
/// Updated whenever the config is loaded or reloaded via [`Config::publish_env_vars`].
static ENV_VARS_CACHE: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

static PROMPT_SAFE_ENV_VARS_CACHE: OnceLock<RwLock<Vec<PromptSafeEnvVarEntry>>> = OnceLock::new();

fn env_vars_cache() -> &'static RwLock<HashMap<String, String>> {
    ENV_VARS_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn prompt_safe_env_vars_cache() -> &'static RwLock<Vec<PromptSafeEnvVarEntry>> {
    PROMPT_SAFE_ENV_VARS_CACHE.get_or_init(|| RwLock::new(Vec::new()))
}

impl Config {
    /// Load configuration from file with environment variable overrides
    ///
    /// Configuration loading order:
    /// 1. Try loading from `config.json` (`{data_dir}/config.json`)
    /// 2. Use defaults
    /// 3. Apply environment variable overrides (highest priority)
    ///
    /// # Environment Variables
    ///
    /// - `BAMBOO_PORT`: Override server port
    /// - `BAMBOO_BIND`: Override bind address
    /// - `BAMBOO_DATA_DIR`: Override data directory
    /// - `BAMBOO_PROVIDER`: Override default provider
    /// - `BAMBOO_HEADLESS`: Enable headless authentication mode
    /// - `BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION`: Override project durable-memory index prompt injection
    /// - `BAMBOO_MEMORY_RELEVANT_RECALL`: Override relevant durable-memory recall prompt injection
    /// - `BAMBOO_MEMORY_RELEVANT_RECALL_RERANK`: Override model-based relevant recall reranking
    /// - `BAMBOO_MEMORY_PROJECT_FIRST_DREAM`: Override project-first Dream prompt behavior
    pub fn new() -> Self {
        Self::from_data_dir(None)
    }

    /// Load configuration from a specific data directory.
    ///
    /// Use [`Config::from_data_dir`] (publishes env vars to the global cache, for
    /// the context that OWNS the cache — the server bootstrap) or
    /// [`Config::from_data_dir_without_publish`] (for non-owning readers that must
    /// not clobber the live cache). #40.
    ///
    /// * `data_dir` - Optional data directory path. If None, uses default (`BAMBOO_DATA_DIR` or `${HOME}/.bamboo`)
    fn from_data_dir_impl(data_dir: Option<PathBuf>, publish: bool, apply_env: bool) -> Self {
        // Determine data_dir early (needed to find config file)
        let data_dir = data_dir
            .or_else(|| std::env::var("BAMBOO_DATA_DIR").ok().map(PathBuf::from))
            .unwrap_or_else(default_data_dir);

        let config_path = data_dir.join("config.json");

        let mut config = if config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                Self::parse_and_hydrate(&content).unwrap_or_else(|e| {
                    // Don't silently discard the user's config on corruption.
                    // Quarantine the unparseable file, then recover the MOST recent
                    // intent in order: (1) SALVAGE the still-valid fields from the
                    // corrupt file (a single bad field shouldn't drop everything),
                    // (2) the last-known-good config.json.bak, (3) defaults.
                    // #37 / #135.
                    tracing::warn!(
                        "Failed to parse config.json ({}); quarantining it and attempting recovery",
                        e
                    );
                    quarantine_corrupt_config(&config_path);
                    Self::salvage_partial(&content, &data_dir)
                        .or_else(|| Self::load_backup(&data_dir))
                        .unwrap_or_else(|| {
                            tracing::warn!(
                                "Could not salvage and no usable config.json.bak; using defaults"
                            );
                            Self::create_default()
                        })
                })
            } else {
                Self::create_default()
            }
        } else {
            Self::create_default()
        };

        // Decrypt encrypted proxy auth into in-memory plaintext form.
        config.hydrate_proxy_auth_from_encrypted();
        // Decrypt encrypted provider API keys into in-memory plaintext form.
        config.hydrate_provider_api_keys_from_encrypted();
        // Decrypt encrypted provider-instance API keys into in-memory plaintext form.
        config.hydrate_provider_instance_api_keys_from_encrypted();
        // Decrypt encrypted MCP secrets into in-memory plaintext form.
        config.hydrate_mcp_secrets_from_encrypted();
        // Decrypt encrypted env vars into in-memory plaintext form.
        config.hydrate_env_vars_from_encrypted();
        // Decrypt encrypted cluster-fabric SSH secrets into in-memory plaintext.
        config.hydrate_cluster_fabric_from_encrypted();
        // Decrypt the encrypted broker token into in-memory plaintext.
        config.hydrate_broker_token_from_encrypted();
        // Decrypt encrypted notification-channel secrets into in-memory plaintext.
        config.hydrate_notifications_from_encrypted();
        // Decrypt encrypted bamboo-connect platform tokens into in-memory plaintext.
        config.hydrate_connect_platform_tokens_from_encrypted();
        config.normalize_tool_settings();
        config.normalize_skill_settings();
        config.normalize_plugin_trust_settings();

        // Legacy: `data_dir` is no longer a persisted config field. The data directory is
        // derived from runtime (BAMBOO_DATA_DIR or `${HOME}/.bamboo`).
        config.extra.remove("data_dir");

        // Apply environment variable overrides (highest priority). Skipped by
        // one-shot CLI writers (`bamboo init` / `config set`) so transient
        // `BAMBOO_*` values are never baked into the persisted config.json.
        if apply_env {
            config.apply_env_overrides();
        }

        // Publish env vars to the global cache so Bash tools can inject them —
        // ONLY when the caller owns that cache. Non-owning readers pass
        // publish=false so they don't clobber the server's live env-var cache.
        if publish {
            config.publish_env_vars();
        }

        config
    }

    /// Apply `BAMBOO_*` environment overrides (highest priority) onto a loaded
    /// config. Factored out so one-shot writers can skip it (see
    /// [`Config::from_data_dir_without_env`]).
    fn apply_env_overrides(&mut self) {
        if let Ok(port) = std::env::var("BAMBOO_PORT") {
            if let Ok(port) = port.parse() {
                self.server.port = port;
            }
        }

        if let Ok(bind) = std::env::var("BAMBOO_BIND") {
            self.server.bind = bind;
        }

        // Note: BAMBOO_DATA_DIR already handled by the caller.
        if let Ok(provider) = std::env::var("BAMBOO_PROVIDER") {
            self.provider = provider;
        }

        if let Ok(headless) = std::env::var("BAMBOO_HEADLESS") {
            self.headless_auth = parse_bool_env(&headless);
        }

        if let Ok(project_prompt_injection) =
            std::env::var("BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION")
        {
            let memory = self.memory.get_or_insert_with(MemoryConfig::default);
            memory.project_prompt_injection = parse_bool_env(&project_prompt_injection);
        }

        if let Ok(relevant_recall) = std::env::var("BAMBOO_MEMORY_RELEVANT_RECALL") {
            let memory = self.memory.get_or_insert_with(MemoryConfig::default);
            memory.relevant_recall = parse_bool_env(&relevant_recall);
        }

        if let Ok(relevant_recall_rerank) = std::env::var("BAMBOO_MEMORY_RELEVANT_RECALL_RERANK") {
            let memory = self.memory.get_or_insert_with(MemoryConfig::default);
            memory.relevant_recall_rerank = parse_bool_env(&relevant_recall_rerank);
        }

        if let Ok(project_first_dream) = std::env::var("BAMBOO_MEMORY_PROJECT_FIRST_DREAM") {
            let memory = self.memory.get_or_insert_with(MemoryConfig::default);
            memory.project_first_dream = parse_bool_env(&project_first_dream);
        }

        // Per-provider API keys from the environment (highest priority). Lets a
        // 12-factor / secret-manager / --env-file / k8s-Secret deploy supply the
        // key at runtime instead of baking a plaintext `api_key` into a mounted
        // config.json. The `api_key_from_env` flag keeps `refresh_provider_api_keys_encrypted`
        // from re-encrypting these keys into `api_key_encrypted` on a later save,
        // so an env key is never persisted to disk. (#253)
        if let Ok(key) = std::env::var("BAMBOO_OPENAI_API_KEY") {
            let key = key.trim();
            if !key.is_empty() {
                let openai = self
                    .providers
                    .openai
                    .get_or_insert_with(OpenAIConfig::default);
                openai.api_key = key.to_string();
                openai.api_key_from_env = true;
            }
        }
        if let Ok(key) = std::env::var("BAMBOO_ANTHROPIC_API_KEY") {
            let key = key.trim();
            if !key.is_empty() {
                let anthropic = self
                    .providers
                    .anthropic
                    .get_or_insert_with(AnthropicConfig::default);
                anthropic.api_key = key.to_string();
                anthropic.api_key_from_env = true;
            }
        }
        if let Ok(key) = std::env::var("BAMBOO_GEMINI_API_KEY") {
            let key = key.trim();
            if !key.is_empty() {
                let gemini = self
                    .providers
                    .gemini
                    .get_or_insert_with(GeminiConfig::default);
                gemini.api_key = key.to_string();
                gemini.api_key_from_env = true;
            }
        }
    }

    /// Load config from disk AND publish its env vars to the process-global cache
    /// (so Bash tools inject them). For the context that OWNS that cache — the
    /// server bootstrap. Library / secondary readers that only need to read a
    /// value must use [`Config::from_data_dir_without_publish`] instead, or they
    /// will clobber the server's live cache with stale disk data (#38 / #40).
    pub fn from_data_dir(data_dir: Option<PathBuf>) -> Self {
        Self::from_data_dir_impl(data_dir, true, true)
    }

    /// Load config from disk WITHOUT publishing env vars to the global cache.
    /// For non-owning readers (e.g. permission storage) that just need a config
    /// value and must not clobber the live env-var cache. #40.
    pub fn from_data_dir_without_publish(data_dir: Option<PathBuf>) -> Self {
        Self::from_data_dir_impl(data_dir, false, true)
    }

    /// Load config from disk WITHOUT applying `BAMBOO_*` env-var overrides and
    /// WITHOUT publishing to the global cache. For one-shot CLI writers
    /// (`bamboo init` / `config set`) that immediately re-save: applying env
    /// overrides here would bake transient values (port/bind/provider/memory
    /// flags) permanently into config.json. Same corruption-recovery + default
    /// fallback as the normal load.
    pub fn from_data_dir_without_env(data_dir: Option<PathBuf>) -> Self {
        Self::from_data_dir_impl(data_dir, false, false)
    }

    /// Deserialize config JSON and run the in-memory hydration + normalization
    /// chain. Shared by the primary load and the backup-recovery path (#37).
    fn parse_and_hydrate(content: &str) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_str::<Config>(content).map(|mut config| {
            config.hydrate_proxy_auth_from_encrypted();
            config.hydrate_provider_api_keys_from_encrypted();
            config.hydrate_provider_instance_api_keys_from_encrypted();
            config.hydrate_mcp_secrets_from_encrypted();
            config.hydrate_env_vars_from_encrypted();
            config.hydrate_cluster_fabric_from_encrypted();
            config.hydrate_broker_token_from_encrypted();
            config.hydrate_notifications_from_encrypted();
            config.hydrate_connect_platform_tokens_from_encrypted();
            config.normalize_tool_settings();
            config.normalize_skill_settings();
            config
        })
    }

    /// Try to recover from the rotated `config.json.bak[.N]` generations (each a
    /// last-known-good written before a save) when the primary `config.json` is
    /// corrupt. Walks newest -> oldest and returns the first that parses; `None`
    /// if every generation is missing or also unparseable. #37 / #135.
    fn load_backup(data_dir: &std::path::Path) -> Option<Self> {
        let config_path = data_dir.join("config.json");
        for gen in 0..BAK_GENERATIONS {
            let backup = backup_path_for(&config_path, gen);
            let Ok(content) = std::fs::read_to_string(&backup) else {
                continue;
            };
            match Self::parse_and_hydrate(&content) {
                Ok(config) => {
                    tracing::info!("Recovered configuration from {:?}", backup);
                    return Some(config);
                }
                Err(e) => {
                    tracing::warn!(
                        "Backup {:?} is unparseable ({}); trying an older generation",
                        backup,
                        e
                    );
                }
            }
        }
        None
    }

    /// Largest corrupt-object key count we'll attempt to salvage. The overlay loop
    /// is O(keys) full-Config deserializes over a growing object (the `extra`
    /// catch-all absorbs unknown keys), i.e. O(n²) on a pathological file; cap it
    /// so a junk-key-flooded config.json can't stall a load. A real config has a
    /// few dozen top-level keys, so this only ever trips on garbage.
    const SALVAGE_MAX_KEYS: usize = 512;

    /// Best-effort PARTIAL salvage of a corrupt `config.json` (#135): parse it as a
    /// generic JSON object and overlay each top-level field onto the richest
    /// known-good baseline — the last-known-good `config.json.bak` if present, else
    /// a fresh default — keeping only the fields that still yield a valid [`Config`].
    /// A single bad field (wrong type, malformed section, …) then keeps the
    /// baseline's value instead of discarding ALL the user's other settings.
    ///
    /// Overlaying onto `.bak` (rather than defaults) means the result is the
    /// best-of-both: the backup's complete recent-good state PLUS the corrupt
    /// file's still-valid newer edits on top — so salvage is never worse than the
    /// plain `.bak` fallback, removing the "sparse salvage defeats a rich backup"
    /// hazard. Tried BEFORE the bare `.bak` fallback.
    ///
    /// Returns the hydrated salvaged config, or `None` when the corrupt file isn't
    /// even a JSON object (nothing field-wise to salvage) so the caller falls
    /// through to `.bak` / defaults.
    ///
    /// NOTE: the per-field overlay guarantees a VALID `Config`, not a *maximal* or
    /// attribution-perfect one. Deterministic alphabetical key order (serde_json is
    /// BTreeMap-backed, no `preserve_order`) means a rename/alias pair like
    /// `mcp`/`mcpServers` can drop the second-seen even if it'd be valid alone — the
    /// outcome is still a valid config, just not necessarily the richest possible.
    fn salvage_partial(content: &str, data_dir: &std::path::Path) -> Option<Self> {
        // Must at least be a JSON object; otherwise there's nothing field-wise to
        // salvage (a truncated/garbage file just falls through to .bak/defaults).
        let corrupt: serde_json::Value = serde_json::from_str(content).ok()?;
        let corrupt_obj = corrupt.as_object()?;
        if corrupt_obj.len() > Self::SALVAGE_MAX_KEYS {
            tracing::warn!(
                "config.json has {} top-level keys (> {}); skipping salvage to avoid an O(n^2) load",
                corrupt_obj.len(),
                Self::SALVAGE_MAX_KEYS
            );
            return None;
        }

        // Overlay onto the richest known-good baseline: the last-known-good backup
        // if it parses, else a fresh default. This makes salvage >= the plain .bak
        // fallback in every case.
        let mut base = Self::load_backup(data_dir)
            .and_then(|backup| serde_json::to_value(backup).ok())
            .or_else(|| serde_json::to_value(Self::create_default()).ok())?;
        let base_obj = base.as_object_mut()?;

        let mut salvaged: Vec<String> = Vec::new();
        for (key, value) in corrupt_obj {
            let previous = base_obj.insert(key.clone(), value.clone());
            // Keep the field iff the WHOLE config still deserializes with it
            // overlaid — base is valid before each step, so a failure isolates THIS
            // field as the corrupt one (and inter-field constraints are respected).
            if serde_json::from_value::<Self>(serde_json::Value::Object(base_obj.clone())).is_ok() {
                salvaged.push(key.clone());
            } else {
                match previous {
                    Some(prev) => {
                        base_obj.insert(key.clone(), prev);
                    }
                    None => {
                        base_obj.remove(key);
                    }
                }
            }
        }

        tracing::warn!(
            "Salvaged {} field(s) from corrupt config.json ({}); corrupt fields kept the \
             last-known-good/default value",
            salvaged.len(),
            salvaged.join(", ")
        );

        // Re-serialize the rebuilt (all-valid) object and run it back through the
        // normal parse+hydrate path so secret-decryption / normalization match a
        // clean load exactly.
        let rebuilt = serde_json::to_string(&base).ok()?;
        Self::parse_and_hydrate(&rebuilt).ok()
    }

    /// Get the effective default model for the currently active provider.
    ///
    /// When `features.provider_model_ref` is enabled, reads from `defaults.chat`
    /// before falling back to legacy provider-specific config.
    ///
    /// Note: for most providers this is a required config value (returns None when absent).
    /// Copilot has a built-in fallback when no model is configured.
    pub fn get_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self.defaults.as_ref().map(|d| &d.chat) {
                return Some(model_ref.model.clone());
            }
        }
        match self.provider.as_str() {
            "openai" => self.providers.openai.as_ref().and_then(|c| c.model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.model.clone()),
            "gemini" => self.providers.gemini.as_ref().and_then(|c| c.model.clone()),
            "copilot" => Some(
                self.providers
                    .copilot
                    .as_ref()
                    .and_then(|c| c.model.clone())
                    .unwrap_or_else(|| "gpt-4o".to_string()),
            ),
            _ => None,
        }
    }

    /// Get the fast/cheap model for the currently active provider.
    ///
    /// When `features.provider_model_ref` is enabled, reads from `defaults.fast`
    /// before falling back to legacy provider-specific config.
    ///
    /// Used for lightweight tasks like title generation and summarization.
    /// Falls back to `get_model()` when no fast_model is configured.
    pub fn get_fast_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self.defaults.as_ref().and_then(|d| d.fast.as_ref()) {
                return Some(model_ref.model.clone());
            }
        }
        let fast = match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            _ => None,
        };
        fast.or_else(|| self.get_model())
    }

    /// Get the configured task summarization model.
    ///
    /// When `features.provider_model_ref` is enabled, reads from
    /// `defaults.task_summary` before falling back through
    /// `defaults.memory_background` → `defaults.fast` → `defaults.chat`.
    ///
    /// This is used for conversation/task summarization and context compression.
    pub fn get_task_summary_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self
                .defaults
                .as_ref()
                .and_then(|d| d.task_summary.as_ref())
                .or_else(|| {
                    self.defaults
                        .as_ref()
                        .and_then(|d| d.memory_background.as_ref())
                })
                .or_else(|| self.defaults.as_ref().and_then(|d| d.fast.as_ref()))
                .or_else(|| self.defaults.as_ref().map(|d| &d.chat))
            {
                return Some(model_ref.model.clone());
            }
        }

        self.get_memory_background_model()
            .or_else(|| self.get_model())
    }

    /// Get the configured memory/background summarization model.
    ///
    /// When `features.provider_model_ref` is enabled, reads from
    /// `defaults.memory_background` before falling back to legacy config.
    ///
    /// Falls back to the provider fast model when no background model is
    /// configured or resolves to an empty string.
    ///
    /// IMPORTANT: this intentionally does **not** fall back to the main
    /// interaction model. Memory compaction / reflection should be skipped or
    /// fail loudly when no background/fast model is configured.
    pub fn get_memory_background_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self
                .defaults
                .as_ref()
                .and_then(|d| d.memory_background.as_ref())
            {
                return Some(model_ref.model.clone());
            }
            if let Some(model_ref) = self.defaults.as_ref().and_then(|d| d.fast.as_ref()) {
                return Some(model_ref.model.clone());
            }
        }
        let configured = self
            .memory
            .as_ref()
            .and_then(|memory| memory.background_model.as_ref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        configured.or_else(|| match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            _ => None,
        })
    }

    /// Resolve the configured default work area path when present.
    ///
    /// This validates that the configured directory exists, but intentionally
    /// returns the stable expanded path rather than the platform-specific
    /// canonicalized path. On macOS, `canonicalize()` may rewrite `/var/...`
    /// to `/private/var/...`, which is correct at the filesystem layer but
    /// undesirable as a user-facing/config-derived workspace path.
    pub fn get_default_work_area_path(&self) -> Option<PathBuf> {
        let raw = self
            .default_work_area
            .as_ref()
            .and_then(|config| config.path.as_ref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())?;

        let candidate = expand_user_path(raw);
        if candidate.is_absolute() {
            let canonical = std::fs::canonicalize(&candidate).ok();
            return canonical
                .as_ref()
                .filter(|path| path.is_dir())
                .map(|_| candidate.clone())
                .or_else(|| candidate.is_dir().then_some(candidate));
        }

        let from_bamboo_dir = crate::paths::bamboo_dir().join(&candidate);
        let canonical = std::fs::canonicalize(&from_bamboo_dir).ok();
        canonical
            .as_ref()
            .filter(|path| path.is_dir())
            .map(|_| from_bamboo_dir.clone())
            .or_else(|| from_bamboo_dir.is_dir().then_some(from_bamboo_dir))
            .or_else(|| candidate.is_dir().then_some(candidate))
    }

    /// Get the vision-capable model for the currently active provider.
    ///
    /// Used for image understanding tasks.
    /// Falls back to `get_model()` when no vision_model is configured.
    pub fn get_vision_model(&self) -> Option<String> {
        let vision = match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            _ => None,
        };
        vision.or_else(|| self.get_model())
    }

    /// Get the default reasoning effort for the currently active provider.
    pub fn get_reasoning_effort(&self) -> Option<ReasoningEffort> {
        self.reasoning_effort_for_key(&self.provider)
    }

    /// Resolve the configured default reasoning effort for a provider routing key.
    ///
    /// The key may be a multi-instance provider id (for example `"copilot-work"`)
    /// or a legacy provider type (for example `"openai"`). In multi-instance mode
    /// the per-instance `reasoning_effort` lives under `provider_instances[<id>]`,
    /// so we resolve instance ids there first; otherwise we fall back to the
    /// legacy per-provider config. Both the execute path
    /// ([`crate`]'s `get_reasoning_effort_for_provider`) and the session-create
    /// path ([`Self::get_reasoning_effort`]) delegate here so the two cannot drift.
    pub fn reasoning_effort_for_key(&self, key: &str) -> Option<ReasoningEffort> {
        let trimmed = key.trim();
        if trimmed.is_empty() {
            return None;
        }

        // Multi-instance mode: the routing key is an instance id.
        if let Some(instance) = self.provider_instances.get(trimmed) {
            return instance.reasoning_effort;
        }

        // Legacy mode: the routing key is a provider type.
        match trimmed {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "bodhi" => self
                .providers
                .bodhi
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            _ => None,
        }
    }

    /// Get normalized disabled tool names.
    pub fn disabled_tool_names(&self) -> BTreeSet<String> {
        self.tools
            .disabled
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| normalize_tool_ref(name).unwrap_or_else(|| name.to_string()))
            .collect()
    }

    /// Normalize tool settings (trim / dedupe / sort).
    pub fn normalize_tool_settings(&mut self) {
        self.tools.disabled = self.disabled_tool_names().into_iter().collect();
    }

    /// Get normalized disabled skill IDs.
    pub fn disabled_skill_ids(&self) -> BTreeSet<String> {
        self.skills
            .disabled
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(|id| id.to_string())
            .collect()
    }

    /// Normalize skill settings (trim / dedupe / sort).
    pub fn normalize_skill_settings(&mut self) {
        self.skills.disabled = self.disabled_skill_ids().into_iter().collect();
    }

    /// Normalize `plugin_trust.trusted_hosts` entries (trim / lowercase / drop
    /// empties) so a hand-edited `config.json` doesn't silently accumulate
    /// mixed-case or whitespace-padded entries. [`is_host_trusted`] itself
    /// already matches case-insensitively regardless of how an entry is
    /// stored, so this is defense in depth / a canonical on-disk form, not
    /// the source of the security fix — that's the host/path-component
    /// matching in [`is_host_trusted`] itself.
    pub fn normalize_plugin_trust_settings(&mut self) {
        self.plugin_trust.trusted_hosts = self
            .plugin_trust
            .trusted_hosts
            .iter()
            .map(|entry| entry.trim().to_ascii_lowercase())
            .filter(|entry| !entry.is_empty())
            .collect();
    }

    /// Return the effective default provider key.
    ///
    /// Prefers `default_provider_instance` when set; falls back to the
    /// legacy `provider` string.
    pub fn effective_default_provider(&self) -> &str {
        self.default_provider_instance
            .as_deref()
            .unwrap_or(&self.provider)
    }

    /// Whether provider instances are configured (new multi-instance path).
    pub fn has_provider_instances(&self) -> bool {
        !self.provider_instances.is_empty()
    }

    /// Build a flat map of all env vars with non-empty values (for process injection).
    pub fn env_vars_as_map(&self) -> HashMap<String, String> {
        self.env_vars
            .iter()
            .filter(|e| !e.value.trim().is_empty())
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect()
    }

    fn prompt_safe_env_vars(&self) -> Vec<PromptSafeEnvVarEntry> {
        self.env_vars
            .iter()
            .filter(|entry| !entry.name.trim().is_empty() && !entry.value.trim().is_empty())
            .map(|entry| PromptSafeEnvVarEntry {
                name: entry.name.clone(),
                secret: entry.secret,
                description: entry
                    .description
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            })
            .collect()
    }

    /// Update the global env vars cache (called on config load / reload).
    pub fn publish_env_vars(&self) {
        let map = self.env_vars_as_map();
        let mut env_guard = env_vars_cache().write().recover_poison();
        *env_guard = map;

        let prompt_safe = self.prompt_safe_env_vars();
        let mut prompt_guard = prompt_safe_env_vars_cache().write().recover_poison();
        *prompt_guard = prompt_safe;
    }

    /// Read the current env vars snapshot (called by Bash tool at process spawn time).
    pub fn current_env_vars() -> HashMap<String, String> {
        env_vars_cache().read().recover_poison().clone()
    }

    /// Read the current prompt-safe env var snapshot (names + metadata only; no secret values).
    pub fn current_prompt_safe_env_vars() -> Vec<PromptSafeEnvVarEntry> {
        prompt_safe_env_vars_cache().read().recover_poison().clone()
    }

    /// Create a default configuration without loading from file
    fn create_default() -> Self {
        Config {
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            proxy_auth_encrypted: None,
            headless_auth: false,
            subagents: SubagentsConfig::default(),
            cluster_fabric: crate::cluster_fabric::ClusterFabricConfig::default(),
            provider: default_provider(),
            providers: ProviderConfigs::default(),
            provider_instances: HashMap::new(),
            default_provider_instance: None,
            server: ServerConfig::default(),
            keyword_masking: KeywordMaskingConfig::default(),
            anthropic_model_mapping: AnthropicModelMapping::default(),
            gemini_model_mapping: GeminiModelMapping::default(),
            hooks: HooksConfig::default(),
            tools: ToolsConfig::default(),
            skills: SkillsConfig::default(),
            env_vars: Vec::new(),
            default_work_area: None,
            access_control: None,
            features: FeatureFlags::default(),
            defaults: None,
            memory: None,
            mcp: bamboo_domain::mcp_config::McpConfig::default(),
            notifications: NotificationsConfig::default(),
            connect: ConnectConfig::default(),
            plugin_trust: PluginTrustConfig::default(),
            extra: BTreeMap::new(),
        }
    }

    /// Get the full server address (bind:port)
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }

    /// Save configuration to disk
    pub fn save(&self) -> Result<()> {
        self.save_to_dir(default_data_dir())
    }

    /// Save configuration to disk under the provided data directory.
    ///
    /// Configuration is always stored as `{data_dir}/config.json`.
    pub fn save_to_dir(&self, data_dir: PathBuf) -> Result<()> {
        let path = data_dir.join("config.json");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config dir: {:?}", parent))?;
        }

        let mut to_save = self.clone();
        // Never persist `data_dir` into config.json (data dir is runtime-derived).
        to_save.extra.remove("data_dir");
        // Root-level `model` is deprecated; do not persist it.
        to_save.extra.remove("model");
        to_save.refresh_proxy_auth_encrypted()?;
        to_save.refresh_provider_api_keys_encrypted()?;
        to_save.refresh_provider_instance_api_keys_encrypted()?;
        to_save.refresh_env_vars_encrypted()?;
        to_save.sanitize_env_vars_for_disk();
        to_save.refresh_cluster_fabric_encrypted()?;
        to_save.sanitize_cluster_fabric_for_disk();
        // `subagents.broker` is `#[serde(skip)]` (runtime-only, lives in its own
        // broker.json / embedded in-process) — nothing to encrypt or persist here.
        to_save.refresh_notifications_encrypted()?;
        to_save.refresh_connect_platform_tokens_encrypted()?;
        to_save.normalize_tool_settings();
        to_save.normalize_skill_settings();
        let content =
            serde_json::to_string_pretty(&to_save).context("Failed to serialize config to JSON")?;

        // Back up the current on-disk config (last-known-good) before overwriting,
        // so corruption (a bad/partial write, external edit, disk issue) stays
        // recoverable via config.json.bak on the next load. Best-effort. Only
        // refresh the backup from a PARSEABLE config.json — otherwise a save right
        // after an in-memory recovery (where the on-disk config.json is still the
        // corrupt original) would clobber the good .bak with garbage. #37.
        if path.exists()
            && std::fs::read_to_string(&path)
                .ok()
                .is_some_and(|c| Self::parse_and_hydrate(&c).is_ok())
        {
            // Rotate the older generations down (.bak -> .bak.1 -> .bak.2 …) so a
            // few last-known-good snapshots survive, then snapshot the current
            // (parseable) config.json as the freshest .bak. #135.
            rotate_backups(&path, BAK_GENERATIONS);
            let backup = backup_path_for(&path, 0);
            if let Err(e) = std::fs::copy(&path, &backup) {
                tracing::warn!("Failed to back up config.json before save: {}", e);
            }
        }

        write_atomic(&path, content.as_bytes())
            .with_context(|| format!("Failed to write config file: {:?}", path))?;

        Ok(())
    }
}

/// How many `config.json.corrupted.*` quarantine files to keep. Each corrupt load
/// drops one; without a cap they accumulate unbounded. Newest `N` are retained.
const QUARANTINE_KEEP: usize = 5;

/// Copy a corrupt config file aside to `config.json.corrupted.<nanos>` so the
/// user's (unparseable) configuration is preserved for inspection/recovery
/// instead of being silently discarded and then overwritten by defaults. #37.
fn quarantine_corrupt_config(config_path: &std::path::Path) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Two corrupt loads in the same nanosecond would land on the same name and the
    // second `copy` would silently overwrite the first. Append a counter on
    // collision so each quarantine is preserved distinctly. #135.
    let mut quarantine = config_path.with_extension(format!("json.corrupted.{nanos}"));
    let mut dedup = 1u32;
    while quarantine.exists() {
        quarantine = config_path.with_extension(format!("json.corrupted.{nanos}.{dedup}"));
        dedup += 1;
    }
    match std::fs::copy(config_path, &quarantine) {
        Ok(_) => tracing::warn!("Quarantined corrupt config.json to {:?}", quarantine),
        Err(e) => tracing::error!("Failed to quarantine corrupt config.json: {}", e),
    }
    prune_quarantine_files(config_path, QUARANTINE_KEEP);
}

/// Keep only the newest `keep` `config.json.corrupted.*` files next to
/// `config_path`, deleting older ones so quarantines don't grow unbounded. #135.
fn prune_quarantine_files(config_path: &std::path::Path, keep: usize) {
    let Some(dir) = config_path.parent() else {
        return;
    };
    let prefix = "config.json.corrupted.";
    let mut quarantines: Vec<std::path::PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(prefix))
            })
            .collect(),
        Err(_) => return,
    };
    if quarantines.len() <= keep {
        return;
    }
    // Oldest first (by mtime; missing mtime sorts oldest so it's pruned first).
    quarantines.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
    let remove = quarantines.len() - keep;
    for stale in quarantines.into_iter().take(remove) {
        if let Err(e) = std::fs::remove_file(&stale) {
            tracing::warn!("Failed to prune old quarantine file {:?}: {}", stale, e);
        }
    }
}

/// Number of `config.json.bak[.N]` generations to retain (`.bak` + `N-1` numbered).
/// More generations = more recovery points if a fresher backup is itself bad. #135.
const BAK_GENERATIONS: usize = 3;

/// The on-disk path of backup generation `gen` (0 == `config.json.bak`).
fn backup_path_for(config_path: &std::path::Path, gen: usize) -> std::path::PathBuf {
    if gen == 0 {
        config_path.with_extension("json.bak")
    } else {
        config_path.with_extension(format!("json.bak.{gen}"))
    }
}

/// Shift the backup generations down before a fresh `.bak` is written:
/// `.bak.(N-2) -> .bak.(N-1)`, …, `.bak -> .bak.1`. The oldest is overwritten by
/// the shift; the caller then writes the new `.bak`. Walks the highest (oldest)
/// destination slot first so no rename clobbers a slot a later move still needs to
/// read. Best-effort. #135.
fn rotate_backups(config_path: &std::path::Path, generations: usize) {
    for gen in (1..generations).rev() {
        let from = backup_path_for(config_path, gen - 1);
        let to = backup_path_for(config_path, gen);
        if from.exists() {
            if let Err(e) = std::fs::rename(&from, &to) {
                tracing::warn!("Failed to rotate backup {:?} -> {:?}: {}", from, to, e);
            }
        }
    }
}

fn write_atomic(path: &std::path::Path, content: &[u8]) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return std::fs::write(path, content);
    };

    std::fs::create_dir_all(parent)?;

    // Write to a temp file in the same directory then rename to ensure atomic replace.
    // (Rename is atomic on Unix when source/dest are on the same filesystem.)
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("config.json");
    let tmp_name = format!(".{}.tmp.{}", file_name, std::process::id());
    let tmp_path = parent.join(tmp_name);

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(content)?;
        file.sync_all()?;
    }

    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn subagents_config_without_remote_placements_deserializes_empty() {
        // An OLD config (predating P1.5) has no `remote_placements` key — it must
        // still deserialize, with an empty placement list (default = local path).
        let json = r#"{ "max_concurrent": 4 }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("old config deserializes");
        assert_eq!(cfg.max_concurrent, Some(4));
        assert!(cfg.remote_placements.is_empty());
        // And an empty placement list is omitted on re-serialize (skip_if empty).
        let back = serde_json::to_string(&cfg).unwrap();
        assert!(
            !back.contains("remote_placements"),
            "empty vec is skipped: {back}"
        );
    }

    #[test]
    fn remote_actor_placement_round_trips() {
        let json = r#"{
            "remote_placements": [
                {
                    "role": "explorer",
                    "endpoint": "wss://gpu-host:8443",
                    "token_env": "WORKER_TOKEN",
                    "ca_cert_file": "/etc/bamboo/worker.pem"
                },
                { "role": "writer", "endpoint": "ws://127.0.0.1:9001" }
            ]
        }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("populated config");
        assert_eq!(cfg.remote_placements.len(), 2);
        let p0 = &cfg.remote_placements[0];
        assert_eq!(p0.role, "explorer");
        assert_eq!(p0.endpoint, "wss://gpu-host:8443");
        assert_eq!(p0.token_env.as_deref(), Some("WORKER_TOKEN"));
        assert_eq!(p0.ca_cert_file.as_deref(), Some("/etc/bamboo/worker.pem"));
        // Optional fields default to None and are skipped on serialize.
        let p1 = &cfg.remote_placements[1];
        assert_eq!(p1.role, "writer");
        assert!(p1.token_env.is_none());
        assert!(p1.ca_cert_file.is_none());

        let back = serde_json::to_string(&cfg).unwrap();
        let reparsed: SubagentsConfig = serde_json::from_str(&back).unwrap();
        assert_eq!(cfg, reparsed, "round-trip is stable");
        assert!(!back.contains("\"token_env\":null"));
        assert!(!back.contains("\"ca_cert_file\":null"));
    }

    #[test]
    fn subagents_config_without_schedulable_placements_deserializes_empty() {
        // An OLD config (predating P2b) has no `schedulable_placements` key — it
        // must still deserialize, with an empty list (default = local path).
        let json = r#"{ "max_concurrent": 4 }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("old config deserializes");
        assert!(cfg.schedulable_placements.is_empty());
        // An empty list is omitted on re-serialize (skip_if empty).
        let back = serde_json::to_string(&cfg).unwrap();
        assert!(
            !back.contains("schedulable_placements"),
            "empty vec is skipped: {back}"
        );
    }

    #[test]
    fn schedulable_placement_round_trips() {
        let json = r#"{
            "schedulable_placements": [
                {
                    "role": "explorer",
                    "pool": "gpu-pool",
                    "registry_url": "https://control-plane:9562",
                    "token_env": "WORKER_TOKEN",
                    "ca_cert_file": "/etc/bamboo/worker.pem"
                },
                { "role": "writer", "pool": "cpu-pool", "registry_url": "http://127.0.0.1:8080" }
            ]
        }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("populated config");
        assert_eq!(cfg.schedulable_placements.len(), 2);
        let p0 = &cfg.schedulable_placements[0];
        assert_eq!(p0.role, "explorer");
        assert_eq!(p0.pool, "gpu-pool");
        assert_eq!(p0.registry_url, "https://control-plane:9562");
        assert_eq!(p0.token_env.as_deref(), Some("WORKER_TOKEN"));
        assert_eq!(p0.ca_cert_file.as_deref(), Some("/etc/bamboo/worker.pem"));
        // Optional fields default to None and are skipped on serialize.
        let p1 = &cfg.schedulable_placements[1];
        assert_eq!(p1.role, "writer");
        assert_eq!(p1.pool, "cpu-pool");
        assert!(p1.token_env.is_none());
        assert!(p1.ca_cert_file.is_none());

        let back = serde_json::to_string(&cfg).unwrap();
        let reparsed: SubagentsConfig = serde_json::from_str(&back).unwrap();
        assert_eq!(cfg, reparsed, "round-trip is stable");
        assert!(!back.contains("\"token_env\":null"));
        assert!(!back.contains("\"ca_cert_file\":null"));
    }

    #[test]
    fn server_config_without_tls_field_deserializes_back_compat() {
        // An old config.json `server` section with no `tls` key must still
        // deserialize, leaving `tls` as None (zero behavior change on upgrade).
        let server: ServerConfig = serde_json::from_value(serde_json::json!({
            "port": 9562,
            "bind": "127.0.0.1"
        }))
        .expect("legacy server config without tls should deserialize");

        assert_eq!(server.tls, None);
        assert_eq!(server.port, 9562);
        assert_eq!(server.bind, "127.0.0.1");
    }

    #[test]
    fn server_config_omits_tls_when_none() {
        // `skip_serializing_if = "Option::is_none"` keeps the on-disk shape
        // identical to before for the common (no-TLS) case.
        let server = ServerConfig::default();
        let value = serde_json::to_value(&server).expect("server config should serialize");
        let obj = value
            .as_object()
            .expect("server config serializes to object");
        assert!(
            !obj.contains_key("tls"),
            "tls must be omitted when None, got: {value}"
        );
    }

    #[test]
    fn server_config_with_tls_roundtrips() {
        let server: ServerConfig = serde_json::from_value(serde_json::json!({
            "port": 9562,
            "bind": "0.0.0.0",
            "tls": { "cert_file": "/etc/bamboo/cert.pem", "key_file": "/etc/bamboo/key.pem" }
        }))
        .expect("server config with tls should deserialize");

        let tls = server.tls.clone().expect("tls should be Some");
        assert_eq!(tls.cert_file, PathBuf::from("/etc/bamboo/cert.pem"));
        assert_eq!(tls.key_file, PathBuf::from("/etc/bamboo/key.pem"));

        // Round-trips: tls survives a serialize → deserialize cycle.
        let value = serde_json::to_value(&server).expect("serialize");
        assert!(value.as_object().unwrap().contains_key("tls"));
        let back: ServerConfig = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back.tls, server.tls);
    }

    #[test]
    fn access_control_without_devices_field_deserializes_back_compat() {
        // An old config.json `access_control` with no `devices` key must still
        // deserialize, leaving `devices` empty (root-password-only mode).
        let access: AccessControlConfig = serde_json::from_value(serde_json::json!({
            "password_enabled": true,
            "password_hash": "deadbeef",
            "password_salt": "01020304",
        }))
        .expect("legacy access_control without devices should deserialize");

        assert!(access.devices.is_empty());
        assert!(access.password_enabled);
    }

    #[test]
    fn access_control_omits_devices_when_empty() {
        // `skip_serializing_if = "Vec::is_empty"` keeps the on-disk shape
        // identical for instances that never paired a device.
        let access = AccessControlConfig {
            password_enabled: true,
            password_hash: Some("deadbeef".to_string()),
            password_salt: Some("01020304".to_string()),
            updated_at: None,
            devices: Vec::new(),
        };
        let value = serde_json::to_value(&access).expect("serialize");
        let obj = value.as_object().expect("object");
        assert!(
            !obj.contains_key("devices"),
            "devices must be omitted when empty, got: {value}"
        );
    }

    #[test]
    fn access_control_with_devices_roundtrips() {
        let device = DeviceCredential {
            device_id: "bamboo_0123456789ab".to_string(),
            label: "iPhone 15".to_string(),
            token_hash: "abcd".to_string(),
            token_salt: "ef01".to_string(),
            created_at: "2026-06-23T00:00:00Z".to_string(),
            last_used_at: None,
            revoked: false,
        };
        let access = AccessControlConfig {
            password_enabled: true,
            password_hash: Some("deadbeef".to_string()),
            password_salt: Some("01020304".to_string()),
            updated_at: None,
            devices: vec![device.clone()],
        };
        let value = serde_json::to_value(&access).expect("serialize");
        assert!(value.as_object().unwrap().contains_key("devices"));
        let back: AccessControlConfig = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back.devices, vec![device]);
    }

    #[test]
    fn reasoning_effort_for_key_resolves_instance_id() {
        // Multi-instance mode: the routing key is an instance id and the effort
        // lives under provider_instances[<id>] — previously this fell through to
        // None because the resolver only matched literal provider types.
        let instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "copilot",
            "reasoning_effort": "high",
        }))
        .expect("instance config should deserialize");

        let mut config = Config::create_default();
        config
            .provider_instances
            .insert("copilot-work".to_string(), instance);

        assert_eq!(
            config.reasoning_effort_for_key("copilot-work"),
            Some(ReasoningEffort::High),
        );
    }

    #[test]
    fn reasoning_effort_for_key_resolves_bodhi_legacy() {
        // Legacy mode: the `bodhi` provider previously had no match arm.
        let mut config = Config::create_default();
        config.providers.bodhi = Some(
            serde_json::from_value(serde_json::json!({
                "reasoning_effort": "xhigh",
            }))
            .expect("bodhi config should deserialize"),
        );

        assert_eq!(
            config.reasoning_effort_for_key("bodhi"),
            Some(ReasoningEffort::Xhigh),
        );
    }

    #[test]
    fn reasoning_effort_for_key_resolves_legacy_provider_type() {
        let mut config = Config::create_default();
        config.providers.openai = Some(
            serde_json::from_value(serde_json::json!({
                "api_key": "sk-test",
                "reasoning_effort": "low",
            }))
            .expect("openai config should deserialize"),
        );

        assert_eq!(
            config.reasoning_effort_for_key("openai"),
            Some(ReasoningEffort::Low),
        );
    }

    #[test]
    fn reasoning_effort_for_key_returns_none_for_unknown_and_empty() {
        let config = Config::create_default();
        assert_eq!(config.reasoning_effort_for_key("nope"), None);
        assert_eq!(config.reasoning_effort_for_key("   "), None);
    }

    struct TempHome {
        path: PathBuf,
    }

    impl TempHome {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "chat-core-config-test-{}-{}",
                std::process::id(),
                nanos
            ));
            std::fs::create_dir_all(&path).expect("failed to create temp home dir");
            Self { path }
        }

        fn set_config_json(&self, content: &str) {
            // Treat `path` as the Bamboo data dir and write `config.json` into it.
            // Tests should prefer BAMBOO_DATA_DIR over HOME to avoid global env contention.
            std::fs::create_dir_all(&self.path).expect("failed to create config dir");
            std::fs::write(self.path.join("config.json"), content)
                .expect("failed to write config.json");
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    // Delegate to the single crate-wide test lock so env-mutating tests across
    // `config`, `encryption`, and `paths` serialize against one another (they
    // all mutate the same process-global env / static caches).
    fn env_lock() -> &'static Mutex<()> {
        crate::test_support::env_cache_lock()
    }

    /// Acquire the environment lock, recovering from poison if a previous test failed
    fn env_lock_acquire() -> std::sync::MutexGuard<'static, ()> {
        env_lock().lock().unwrap_or_else(|poisoned| {
            // Lock was poisoned by a previous test failure - recover it
            poisoned.into_inner()
        })
    }

    #[test]
    fn parse_bool_env_true_values() {
        for value in ["1", "true", "TRUE", " yes ", "Y", "on"] {
            assert!(parse_bool_env(value), "value {value:?} should be true");
        }
    }

    #[test]
    fn parse_bool_env_false_values() {
        for value in ["0", "false", "no", "off", "", "  "] {
            assert!(!parse_bool_env(value), "value {value:?} should be false");
        }
    }

    #[test]
    fn config_new_ignores_http_proxy_env_vars() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "http_proxy": "",
  "https_proxy": ""
}"#,
        );

        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

        assert!(
            config.http_proxy.is_empty(),
            "config should ignore HTTP_PROXY env var"
        );
        assert!(
            config.https_proxy.is_empty(),
            "config should ignore HTTPS_PROXY env var"
        );
    }

    #[test]
    fn config_new_loads_config_when_proxy_fields_omitted() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "provider": "openai",
  "providers": {
    "openai": {
      "api_key": "sk-test",
      "model": "gpt-4o"
    }
  }
}"#,
        );

        let _http_proxy = EnvVarGuard::unset("HTTP_PROXY");
        let _https_proxy = EnvVarGuard::unset("HTTPS_PROXY");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.model.as_deref()),
            Some("gpt-4o"),
            "config should load provider model from config file even when proxy fields are omitted"
        );
        assert!(config.http_proxy.is_empty());
        assert!(config.https_proxy.is_empty());
    }

    #[test]
    fn publish_env_vars_updates_prompt_safe_snapshot_without_secret_values() {
        let _lock = crate::test_support::env_cache_lock_acquire();
        let config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "SECRET_TOKEN".to_string(),
                    value: "top-secret".to_string(),
                    secret: true,
                    value_encrypted: None,
                    description: Some("Service token".to_string()),
                },
                EnvVarEntry {
                    name: "API_BASE".to_string(),
                    value: "https://internal.example".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: Some("Internal API base".to_string()),
                },
            ],
            ..Default::default()
        };

        config.publish_env_vars();

        let injected = Config::current_env_vars();
        assert_eq!(
            injected.get("SECRET_TOKEN").map(String::as_str),
            Some("top-secret")
        );
        assert_eq!(
            injected.get("API_BASE").map(String::as_str),
            Some("https://internal.example")
        );

        let prompt_safe = Config::current_prompt_safe_env_vars();
        assert_eq!(prompt_safe.len(), 2);
        assert!(prompt_safe.iter().any(|entry| {
            entry.name == "SECRET_TOKEN"
                && entry.secret
                && entry.description.as_deref() == Some("Service token")
        }));
        assert!(prompt_safe.iter().any(|entry| {
            entry.name == "API_BASE"
                && !entry.secret
                && entry.description.as_deref() == Some("Internal API base")
        }));
        assert!(!prompt_safe
            .iter()
            .any(|entry| entry.name.contains("top-secret")));
        assert!(!prompt_safe.iter().any(|entry| {
            entry
                .description
                .as_deref()
                .is_some_and(|value| value.contains("https://internal.example"))
        }));
    }

    #[test]
    fn from_data_dir_without_publish_does_not_clobber_global_cache() {
        let _lock = crate::test_support::env_cache_lock_acquire();

        // Seed the global cache with a marker "owned" by the live config.
        Config {
            env_vars: vec![EnvVarEntry {
                name: "BAMBOO_CACHE_OWNER_40".to_string(),
                value: "live".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            }],
            ..Default::default()
        }
        .publish_env_vars();
        assert_eq!(
            Config::current_env_vars()
                .get("BAMBOO_CACHE_OWNER_40")
                .map(String::as_str),
            Some("live")
        );

        // A config.json on disk sets the SAME var to a different (stale) value.
        let temp = TempHome::new();
        temp.set_config_json(
            &serde_json::json!({
                "env_vars": [{ "name": "BAMBOO_CACHE_OWNER_40", "value": "stale-disk" }]
            })
            .to_string(),
        );

        // Non-publishing load reads the disk value into the returned Config but
        // must NOT touch the global cache.
        let loaded = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            loaded
                .env_vars
                .iter()
                .find(|e| e.name == "BAMBOO_CACHE_OWNER_40")
                .map(|e| e.value.as_str()),
            Some("stale-disk"),
            "the returned Config holds the disk value"
        );
        assert_eq!(
            Config::current_env_vars()
                .get("BAMBOO_CACHE_OWNER_40")
                .map(String::as_str),
            Some("live"),
            "but the global cache is UNTOUCHED — no clobber (#40)"
        );

        // Contrast: the publishing variant DOES clobber the cache.
        let _ = Config::from_data_dir(Some(temp.path.clone()));
        assert_eq!(
            Config::current_env_vars()
                .get("BAMBOO_CACHE_OWNER_40")
                .map(String::as_str),
            Some("stale-disk"),
            "the publishing loader clobbers the cache (contrast)"
        );
    }

    fn dir_has_quarantine_file(dir: &std::path::Path) -> bool {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains("config.json.corrupted.")
            })
    }

    #[test]
    fn corrupt_config_recovered_from_backup_and_quarantined() {
        let temp = TempHome::new();
        // Last-known-good backup with a distinctive value.
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({ "http_proxy": "http://from-backup" }).to_string(),
        )
        .unwrap();
        // Corrupt primary config.json.
        std::fs::write(temp.path.join("config.json"), "{ not valid json ").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.http_proxy, "http://from-backup",
            "recovered from config.json.bak instead of losing all config"
        );
        assert!(
            dir_has_quarantine_file(&temp.path),
            "corrupt config.json was quarantined (preserved), not discarded"
        );
    }

    #[test]
    fn corrupt_config_without_backup_quarantines_then_defaults() {
        let temp = TempHome::new();
        std::fs::write(temp.path.join("config.json"), "}}} broken").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(
            config.http_proxy.is_empty(),
            "no backup -> falls back to defaults"
        );
        assert!(
            dir_has_quarantine_file(&temp.path),
            "corrupt config.json is quarantined even when there's no backup"
        );
    }

    #[test]
    fn salvage_recovers_valid_fields_from_partially_corrupt_config() {
        let temp = TempHome::new();
        // A valid JSON OBJECT, but `env_vars` is the wrong type (string, not array)
        // so STRICT parse fails. There is NO config.json.bak, so recovery must come
        // from field-level salvage: `http_proxy` is valid and must survive; the bad
        // `env_vars` resets to its default.
        temp.set_config_json(
            r#"{"http_proxy":"http://salvaged","env_vars":"this-should-be-an-array"}"#,
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.http_proxy, "http://salvaged",
            "the valid field was salvaged from a partially-corrupt config (no .bak existed)"
        );
        assert!(
            config.env_vars.is_empty(),
            "the corrupt field reset to its default instead of failing the whole load"
        );
        assert!(
            dir_has_quarantine_file(&temp.path),
            "the corrupt config.json was still quarantined for inspection"
        );
    }

    #[test]
    fn salvage_preferred_over_backup_for_most_recent_intent() {
        let temp = TempHome::new();
        // An OLDER last-known-good backup...
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({ "http_proxy": "http://old-from-backup" }).to_string(),
        )
        .unwrap();
        // ...and a NEWER config that is corrupt but field-salvageable.
        temp.set_config_json(
            r#"{"http_proxy":"http://new-salvaged","env_vars":"this-should-be-an-array"}"#,
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.http_proxy, "http://new-salvaged",
            "salvage (recent partial) is tried BEFORE the .bak fallback (older complete)"
        );
    }

    #[test]
    fn salvage_merges_backup_baseline_with_corrupt_files_newer_valid_edits() {
        let temp = TempHome::new();
        // Backup carries TWO good values.
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({
                "http_proxy": "http://old-from-backup",
                "https_proxy": "https://kept-from-backup",
            })
            .to_string(),
        )
        .unwrap();
        // The corrupt file updates http_proxy (newer), leaves https_proxy untouched,
        // and has one wrong-type field.
        temp.set_config_json(
            r#"{"http_proxy":"http://newer-edit","env_vars":"this-should-be-an-array"}"#,
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        // Best of both: the corrupt file's newer valid edit wins where it set one...
        assert_eq!(
            config.http_proxy, "http://newer-edit",
            "the corrupt file's newer valid edit is applied"
        );
        // ...and the backup's value survives for fields the corrupt file didn't fix.
        assert_eq!(
            config.https_proxy, "https://kept-from-backup",
            "the backup baseline is preserved for fields not in (or invalid in) the corrupt file"
        );
    }

    #[test]
    fn unparseable_non_object_config_skips_salvage_and_uses_backup() {
        let temp = TempHome::new();
        // Not even a JSON object -> nothing field-wise to salvage -> must fall
        // through to the .bak (the pre-#135 behavior is preserved).
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({ "http_proxy": "http://from-backup" }).to_string(),
        )
        .unwrap();
        std::fs::write(temp.path.join("config.json"), "{ not valid json ").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.http_proxy, "http://from-backup",
            "garbage (non-object) config skips salvage and recovers from .bak"
        );
    }

    #[test]
    fn quarantine_files_are_capped_to_newest_n() {
        let temp = TempHome::new();
        let config_path = temp.path.join("config.json");
        std::fs::write(&config_path, "{}").unwrap();

        // Drop more quarantines than the cap; each call sleeps so nanos (the name)
        // and mtime (the prune sort key) are distinct.
        for _ in 0..(QUARANTINE_KEEP + 3) {
            quarantine_corrupt_config(&config_path);
            std::thread::sleep(std::time::Duration::from_millis(3));
        }

        let count = std::fs::read_dir(&temp.path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("config.json.corrupted.")
            })
            .count();
        assert_eq!(
            count, QUARANTINE_KEEP,
            "old quarantine files are pruned to the newest {QUARANTINE_KEEP}"
        );
    }

    #[test]
    fn load_recovers_from_older_backup_generation_when_bak_is_also_corrupt() {
        let temp = TempHome::new();
        // Primary AND the freshest .bak are corrupt; an older generation is good.
        std::fs::write(temp.path.join("config.json"), "CORRUPT-NOT-JSON").unwrap();
        std::fs::write(temp.path.join("config.json.bak"), "ALSO-CORRUPT").unwrap();
        std::fs::write(
            temp.path.join("config.json.bak.1"),
            serde_json::json!({ "http_proxy": "http://from-gen-1" }).to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.http_proxy, "http://from-gen-1",
            "recovered from .bak.1 when both config.json and .bak are corrupt"
        );
    }

    #[test]
    fn save_rotates_backup_generations() {
        let temp = TempHome::new();
        let path = temp.path.join("config.json");
        // v1 is the existing on-disk config.
        std::fs::write(
            &path,
            serde_json::json!({ "http_proxy": "http://proxy-v1" }).to_string(),
        )
        .unwrap();

        let mut cfg = Config::create_default();
        // Save 1: backs up the existing v1 -> .bak, writes v2.
        cfg.http_proxy = "http://proxy-v2".to_string();
        cfg.save_to_dir(temp.path.clone()).unwrap();
        // Save 2: existing (v2) is parseable -> rotate .bak(v1) -> .bak.1, .bak = v2.
        cfg.http_proxy = "http://proxy-v3".to_string();
        cfg.save_to_dir(temp.path.clone()).unwrap();

        let bak = std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap();
        let bak1 = std::fs::read_to_string(temp.path.join("config.json.bak.1")).unwrap();
        assert!(
            bak.contains("proxy-v2"),
            ".bak holds the previous generation (v2)"
        );
        assert!(
            bak1.contains("proxy-v1"),
            ".bak.1 holds the older rotated generation (v1)"
        );
    }

    #[test]
    fn save_backs_up_existing_config() {
        let temp = TempHome::new();
        // Existing (old) config on disk.
        std::fs::write(
            temp.path.join("config.json"),
            serde_json::json!({ "http_proxy": "http://old" }).to_string(),
        )
        .unwrap();

        let mut config = Config::create_default();
        config.http_proxy = "http://new".to_string();
        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let backup =
            std::fs::read_to_string(temp.path.join("config.json.bak")).expect("config.json.bak");
        assert!(
            backup.contains("http://old"),
            "config.json.bak holds the PREVIOUS config (last-known-good)"
        );
        let current = std::fs::read_to_string(temp.path.join("config.json")).unwrap();
        assert!(
            current.contains("http://new"),
            "config.json holds the new config"
        );
    }

    #[test]
    fn save_does_not_overwrite_good_backup_with_corrupt_config() {
        let temp = TempHome::new();
        // A good last-known-good backup...
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({ "http_proxy": "http://good-bak" }).to_string(),
        )
        .unwrap();
        // ...but the on-disk config.json is corrupt (as it would be right after an
        // in-memory recovery, before any clean save).
        std::fs::write(temp.path.join("config.json"), "{{ corrupt").unwrap();

        let mut config = Config::create_default();
        config.http_proxy = "http://new".to_string();
        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        // The good .bak must NOT have been clobbered by the corrupt config.json.
        let backup = std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap();
        assert!(
            backup.contains("http://good-bak"),
            "good last-known-good backup is preserved (not overwritten by corrupt config.json)"
        );
    }

    #[test]
    fn config_new_ignores_proxy_env_vars_when_proxy_fields_omitted() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "provider": "openai",
  "providers": {
    "openai": {
      "api_key": "sk-test",
      "model": "gpt-4o"
    }
  }
}"#,
        );

        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.model.as_deref()),
            Some("gpt-4o")
        );
        assert!(
            config.http_proxy.is_empty(),
            "config should keep http_proxy empty when field is omitted"
        );
        assert!(
            config.https_proxy.is_empty(),
            "config should keep https_proxy empty when field is omitted"
        );
    }

    #[test]
    fn get_memory_background_model_prefers_memory_specific_override() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: Some("gpt-fast".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
            api_key_from_env: false,
        });
        config.memory = Some(MemoryConfig {
            background_model: Some("memory-fast".to_string()),
            ..MemoryConfig::default()
        });

        assert_eq!(
            config.get_memory_background_model().as_deref(),
            Some("memory-fast")
        );
    }

    #[test]
    fn preserve_env_sourced_provider_keys_restores_only_dropped_env_keys() {
        // #373: the settings-PATCH serde round-trip drops every provider's
        // skip_serializing api_key; an env-sourced key (no ciphertext) can't be
        // re-hydrated, so it must be copied back from the live `current` config —
        // but an explicitly re-set key and non-env keys must NOT be touched.
        let openai = |api_key: &str, from_env: bool| OpenAIConfig {
            api_key: api_key.to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
            api_key_from_env: from_env,
        };

        // Env-sourced key dropped by the round-trip → restored.
        let mut current = Config::default();
        current.providers.openai = Some(openai("sk-env", true));
        let mut merged = Config::default();
        merged.providers.openai = Some(openai("", false)); // post-round-trip
        merged.preserve_env_sourced_provider_keys(&current);
        let got = merged.providers.openai.as_ref().unwrap();
        assert_eq!(got.api_key, "sk-env", "env-sourced key restored");
        assert!(got.api_key_from_env, "env flag restored");

        // A key explicitly re-set by the patch is NOT overridden.
        let mut merged = Config::default();
        merged.providers.openai = Some(openai("sk-explicit", false));
        merged.preserve_env_sourced_provider_keys(&current);
        assert_eq!(
            merged.providers.openai.as_ref().unwrap().api_key,
            "sk-explicit",
            "explicit patch key must win"
        );

        // A non-env key in current is NOT restored here (that's ciphertext hydration's job).
        let mut current_plain = Config::default();
        current_plain.providers.openai = Some(openai("sk-plain", false));
        let mut merged = Config::default();
        merged.providers.openai = Some(openai("", false));
        merged.preserve_env_sourced_provider_keys(&current_plain);
        assert!(
            merged.providers.openai.as_ref().unwrap().api_key.is_empty(),
            "non-env key must not be restored by this path"
        );
    }

    #[test]
    fn refresh_preserves_ciphertext_when_plaintext_empty() {
        // #268: a provider whose stored ciphertext failed to decrypt at hydration
        // has an empty in-memory api_key. An unrelated later save must NOT null its
        // ciphertext — that would permanently drop a key the user never touched.
        let openai = |api_key: &str, enc: Option<&str>| OpenAIConfig {
            api_key: api_key.to_string(),
            api_key_encrypted: enc.map(str::to_string),
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
            api_key_from_env: false,
        };

        // Empty plaintext + existing ciphertext → ciphertext preserved (the bug).
        let mut config = Config::default();
        config.providers.openai = Some(openai("", Some("preexisting-ciphertext")));
        config
            .refresh_provider_api_keys_encrypted()
            .expect("refresh");
        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .unwrap()
                .api_key_encrypted
                .as_deref(),
            Some("preexisting-ciphertext"),
            "existing ciphertext must be preserved when plaintext is empty"
        );

        // Empty plaintext + no ciphertext → stays None (nothing to preserve).
        let mut config = Config::default();
        config.providers.openai = Some(openai("", None));
        config
            .refresh_provider_api_keys_encrypted()
            .expect("refresh");
        assert!(
            config
                .providers
                .openai
                .as_ref()
                .unwrap()
                .api_key_encrypted
                .is_none(),
            "no key + no ciphertext should stay None"
        );

        // Non-empty plaintext → (re)encrypted to a fresh, non-empty ciphertext.
        let mut config = Config::default();
        config.providers.openai = Some(openai("sk-live", Some("stale-ciphertext")));
        config
            .refresh_provider_api_keys_encrypted()
            .expect("refresh");
        let enc = config
            .providers
            .openai
            .as_ref()
            .unwrap()
            .api_key_encrypted
            .clone()
            .expect("ciphertext present");
        assert!(
            !enc.is_empty() && enc != "stale-ciphertext",
            "plaintext re-encrypted"
        );
    }

    #[test]
    fn get_memory_background_model_falls_back_to_provider_fast_model() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: Some("gpt-fast".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
            api_key_from_env: false,
        });

        assert_eq!(
            config.get_memory_background_model().as_deref(),
            Some("gpt-fast")
        );
    }

    #[test]
    fn get_memory_background_model_does_not_fall_back_to_main_model() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
            api_key_from_env: false,
        });

        assert!(config.get_memory_background_model().is_none());
    }

    #[test]
    fn memory_config_preserves_auto_dream_dream_refine_and_prompt_flags() {
        let config = Config {
            memory: Some(MemoryConfig {
                background_model: Some("dream-fast".to_string()),
                auto_dream_enabled: true,
                auto_dream_interval_secs: 900,
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: true,
                project_first_dream: false,
                dream_refine_mode: true,
                gardener_enabled: true,
                gardener_interval_secs: 3_600,
                gardener_volume_trigger: 40,
                gardener_max_splits_per_run: 4,
                gardener_min_sections: 7,
                dedup_gardener_enabled: true,
                dedup_gardener_min_score: 0.7,
                dedup_gardener_max_merges_per_run: 3,
                memory_active_capacity: 500,
                capacity_max_archivals_per_run: 10,
            }),
            ..Config::default()
        };

        let serialized = serde_json::to_string(&config).expect("config should serialize");
        let roundtrip: Config =
            serde_json::from_str(&serialized).expect("config should deserialize");
        let memory = roundtrip.memory.expect("memory config should exist");
        assert!(memory.auto_dream_enabled);
        assert!(!memory.project_prompt_injection);
        assert!(!memory.relevant_recall);
        assert!(memory.relevant_recall_rerank);
        assert!(!memory.project_first_dream);
        assert!(memory.dream_refine_mode);
        assert!(memory.gardener_enabled);
        assert_eq!(memory.gardener_interval_secs, 3_600);
        assert_eq!(memory.gardener_volume_trigger, 40);
        assert_eq!(memory.gardener_max_splits_per_run, 4);
        assert_eq!(memory.gardener_min_sections, 7);
        assert!(memory.dedup_gardener_enabled);
        assert_eq!(memory.dedup_gardener_min_score, 0.7);
        assert_eq!(memory.dedup_gardener_max_merges_per_run, 3);
        assert_eq!(memory.memory_active_capacity, 500);
        assert_eq!(memory.capacity_max_archivals_per_run, 10);
    }

    /// L5: capacity is OFF by default (0 = unbounded) — an opt-in feature.
    #[test]
    fn memory_active_capacity_defaults_off() {
        assert_eq!(MemoryConfig::default().memory_active_capacity, 0);
        assert_eq!(MemoryConfig::default().capacity_max_archivals_per_run, 50);
        let parsed: Config = serde_json::from_str(r#"{"memory":{}}"#).expect("parse");
        let memory = parsed.memory.unwrap();
        assert_eq!(memory.memory_active_capacity, 0);
        assert_eq!(
            memory.capacity_max_archivals_per_run, 50,
            "omitted field takes the serde default fn"
        );
    }

    /// L4: the maintenance integrators are ON by default — both via
    /// `MemoryConfig::default()` AND when a config file omits the flags entirely
    /// (serde `default = fn`, not the bare `#[serde(default)]` = `false`).
    #[test]
    fn memory_maintenance_integrators_default_on() {
        let defaults = MemoryConfig::default();
        assert!(defaults.auto_dream_enabled);
        assert!(defaults.gardener_enabled);
        assert!(defaults.dedup_gardener_enabled);
        assert_eq!(defaults.gardener_volume_trigger, 25);

        // A config that mentions `memory` but omits the flags must still be ON.
        let parsed: Config = serde_json::from_str(r#"{"memory":{}}"#).expect("parse");
        let memory = parsed.memory.expect("memory present");
        assert!(
            memory.auto_dream_enabled,
            "auto_dream on when field omitted"
        );
        assert!(memory.gardener_enabled, "gardener on when field omitted");
        assert!(
            memory.dedup_gardener_enabled,
            "dedup gardener on when field omitted"
        );
        // An explicit opt-out is still honored.
        let opted_out: Config =
            serde_json::from_str(r#"{"memory":{"gardener_enabled":false}}"#).expect("parse");
        assert!(!opted_out.memory.unwrap().gardener_enabled);
    }

    #[test]
    fn memory_config_env_overrides_prompt_flags() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());
        let _project_prompt = EnvVarGuard::set("BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION", "false");
        let _relevant_recall = EnvVarGuard::set("BAMBOO_MEMORY_RELEVANT_RECALL", "0");
        let _relevant_recall_rerank =
            EnvVarGuard::set("BAMBOO_MEMORY_RELEVANT_RECALL_RERANK", "yes");
        let _project_first_dream = EnvVarGuard::set("BAMBOO_MEMORY_PROJECT_FIRST_DREAM", "no");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let memory = config
            .memory
            .expect("memory config should be created by env overrides");
        assert!(!memory.project_prompt_injection);
        assert!(!memory.relevant_recall);
        assert!(memory.relevant_recall_rerank);
        assert!(!memory.project_first_dream);
    }

    #[test]
    fn provider_api_keys_injected_from_env_and_never_persisted() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());
        let _anthropic = EnvVarGuard::set("BAMBOO_ANTHROPIC_API_KEY", "sk-ant-from-env");
        let _openai = EnvVarGuard::set("BAMBOO_OPENAI_API_KEY", "sk-oai-from-env");

        // No config.json on disk → the providers are created from the env keys
        // alone (#253: deploy without a plaintext api_key in a mounted file).
        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(
            config
                .providers
                .anthropic
                .as_ref()
                .expect("anthropic created from env")
                .api_key,
            "sk-ant-from-env"
        );
        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .expect("openai created from env")
                .api_key,
            "sk-oai-from-env"
        );
        // An unset provider is not fabricated.
        assert!(config.providers.gemini.is_none());

        // The real "never persisted" guarantee: saving the config must NOT bake
        // the env key into config.json — not as plaintext AND not re-encrypted
        // into `api_key_encrypted` (which save's `refresh_provider_api_keys_encrypted`
        // would otherwise do). This is what actually happens on the server when
        // any unrelated setting is saved / on a fabric-reconcile boot.
        config
            .save_to_dir(temp_home.path.clone())
            .expect("save config");
        let on_disk = std::fs::read_to_string(temp_home.path.join("config.json"))
            .expect("read persisted config.json");
        assert!(
            !on_disk.contains("sk-ant-from-env") && !on_disk.contains("sk-oai-from-env"),
            "env key must not be persisted as plaintext"
        );
        let disk_json: serde_json::Value = serde_json::from_str(&on_disk).expect("parse");
        assert!(
            disk_json["providers"]["anthropic"]
                .get("api_key_encrypted")
                .is_none(),
            "env-sourced anthropic key must not be re-encrypted into config.json"
        );
        assert!(
            disk_json["providers"]["openai"]
                .get("api_key_encrypted")
                .is_none(),
            "env-sourced openai key must not be re-encrypted into config.json"
        );

        // And once the env vars are gone, a reload from that same dir has no key
        // (nothing was persisted).
        drop(_anthropic);
        drop(_openai);
        let reloaded = Config::from_data_dir(Some(temp_home.path.clone()));
        assert!(reloaded
            .providers
            .anthropic
            .as_ref()
            .map(|a| a.api_key.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn get_default_work_area_path_expands_tilde_and_requires_directory() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());
        let target = temp_home.path.join("workspace-default");
        std::fs::create_dir_all(&target).expect("default work area dir should exist");

        let config = Config {
            default_work_area: Some(DefaultWorkAreaConfig {
                path: Some("~/workspace-default".to_string()),
            }),
            ..Default::default()
        };

        assert_eq!(config.get_default_work_area_path(), Some(target));
    }

    #[test]
    fn get_default_work_area_path_returns_none_for_missing_directory() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());

        let config = Config {
            default_work_area: Some(DefaultWorkAreaConfig {
                path: Some("~/missing-default-work-area".to_string()),
            }),
            ..Default::default()
        };

        assert!(config.get_default_work_area_path().is_none());
    }

    #[test]
    fn normalize_tool_settings_trims_dedupes_canonicalizes_and_sorts() {
        let mut config = Config::default();
        config.tools.disabled = vec![
            "  read_file  ".to_string(),
            "".to_string(),
            "read_file".to_string(),
            "bash".to_string(),
            "default::getCurrentDir".to_string(),
        ];

        config.normalize_tool_settings();

        assert_eq!(config.tools.disabled, vec!["Bash", "GetCurrentDir", "Read"]);
    }

    #[test]
    fn config_load_reads_disabled_tools_as_canonical_names() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "tools": {
    "disabled": ["bash", " read_file ", "bash", "default::getCurrentDir"]
  }
}"#,
        );

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.tools.disabled, vec!["Bash", "GetCurrentDir", "Read"]);
        assert!(config.disabled_tool_names().contains("Bash"));
        assert!(config.disabled_tool_names().contains("Read"));
        assert!(config.disabled_tool_names().contains("GetCurrentDir"));
    }

    #[test]
    fn normalize_skill_settings_trims_dedupes_and_sorts() {
        let mut config = Config::default();
        config.skills.disabled = vec![
            " pdf ".to_string(),
            "".to_string(),
            "pdf".to_string(),
            "skill-creator".to_string(),
        ];

        config.normalize_skill_settings();

        assert_eq!(
            config.skills.disabled,
            vec!["pdf".to_string(), "skill-creator".to_string()]
        );
    }

    #[test]
    fn config_load_reads_disabled_skills_as_normalized_ids() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "skills": {
    "disabled": [" pdf ", "skill-creator", "pdf", ""]
  }
}"#,
        );

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(
            config.skills.disabled,
            vec!["pdf".to_string(), "skill-creator".to_string()]
        );
        assert!(config.disabled_skill_ids().contains("pdf"));
        assert!(config.disabled_skill_ids().contains("skill-creator"));
    }

    #[test]
    fn test_server_config_defaults() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.server.port, 9562);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.workers, 10);
        assert!(config.server.static_dir.is_none());
    }

    #[test]
    fn test_server_addr() {
        let mut config = Config::default();
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();
        assert_eq!(config.server_addr(), "0.0.0.0:9000");
    }

    #[test]
    fn test_env_var_overrides() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let _port = EnvVarGuard::set("BAMBOO_PORT", "9999");
        let _bind = EnvVarGuard::set("BAMBOO_BIND", "192.168.1.1");
        let _provider = EnvVarGuard::set("BAMBOO_PROVIDER", "openai");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.server.bind, "192.168.1.1");
        assert_eq!(config.provider, "openai");
    }

    #[test]
    fn test_config_save_and_load() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();
        config.provider = "anthropic".to_string();

        // Save
        config
            .save_to_dir(temp_home.path.clone())
            .expect("Failed to save config");

        // Load again
        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));

        // Verify
        assert_eq!(loaded.server.port, 9000);
        assert_eq!(loaded.server.bind, "0.0.0.0");
        assert_eq!(loaded.provider, "anthropic");
    }

    #[test]
    fn config_decrypts_proxy_auth_from_encrypted_field() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

        temp_home.set_config_json(&format!(
            r#"{{
  "http_proxy": "http://proxy.example.com:8080",
  "proxy_auth_encrypted": "{encrypted}"
}}"#
        ));
        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = config.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_decrypts_proxy_auth_from_legacy_scheme_encrypted_fields() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

        // Simulate older Bodhi/Tauri persisted config keys.
        temp_home.set_config_json(&format!(
            r#"{{
  "http_proxy": "http://proxy.example.com:8080",
  "http_proxy_auth_encrypted": "{encrypted}",
  "https_proxy_auth_encrypted": "{encrypted}"
}}"#
        ));

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = config.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_save_encrypts_proxy_auth_and_load_hydrates_plaintext() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.proxy_auth = Some(ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        });
        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should encrypt proxy auth");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("proxy_auth_encrypted"),
            "config.json should store encrypted proxy auth"
        );
        assert!(
            !content.contains("\"proxy_auth\""),
            "config.json should not store plaintext proxy_auth"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = loaded.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_save_encrypts_provider_api_keys_and_does_not_persist_plaintext() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-test-provider-key".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });

        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should encrypt provider api keys");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("\"api_key_encrypted\""),
            "config.json should store encrypted provider keys"
        );
        assert!(
            !content.contains("\"api_key\""),
            "config.json should not store plaintext provider keys"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let openai = loaded
            .providers
            .openai
            .expect("openai config should be present");
        assert_eq!(openai.api_key, "sk-test-provider-key");

        drop(key_guard);
    }

    #[test]
    fn config_save_persists_mcp_servers_in_mainstream_format() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));

        let mut env = std::collections::HashMap::new();
        env.insert("TOKEN".to_string(), "supersecret".to_string());

        config.mcp.servers = vec![
            bamboo_domain::mcp_config::McpServerConfig {
                id: "stdio-secret".to_string(),
                name: None,
                enabled: true,
                transport: bamboo_domain::mcp_config::TransportConfig::Stdio(
                    bamboo_domain::mcp_config::StdioConfig {
                        command: "echo".to_string(),
                        args: vec![],
                        cwd: None,
                        env,
                        env_encrypted: std::collections::HashMap::new(),
                        startup_timeout_ms: 5000,
                    },
                ),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: bamboo_domain::mcp_config::ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
            bamboo_domain::mcp_config::McpServerConfig {
                id: "sse-secret".to_string(),
                name: None,
                enabled: true,
                transport: bamboo_domain::mcp_config::TransportConfig::Sse(
                    bamboo_domain::mcp_config::SseConfig {
                        url: "http://localhost:8080/sse".to_string(),
                        headers: vec![bamboo_domain::mcp_config::HeaderConfig {
                            name: "Authorization".to_string(),
                            value: "Bearer token123".to_string(),
                            value_encrypted: None,
                        }],
                        connect_timeout_ms: 5000,
                    },
                ),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: bamboo_domain::mcp_config::ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
        ];

        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should persist MCP servers");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("\"mcpServers\""),
            "config.json should store MCP servers under the mainstream 'mcpServers' key"
        );
        assert!(
            content.contains("supersecret"),
            "config.json should persist MCP stdio env in mainstream format"
        );
        assert!(
            content.contains("Bearer token123"),
            "config.json should persist MCP SSE headers in mainstream format"
        );
        assert!(
            !content.contains("\"env_encrypted\""),
            "config.json should not persist legacy env_encrypted fields"
        );
        assert!(
            !content.contains("\"value_encrypted\""),
            "config.json should not persist legacy value_encrypted fields"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let stdio = loaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "stdio-secret")
            .expect("stdio server should exist");
        match &stdio.transport {
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                assert_eq!(
                    stdio.env.get("TOKEN").map(|s| s.as_str()),
                    Some("supersecret")
                );
            }
            _ => panic!("Expected stdio transport"),
        }

        let sse = loaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "sse-secret")
            .expect("sse server should exist");
        match &sse.transport {
            bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
                assert_eq!(sse.headers[0].value, "Bearer token123");
            }
            _ => panic!("Expected SSE transport"),
        }
    }

    // ── Env vars lifecycle tests ──────────────────────────────

    #[test]
    fn env_vars_as_map_includes_only_non_empty_values() {
        let config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "A".to_string(),
                    value: "val_a".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: None,
                },
                EnvVarEntry {
                    name: "B".to_string(),
                    value: "".to_string(), // empty → should be excluded
                    secret: true,
                    value_encrypted: None,
                    description: None,
                },
                EnvVarEntry {
                    name: "C".to_string(),
                    value: "  ".to_string(), // whitespace-only → excluded
                    secret: false,
                    value_encrypted: None,
                    description: None,
                },
                EnvVarEntry {
                    name: "D".to_string(),
                    value: "val_d".to_string(),
                    secret: true,
                    value_encrypted: Some("enc".to_string()),
                    description: Some("desc".to_string()),
                },
            ],
            ..Default::default()
        };

        let map = config.env_vars_as_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("A"), Some(&"val_a".to_string()));
        assert_eq!(map.get("D"), Some(&"val_d".to_string()));
        assert!(!map.contains_key("B"));
        assert!(!map.contains_key("C"));
    }

    #[test]
    fn sanitize_env_vars_for_disk_clears_secret_plaintext() {
        let mut config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "PLAIN".to_string(),
                    value: "visible".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: None,
                },
                EnvVarEntry {
                    name: "SECRET".to_string(),
                    value: "hidden_value".to_string(),
                    secret: true,
                    value_encrypted: Some("enc_data".to_string()),
                    description: None,
                },
            ],
            ..Default::default()
        };

        config.sanitize_env_vars_for_disk();

        assert_eq!(config.env_vars[0].value, "visible"); // plain kept
        assert_eq!(config.env_vars[1].value, ""); // secret cleared
    }

    #[test]
    fn sanitize_env_vars_for_disk_preserves_encrypted() {
        let mut config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "OPEN".to_string(),
                    value: "val".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: None,
                },
                EnvVarEntry {
                    name: "HIDDEN".to_string(),
                    value: "real_secret".to_string(),
                    secret: true,
                    value_encrypted: Some("enc".to_string()),
                    description: None,
                },
            ],
            ..Default::default()
        };

        config.sanitize_env_vars_for_disk();

        // Plain value untouched
        assert_eq!(config.env_vars[0].value, "val");
        // Secret plaintext cleared, but encrypted preserved
        assert_eq!(config.env_vars[1].value, "");
        assert_eq!(config.env_vars[1].value_encrypted.as_deref(), Some("enc"));
    }

    #[test]
    fn refresh_env_vars_encrypted_round_trip() {
        let mut config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "TOKEN".to_string(),
                    value: "my-secret-token".to_string(),
                    secret: true,
                    value_encrypted: None,
                    description: Some("A token".to_string()),
                },
                EnvVarEntry {
                    name: "PLAIN_VAR".to_string(),
                    value: "hello".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: None,
                },
            ],
            ..Default::default()
        };

        // Encrypt
        config
            .refresh_env_vars_encrypted()
            .expect("encryption should succeed");

        // Secret should now have encrypted value
        assert!(config.env_vars[0].value_encrypted.is_some());
        // Plain should have no encrypted value
        assert!(config.env_vars[1].value_encrypted.is_none());

        // Save encrypted value for later comparison
        let encrypted = config.env_vars[0].value_encrypted.clone().unwrap();
        assert_ne!(encrypted, "my-secret-token"); // shouldn't be plaintext

        // Clear plaintext (simulating disk write)
        config.sanitize_env_vars_for_disk();
        assert_eq!(config.env_vars[0].value, "");

        // Hydrate (simulating disk read)
        config.hydrate_env_vars_from_encrypted();
        assert_eq!(config.env_vars[0].value, "my-secret-token");
        assert_eq!(config.env_vars[1].value, "hello"); // plain untouched
    }

    #[test]
    fn publish_and_current_env_vars_round_trip() {
        let config = Config {
            env_vars: vec![EnvVarEntry {
                name: "TEST_PUBLISH".to_string(),
                value: "pub_value".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            }],
            ..Default::default()
        };

        for _ in 0..10 {
            config.publish_env_vars();
            let map = Config::current_env_vars();
            if map.get("TEST_PUBLISH") == Some(&"pub_value".to_string()) {
                return;
            }
        }
        panic!("TEST_PUBLISH not found in cache after retries");
    }

    #[test]
    fn broker_token_round_trips_encrypt_sanitize_hydrate() {
        let mut config = Config::default();
        config.subagents.broker = Some(BrokerClientConfig {
            endpoint: "ws://127.0.0.1:9600".to_string(),
            token: "super-secret-token".to_string(),
            token_encrypted: None,
        });

        // Persist path: encrypt then sanitize (what save_to_dir does).
        config.refresh_broker_token_encrypted().unwrap();
        config.sanitize_broker_token_for_disk();
        let broker = config.subagents.broker.as_ref().unwrap();
        assert!(broker.token.is_empty(), "plaintext cleared for disk");
        assert!(broker.token_encrypted.is_some(), "ciphertext stored");
        assert_ne!(
            broker.token_encrypted.as_deref(),
            Some("super-secret-token")
        );

        // Load path: hydrate restores plaintext.
        config.hydrate_broker_token_from_encrypted();
        assert_eq!(
            config.subagents.broker.as_ref().unwrap().token,
            "super-secret-token"
        );
    }

    #[test]
    fn broker_token_empty_refresh_preserves_ciphertext() {
        // A redacted round-trip (token empty) must not wipe the stored ciphertext.
        let mut config = Config::default();
        config.subagents.broker = Some(BrokerClientConfig {
            endpoint: "ws://h:9600".to_string(),
            token: String::new(),
            token_encrypted: Some("existing-cipher".to_string()),
        });
        config.refresh_broker_token_encrypted().unwrap();
        assert_eq!(
            config
                .subagents
                .broker
                .as_ref()
                .unwrap()
                .token_encrypted
                .as_deref(),
            Some("existing-cipher"),
        );
    }

    #[test]
    fn notifications_config_defaults_when_key_missing() {
        // Additive/back-compat: an absent `notifications` key must deserialize
        // to the built-in defaults (desktop auto, ntfy/bark disabled).
        let config: Config = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(config.notifications, NotificationsConfig::default());
        assert_eq!(config.notifications.desktop.enabled, None);
        assert!(!config.notifications.ntfy.enabled);
        assert_eq!(config.notifications.ntfy.base_url, "https://ntfy.sh");
        assert_eq!(config.notifications.ntfy.token, None);
        assert!(!config.notifications.bark.enabled);
        assert_eq!(config.notifications.bark.base_url, "https://api.day.app");
        assert_eq!(config.notifications.bark.device_key, None);
    }

    #[test]
    fn ntfy_token_round_trips_encrypt_serialize_hydrate() {
        let mut config = Config::default();
        config.notifications.ntfy = NtfyChannelConfig {
            enabled: true,
            base_url: "https://ntfy.sh".to_string(),
            topic: "bamboo-alerts".to_string(),
            token: Some("tk_super_secret".to_string()),
            token_encrypted: None,
        };

        // Persist path: encrypt (what save_to_dir does).
        config.refresh_notifications_encrypted().unwrap();
        assert!(config.notifications.ntfy.token_encrypted.is_some());
        assert_ne!(
            config.notifications.ntfy.token_encrypted.as_deref(),
            Some("tk_super_secret")
        );

        // `token` is `#[serde(skip_serializing)]` — never lands on disk, only
        // the ciphertext does.
        let json = serde_json::to_string(&config.notifications.ntfy).unwrap();
        assert!(
            !json.contains("tk_super_secret"),
            "plaintext token must never be serialized"
        );
        assert!(json.contains("token_encrypted"));

        // Load path: simulate a fresh load (plaintext gone, ciphertext present)
        // and confirm hydrate restores the plaintext.
        config.notifications.ntfy.token = None;
        config.hydrate_notifications_from_encrypted();
        assert_eq!(
            config.notifications.ntfy.token.as_deref(),
            Some("tk_super_secret")
        );
    }

    #[test]
    fn bark_device_key_round_trips_encrypt_serialize_hydrate() {
        let mut config = Config::default();
        config.notifications.bark = BarkChannelConfig {
            enabled: true,
            base_url: "https://api.day.app".to_string(),
            device_key: Some("dk_super_secret".to_string()),
            device_key_encrypted: None,
        };

        config.refresh_notifications_encrypted().unwrap();
        assert!(config.notifications.bark.device_key_encrypted.is_some());
        assert_ne!(
            config.notifications.bark.device_key_encrypted.as_deref(),
            Some("dk_super_secret")
        );

        let json = serde_json::to_string(&config.notifications.bark).unwrap();
        assert!(
            !json.contains("dk_super_secret"),
            "plaintext device key must never be serialized"
        );
        assert!(json.contains("device_key_encrypted"));

        config.notifications.bark.device_key = None;
        config.hydrate_notifications_from_encrypted();
        assert_eq!(
            config.notifications.bark.device_key.as_deref(),
            Some("dk_super_secret")
        );
    }

    #[test]
    fn notification_secrets_empty_refresh_preserves_ciphertext() {
        // A redacted round-trip (plaintext empty/absent) must not wipe the
        // stored ciphertext for either channel.
        let mut config = Config::default();
        config.notifications.ntfy.token_encrypted = Some("existing-ntfy-cipher".to_string());
        config.notifications.bark.device_key_encrypted = Some("existing-bark-cipher".to_string());

        config.refresh_notifications_encrypted().unwrap();

        assert_eq!(
            config.notifications.ntfy.token_encrypted.as_deref(),
            Some("existing-ntfy-cipher")
        );
        assert_eq!(
            config.notifications.bark.device_key_encrypted.as_deref(),
            Some("existing-bark-cipher")
        );
    }

    #[test]
    fn hydrate_skips_non_secret_entries() {
        let mut config = Config {
            env_vars: vec![EnvVarEntry {
                name: "PLAIN".to_string(),
                value: "original".to_string(),
                secret: false,
                value_encrypted: Some("should-be-ignored".to_string()),
                description: None,
            }],
            ..Default::default()
        };

        config.hydrate_env_vars_from_encrypted();
        // Non-secret entry should keep its original value
        assert_eq!(config.env_vars[0].value, "original");
    }

    #[test]
    fn default_config_has_empty_env_vars() {
        // `Config::default()` is a pure in-memory constructor (no disk read, no
        // env overrides), so this is independent of the developer's
        // `~/.bamboo/config.json` — no temp-dir isolation needed. Directly
        // asserts the #38 invariant that default() does not touch the filesystem.
        assert!(Config::default().env_vars.is_empty());
    }

    #[test]
    fn serde_round_trip_with_env_vars() {
        let config = Config {
            env_vars: vec![
                EnvVarEntry {
                    name: "KEY1".to_string(),
                    value: "val1".to_string(),
                    secret: false,
                    value_encrypted: None,
                    description: Some("First key".to_string()),
                },
                EnvVarEntry {
                    name: "KEY2".to_string(),
                    value: "".to_string(), // on-disk secret has no plaintext
                    secret: true,
                    value_encrypted: Some("enc123".to_string()),
                    description: None,
                },
            ],
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let restored: Config = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.env_vars.len(), 2);
        assert_eq!(restored.env_vars[0].name, "KEY1");
        assert_eq!(restored.env_vars[0].value, "val1");
        assert!(!restored.env_vars[0].secret);
        assert_eq!(restored.env_vars[1].name, "KEY2");
        assert!(restored.env_vars[1].secret);
        assert_eq!(
            restored.env_vars[1].value_encrypted.as_deref(),
            Some("enc123")
        );
    }

    // ---- defaults.* model resolution tests ----

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_model_prefers_defaults_chat_when_provider_model_ref_enabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("legacy-gpt-4o".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(config.get_model(), Some("claude-3-7-sonnet".to_string()));
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_model_ignores_defaults_chat_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("legacy-gpt-4o".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(config.get_model(), Some("legacy-gpt-4o".to_string()));
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_fast_model_prefers_defaults_fast_when_provider_model_ref_enabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_fast_model_ignores_defaults_fast_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("legacy-gpt-4o-mini".to_string())
        );
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_fast_model_falls_back_to_defaults_chat_when_fast_unset() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("claude-3-7-sonnet".to_string())
        );
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_memory_background_model_prefers_defaults_memory_background() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-4o-mini",
            )),
            task_summary: None,
            vision: None,
            memory_background: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_memory_background_model_falls_back_to_defaults_fast_when_memory_background_unset() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn get_memory_background_model_ignores_defaults_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            task_summary: None,
            vision: None,
            memory_background: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("legacy-gpt-4o-mini".to_string())
        );
    }

    // -------------------------------------------------------------------
    // `is_host_trusted` — plugin source-trust host allowlist (component
    // matching, not raw string-prefix matching; see the function's own docs
    // for the bypasses this closes).
    // -------------------------------------------------------------------

    #[test]
    fn is_host_trusted_requires_https_scheme() {
        let hosts = vec!["github.com/bigduu/".to_string()];
        assert!(!is_host_trusted("http://github.com/bigduu/x", &hosts));
        assert!(is_host_trusted("https://github.com/bigduu/x", &hosts));
    }

    #[test]
    fn is_host_trusted_is_case_insensitive_on_both_sides() {
        // A lowercase URL host against a mixed-case config entry...
        let hosts = vec!["GitHub.com/BigDuu/".to_string()];
        assert!(is_host_trusted("https://github.com/bigduu/x", &hosts));
        // ...and a mixed-case URL host against a lowercase config entry.
        let hosts = vec!["github.com/bigduu/".to_string()];
        assert!(is_host_trusted("https://GitHub.Com/bigduu/x", &hosts));
    }

    #[test]
    fn is_host_trusted_refuses_domain_gluing_bypass_of_a_bare_host_entry() {
        let hosts = vec!["trusted.example.com".to_string()];
        assert!(is_host_trusted("https://trusted.example.com/x", &hosts));
        // Both demonstrated bypasses of a raw string-prefix match: gluing a
        // longer attacker-controlled label onto the trusted host, with or
        // without a separating dot.
        assert!(!is_host_trusted(
            "https://trusted.example.com.evil.com/x",
            &hosts
        ));
        assert!(!is_host_trusted(
            "https://trusted.example.comevil.com/x",
            &hosts
        ));
    }

    #[test]
    fn is_host_trusted_refuses_sibling_path_prefix_bypass() {
        // No trailing slash on the config entry's path component.
        let hosts = vec!["github.com/bigduu/".to_string()];
        assert!(is_host_trusted("https://github.com/bigduu/x", &hosts));
        assert!(!is_host_trusted("https://github.com/bigduu-evil/x", &hosts));
    }

    #[test]
    fn is_host_trusted_bare_host_entry_matches_any_path_on_exactly_that_host() {
        let hosts = vec!["example.com".to_string()];
        assert!(is_host_trusted("https://example.com/", &hosts));
        assert!(is_host_trusted("https://example.com/any/deep/path", &hosts));
        // Still only that exact host — a bare-host entry must not become a
        // blanket "any host containing this string" match.
        assert!(!is_host_trusted("https://example.com.evil.com/", &hosts));
        assert!(!is_host_trusted("https://evil-example.com/", &hosts));
    }

    #[test]
    fn is_host_trusted_uses_the_real_host_not_userinfo() {
        let hosts = vec!["github.com/bigduu/".to_string()];
        // `user@host` userinfo does not change the actual host.
        assert!(is_host_trusted(
            "https://someuser@github.com/bigduu/x",
            &hosts
        ));
        // A decoy host placed in the userinfo position must not be mistaken
        // for the real host — the real host here is `evil.com`.
        assert!(!is_host_trusted(
            "https://github.com@evil.com/bigduu/",
            &hosts
        ));
    }

    #[test]
    fn is_host_trusted_ignores_an_explicit_port() {
        let hosts = vec!["github.com/bigduu/".to_string()];
        assert!(is_host_trusted("https://github.com:443/bigduu/x", &hosts));
    }

    #[test]
    fn is_host_trusted_malformed_url_is_refused_without_panicking() {
        let hosts = vec!["github.com/bigduu/".to_string()];
        assert!(!is_host_trusted("not a url at all", &hosts));
        assert!(!is_host_trusted("", &hosts));
        assert!(!is_host_trusted("github.com/bigduu/x", &hosts)); // no scheme
    }

    #[test]
    fn is_host_trusted_normalizes_dot_segments_before_matching() {
        let hosts = vec!["github.com/bigduu/".to_string()];
        // `Url::parse` resolves `..` segments before `path()` is ever
        // consulted, so this cannot be used to escape the trusted prefix.
        assert!(!is_host_trusted(
            "https://github.com/bigduu/../evil/x",
            &hosts
        ));
        // A `..` that stays under the trusted prefix once resolved is fine.
        assert!(is_host_trusted("https://github.com/bigduu/x/../y", &hosts));
    }

    #[test]
    fn normalize_plugin_trust_settings_lowercases_and_trims_and_drops_empties() {
        let mut config = Config::default();
        config.plugin_trust.trusted_hosts = vec![
            "  GitHub.com/BigDuu/ ".to_string(),
            "".to_string(),
            "   ".to_string(),
            "Example.COM".to_string(),
        ];
        config.normalize_plugin_trust_settings();
        assert_eq!(
            config.plugin_trust.trusted_hosts,
            vec!["github.com/bigduu/".to_string(), "example.com".to_string()]
        );
    }
}
