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
    /// Whether the ledger agenda (overdue/upcoming prospective records — todos,
    /// events, reminders) is injected into the main prompt. Free when the
    /// ledger is empty: the section is simply omitted.
    #[serde(
        default = "default_true_memory_ledger_agenda",
        alias = "memory_ledger_agenda_injection"
    )]
    pub ledger_agenda_injection: bool,
    /// Whether the background ledger gardener runs (expires past events/reminders,
    /// reconciles record↔schedule drift, distills completed records into durable
    /// memory). Expiry and reconciliation are deterministic and free; only
    /// distillation uses the background model, and it no-ops without one.
    #[serde(default = "default_true_ledger_gardener_enabled")]
    pub ledger_gardener_enabled: bool,
    /// Seconds between ledger gardener runs (default 6 hours).
    #[serde(default = "default_ledger_gardener_interval_secs")]
    pub ledger_gardener_interval_secs: u64,
    /// Whether the ledger gardener's distillation pass (completed records →
    /// durable memories via the background model) is enabled.
    #[serde(default = "default_true_ledger_distillation_enabled")]
    pub ledger_distillation_enabled: bool,
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
    /// Whether the background freshness gardener may conservatively demote Active
    /// day/week-granularity memories to Stale once they cross their documented
    /// staleness window (issue #61 phase 2; see
    /// `bamboo_memory::memory_store::freshness::granularity_expired`). Default ON,
    /// matching the other gardener passes: deterministic (no LLM, no cost), and
    /// non-destructive — it only ever moves Active → Stale, never archives or
    /// deletes. Set false to opt out.
    #[serde(default = "default_true_granularity_freshness_gardener_enabled")]
    pub granularity_freshness_gardener_enabled: bool,
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
            ledger_agenda_injection: default_true_memory_ledger_agenda(),
            ledger_gardener_enabled: default_true_ledger_gardener_enabled(),
            ledger_gardener_interval_secs: default_ledger_gardener_interval_secs(),
            ledger_distillation_enabled: default_true_ledger_distillation_enabled(),
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
            granularity_freshness_gardener_enabled:
                default_true_granularity_freshness_gardener_enabled(),
        }
    }
}

fn default_true_granularity_freshness_gardener_enabled() -> bool {
    true
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

fn default_true_memory_ledger_agenda() -> bool {
    true
}

fn default_true_ledger_gardener_enabled() -> bool {
    true
}

fn default_ledger_gardener_interval_secs() -> u64 {
    21_600
}

fn default_true_ledger_distillation_enabled() -> bool {
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

/// Per-run resource guardrails (issue #221): a cost/resource ceiling applied
/// across an entire `AgentRuntime::execute()` call (i.e. one user turn's worth
/// of internal rounds — the same "run" granularity `max_rounds` already uses).
///
/// Every field is `None` by default (unlimited), matching the rest of this
/// config's opt-in-only posture. A per-request `ExecuteRequest::run_budget`
/// override (HTTP `POST /execute` body) may only TIGHTEN this config-level
/// default, never loosen it — per field, the effective limit is the minimum
/// of the two (see [`RunBudgetConfig::merged_with_override`] and
/// `bamboo_engine::runtime::runtime::AgentRuntime::execute`).
///
/// Exceeding any configured limit gracefully stops the run (mirrors the
/// `max_rounds` exhaustion path: one final summary turn, then a terminal stop
/// with `runtime.completion_reason = "budget_exceeded"` on the session, plus a
/// structured `AgentEvent::BudgetExceeded`) rather than erroring out — the run
/// stays resumable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct RunBudgetConfig {
    /// Maximum total tokens (prompt + completion, actual provider-reported
    /// usage summed across the run's rounds) before the run is stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u64>,
    /// Maximum total tool calls (across every round of the run, not just one
    /// round — see `max_tool_calls_per_round` for the existing per-round cap)
    /// before the run is stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<u32>,
    /// Maximum total `SubAgent` create calls (across the whole run) before the
    /// run is stopped. Distinct from `subagents.max_concurrent`, which caps how
    /// many child actor processes run AT ONCE, not how many a single run may
    /// spawn in total over its lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subagents: Option<u32>,
}

/// Tighten-only per-field merge: the effective limit is the MINIMUM of the
/// config default and the request override, with `None` = unlimited.
fn min_limit<T: Ord + Copy>(config_default: Option<T>, request: Option<T>) -> Option<T> {
    match (config_default, request) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

impl RunBudgetConfig {
    /// Merge a per-request override with this config-level default,
    /// **tighten-only** (issue #221, PR #539 review): per field, the
    /// effective limit is the MINIMUM of the two (`None` = unlimited), so a
    /// `POST /execute` caller can lower a budget below the operator's
    /// configured ceiling but can never raise or remove it.
    ///
    /// Rationale: `run_budget` is a defensive cost circuit-breaker, and the
    /// server's other guardrails (`max_rounds`, per-round tool caps, …) are
    /// not client-overridable at all. A client-loosenable ceiling would be no
    /// ceiling: any caller of `/execute` could send
    /// `max_total_tokens: u64::MAX` and erase the operator's cap. Overrides
    /// looser than the config default are silently clamped to it rather than
    /// rejected — the caller still gets the strictest applicable budget,
    /// which is always a safe interpretation of their request.
    pub fn merged_with_override(&self, request_override: Option<&RunBudgetConfig>) -> Self {
        let Some(over) = request_override else {
            return *self;
        };
        Self {
            max_total_tokens: min_limit(self.max_total_tokens, over.max_total_tokens),
            max_tool_calls: min_limit(self.max_tool_calls, over.max_tool_calls),
            max_subagents: min_limit(self.max_subagents, over.max_subagents),
        }
    }
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
    /// Per-role allowlist scoping which host-bound MCP tools a sub-agent role
    /// may see/call through the orchestrator's MCP proxy (issue #54;
    /// `bamboo_broker::RoleToolAllowlist`). Read and enforced
    /// ORCHESTRATOR-side when wiring `serve_mcp_proxy` — this is deliberately
    /// NOT part of the worker-facing `McpProxyConfig` a deployed worker
    /// receives, because a worker self-declaring its own allowlist would be
    /// insecure (it could simply claim to be unrestricted). A role absent
    /// from this list is unrestricted (sees/can call every proxiable tool),
    /// so adding this policy never silently strips tools from an
    /// already-deployed role you have not listed here. Empty (the default)
    /// keeps every role unrestricted — fully backward compatible with
    /// pre-#54 behavior.
    ///
    /// Role AND tool names are matched by exact string equality against the
    /// worker-asserted `AgentRef.role` / the requested tool name — see
    /// `RoleToolAllowlist`'s doc comment for the resulting self-asserted-role
    /// caveat (this policy is adequate against a confused/hallucinating
    /// worker, not a malicious one that lies about its own role).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp_role_allowlist: Vec<McpRoleAllowlistEntry>,
}

/// One role's MCP proxy tool allowlist entry (issue #54). See
/// [`SubagentsConfig::mcp_role_allowlist`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct McpRoleAllowlistEntry {
    /// Sub-agent role this entry restricts — matches the worker-asserted
    /// `AgentRef.role` (itself the child session's `subagent_type` /
    /// `ChildIdentity.role`). There is no fixed registry of valid roles in
    /// this codebase (roles are free-form profile ids), so a typo here is
    /// NOT caught against a "known roles" list — only structurally (blank
    /// names are dropped, duplicates warn) at load time. Double-check this
    /// against the role string your profile/deploy config actually uses.
    pub role: String,
    /// Tool names this role may see in its manifest / call through the
    /// proxy, matched by exact string equality against the backend's
    /// registered tool name. An entry with an EMPTY list is an explicit
    /// lockout (no tools) for that role, distinct from the role being absent
    /// from this Vec entirely (unrestricted). Validated at load time against
    /// the orchestrator's live MCP tool set where available — an unknown
    /// name is still enforced (kept) but logged as a likely typo.
    #[serde(default)]
    pub tools: Vec<String>,
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
    /// Stable per-entry identifier (#496). Not part of the original schema —
    /// absent on legacy/hand-written entries and on a freshly-echoed new
    /// entry from a client. [`Config::save_to_dir`] assigns one (a random
    /// UUID) to any entry that lacks it as part of the normal save path
    /// (migration-on-write); load never mutates/rewrites the config to
    /// backfill it, per #493's never-overwrite-until-confirmed semantics.
    ///
    /// Used by [`crate::patch::preserve_masked_connect_secrets`] as the
    /// FIRST resolution strategy for a masked secret in a settings PATCH,
    /// ahead of the positional/`type`-based fallbacks (#490/#492) — an exact
    /// id match unambiguously identifies the same logical entry even when
    /// two entries share the same `platform_type` and have been reordered,
    /// which position+type alone cannot always disambiguate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
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
    /// Platform app id (Feishu `app_id`). Not a secret — serialized normally.
    /// Unused by the Telegram adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// Platform app secret (Feishu `app_secret`).
    ///
    /// Secret: encrypted at rest in `app_secret_encrypted`; this plaintext
    /// field is never serialized and is hydrated in memory on load (mirrors
    /// `token` above).
    #[serde(default, skip_serializing)]
    pub app_secret: Option<String>,
    /// Encrypted ciphertext of `app_secret` (the at-rest representation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_secret_encrypted: Option<String>,
    /// Platform domain/base-URL selector (Feishu-only today). Not a secret —
    /// serialized normally. `None`/`"feishu"` -> open.feishu.cn, `"lark"` ->
    /// open.larksuite.com, an `https://` value -> a private-deployment base
    /// URL used verbatim. Validation happens in the server registration arm,
    /// not here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
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

/// The official plugin-signing keys trusted by default, so an out-of-the-box
/// `bamboo plugin install <official release url>` needs no `--allow-unsigned`
/// for a bundle those repos' release CI signed. One entry per first-party
/// plugin publisher; each repo commits its public half as
/// `packaging/plugin/signing-key.pub` (nova) / `plugin/signing-key.pub`
/// (magpie) for cross-checking.
fn default_trusted_keys() -> Vec<TrustedKey> {
    vec![
        TrustedKey {
            label: "nova (bigduu official)".to_string(),
            algorithm: "ed25519".to_string(),
            public_key: "e3c429e1be50098b12c6f45737abf457189b668535875b5b3e2b4349be86ea59"
                .to_string(),
        },
        TrustedKey {
            label: "magpie (bigduu official)".to_string(),
            algorithm: "ed25519".to_string(),
            public_key: "47e971c39cd93adb18cff50e097cb387df49e9c4d33b0ed62f693eabbe7fc66e"
                .to_string(),
        },
    ]
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
    /// Persistent, config-level escape hatch for the whole three-layer
    /// policy above: `Off` makes every `url` plugin install/update behave as
    /// if `--insecure` (equivalently, `--allow-untrusted-host
    /// --allow-unsigned --allow-unverified`) were passed, WITHOUT needing the
    /// per-install flag every time — the "I run a private/dev bamboo and
    /// don't want to pass flags on every install" customization. Defaults to
    /// [`PluginTrustEnforcement::Strict`] — a fresh config, or one with no
    /// `plugin_trust.enforcement` key at all, is secure by default; relaxing
    /// it is always an explicit, user-initiated edit
    /// (`bamboo config set plugin_trust.enforcement off`), never a silent
    /// weakening. See `bamboo-server`'s `plugin_source.rs` for where this is
    /// enforced, and `AppState::new` for the loud startup warning emitted
    /// whenever a server boots with this set to `Off`.
    #[serde(default)]
    pub enforcement: PluginTrustEnforcement,
}

impl Default for PluginTrustConfig {
    fn default() -> Self {
        Self {
            trusted_hosts: default_trusted_hosts(),
            trusted_keys: default_trusted_keys(),
            enforcement: PluginTrustEnforcement::default(),
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

    /// True when `enforcement` is [`PluginTrustEnforcement::Off`] — every
    /// `url` plugin install/update should skip the host allowlist, signature,
    /// and checksum-requirement layers, exactly as if `--insecure` were
    /// passed to that individual install. See the field's doc comment for
    /// the full rationale.
    pub fn enforcement_is_off(&self) -> bool {
        matches!(self.enforcement, PluginTrustEnforcement::Off)
    }
}

/// `plugin_trust.enforcement`: the persistent, config-level form of the
/// `--insecure` escape hatch (see [`PluginTrustConfig::enforcement`]).
///
/// Deserialization accepts either the canonical string form (`"strict"` /
/// `"off"`, case-insensitive) or a bool-ish alias (`true` == `Strict`,
/// `false` == `Off`) for a hand-edited `config.json` — `true`/`false` read
/// naturally as "is enforcement on?". The string form is what
/// `bamboo config set plugin_trust.enforcement off` writes (and the only
/// form the generic dot-path setter's round-trip check accepts on write,
/// since this type always *serializes* back out as a string — see
/// `bamboo-config`'s `dot_path` module); the bool alias is a read-side
/// convenience for whoever edits `config.json` directly.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginTrustEnforcement {
    /// Secure by default: the host allowlist, signature, and checksum layers
    /// are all enforced (each individually waivable via
    /// `--allow-untrusted-host` / `--allow-unsigned` / `--allow-unverified`,
    /// or all at once via `--insecure`).
    #[default]
    Strict,
    /// Every `url` plugin install/update skips all three trust layers,
    /// without needing any per-install flag. Opt-in only — never the
    /// default for a fresh or pre-existing config.
    Off,
}

impl<'de> Deserialize<'de> for PluginTrustEnforcement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Bool(bool),
            Str(String),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Bool(true) => Ok(PluginTrustEnforcement::Strict),
            Repr::Bool(false) => Ok(PluginTrustEnforcement::Off),
            Repr::Str(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "strict" => Ok(PluginTrustEnforcement::Strict),
                "off" => Ok(PluginTrustEnforcement::Off),
                other => Err(serde::de::Error::custom(format!(
                    "invalid `plugin_trust.enforcement` value '{other}': expected \"strict\" or \
                     \"off\""
                ))),
            },
        }
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

    /// Config-level default per-run token/tool-call/subagent budget (issue
    /// #221). `None` fields are unlimited. A per-request `ExecuteRequest`
    /// override may only tighten these ceilings, never loosen them; see
    /// [`RunBudgetConfig::merged_with_override`].
    #[serde(default)]
    pub run_budget: RunBudgetConfig,

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

    /// In-memory-only marker set when this `Config` was recovered from a
    /// corrupt `config.json` at load time (salvage / `.bak` / defaults) and
    /// has not yet been confirmed. Never persisted (`#[serde(skip)]`), so
    /// every clean load starts at `None` and a fresh process only ever sees
    /// it populated right after `Config::from_data_dir` hit corruption.
    ///
    /// [`Config::save_to_dir`] refuses to overwrite `config.json` while this
    /// is `Some` and not `confirmed` — the corrupt original stays exactly as
    /// it was on disk until [`Config::confirm_recovery`] (or
    /// [`Config::confirm_recovery_and_save_to_dir`]) is called. #153.
    #[serde(skip)]
    pub recovery_status: Option<ConfigRecoveryStatus>,
}

/// Where a [`ConfigRecoveryStatus`]'s recovered values came from. #153.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConfigRecoverySource {
    /// Field-by-field salvage from the corrupt file itself
    /// ([`Config::salvage_partial`]); `fields` lists the top-level keys that
    /// were recovered from the corrupt document (any other field fell back to
    /// the backup/default baseline instead).
    Salvaged { fields: Vec<String> },
    /// Recovered wholesale from a `config.json.bak[.N]` generation
    /// (`generation` 0 == `.bak`, 1 == `.bak.1`, …).
    Backup { generation: usize },
    /// No usable salvage or backup; fell back to built-in defaults.
    Defaults,
}

/// Describes a pending config-corruption recovery (#153, following on from
/// #37/#135's quarantine + salvage/backup chain): `config.json` failed to
/// parse at load time, the corrupt original was quarantined (copied aside,
/// not deleted) to `quarantine_path`, and the owning [`Config`] holds the
/// recovered in-memory state instead.
///
/// [`Config::save_to_dir`] refuses to overwrite `config.json` while
/// `confirmed` is `false`, so a user who would rather hand-fix the original
/// isn't surprised by an automatic overwrite on the next save. Call
/// [`Config::confirm_recovery`] (or [`Config::confirm_recovery_and_save_to_dir`])
/// to allow the next save through.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConfigRecoveryStatus {
    /// Where the recovered values came from.
    pub source: ConfigRecoverySource,
    /// Absolute path of the preserved copy of the corrupt original
    /// (`config.json.corrupted.<nanos>`), or `None` if even the quarantine
    /// copy failed (the corrupt original still remains in place at
    /// `config.json` itself either way — quarantining copies, it doesn't
    /// move — so the guard below still applies).
    pub quarantine_path: Option<PathBuf>,
    /// Set `true` once the user has explicitly confirmed the recovery; only
    /// then may `save_to_dir` persist over the original `config.json`.
    pub confirmed: bool,
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

    /// Unconditionally replay a prior turn's `reasoning` as a `thinking`
    /// content block, regardless of whether bamboo captured a valid signature
    /// for it (issue #520).
    ///
    /// Defaults to `false`/absent, which is REQUIRED for real Anthropic: it
    /// requires `thinking` input blocks to carry a signature it minted itself,
    /// and bamboo never captures one, so an unconditionally-replayed block is
    /// always rejected with a 400 (either because it's foreign — minted by a
    /// different provider after a mid-session model switch — or because it's
    /// an unsigned copy of Claude's own prior turn).
    ///
    /// Set this to `true` only when pointing `base_url` at an
    /// Anthropic-COMPATIBLE upstream (e.g. GLM's `/anthropic` endpoint) that
    /// has the opposite contract: it requires the `thinking` block to be
    /// present whenever thinking is enabled, but never validates its
    /// signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_replay_always: Option<bool>,

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
                    // #37 / #135. Tag the recovered config with a
                    // ConfigRecoveryStatus (unconfirmed) so save_to_dir refuses to
                    // overwrite config.json until the caller confirms — the
                    // quarantined original stays hand-recoverable until then. #153.
                    tracing::warn!(
                        "Failed to parse config.json ({}); quarantining it and attempting recovery",
                        e
                    );
                    let quarantine_path = quarantine_corrupt_config(&config_path);
                    let (mut recovered, source) = Self::salvage_partial(&content, &data_dir)
                        .map(|(cfg, fields)| (cfg, ConfigRecoverySource::Salvaged { fields }))
                        .or_else(|| {
                            Self::load_backup(&data_dir).map(|(cfg, generation)| {
                                (cfg, ConfigRecoverySource::Backup { generation })
                            })
                        })
                        .unwrap_or_else(|| {
                            tracing::warn!(
                                "Could not salvage and no usable config.json.bak; using defaults"
                            );
                            (Self::create_default(), ConfigRecoverySource::Defaults)
                        });
                    recovered.recovery_status = Some(ConfigRecoveryStatus {
                        source,
                        quarantine_path,
                        confirmed: false,
                    });
                    recovered
                })
            } else {
                Self::create_default()
            }
        } else {
            Self::create_default()
        };

        // Phase-1 registrar migration: an existing sidecar is authoritative;
        // when absent, retain the legacy inline value loaded from config.json.
        // A malformed sidecar is never rewritten during load and the inline
        // value remains available, preventing a bad independent edit from
        // erasing the user's last usable configuration.
        let mut memory_module = crate::MemoryConfigModule(config.memory.clone());
        match memory_module.load_sync(&data_dir) {
            Ok(true) => config.memory = memory_module.0,
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "Failed to load memory.json; using legacy config.json memory: {error}"
            ),
        }
        let mut subagents_module = crate::SubagentsConfigModule(config.subagents.clone());
        match subagents_module.load_sync(&data_dir) {
            Ok(true) => config.subagents = subagents_module.0,
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "Failed to load subagents.json; using legacy config.json subagents: {error}"
            ),
        }
        let mut providers_module = crate::ProviderConfigsModule(config.providers.clone());
        match providers_module.load_sync(&data_dir) {
            Ok(true) => config.providers = providers_module.0,
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "Failed to load providers.json; using legacy config.json providers: {error}"
            ),
        }

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
        // Merge the standalone connect.json (#455) onto `config.connect`,
        // migrating a legacy inline `connect` key from config.json (#453
        // state) when present. MUST run before the token hydration below so
        // it decrypts the post-merge ciphertext, not a stale/legacy copy.
        config.merge_connect_config(&data_dir);
        // One-time (idempotent) sweep of the rotated config.json.bak[.N]
        // generations for a legacy embedded `connect` sub-tree left behind by
        // a pre-#455 build (#468, follow-up to #457). Independent of whether
        // `merge_connect_config` just migrated the CURRENT config.json above —
        // an instance that was already migrated by an earlier run of this
        // binary has a clean config.json today but may still carry the
        // legacy key in an untouched `.bak`/`.bak.1`/`.bak.2` generation, since
        // backup rotation only overwrites those on a fresh SAVE. Runs on every
        // load but is cheap and a no-op once every generation has been swept.
        scrub_legacy_connect_from_config_backups(&data_dir);
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

    /// Merge the standalone `connect.json` (#455) onto `self.connect`, the
    /// load-side counterpart of [`save_connect_config`]. Called once per load,
    /// BEFORE [`Config::hydrate_connect_platform_tokens_from_encrypted`] runs,
    /// so hydration decrypts the POST-merge ciphertext rather than a stale
    /// copy still embedded in `config.json`.
    ///
    /// - `connect.json` present & parseable: authoritative — OVERWRITES
    ///   whatever `self.connect` currently holds. If `self.connect` was ALSO
    ///   non-empty (a legacy inline `connect` key still in config.json, e.g.
    ///   #453-era state, or written by an older binary), that's a stale
    ///   duplicate: log a warning and proactively strip the superseded key
    ///   from config.json now (#457) rather than waiting for the next
    ///   natural save — cheap, and consistent with not spreading token
    ///   ciphertext across files.
    /// - `connect.json` present but corrupt/unparsable: fail SAFE for this
    ///   security-sensitive feature. Log an error, quarantine the bad file to
    ///   `connect.json.bak` (best-effort), and continue with an EMPTY
    ///   `ConnectConfig` — never falls back to a legacy config.json copy.
    /// - `connect.json` absent & `self.connect` non-empty (pure legacy
    ///   state): migrate proactively. Adopt the legacy value (already parsed
    ///   into `self`) and persist it: strip the `connect` key from
    ///   config.json and write connect.json (#457 — NOT a full
    ///   [`Config::save_to_dir`], which would re-encrypt every OTHER secret
    ///   in config.json and rotate its backups as a load-time side effect,
    ///   even for a read-only command like `bamboo config get`), logged at
    ///   info.
    /// - `connect.json` absent & `self.connect` empty: nothing to do.
    fn merge_connect_config(&mut self, data_dir: &std::path::Path) {
        let connect_path = data_dir.join("connect.json");
        match std::fs::read_to_string(&connect_path) {
            Ok(content) => match serde_json::from_str::<ConnectConfig>(&content) {
                Ok(connect) => {
                    let legacy_key_present = !connect_config_is_empty(&self.connect);
                    if legacy_key_present {
                        tracing::warn!(
                            "config.json still has a legacy `connect` key alongside \
                             connect.json; connect.json takes precedence — dropping the \
                             stale key from config.json now"
                        );
                    }
                    self.connect = connect;
                    if legacy_key_present {
                        strip_legacy_connect_key_from_config_json(data_dir);
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to parse {:?} ({}); continuing with an empty (inert) \
                         connect config instead of falling back to a legacy config.json copy",
                        connect_path,
                        e
                    );
                    quarantine_corrupt_connect(&connect_path);
                    self.connect = ConnectConfig::default();
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if !connect_config_is_empty(&self.connect) {
                    tracing::info!(
                        "Migrating legacy `connect` config from config.json to a \
                         standalone connect.json"
                    );
                    // Narrow migration write (#457): strip only the `connect` key
                    // from config.json and write connect.json directly, instead of
                    // routing through a full `save_to_dir` (see doc comment above).
                    strip_legacy_connect_key_from_config_json(data_dir);
                    if let Err(e) = save_connect_config(&self.connect, data_dir) {
                        tracing::error!("Failed to write connect.json during migration: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "Failed to read {:?} ({}); continuing with an empty (inert) connect config",
                    connect_path,
                    e
                );
                self.connect = ConnectConfig::default();
            }
        }
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
    /// corrupt. Walks newest -> oldest and returns the first that parses (paired
    /// with its generation index, 0 == `.bak`, for [`ConfigRecoverySource::Backup`]);
    /// `None` if every generation is missing or also unparseable. #37 / #135.
    fn load_backup(data_dir: &std::path::Path) -> Option<(Self, usize)> {
        let config_path = data_dir.join("config.json");
        for gen in 0..BAK_GENERATIONS {
            let backup = backup_path_for(&config_path, gen);
            let Ok(content) = std::fs::read_to_string(&backup) else {
                continue;
            };
            match Self::parse_and_hydrate(&content) {
                Ok(config) => {
                    tracing::info!("Recovered configuration from {:?}", backup);
                    return Some((config, gen));
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
    ///
    /// Returns the hydrated salvaged config paired with the top-level keys that
    /// were actually recovered from the corrupt document (used to populate
    /// [`ConfigRecoverySource::Salvaged`]).
    fn salvage_partial(content: &str, data_dir: &std::path::Path) -> Option<(Self, Vec<String>)> {
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
            .and_then(|(backup, _generation)| serde_json::to_value(backup).ok())
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
        Self::parse_and_hydrate(&rebuilt)
            .ok()
            .map(|config| (config, salvaged))
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
            run_budget: RunBudgetConfig::default(),
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
            recovery_status: None,
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

    /// Persist only the memory module, leaving every other config file untouched.
    pub fn save_memory_to_dir(&self, data_dir: &std::path::Path) -> Result<()> {
        crate::MemoryConfigModule(self.memory.clone()).save_sync(data_dir)
    }

    /// Persist only the sub-agent module, leaving every other config file untouched.
    pub fn save_subagents_to_dir(&self, data_dir: &std::path::Path) -> Result<()> {
        crate::SubagentsConfigModule(self.subagents.clone()).save_sync(data_dir)
    }

    /// Persist only provider configuration. Provider plaintext keys are first
    /// refreshed into their encrypted at-rest representation.
    pub fn save_providers_to_dir(&self, data_dir: &std::path::Path) -> Result<()> {
        let mut config = self.clone();
        config.refresh_provider_api_keys_encrypted()?;
        crate::ProviderConfigsModule(config.providers).save_sync(data_dir)
    }

    /// The pending config-corruption recovery, if `config.json` failed to
    /// parse on load and the recovery hasn't been confirmed yet. `None` on
    /// every clean load. #153.
    pub fn recovery_status(&self) -> Option<&ConfigRecoveryStatus> {
        self.recovery_status.as_ref()
    }

    /// Confirm a pending recovery, allowing the next [`Config::save`] /
    /// [`Config::save_to_dir`] to overwrite the quarantined-corrupt
    /// `config.json` with this recovered state. No-op if there's no pending
    /// recovery. Prefer [`Config::confirm_recovery_and_save_to_dir`], which
    /// also persists and clears the flag in one step. #153.
    pub fn confirm_recovery(&mut self) {
        if let Some(status) = self.recovery_status.as_mut() {
            status.confirmed = true;
        }
    }

    /// Confirm a pending recovery AND persist it in one step: marks it
    /// confirmed (satisfying the [`Config::save_to_dir`] guard), writes the
    /// recovered state to `config.json`, then clears `recovery_status`
    /// entirely — once this succeeds the config is no longer "pending
    /// confirmation", it's just the normal on-disk config. Errors (and
    /// leaves `recovery_status` untouched) if there's nothing pending, or if
    /// the save itself fails. #153.
    pub fn confirm_recovery_and_save_to_dir(&mut self, data_dir: PathBuf) -> Result<()> {
        if self.recovery_status.is_none() {
            anyhow::bail!("No pending config-corruption recovery to confirm");
        }
        self.confirm_recovery();
        self.save_to_dir(data_dir)?;
        self.recovery_status = None;
        Ok(())
    }

    /// Assign a stable [`ConnectPlatformConfig::id`] to every `connect.platforms`
    /// entry that doesn't already have one (#496).
    ///
    /// Migration-on-write: [`Config::save_to_dir`] always calls this on its
    /// internal save-copy before persisting, so every path that writes
    /// `connect.json` gets ids backfilled. Callers that mutate the *live*
    /// in-memory config as part of a save (e.g. the server's settings-PATCH
    /// handler) should also call this directly on that in-memory value
    /// before responding, so a client that echoes the response straight
    /// back round-trips the id immediately rather than only after the next
    /// reload/restart. Never called from load — a config that's never saved
    /// again (e.g. one sitting in an unconfirmed-recovery state, see #493)
    /// is never rewritten just to backfill ids. An entry that already has
    /// an id keeps it unchanged; ids are never reassigned or deduplicated
    /// once set.
    pub fn assign_connect_platform_ids(&mut self) {
        for platform in &mut self.connect.platforms {
            if platform.id.is_none() {
                platform.id = Some(uuid::Uuid::new_v4().to_string());
            }
        }
    }

    /// Save configuration to disk under the provided data directory.
    ///
    /// Configuration is always stored as `{data_dir}/config.json`.
    ///
    /// Refuses to write when this config carries an unconfirmed
    /// [`ConfigRecoveryStatus`] (#153) — i.e. it was recovered from a corrupt
    /// `config.json` and the recovery hasn't been confirmed — so a corrupt
    /// original a user might want to hand-fix is never silently clobbered by
    /// an auto-persisted recovery. Call [`Config::confirm_recovery`] (or
    /// [`Config::confirm_recovery_and_save_to_dir`]) first.
    pub fn save_to_dir(&self, data_dir: PathBuf) -> Result<()> {
        if let Some(status) = self.recovery_status.as_ref().filter(|s| !s.confirmed) {
            anyhow::bail!(
                "refusing to overwrite config.json: it was recovered from corruption ({:?}) and \
                 has not been confirmed; the corrupt original is preserved at {:?}. Call \
                 Config::confirm_recovery (or the recovery-confirm API) first. (#153)",
                status.source,
                status.quarantine_path,
            );
        }

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
        // `subagents.broker` is `#[serde(skip)]` (runtime-only, lives in its own
        // broker.json / embedded in-process) — nothing to encrypt or persist here.
        to_save.refresh_encrypted_secrets()?;
        to_save.sanitize_env_vars_for_disk();
        to_save.sanitize_cluster_fabric_for_disk();
        to_save.assign_connect_platform_ids();
        to_save.normalize_tool_settings();
        to_save.normalize_skill_settings();

        // Split `connect` (#455) out of the config.json document: bamboo-connect
        // platform-bridge credentials (bot tokens, allowlists) get their own
        // sibling file, connect.json (written below), instead of living in
        // config.json — different sensitivity/lifecycle. The `connect` FIELD on
        // `Config` keeps its normal serde shape unchanged (still required by the
        // settings API / `preserve_masked_connect_secrets`, which operate on the
        // in-memory struct) — only the serialized DOCUMENT that becomes
        // config.json's bytes has the key stripped, and that's done on the
        // `serde_json::Value` here, not via `#[serde(skip)]` on the field.
        let mut config_value =
            serde_json::to_value(&to_save).context("Failed to serialize config to JSON")?;
        if let Some(obj) = config_value.as_object_mut() {
            obj.remove("connect");
            obj.remove("memory");
            obj.remove("subagents");
            obj.remove("providers");
        }
        let content = serde_json::to_string_pretty(&config_value)
            .context("Failed to serialize config to JSON")?;

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

        crate::MemoryConfigModule(to_save.memory.clone()).save_sync(&data_dir)?;
        crate::SubagentsConfigModule(to_save.subagents.clone()).save_sync(&data_dir)?;
        crate::ProviderConfigsModule(to_save.providers.clone()).save_sync(&data_dir)?;
        save_connect_config(&to_save.connect, &data_dir)?;

        Ok(())
    }
}

/// Persist `connect` (#455) to its own sibling file, `connect.json`, next to
/// config.json — the save-side counterpart of [`Config::merge_connect_config`].
///
/// Only writes when the config is non-empty OR the file already exists, so a
/// fresh/default install with no platforms configured never gets a
/// `connect.json` littering its data dir. Before an existing file is
/// overwritten, it's copied aside to a single `connect.json.bak` generation
/// (best-effort) — connect.json doesn't need config.json's multi-generation
/// rotation, one last-known-good snapshot is enough.
fn save_connect_config(connect: &ConnectConfig, data_dir: &std::path::Path) -> Result<()> {
    let path = data_dir.join("connect.json");
    if connect_config_is_empty(connect) && !path.exists() {
        return Ok(());
    }

    if path.exists() {
        let backup = path.with_extension("json.bak");
        if let Err(e) = std::fs::copy(&path, &backup) {
            tracing::warn!("Failed to back up connect.json before save: {}", e);
        }
    }

    let content = serde_json::to_string_pretty(connect)
        .context("Failed to serialize connect config to JSON")?;
    write_atomic(&path, content.as_bytes())
        .with_context(|| format!("Failed to write connect config file: {:?}", path))?;
    Ok(())
}

/// Remove the legacy inline `connect` key from `config.json` on disk, if
/// present — the narrow, load-side counterpart of the full-document rewrite
/// [`Config::save_to_dir`] would otherwise perform just to drop one stale
/// key. Used by [`Config::merge_connect_config`] both when adopting a
/// pure-legacy `connect` key (migration) and when a stale legacy key lingers
/// alongside an authoritative connect.json. #457.
///
/// Operates on the raw `serde_json::Value` read straight from disk — NOT on
/// the typed `Config` — so it touches nothing but the one key: no other
/// secret gets re-encrypted, and no `config.json.bak` generation gets
/// rotated, as a side effect of a load.
///
/// Best-effort: read/parse/write failures are logged, not propagated — this
/// runs as a side effect of `Config::new()` / load, which has no `Result` to
/// surface it through. A failure here just leaves the stale key in place
/// until the next natural save; connect.json (written separately) is already
/// authoritative in memory either way.
fn strip_legacy_connect_key_from_config_json(data_dir: &std::path::Path) {
    let config_path = data_dir.join("config.json");
    let content = match std::fs::read_to_string(&config_path) {
        Ok(content) => content,
        Err(e) => {
            tracing::error!(
                "Failed to read config.json to strip legacy `connect` key: {}",
                e
            );
            return;
        }
    };
    let mut value: serde_json::Value = match serde_json::from_str(&content) {
        Ok(value) => value,
        Err(e) => {
            tracing::error!(
                "Failed to parse config.json to strip legacy `connect` key: {}",
                e
            );
            return;
        }
    };
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    if obj.remove("connect").is_none() {
        // Nothing to strip (e.g. raced with a concurrent save that already
        // dropped it) — avoid an unnecessary rewrite.
        return;
    }
    let rewritten = match serde_json::to_string_pretty(&value) {
        Ok(rewritten) => rewritten,
        Err(e) => {
            tracing::error!(
                "Failed to serialize config.json after stripping legacy `connect` key: {}",
                e
            );
            return;
        }
    };
    if let Err(e) = write_atomic(&config_path, rewritten.as_bytes()) {
        tracing::error!(
            "Failed to write config.json after stripping legacy `connect` key: {}",
            e
        );
    }
}

/// Sweep the rotated `config.json.bak[.N]` generations for a legacy embedded
/// `connect` sub-tree that predates the #455 connect.json split, and strip it
/// in place. #468 (follow-up to #457).
///
/// `strip_legacy_connect_key_from_config_json` only ever rewrites the CURRENT
/// `config.json` — it never reaches into `.bak` generations, and the normal
/// backup-rotation path (see [`rotate_backups`]) only overwrites a `.bak[.N]`
/// slot as a side effect of a fresh SAVE. An instance that upgraded from a
/// pre-#455 build but rarely (or never) triggers a config save can therefore
/// carry the legacy, encrypted `connect` sub-tree — including bot tokens, an
/// immediately-usable remote-control credential — in an old backup generation
/// indefinitely, even after its live config.json has long since been
/// migrated.
///
/// Deliberately surgical, mirroring the `.bak` files' role as the user's
/// recovery net (#493's "backups are a low-sensitivity snapshot, don't fuss
/// with them" posture):
/// - a generation that doesn't exist, or that fails to parse as JSON, is
///   SKIPPED — logged, never deleted, never guessed at. Corrupt/foreign
///   content in a `.bak` slot is left exactly as found for hand inspection.
/// - a generation that parses but carries no `connect` key (the overwhelming
///   majority, especially on any instance that predates this fix by more
///   than `BAK_GENERATIONS` saves) is left COMPLETELY untouched — not even a
///   byte-identical rewrite — so its mtime and on-disk bytes survive.
/// - only a generation that actually parses AND carries the legacy key gets
///   rewritten, via the same key-removal-on-the-raw-`Value` + `write_atomic`
///   approach as `strip_legacy_connect_key_from_config_json`, so every other
///   byte of that snapshot (all other settings, formatting aside) survives.
///
/// Runs unconditionally on every load (not gated on the CURRENT config.json
/// still carrying the legacy key) specifically to catch already-migrated
/// installs whose backups predate this fix. Cheap: at most `BAK_GENERATIONS`
/// small file reads, and a genuine no-op (zero writes) once every generation
/// has been swept once. Best-effort like its sibling: failures are logged,
/// not propagated, since this runs as a side effect of `Config::new()` /
/// load, which has no `Result` to surface it through.
fn scrub_legacy_connect_from_config_backups(data_dir: &std::path::Path) {
    let config_path = data_dir.join("config.json");
    for gen in 0..BAK_GENERATIONS {
        let backup = backup_path_for(&config_path, gen);
        let content = match std::fs::read_to_string(&backup) {
            Ok(content) => content,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "Failed to read {:?} while scanning for legacy connect data ({}); \
                         leaving it untouched",
                        backup,
                        e
                    );
                }
                continue;
            }
        };
        let mut value: serde_json::Value = match serde_json::from_str(&content) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(
                    "Skipping unparsable backup {:?} while scanning for legacy connect data \
                     ({}); left untouched (never deleted)",
                    backup,
                    e
                );
                continue;
            }
        };
        let Some(obj) = value.as_object_mut() else {
            // Not a JSON object (e.g. `null`/an array) — nothing to strip, and
            // not a shape we should try to rewrite. Leave it alone.
            continue;
        };
        if obj.remove("connect").is_none() {
            // No legacy key in this generation — skip without writing so the
            // file's bytes/mtime are left completely untouched.
            continue;
        }
        let rewritten = match serde_json::to_string_pretty(&value) {
            Ok(rewritten) => rewritten,
            Err(e) => {
                tracing::error!(
                    "Failed to serialize {:?} after stripping legacy connect data: {}",
                    backup,
                    e
                );
                continue;
            }
        };
        match write_atomic(&backup, rewritten.as_bytes()) {
            Ok(()) => tracing::info!(
                "Scrubbed legacy embedded connect data from backup generation {:?} (#468)",
                backup
            ),
            Err(e) => tracing::error!(
                "Failed to write {:?} after stripping legacy connect data: {}",
                backup,
                e
            ),
        }
    }
}

/// Quarantine an unparsable `connect.json` to a single `connect.json.bak`
/// generation (best-effort) so the bad content survives for inspection
/// instead of being silently discarded. Unlike config.json's timestamped,
/// N-generation quarantine, connect.json only needs one slot — it's a much
/// smaller, less complex document and this is a fail-SAFE (empty/inert
/// bridge), not a fail-recover, posture. #455.
///
/// MOVES the corrupt file rather than copying it (#457): a copy would leave
/// the same corrupt `connect.json` sitting in the data dir right next to its
/// own quarantine copy, which reads as confusing/ambiguous mid-incident
/// (which one is live?). `rename` is used first (atomic, no partial-copy
/// window); if that fails — e.g. `connect.json.bak` and the data dir are on
/// different filesystems — fall back to copy-then-remove so the corrupt
/// original still doesn't linger.
fn quarantine_corrupt_connect(connect_path: &std::path::Path) {
    let backup = connect_path.with_extension("json.bak");
    match std::fs::rename(connect_path, &backup) {
        Ok(()) => tracing::warn!("Quarantined corrupt connect.json to {:?}", backup),
        Err(e) => {
            tracing::warn!(
                "Failed to rename corrupt connect.json to {:?} ({}); falling back to copy+remove",
                backup,
                e
            );
            if let Err(e) = std::fs::copy(connect_path, &backup) {
                tracing::error!("Failed to quarantine corrupt connect.json: {}", e);
                return;
            }
            if let Err(e) = std::fs::remove_file(connect_path) {
                tracing::error!(
                    "Quarantined corrupt connect.json to {:?} but failed to remove the \
                     original {:?}: {}",
                    backup,
                    connect_path,
                    e
                );
            }
        }
    }
}

/// How many `config.json.corrupted.*` quarantine files to keep. Each corrupt load
/// drops one; without a cap they accumulate unbounded. Newest `N` are retained.
const QUARANTINE_KEEP: usize = 5;

/// Copy a corrupt config file aside to `config.json.corrupted.<nanos>` so the
/// user's (unparseable) configuration is preserved for inspection/recovery
/// instead of being silently discarded and then overwritten by defaults. #37.
///
/// Returns the quarantine path on success, so the caller can attach it to a
/// [`ConfigRecoveryStatus`] (#153); `None` if even the copy failed (the
/// corrupt original is still left in place at `config_path` regardless, since
/// this only ever copies, never moves/deletes).
fn quarantine_corrupt_config(config_path: &std::path::Path) -> Option<PathBuf> {
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
    let result = match std::fs::copy(config_path, &quarantine) {
        Ok(_) => {
            tracing::warn!("Quarantined corrupt config.json to {:?}", quarantine);
            Some(quarantine)
        }
        Err(e) => {
            tracing::error!("Failed to quarantine corrupt config.json: {}", e);
            None
        }
    };
    prune_quarantine_files(config_path, QUARANTINE_KEEP);
    result
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

pub(crate) fn write_atomic(path: &std::path::Path, content: &[u8]) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return std::fs::write(path, content);
    };

    std::fs::create_dir_all(parent)?;

    // Write to a temp file in the same directory then rename to ensure atomic replace.
    // (Rename is atomic on Unix when source/dest are on the same filesystem.)
    //
    // The temp name must be unique PER CALL, not just per-process (issue
    // #486): it used to be derived from `process_id()` alone, which is
    // IDENTICAL across every thread of the same process. Two `write_atomic`
    // calls racing on the same directory (observed: two `#[test]` fns in
    // this file's suite, run concurrently by the default multi-threaded
    // test harness) therefore computed the exact same `tmp_path`. Whichever
    // caller's `File::create` ran second truncated the first caller's
    // in-flight temp file out from under it; whichever caller's `rename`
    // then lost the race failed with ENOENT (its temp file had already been
    // renamed away by the other caller) — reproducing
    // `save_rotates_backup_generations`'s exact CI failure: "Failed to
    // write config file ... No such file or directory (os error 2)". A
    // monotonic per-process counter alongside the PID makes every call's
    // temp file distinct, regardless of how many callers target the same
    // directory concurrently.
    static NEXT_TMP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let unique = NEXT_TMP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("config.json");
    let tmp_name = format!(".{}.tmp.{}.{}", file_name, std::process::id(), unique);
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
    fn run_budget_config_merge_is_tighten_only_per_field() {
        let config_default = RunBudgetConfig {
            max_total_tokens: Some(100_000),
            max_tool_calls: Some(500),
            max_subagents: Some(10),
        };

        // No override at all: config default passes through unchanged.
        assert_eq!(
            config_default.merged_with_override(None),
            config_default,
            "no override falls back to the config default entirely"
        );

        // Override TIGHTENS exactly one field; the other two keep the config
        // default (per-field, not all-or-nothing).
        let tighten_one = RunBudgetConfig {
            max_total_tokens: Some(5_000),
            max_tool_calls: None,
            max_subagents: None,
        };
        let merged = config_default.merged_with_override(Some(&tighten_one));
        assert_eq!(merged.max_total_tokens, Some(5_000));
        assert_eq!(merged.max_tool_calls, Some(500));
        assert_eq!(merged.max_subagents, Some(10));

        // A LOOSER override is clamped to the config default: a client can
        // never raise the operator's ceiling (PR #539 review, finding #3).
        let loosen_attempt = RunBudgetConfig {
            max_total_tokens: Some(999_999_999),
            max_tool_calls: Some(10_000),
            max_subagents: Some(1_000),
        };
        assert_eq!(
            config_default.merged_with_override(Some(&loosen_attempt)),
            config_default,
            "looser per-request values must be clamped to the config ceiling"
        );

        // Nor can it REMOVE a configured ceiling by omitting the field: an
        // absent override field keeps the config default, it does not mean
        // unlimited.
        let empty_override = RunBudgetConfig::default();
        assert_eq!(
            config_default.merged_with_override(Some(&empty_override)),
            config_default,
            "an all-absent override body keeps every configured ceiling"
        );

        // An unlimited config default CAN be tightened by the request (the
        // request is the only ceiling then), and stays unlimited on fields the
        // request does not set.
        let unlimited_default = RunBudgetConfig::default();
        let merged = unlimited_default.merged_with_override(Some(&tighten_one));
        assert_eq!(merged.max_total_tokens, Some(5_000));
        assert_eq!(merged.max_tool_calls, None);
        assert_eq!(merged.max_subagents, None);
    }

    #[test]
    fn run_budget_config_json_round_trips_and_defaults_are_unlimited() {
        assert_eq!(RunBudgetConfig::default().max_total_tokens, None);
        assert_eq!(RunBudgetConfig::default().max_tool_calls, None);
        assert_eq!(RunBudgetConfig::default().max_subagents, None);

        let json = r#"{ "max_total_tokens": 250000, "max_subagents": 3 }"#;
        let cfg: RunBudgetConfig = serde_json::from_str(json).expect("deserializes");
        assert_eq!(cfg.max_total_tokens, Some(250_000));
        assert_eq!(
            cfg.max_tool_calls, None,
            "absent field defaults to unlimited"
        );
        assert_eq!(cfg.max_subagents, Some(3));

        // Absent fields are omitted on serialize (skip_serializing_if), so an
        // all-default config round-trips to `{}` rather than three explicit
        // nulls.
        let empty = serde_json::to_string(&RunBudgetConfig::default()).unwrap();
        assert_eq!(empty, "{}");
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
    fn subagents_config_without_mcp_role_allowlist_deserializes_empty() {
        // An OLD config (predating #54's wiring) has no `mcp_role_allowlist`
        // key — it must still deserialize, with an empty list (default =
        // every role unrestricted, identical to pre-#54 behavior).
        let json = r#"{ "max_concurrent": 4 }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("old config deserializes");
        assert!(cfg.mcp_role_allowlist.is_empty());
        // An empty list is omitted on re-serialize (skip_if empty).
        let back = serde_json::to_string(&cfg).unwrap();
        assert!(
            !back.contains("mcp_role_allowlist"),
            "empty vec is skipped: {back}"
        );
    }

    #[test]
    fn mcp_role_allowlist_entry_round_trips() {
        let json = r#"{
            "mcp_role_allowlist": [
                { "role": "researcher", "tools": ["fetch_url"] },
                { "role": "sandboxed", "tools": [] }
            ]
        }"#;
        let cfg: SubagentsConfig = serde_json::from_str(json).expect("populated config");
        assert_eq!(cfg.mcp_role_allowlist.len(), 2);
        assert_eq!(cfg.mcp_role_allowlist[0].role, "researcher");
        assert_eq!(cfg.mcp_role_allowlist[0].tools, vec!["fetch_url"]);
        // An empty `tools` list is an explicit lockout, distinct from the role
        // being absent — it must round-trip as an empty (not omitted) list.
        assert_eq!(cfg.mcp_role_allowlist[1].role, "sandboxed");
        assert!(cfg.mcp_role_allowlist[1].tools.is_empty());

        let back = serde_json::to_string(&cfg).unwrap();
        let reparsed: SubagentsConfig = serde_json::from_str(&back).unwrap();
        assert_eq!(cfg, reparsed, "round-trip is stable");
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
            // `pid + nanos` alone is NOT collision-free (issue #486): every
            // test in this binary shares the pid, and two tests started
            // concurrently by the multi-threaded harness can observe the
            // same `SystemTime` nanos tick. Two `TempHome`s colliding on one
            // path means they share a directory — and the first test's
            // `Drop` (`remove_dir_all`) then yanks the directory out from
            // under the other test's in-flight `save_to_dir`, whose
            // tmp-file+rename dance fails with ENOENT ("Failed to write
            // config file ... os error 2" — `save_rotates_backup_generations`'s
            // exact one-off CI failure mode). A per-process atomic counter
            // in the name makes each instance unique unconditionally.
            static NEXT_TEMP_HOME_ID: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let unique = NEXT_TEMP_HOME_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "chat-core-config-test-{}-{}-{}",
                std::process::id(),
                nanos,
                unique
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

    // ── config-corruption recovery confirmation gate (#153) ───────────────

    #[test]
    fn recovery_status_set_from_backup_and_quarantine_preserves_corrupt_bytes() {
        let temp = TempHome::new();
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({ "http_proxy": "http://from-backup" }).to_string(),
        )
        .unwrap();
        let corrupt_bytes = "{ not valid json ";
        std::fs::write(temp.path.join("config.json"), corrupt_bytes).unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        let status = config
            .recovery_status()
            .expect("a corrupt load must set a pending recovery status");
        assert!(!status.confirmed, "a fresh recovery starts unconfirmed");
        assert_eq!(
            status.source,
            ConfigRecoverySource::Backup { generation: 0 },
            "recovered from generation-0 (.bak)"
        );
        let quarantine_path = status
            .quarantine_path
            .as_ref()
            .expect("quarantine copy should have succeeded");
        assert_eq!(
            std::fs::read_to_string(quarantine_path).unwrap(),
            corrupt_bytes,
            "the quarantine copy preserves the corrupt original BYTE FOR BYTE"
        );
        assert_eq!(
            std::fs::read_to_string(temp.path.join("config.json")).unwrap(),
            corrupt_bytes,
            "the original config.json itself is untouched by the load (only copied, not moved)"
        );
    }

    #[test]
    fn recovery_status_set_from_salvage_lists_recovered_fields() {
        let temp = TempHome::new();
        temp.set_config_json(
            r#"{"http_proxy":"http://salvaged","env_vars":"this-should-be-an-array"}"#,
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        let status = config.recovery_status().expect("pending recovery");
        assert!(!status.confirmed);
        match &status.source {
            ConfigRecoverySource::Salvaged { fields } => {
                assert!(
                    fields.iter().any(|f| f == "http_proxy"),
                    "salvaged fields should list the recovered key: {fields:?}"
                );
            }
            other => panic!("expected Salvaged source, got {other:?}"),
        }
    }

    #[test]
    fn recovery_status_set_from_defaults_when_nothing_salvageable() {
        let temp = TempHome::new();
        // Not a JSON object at all -> salvage impossible; no .bak -> defaults.
        std::fs::write(temp.path.join("config.json"), "}}} broken").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        let status = config.recovery_status().expect("pending recovery");
        assert!(!status.confirmed);
        assert_eq!(status.source, ConfigRecoverySource::Defaults);
    }

    #[test]
    fn clean_load_never_sets_recovery_status() {
        let temp = TempHome::new();
        temp.set_config_json(r#"{"http_proxy":"http://clean"}"#);

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(
            config.recovery_status().is_none(),
            "a config.json that parses cleanly must never carry a pending recovery status"
        );
    }

    #[test]
    fn save_to_dir_refuses_to_overwrite_until_recovery_confirmed() {
        let temp = TempHome::new();
        let corrupt_bytes = "}}} broken";
        std::fs::write(temp.path.join("config.json"), corrupt_bytes).unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(config.recovery_status().is_some());

        let err = config
            .save_to_dir(temp.path.clone())
            .expect_err("save must refuse while recovery is unconfirmed");
        assert!(
            err.to_string().contains("recovered from corruption")
                || err.to_string().contains("confirm"),
            "error should explain the refused overwrite: {err}"
        );

        // The corrupt original on disk must be BYTE FOR BYTE unchanged — the
        // refused save must not have touched it at all.
        assert_eq!(
            std::fs::read_to_string(temp.path.join("config.json")).unwrap(),
            corrupt_bytes,
            "a refused save must leave the corrupt original untouched"
        );
    }

    #[test]
    fn half_written_truncated_config_is_quarantined_byte_for_byte_and_blocks_overwrite() {
        let temp = TempHome::new();
        // Simulates a crash mid-write: valid JSON prefix, abruptly cut off.
        let truncated = r#"{"http_proxy":"http://partial","providers":{"anthro"#;
        std::fs::write(temp.path.join("config.json"), truncated).unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        let status = config.recovery_status().expect("pending recovery");
        let quarantine_path = status.quarantine_path.as_ref().expect("quarantined");
        assert_eq!(
            std::fs::read_to_string(quarantine_path).unwrap(),
            truncated,
            "truncated original preserved byte for byte in quarantine"
        );

        let err = config.save_to_dir(temp.path.clone());
        assert!(err.is_err(), "unconfirmed recovery must refuse to save");
        assert_eq!(
            std::fs::read_to_string(temp.path.join("config.json")).unwrap(),
            truncated,
            "the half-written original stays exactly as it was after a refused save"
        );
    }

    #[test]
    fn confirm_recovery_allows_the_next_save() {
        let temp = TempHome::new();
        std::fs::write(temp.path.join("config.json"), "}}} broken").unwrap();

        let mut config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(config.recovery_status().is_some());

        config.confirm_recovery();
        assert!(
            config.recovery_status().is_some_and(|s| s.confirmed),
            "confirm_recovery flips the flag but keeps the status around"
        );

        config
            .save_to_dir(temp.path.clone())
            .expect("save must succeed once the recovery is confirmed");
    }

    #[test]
    fn confirm_recovery_and_save_to_dir_persists_and_clears_status() {
        let temp = TempHome::new();
        std::fs::write(
            temp.path.join("config.json"),
            r#"{"http_proxy":"http://recovered","env_vars":"bad-type"}"#,
        )
        .unwrap();

        let mut config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(config.recovery_status().is_some());
        let quarantine_path = config
            .recovery_status()
            .unwrap()
            .quarantine_path
            .clone()
            .unwrap();

        config
            .confirm_recovery_and_save_to_dir(temp.path.clone())
            .expect("confirm+save should succeed");

        assert!(
            config.recovery_status().is_none(),
            "the pending flag is cleared once the recovery is confirmed and persisted"
        );
        let on_disk = std::fs::read_to_string(temp.path.join("config.json")).unwrap();
        assert!(
            on_disk.contains("http://recovered"),
            "config.json now holds the recovered (salvaged) state"
        );
        // The quarantine copy of the original corrupt file must still exist,
        // untouched, even after the recovery is confirmed and persisted.
        assert!(
            quarantine_path.exists(),
            "the quarantined original survives confirmation — it's never deleted"
        );
    }

    #[test]
    fn confirm_recovery_and_save_to_dir_errors_when_nothing_pending() {
        let temp = TempHome::new();
        let mut config = Config::create_default();
        let err = config.confirm_recovery_and_save_to_dir(temp.path.clone());
        assert!(
            err.is_err(),
            "confirming a recovery that was never pending must error, not silently succeed"
        );
    }

    // ── connect.json split (#455) ────────────────────────────────────────

    fn connect_platform_with_encrypted(
        platform_type: &str,
        token_encrypted: &str,
    ) -> ConnectPlatformConfig {
        ConnectPlatformConfig {
            id: None,
            platform_type: platform_type.to_string(),
            token: None,
            token_encrypted: Some(token_encrypted.to_string()),
            app_id: None,
            app_secret: None,
            app_secret_encrypted: None,
            domain: None,
            allow_from: vec!["user-1".to_string()],
            admin_from: Vec::new(),
        }
    }

    fn connect_json_path(temp: &TempHome) -> PathBuf {
        temp.path.join("connect.json")
    }

    #[test]
    fn save_splits_connect_into_sibling_connect_json() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        config.connect.platforms = vec![connect_platform_with_encrypted("telegram", "")];
        config.connect.platforms[0].token = Some("plain-bot-token".to_string());

        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let config_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path.join("config.json")).unwrap())
                .unwrap();
        assert!(
            config_json.get("connect").is_none(),
            "config.json must not carry the `connect` key after a save"
        );

        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        assert_eq!(connect_json["platforms"][0]["type"], "telegram");
        assert!(
            connect_json["platforms"][0]["token_encrypted"]
                .as_str()
                .is_some_and(|v| !v.is_empty()),
            "the token is persisted in its encrypted form in connect.json"
        );
        assert!(
            connect_json["platforms"][0].get("token").is_none(),
            "the plaintext token is never persisted (skip_serializing)"
        );
    }

    // ── stable connect.platforms id (#496) ───────────────────────────────

    #[test]
    fn save_assigns_a_missing_connect_platform_id() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        config.connect.platforms = vec![connect_platform_with_encrypted("telegram", "cipher")];
        assert!(
            config.connect.platforms[0].id.is_none(),
            "precondition: the entry starts without an id"
        );

        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        let persisted_id = connect_json["platforms"][0]["id"]
            .as_str()
            .expect("save_to_dir must backfill a missing id onto the persisted entry");
        assert!(!persisted_id.is_empty());

        let reloaded = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            reloaded.connect.platforms[0].id.as_deref(),
            Some(persisted_id),
            "the assigned id round-trips through a reload"
        );
    }

    #[test]
    fn save_never_reassigns_an_existing_connect_platform_id() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        let mut platform = connect_platform_with_encrypted("telegram", "cipher");
        platform.id = Some("stable-id-123".to_string());
        config.connect.platforms = vec![platform];

        config
            .save_to_dir(temp.path.clone())
            .expect("first save succeeds");
        // Save again (e.g. an unrelated settings change) — the id must not change.
        config
            .save_to_dir(temp.path.clone())
            .expect("second save succeeds");

        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        assert_eq!(connect_json["platforms"][0]["id"], "stable-id-123");
    }

    #[test]
    fn save_assigns_distinct_ids_to_duplicate_platform_type_entries() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        config.connect.platforms = vec![
            connect_platform_with_encrypted("telegram", "cipher-a"),
            connect_platform_with_encrypted("telegram", "cipher-b"),
        ];

        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        let id_a = connect_json["platforms"][0]["id"].as_str().unwrap();
        let id_b = connect_json["platforms"][1]["id"].as_str().unwrap();
        assert_ne!(
            id_a, id_b,
            "two entries sharing platform_type must still get distinct ids"
        );
    }

    #[test]
    fn load_never_assigns_or_persists_an_id_by_itself() {
        let temp = TempHome::new();
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "cipher-abc", "allow_from": ["u1"] }
                ]
            })
            .to_string(),
        )
        .unwrap();
        let connect_json_before = std::fs::read_to_string(connect_json_path(&temp)).unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        assert!(
            config.connect.platforms[0].id.is_none(),
            "load alone must not backfill an id in memory"
        );
        let connect_json_after = std::fs::read_to_string(connect_json_path(&temp)).unwrap();
        assert_eq!(
            connect_json_before, connect_json_after,
            "load must never rewrite connect.json on disk just to backfill an id (#493)"
        );
    }

    #[test]
    fn load_merges_connect_json_into_config() {
        let temp = TempHome::new();
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "cipher-abc", "allow_from": ["u1"] }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(config.connect.platforms.len(), 1);
        assert_eq!(config.connect.platforms[0].platform_type, "telegram");
        assert_eq!(
            config.connect.platforms[0].token_encrypted.as_deref(),
            Some("cipher-abc")
        );
    }

    #[test]
    fn load_without_connect_json_yields_empty_inert_connect_config() {
        let temp = TempHome::new();
        temp.set_config_json(r#"{"http_proxy":"http://x"}"#);

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(
            config.connect.platforms.is_empty(),
            "no connect.json and no legacy key -> empty/inert connect config"
        );
        assert!(
            !connect_json_path(&temp).exists(),
            "load must not create connect.json when there is nothing to migrate"
        );
    }

    #[test]
    fn migration_adopts_legacy_connect_key_and_writes_both_files() {
        let temp = TempHome::new();
        // Legacy state (#453): connect lives inline in config.json, no connect.json yet.
        temp.set_config_json(
            &serde_json::json!({
                "http_proxy": "http://keep-me",
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-cipher", "allow_from": ["u1"] }
                    ]
                }
            })
            .to_string(),
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        // In-memory: legacy value adopted.
        assert_eq!(config.connect.platforms.len(), 1);
        assert_eq!(
            config.connect.platforms[0].token_encrypted.as_deref(),
            Some("legacy-cipher")
        );
        // An unrelated field from the same load survives the migration rewrite.
        assert_eq!(config.http_proxy, "http://keep-me");

        // On disk: connect.json was created...
        assert!(
            connect_json_path(&temp).exists(),
            "migration proactively creates connect.json"
        );
        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        assert_eq!(
            connect_json["platforms"][0]["token_encrypted"], "legacy-cipher",
            "the encrypted value is preserved (encrypted form intact) by the migration"
        );

        // ...and config.json was rewritten without the `connect` key.
        let config_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path.join("config.json")).unwrap())
                .unwrap();
        assert!(
            config_json.get("connect").is_none(),
            "config.json is rewritten without the legacy `connect` key"
        );
    }

    /// #457: the legacy-key migration must be a NARROW write (strip `connect`
    /// from config.json + write connect.json) — not the full `save_to_dir`,
    /// which would re-encrypt every OTHER secret in config.json and rotate a
    /// `config.json.bak` generation as a load-time side effect. This matters
    /// most for a purely READ-ONLY command (e.g. `bamboo config get`) run on a
    /// machine that still has the legacy `connect` key: it must not silently
    /// rewrite/re-encrypt unrelated secrets or spin up a backup.
    #[test]
    fn migration_write_is_narrow_and_does_not_rewrite_unrelated_secrets_or_backups() {
        let _key = crate::encryption::set_test_encryption_key([0x77; 32]);
        let temp = TempHome::new();

        let original_api_key_encrypted =
            crate::encryption::encrypt("sk-unrelated-secret").expect("encrypt succeeds");
        temp.set_config_json(
            &serde_json::json!({
                "providers": {
                    "openai": {
                        "api_key_encrypted": original_api_key_encrypted,
                    }
                },
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-cipher", "allow_from": ["u1"] }
                    ]
                }
            })
            .to_string(),
        );

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(config.connect.platforms.len(), 1, "legacy key adopted");

        // No `config.json.bak` — the narrow write does not rotate backups the
        // way a full `save_to_dir` would.
        assert!(
            !temp.path.join("config.json.bak").exists(),
            "a read-only load migrating a legacy `connect` key must not rotate \
             config.json backups"
        );

        // The unrelated provider secret's ciphertext is byte-for-byte
        // unchanged — proof it was never decrypted+re-encrypted (encryption
        // uses a random nonce per call, so any re-encryption would change the
        // bytes even for the same plaintext).
        let config_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path.join("config.json")).unwrap())
                .unwrap();
        assert_eq!(
            config_json["providers"]["openai"]["api_key_encrypted"], original_api_key_encrypted,
            "an unrelated secret's ciphertext must not be touched by the connect \
             migration's narrow write"
        );
        assert!(
            config_json.get("connect").is_none(),
            "config.json is still rewritten without the legacy `connect` key"
        );
    }

    #[test]
    fn both_files_present_connect_json_wins() {
        let temp = TempHome::new();
        temp.set_config_json(
            &serde_json::json!({
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "stale-config-json-cipher" }
                    ]
                }
            })
            .to_string(),
        );
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "authoritative-cipher" }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.connect.platforms[0].token_encrypted.as_deref(),
            Some("authoritative-cipher"),
            "connect.json wins over a stale legacy config.json key"
        );
    }

    /// #457: when both files are present, the superseded `connect` key in
    /// config.json must be stripped PROACTIVELY on load — not left to linger
    /// until the next natural save, which spreads token ciphertext across two
    /// files for longer than necessary.
    #[test]
    fn both_files_present_strips_stale_legacy_key_from_config_json_immediately() {
        let temp = TempHome::new();
        temp.set_config_json(
            &serde_json::json!({
                "http_proxy": "http://keep-me",
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "stale-config-json-cipher" }
                    ]
                }
            })
            .to_string(),
        );
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "authoritative-cipher" }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(
            config.connect.platforms[0].token_encrypted.as_deref(),
            Some("authoritative-cipher")
        );
        // Unrelated field survives the strip.
        assert_eq!(config.http_proxy, "http://keep-me");

        let config_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path.join("config.json")).unwrap())
                .unwrap();
        assert!(
            config_json.get("connect").is_none(),
            "the stale legacy `connect` key must be stripped from config.json \
             immediately on load, not left for the next natural save"
        );
    }

    /// #468 (follow-up to #457): a `.bak` generation that predates the #455
    /// split can still carry the legacy embedded `connect` sub-tree even
    /// after the LIVE config.json has long since been migrated (a clean
    /// config.json here, with no `connect` key at all, proves the sweep does
    /// not depend on the current-load migration path having just fired).
    /// Only the tainted generation is rewritten; every other key in it
    /// survives, and the rewrite strips exactly the `connect` key.
    #[test]
    fn scrub_strips_legacy_connect_from_tainted_backup_generation() {
        let temp = TempHome::new();
        temp.set_config_json(&serde_json::json!({ "http_proxy": "http://current" }).to_string());
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({
                "http_proxy": "http://old",
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-bak-cipher", "allow_from": ["u1"] }
                    ]
                }
            })
            .to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        // The live config is unaffected — connect.json never existed and
        // config.json never had the key, so in-memory connect stays empty.
        assert!(config.connect.platforms.is_empty());

        let bak: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap(),
        )
        .unwrap();
        assert!(
            bak.get("connect").is_none(),
            "the legacy `connect` key must be stripped from the tainted .bak generation"
        );
        assert_eq!(
            bak["http_proxy"], "http://old",
            "every other key in the .bak generation survives the scrub byte-for-byte in content"
        );
    }

    /// The sweep must touch EVERY rotated generation that carries the legacy
    /// key, not just `.bak` — an upgraded instance can have the taint several
    /// generations deep depending on how many saves happened since #455/#457
    /// shipped but before this fix.
    #[test]
    fn scrub_reaches_all_rotated_generations() {
        let temp = TempHome::new();
        temp.set_config_json(&serde_json::json!({}).to_string());
        for (gen_suffix, cipher) in [
            ("config.json.bak", "cipher-gen0"),
            ("config.json.bak.1", "cipher-gen1"),
            ("config.json.bak.2", "cipher-gen2"),
        ] {
            std::fs::write(
                temp.path.join(gen_suffix),
                serde_json::json!({
                    "connect": {
                        "platforms": [
                            { "type": "telegram", "token_encrypted": cipher }
                        ]
                    }
                })
                .to_string(),
            )
            .unwrap();
        }

        let _config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        for gen_suffix in ["config.json.bak", "config.json.bak.1", "config.json.bak.2"] {
            let value: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(temp.path.join(gen_suffix)).unwrap())
                    .unwrap();
            assert!(
                value.get("connect").is_none(),
                "{gen_suffix} must have its legacy `connect` key stripped"
            );
        }
    }

    /// A `.bak` generation with NO legacy `connect` key must be left
    /// completely untouched by the sweep — not even a byte-identical
    /// rewrite — preserving the file's bytes/mtime exactly. This is the
    /// overwhelming common case (any backup created after #455/#457 shipped)
    /// and the whole point of the surgical, only-touch-what's-tainted
    /// approach: `.bak` files are the user's recovery net (#493) and
    /// shouldn't be churned by an unrelated sweep.
    #[test]
    fn scrub_leaves_untainted_backup_byte_and_mtime_identical() {
        let temp = TempHome::new();
        temp.set_config_json(&serde_json::json!({}).to_string());
        let bak_path = temp.path.join("config.json.bak");
        std::fs::write(
            &bak_path,
            serde_json::json!({ "http_proxy": "http://clean-backup" }).to_string(),
        )
        .unwrap();

        let before_bytes = std::fs::read(&bak_path).unwrap();
        let before_mtime = std::fs::metadata(&bak_path).unwrap().modified().unwrap();

        // A tiny sleep would make an mtime-changed assertion more robust, but
        // even without one, a same-mtime filesystem is the STRONGER
        // guarantee of "no write happened" — good enough on its own.
        let _config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        let after_bytes = std::fs::read(&bak_path).unwrap();
        let after_mtime = std::fs::metadata(&bak_path).unwrap().modified().unwrap();
        assert_eq!(
            before_bytes, after_bytes,
            "a .bak generation without a legacy `connect` key must not be rewritten at all"
        );
        assert_eq!(
            before_mtime, after_mtime,
            "no write means no mtime change either"
        );
    }

    /// An unparsable `.bak` generation (corrupt/foreign content) must be
    /// skipped, not deleted and not guessed at — it's left exactly as found
    /// so an operator can inspect it by hand, matching the same fail-safe
    /// posture as the rest of the backup/quarantine machinery.
    #[test]
    fn scrub_skips_unparsable_backup_without_deleting_it() {
        let temp = TempHome::new();
        temp.set_config_json(&serde_json::json!({}).to_string());
        std::fs::write(temp.path.join("config.json.bak"), "{ not valid json").unwrap();

        let _config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        let content = std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap();
        assert_eq!(
            content, "{ not valid json",
            "an unparsable .bak generation must be left byte-for-byte untouched, never deleted"
        );
    }

    /// A missing generation (e.g. only `.bak` exists, no `.bak.1`/`.bak.2`
    /// yet) must not trip an error — it's the common case for a young
    /// install and the sweep should just skip straight past it.
    #[test]
    fn scrub_tolerates_missing_generations() {
        let temp = TempHome::new();
        temp.set_config_json(&serde_json::json!({}).to_string());
        // No .bak files at all.
        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(config.connect.platforms.is_empty());
        assert!(!temp.path.join("config.json.bak").exists());
    }

    /// The scrub sweep must not interfere with normal backup rotation on
    /// subsequent saves — rotation keeps working exactly as before.
    #[test]
    fn scrub_does_not_break_backup_rotation() {
        let temp = TempHome::new();
        std::fs::write(
            temp.path.join("config.json"),
            serde_json::json!({
                "http_proxy": "http://proxy-v1",
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-cipher" }
                    ]
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            temp.path.join("config.json.bak"),
            serde_json::json!({
                "http_proxy": "http://proxy-v0",
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-bak-cipher" }
                    ]
                }
            })
            .to_string(),
        )
        .unwrap();

        // Load triggers: migration of the live legacy key + the .bak sweep.
        let mut config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        let bak: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap(),
        )
        .unwrap();
        assert!(bak.get("connect").is_none(), ".bak scrubbed on load");

        // Rotation still works on a subsequent save: v_current -> .bak,
        // .bak(old) -> .bak.1.
        config.http_proxy = "http://proxy-v2".to_string();
        config.save_to_dir(temp.path.clone()).unwrap();

        let new_bak = std::fs::read_to_string(temp.path.join("config.json.bak")).unwrap();
        assert!(
            new_bak.contains("proxy-v1"),
            ".bak reflects the pre-save (migrated, scrub-clean) state after rotation"
        );
        let new_bak1 = std::fs::read_to_string(temp.path.join("config.json.bak.1")).unwrap();
        assert!(
            new_bak1.contains("proxy-v0"),
            ".bak.1 holds the scrubbed older generation after rotation"
        );
        assert!(
            !new_bak1.contains("legacy-bak-cipher"),
            "the rotated-down generation stays scrubbed — rotation doesn't resurrect the \
             stripped secret"
        );
    }

    #[test]
    fn corrupt_connect_json_yields_empty_connect_and_is_quarantined() {
        let temp = TempHome::new();
        temp.set_config_json("{}");
        std::fs::write(connect_json_path(&temp), "{ not valid json").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(
            config.connect.platforms.is_empty(),
            "corrupt connect.json fails SAFE to an empty/inert connect config"
        );

        let backup = connect_json_path(&temp).with_extension("json.bak");
        assert!(
            backup.exists(),
            "the corrupt connect.json is quarantined to connect.json.bak"
        );
        assert!(
            std::fs::read_to_string(backup)
                .unwrap()
                .contains("not valid json"),
            "the quarantined copy holds the bad content"
        );
        // #457: quarantine MOVES the corrupt file rather than copying it, so
        // the data dir doesn't end up with two copies of the same corrupt
        // content (the live `connect.json` and its `.bak`) sitting side by
        // side, which reads as confusing/ambiguous mid-incident.
        assert!(
            !connect_json_path(&temp).exists(),
            "quarantine must MOVE the corrupt connect.json (not copy it) — no \
             connect.json should remain after quarantine"
        );
    }

    #[test]
    fn corrupt_connect_json_does_not_fall_back_to_legacy_config_json_copy() {
        let temp = TempHome::new();
        // A legacy inline `connect` key is present too — it must NOT be used as a
        // fallback when connect.json is corrupt (security-sensitive: fail safe).
        temp.set_config_json(
            &serde_json::json!({
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token_encrypted": "legacy-should-not-be-used" }
                    ]
                }
            })
            .to_string(),
        );
        std::fs::write(connect_json_path(&temp), "{ not valid json").unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert!(
            config.connect.platforms.is_empty(),
            "corrupt connect.json must not fall back to the legacy config.json copy"
        );
    }

    #[test]
    fn empty_connect_config_with_no_existing_file_creates_no_connect_json() {
        let temp = TempHome::new();
        let config = Config::create_default();
        assert!(config.connect.platforms.is_empty());

        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        assert!(
            !connect_json_path(&temp).exists(),
            "an empty connect config with no pre-existing file must not create one"
        );
    }

    #[test]
    fn connect_json_backed_up_before_overwrite() {
        let temp = TempHome::new();
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "old-cipher" }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let mut config = Config::create_default();
        config.connect.platforms = vec![connect_platform_with_encrypted("telegram", "new-cipher")];
        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let backup = connect_json_path(&temp).with_extension("json.bak");
        assert!(
            std::fs::read_to_string(backup)
                .unwrap()
                .contains("old-cipher"),
            "the previous connect.json is preserved as connect.json.bak before the overwrite"
        );
        let current = std::fs::read_to_string(connect_json_path(&temp)).unwrap();
        assert!(current.contains("new-cipher"));
    }

    // ── Feishu adapter config fields (epic #447 phase 3, §2a) ───────────

    #[test]
    fn save_splits_feishu_app_secret_into_connect_json_encrypted_alongside_app_id_and_domain() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        config.connect.platforms = vec![ConnectPlatformConfig {
            id: None,
            platform_type: "feishu".to_string(),
            token: None,
            token_encrypted: None,
            app_id: Some("cli_real_app_id".to_string()),
            app_secret: Some("plain-app-secret".to_string()),
            app_secret_encrypted: None,
            domain: Some("lark".to_string()),
            allow_from: vec!["ou_1".to_string()],
            admin_from: Vec::new(),
        }];

        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let connect_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(connect_json_path(&temp)).unwrap())
                .unwrap();
        assert_eq!(connect_json["platforms"][0]["type"], "feishu");
        assert_eq!(connect_json["platforms"][0]["app_id"], "cli_real_app_id");
        assert_eq!(connect_json["platforms"][0]["domain"], "lark");
        assert!(
            connect_json["platforms"][0]["app_secret_encrypted"]
                .as_str()
                .is_some_and(|v| !v.is_empty()),
            "app_secret is persisted in its encrypted form in connect.json"
        );
        assert!(
            connect_json["platforms"][0].get("app_secret").is_none(),
            "the plaintext app_secret is never persisted (skip_serializing)"
        );
    }

    #[test]
    fn load_hydrates_feishu_app_secret_from_encrypted() {
        let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
        let temp = TempHome::new();

        let mut config = Config::create_default();
        config.connect.platforms = vec![ConnectPlatformConfig {
            id: None,
            platform_type: "feishu".to_string(),
            token: None,
            token_encrypted: None,
            app_id: Some("cli_real_app_id".to_string()),
            app_secret: Some("plain-app-secret".to_string()),
            app_secret_encrypted: None,
            domain: Some("lark".to_string()),
            allow_from: vec!["ou_1".to_string()],
            admin_from: Vec::new(),
        }];
        config
            .save_to_dir(temp.path.clone())
            .expect("save succeeds");

        let reloaded = Config::from_data_dir_without_publish(Some(temp.path.clone()));
        assert_eq!(reloaded.connect.platforms.len(), 1);
        assert_eq!(
            reloaded.connect.platforms[0].app_secret.as_deref(),
            Some("plain-app-secret"),
            "reload hydrates app_secret from app_secret_encrypted"
        );
        assert_eq!(
            reloaded.connect.platforms[0].app_id.as_deref(),
            Some("cli_real_app_id")
        );
        assert_eq!(
            reloaded.connect.platforms[0].domain.as_deref(),
            Some("lark")
        );
    }

    #[test]
    fn legacy_telegram_only_connect_entry_without_feishu_fields_still_deserializes() {
        let temp = TempHome::new();
        std::fs::write(
            connect_json_path(&temp),
            serde_json::json!({
                "platforms": [
                    { "type": "telegram", "token_encrypted": "legacy-cipher", "allow_from": ["u1"] }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let config = Config::from_data_dir_without_publish(Some(temp.path.clone()));

        assert_eq!(config.connect.platforms.len(), 1);
        assert_eq!(config.connect.platforms[0].platform_type, "telegram");
        assert_eq!(
            config.connect.platforms[0].token_encrypted.as_deref(),
            Some("legacy-cipher")
        );
        assert_eq!(
            config.connect.platforms[0].app_id, None,
            "a legacy entry with no Feishu fields deserializes them as None"
        );
        assert_eq!(config.connect.platforms[0].app_secret, None);
        assert_eq!(config.connect.platforms[0].app_secret_encrypted, None);
        assert_eq!(config.connect.platforms[0].domain, None);
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
    fn refresh_encrypted_secrets_makes_instance_key_survive_serde_roundtrip() {
        // #516: `save_to_dir` refreshes ciphertext only on its save-time clone,
        // so a provider instance created over HTTP stays plaintext-only in the
        // live config. Serializing that live config (as the settings-PATCH
        // merge does) drops the `skip_serializing` plaintext and the key is
        // gone. `refresh_encrypted_secrets` on the live config closes the gap.
        let mut config = Config::default();
        let instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "api_key": "sk-instance-live",
        }))
        .expect("valid instance");
        config
            .provider_instances
            .insert("work".to_string(), instance);

        config.refresh_encrypted_secrets().expect("refresh");
        assert!(
            config.provider_instances["work"]
                .api_key_encrypted
                .is_some(),
            "live config must hold ciphertext after refresh"
        );

        // The build_merged_config-style round-trip.
        let value = serde_json::to_value(&config).expect("serialize");
        let mut back: Config = serde_json::from_value(value).expect("deserialize");
        assert!(
            back.provider_instances["work"].api_key.is_empty(),
            "plaintext is skip_serializing"
        );
        back.hydrate_provider_instance_api_keys_from_encrypted();
        assert_eq!(
            back.provider_instances["work"].api_key, "sk-instance-live",
            "key must be recoverable from the round-tripped ciphertext"
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
                ledger_agenda_injection: false,
                ledger_gardener_enabled: false,
                ledger_gardener_interval_secs: 7_200,
                ledger_distillation_enabled: false,
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
                granularity_freshness_gardener_enabled: false,
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
        assert!(!memory.granularity_freshness_gardener_enabled);
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

        let content = std::fs::read_to_string(temp_home.path.join("providers.json"))
            .expect("read providers.json");
        assert!(
            content.contains("\"api_key_encrypted\""),
            "providers.json should store encrypted provider keys"
        );
        assert!(
            !content.contains("\"api_key\""),
            "providers.json should not store plaintext provider keys"
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
        // `publish_env_vars` REPLACES the process-global env-vars cache
        // wholesale, so every test that touches that cache must hold the
        // crate-wide env lock. This test didn't (issue #486): running
        // concurrently with a lock-holding cache test (e.g.
        // `from_data_dir_without_publish_does_not_clobber_global_cache`) it
        // wiped that test's just-seeded marker out of the cache mid-assert —
        // and its own 10x retry loop below was itself a symptom of losing
        // the same race in the other direction. With the lock held, one
        // publish is deterministic.
        let _lock = crate::test_support::env_cache_lock_acquire();
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

        config.publish_env_vars();
        assert_eq!(
            Config::current_env_vars()
                .get("TEST_PUBLISH")
                .map(String::as_str),
            Some("pub_value")
        );
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

    // -----------------------------------------------------------------
    // `plugin_trust.enforcement` — the persistent, config-level form of the
    // `--insecure` escape hatch.
    // -----------------------------------------------------------------

    #[test]
    fn plugin_trust_enforcement_defaults_to_strict_when_absent() {
        // A fresh `Config::default()` (nothing on disk at all).
        let config = Config::default();
        assert_eq!(
            config.plugin_trust.enforcement,
            PluginTrustEnforcement::Strict
        );
        assert!(!config.plugin_trust.enforcement_is_off());

        // A `plugin_trust` object present in JSON but with NO `enforcement`
        // key at all (e.g. a config.json written before this field existed)
        // must ALSO deserialize to Strict, not fail or silently do something
        // else — additive/back-compat, matching `trusted_hosts`/
        // `trusted_keys`'s own `#[serde(default = ...)]` behavior.
        let json = serde_json::json!({
            "trusted_hosts": ["example.com"],
            "trusted_keys": [],
        });
        let trust: PluginTrustConfig = serde_json::from_value(json).expect("deserializes");
        assert_eq!(trust.enforcement, PluginTrustEnforcement::Strict);
    }

    #[test]
    fn plugin_trust_enforcement_off_string_parses_case_insensitively() {
        for raw in ["off", "OFF", "Off", " off "] {
            let trust: PluginTrustConfig = serde_json::from_value(serde_json::json!({
                "enforcement": raw,
            }))
            .unwrap_or_else(|e| panic!("'{raw}' should parse as Off: {e}"));
            assert_eq!(trust.enforcement, PluginTrustEnforcement::Off, "{raw}");
            assert!(trust.enforcement_is_off());
        }
        for raw in ["strict", "STRICT", " Strict "] {
            let trust: PluginTrustConfig = serde_json::from_value(serde_json::json!({
                "enforcement": raw,
            }))
            .unwrap_or_else(|e| panic!("'{raw}' should parse as Strict: {e}"));
            assert_eq!(trust.enforcement, PluginTrustEnforcement::Strict, "{raw}");
        }

        let err = serde_json::from_value::<PluginTrustConfig>(serde_json::json!({
            "enforcement": "nonsense",
        }))
        .expect_err("an unrecognized string must be rejected, not silently default");
        assert!(err.to_string().contains("nonsense"));
    }

    #[test]
    fn plugin_trust_enforcement_accepts_a_bool_ish_alias() {
        // A hand-edited config.json using a plain bool reads naturally: is
        // enforcement ON (`true`) or OFF (`false`)?
        let trust: PluginTrustConfig =
            serde_json::from_value(serde_json::json!({ "enforcement": false })).unwrap();
        assert_eq!(trust.enforcement, PluginTrustEnforcement::Off);

        let trust: PluginTrustConfig =
            serde_json::from_value(serde_json::json!({ "enforcement": true })).unwrap();
        assert_eq!(trust.enforcement, PluginTrustEnforcement::Strict);
    }

    #[test]
    fn plugin_trust_enforcement_always_serializes_as_the_canonical_string() {
        // Regardless of which accepted input form produced it, the
        // in-memory value always serializes back out as the canonical
        // string — this is what the dot-path `config set` setter's
        // round-trip check relies on (see `dot_path.rs`'s module docs).
        let trust = PluginTrustConfig {
            enforcement: PluginTrustEnforcement::Off,
            ..PluginTrustConfig::default()
        };
        let json = serde_json::to_value(&trust).unwrap();
        assert_eq!(json["enforcement"], "off");

        let trust = PluginTrustConfig {
            enforcement: PluginTrustEnforcement::Strict,
            ..PluginTrustConfig::default()
        };
        let json = serde_json::to_value(&trust).unwrap();
        assert_eq!(json["enforcement"], "strict");
    }

    #[test]
    fn normalize_plugin_trust_settings_does_not_disturb_enforcement() {
        // `normalize_plugin_trust_settings` only touches `trusted_hosts` —
        // confirm it's a true no-op on `enforcement` either way.
        let mut config = Config::default();
        config.plugin_trust.enforcement = PluginTrustEnforcement::Off;
        config.normalize_plugin_trust_settings();
        assert_eq!(config.plugin_trust.enforcement, PluginTrustEnforcement::Off);
    }

    #[test]
    fn config_set_plugin_trust_enforcement_off_round_trips_through_the_dot_path_setter() {
        // Confirms the dot-path `bamboo config set plugin_trust.enforcement
        // off` path actually works end to end through
        // `crate::dot_path::apply_dot_path_set` (the generic JSON-patch
        // setter), not just direct field assignment.
        let config = Config::from_data_dir_without_env(Some(std::path::PathBuf::from(
            "/nonexistent-bamboo-plugin-trust-enforcement-test-dir",
        )));
        assert_eq!(
            config.plugin_trust.enforcement,
            PluginTrustEnforcement::Strict
        );

        let outcome = crate::dot_path::apply_dot_path_set(
            &config,
            "plugin_trust.enforcement",
            crate::dot_path::parse_cli_value("off"),
        )
        .expect("plugin_trust.enforcement should be settable via the generic dot-path setter");
        assert_eq!(
            outcome.config.plugin_trust.enforcement,
            PluginTrustEnforcement::Off
        );

        // And back to strict.
        let outcome = crate::dot_path::apply_dot_path_set(
            &outcome.config,
            "plugin_trust.enforcement",
            crate::dot_path::parse_cli_value("strict"),
        )
        .expect("setting it back to strict should also round-trip");
        assert_eq!(
            outcome.config.plugin_trust.enforcement,
            PluginTrustEnforcement::Strict
        );
    }
}
