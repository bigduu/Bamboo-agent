//! Model context window limits registry.
//!
//! There is intentionally **no** built-in per-model table. Real per-model
//! values come from provider runtime metadata (e.g. Copilot reports real
//! context/output) and user overrides persisted in `model_limits.json`.
//! Runtime resolution gives explicit user configuration precedence over
//! provider metadata.
//!
//! Anything without a match falls back to a single global default
//! (`DEFAULT_MAX_CONTEXT_TOKENS` / `DEFAULT_MAX_OUTPUT_TOKENS`). This keeps the
//! registry from going stale as models churn — see `token_budget.rs`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// Sentinel pattern used for the single global fallback limit.
pub const DEFAULT_MODEL_PATTERN: &str = "default";

/// Global default context window applied to any model without a provider
/// metadata value or a user override. 1M reflects the current mainstream
/// range across frontier models (Claude 3.5, GPT-4o, Gemini 1.5, etc.).
pub const DEFAULT_MAX_CONTEXT_TOKENS: u32 = 1_000_000;

/// Global default maximum output tokens.
pub const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 128_000;

/// Default safety margin for token counting errors (floor; scales with context
/// window via [`ModelLimit::get_safety_margin`]).
pub const DEFAULT_SAFETY_MARGIN: u32 = 1000;

/// Build the single global default limit (`1M` context / `128K` output).
pub fn default_model_limit() -> ModelLimit {
    builtin_limit(
        DEFAULT_MODEL_PATTERN,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_OUTPUT_TOKENS,
    )
}

/// Whether a user override is a no-op — identical to the global default, so it
/// carries no information and need not be persisted (diff-only storage).
///
/// The `model_pattern` is irrelevant: any model pinned to exactly the default
/// context/output with no explicit safety margin resolves to the same budget
/// as having no override at all.
pub fn is_default_limit(limit: &ModelLimit) -> bool {
    limit.max_context_tokens == DEFAULT_MAX_CONTEXT_TOKENS
        && limit.max_output_tokens == Some(DEFAULT_MAX_OUTPUT_TOKENS)
        && limit.safety_margin.is_none()
}

/// Model limit configuration (user-overridable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelLimit {
    /// Model identifier (partial match supported, e.g., "gpt-4" matches "gpt-4o")
    pub model_pattern: String,
    /// Maximum total context window size (input + output) in tokens
    pub max_context_tokens: u32,
    /// Maximum output tokens (defaults to min(max_context / 4,
    /// DEFAULT_MAX_OUTPUT_TOKENS) when unset — see [`Self::get_max_output_tokens`])
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// Safety margin for token counting (defaults to 1000)
    #[serde(default)]
    pub safety_margin: Option<u32>,
}

impl ModelLimit {
    /// Create a new model limit with defaults.
    pub fn new(model_pattern: impl Into<String>, max_context_tokens: u32) -> Self {
        Self {
            model_pattern: model_pattern.into(),
            max_context_tokens,
            max_output_tokens: None,
            safety_margin: None,
        }
    }

    /// Get max output tokens with default calculation.
    ///
    /// When unset, derive from the context window (`max_context_tokens / 4`)
    /// capped at the global [`DEFAULT_MAX_OUTPUT_TOKENS`]. The cap tracks the
    /// global default rather than a hard-coded `4096`, so a user override like
    /// `ModelLimit::new("gpt-4o", 128_000)` (no explicit `max_output_tokens`)
    /// resolves to `min(32_000, 128_000) = 32_000` instead of collapsing to
    /// `4096` — see issue #20, bug 4.
    pub fn get_max_output_tokens(&self) -> u32 {
        self.max_output_tokens
            .unwrap_or_else(|| (self.max_context_tokens / 4).min(DEFAULT_MAX_OUTPUT_TOKENS))
    }

    /// Get safety margin, scaling proportionally with context window.
    pub fn get_safety_margin(&self) -> u32 {
        self.safety_margin
            .unwrap_or_else(|| (self.max_context_tokens / 100).max(DEFAULT_SAFETY_MARGIN))
    }
}

fn builtin_limit(pattern: &str, max_context_tokens: u32, max_output_tokens: u32) -> ModelLimit {
    let mut limit = ModelLimit::new(pattern.to_string(), max_context_tokens);
    limit.max_output_tokens = Some(max_output_tokens);
    limit
}

/// Registry for model limits with built-in defaults and user overrides.
#[derive(Debug, Clone)]
pub struct ModelLimitsRegistry {
    /// User-provided overrides (higher priority than built-in)
    user_limits: HashMap<String, ModelLimit>,
    /// Default path for user configuration file
    config_path: Option<PathBuf>,
}

impl ModelLimitsRegistry {
    /// Create a new registry with built-in defaults only.
    pub fn new() -> Self {
        Self {
            user_limits: HashMap::new(),
            config_path: None,
        }
    }

    /// Create a registry with a specific config file path.
    pub fn with_config_path(path: impl Into<PathBuf>) -> Self {
        Self {
            user_limits: HashMap::new(),
            config_path: Some(path.into()),
        }
    }

    /// Load user overrides from the registry's configured path.
    ///
    /// Accepts both the legacy raw array and the revisioned section envelope
    /// written by Bamboo's modular configuration store.
    ///
    /// No-op when the registry was created without a `config_path` (use
    /// [`Self::with_config_path`] — e.g. with [`get_default_config_path`] — to
    /// point it at `{bamboo_data_dir}/model_limits.json`).
    pub async fn load_user_config(&mut self) -> std::io::Result<()> {
        let Some(path) = self.config_path.clone() else {
            return Ok(());
        };

        if !path.exists() {
            // A previously loaded file may have been deleted between reloads.
            // Treat absence as an empty override set instead of retaining stale
            // in-memory patterns.
            self.user_limits.clear();
            return Ok(());
        }

        let content = tokio::fs::read_to_string(&path).await?;
        let value: Value = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let data = value
            .as_object()
            .filter(|object| {
                object.contains_key("schema_version")
                    && object.contains_key("revision")
                    && object.contains_key("data")
            })
            .and_then(|object| object.get("data"))
            .cloned()
            .unwrap_or(value);
        let limits: Vec<ModelLimit> = serde_json::from_value(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        self.user_limits = limits
            .into_iter()
            .map(|limit| (limit.model_pattern.clone(), limit))
            .collect();

        tracing::info!(
            "Loaded {} user model limits from {:?}",
            self.user_limits.len(),
            path
        );
        Ok(())
    }

    /// Add a user limit override.
    pub fn add_limit(&mut self, limit: ModelLimit) {
        self.user_limits.insert(limit.model_pattern.clone(), limit);
    }

    /// Get limit for a model, with user overrides taking priority.
    ///
    /// Returns `None` if no matching limit is found.
    ///
    /// # Matching Strategy
    /// 1. Exact match (highest priority)
    /// 2. Model contains pattern (e.g., "gpt-4o-mini" contains "gpt-4o")
    ///
    /// For partial matches, the longest (most specific) pattern wins.
    ///
    /// Only the `model.contains(pattern)` direction is correct: the configured
    /// pattern must be a substring of the runtime model id. The reverse
    /// (`pattern.contains(model)`) was a bug (#20, bug 3) — it let a short model
    /// id like `"gpt-4o"` match a longer, unrelated pattern like `"gpt-4o-mini"`
    /// and inherit the wrong limit.
    pub fn get(&self, model: &str) -> Option<ModelLimit> {
        // Exact user override match (highest priority).
        if let Some(limit) = self.user_limits.get(model) {
            return Some(limit.clone());
        }

        // Best partial match among user overrides: the pattern must be a
        // substring of the model id. Longer (more specific) patterns win for
        // deterministic selection. There is no built-in table; a miss returns
        // None and the caller falls back to the global default.
        self.user_limits
            .iter()
            .filter(|(pattern, _)| model.contains(pattern.as_str()))
            .max_by_key(|(pattern, _)| pattern.len())
            .map(|(_, limit)| limit.clone())
    }

    /// Get limit for a model with fallback to default.
    pub fn get_or_default(&self, model: &str) -> ModelLimit {
        self.get(model).unwrap_or_else(default_model_limit)
    }

    /// Save current user limits to the configured file.
    ///
    /// No-op when the registry has no `config_path`.
    pub async fn save_user_config(&self) -> std::io::Result<()> {
        let Some(path) = self.config_path.clone() else {
            return Ok(());
        };

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let limits: Vec<&ModelLimit> = self.user_limits.values().collect();
        let content = serde_json::to_string_pretty(&limits)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        tokio::fs::write(&path, content).await?;

        Ok(())
    }

    /// List all user-defined limits.
    pub fn list_user_limits(&self) -> Vec<&ModelLimit> {
        self.user_limits.values().collect()
    }
}

impl Default for ModelLimitsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the default configuration file path.
///
/// Returns `{bamboo_data_dir}/model_limits.json`, given the Bamboo data dir.
///
/// The caller supplies the base directory so this crate stays free of any
/// infrastructure/filesystem-config dependency.
pub fn get_default_config_path(bamboo_dir: &std::path::Path) -> PathBuf {
    bamboo_dir.join("model_limits.json")
}

/// Load user model limits from the unified `config.json` `model_limits` value.
///
/// The caller extracts the raw `model_limits` JSON value (e.g. from
/// `config.extra.get("model_limits")`) and passes it here, keeping this crate
/// independent of the concrete `Config` type.
///
/// Returns:
/// - `Ok(None)` when `model_limits` is absent.
/// - `Ok(Some(vec))` when present and valid (including empty array).
/// - `Err(...)` when present but not a valid `Vec<ModelLimit>`.
pub fn load_model_limits_from_unified_config(
    raw_limits: Option<&Value>,
) -> Result<Option<Vec<ModelLimit>>, String> {
    let Some(raw_limits) = raw_limits else {
        return Ok(None);
    };

    if raw_limits.is_null() {
        return Ok(Some(Vec::new()));
    }

    match raw_limits {
        Value::Array(_) => serde_json::from_value::<Vec<ModelLimit>>(raw_limits.clone())
            .map(Some)
            .map_err(|error| format!("invalid config.model_limits format: {error}")),
        _ => Err("invalid config.model_limits format: expected array".to_string()),
    }
}

/// Create a token budget for a specific model, resolving its limit from the
/// supplied `registry` (with user overrides loaded) and falling back to the
/// global default when there is no match.
///
/// The registry is a required parameter on purpose: a previous version built a
/// fresh empty `ModelLimitsRegistry::default()` internally, which silently
/// discarded every user override from `model_limits.json` and always returned
/// the global default (#20, bug 2). Callers must pass a registry they have
/// loaded user overrides into (or [`ModelLimitsRegistry::new`] when they
/// genuinely want the global default).
pub fn create_budget_for_model(
    model: &str,
    strategy: crate::BudgetStrategy,
    registry: &ModelLimitsRegistry,
) -> crate::TokenBudget {
    let limit = registry.get_or_default(model);

    crate::TokenBudget {
        max_context_tokens: limit.max_context_tokens,
        max_output_tokens: limit.get_max_output_tokens(),
        strategy,
        safety_margin: limit.get_safety_margin(),
        compression_trigger_percent: 85, // legacy — only used when working_reserve_tokens == 0
        compression_target_percent: 45,
        working_reserve_tokens: 50_000,
        fallback_trigger_percent: 75,
        prompt_cache_min_tool_output_chars: 1_200,
        prompt_cache_head_chars: 280,
        prompt_cache_tail_chars: 180,
        prompt_cache_recent_user_turns: 2,
        prompt_cache_recent_tool_chains: 2,
        max_tool_output_tokens: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limit_is_1m_128k() {
        let limit = default_model_limit();
        assert_eq!(limit.model_pattern, DEFAULT_MODEL_PATTERN);
        assert_eq!(limit.max_context_tokens, 1_000_000);
        assert_eq!(limit.get_max_output_tokens(), 128_000);
    }

    #[test]
    fn is_default_limit_detects_no_op_overrides() {
        // A row pinned to exactly the default values (any pattern) is a no-op.
        let mut noop = ModelLimit::new("gpt-4o", DEFAULT_MAX_CONTEXT_TOKENS);
        noop.max_output_tokens = Some(DEFAULT_MAX_OUTPUT_TOKENS);
        assert!(is_default_limit(&noop));

        // The synthesized global default is itself a no-op override.
        assert!(is_default_limit(&default_model_limit()));

        // A different context window is a real override.
        let mut smaller = ModelLimit::new("gpt-4o", 128_000);
        smaller.max_output_tokens = Some(DEFAULT_MAX_OUTPUT_TOKENS);
        assert!(!is_default_limit(&smaller));

        // An explicit safety margin is a real override even at default size.
        let mut custom_margin = ModelLimit::new("gpt-4o", DEFAULT_MAX_CONTEXT_TOKENS);
        custom_margin.max_output_tokens = Some(DEFAULT_MAX_OUTPUT_TOKENS);
        custom_margin.safety_margin = Some(500);
        assert!(!is_default_limit(&custom_margin));
    }

    #[test]
    fn registry_returns_none_for_unknown_without_overrides() {
        // No built-in table: an unknown model with no user override has no match.
        let registry = ModelLimitsRegistry::new();
        assert!(registry.get("gpt-5.2-codex").is_none());
        assert!(registry.get("some-brand-new-model").is_none());
    }

    #[test]
    fn registry_returns_default_for_unknown() {
        let registry = ModelLimitsRegistry::new();
        let limit = registry.get_or_default("unknown-model-xyz");
        assert_eq!(limit.model_pattern, DEFAULT_MODEL_PATTERN);
        assert_eq!(limit.max_context_tokens, 1_000_000);
        assert_eq!(limit.get_max_output_tokens(), 128_000);
    }

    #[test]
    fn user_override_exact_match_wins() {
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-5.2-codex", 64_000)); // Override with smaller limit

        let limit = registry
            .get("gpt-5.2-codex")
            .expect("Should find overridden limit");
        assert_eq!(limit.max_context_tokens, 64_000);
    }

    #[test]
    fn user_override_partial_match_longest_wins() {
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-5", 111_000));
        registry.add_limit(ModelLimit::new("gpt-5.2-codex", 222_000));

        // "gpt-5.2-codex-preview" contains both patterns; the longest wins.
        let limit = registry
            .get("gpt-5.2-codex-preview")
            .expect("Should partial-match a user override");
        assert_eq!(limit.max_context_tokens, 222_000);
    }

    #[test]
    fn model_limit_calculates_default_output_tokens() {
        let limit = ModelLimit::new("test", 100_000);
        // Default is min(max_context / 4, DEFAULT_MAX_OUTPUT_TOKENS)
        //        = min(25_000, 128_000) = 25_000 (no longer capped at 4096, #20 bug 4)
        assert_eq!(limit.get_max_output_tokens(), 25_000);
    }

    #[test]
    fn user_override_without_explicit_output_is_not_capped_at_4096() {
        // Issue #20 bug 4: a user override created with `ModelLimit::new` leaves
        // `max_output_tokens = None`. The derived default must scale with the
        // context window (context / 4) rather than collapsing to 4096.
        let gpt4o = ModelLimit::new("gpt-4o", 128_000);
        assert!(gpt4o.max_output_tokens.is_none());
        assert_eq!(gpt4o.get_max_output_tokens(), 32_000);

        // Very large context windows are still capped at the global default so a
        // single override can't request an unbounded output budget.
        let huge = ModelLimit::new("huge", 2_000_000);
        assert_eq!(huge.get_max_output_tokens(), DEFAULT_MAX_OUTPUT_TOKENS);
    }

    #[test]
    fn matching_is_directional_model_contains_pattern_only() {
        // Issue #20 bug 3: a short model id must NOT match a longer pattern.
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-4o-mini", 128_000));

        // "gpt-4o" does not contain "gpt-4o-mini", so it must NOT inherit the
        // mini override (the old `pattern.contains(model)` direction did).
        assert!(registry.get("gpt-4o").is_none());

        // The reverse still works: a model id that contains the pattern matches.
        let mini = registry
            .get("gpt-4o-mini-2024")
            .expect("model id contains the pattern");
        assert_eq!(mini.max_context_tokens, 128_000);
    }

    #[test]
    fn model_limit_uses_custom_output_tokens() {
        let mut limit = ModelLimit::new("test", 100_000);
        limit.max_output_tokens = Some(8192);
        assert_eq!(limit.get_max_output_tokens(), 8192);
    }

    #[test]
    fn model_limit_calculates_small_context_output() {
        let limit = ModelLimit::new("test", 8_192);
        // Default is min(8192 / 4, 4096) = 2048
        assert_eq!(limit.get_max_output_tokens(), 2048);
    }

    #[test]
    fn unified_config_loader_returns_none_when_absent() {
        let loaded = load_model_limits_from_unified_config(None).expect("should parse");
        assert!(loaded.is_none());
    }

    #[test]
    fn unified_config_loader_reads_valid_model_limits() {
        let raw = serde_json::json!([
            {
                "model_pattern": "gpt-5.2-codex",
                "max_context_tokens": 64000,
                "max_output_tokens": 2048,
                "safety_margin": 512
            }
        ]);

        let loaded = load_model_limits_from_unified_config(Some(&raw))
            .expect("should parse")
            .expect("should exist");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].model_pattern, "gpt-5.2-codex");
        assert_eq!(loaded[0].max_context_tokens, 64_000);
        assert_eq!(loaded[0].max_output_tokens, Some(2048));
        assert_eq!(loaded[0].safety_margin, Some(512));
    }

    #[test]
    fn unified_config_loader_errors_on_invalid_shape() {
        let raw = serde_json::json!({"unexpected": true});
        let error = load_model_limits_from_unified_config(Some(&raw)).expect_err("should error");
        assert!(error.contains("expected array"));
    }

    #[test]
    fn safety_margin_scales_with_context_window() {
        // Small context → floor at DEFAULT_SAFETY_MARGIN (1000)
        let small = ModelLimit::new("test", 8_192);
        assert_eq!(small.get_safety_margin(), 1000);

        // Medium context → proportional
        let medium = ModelLimit::new("test", 200_000);
        assert_eq!(medium.get_safety_margin(), 2000);

        // Large context → proportional
        let large = ModelLimit::new("test", 1_050_000);
        assert_eq!(large.get_safety_margin(), 10_500);

        // Explicit override takes precedence
        let mut custom = ModelLimit::new("test", 200_000);
        custom.safety_margin = Some(500);
        assert_eq!(custom.get_safety_margin(), 500);
    }

    #[tokio::test]
    async fn persisted_overrides_drive_runtime_resolution() {
        // Integration: a `model_limits.json` on disk → registry load → resolve.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            r#"[{"model_pattern":"gpt-4o","max_context_tokens":128000,"max_output_tokens":16384}]"#,
        )
        .await
        .expect("seed overrides");

        let mut registry = ModelLimitsRegistry::with_config_path(path);
        registry.load_user_config().await.expect("load user config");

        // Persisted override is applied at runtime.
        let gpt4o = registry.get("gpt-4o").expect("override present");
        assert_eq!(gpt4o.max_context_tokens, 128_000);
        assert_eq!(gpt4o.get_max_output_tokens(), 16_384);

        // Unknown model with no override falls back to the single global default.
        let unknown = registry.get_or_default("brand-new-frontier-model");
        assert_eq!(unknown.model_pattern, DEFAULT_MODEL_PATTERN);
        assert_eq!(unknown.max_context_tokens, 1_000_000);
        assert_eq!(unknown.get_max_output_tokens(), 128_000);
    }

    #[tokio::test]
    async fn revisioned_model_limits_sidecar_drives_runtime_resolution() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            r#"{
                "schema_version": 1,
                "revision": 7,
                "data": [{
                    "model_pattern": "dynamic-summary-model",
                    "max_context_tokens": 96000,
                    "max_output_tokens": 12000,
                    "safety_margin": 800
                }]
            }"#,
        )
        .await
        .expect("seed revisioned overrides");

        let mut registry = ModelLimitsRegistry::with_config_path(path);
        registry
            .load_user_config()
            .await
            .expect("load revisioned model-limit sidecar");

        let limit = registry
            .get("dynamic-summary-model")
            .expect("revisioned override present");
        assert_eq!(limit.max_context_tokens, 96_000);
        assert_eq!(limit.get_max_output_tokens(), 12_000);
        assert_eq!(limit.get_safety_margin(), 800);
    }

    #[tokio::test]
    async fn reloading_user_config_replaces_removed_patterns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            r#"[{
                "model_pattern": "removed-model",
                "max_context_tokens": 64000,
                "max_output_tokens": 8000
            }]"#,
        )
        .await
        .expect("seed legacy sidecar");

        let mut registry = ModelLimitsRegistry::with_config_path(&path);
        registry.load_user_config().await.expect("first load");
        assert!(registry.get("removed-model").is_some());

        tokio::fs::write(
            &path,
            r#"{
                "schema_version": 1,
                "revision": 2,
                "data": [{
                    "model_pattern": "replacement-model",
                    "max_context_tokens": 96000,
                    "max_output_tokens": 12000
                }]
            }"#,
        )
        .await
        .expect("replace sidecar");
        registry.load_user_config().await.expect("reload");

        assert!(registry.get("removed-model").is_none());
        assert_eq!(
            registry
                .get("replacement-model")
                .expect("replacement pattern")
                .max_context_tokens,
            96_000
        );
    }

    #[tokio::test]
    async fn reloading_after_sidecar_deletion_clears_stale_patterns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            r#"[{
                "model_pattern": "deleted-model",
                "max_context_tokens": 64000,
                "max_output_tokens": 8000
            }]"#,
        )
        .await
        .expect("seed model-limit sidecar");

        let mut registry = ModelLimitsRegistry::with_config_path(&path);
        registry.load_user_config().await.expect("first load");
        assert!(registry.get("deleted-model").is_some());

        tokio::fs::remove_file(&path)
            .await
            .expect("remove model-limit sidecar");
        registry.load_user_config().await.expect("reload deletion");

        assert!(
            registry.get("deleted-model").is_none(),
            "deleting the sidecar must not leave its patterns active in memory"
        );
    }

    #[tokio::test]
    async fn persisted_override_drives_runtime_token_budget() {
        // Full chain (issue #20 acceptance): set a model limit on disk →
        // load registry → build the runtime TokenBudget → the budget reflects
        // the user-configured context window, not a stale/default value.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        // Note: NO explicit max_output_tokens, exercising the bug-4 default path.
        tokio::fs::write(
            &path,
            r#"[{"model_pattern":"gpt-4o","max_context_tokens":128000}]"#,
        )
        .await
        .expect("seed overrides");

        let mut registry = ModelLimitsRegistry::with_config_path(path);
        registry.load_user_config().await.expect("load user config");

        // The runtime budget for the configured model matches the user limit...
        let budget = create_budget_for_model("gpt-4o", crate::BudgetStrategy::default(), &registry);
        assert_eq!(budget.max_context_tokens, 128_000);
        // ...and the derived output budget is context/4 (32K), not the old 4096 cap.
        assert_eq!(budget.max_output_tokens, 32_000);

        // A model that does NOT contain the pattern is unaffected (bug-3 fix):
        // it resolves to the global default, not the gpt-4o override.
        let other =
            create_budget_for_model("claude-sonnet", crate::BudgetStrategy::default(), &registry);
        assert_eq!(other.max_context_tokens, 1_000_000);
    }

    #[test]
    fn create_budget_for_model_uses_global_default_for_unmatched_model() {
        // An empty registry yields the global default for any model.
        let registry = ModelLimitsRegistry::new();
        let budget = create_budget_for_model(
            "anything-at-all",
            crate::BudgetStrategy::default(),
            &registry,
        );
        assert_eq!(budget.max_context_tokens, 1_000_000);
        assert_eq!(budget.max_output_tokens, 128_000);
    }

    #[test]
    fn create_budget_for_model_honors_registry_user_overrides() {
        // Issue #20 bug 2: the budget must reflect the user override carried by
        // the registry, not silently fall back to the global default.
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-4o", 128_000));

        let budget = create_budget_for_model("gpt-4o", crate::BudgetStrategy::default(), &registry);
        assert_eq!(budget.max_context_tokens, 128_000);
        // And the derived output budget is the un-capped context/4 (bug 4), not 4096.
        assert_eq!(budget.max_output_tokens, 32_000);
    }
}
