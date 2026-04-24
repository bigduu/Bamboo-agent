//! Model context window limits registry.
//!
//! Provides known context window sizes for common models, with fallback to
//! configurable user limits via file or session overrides.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// Known model defaults: `(pattern, max_context_tokens, max_output_tokens)`.
///
/// These are the built-in defaults used when there is no user override in
/// `config.json:model_limits` and no legacy `model_limits.json`.
pub const KNOWN_MODEL_LIMITS: &[(&str, u32, u32)] = &[
    // GitHub Copilot model profiles (2026-03)
    // Anthropic
    ("claude-haiku-4.5", 160_000, 32_000),
    ("claude-opus-4.5", 160_000, 32_000),
    ("claude-opus-4.6", 192_000, 64_000),
    ("claude-sonnet-4.6", 200_000, 32_000),
    ("claude-sonnet-4-6", 200_000, 32_000), // Alternate wire format
    ("claude-sonnet-4.5", 200_000, 32_000),
    ("claude-sonnet-4-5", 200_000, 32_000), // Alternate wire format
    // Google
    ("gemini-2.5-pro", 128_000, 16_000),
    ("gemini-3-flash-preview", 1_000_000, 8_192),
    ("gemini-3.1-pro-preview", 128_000, 64_000),
    // OpenAI
    ("gpt-5.4", 1_050_000, 32_768),
    ("gpt-5.3-codex", 400_000, 128_000),
    ("gpt-5.2-codex", 400_000, 128_000),
    ("gpt-5.2", 400_000, 128_000),
    ("gpt-5.1", 400_000, 128_000),
    ("gpt-5", 400_000, 128_000),
    ("gpt-5.4-mini", 128_000, 16_384),
    ("gpt-4.1", 128_000, 16_384),
    ("gpt-4-o-preview", 128_000, 16_384),
    ("gpt-4o-preview", 128_000, 16_384), // Alternate spelling
    // xAI
    ("grok-code-fast-1", 128_000, 10_240),
    // GitHub specialized
    ("oswe-vscode-prime", 264_000, 64_000),
    // Backward-compatible aliases and non-Copilot profiles
    ("gpt-5.4-thinking", 1_000_000, 128_000),
    ("gpt-5.2-pro", 256_000, 64_000),
    ("gpt-5-mini", 400_000, 128_000),
    ("gpt-4o", 128_000, 16_000),
    // Moonshot
    ("kimi-k2.5", 256_000, 64_000),
    ("kimi-for-coding", 256_000, 64_000),
    // Zhipu
    ("glm-5", 200_000, 128_000),
    // Compatibility fallbacks
    ("gpt-4o-mini", 128_000, 16_000),
    ("gpt-4-turbo", 128_000, 16_000),
    ("gpt-4", 8_192, 4_096),
    ("gpt-3.5-turbo", 16_385, 4_096),
    ("claude-3-5-sonnet", 200_000, 8_192),
    ("claude-3-5-sonnet-20241022", 200_000, 8_192),
    ("claude-3-5-sonnet-20240620", 200_000, 8_192),
    ("claude-3-opus", 200_000, 8_192),
    ("claude-3-opus-20240229", 200_000, 8_192),
    ("claude-3-sonnet", 200_000, 8_192),
    ("claude-3-haiku", 200_000, 8_192),
    ("copilot-chat", 128_000, 16_000),
    // Default fallback
    ("default", 128_000, 4_096),
];

/// Default maximum output tokens (reserve ~25% for response).
pub const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 4096;

/// Default safety margin for token counting errors.
pub const DEFAULT_SAFETY_MARGIN: u32 = 1000;

/// Model limit configuration (user-overridable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelLimit {
    /// Model identifier (partial match supported, e.g., "gpt-4" matches "gpt-4o")
    pub model_pattern: String,
    /// Maximum context window size in tokens
    pub max_context_tokens: u32,
    /// Maximum output tokens (defaults to min(4096, max_context / 4))
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
    pub fn get_max_output_tokens(&self) -> u32 {
        self.max_output_tokens
            .unwrap_or_else(|| (self.max_context_tokens / 4).min(4096))
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

    /// Load user overrides from the default configuration path.
    ///
    /// Default path: `{bamboo_data_dir}/model_limits.json`
    pub async fn load_user_config(&mut self) -> std::io::Result<()> {
        let path = self
            .config_path
            .clone()
            .unwrap_or_else(get_default_config_path);

        if !path.exists() {
            return Ok(());
        }

        let content = tokio::fs::read_to_string(&path).await?;
        let limits: Vec<ModelLimit> = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        for limit in limits {
            self.user_limits.insert(limit.model_pattern.clone(), limit);
        }

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
    /// 3. Pattern contains model (e.g., "gpt-4" contains "gpt")
    ///
    /// For partial matches, the longest (most specific) pattern wins.
    pub fn get(&self, model: &str) -> Option<ModelLimit> {
        // First check user limits for exact match
        if let Some(limit) = self.user_limits.get(model) {
            return Some(limit.clone());
        }

        // Check built-in limits for exact match
        for (pattern, max_context_tokens, max_output_tokens) in KNOWN_MODEL_LIMITS {
            if *pattern == model {
                return Some(builtin_limit(
                    model,
                    *max_context_tokens,
                    *max_output_tokens,
                ));
            }
        }

        // Find the best partial match from user limits
        // Sort by pattern length (longer = more specific) for deterministic selection
        let best_user_match = self
            .user_limits
            .iter()
            .filter(|(pattern, _)| model.contains(*pattern) || pattern.contains(model))
            .max_by_key(|(pattern, _)| pattern.len())
            .map(|(_, limit)| limit.clone());

        if let Some(limit) = best_user_match {
            return Some(limit);
        }

        // Find the best partial match from built-in limits
        let best_builtin_match = KNOWN_MODEL_LIMITS
            .iter()
            .filter(|(pattern, _, _)| model.contains(*pattern) || pattern.contains(model))
            .max_by_key(|(pattern, _, _)| pattern.len());

        if let Some((pattern, max_context_tokens, max_output_tokens)) = best_builtin_match {
            return Some(builtin_limit(
                pattern,
                *max_context_tokens,
                *max_output_tokens,
            ));
        }

        None
    }

    /// Get limit for a model with fallback to default.
    pub fn get_or_default(&self, model: &str) -> ModelLimit {
        self.get(model).unwrap_or_else(|| {
            let default = KNOWN_MODEL_LIMITS
                .iter()
                .find(|(k, _, _)| *k == "default")
                .map(|(_, max_context_tokens, max_output_tokens)| {
                    (*max_context_tokens, *max_output_tokens)
                })
                .unwrap_or((128_000, DEFAULT_MAX_OUTPUT_TOKENS));
            let mut limit = ModelLimit::new("default", default.0);
            limit.max_output_tokens = Some(default.1);
            limit
        })
    }

    /// Save current user limits to the configuration file.
    pub async fn save_user_config(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .clone()
            .unwrap_or_else(get_default_config_path);

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
/// Returns `{bamboo_data_dir}/model_limits.json`.
pub fn get_default_config_path() -> PathBuf {
    bamboo_infrastructure::paths::bamboo_dir().join("model_limits.json")
}

/// Load user model limits from unified `config.json` root key `model_limits`.
///
/// Returns:
/// - `Ok(None)` when `model_limits` key is absent.
/// - `Ok(Some(vec))` when key exists and is valid (including empty array).
/// - `Err(...)` when key exists but is not a valid `Vec<ModelLimit>`.
pub fn load_model_limits_from_unified_config(
    config: &bamboo_infrastructure::Config,
) -> Result<Option<Vec<ModelLimit>>, String> {
    let Some(raw_limits) = config.extra.get("model_limits") else {
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

/// Create a token budget for a specific model.
///
/// This is a convenience function that creates a budget with appropriate defaults.
pub fn create_budget_for_model(model: &str, strategy: crate::BudgetStrategy) -> crate::TokenBudget {
    let registry = ModelLimitsRegistry::default();
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
    fn builtin_limits_contain_common_models() {
        let gpt54 = KNOWN_MODEL_LIMITS
            .iter()
            .find(|(k, _, _)| *k == "gpt-5.4")
            .expect("Should have gpt-5.4");
        assert_eq!(gpt54.1, 1_050_000);
        assert_eq!(gpt54.2, 32_768);

        let gpt53_codex = KNOWN_MODEL_LIMITS
            .iter()
            .find(|(k, _, _)| *k == "gpt-5.3-codex")
            .expect("Should have gpt-5.3-codex");
        assert_eq!(gpt53_codex.1, 400_000);
        assert_eq!(gpt53_codex.2, 128_000);

        let gpt52_codex = KNOWN_MODEL_LIMITS
            .iter()
            .find(|(k, _, _)| *k == "gpt-5.2-codex")
            .expect("Should have gpt-5.2-codex");
        assert_eq!(gpt52_codex.1, 400_000);
        assert_eq!(gpt52_codex.2, 128_000);

        let gemini31_pro_preview = KNOWN_MODEL_LIMITS
            .iter()
            .find(|(k, _, _)| *k == "gemini-3.1-pro-preview")
            .expect("Should have gemini-3.1-pro-preview");
        assert_eq!(gemini31_pro_preview.1, 128_000);
        assert_eq!(gemini31_pro_preview.2, 64_000);
    }

    #[test]
    fn registry_finds_builtin_by_exact_match() {
        let registry = ModelLimitsRegistry::new();
        let limit = registry
            .get("gpt-5.2-codex")
            .expect("Should find gpt-5.2-codex");
        assert_eq!(limit.max_context_tokens, 400_000);
        assert_eq!(limit.get_max_output_tokens(), 128_000);
    }

    #[test]
    fn registry_finds_builtin_by_partial_match() {
        let registry = ModelLimitsRegistry::new();
        // "gpt-5.2-codex-preview" contains "gpt-5.2-codex"
        let limit = registry
            .get("gpt-5.2-codex-preview")
            .expect("Should find gpt-5.2-codex");
        assert_eq!(limit.max_context_tokens, 400_000);
        assert_eq!(limit.get_max_output_tokens(), 128_000);
    }

    #[test]
    fn registry_returns_default_for_unknown() {
        let registry = ModelLimitsRegistry::new();
        let limit = registry.get_or_default("unknown-model-xyz");
        assert_eq!(limit.model_pattern, "default");
    }

    #[test]
    fn user_override_takes_precedence() {
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-5.2-codex", 64_000)); // Override with smaller limit

        let limit = registry
            .get("gpt-5.2-codex")
            .expect("Should find overridden limit");
        assert_eq!(limit.max_context_tokens, 64_000);
    }

    #[test]
    fn model_limit_calculates_default_output_tokens() {
        let limit = ModelLimit::new("test", 100_000);
        // Default is min(max_context / 4, 4096) = min(25000, 4096) = 4096
        assert_eq!(limit.get_max_output_tokens(), 4096);
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
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config =
            bamboo_infrastructure::Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        let loaded = load_model_limits_from_unified_config(&config).expect("should parse");
        assert!(loaded.is_none());
    }

    #[test]
    fn unified_config_loader_reads_valid_model_limits() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut config =
            bamboo_infrastructure::Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        config.extra.insert(
            "model_limits".to_string(),
            serde_json::json!([
                {
                    "model_pattern": "gpt-5.2-codex",
                    "max_context_tokens": 64000,
                    "max_output_tokens": 2048,
                    "safety_margin": 512
                }
            ]),
        );

        let loaded = load_model_limits_from_unified_config(&config)
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
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut config =
            bamboo_infrastructure::Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        config.extra.insert(
            "model_limits".to_string(),
            serde_json::json!({"unexpected": true}),
        );

        let error = load_model_limits_from_unified_config(&config).expect_err("should error");
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
}
