use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;
use bamboo_compression::limits::{load_model_limits_from_unified_config, ModelLimit};
use bamboo_compression::{ModelLimitsRegistry, TokenBudget};
use bamboo_llm::provider::LLMProvider;

pub(super) async fn resolve_token_budget(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    // Priority: session override > config override > model defaults.
    if let Some(ref budget) = session.token_budget {
        tracing::debug!("Using session-specific token budget");
        return budget.clone();
    }

    if let Some(ref budget) = config.token_budget {
        tracing::debug!("Using config token budget");
        return budget.clone();
    }

    // Default to model limits:
    // 1. built-in defaults
    // 2. provider runtime metadata (copilot only)
    // 3. dedicated config file: model_limits.json
    // 4. legacy fallback: config.json -> model_limits
    let mut registry = ModelLimitsRegistry::with_config_path(
        bamboo_compression::limits::get_default_config_path(&bamboo_config::paths::bamboo_dir()),
    );

    if let Some(provider_limit) = resolve_provider_runtime_limit(config, llm, model_name).await {
        registry.add_limit(provider_limit);
    }

    let loaded_from_file = match registry.load_user_config().await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                "Failed to load model limits from {:?}: {}. Falling back to legacy config.json key.",
                bamboo_compression::limits::get_default_config_path(&bamboo_config::paths::bamboo_dir()),
                error
            );
            false
        }
    };

    if !loaded_from_file {
        // Legacy fallback: parse the config.json `model_limits` key. The value is
        // snapshotted from the live in-memory config at loop-config build time
        // (config.legacy_model_limits) — NOT re-read from disk via Config::new(),
        // which would diverge from the server's live config and clobber the global
        // env-var cache (#38). Pure JSON parse, so no spawn_blocking needed.
        apply_legacy_model_limits(&mut registry, config.legacy_model_limits.as_ref());
    }

    let matched_limit = registry.get(model_name);
    let model_limit = matched_limit
        .clone()
        .unwrap_or_else(|| registry.get_or_default(model_name));

    if matched_limit.is_some() {
        tracing::debug!(
            "Using model limit for '{}': context={}, max_output={}, safety_margin={}",
            model_name,
            model_limit.max_context_tokens,
            model_limit.get_max_output_tokens(),
            model_limit.get_safety_margin()
        );
    } else {
        tracing::info!(
            "No model limit match for '{}', using fallback '{}' (context={}). Override via {:?}",
            model_name,
            model_limit.model_pattern,
            model_limit.max_context_tokens,
            bamboo_compression::limits::get_default_config_path(&bamboo_config::paths::bamboo_dir())
        );
    }

    TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        bamboo_compression::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    )
}

/// Apply the legacy `config.json` `model_limits` value (snapshotted from the
/// live in-memory config) to `registry`. Pure: parses the JSON and adds each
/// limit; a parse error is logged and ignored (the registry keeps its defaults).
fn apply_legacy_model_limits(
    registry: &mut ModelLimitsRegistry,
    legacy_model_limits: Option<&serde_json::Value>,
) {
    match load_model_limits_from_unified_config(legacy_model_limits) {
        Ok(Some(limits)) => {
            for limit in limits {
                registry.add_limit(limit);
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                "Failed to parse legacy model limits from config.json key 'model_limits': {}.",
                error
            );
        }
    }
}

async fn resolve_provider_runtime_limit(
    config: &AgentLoopConfig,
    llm: &dyn LLMProvider,
    model_name: &str,
) -> Option<ModelLimit> {
    if config.provider_type.as_deref() != Some("copilot") {
        return None;
    }

    let model_info = match llm.list_model_info().await {
        Ok(models) => models.into_iter().find(|entry| entry.id == model_name),
        Err(error) => {
            tracing::warn!(
                "Failed to fetch Copilot model metadata for token budget: {}",
                error
            );
            None
        }
    }?;

    let max_context_tokens = model_info.max_context_tokens?;

    let mut limit = ModelLimit::new(model_name.to_string(), max_context_tokens);
    limit.max_output_tokens = model_info.max_output_tokens;

    tracing::info!(
        "Using Copilot runtime model metadata for '{}': context={}, max_output={}",
        model_name,
        max_context_tokens,
        model_info
            .max_output_tokens
            .map(|value| value.to_string())
            .unwrap_or_else(|| "auto".to_string())
    );

    Some(limit)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use async_trait::async_trait;
    use futures::{stream, Stream};

    use super::*;
    use bamboo_agent_core::{tools::ToolSchema, Message};
    use bamboo_llm::provider::{LLMError, ProviderModelInfo, Result};
    use bamboo_llm::types::LLMChunk;

    #[test]
    fn apply_legacy_model_limits_adds_parsed_limits_to_registry() {
        // A legacy config.json `model_limits` value, as snapshotted into
        // AgentLoopConfig.legacy_model_limits from the live in-memory config.
        let legacy = serde_json::json!([
            { "model_pattern": "legacy-model", "max_context_tokens": 12345 }
        ]);
        let mut registry = ModelLimitsRegistry::new();
        apply_legacy_model_limits(&mut registry, Some(&legacy));
        let got = registry
            .get("legacy-model")
            .expect("legacy model limit was applied to the registry");
        assert_eq!(got.max_context_tokens, 12345);
    }

    #[test]
    fn apply_legacy_model_limits_is_noop_for_none_or_malformed() {
        let mut registry = ModelLimitsRegistry::new();
        // No legacy value -> nothing added.
        apply_legacy_model_limits(&mut registry, None);
        assert!(registry.get("legacy-model").is_none());
        // Malformed value -> logged + ignored, not a panic, nothing added.
        let bad = serde_json::json!({ "not": "an array" });
        apply_legacy_model_limits(&mut registry, Some(&bad));
        assert!(registry.get("legacy-model").is_none());
    }

    #[derive(Default)]
    struct MetadataProvider {
        models: Vec<ProviderModelInfo>,
    }

    #[async_trait]
    impl LLMProvider for MetadataProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>> {
            Ok(Box::pin(stream::empty()))
        }

        async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(self.models.clone())
        }
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_uses_copilot_metadata() {
        let config = AgentLoopConfig {
            provider_type: Some("copilot".to_string()),
            ..Default::default()
        };

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex")
            .await
            .expect("copilot metadata should resolve");
        assert_eq!(limit.max_context_tokens, 222_000);
        assert_eq!(limit.max_output_tokens, Some(33_000));
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_ignores_non_copilot_provider() {
        let config = AgentLoopConfig {
            provider_type: Some("openai".to_string()),
            ..Default::default()
        };

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_requires_context_tokens() {
        let config = AgentLoopConfig {
            provider_type: Some("copilot".to_string()),
            ..Default::default()
        };

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: None,
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_returns_none_on_model_info_error() {
        struct FailingProvider;

        #[async_trait]
        impl LLMProvider for FailingProvider {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> Result<Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>> {
                Ok(Box::pin(stream::empty()))
            }

            async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
                Err(LLMError::Api("boom".to_string()))
            }
        }

        let config = AgentLoopConfig {
            provider_type: Some("copilot".to_string()),
            ..Default::default()
        };

        let limit =
            resolve_provider_runtime_limit(&config, &FailingProvider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }
}
