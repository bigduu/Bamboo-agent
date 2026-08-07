use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;
use bamboo_compression::limits::{load_model_limits_from_unified_config, ModelLimit};
use bamboo_compression::{ModelLimitsRegistry, TokenBudget};
use bamboo_domain::bounded_dedup::{BoundedFingerprintSet, DEFAULT_BOUNDED_FINGERPRINT_CAPACITY};
use bamboo_llm::provider::LLMProvider;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

const CONSERVATIVE_SUMMARIZER_CONTEXT_TOKENS: u32 = 32_000;
const CONSERVATIVE_SUMMARIZER_OUTPUT_TOKENS: u32 = 8_000;
const CONSERVATIVE_SUMMARIZER_SAFETY_MARGIN: u32 = 1_000;

static STATIC_WARNINGS: LazyLock<BoundedFingerprintSet> =
    LazyLock::new(|| BoundedFingerprintSet::new(DEFAULT_BOUNDED_FINGERPRINT_CAPACITY));

fn model_limits_path(config: &AgentLoopConfig) -> PathBuf {
    let data_dir = config
        .app_data_dir
        .clone()
        .unwrap_or_else(bamboo_config::paths::bamboo_dir);
    bamboo_compression::limits::get_default_config_path(&data_dir)
}

/// Resolve the budget for an auxiliary model/provider pair without touching the
/// chat model's session cache. Compression can route to a completely different
/// provider and must therefore never inherit the triggering model's limits.
pub(super) async fn resolve_auxiliary_token_budget(
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    let model_limits_path = model_limits_path(config);

    let configured_limit =
        resolve_configured_model_limit(config, model_name, &model_limits_path, "summarization")
            .await;
    let provider_limit = if configured_limit.is_some() {
        None
    } else {
        match llm.list_model_info().await {
            Ok(models) => models
                .into_iter()
                .find(|entry| entry.id == model_name)
                .and_then(|model_info| {
                    model_info.max_context_tokens.map(|max_context_tokens| {
                        let mut limit = ModelLimit::new(model_name.to_string(), max_context_tokens);
                        limit.max_output_tokens = model_info.max_output_tokens;
                        limit
                    })
                }),
            Err(error) => {
                tracing::warn!(
                    model = model_name,
                    error = %error,
                    "Failed to resolve summarization-model runtime limits"
                );
                None
            }
        }
    };

    // Explicit user overrides (including partial patterns) outrank exact
    // provider metadata. Keeping every source in its own registry avoids an
    // exact provider or legacy entry accidentally shadowing a higher-priority
    // user pattern.
    let model_limit = match configured_limit.or(provider_limit) {
        Some(limit) => limit,
        None => {
            let key = ("unknown-summarization-model-limit", model_name);
            let error = "no configured or provider-reported limit";
            if STATIC_WARNINGS.insert_if_new(&key, error) {
                tracing::warn!(
                    model = model_name,
                    context_tokens = CONSERVATIVE_SUMMARIZER_CONTEXT_TOKENS,
                    output_tokens = CONSERVATIVE_SUMMARIZER_OUTPUT_TOKENS,
                    "No summarization-model limit is known; using conservative bounded fallback"
                );
            } else {
                tracing::debug!(
                    model = model_name,
                    context_tokens = CONSERVATIVE_SUMMARIZER_CONTEXT_TOKENS,
                    output_tokens = CONSERVATIVE_SUMMARIZER_OUTPUT_TOKENS,
                    "No summarization-model limit is known; using conservative bounded fallback"
                );
            }
            let mut fallback = ModelLimit::new(
                model_name.to_string(),
                CONSERVATIVE_SUMMARIZER_CONTEXT_TOKENS,
            );
            fallback.max_output_tokens = Some(CONSERVATIVE_SUMMARIZER_OUTPUT_TOKENS);
            fallback.safety_margin = Some(CONSERVATIVE_SUMMARIZER_SAFETY_MARGIN);
            fallback
        }
    };

    TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        bamboo_compression::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    )
}

pub(super) async fn resolve_token_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    let model_limits_path = model_limits_path(config);
    resolve_token_budget_with_model_limits_path(
        session,
        config,
        model_name,
        llm,
        &model_limits_path,
    )
    .await
}

async fn resolve_token_budget_with_model_limits_path(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
    model_limits_path: &Path,
) -> TokenBudget {
    // Priority: session/child override > config override > freshly-resolved
    // model defaults.
    if let Some(ref budget) = session.token_budget {
        tracing::debug!("Using session-specific token budget");
        return budget.clone();
    }

    if let Some(ref budget) = config.token_budget {
        tracing::debug!("Using config token budget");
        return budget.clone();
    }

    // Resolve each source independently so an exact lower-priority provider
    // record cannot shadow a higher-priority partial user pattern:
    // 1. dedicated model_limits.json
    // 2. legacy config.json -> model_limits
    // 3. provider runtime metadata (Copilot)
    // 4. global fallback
    let configured_limit =
        resolve_configured_model_limit(config, model_name, model_limits_path, "chat").await;
    let provider_limit = resolve_provider_runtime_limit(config, llm, model_name).await;
    let matched_limit = configured_limit.or(provider_limit);
    let model_limit = matched_limit
        .clone()
        .unwrap_or_else(|| ModelLimitsRegistry::new().get_or_default(model_name));

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
            model_limits_path
        );
    }

    let resolved = TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        bamboo_compression::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    );

    // Publish the freshly resolved budget on the session so every downstream
    // reader in this round — `build_context_pressure` (tool-output truncation),
    // the server context bar, `estimate_context_compression_exposure` — sees the
    // same limits via `Session::effective_token_budget` instead of `None`
    // (issue #20 bug 1). This runtime snapshot is never used to skip resolution:
    // every new round re-reads `model_limits.json` and refreshes provider
    // metadata, so an edit takes effect in an already-running session.
    session.resolved_token_budget = Some((model_name.to_string(), resolved.clone()));

    resolved
}

/// Resolve user-configured limits without mixing source precedence into a
/// single registry.
///
/// The dedicated revisioned sidecar is the authoritative first layer. A
/// legacy flattened config entry is still checked per model when the sidecar
/// has no matching pattern, which supports gradual migration without allowing
/// an exact provider metadata record to shadow a user family pattern.
async fn resolve_configured_model_limit(
    config: &AgentLoopConfig,
    model_name: &str,
    model_limits_path: &Path,
    purpose: &str,
) -> Option<ModelLimit> {
    let mut dedicated_registry = ModelLimitsRegistry::with_config_path(model_limits_path);
    if let Err(error) = dedicated_registry.load_user_config().await {
        let error_fingerprint = error.to_string();
        let key = ("model-limits-file", model_limits_path);
        if STATIC_WARNINGS.insert_if_new(&key, &error_fingerprint) {
            tracing::warn!(
                model = model_name,
                purpose,
                error = %error,
                path = ?model_limits_path,
                "Failed to load model_limits.json; checking legacy configured limits"
            );
        } else {
            tracing::debug!(
                model = model_name,
                purpose,
                error = %error,
                path = ?model_limits_path,
                "Failed to load model_limits.json; checking legacy configured limits"
            );
        }
    } else if let Some(limit) = dedicated_registry.get(model_name) {
        return Some(limit);
    }

    // The legacy value is snapshotted from the live in-memory config at
    // loop-config build time. Do not re-read Config::new() here: that would
    // diverge from the server's live config and clobber the global env cache
    // (#38).
    let mut legacy_registry = ModelLimitsRegistry::new();
    apply_legacy_model_limits(&mut legacy_registry, config.legacy_model_limits.as_ref());
    legacy_registry.get(model_name)
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
            let error_fingerprint = error.to_string();
            if STATIC_WARNINGS.insert_if_new("legacy-model-limits", &error_fingerprint) {
                tracing::warn!(
                    "Failed to parse legacy model limits from config.json key 'model_limits': {}.",
                    error
                );
            } else {
                tracing::debug!(
                    "Failed to parse legacy model limits from config.json key 'model_limits': {}.",
                    error
                );
            }
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
    use std::sync::atomic::{AtomicU32, Ordering};

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

    // Issue #20 bug 1: `resolve_token_budget` must publish the resolved budget so
    // every downstream reader (build_context_pressure, the server context bar,
    // estimate_context_compression_exposure) observes it via
    // `effective_token_budget` instead of `None`. Per #180 the snapshot lives in the
    // non-persisted `resolved_token_budget` slot (keyed by model), NOT the
    // persisted `token_budget` override slot.
    #[tokio::test]
    async fn resolve_token_budget_publishes_resolved_budget_on_session() {
        let mut session = bamboo_agent_core::Session::new("budget-cache", "some-model");
        assert!(
            session.effective_token_budget().is_none(),
            "precondition: a fresh session has no budget"
        );

        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();

        let resolved = resolve_token_budget(&mut session, &config, "some-model", &provider).await;

        // The persisted override slot stays empty; the snapshot holds the resolved
        // budget keyed by model, and effective_token_budget surfaces it.
        assert!(
            session.token_budget.is_none(),
            "the resolved snapshot must NOT populate the persisted override slot (#180)"
        );
        let (cached_model, cached) = session
            .resolved_token_budget
            .clone()
            .expect("resolved budget must be published on the session (#20 bug 1)");
        assert_eq!(cached_model, "some-model");
        assert_eq!(cached.max_context_tokens, resolved.max_context_tokens);
        assert_eq!(cached.max_output_tokens, resolved.max_output_tokens);
        assert_eq!(cached.safety_margin, resolved.safety_margin);
        assert_eq!(
            session.effective_token_budget().unwrap().max_context_tokens,
            resolved.max_context_tokens
        );
    }

    // #180: the resolved snapshot is `#[serde(skip)]`, so it never persists. A
    // reloaded long-lived session therefore starts with no snapshot and re-resolves
    // from the current `model_limits.json` — the exact staleness #180 fixes.
    #[tokio::test]
    async fn resolved_token_budget_is_not_persisted() {
        let mut session = bamboo_agent_core::Session::new("budget-persist", "some-model");
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();
        let _ = resolve_token_budget(&mut session, &config, "some-model", &provider).await;
        assert!(
            session.resolved_token_budget.is_some(),
            "runtime snapshot populated"
        );

        let json = serde_json::to_string(&session).expect("serialize");
        assert!(
            !json.contains("resolved_token_budget"),
            "the resolved snapshot must not be serialized (#180)"
        );
        let reloaded: bamboo_agent_core::Session =
            serde_json::from_str(&json).expect("deserialize");
        assert!(
            reloaded.resolved_token_budget.is_none(),
            "a reloaded session must have no resolved snapshot, so it re-resolves (#180)"
        );
    }

    // #180: a mid-session model switch updates the keyed runtime snapshot.
    #[tokio::test]
    async fn resolve_token_budget_reresolves_on_model_switch() {
        let mut session = bamboo_agent_core::Session::new("budget-switch", "model-a");
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();

        let _ = resolve_token_budget(&mut session, &config, "model-a", &provider).await;
        assert_eq!(session.resolved_token_budget.as_ref().unwrap().0, "model-a");

        // Switch the model: resolution refreshes and re-keys the snapshot.
        let _ = resolve_token_budget(&mut session, &config, "model-b", &provider).await;
        assert_eq!(
            session.resolved_token_budget.as_ref().unwrap().0,
            "model-b",
            "a model switch must re-resolve and re-key the cache (#180)"
        );
    }

    // A budget already present on the session is the highest-priority source and
    // must be returned verbatim (never recomputed/overwritten).
    #[tokio::test]
    async fn resolve_token_budget_prefers_existing_session_budget() {
        let mut session = bamboo_agent_core::Session::new("budget-existing", "some-model");
        let preset = TokenBudget::with_safety_margin(
            123_456,
            7_890,
            bamboo_compression::BudgetStrategy::default(),
            321,
        );
        session.token_budget = Some(preset.clone());

        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();

        let resolved = resolve_token_budget(&mut session, &config, "some-model", &provider).await;
        assert_eq!(resolved.max_context_tokens, 123_456);
        assert_eq!(resolved.max_output_tokens, 7_890);
        assert_eq!(resolved.safety_margin, preset.safety_margin);
        // Still exactly the preset value on the session.
        assert_eq!(
            session.token_budget.as_ref().unwrap().max_context_tokens,
            123_456
        );
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

    struct MutableMetadataProvider {
        max_context_tokens: AtomicU32,
        max_output_tokens: AtomicU32,
    }

    #[async_trait]
    impl LLMProvider for MutableMetadataProvider {
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
            Ok(vec![ProviderModelInfo {
                id: "dynamic-model-limit-763".to_string(),
                max_context_tokens: Some(self.max_context_tokens.load(Ordering::SeqCst)),
                max_output_tokens: Some(self.max_output_tokens.load(Ordering::SeqCst)),
            }])
        }
    }

    #[tokio::test]
    async fn same_model_limit_is_refreshed_each_round_without_session_reload() {
        let mut session =
            bamboo_agent_core::Session::new("dynamic-limit", "dynamic-model-limit-763");
        let config = AgentLoopConfig {
            provider_type: Some("copilot".to_string()),
            ..Default::default()
        };
        let provider = MutableMetadataProvider {
            max_context_tokens: AtomicU32::new(64_000),
            max_output_tokens: AtomicU32::new(8_000),
        };

        let first =
            resolve_token_budget(&mut session, &config, "dynamic-model-limit-763", &provider).await;
        assert_eq!(first.max_context_tokens, 64_000);
        assert_eq!(first.max_output_tokens, 8_000);

        provider.max_context_tokens.store(96_000, Ordering::SeqCst);
        provider.max_output_tokens.store(12_000, Ordering::SeqCst);
        let refreshed =
            resolve_token_budget(&mut session, &config, "dynamic-model-limit-763", &provider).await;

        assert_eq!(refreshed.max_context_tokens, 96_000);
        assert_eq!(refreshed.max_output_tokens, 12_000);
        assert_eq!(
            session
                .resolved_token_budget
                .as_ref()
                .expect("latest runtime snapshot")
                .1
                .max_context_tokens,
            96_000
        );
    }

    #[tokio::test]
    async fn revisioned_model_limit_edit_refreshes_same_running_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        let write_limit = |revision: u64, context: u32, output: u32| {
            let path = path.clone();
            async move {
                tokio::fs::write(
                    path,
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "revision": revision,
                        "data": [{
                            "model_pattern": "live-sidecar-model-763",
                            "max_context_tokens": context,
                            "max_output_tokens": output,
                            "safety_margin": 500
                        }]
                    }))
                    .expect("serialize model-limit envelope"),
                )
                .await
                .expect("write model-limit envelope");
            }
        };
        write_limit(1, 64_000, 8_000).await;

        let mut session = bamboo_agent_core::Session::new("live-sidecar", "live-sidecar-model-763");
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();
        let first = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "live-sidecar-model-763",
            &provider,
            &path,
        )
        .await;
        assert_eq!(first.max_context_tokens, 64_000);
        assert_eq!(first.max_output_tokens, 8_000);

        write_limit(2, 96_000, 12_000).await;
        let refreshed = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "live-sidecar-model-763",
            &provider,
            &path,
        )
        .await;

        assert_eq!(refreshed.max_context_tokens, 96_000);
        assert_eq!(refreshed.max_output_tokens, 12_000);
        assert_eq!(
            session
                .resolved_token_budget
                .as_ref()
                .expect("latest runtime snapshot")
                .1
                .max_context_tokens,
            96_000
        );
    }

    #[tokio::test]
    async fn runtime_reads_model_limits_from_the_instance_app_data_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 1,
                "data": [{
                    "model_pattern": "instance-data-dir-model-763",
                    "max_context_tokens": 72_000,
                    "max_output_tokens": 9_000,
                    "safety_margin": 600
                }]
            }))
            .expect("serialize instance model limits"),
        )
        .await
        .expect("write instance model limits");

        let config = AgentLoopConfig {
            app_data_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let provider = MetadataProvider::default();
        let mut session =
            bamboo_agent_core::Session::new("instance-data-dir", "instance-data-dir-model-763");

        let budget = resolve_token_budget(
            &mut session,
            &config,
            "instance-data-dir-model-763",
            &provider,
        )
        .await;

        assert_eq!(budget.max_context_tokens, 72_000);
        assert_eq!(budget.max_output_tokens, 9_000);
        assert_eq!(budget.safety_margin, 600);
    }

    #[tokio::test]
    async fn dedicated_partial_pattern_outranks_exact_provider_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 7,
                "data": [{
                    "model_pattern": "configured-family-763",
                    "max_context_tokens": 48_000,
                    "max_output_tokens": 6_000,
                    "safety_margin": 500
                }]
            }))
            .expect("serialize model-limit envelope"),
        )
        .await
        .expect("write model-limit envelope");

        let mut session =
            bamboo_agent_core::Session::new("configured-over-provider", "configured-family-763-v2");
        let config = AgentLoopConfig {
            provider_type: Some("copilot".to_string()),
            ..Default::default()
        };
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "configured-family-763-v2".to_string(),
                max_context_tokens: Some(128_000),
                max_output_tokens: Some(16_000),
            }],
        };

        let budget = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "configured-family-763-v2",
            &provider,
            &path,
        )
        .await;

        assert_eq!(budget.max_context_tokens, 48_000);
        assert_eq!(budget.max_output_tokens, 6_000);
        assert_eq!(budget.safety_margin, 500);
    }

    #[tokio::test]
    async fn legacy_limit_is_used_when_dedicated_sidecar_has_no_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing_path = dir.path().join("model_limits.json");
        let model = "legacy-chat-model-763";
        let config = AgentLoopConfig {
            legacy_model_limits: Some(serde_json::json!([{
                "model_pattern": model,
                "max_context_tokens": 24_000,
                "max_output_tokens": 4_000,
                "safety_margin": 400
            }])),
            ..Default::default()
        };
        let provider = MetadataProvider::default();
        let mut session = bamboo_agent_core::Session::new("legacy-chat-limit", model);

        let budget = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            model,
            &provider,
            &missing_path,
        )
        .await;

        assert_eq!(budget.max_context_tokens, 24_000);
        assert_eq!(budget.max_output_tokens, 4_000);
        assert_eq!(budget.safety_margin, 400);
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

    #[tokio::test]
    async fn auxiliary_budget_uses_exact_selected_provider_model_metadata() {
        let config = AgentLoopConfig {
            provider_type: Some("openai".to_string()),
            ..Default::default()
        };
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "summary-model-763".to_string(),
                max_context_tokens: Some(32_000),
                max_output_tokens: Some(6_000),
            }],
        };

        let budget = resolve_auxiliary_token_budget(&config, "summary-model-763", &provider).await;
        assert_eq!(budget.max_context_tokens, 32_000);
        assert_eq!(budget.max_output_tokens, 6_000);
        assert_eq!(budget.safety_margin, 1_000);
    }

    #[tokio::test]
    async fn auxiliary_budget_honors_legacy_model_limit_when_dedicated_file_is_absent() {
        let model = "legacy-summary-model-763-no-user-file-collision";
        let config = AgentLoopConfig {
            legacy_model_limits: Some(serde_json::json!([
                {
                    "model_pattern": model,
                    "max_context_tokens": 12_345,
                    "max_output_tokens": 2_345,
                    "safety_margin": 345
                }
            ])),
            ..Default::default()
        };
        let provider = MetadataProvider::default();

        let budget = resolve_auxiliary_token_budget(&config, model, &provider).await;
        assert_eq!(budget.max_context_tokens, 12_345);
        assert_eq!(budget.max_output_tokens, 2_345);
        assert_eq!(budget.safety_margin, 345);
    }

    #[tokio::test]
    async fn auxiliary_user_pattern_override_outranks_exact_provider_metadata() {
        let config = AgentLoopConfig {
            legacy_model_limits: Some(serde_json::json!([
                {
                    "model_pattern": "summary-family-763",
                    "max_context_tokens": 16_000,
                    "max_output_tokens": 3_000,
                    "safety_margin": 500
                }
            ])),
            ..Default::default()
        };
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "summary-family-763-latest".to_string(),
                max_context_tokens: Some(64_000),
                max_output_tokens: Some(8_000),
            }],
        };

        let budget =
            resolve_auxiliary_token_budget(&config, "summary-family-763-latest", &provider).await;
        assert_eq!(budget.max_context_tokens, 16_000);
        assert_eq!(budget.max_output_tokens, 3_000);
        assert_eq!(budget.safety_margin, 500);
    }

    #[tokio::test]
    async fn auxiliary_budget_uses_conservative_fallback_when_limit_is_unknown() {
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();

        let budget = resolve_auxiliary_token_budget(
            &config,
            "definitely-unknown-summary-model-763",
            &provider,
        )
        .await;
        assert_eq!(
            budget.max_context_tokens,
            CONSERVATIVE_SUMMARIZER_CONTEXT_TOKENS
        );
        assert_eq!(
            budget.max_output_tokens,
            CONSERVATIVE_SUMMARIZER_OUTPUT_TOKENS
        );
        assert_eq!(budget.safety_margin, CONSERVATIVE_SUMMARIZER_SAFETY_MARGIN);
    }
}
