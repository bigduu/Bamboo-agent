use crate::llm_summarizer::COMPATIBILITY_SUMMARY_INPUT_TOKENS;
use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;
use bamboo_compression::limits::{
    default_model_limit, load_model_limits_from_unified_config, ModelLimit,
    DEFAULT_MAX_OUTPUT_TOKENS,
};
use bamboo_compression::{ModelLimitsRegistry, TokenBudget};
use bamboo_domain::bounded_dedup::{BoundedFingerprintSet, DEFAULT_BOUNDED_FINGERPRINT_CAPACITY};
use bamboo_llm::provider::{LLMProvider, ProviderModelInfo};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

const MODEL_INFO_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, PartialEq, Eq)]
enum WindowMode {
    AgentRound,
    Child,
    Summary,
}

static STATIC_WARNINGS: LazyLock<BoundedFingerprintSet> =
    LazyLock::new(|| BoundedFingerprintSet::new(DEFAULT_BOUNDED_FINGERPRINT_CAPACITY));

fn model_limits_path(config: &AgentLoopConfig) -> PathBuf {
    let data_dir = config
        .app_data_dir
        .clone()
        .unwrap_or_else(bamboo_config::paths::bamboo_dir);
    bamboo_compression::limits::get_default_config_path(&data_dir)
}

/// Durable accounting reserves actual configured/reported ceilings. A prompt
/// estimator or the chat/summarizer's compatibility fallback is not evidence of
/// a provider's maximum input size.
pub(crate) async fn resolve_known_token_ceilings(
    config: &AgentLoopConfig,
    model: &str,
    provider: &dyn LLMProvider,
) -> Option<(u32, u32)> {
    let configured = resolve_configured_model_limit(
        config,
        model,
        &model_limits_path(config),
        "work_accounting",
    )
    .await;
    let reported = if configured
        .as_ref()
        .is_none_or(|limit| limit.max_output_tokens.is_none())
    {
        provider
            .model_info_for_request(model)
            .await
            .ok()?
            .filter(|info| info.id == model)
    } else {
        None
    };
    let input = configured
        .as_ref()
        .map(|limit| limit.max_context_tokens)
        .or_else(|| {
            let reported = reported.as_ref()?;
            reported.max_input_tokens.or(reported.max_context_tokens)
        })?;
    let output = configured
        .and_then(|limit| limit.max_output_tokens)
        .or_else(|| reported?.max_output_tokens)?;
    (input > 0 && output > 0).then_some((input, output))
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
    let reported = reported_model_limits(llm, model_name).await;
    let (budget, _) = resolve_window(
        configured_limit.as_ref(),
        reported.as_ref(),
        WindowMode::Summary,
    )
    .expect("summary budget has compatibility defaults");
    budget
}

pub(super) async fn resolve_token_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    resolve_round_token_budget(session, config, model_name, llm)
        .await
        .0
}

pub(super) async fn resolve_round_token_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> (TokenBudget, bool) {
    let model_limits_path = model_limits_path(config);
    resolve_token_budget_with_model_limits_path(
        session,
        config,
        model_name,
        llm,
        &model_limits_path,
        true,
    )
    .await
}

pub(super) async fn resolve_child_token_budget(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    resolve_token_budget_with_model_limits_path(
        session,
        config,
        model_name,
        llm,
        &model_limits_path(config),
        false,
    )
    .await
    .0
}

async fn resolve_token_budget_with_model_limits_path(
    session: &mut Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
    model_limits_path: &Path,
    dynamic: bool,
) -> (TokenBudget, bool) {
    // Priority: session/child override > config override > freshly-resolved
    // model defaults.
    if let Some(ref budget) = session.token_budget {
        tracing::debug!("Using session-specific token budget");
        let resolved = config
            .context_management
            .apply_compression_policy(budget.clone());
        session.resolved_token_budget = Some((model_name.to_string(), resolved.clone()));
        return (resolved, false);
    }

    if let Some(ref budget) = config.token_budget {
        tracing::debug!("Using config token budget");
        let resolved = config
            .context_management
            .apply_compression_policy(budget.clone());
        session.resolved_token_budget = Some((model_name.to_string(), resolved.clone()));
        return (resolved, false);
    }

    // Resolve the existing per-model settings first, then intersect them with
    // limits reported by the selected route. An omitted field can be filled by
    // the route without replacing the configured fields of the same row.
    let configured_limit =
        resolve_configured_model_limit(config, model_name, model_limits_path, "chat").await;
    let provider_limit = resolve_provider_runtime_limit(config, llm, model_name).await;
    let mode = if dynamic {
        WindowMode::AgentRound
    } else {
        WindowMode::Child
    };
    let (budget, provider_default_output) =
        resolve_window(configured_limit.as_ref(), provider_limit.as_ref(), mode).unwrap_or_else(
            || {
                let fallback = default_model_limit();
                tracing::info!(
                    model = model_name,
                    context = fallback.max_context_tokens,
                    output = fallback.get_max_output_tokens(),
                    "Using compatibility token limits because the route supplied no capacity"
                );
                (
                    TokenBudget::with_safety_margin(
                        fallback.max_context_tokens,
                        fallback.get_max_output_tokens(),
                        bamboo_compression::BudgetStrategy::default(),
                        fallback.get_safety_margin(),
                    ),
                    false,
                )
            },
        );
    let resolved = config.context_management.apply_compression_policy(budget);

    // Publish the freshly resolved budget on the session so every downstream
    // reader in this round — `build_context_pressure` (tool-output truncation),
    // the server context bar, `estimate_context_compression_exposure` — sees the
    // same limits via `Session::effective_token_budget` instead of `None`
    // (issue #20 bug 1). This runtime snapshot is never used to skip resolution:
    // every new round reads the host's latest validated section (or the
    // standalone model_limits.json) and refreshes provider metadata, so an edit
    // takes effect in an already-running session.
    session.resolved_token_budget = Some((model_name.to_string(), resolved.clone()));

    (resolved, provider_default_output)
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
    let loaded = if let Some(section) = &config.model_limits_section {
        let snapshot = section.snapshot();
        apply_model_limits_section(&mut dedicated_registry, &snapshot.data)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    } else {
        dedicated_registry.load_user_config().await
    };
    if let Err(error) = loaded {
        if error.kind() == std::io::ErrorKind::InvalidData {
            let error_fingerprint = error.to_string();
            let key = ("invalid-model-limits-file", model_limits_path);
            if STATIC_WARNINGS.insert_if_new(&key, &error_fingerprint) {
                tracing::warn!(
                    model = model_name,
                    purpose,
                    error = %error,
                    path = ?model_limits_path,
                    "Failed to resolve configured model limits; checking legacy configured limits"
                );
            } else {
                tracing::debug!(
                    model = model_name,
                    purpose,
                    error = %error,
                    path = ?model_limits_path,
                    "Failed to resolve configured model limits; checking legacy configured limits"
                );
            }
        } else {
            tracing::warn!(
                model = model_name,
                purpose,
                error = %error,
                path = ?model_limits_path,
                "Failed to resolve configured model limits; checking legacy configured limits"
            );
        }
    } else if let Some(limit) = dedicated_registry.get(model_name) {
        return Some(limit);
    }

    // The legacy value is snapshotted from the live in-memory config at
    // loop-config build time. Do not re-read Config::new() here: that would
    // diverge from the server's live config and clobber the global env cache
    // (#38).
    legacy_model_limit(config.legacy_model_limits.as_ref(), model_name)
}

pub(crate) fn configured_model_ceilings(
    section: Option<&bamboo_config::ModelLimitsSection>,
    legacy: Option<&serde_json::Value>,
    model: &str,
) -> Option<(u32, u32)> {
    let mut registry = ModelLimitsRegistry::new();
    if let Some(section) = section {
        apply_model_limits_section(&mut registry, section).ok()?;
    }
    let limit = registry
        .get(model)
        .or_else(|| legacy_model_limit(legacy, model))?;
    let output = limit.max_output_tokens?;
    (limit.max_context_tokens > 0 && output > 0).then_some((limit.max_context_tokens, output))
}

fn apply_model_limits_section(
    registry: &mut ModelLimitsRegistry,
    section: &bamboo_config::ModelLimitsSection,
) -> serde_json::Result<()> {
    for limit in
        serde_json::from_value::<Vec<ModelLimit>>(serde_json::Value::Array(section.0.clone()))?
    {
        registry.add_limit(limit);
    }
    Ok(())
}

fn legacy_model_limit(legacy: Option<&serde_json::Value>, model: &str) -> Option<ModelLimit> {
    let mut registry = ModelLimitsRegistry::new();
    apply_legacy_model_limits(&mut registry, legacy);
    registry.get(model)
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
) -> Option<ProviderModelInfo> {
    // Host-supplied providers without a configured route can still use local
    // limits. Ordinary configured UI, CLI and SDK runs carry a provider name.
    if config.provider_name.is_none() && config.provider_type.is_none() {
        return None;
    }
    reported_model_limits(llm, model_name).await
}

async fn reported_model_limits(
    llm: &dyn LLMProvider,
    model_name: &str,
) -> Option<ProviderModelInfo> {
    match tokio::time::timeout(MODEL_INFO_TIMEOUT, llm.model_info_for_request(model_name)).await {
        Ok(Ok(entry)) => entry.filter(|entry| {
            entry.id == model_name
                && (entry.max_context_tokens.is_some()
                    || entry.max_input_tokens.is_some()
                    || entry.max_output_tokens.is_some())
        }),
        _ => {
            tracing::debug!(model = model_name, "Provider model limits unavailable");
            None
        }
    }
}

fn resolve_window(
    configured: Option<&ModelLimit>,
    reported: Option<&ProviderModelInfo>,
    mode: WindowMode,
) -> Option<(TokenBudget, bool)> {
    fn tighter(left: Option<u32>, right: Option<u32>) -> Option<u32> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left.min(right)),
            _ => left.or(right),
        }
    }

    let joint_window = tighter(
        configured.map(|limit| limit.max_context_tokens),
        reported.and_then(|info| info.max_context_tokens),
    );
    let reported_input_limit = reported.and_then(|info| info.max_input_tokens);
    // With no known summary input limit, keep input chunks small and size the
    // request with a separate output allowance for reasoning and summary text.
    let input_limit = reported_input_limit.or_else(|| {
        (joint_window.is_none() && mode == WindowMode::Summary)
            .then_some(COMPATIBILITY_SUMMARY_INPUT_TOKENS)
    });
    let output_limit = tighter(
        configured.and_then(|limit| limit.max_output_tokens),
        reported.and_then(|info| info.max_output_tokens),
    );
    if joint_window.is_none() && input_limit.is_none() && output_limit.is_none() {
        return None;
    }

    let fallback = default_model_limit();
    let dynamic = mode == WindowMode::AgentRound;
    let provider_default_output =
        dynamic && output_limit.is_none() && configured.is_some_and(|limit| limit.unlimited_output);
    let output = output_limit.unwrap_or_else(|| {
        if mode == WindowMode::Summary {
            return DEFAULT_MAX_OUTPUT_TOKENS;
        }
        let sizing_window = joint_window
            .or(input_limit)
            .unwrap_or(fallback.max_context_tokens);
        let derived = ModelLimit::new("", sizing_window).get_max_output_tokens();
        if configured.is_none() {
            derived.min(fallback.get_max_output_tokens())
        } else {
            derived
        }
    });
    // Separate input/output limits give a planning bound. They do not
    // establish a provider's joint context window.
    let context = joint_window
        .or_else(|| input_limit.map(|input| input.saturating_add(output)))
        .unwrap_or(fallback.max_context_tokens);
    let compression_window = context.min(input_limit.unwrap_or(u32::MAX));
    let safety_margin = configured
        .and_then(|limit| limit.safety_margin)
        .unwrap_or_else(|| ModelLimit::new("", compression_window).get_safety_margin());
    let mut budget = TokenBudget::with_safety_margin(
        context,
        output,
        bamboo_compression::BudgetStrategy::default(),
        safety_margin,
    );
    budget.max_input_tokens = input_limit;
    if dynamic {
        budget.output_reserve_tokens = Some(output.min(safety_margin.max(1)));
    }
    tracing::debug!(
        configured = configured.is_some(),
        provider_metadata = reported.is_some(),
        ?joint_window,
        ?reported_input_limit,
        ?input_limit,
        ?output_limit,
        planned_output_limit = output,
        planning_window = context,
        "Resolved model token limits"
    );
    Some((budget, provider_default_output))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::{stream, Stream};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;

    use super::*;
    use bamboo_agent_core::{tools::ToolSchema, Message};
    use bamboo_llm::provider::{LLMError, ProviderModelInfo, Result};
    use bamboo_llm::types::LLMChunk;

    #[derive(Clone)]
    struct TokenBudgetLevelCollector(Arc<Mutex<Vec<Level>>>);

    impl<S> Layer<S> for TokenBudgetLevelCollector
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event
                .metadata()
                .target()
                .ends_with("round_lifecycle::token_budget")
            {
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(*event.metadata().level());
            }
        }
    }

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

    fn write_snapshot_limit(path: &Path, revision: u64, model: &str, context: u32) {
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "revision": revision,
                "data": [{
                    "model_pattern": model,
                    "max_context_tokens": context,
                    "max_output_tokens": 4_000,
                    "safety_margin": 400
                }]
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn hosted_model_limits_follow_live_lkg_across_rounds_and_repair() {
        let dir = tempfile::tempdir().unwrap();
        let facade = bamboo_config::ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let section = facade.registry().model_limits.clone();
        let path = dir.path().join("model_limits.json");
        let model = "hosted-live-model-1861";
        let first_revision = section.snapshot().revision + 1;
        write_snapshot_limit(&path, first_revision, model, 48_000);
        section.reload();
        assert_eq!(
            section.snapshot().status,
            bamboo_config::SectionStatus::Healthy
        );

        let config = AgentLoopConfig {
            app_data_dir: Some(dir.path().to_path_buf()),
            model_limits_section: Some(section.clone()),
            provider_type: Some("copilot".into()),
            ..Default::default()
        };
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: model.into(),
                max_context_tokens: Some(128_000),
                max_input_tokens: None,
                max_output_tokens: Some(16_000),
            }],
        };
        let mut session = Session::new("hosted-live", model);
        let first = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(first.max_context_tokens, 48_000);

        // A disk edit is not authority until the host's existing watcher has
        // validated and published it. Budget resolution must not read it itself.
        write_snapshot_limit(&path, first_revision + 1, model, 72_000);
        let unpublished = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(unpublished.max_context_tokens, 48_000);
        section.reload();
        let next_round = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(next_round.max_context_tokens, 72_000);

        for corrupt in [
            "{invalid".to_string(),
            serde_json::json!({
                "schema_version": 1,
                "revision": first_revision + 2,
                "data": [{}]
            })
            .to_string(),
        ] {
            std::fs::write(&path, corrupt).unwrap();
            section.reload();
            let invalid = section.snapshot();
            assert_ne!(invalid.status, bamboo_config::SectionStatus::Healthy);
            assert!(invalid.last_error.is_some());
            assert_eq!(invalid.revision, first_revision + 1);
            let after_corruption =
                resolve_token_budget(&mut session, &config, model, &provider).await;
            assert_eq!(after_corruption.max_context_tokens, 72_000);
        }
        let auxiliary = resolve_auxiliary_token_budget(&config, model, &provider).await;
        assert_eq!(auxiliary.max_context_tokens, 72_000);

        std::fs::remove_file(&path).unwrap();
        section.reload();
        assert_ne!(
            section.snapshot().status,
            bamboo_config::SectionStatus::Healthy
        );
        let after_deletion = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(after_deletion.max_context_tokens, 72_000);

        write_snapshot_limit(&path, first_revision + 2, model, 96_000);
        section.reload();
        assert_eq!(
            section.snapshot().status,
            bamboo_config::SectionStatus::Healthy
        );
        assert!(section.snapshot().last_error.is_none());
        let repaired = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(repaired.max_context_tokens, 96_000);
        assert_eq!(
            session
                .resolved_token_budget
                .as_ref()
                .unwrap()
                .1
                .max_context_tokens,
            96_000
        );
    }

    #[tokio::test]
    async fn hosted_model_limit_sections_are_isolated_and_keep_override_precedence() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let model = "isolated-family-1861-latest";
        let mut configs = Vec::new();
        for (dir, context) in [(&first_dir, 48_000), (&second_dir, 80_000)] {
            let facade = bamboo_config::ConfigFacade::open_or_migrate(dir.path()).unwrap();
            let section = facade.registry().model_limits.clone();
            write_snapshot_limit(
                &dir.path().join("model_limits.json"),
                section.snapshot().revision + 1,
                "isolated-family-1861",
                context,
            );
            section.reload();
            configs.push(AgentLoopConfig {
                model_limits_section: Some(section),
                // Even an exact legacy entry cannot shadow the dedicated
                // section's higher-priority family pattern.
                legacy_model_limits: Some(serde_json::json!([{
                    "model_pattern": model,
                    "max_context_tokens": 160_000
                }, {
                    "model_pattern": "legacy-only-1861",
                    "max_context_tokens": 24_000
                }])),
                ..Default::default()
            });
        }
        let provider = MetadataProvider::default();
        let mut session = Session::new("isolated-model-limits", model);
        for (config, context) in configs.iter().zip([48_000, 80_000]) {
            let budget = resolve_token_budget(&mut session, config, model, &provider).await;
            assert_eq!(budget.max_context_tokens, context);
            let legacy =
                resolve_token_budget(&mut session, config, "legacy-only-1861", &provider).await;
            assert_eq!(legacy.max_context_tokens, 24_000);
        }

        let explicit =
            TokenBudget::new(20_000, 2_000, bamboo_compression::BudgetStrategy::default());
        configs[0].token_budget = Some(explicit.clone());
        let configured = resolve_token_budget(&mut session, &configs[0], model, &provider).await;
        assert_eq!(configured.max_context_tokens, 20_000);
        session.token_budget = Some(TokenBudget::new(
            12_000,
            1_000,
            bamboo_compression::BudgetStrategy::default(),
        ));
        let session_override =
            resolve_token_budget(&mut session, &configs[0], model, &provider).await;
        assert_eq!(session_override.max_context_tokens, 12_000);
    }

    #[tokio::test]
    async fn model_limits_io_failure_warns_on_every_attempt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("model_limits.json");
        tokio::fs::create_dir(&path)
            .await
            .expect("directory forces a read error");
        let levels = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(TokenBudgetLevelCollector(levels.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);
        let config = AgentLoopConfig::default();

        for _ in 0..2 {
            assert!(resolve_configured_model_limit(
                &config,
                "dynamic-io-error-model",
                &path,
                "chat",
            )
            .await
            .is_none());
        }

        assert_eq!(
            *levels
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [Level::WARN, Level::WARN]
        );
    }

    #[tokio::test]
    async fn malformed_model_limits_warns_once_then_downgrades_to_debug() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("malformed-model-limits.json");
        tokio::fs::write(&path, b"{not valid json")
            .await
            .expect("write malformed fixture");
        let levels = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(TokenBudgetLevelCollector(levels.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);
        let config = AgentLoopConfig::default();

        for _ in 0..2 {
            assert!(resolve_configured_model_limit(
                &config,
                "stable-invalid-data-model",
                &path,
                "chat",
            )
            .await
            .is_none());
        }

        assert_eq!(
            *levels
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [Level::WARN, Level::DEBUG]
        );
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

    #[tokio::test]
    async fn compatibility_output_default_preserves_a_saved_32k_override() {
        let dir = tempfile::tempdir().unwrap();
        let model = "metadata-free-model";
        let mut session = Session::new("default-model-limit", model);
        let config = AgentLoopConfig {
            app_data_dir: Some(dir.path().to_path_buf()),
            provider_name: Some("compatible-route".into()),
            ..Default::default()
        };
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo::from_id(model)],
        };

        let fallback = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(fallback.max_context_tokens, 32_768);
        assert_eq!(fallback.max_output_tokens, 4_096);

        let path = model_limits_path(&config);
        let stored = r#"[{"model_pattern":"metadata-free-model","max_context_tokens":1000000,"max_output_tokens":32000}]"#;
        tokio::fs::write(&path, stored).await.unwrap();
        let saved = resolve_token_budget(&mut session, &config, model, &provider).await;
        assert_eq!(saved.max_output_tokens, 32_000);
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), stored);
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
                max_input_tokens: None,
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
        let (first, _) = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "live-sidecar-model-763",
            &provider,
            &path,
            true,
        )
        .await;
        assert_eq!(first.max_context_tokens, 64_000);
        assert_eq!(first.max_output_tokens, 8_000);

        write_limit(2, 96_000, 12_000).await;
        let (refreshed, _) = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "live-sidecar-model-763",
            &provider,
            &path,
            true,
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
                max_input_tokens: None,
                max_output_tokens: Some(16_000),
            }],
        };

        let (budget, _) = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            "configured-family-763-v2",
            &provider,
            &path,
            true,
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

        let (budget, _) = resolve_token_budget_with_model_limits_path(
            &mut session,
            &config,
            model,
            &provider,
            &missing_path,
            true,
        )
        .await;

        assert_eq!(budget.max_context_tokens, 24_000);
        assert_eq!(budget.max_output_tokens, 4_000);
        assert_eq!(budget.safety_margin, 400);
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_uses_selected_route_metadata() {
        let config = AgentLoopConfig {
            provider_name: Some("configured-route".to_string()),
            ..Default::default()
        };

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_input_tokens: Some(200_000),
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex")
            .await
            .expect("selected provider metadata should resolve");
        assert_eq!(limit.max_context_tokens, Some(222_000));
        assert_eq!(limit.max_input_tokens, Some(200_000));
        assert_eq!(limit.max_output_tokens, Some(33_000));
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_uses_no_catalog_for_unconfigured_provider() {
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_input_tokens: None,
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_preserves_an_output_only_record() {
        let config = AgentLoopConfig {
            provider_type: Some("openai".to_string()),
            ..Default::default()
        };

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: None,
                max_input_tokens: None,
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex")
            .await
            .unwrap();
        assert_eq!(limit.max_context_tokens, None);
        assert_eq!(limit.max_output_tokens, Some(33_000));
    }

    #[test]
    fn model_window_intersects_operator_and_provider_limits_independently() {
        let explicit = ModelLimit {
            model_pattern: "same-model".into(),
            max_context_tokens: 128_000,
            max_output_tokens: Some(65_536),
            unlimited_output: false,
            safety_margin: Some(1_000),
        };
        let provider = ProviderModelInfo {
            id: "same-model".into(),
            max_context_tokens: Some(96_000),
            max_input_tokens: Some(67_000),
            max_output_tokens: Some(48_000),
        };
        let (budget, provider_default) =
            resolve_window(Some(&explicit), Some(&provider), WindowMode::AgentRound).unwrap();
        assert!(!provider_default);
        assert_eq!(budget.max_context_tokens, 96_000);
        assert_eq!(budget.max_request_input_tokens(), 66_000);
        assert_eq!(budget.output_limit_for_input(10_000), 48_000);
        assert_eq!(budget.output_limit_for_input(61_000), 34_000);
        assert_eq!(budget.output_limit_for_input(67_000), 0);
    }

    #[test]
    fn unlimited_output_respects_route_ceilings_and_allows_provider_defaults() {
        let mut configured = ModelLimit::new("fixture-model", 256_000);
        configured.safety_margin = Some(1_000);
        let (automatic, provider_default) =
            resolve_window(Some(&configured), None, WindowMode::AgentRound).unwrap();
        assert!(!provider_default);
        assert_eq!(automatic.output_limit_for_input(32_000), 64_000);

        configured.unlimited_output = true;
        let (unlimited, provider_default) =
            resolve_window(Some(&configured), None, WindowMode::AgentRound).unwrap();
        assert!(provider_default);
        assert_eq!(unlimited.max_request_input_tokens(), 254_000);
        assert_eq!(unlimited.output_limit_for_input(32_000), 64_000);
        assert_eq!(unlimited.output_limit_for_input(255_000), 0);

        let mut reported = ProviderModelInfo {
            id: "fixture-model".into(),
            max_context_tokens: Some(160_000),
            max_input_tokens: Some(110_000),
            max_output_tokens: Some(80_000),
        };
        let (with_metadata, provider_default) =
            resolve_window(Some(&configured), Some(&reported), WindowMode::AgentRound).unwrap();
        assert!(!provider_default);
        assert_eq!(with_metadata.output_limit_for_input(32_000), 80_000);
        assert_eq!(with_metadata.output_limit_for_input(108_000), 51_000);
        assert_eq!(with_metadata.output_limit_for_input(110_000), 0);

        reported.max_output_tokens = None;
        let (joint_only, provider_default) =
            resolve_window(Some(&configured), Some(&reported), WindowMode::AgentRound).unwrap();
        assert!(provider_default);
        assert_eq!(joint_only.output_limit_for_input(32_000), 40_000);

        let (fixed, provider_default) =
            resolve_window(Some(&configured), None, WindowMode::Child).unwrap();
        assert!(!provider_default);
        assert_eq!(fixed.max_output_tokens, 64_000);
        assert_eq!(fixed.output_reserve_tokens, None);
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
                max_input_tokens: None,
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
    async fn auxiliary_budget_intersects_user_pattern_and_provider_input_ceiling() {
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
                max_input_tokens: Some(8_500),
                max_output_tokens: Some(8_000),
            }],
        };

        let budget =
            resolve_auxiliary_token_budget(&config, "summary-family-763-latest", &provider).await;
        assert_eq!(budget.max_context_tokens, 16_000);
        assert_eq!(budget.max_output_tokens, 3_000);
        assert_eq!(budget.safety_margin, 500);
        assert_eq!(budget.input_ceiling(), 8_000);
    }

    #[tokio::test]
    async fn auxiliary_budget_defaults_input_and_output_independently() {
        let config = AgentLoopConfig::default();
        let provider = MetadataProvider::default();

        let budget = resolve_auxiliary_token_budget(
            &config,
            "definitely-unknown-summary-model-763",
            &provider,
        )
        .await;
        assert_eq!(
            budget.max_input_tokens,
            Some(COMPATIBILITY_SUMMARY_INPUT_TOKENS)
        );
        assert_eq!(
            budget.max_context_tokens,
            COMPATIBILITY_SUMMARY_INPUT_TOKENS + DEFAULT_MAX_OUTPUT_TOKENS
        );
        assert_eq!(budget.max_output_tokens, DEFAULT_MAX_OUTPUT_TOKENS);
        assert_eq!(budget.safety_margin, 1_000);
    }
}
