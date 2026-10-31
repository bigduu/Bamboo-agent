use bamboo_agent_core::Session;
use bamboo_config::MemoryConfig;

use super::{
    normalize_auxiliary_evaluation_max_concurrency, AgentLoopConfig, PromptMemoryFlags,
    DEFAULT_AUXILIARY_EVALUATION_MAX_CONCURRENCY, MAX_AUXILIARY_EVALUATION_MAX_CONCURRENCY,
};

#[test]
fn auxiliary_evaluation_concurrency_defaults_and_clamps_oversized_config() {
    assert_eq!(
        normalize_auxiliary_evaluation_max_concurrency(None),
        DEFAULT_AUXILIARY_EVALUATION_MAX_CONCURRENCY
    );
    assert_eq!(
        normalize_auxiliary_evaluation_max_concurrency(Some(0)),
        DEFAULT_AUXILIARY_EVALUATION_MAX_CONCURRENCY
    );
    assert_eq!(normalize_auxiliary_evaluation_max_concurrency(Some(7)), 7);
    assert_eq!(
        normalize_auxiliary_evaluation_max_concurrency(Some(u64::MAX)),
        MAX_AUXILIARY_EVALUATION_MAX_CONCURRENCY
    );
}

#[test]
fn agent_loop_config_model_name_defaults_to_none() {
    let config = AgentLoopConfig::default();
    assert!(
        config.model_name.is_none(),
        "model_name should default to None, forcing explicit setting"
    );
    assert!(
        config.selected_skill_ids.is_none(),
        "selected_skill_ids should default to None"
    );
    assert!(
        config.selected_skill_mode.is_none(),
        "selected_skill_mode should default to None"
    );
    assert!(
        config.provider_type.is_none(),
        "provider_type should default to None"
    );
}

#[test]
fn agent_loop_config_can_set_model_name() {
    let config = AgentLoopConfig {
        model_name: Some("kimi-for-coding".to_string()),
        ..Default::default()
    };
    assert_eq!(config.model_name, Some("kimi-for-coding".to_string()));
}

#[test]
fn model_must_come_from_config_not_session() {
    let config = AgentLoopConfig {
        model_name: Some("config-model".to_string()),
        ..Default::default()
    };

    let session = Session::new("test", "session-model");
    let execution_model = config.model_name.as_deref().unwrap();

    assert_eq!(
        execution_model, "config-model",
        "Model must come from config.model_name, not session.model"
    );
    assert_eq!(
        session.model, "session-model",
        "session.model is just for recording, not execution"
    );
}

#[test]
fn prompt_memory_flags_map_from_memory_config() {
    let memory = MemoryConfig {
        project_prompt_injection: false,
        relevant_recall: false,
        relevant_recall_rerank: true,
        project_first_dream: false,
        ..MemoryConfig::default()
    };

    let flags = PromptMemoryFlags::from(&memory);
    assert!(!flags.project_prompt_injection);
    assert!(!flags.relevant_recall);
    assert!(flags.relevant_recall_rerank);
    assert!(!flags.project_first_dream);
}

#[test]
fn resolve_disabled_filters_uses_snapshot_without_resolver() {
    // #44 frozen behavior: with no resolver, the snapshotted sets are returned.
    let config = AgentLoopConfig {
        disabled_tools: std::collections::BTreeSet::from(["frozen_tool".to_string()]),
        disabled_skill_ids: std::collections::BTreeSet::from(["frozen_skill".to_string()]),
        ..Default::default()
    };

    let (tools, skills) = config.resolve_disabled_filters();
    assert!(tools.contains("frozen_tool"));
    assert!(skills.contains("frozen_skill"));
}

#[test]
fn resolve_disabled_filters_uses_live_resolver_and_reinvokes() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // #136: a wired resolver overrides the snapshot AND is re-invoked each call,
    // so the set can change between rounds.
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let config = AgentLoopConfig {
        disabled_tools: std::collections::BTreeSet::from(["ignored_snapshot".to_string()]),
        disabled_filter_resolver: Some(Arc::new(move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            (
                std::collections::BTreeSet::from([format!("live-{n}")]),
                std::collections::BTreeSet::new(),
            )
        })),
        ..Default::default()
    };

    let (t1, _) = config.resolve_disabled_filters();
    assert!(t1.contains("live-0"), "resolver result is used");
    assert!(
        !t1.contains("ignored_snapshot"),
        "the resolver overrides the frozen snapshot"
    );

    let (t2, _) = config.resolve_disabled_filters();
    assert!(
        t2.contains("live-1"),
        "the resolver is re-invoked each call (live between rounds)"
    );
}

// Each execution owns its provider key even when it shares one callback.
#[test]
fn background_override_uses_each_concurrent_run_provider_key() {
    use super::{AuxiliaryModelConfig, BackgroundModelResolver};
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{LLMError, LLMProvider, LLMStream, ResolvedModel};
    use std::sync::Arc;

    struct SyntheticProvider;
    #[async_trait::async_trait]
    impl LLMProvider for SyntheticProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("synthetic provider is not dispatched".into()))
        }
    }

    let account_a: Arc<dyn LLMProvider> = Arc::new(SyntheticProvider);
    let account_b: Arc<dyn LLMProvider> = Arc::new(SyntheticProvider);
    let a = Arc::clone(&account_a);
    let b = Arc::clone(&account_b);
    let resolver: BackgroundModelResolver = Arc::new(move |provider| match provider {
        "account-a" => Some(ResolvedModel::new(Arc::clone(&a), "memory-a")),
        "account-b" => Some(ResolvedModel::new(Arc::clone(&b), "memory-b")),
        _ => None,
    });
    let make_config = |provider: &str| AgentLoopConfig {
        provider_name: Some(provider.to_string()),
        auxiliary_model_resolver: Some(Arc::new(|| AuxiliaryModelConfig {
            fast_model_name: Some("frozen-fast".into()),
            background_model_name: Some("stale-background".into()),
            summarization_model_name: Some("frozen-summary".into()),
            ..Default::default()
        })),
        background_model_resolver: Some(Arc::clone(&resolver)),
        ..Default::default()
    };
    let run_a = make_config("account-a");
    let run_b = make_config("account-b");
    let missing = make_config("missing").resolve_auxiliary_models();
    assert!(missing.background_model_name.is_none());
    assert!(missing.background_model_provider.is_none());

    fn check(config: AgentLoopConfig, name: &str, expected: Arc<dyn LLMProvider>) {
        for _ in 0..64 {
            let selected = config.resolve_auxiliary_models();
            assert_eq!(selected.fast_model_name.as_deref(), Some("frozen-fast"));
            assert_eq!(
                selected.summarization_model_name.as_deref(),
                Some("frozen-summary")
            );
            assert_eq!(selected.background_model_name.as_deref(), Some(name));
            assert!(Arc::ptr_eq(
                selected.background_model_provider.as_ref().unwrap(),
                &expected
            ));
        }
    }
    std::thread::scope(|scope| {
        scope.spawn(move || check(run_a, "memory-a", account_a));
        scope.spawn(move || check(run_b, "memory-b", account_b));
    });
}
