use super::*;
use crate::model_areas::resolve_global_area_models;
use crate::runtime::config::AuxiliaryModelConfig;
use bamboo_domain::ProviderModelRef;
use bamboo_llm::provider_registry::ProviderMetadata;
use bamboo_llm::{Config, ProviderRegistry};
use std::collections::HashMap;

fn summary_auxiliary_models(
    config: &Config,
    registry: &Arc<ProviderRegistry>,
) -> AuxiliaryModelConfig {
    let areas = resolve_global_area_models(config, "different-session-provider", registry);
    AuxiliaryModelConfig {
        summarization_model_name: areas
            .summarization
            .as_ref()
            .map(|model| model.model_name.clone()),
        summarization_model_provider: areas.summarization.map(|model| model.provider),
        summarization_model_error: areas.summarization_error,
        background_model_name: areas
            .background
            .as_ref()
            .map(|model| model.model_name.clone()),
        background_model_provider: areas.background.map(|model| model.provider),
        ..Default::default()
    }
}

#[tokio::test]
async fn compression_uses_live_global_summary_fast_chat_roles_for_anthropic_instance() {
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
    ] {
        let data_dir = tempfile::tempdir().unwrap();
        let (summary_provider, summary_calls) = recording_llm();
        let (fast_provider, fast_calls) = recording_llm();
        let (chat_provider, chat_calls) = recording_llm();
        let (main_provider, main_calls) = recording_llm();
        let providers = [
            ("eva-mux", "anthropic", summary_provider),
            ("fast-route", "openai", fast_provider),
            ("chat-route", "openai", chat_provider),
        ];
        let metadata = providers
            .iter()
            .map(|(id, kind, _)| {
                (
                    (*id).into(),
                    ProviderMetadata {
                        id: (*id).into(),
                        provider_type: (*kind).into(),
                        display_name: (*id).into(),
                    },
                )
            })
            .collect();
        let registry = Arc::new(ProviderRegistry::new_with_metadata(
            providers
                .into_iter()
                .map(|(id, _, provider)| (id.into(), provider))
                .collect(),
            metadata,
            "eva-mux".into(),
        ));
        registry.set_runtime_models(HashMap::from([
            ("eva-mux".into(), vec!["glm-5.3-dashscope".into()]),
            ("fast-route".into(), vec!["fast-model".into()]),
            ("chat-route".into(), vec!["chat-model".into()]),
        ]));
        let global_config = Arc::new(Mutex::new(Config::default()));
        let live_config = Arc::clone(&global_config);
        let config = AgentLoopConfig {
            app_data_dir: Some(data_dir.path().to_path_buf()),
            // The execution started before model preferences were saved.
            summarization_model_name: None,
            background_model_name: None,
            context_management: ContextManagementConfig {
                strategy,
                ..Default::default()
            },
            auxiliary_model_resolver: Some(Arc::new(move || {
                summary_auxiliary_models(&live_config.lock().unwrap(), &registry)
            })),
            ..Default::default()
        };
        let calls = [summary_calls, fast_calls, chat_calls];
        // Older settings clients can save role refs without enabling the rollout.
        for feature in [None, Some(false), Some(true)] {
            for (task_summary, fast, winner, expected_model) in [
                (
                    Some("glm-5.3-dashscope"),
                    Some("fast-model"),
                    0,
                    "glm-5.3-dashscope",
                ),
                (Some("glm-5.3-dashscope"), None, 0, "glm-5.3-dashscope"),
                (None, Some("fast-model"), 1, "fast-model"),
                (None, None, 2, "chat-model"),
            ] {
                let mut saved = serde_json::json!({"defaults": {
                    "chat": ProviderModelRef::new("chat-route", "chat-model"),
                    "fast": fast.map(|model| ProviderModelRef::new("fast-route", model)),
                    "task_summary": task_summary.map(|model| ProviderModelRef::new("eva-mux", model)),
                }});
                if let Some(enabled) = feature {
                    saved["features"] = serde_json::json!({"provider_model_ref": enabled});
                }
                *global_config.lock().unwrap() = serde_json::from_value(saved).unwrap();
                for list in &calls {
                    list.lock().unwrap().clear();
                }
                let mut session = bounded_compression_session("live-global-summary-model");
                session.model_ref = Some(ProviderModelRef::new(
                    "different-session-provider",
                    "different-session-model",
                ));
                assert!(
                    maybe_apply_host_context_compression(
                        &mut session,
                        &config,
                        "main-model-763",
                        "live-global-summary-model",
                        &[],
                        &main_provider,
                        None,
                        "pre-turn",
                    )
                    .await
                    .unwrap(),
                    "feature={feature:?}, model={expected_model}"
                );
                for (index, list) in calls.iter().enumerate() {
                    let list = list.lock().unwrap();
                    if index == winner {
                        assert!(list.len() >= 2, "compression must complete map and reduce");
                        assert!(list.iter().all(|model| model == expected_model));
                    } else {
                        assert!(list.is_empty(), "must not call a different role/provider");
                    }
                }
                assert!(main_calls.lock().unwrap().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn compression_refreshes_model_and_provider_together_between_passes() {
    let data_dir = tempfile::tempdir().unwrap();
    let (stale_provider, stale_calls) = recording_llm();
    let (first_provider, first_calls) = recording_llm();
    let (second_provider, second_calls) = recording_llm();
    let live = Arc::new(Mutex::new(AuxiliaryModelConfig {
        summarization_model_name: Some("first-summary-model".into()),
        summarization_model_provider: Some(first_provider),
        ..Default::default()
    }));
    let resolver_state = Arc::clone(&live);
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        summarization_model_name: Some("stale-startup-model".into()),
        summarization_model_provider: Some(Arc::clone(&stale_provider)),
        auxiliary_model_resolver: Some(Arc::new(move || resolver_state.lock().unwrap().clone())),
        ..Default::default()
    };

    for (index, expected_model) in ["first-summary-model", "second-summary-model"]
        .iter()
        .enumerate()
    {
        if index == 1 {
            *live.lock().unwrap() = AuxiliaryModelConfig {
                summarization_model_name: Some((*expected_model).into()),
                summarization_model_provider: Some(Arc::clone(&second_provider)),
                ..Default::default()
            };
        }
        let mut session = bounded_compression_session("summary-provider-switch");
        assert!(maybe_apply_host_context_compression(
            &mut session,
            &config,
            "main-model-763",
            "summary-provider-switch",
            &[],
            &stale_provider,
            None,
            "mid-turn",
        )
        .await
        .unwrap());
    }
    assert!(stale_calls.lock().unwrap().is_empty());
    for (calls, expected_model) in [
        (first_calls, "first-summary-model"),
        (second_calls, "second-summary-model"),
    ] {
        let calls = calls.lock().unwrap();
        assert!(calls.len() >= 2);
        assert!(calls.iter().all(|model| model == expected_model));
    }
}

// A background-only resolver must not suppress the standalone/SDK fallback
// from missing summary to background, and must keep the resolved pair intact.
#[tokio::test]
async fn compression_uses_background_override_with_no_full_auxiliary_resolver() {
    let data_dir = tempfile::tempdir().unwrap();
    let (main, main_calls) = recording_llm();
    let (background, background_calls) = recording_llm();
    let resolved_background = Arc::clone(&background);
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        background_model_name: Some("stale-background".into()),
        background_model_provider: Some(Arc::clone(&main)),
        summarization_model_name: None,
        auxiliary_model_resolver: None,
        background_model_resolver: Some(Arc::new(move |_| {
            Some(bamboo_llm::ResolvedModel::new(
                Arc::clone(&resolved_background),
                "live-background",
            ))
        })),
        ..Default::default()
    };
    let mut session = bounded_compression_session("background-only-override");
    assert!(maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "background-only-override",
        &[],
        &main,
        None,
        "mid-turn",
    )
    .await
    .unwrap());
    assert!(main_calls.lock().unwrap().is_empty());
    let calls = background_calls.lock().unwrap();
    assert!(calls.len() >= 2, "compression must complete map and reduce");
    assert!(calls.iter().all(|model| model == "live-background"));
}

#[tokio::test]
async fn compression_does_not_reuse_stale_or_memory_route_when_live_summary_is_unavailable() {
    let data_dir = tempfile::tempdir().unwrap();
    let (provider, calls) = recording_llm();
    let memory_provider = Arc::clone(&provider);
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        summarization_model_name: Some("stale-summary-model".into()),
        summarization_model_provider: Some(Arc::clone(&provider)),
        auxiliary_model_resolver: Some(Arc::new(move || AuxiliaryModelConfig {
            // The global summary resolver already applies fast/chat fallback.
            // A missing result can mean its explicit provider/model was rejected.
            background_model_name: Some("different-memory-model".into()),
            background_model_provider: Some(Arc::clone(&memory_provider)),
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut session = bounded_compression_session("summary-route-unavailable");
    let before = archive_state(&session);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    assert!(!maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "summary-route-unavailable",
        &[],
        &provider,
        Some(&event_tx),
        "pre-turn",
    )
    .await
    .unwrap());
    assert_eq!(archive_state(&session), before);
    assert!(session.force_manual_compression.is_some());
    assert!(calls.lock().unwrap().is_empty());
    assert!(
        std::iter::from_fn(|| event_rx.try_recv().ok()).any(|event| matches!(
            event,
            AgentEvent::ContextCompressionStatus { status, .. }
                if status == "skipped_no_background_model"
        ))
    );
}

#[tokio::test]
async fn compression_reports_invalid_explicit_role_without_falling_through_to_other_accounts() {
    for (summary, fast) in [
        (
            Some(ProviderModelRef::new("missing-provider", "summary")),
            Some(ProviderModelRef::new("valid", "fast")),
        ),
        (
            Some(ProviderModelRef::new("valid", "not-admitted")),
            Some(ProviderModelRef::new("valid", "fast")),
        ),
        (
            None,
            Some(ProviderModelRef::new("missing-provider", "fast")),
        ),
    ] {
        let data_dir = tempfile::tempdir().unwrap();
        let (provider, calls) = recording_llm();
        let registry = Arc::new(ProviderRegistry::new(
            HashMap::from([("valid".into(), Arc::clone(&provider))]),
            "valid".into(),
        ));
        registry.set_runtime_models(HashMap::from([(
            "valid".into(),
            vec!["fast".into(), "chat".into()],
        )]));
        let global: Config = serde_json::from_value(serde_json::json!({"defaults": {
            "chat": ProviderModelRef::new("valid", "chat"), "fast": fast, "task_summary": summary,
        }}))
        .unwrap();
        let config = AgentLoopConfig {
            app_data_dir: Some(data_dir.path().to_path_buf()),
            auxiliary_model_resolver: Some(Arc::new(move || {
                summary_auxiliary_models(&global, &registry)
            })),
            ..Default::default()
        };
        let mut session = bounded_compression_session("invalid-summary-route");
        let before = archive_state(&session);
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let error = maybe_apply_host_context_compression(
            &mut session,
            &config,
            "main-model-763",
            "invalid-summary-route",
            &[],
            &provider,
            Some(&event_tx),
            "pre-turn",
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("Task-summary model resolution failed"));
        assert_eq!(archive_state(&session), before);
        assert!(session.force_manual_compression.is_some());
        assert!(calls.lock().unwrap().is_empty());
        assert!(
            std::iter::from_fn(|| event_rx.try_recv().ok()).any(|event| matches!(
                event, AgentEvent::ContextCompressionStatus { status, .. }
                    if status.starts_with("failed_model_resolution:")
            ))
        );
    }
}

struct SummaryAuthFailureProvider {
    models: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl LLMProvider for SummaryAuthFailureProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.models.lock().unwrap().push(model.into());
        Err(LLMError::Auth(
            "synthetic summary authentication failure".into(),
        ))
    }
}

#[tokio::test]
async fn compression_reports_summary_auth_failure_without_retrying_fast_chat_or_main_provider() {
    let data_dir = tempfile::tempdir().unwrap();
    let auth_calls = Arc::new(Mutex::new(Vec::new()));
    let auth_provider: Arc<dyn LLMProvider> = Arc::new(SummaryAuthFailureProvider {
        models: Arc::clone(&auth_calls),
    });
    let (fallback_provider, fallback_calls) = recording_llm();
    let registry = Arc::new(ProviderRegistry::new(
        HashMap::from([
            ("summary".into(), auth_provider),
            ("fallback".into(), Arc::clone(&fallback_provider)),
        ]),
        "fallback".into(),
    ));
    registry.set_runtime_models(HashMap::from([
        ("summary".into(), vec!["summary-model".into()]),
        ("fallback".into(), vec!["fast".into(), "chat".into()]),
    ]));
    let global: Config = serde_json::from_value(serde_json::json!({"defaults": {
        "chat": ProviderModelRef::new("fallback", "chat"),
        "fast": ProviderModelRef::new("fallback", "fast"),
        "task_summary": ProviderModelRef::new("summary", "summary-model"),
    }}))
    .unwrap();
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        auxiliary_model_resolver: Some(Arc::new(move || {
            summary_auxiliary_models(&global, &registry)
        })),
        ..Default::default()
    };
    let mut session = bounded_compression_session("summary-auth-failure");
    let before = archive_state(&session);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let error = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "main-model-763",
        "summary-auth-failure",
        &[],
        &fallback_provider,
        Some(&event_tx),
        "pre-turn",
    )
    .await
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("synthetic summary authentication failure"));
    assert_eq!(archive_state(&session), before);
    assert!(session.force_manual_compression.is_some());
    assert!(fallback_calls.lock().unwrap().is_empty());
    assert_eq!(auth_calls.lock().unwrap().as_slice(), ["summary-model"]);
    assert!(
        std::iter::from_fn(|| event_rx.try_recv().ok()).any(|event| matches!(
            event, AgentEvent::ContextCompressionStatus { status, .. }
                if status.starts_with("failed:")
        ))
    );
}
