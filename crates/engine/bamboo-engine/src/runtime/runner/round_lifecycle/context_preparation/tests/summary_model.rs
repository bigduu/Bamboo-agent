use super::*;
use crate::model_areas::resolve_global_area_models;
use crate::runtime::config::AuxiliaryModelConfig;
use bamboo_domain::ProviderModelRef;
use bamboo_llm::provider_registry::ProviderMetadata;
use bamboo_llm::{Config, ProviderRegistry};
use std::collections::HashMap;

#[tokio::test]
async fn compression_uses_live_global_summary_fast_chat_roles_for_anthropic_instance() {
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
    ] {
        let data_dir = tempfile::tempdir().unwrap();
        let (summary_provider, summary_calls) = recording_llm();
        let (main_provider, main_calls) = recording_llm();
        let registry = Arc::new(ProviderRegistry::new_with_metadata(
            HashMap::from([("eva-mux".into(), summary_provider)]),
            HashMap::from([(
                "eva-mux".into(),
                ProviderMetadata {
                    id: "eva-mux".into(),
                    provider_type: "anthropic".into(),
                    display_name: "Eva-mux".into(),
                },
            )]),
            "eva-mux".into(),
        ));
        registry.set_runtime_models(HashMap::from([(
            "eva-mux".into(),
            vec![
                "glm-5.3-dashscope".into(),
                "fast-model".into(),
                "chat-model".into(),
            ],
        )]));
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
                let areas = resolve_global_area_models(
                    &live_config.lock().unwrap(),
                    "different-session-provider",
                    &registry,
                );
                AuxiliaryModelConfig {
                    summarization_model_name: areas
                        .summarization
                        .as_ref()
                        .map(|model| model.model_name.clone()),
                    summarization_model_provider: areas.summarization.map(|model| model.provider),
                    background_model_name: areas
                        .background
                        .as_ref()
                        .map(|model| model.model_name.clone()),
                    background_model_provider: areas.background.map(|model| model.provider),
                    ..Default::default()
                }
            })),
            ..Default::default()
        };

        for (task_summary, fast, expected_model) in [
            (
                Some("glm-5.3-dashscope"),
                Some("fast-model"),
                "glm-5.3-dashscope",
            ),
            (None, Some("fast-model"), "fast-model"),
            (None, None, "chat-model"),
        ] {
            {
                let mut global = global_config.lock().unwrap();
                global.features.provider_model_ref = true;
                global.defaults = Some(
                    serde_json::from_value(serde_json::json!({
                        "chat": {"provider": "eva-mux", "model": "chat-model"},
                        "fast": fast.map(|model| ProviderModelRef::new("eva-mux", model)),
                        "task_summary": task_summary.map(|model| ProviderModelRef::new("eva-mux", model)),
                    }))
                    .unwrap(),
                );
            }
            summary_calls.lock().unwrap().clear();
            let mut session = bounded_compression_session("live-global-summary-model");
            session.model_ref = Some(ProviderModelRef::new(
                "different-session-provider",
                "different-session-model",
            ));

            assert!(maybe_apply_host_context_compression(
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
            .unwrap());
            let calls = summary_calls.lock().unwrap();
            assert!(calls.len() >= 2, "compression must complete map and reduce");
            assert!(calls.iter().all(|model| model == expected_model));
            assert!(main_calls.lock().unwrap().is_empty());
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
