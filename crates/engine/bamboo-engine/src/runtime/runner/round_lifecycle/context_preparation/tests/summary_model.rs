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

// Tests-first regression for the actual resume snapshot and compression consumer.
// The AgentLoopConfig fields below deliberately reproduce execute_bound's current
// mapping; this is not a full Connect approval/session or Agent execution test.
async fn rejected_resume_summary_must_not_dispatch(explicit: bool, lock_busy: bool) {
    use crate::model_config_helper::try_resolve_task_summary_model;
    use crate::session_app::resolution::resolve_resume_config_snapshot;
    use tokio::sync::RwLock;

    let data_dir = tempfile::tempdir().unwrap();
    let (account_a, calls_a) = recording_llm();
    let (account_b, calls_b) = recording_llm();
    let registry = Arc::new(ProviderRegistry::new(
        HashMap::from([
            ("account-a".into(), Arc::clone(&account_a)),
            ("account-b".into(), Arc::clone(&account_b)),
        ]),
        "account-a".into(),
    ));
    let admitted_a = vec!["chat-a".into(), "fast-a".into(), "memory-a".into()];
    let admitted_b = vec!["chat-b".into(), "memory-b".into(), "memory-only".into()];
    registry.set_runtime_models(HashMap::from([
        ("account-a".into(), admitted_a),
        ("account-b".into(), admitted_b),
    ]));
    let mut global = Config::default();
    global.provider = "account-a".into();
    global.default_provider_instance = Some("account-a".into());
    global.features.provider_model_ref = true;
    for (id, instance) in [
        (
            "account-a",
            serde_json::json!({
                "provider_type": "openai", "enabled": true,
                "model": "chat-a", "fast_model": "fast-a",
                "runtime_models": ["chat-a", "fast-a", "memory-a"],
            }),
        ),
        (
            "account-b",
            serde_json::json!({
                "provider_type": "openai", "enabled": true,
                "model": "chat-b", "fast_model": "blocked-fast-b",
                "runtime_models": ["chat-b", "memory-b", "memory-only"],
            }),
        ),
    ] {
        global
            .provider_instances
            .insert(id.into(), serde_json::from_value(instance).unwrap());
    }
    if explicit {
        global.defaults = Some(
            serde_json::from_value(serde_json::json!({
                "chat": ProviderModelRef::new("account-a", "chat-a"),
                "task_summary": ProviderModelRef::new("account-a", "blocked-summary"),
                "memory_background": ProviderModelRef::new("account-b", "memory-b"),
            }))
            .unwrap(),
        );
    } else {
        global.defaults = None;
        global
            .memory_mut()
            .get_or_insert_with(Default::default)
            .background_model = Some("memory-only".into());
    }

    let mut session = bounded_compression_session("rejected-resume-summary");
    session.model_ref = Some(ProviderModelRef::new("account-b", "chat-b"));
    session.set_provider_name("account-b");
    assert!(try_resolve_task_summary_model(&global, "account-b", &registry).is_err());
    let resume = resolve_resume_config_snapshot(&global, &registry, &session, None);
    assert_eq!(resume.provider_name, "account-b");
    assert!(resume.summarization_model.is_none());
    assert!(resume.summarization_model_provider.is_none());
    assert_eq!(
        resume.background_model.as_deref(),
        Some(if explicit { "memory-b" } else { "memory-only" }),
    );
    assert!(resume
        .background_model_provider
        .as_ref()
        .is_some_and(|provider| Arc::ptr_eq(provider, &account_b)));
    let fallback_name = if explicit {
        "blocked-summary"
    } else {
        "fast-a"
    };
    assert_eq!(
        global.get_task_summary_model().as_deref(),
        Some(fallback_name)
    );

    let live = Arc::new(RwLock::new(global.clone()));
    let resume_resolver = crate::model_areas::live_resume_auxiliary_resolver(
        Arc::clone(&live),
        Arc::clone(&registry),
        resume.provider_name.clone(),
    );
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        provider_name: Some(resume.provider_name.clone()),
        fast_model_name: resume
            .fast_model
            .clone()
            .or_else(|| global.get_fast_model()),
        background_model_name: resume
            .background_model
            .clone()
            .or_else(|| global.get_memory_background_model()),
        background_model_provider: resume.background_model_provider.clone(),
        summarization_model_name: resume
            .summarization_model
            .clone()
            .or_else(|| global.get_task_summary_model()),
        summarization_model_provider: resume.summarization_model_provider.clone(),
        auxiliary_model_resolver: None,
        resume_auxiliary_resolver: Some(resume_resolver),
        ..Default::default()
    };
    let before = archive_state(&session);
    let guard = if lock_busy {
        Some(live.write().await)
    } else {
        None
    };
    let result = maybe_apply_host_context_compression(
        &mut session,
        &config,
        "chat-b",
        "rejected-resume-summary",
        &[],
        &account_a,
        None,
        "mid-turn",
    )
    .await;
    drop(guard);
    let observed_a = calls_a.lock().unwrap().clone();
    let observed_b = calls_b.lock().unwrap().clone();
    assert!(
        observed_a.is_empty() && observed_b.is_empty(),
        "rejected resume summary must not dispatch: explicit={explicit} lock_busy={lock_busy} result={result:?} A={observed_a:?} B={observed_b:?}"
    );
    assert!(
        result.is_err(),
        "rejected explicit/fast route must be reported"
    );
    assert_eq!(archive_state(&session), before);
    assert!(session.force_manual_compression.is_some());
}

#[tokio::test]
async fn rejected_explicit_resume_summary_does_not_dispatch_when_unlocked() {
    rejected_resume_summary_must_not_dispatch(true, false).await;
}

#[tokio::test]
async fn rejected_explicit_resume_summary_does_not_dispatch_when_locked() {
    rejected_resume_summary_must_not_dispatch(true, true).await;
}

#[tokio::test]
async fn rejected_legacy_resume_fast_does_not_dispatch_when_unlocked() {
    rejected_resume_summary_must_not_dispatch(false, false).await;
}

#[tokio::test]
async fn rejected_legacy_resume_fast_does_not_dispatch_when_locked() {
    rejected_resume_summary_must_not_dispatch(false, true).await;
}

// Ok(None) from the real resume resolver keeps the original background
// fallback; it must not resurrect the default provider's startup model.
#[tokio::test]
async fn unconfigured_resume_summary_falls_back_to_admitted_background() {
    use crate::session_app::resolution::resolve_resume_config_snapshot;
    use tokio::sync::RwLock;

    let data_dir = tempfile::tempdir().unwrap();
    let (account_a, calls_a) = recording_llm();
    let (account_b, calls_b) = recording_llm();
    let registry = Arc::new(ProviderRegistry::new(
        HashMap::from([
            ("account-a".into(), Arc::clone(&account_a)),
            ("account-b".into(), Arc::clone(&account_b)),
        ]),
        "account-a".into(),
    ));
    registry.set_runtime_models(HashMap::from([
        ("account-a".into(), vec!["chat-a".into(), "fast-a".into()]),
        ("account-b".into(), vec!["memory-only".into()]),
    ]));
    let mut global = Config::default();
    global.provider = "account-a".into();
    global.default_provider_instance = Some("account-a".into());
    global.defaults = None;
    global.provider_instances.insert(
        "account-a".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type": "openai", "model": "chat-a", "fast_model": "fast-a",
            "runtime_models": ["chat-a", "fast-a"]
        }))
        .unwrap(),
    );
    global.provider_instances.insert(
        "account-b".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type": "openai", "runtime_models": ["memory-only"]
        }))
        .unwrap(),
    );
    global
        .memory_mut()
        .get_or_insert_with(Default::default)
        .background_model = Some("memory-only".into());
    let mut session = bounded_compression_session("unset-resume-summary");
    session.model_ref = Some(ProviderModelRef::new("account-b", "chat-b"));
    session.set_provider_name("account-b");
    let resume = resolve_resume_config_snapshot(&global, &registry, &session, None);
    assert_eq!(resume.provider_name, "account-b");
    assert!(resume.summarization_model.is_none());
    assert_eq!(resume.background_model.as_deref(), Some("memory-only"));
    let live = Arc::new(RwLock::new(global.clone()));
    let resolver = crate::model_areas::live_resume_auxiliary_resolver(
        live,
        Arc::clone(&registry),
        resume.provider_name.clone(),
    );
    let (summary, background) = resolver();
    assert!(summary.unwrap().is_none());
    assert_eq!(background.unwrap().model_name, "memory-only");
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        provider_name: Some(resume.provider_name),
        summarization_model_name: global.get_task_summary_model(),
        background_model_name: resume.background_model,
        background_model_provider: resume.background_model_provider,
        auxiliary_model_resolver: None,
        resume_auxiliary_resolver: Some(resolver),
        ..Default::default()
    };
    assert!(maybe_apply_host_context_compression(
        &mut session,
        &config,
        "chat-b",
        "unset-resume-summary",
        &[],
        &account_a,
        None,
        "mid-turn",
    )
    .await
    .unwrap());
    assert!(calls_a.lock().unwrap().is_empty());
    let models = calls_b.lock().unwrap();
    assert!(models.len() >= 2);
    assert!(models.iter().all(|model| model == "memory-only"));
}

// A valid explicit summary and a background target may belong to different
// providers; compression must retain the summary's own name/provider pair.
#[tokio::test]
async fn configured_resume_summary_keeps_its_own_provider_pair() {
    use crate::session_app::resolution::resolve_resume_config_snapshot;
    use tokio::sync::RwLock;

    let data_dir = tempfile::tempdir().unwrap();
    let (account_a, calls_a) = recording_llm();
    let (account_b, calls_b) = recording_llm();
    let registry = Arc::new(ProviderRegistry::new(
        HashMap::from([
            ("account-a".into(), Arc::clone(&account_a)),
            ("account-b".into(), Arc::clone(&account_b)),
        ]),
        "account-a".into(),
    ));
    registry.set_runtime_models(HashMap::from([
        (
            "account-a".into(),
            vec!["chat-a".into(), "summary-a".into()],
        ),
        ("account-b".into(), vec!["chat-b".into(), "memory-b".into()]),
    ]));
    let mut global = Config::default();
    global.provider = "account-a".into();
    global.default_provider_instance = Some("account-a".into());
    global.features.provider_model_ref = true;
    for (id, instance) in [
        (
            "account-a",
            serde_json::json!({
                "provider_type": "openai", "model": "chat-a",
                "runtime_models": ["chat-a", "summary-a"]
            }),
        ),
        (
            "account-b",
            serde_json::json!({
                "provider_type": "openai", "model": "chat-b",
                "runtime_models": ["chat-b", "memory-b"]
            }),
        ),
    ] {
        global
            .provider_instances
            .insert(id.into(), serde_json::from_value(instance).unwrap());
    }
    global.defaults = Some(
        serde_json::from_value(serde_json::json!({
            "chat": ProviderModelRef::new("account-a", "chat-a"),
            "task_summary": ProviderModelRef::new("account-a", "summary-a"),
            "memory_background": ProviderModelRef::new("account-b", "memory-b"),
        }))
        .unwrap(),
    );
    let mut session = bounded_compression_session("paired-resume-summary");
    session.model_ref = Some(ProviderModelRef::new("account-b", "chat-b"));
    session.set_provider_name("account-b");
    let resume = resolve_resume_config_snapshot(&global, &registry, &session, None);
    let resolver = crate::model_areas::live_resume_auxiliary_resolver(
        Arc::new(RwLock::new(global)),
        Arc::clone(&registry),
        resume.provider_name.clone(),
    );
    let (summary, background) = resolver();
    let summary = summary.unwrap().unwrap();
    let background = background.unwrap();
    assert_eq!(summary.model_name, "summary-a");
    assert!(Arc::ptr_eq(&summary.provider, &account_a));
    assert_eq!(background.model_name, "memory-b");
    assert!(Arc::ptr_eq(&background.provider, &account_b));
    let config = AgentLoopConfig {
        app_data_dir: Some(data_dir.path().to_path_buf()),
        provider_name: Some(resume.provider_name),
        auxiliary_model_resolver: None,
        resume_auxiliary_resolver: Some(resolver),
        ..Default::default()
    };
    assert!(maybe_apply_host_context_compression(
        &mut session,
        &config,
        "chat-b",
        "paired-resume-summary",
        &[],
        &account_b,
        None,
        "mid-turn"
    )
    .await
    .unwrap());
    assert!(calls_b.lock().unwrap().is_empty());
    let models = calls_a.lock().unwrap();
    assert!(models.len() >= 2);
    assert!(models.iter().all(|model| model == "summary-a"));
}
