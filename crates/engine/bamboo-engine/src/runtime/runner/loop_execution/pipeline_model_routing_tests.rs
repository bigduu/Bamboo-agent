use super::*;
use crate::app_context::model_routing_test_support::{Failure, RoutingContext};
use crate::runtime::config::GoldConfig;
use crate::runtime::runner::loop_execution::startup::{
    GoldEvaluationState, LoopRunState, OverflowRecoveryState, TaskEvaluationState,
};
use bamboo_domain::ProviderModelRef;

fn fixture(session: &mut Session) -> LoopRunState {
    let now = chrono::Utc::now();
    session.set_task_list(bamboo_domain::TaskList {
        session_id: session.id.clone(),
        title: "Synthetic routing plan".into(),
        items: vec![bamboo_domain::TaskItem {
            id: "synthetic-item".into(),
            description: "Verify synthetic routing".into(),
            status: bamboo_domain::TaskItemStatus::InProgress,
            ..Default::default()
        }],
        created_at: now,
        updated_at: now,
    });
    session.set_task_list_version_meta("1");
    let mut task_context = TaskLoopContext::from_session(session).unwrap();
    task_context.task_list_dirty = true;
    LoopRunState {
        session_id: session.id.clone(),
        execution_id: "synthetic-routing-execution".into(),
        current_inputs: None,
        model_name: "main-a".into(),
        metrics_collector: None,
        debug_logger: crate::runtime::runner::logging::DebugLogger::new(false),
        task_context: Some(task_context),
        overflow_recovery: OverflowRecoveryState::default(),
        task_evaluation: TaskEvaluationState::default(),
        gold_evaluation: GoldEvaluationState::default(),
        auxiliary_models: Default::default(),
        runtime_state: AgentRuntimeState::new(session.id.clone()),
    }
}

fn config(context: &RoutingContext) -> AgentLoopConfig {
    AgentLoopConfig {
        model_name: Some("main-a".into()),
        provider_name: Some("chat-a".into()),
        auxiliary_model_resolver: Some(crate::model_areas::live_auxiliary_model_resolver(
            context.config.clone(),
            context.registry.clone(),
            "chat-a".into(),
        )),
        gold_config: Some(GoldConfig {
            enabled: true,
            auto_continue_enabled: true,
            goal: Some("Verify synthetic routing".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn finish_evaluations(state: &mut LoopRunState) {
    if let Some(task) = state.task_evaluation.in_flight.take() {
        let result = tokio::time::timeout(Duration::from_secs(2), task.join_handle)
            .await
            .expect("synthetic task evaluation must finish")
            .expect("Task evaluator must not panic")
            .expect("Task evaluation was not cancelled");
        assert!(result.error.is_some(), "the synthetic Task provider failed");
    }
    if let Some(task) = state.gold_evaluation.in_flight.take() {
        let result = tokio::time::timeout(Duration::from_secs(2), task.join_handle)
            .await
            .expect("synthetic Gold evaluation must finish")
            .expect("Gold evaluator must not panic")
            .expect("Gold evaluation was not cancelled")
            .expect("an upstream failure retains Gold's fallback result");
        assert!(result
            .evaluation_result
            .reasoning
            .starts_with("Gold evaluation failed:"));
    }
}

fn selected_providers(
    state: &LoopRunState,
    config: &AgentLoopConfig,
    main: &Arc<dyn LLMProvider>,
) -> (Arc<dyn LLMProvider>, Arc<dyn LLMProvider>) {
    let fast = state
        .auxiliary_models
        .fast_model_provider
        .clone()
        .unwrap_or_else(|| main.clone());
    let gold = select_gold_evaluation_provider(
        config,
        main,
        state.auxiliary_models.fast_model_provider.as_ref(),
    );
    (fast, gold)
}

#[tokio::test]
async fn routing_contract_task_and_gold_reject_invalid_fast_without_dispatch() {
    for flag in [None, Some(false), Some(true)] {
        for rejection in ["unknown", "disabled", "unadmitted"] {
            let context = RoutingContext::new(flag);
            {
                let mut saved = context.config.write().await;
                let target = match rejection {
                    "unknown" => ProviderModelRef::new("missing-account", "cheap-b"),
                    "disabled" => {
                        saved.provider_instances.get_mut("fast-b").unwrap().enabled = false;
                        ProviderModelRef::new("fast-b", "cheap-b")
                    }
                    _ => ProviderModelRef::new("fast-b", "unadmitted-model"),
                };
                saved.defaults.as_mut().unwrap().fast = Some(target);
            }
            let config = config(&context);
            let mut session = RoutingContext::session();
            let mut state = fixture(&mut session);
            refresh_auxiliary_models_for_round(&mut state, &config);
            let main: Arc<dyn LLMProvider> = context.providers["chat-a"].clone();
            let (fast, gold) = selected_providers(&state, &config, &main);
            assert!(fast.known_pre_dispatch_error().is_some());
            assert!(gold.known_pre_dispatch_error().is_some());
            let (tx, mut rx) = mpsc::channel(32);
            spawn_task_evaluation_if_needed(
                0,
                &session,
                &tx,
                &config,
                &mut state,
                fast,
                CancellationToken::new(),
            )
            .expect("the rejected Task route is handled by the evaluator");
            spawn_gold_evaluation_if_needed(
                0,
                &session,
                &tx,
                &config,
                &mut state,
                gold,
                CancellationToken::new(),
            )
            .expect("the rejected Gold route is handled by the evaluator");

            let task = state
                .task_evaluation
                .in_flight
                .take()
                .expect("Task evaluation");
            let task = tokio::time::timeout(Duration::from_secs(2), task.join_handle)
                .await
                .expect("rejected Task evaluation must finish")
                .expect("Task evaluator must not panic")
                .expect("Task evaluation was not cancelled");
            assert!(task.error.is_some(), "Task must report {rejection}");
            assert!(task.evaluation_result.updates.is_empty());
            let gold = state
                .gold_evaluation
                .in_flight
                .take()
                .expect("Gold evaluation");
            let gold = tokio::time::timeout(Duration::from_secs(2), gold.join_handle)
                .await
                .expect("rejected Gold evaluation must finish")
                .expect("Gold evaluator must not panic");
            assert!(matches!(gold, Some(Err(_))), "Gold must reject {rejection}");
            context.assert_no_requests();
            while let Ok(event) = rx.try_recv() {
                assert!(!matches!(
                    event,
                    AgentEvent::TaskEvaluationCompleted { .. }
                        | AgentEvent::GoldEvaluationStarted { .. }
                        | AgentEvent::GoldEvaluationCompleted { .. }
                ));
            }
        }
    }
}

#[tokio::test]
async fn routing_contract_queued_unmanaged_task_and_gold_keep_their_provider_model_pair() {
    for failure in [Failure::Auth, Failure::Quota, Failure::Network] {
        // RoutingContext uses an unmanaged registry and opaque SDK providers.
        // Real managed routes must be checked against the current generation.
        let context = RoutingContext::new(Some(false));
        for provider in context.providers.values() {
            *provider.failure.lock().unwrap() = Some(failure);
        }
        let config = config(&context);
        let mut session = RoutingContext::session();
        let mut state = fixture(&mut session);
        let (tx, _rx) = mpsc::channel(64);
        let main: Arc<dyn LLMProvider> = context.providers["chat-a"].clone();
        refresh_auxiliary_models_for_round(&mut state, &config);
        let (fast, gold) = selected_providers(&state, &config, &main);
        assert!(!fast.queue_requires_current_route());
        spawn_task_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            fast.clone(),
            CancellationToken::new(),
        )
        .unwrap();
        spawn_gold_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            gold.clone(),
            CancellationToken::new(),
        )
        .unwrap();
        state.task_context.as_mut().unwrap().task_list_dirty = true;
        spawn_task_evaluation_if_needed(
            1,
            &session,
            &tx,
            &config,
            &mut state,
            fast,
            CancellationToken::new(),
        )
        .unwrap();
        spawn_gold_evaluation_if_needed(
            1,
            &session,
            &tx,
            &config,
            &mut state,
            gold,
            CancellationToken::new(),
        )
        .unwrap();
        assert!(state.task_evaluation.queued_request.is_some());
        assert!(state.gold_evaluation.queued_request.is_some());
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("memory-c", "background-c"));
        refresh_auxiliary_models_for_round(&mut state, &config);
        finish_evaluations(&mut state).await;
        start_queued_task_evaluation_if_idle(
            &mut state,
            &tx,
            main.clone(),
            CancellationToken::new(),
            1,
        );
        let (_, current_gold) = selected_providers(&state, &config, &main);
        start_queued_gold_evaluation_if_idle(
            &mut state,
            &tx,
            current_gold,
            &config,
            CancellationToken::new(),
            1,
        );
        finish_evaluations(&mut state).await;
        assert_eq!(context.models("fast-b"), vec!["cheap-b"; 4]);
        assert!(context.requests("memory-c").is_empty());
        assert!(context.requests("chat-a").is_empty());
    }
}

#[tokio::test]
async fn routing_contract_gold_dedicated_unmanaged_sdk_uses_executing_provider() {
    for selected in ["explicit-gold", "unadmitted-gold"] {
        let context = RoutingContext::new(Some(false));
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("missing-account", "invalid-fast"));
        let mut config = config(&context);
        config.gold_config.as_mut().unwrap().model_name = Some(selected.into());
        assert!(config.gold_target_resolver.is_none());
        let mut session = RoutingContext::session();
        let mut state = fixture(&mut session);
        refresh_auxiliary_models_for_round(&mut state, &config);
        let main: Arc<dyn LLMProvider> = context.providers["chat-a"].clone();
        let (_, gold) = selected_providers(&state, &config, &main);
        assert!(Arc::ptr_eq(&gold, &main));
        let (tx, _rx) = mpsc::channel(64);
        spawn_gold_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            gold,
            CancellationToken::new(),
        )
        .unwrap();
        let gold = state
            .gold_evaluation
            .in_flight
            .take()
            .expect("Gold evaluation");
        let result = tokio::time::timeout(Duration::from_secs(2), gold.join_handle)
            .await
            .expect("SDK Gold evaluation must finish")
            .expect("Gold evaluator must not panic")
            .expect("Gold evaluation was not cancelled")
            .expect("unmanaged SDK provider owns its admission");
        assert_eq!(result.model_name, selected);
        assert_eq!(context.models("chat-a"), vec![selected]);
        assert!(context.requests("fast-b").is_empty());
        assert!(context.requests("memory-c").is_empty());
    }
}

#[tokio::test]
async fn routing_contract_gold_dedicated_managed_guard_checks_admission() {
    for selected in ["explicit-gold", "unadmitted-gold"] {
        let context = RoutingContext::new(Some(false));
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("missing-account", "invalid-fast"));
        let mut config = config(&context);
        config.gold_config.as_mut().unwrap().model_name = Some(selected.into());

        // Bind a real managed origin and use the capture provider only as the
        // synthetic dispatch delegate; a name-only registry is not a Host guard.
        let mut managed_config = bamboo_llm::Config::default();
        managed_config.provider = "chat-a".into();
        managed_config.default_provider_instance = Some("chat-a".into());
        managed_config.http_proxy = "http://127.0.0.1:9".into();
        managed_config.https_proxy.clear();
        managed_config.proxy_auth = None;
        managed_config.provider_instances.insert(
            "chat-a".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": true,
                "api_key": "synthetic-only", "base_url": "http://127.0.0.1:1",
                "model": "main-a", "runtime_models": ["main-a", "explicit-gold"]
            }))
            .unwrap(),
        );
        let data = tempfile::tempdir().unwrap();
        let registry = Arc::new(
            bamboo_llm::ProviderRegistry::from_config(&managed_config, data.path().into())
                .await
                .expect("managed registry"),
        );
        let (expected, origin) = registry
            .provider_for_model_with_origin(&ProviderModelRef::new("chat-a", "main-a"))
            .expect("managed executing provider");
        let origin = origin.expect("a real managed origin is required");
        let live = Arc::new(tokio::sync::RwLock::new(managed_config));
        config.gold_target_resolver = Some(Arc::new(move |model, delegate| {
            bamboo_llm::gold_admission::guarded_gold_target_with_origin(
                live.clone(),
                origin.clone(),
                ProviderModelRef::new("chat-a", model),
                expected.clone(),
                delegate,
            )
        }));
        let mut session = RoutingContext::session();
        let mut state = fixture(&mut session);
        refresh_auxiliary_models_for_round(&mut state, &config);
        let main: Arc<dyn LLMProvider> = context.providers["chat-a"].clone();
        let (_, gold) = selected_providers(&state, &config, &main);
        assert!(gold.queue_requires_current_route());
        assert_eq!(
            gold.known_pre_dispatch_error().is_none(),
            selected == "explicit-gold"
        );
        let (tx, mut rx) = mpsc::channel(64);
        spawn_gold_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            gold,
            CancellationToken::new(),
        )
        .unwrap();
        let gold = state
            .gold_evaluation
            .in_flight
            .take()
            .expect("Gold evaluation");
        let result = tokio::time::timeout(Duration::from_secs(2), gold.join_handle)
            .await
            .expect("guarded Gold evaluation must finish")
            .expect("Gold evaluator must not panic");
        match (selected, result) {
            ("explicit-gold", Some(Ok(result))) => assert_eq!(result.model_name, selected),
            ("unadmitted-gold", Some(Err(_))) => {}
            _ => panic!("unexpected managed Gold evaluation outcome for {selected}"),
        }
        assert_eq!(
            context.models("chat-a"),
            if selected == "explicit-gold" {
                vec![selected]
            } else {
                vec![]
            }
        );
        assert!(context.requests("fast-b").is_empty());
        assert!(context.requests("memory-c").is_empty());
        if selected == "unadmitted-gold" {
            while let Ok(event) = rx.try_recv() {
                assert!(!matches!(
                    event,
                    AgentEvent::GoldEvaluationStarted { .. }
                        | AgentEvent::GoldEvaluationCompleted { .. }
                ));
            }
        }
    }
}

#[tokio::test]
async fn routing_contract_gold_terminal_reads_live_fast_and_preserves_unverified_failure() {
    let context = RoutingContext::new(None);
    let config = config(&context);
    let (tx, _rx) = mpsc::channel(64);
    for (provider, model) in [("fast-b", "cheap-b"), ("memory-c", "background-c")] {
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new(provider, model));
        let mut session = RoutingContext::session();
        let session_id = session.id.clone();
        evaluate_gold_terminal(
            &mut session,
            &None,
            &config,
            "stale-fast-model",
            None,
            &session_id,
            1,
            context.providers["chat-a"].clone(),
            &tx,
        )
        .await;
        assert_eq!(context.models(provider), vec![model]);
    }
    context.config.write().await.defaults.as_mut().unwrap().fast =
        Some(ProviderModelRef::new("unknown-account", "unknown-model"));
    let mut session = RoutingContext::session();
    let session_id = session.id.clone();
    assert!(matches!(
        evaluate_gold_terminal(
            &mut session,
            &None,
            &config,
            "stale-fast-model",
            None,
            &session_id,
            1,
            context.providers["chat-a"].clone(),
            &tx,
        )
        .await,
        GoldTerminalDecision::Stop
    ));
    assert!(context.requests("chat-a").is_empty());
    assert_eq!(context.models("fast-b"), vec!["cheap-b"]);
    assert_eq!(context.models("memory-c"), vec!["background-c"]);
    assert_eq!(
        crate::runtime::goal_state::read_goal_state(&session)
            .unwrap()
            .status,
        crate::runtime::goal_state::GoalRuntimeStatus::NeedInput
    );
}

#[test]
fn child_gold_policy_denial_drops_previously_allowed_queued_target() {
    use crate::runtime::agent::compose_child_gold_policy;
    use crate::runtime::config::GoldTargetResolver;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let context = RoutingContext::new(None);
    let selected: Arc<dyn LLMProvider> = context.providers["chat-a"].clone();
    let other: Arc<dyn LLMProvider> = context.providers["fast-b"].clone();
    let allowed = Arc::new(AtomicBool::new(true));
    let checks = Arc::new(AtomicUsize::new(0));
    let state = allowed.clone();
    let calls = checks.clone();
    let replacement = other.clone();
    let inherited: GoldTargetResolver = Arc::new(move |_, delegate| {
        calls.fetch_add(1, Ordering::SeqCst);
        if state.load(Ordering::SeqCst) {
            delegate
        } else {
            replacement.clone()
        }
    });
    let managed: GoldTargetResolver =
        Arc::new(|_, delegate| bamboo_llm::provider::mark_host_queued_route(delegate));
    let resolver = compose_child_gold_policy(Some(inherited), managed);
    let config = AgentLoopConfig {
        gold_config: Some(GoldConfig {
            enabled: true,
            model_name: None,
            ..Default::default()
        }),
        gold_target_resolver: Some(resolver.clone()),
        ..Default::default()
    };
    // No explicit Gold model keeps the Fast path and does not invoke policy.
    let fast = select_gold_evaluation_provider(&config, &selected, Some(&other));
    assert!(Arc::ptr_eq(&fast, &other));
    assert_eq!(checks.load(Ordering::SeqCst), 0);
    let initial = resolver("gold", selected.clone());
    assert!(initial.queue_requires_current_route());
    let queued = QueuedEvaluation::new((), initial, "gold".into()).unwrap();
    allowed.store(false, Ordering::SeqCst);
    let current = resolver("gold", selected);
    assert!(current.known_pre_dispatch_error().is_some());
    assert!(queued.current_provider(current, "gold").is_none());
    context.assert_no_requests();
}
