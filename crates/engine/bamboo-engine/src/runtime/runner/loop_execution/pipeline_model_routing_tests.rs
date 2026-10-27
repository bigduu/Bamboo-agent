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
        tokio::time::timeout(Duration::from_secs(2), task.join_handle)
            .await
            .expect("synthetic task evaluation must finish")
            .unwrap();
    }
    if let Some(task) = state.gold_evaluation.in_flight.take() {
        tokio::time::timeout(Duration::from_secs(2), task.join_handle)
            .await
            .expect("synthetic Gold evaluation must finish")
            .unwrap();
    }
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
            let (tx, _rx) = mpsc::channel(32);
            assert!(
                spawn_task_evaluation_if_needed(
                    0,
                    &session,
                    &tx,
                    &config,
                    &mut state,
                    context.providers["chat-a"].clone(),
                    CancellationToken::new(),
                )
                .is_err(),
                "Task must reject {rejection}"
            );
            assert!(
                spawn_gold_evaluation_if_needed(
                    0,
                    &session,
                    &tx,
                    &config,
                    &mut state,
                    context.providers["chat-a"].clone(),
                    CancellationToken::new(),
                )
                .is_err(),
                "Gold must reject {rejection}"
            );
            assert!(state.task_evaluation.in_flight.is_none());
            assert!(state.gold_evaluation.in_flight.is_none());
            context.assert_no_requests();
        }
    }
}

#[tokio::test]
async fn routing_contract_queued_task_and_gold_keep_their_admitted_provider_model_pair() {
    for failure in [Failure::Auth, Failure::Quota, Failure::Network] {
        let context = RoutingContext::new(Some(false));
        for provider in context.providers.values() {
            *provider.failure.lock().unwrap() = Some(failure);
        }
        let config = config(&context);
        let mut session = RoutingContext::session();
        let mut state = fixture(&mut session);
        let (tx, _rx) = mpsc::channel(64);
        refresh_auxiliary_models_for_round(&mut state, &config);
        spawn_task_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            context.providers["chat-a"].clone(),
            CancellationToken::new(),
        )
        .unwrap();
        spawn_gold_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            context.providers["chat-a"].clone(),
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
            context.providers["chat-a"].clone(),
            CancellationToken::new(),
        )
        .unwrap();
        spawn_gold_evaluation_if_needed(
            1,
            &session,
            &tx,
            &config,
            &mut state,
            context.providers["chat-a"].clone(),
            CancellationToken::new(),
        )
        .unwrap();
        assert!(state.task_evaluation.queued_request.is_some());
        assert!(state.gold_evaluation.queued_request.is_some());
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("memory-c", "background-c"));
        refresh_auxiliary_models_for_round(&mut state, &config);
        finish_evaluations(&mut state).await;
        start_queued_task_evaluation_if_idle(&mut state, &tx, CancellationToken::new(), 1);
        start_queued_gold_evaluation_if_idle(&mut state, &tx, CancellationToken::new(), 1);
        finish_evaluations(&mut state).await;
        assert_eq!(context.models("fast-b"), vec!["cheap-b"; 4]);
        assert!(context.requests("memory-c").is_empty());
        assert!(context.requests("chat-a").is_empty());
    }
}

#[tokio::test]
async fn routing_contract_gold_dedicated_model_uses_executing_account_and_validates_admission() {
    for selected in ["explicit-gold", "unadmitted-gold"] {
        let context = RoutingContext::new(Some(false));
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("missing-account", "invalid-fast"));
        let mut config = config(&context);
        config.gold_config.as_mut().unwrap().model_name = Some(selected.into());
        let mut session = RoutingContext::session();
        let mut state = fixture(&mut session);
        refresh_auxiliary_models_for_round(&mut state, &config);
        let (tx, _rx) = mpsc::channel(64);
        let result = spawn_gold_evaluation_if_needed(
            0,
            &session,
            &tx,
            &config,
            &mut state,
            context.providers["chat-a"].clone(),
            CancellationToken::new(),
        );
        assert_eq!(result.is_ok(), selected == "explicit-gold");
        finish_evaluations(&mut state).await;
        assert_eq!(
            context.models("chat-a"),
            if selected == "explicit-gold" {
                vec!["explicit-gold"]
            } else {
                vec![]
            }
        );
        assert!(context.requests("fast-b").is_empty());
        assert!(context.requests("memory-c").is_empty());
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
