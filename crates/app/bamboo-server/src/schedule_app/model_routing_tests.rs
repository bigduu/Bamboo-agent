use super::*;
use bamboo_domain::ScheduleRunConfig;

async fn context(state: &crate::AppState) -> ScheduleContext {
    let store = Arc::new(
        ScheduleStore::new(state.app_data_dir.clone())
            .await
            .unwrap(),
    );
    let base = ScheduleContext {
        schedule_store: store,
        workflow_runs: None,
        agent: state.agent.clone(),
        persistence: state.persistence.clone(),
        tools: state.tools_for(crate::tools::ToolSurface::Root),
        permission_config: state.permission_checker.permission_config(),
        sessions_cache: state.sessions.clone(),
        agent_runners: state.agent_runners.clone(),
        session_event_senders: state.session_event_senders.clone(),
        account_feed_inbox: Some(state.account_sink.inbox()),
        root_account_sink: Some(state.account_sink.clone()),
        app_data_dir: Some(state.app_data_dir.clone()),
        trigger_engine: super::super::default_trigger_engine(),
        project_store: state.project_store.clone(),
        workspace_resolver: state.workspace_resolver.clone(),
        notification_relay: state.notification_relay_deps(),
        resolve_run_config: Arc::new(|_| unreachable!()),
    };
    build_schedule_context(base, state.config.clone(), state.provider_registry.clone())
}

fn job() -> ScheduleRunJob {
    ScheduleRunJob {
        run_id: "synthetic-model-run".into(),
        schedule_id: "synthetic-model-schedule".into(),
        schedule_name: "Synthetic routing".into(),
        scheduled_for: chrono::Utc::now(),
        claimed_at: chrono::Utc::now(),
        was_catch_up: false,
        run_config: serde_json::from_value::<ScheduleRunConfig>(serde_json::json!({
            "auto_execute":true, "task_message":"Synthetic routing prompt",
        }))
        .unwrap(),
    }
}

async fn await_request(provider: &crate::model_routing_test_support::CaptureProvider) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while provider.models.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the admitted schedule must reach its selected synthetic provider"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn routing_contract_schedule_dispatches_the_fast_pair_without_using_the_startup_account() {
    let (_dir, state, providers) = crate::model_routing_test_support::state().await;
    let context = context(&state).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_schedule_job(context, job(), None),
    )
    .await
    .expect("synthetic schedule admission must settle")
    .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !providers["fast-c"].models.lock().unwrap().is_empty()
            || !providers["startup-a"].models.lock().unwrap().is_empty()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the admitted schedule must reach a synthetic provider"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        *providers["fast-c"].models.lock().unwrap(),
        vec!["cheap-model"]
    );
    assert!(providers["startup-a"].models.lock().unwrap().is_empty());
    assert!(providers["chat-b"].models.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routing_contract_schedule_rejects_invalid_explicit_roles_and_lock_failure_before_persist()
{
    for enabled in [false, true] {
        for bad in ["unknown", "disabled", "unadmitted"] {
            let (_dir, state, providers) = crate::model_routing_test_support::state().await;
            {
                let mut saved = state.config.write().await;
                saved.features.provider_model_ref = enabled;
                let target = match bad {
                    "unknown" => {
                        bamboo_domain::ProviderModelRef::new("missing-account", "cheap-model")
                    }
                    "disabled" => {
                        saved.provider_instances.get_mut("fast-c").unwrap().enabled = false;
                        bamboo_domain::ProviderModelRef::new("fast-c", "cheap-model")
                    }
                    _ => bamboo_domain::ProviderModelRef::new("fast-c", "unadmitted-model"),
                };
                saved.defaults.as_mut().unwrap().fast = Some(target);
            }
            let context = context(&state).await;
            let before = state.session_store.list_index_entries().await.len();
            let error = run_schedule_job(context, job(), None)
                .await
                .err()
                .expect("bad explicit schedule role must not become an unset/default target");
            assert!(
                error.contains("Configured schedule model is unavailable"),
                "{error}"
            );
            assert_eq!(state.session_store.list_index_entries().await.len(), before);
            for provider in providers.values() {
                assert!(provider.models.lock().unwrap().is_empty());
            }
        }
    }
    let (_dir, state, providers) = crate::model_routing_test_support::state().await;
    let context = context(&state).await;
    let _write = state.config.write().await;
    let error = run_schedule_job(context, job(), None).await.err().unwrap();
    assert!(error.contains("temporarily unavailable"), "{error}");
    for provider in providers.values() {
        assert!(provider.models.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn routing_contract_schedule_unset_fast_uses_chat_pair_and_explicit_override_uses_default_account(
) {
    let (_dir, state, providers) = crate::model_routing_test_support::state().await;
    state.config.write().await.defaults.as_mut().unwrap().fast = None;
    let context = context(&state).await;
    run_schedule_job(context.clone(), job(), None)
        .await
        .unwrap();
    await_request(&providers["chat-b"]).await;
    assert_eq!(
        *providers["chat-b"].models.lock().unwrap(),
        vec!["chat-model"]
    );
    state.config.write().await.defaults.as_mut().unwrap().fast = Some(
        bamboo_domain::ProviderModelRef::new("missing-account", "invalid-fast"),
    );
    let mut explicit = job();
    explicit.run_config.model = Some("startup-model".into());
    run_schedule_job(context, explicit, None).await.unwrap();
    await_request(&providers["startup-a"]).await;
    assert_eq!(
        *providers["startup-a"].models.lock().unwrap(),
        vec!["startup-model"]
    );
    assert!(providers["fast-c"].models.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routing_contract_schedule_explicit_reasoning_wins_role_policy_at_dispatch() {
    let (_dir, state, providers) = crate::model_routing_test_support::state().await;
    state
        .config
        .write()
        .await
        .defaults
        .as_mut()
        .unwrap()
        .fast
        .as_mut()
        .unwrap()
        .reasoning_effort = Some(bamboo_domain::ReasoningEffort::Low);
    let context = context(&state).await;
    for (step, requested, expected) in [
        (
            1,
            Some(bamboo_domain::ReasoningEffort::High),
            bamboo_domain::ReasoningEffort::High,
        ),
        (2, None, bamboo_domain::ReasoningEffort::Low),
    ] {
        let mut request = job();
        request.run_config.reasoning_effort = requested;
        run_schedule_job(context.clone(), request, None)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while providers["fast-c"].efforts.lock().unwrap().len() < step {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the schedule must dispatch its reasoning policy"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            providers["fast-c"].efforts.lock().unwrap()[step - 1],
            Some(expected)
        );
    }
    assert!(providers["startup-a"].models.lock().unwrap().is_empty());
}
