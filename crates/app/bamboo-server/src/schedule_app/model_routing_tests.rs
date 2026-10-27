use super::*;
use bamboo_domain::ScheduleRunConfig;

#[tokio::test]
async fn routing_contract_schedule_dispatches_the_fast_pair_without_using_the_startup_account() {
    let (_dir, state, providers) = crate::model_routing_test_support::state().await;
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
    let context =
        build_schedule_context(base, state.config.clone(), state.provider_registry.clone());
    let job = ScheduleRunJob {
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
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_schedule_job(context, job, None),
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
