use super::*;

#[test]
fn all_strategies_use_the_same_output_aware_range() {
    for (window, output, safety, trigger, target) in [
        (1_000_000, 32_000, 10_000, 800_000, 250_000),
        (200_000, 32_000, 2_000, 134_000, 50_000),
        (32_000, 8_000, 1_000, 18_400, 8_000),
    ] {
        for strategy in [
            ContextManagementStrategy::Summary,
            ContextManagementStrategy::MessageSummary,
            ContextManagementStrategy::RetrievalWindow,
        ] {
            let config = AgentLoopConfig {
                context_management: ContextManagementConfig {
                    strategy,
                    ..Default::default()
                },
                ..Default::default()
            };
            let budget = config.context_management.apply_compression_policy(
                TokenBudget::with_safety_margin(window, output, BudgetStrategy::default(), safety),
            );
            let range = budget.compression_thresholds();
            assert_eq!(range.trigger_tokens, trigger);
            assert_eq!(range.target_tokens, target);
            assert_eq!(
                super::super::retrieval_window_trigger_tokens(&config, &budget),
                trigger
            );
            assert_eq!(
                effective_retrieval_window_target_tokens(&budget, range.target_usage_percent),
                target
            );
            assert!(range.trigger_tokens < range.input_limit);
        }
    }
}

#[tokio::test]
async fn shared_range_compresses_both_summary_modes_at_eighty_percent_during_work() {
    let _env = isolate_prompt_safe_env_cache();
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
    ] {
        let id = format!("shared-range-{strategy:?}");
        let mut session = bounded_compression_session(&id);
        session.token_budget = Some(TokenBudget::with_safety_margin(
            1_000_000,
            32_000,
            BudgetStrategy::default(),
            10_000,
        ));
        session.token_usage = Some(pressure_usage(800_000, 1_000_000));
        session
            .token_usage
            .as_mut()
            .unwrap()
            .cache_read_input_tokens = 790_000;
        session.task_list = Some(sample_task_list(&id, TaskItemStatus::InProgress));
        session.force_manual_compression = None;
        let (provider, calls) = prompt_capture_llm();
        let mut config = bounded_compression_config(Arc::clone(&provider));
        config.context_management.strategy = strategy;
        assert!(maybe_apply_host_context_compression(
            &mut session,
            &config,
            "test-model",
            &id,
            &[],
            &provider,
            None,
            "pre-turn"
        )
        .await
        .unwrap());
        assert!(
            !calls.lock().unwrap().is_empty(),
            "active tasks and cache hits must not suppress maintenance"
        );
        let event = session.compression_events.last().unwrap();
        assert!(event.usage_after_percent <= 25.0);
        assert_eq!(event.trigger_type, CompressionTriggerType::Auto);
        assert!(session
            .task_list
            .as_ref()
            .unwrap()
            .has_active_execution_tasks());
    }
}

#[tokio::test]
async fn shared_range_retrieval_receipt_uses_the_same_target_without_summary_calls() {
    let _env = isolate_prompt_safe_env_cache();
    let id = "shared-range-retrieval";
    let mut session = retrieval_window_session(id);
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = retrieval_window_config(persistence);
    config.context_management.trigger_usage_ratio = 0.80;
    config.context_management.target_usage_ratio = 0.25;
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 1;
    let (provider, calls) = recording_llm();
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        id,
        &[retrieval_history_tool_schema()],
        &provider,
        None,
    )
    .await
    .unwrap();
    assert_eq!(checkpoints.lock().unwrap().len(), 1);
    assert!(calls.lock().unwrap().is_empty());
    let event = session.compression_events.last().unwrap();
    assert_eq!(event.retrieval_target_usage_percent, 25);
    assert_eq!(
        event.retrieval_target_tokens,
        prepared.budget.compression_thresholds().target_tokens
    );
    assert!(session.conversation_summary.is_none());
    assert!(!prepared.prepared_context.truncation_occurred);
}

#[tokio::test]
async fn shared_range_rejects_an_infeasible_input_budget_before_provider_dispatch() {
    let _env = isolate_prompt_safe_env_cache();
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
        ContextManagementStrategy::RetrievalWindow,
    ] {
        let mut session = Session::new("shared-range-infeasible", "test-model");
        session.add_message(Message::system("system"));
        session.add_message(Message::user("request"));
        session.token_budget = Some(TokenBudget::with_safety_margin(
            100,
            99,
            BudgetStrategy::default(),
            0,
        ));
        let (provider, calls) = recording_llm();
        let config = AgentLoopConfig {
            context_management: ContextManagementConfig {
                strategy,
                ..Default::default()
            },
            ..Default::default()
        };
        let error = prepare_round_context(
            &mut session,
            &config,
            "test-model",
            "shared-range-infeasible",
            &[],
            &provider,
            None,
        )
        .await
        .err()
        .unwrap();
        assert!(error
            .to_string()
            .contains("compression range is infeasible"));
        assert!(calls.lock().unwrap().is_empty());
        assert!(session.compression_events.is_empty());
    }
}

#[tokio::test]
async fn shared_range_rejects_fixed_tool_schema_cost_before_summary_and_archive() {
    let _env = isolate_prompt_safe_env_cache();
    let mut session = Session::new("shared-range-fixed", "test-model");
    session.add_message(Message::system("system"));
    session.add_message(Message::user("earlier request"));
    session.add_message(Message::assistant("earlier answer", None));
    session.add_message(Message::user("current request"));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        1_000,
        BudgetStrategy::default(),
        0,
    ));
    session.force_manual_compression = Some(String::new());
    let before = archive_state(&session);
    let (provider, calls) = prompt_capture_llm();
    let mut config = bounded_compression_config(Arc::clone(&provider));
    config.context_management.strategy = ContextManagementStrategy::MessageSummary;
    let schema = ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: "mandatory-large-schema".into(),
            description: "mandatory provider schema detail ".repeat(4_000),
            parameters: serde_json::json!({"type":"object","properties":{}}),
        },
    };
    let (tx, mut rx) = mpsc::channel(16);
    assert!(!maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "shared-range-fixed",
        &[schema],
        &provider,
        Some(&tx),
        "pre-turn"
    )
    .await
    .unwrap());
    assert_eq!(archive_state(&session), before);
    assert!(calls.lock().unwrap().is_empty());
    assert!(std::iter::from_fn(|| rx.try_recv().ok()).any(|event| matches!(event,
        AgentEvent::ContextCompressionStatus { status, .. } if status.starts_with("failed_candidate_plan:"))));
}

#[tokio::test]
async fn shared_range_summary_does_not_stop_after_cache_only_reaches_below_trigger() {
    let _env = isolate_prompt_safe_env_cache();
    let mut session = Session::new("shared-range-cache", "test-model");
    session.add_message(Message::system("system"));
    session.add_message(Message::user("initial request"));
    let counter = TiktokenTokenCounter::default();
    let old_unit = "old tool line\n";
    let prose_unit = "durable design evidence ";
    for (call, output) in [
        (
            "old",
            old_unit.repeat((24_000 / counter.count_text(old_unit)) as usize),
        ),
        ("recent-one", "recent result".into()),
        ("recent-two", "latest result".into()),
    ] {
        let mut assistant = Message::assistant("", None);
        assistant.tool_calls = Some(vec![ToolCall {
            id: call.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Bash".into(),
                arguments: "{}".into(),
            },
        }]);
        session.add_message(assistant);
        session.add_message(Message::tool_result(call, output));
        if call == "old" {
            session.add_message(Message::assistant(
                prose_unit.repeat((32_000 / counter.count_text(prose_unit)) as usize),
                None,
            ));
        }
        session.add_message(Message::user(format!("continue after {call}")));
    }
    session.add_message(Message::assistant("latest outcome", None));
    let budget = TokenBudget::with_safety_margin(64_000, 512, BudgetStrategy::default(), 0);
    let before = counter.count_messages(&session.messages);
    let savings = super::super::estimate_prompt_cache_savings_with_fixed_tokens(
        &session, &budget, &counter, 0, 0,
    );
    let projected = before - savings;
    assert!(before > budget.compression_thresholds().trigger_tokens);
    assert!(projected < budget.compression_thresholds().trigger_tokens);
    assert!(projected > budget.compression_thresholds().target_tokens);
    session.token_budget = Some(budget);
    let (provider, calls) = prompt_capture_llm();
    let config = bounded_compression_config(Arc::clone(&provider));
    assert!(maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "shared-range-cache",
        &[],
        &provider,
        None,
        "pre-turn"
    )
    .await
    .unwrap());
    assert!(!calls.lock().unwrap().is_empty());
    assert!(!session.compression_events.is_empty());
}
