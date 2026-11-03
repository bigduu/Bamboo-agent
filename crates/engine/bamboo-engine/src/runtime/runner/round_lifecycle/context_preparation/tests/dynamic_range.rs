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
    let repeated = |unit: &str, target: u32| {
        let first = unit.repeat((target / counter.count_text(unit)) as usize);
        let adjusted = ((first.len() / unit.len()) as u64 * u64::from(target)
            / u64::from(counter.count_text(&first))) as usize
            + 1;
        unit.repeat(adjusted)
    };
    let old_unit = "old tool line\n";
    let prose_unit = "durable design evidence ";
    for (call, output) in [
        ("old", repeated(old_unit, 24_000)),
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
            session.add_message(Message::assistant(repeated(prose_unit, 32_000), None));
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
    assert!(
        before > budget.compression_thresholds().trigger_tokens,
        "before={before}"
    );
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

struct CountingVisionProvider {
    vision_calls: Arc<AtomicUsize>,
    main_requests: Arc<Mutex<Vec<Vec<Message>>>>,
    explicit_native: bool,
}

#[async_trait::async_trait]
impl LLMProvider for CountingVisionProvider {
    async fn vision_support_override(&self, _model: &str) -> Option<bool> {
        self.explicit_native.then_some(true)
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _output: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let response = if model == "vision-single" {
            let call = self.vision_calls.fetch_add(1, Ordering::SeqCst) + 1;
            format!("VISION_DESCRIPTION_{call}")
        } else {
            self.main_requests.lock().unwrap().push(messages.to_vec());
            "main answer".into()
        };
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token(response)),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn shared_range_reuses_the_only_paid_vision_transform_for_actual_dispatch() {
    let _env = isolate_prompt_safe_env_cache();
    for strategy in [
        ContextManagementStrategy::Summary,
        ContextManagementStrategy::MessageSummary,
    ] {
        let id = format!("shared-range-vision-{strategy:?}");
        let mut session = bounded_compression_session(&id);
        session.token_budget = Some(TokenBudget::with_safety_margin(
            200_000,
            8_000,
            BudgetStrategy::default(),
            2_000,
        ));
        session.add_message(Message::user_with_parts(
            "latest image",
            vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/latest.png".into(),
                    detail: None,
                },
            }
            .into()],
        ));
        let (summary_provider, _) = prompt_capture_llm();
        let mut config = bounded_compression_config(summary_provider);
        config.model_name = Some("test-model".into());
        config.context_management.strategy = strategy;
        config.image_fallback = Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("vision-single".into()),
        });
        let vision_calls = Arc::new(AtomicUsize::new(0));
        let main_requests = Arc::new(Mutex::new(Vec::new()));
        let llm: Arc<dyn LLMProvider> = Arc::new(CountingVisionProvider {
            vision_calls: Arc::clone(&vision_calls),
            main_requests: Arc::clone(&main_requests),
            explicit_native: false,
        });
        let before = archive_state(&session);
        assert!(!maybe_apply_host_context_compression(
            &mut session,
            &config,
            "test-model",
            &id,
            &[],
            &llm,
            None,
            "mid-turn"
        )
        .await
        .unwrap());
        assert_eq!(archive_state(&session), before);
        assert_eq!(vision_calls.load(Ordering::SeqCst), 0);
        let (tx, _rx) = mpsc::channel(128);
        super::super::super::execute_llm_round(
            &mut session,
            &config,
            &llm,
            &tx,
            &tokio_util::sync::CancellationToken::new(),
            &id,
            "test-model",
            &[],
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(vision_calls.load(Ordering::SeqCst), 1);
        let requests = main_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let sent = serde_json::to_string(&requests[0]).unwrap();
        assert!(sent.contains("VISION_DESCRIPTION_1"));
        assert!(!sent.contains("VISION_DESCRIPTION_2"));
        assert!(session.messages.last().unwrap().content_parts.is_some());
        assert!(!session.compression_events.is_empty());
    }
}

#[tokio::test]
async fn shared_range_native_image_cost_cannot_pass_a_text_only_target_check() {
    let _env = isolate_prompt_safe_env_cache();
    let mut session = Session::new("shared-range-native-image", "test-model");
    session.add_message(Message::system("system"));
    session.add_message(Message::user("old requirement"));
    session.add_message(Message::assistant("old response", None));
    session.add_message(Message::user_with_parts(
        "latest image",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "https://example.com/latest.png".into(),
                detail: Some("original".into()),
            },
        }
        .into()],
    ));
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        1_000,
        BudgetStrategy::default(),
        0,
    ));
    session.force_manual_compression = Some(String::new());
    let before = archive_state(&session);
    let (summary_provider, _) = prompt_capture_llm();
    let mut config = bounded_compression_config(summary_provider);
    config.context_management.strategy = ContextManagementStrategy::MessageSummary;
    let llm: Arc<dyn LLMProvider> = Arc::new(CountingVisionProvider {
        vision_calls: Arc::new(AtomicUsize::new(0)),
        main_requests: Arc::new(Mutex::new(Vec::new())),
        explicit_native: true,
    });
    let (tx, mut rx) = mpsc::channel(128);
    assert!(!maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "shared-range-native-image",
        &[],
        &llm,
        Some(&tx),
        "pre-turn"
    )
    .await
    .unwrap());
    assert_eq!(archive_state(&session), before);
    assert!(session.compression_events.is_empty());
    assert!(std::iter::from_fn(|| rx.try_recv().ok()).any(|event| matches!(event,
        AgentEvent::ContextCompressionStatus { status, .. } if status.starts_with("failed_postcondition:"))));
}

struct AdoptingSummaryPersistence {
    append_user: bool,
}

#[async_trait::async_trait]
impl RuntimeSessionPersistence for AdoptingSummaryPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        if self.append_user {
            session.add_message(Message::user("NEWLY_ADMITTED_USER_REQUIREMENT"));
        } else {
            session.updated_at += chrono::Duration::seconds(1);
        }
        Ok(())
    }
}

#[tokio::test]
async fn shared_range_prepared_reuse_detects_adopted_context_before_main_dispatch() {
    let _env = isolate_prompt_safe_env_cache();
    for append_user in [false, true] {
        let id = format!("shared-range-save-{append_user}");
        let mut session = bounded_compression_session(&id);
        session.token_budget = Some(TokenBudget::with_safety_margin(
            200_000,
            8_000,
            BudgetStrategy::default(),
            2_000,
        ));
        session.add_message(Message::user_with_parts(
            "latest image",
            vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/latest.png".into(),
                    detail: None,
                },
            }
            .into()],
        ));
        let (summary_provider, _) = prompt_capture_llm();
        let mut config = bounded_compression_config(summary_provider);
        config.model_name = Some("test-model".into());
        config.persistence = Some(Arc::new(AdoptingSummaryPersistence { append_user }));
        config.image_fallback = Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("vision-single".into()),
        });
        let vision_calls = Arc::new(AtomicUsize::new(0));
        let main_requests = Arc::new(Mutex::new(Vec::new()));
        let llm: Arc<dyn LLMProvider> = Arc::new(CountingVisionProvider {
            vision_calls: Arc::clone(&vision_calls),
            main_requests: Arc::clone(&main_requests),
            explicit_native: false,
        });
        let (tx, _rx) = mpsc::channel(128);
        if append_user {
            let result = super::super::super::execute_llm_round(
                &mut session,
                &config,
                &llm,
                &tx,
                &tokio_util::sync::CancellationToken::new(),
                &id,
                "test-model",
                &[],
                None,
                None,
            )
            .await;
            assert!(result
                .err()
                .unwrap()
                .to_string()
                .contains("became stale during persistence"));
            assert!(main_requests.lock().unwrap().is_empty());
            assert!(session
                .messages
                .iter()
                .any(|message| message.content == "NEWLY_ADMITTED_USER_REQUIREMENT"));
        } else {
            // Verify this new summary/persistence handoff independently of the
            // existing final-dispatch ledger checkpoint/reprepare loop.
            let prepared = prepare_round_context(
                &mut session,
                &config,
                "test-model",
                &id,
                &[],
                &llm,
                Some(&tx),
            )
            .await
            .unwrap();
            assert!(serde_json::to_string(&prepared.prepared_context.messages)
                .unwrap()
                .contains("VISION_DESCRIPTION_1"));
        }
        assert_eq!(vision_calls.load(Ordering::SeqCst), 1);
    }
}
