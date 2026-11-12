use super::super::{
    force_overflow_context_recovery, maybe_apply_summary_context_compression_with_budget,
    AgentError,
};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn message_summary_session() -> Session {
    let mut session = Session::new("message-summary-1827", "test-model");
    session.token_budget = Some(TokenBudget::with_safety_margin(
        32_000,
        1_000,
        BudgetStrategy::default(),
        0,
    ));
    session.add_message(Message::system("System instructions"));
    session.add_message(Message::user("OLDER_USER_PROSE: keep the chosen design"));
    let mut assistant = Message::assistant(
        "ASSISTANT_WITH_CALL_PROSE: checking the implementation",
        Some(vec![ToolCall {
            id: "private-tool-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "PRIVATE_TOOL_NAME".into(),
                arguments: "{\"path\":\"PRIVATE_TOOL_ARGUMENT\"}".into(),
            },
        }]),
    );
    assistant.reasoning = Some("PRIVATE_REASONING".into());
    assistant.reasoning_signature = Some("PRIVATE_SIGNATURE".into());
    assistant.metadata = Some(serde_json::json!({"source":"PRIVATE_METADATA"}));
    session.add_message(assistant);
    session.add_message(Message::tool_result(
        "private-tool-call",
        "PRIVATE_TOOL_RESULT",
    ));
    session.add_message(Message::assistant(
        "RECENT_ASSISTANT_PROSE: the change is verified",
        None,
    ));
    session.add_message(Message::user(
        "CURRENT_USER_REQUEST: continue with the next step",
    ));
    session.force_manual_compression = Some(String::new());
    session.metadata.insert(
        "responses.previous_response_id".into(),
        "old-provider-continuation".into(),
    );
    session
}

fn message_summary_config(provider: Arc<dyn LLMProvider>) -> AgentLoopConfig {
    let mut config = bounded_compression_config(provider);
    config.context_management.strategy = ContextManagementStrategy::MessageSummary;
    config
}

async fn run_message_summary(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<bool, AgentError> {
    let budget = session.token_budget.clone().unwrap();
    maybe_apply_summary_context_compression_with_budget(
        session,
        config,
        "test-model",
        "message-summary-1827",
        &[],
        &noop_llm(),
        &budget,
        None,
        "pre-turn",
        None,
        None,
    )
    .await
}

#[tokio::test]
async fn message_summary_excludes_tools_and_metadata_from_summary_and_next_request() {
    let mut session = message_summary_session();
    session.force_manual_compression = Some("PRIVATE_MANUAL_TOOL_ARGUMENT".into());
    let originals = session.messages.clone();
    let current = session.messages.last().unwrap().clone();
    let boundary = provider_transcript_boundary_sha256(Some("test"), Some("openai")).unwrap();
    session
        .activate_provider_transcript_route(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &boundary,
        )
        .unwrap();
    let item = ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider, ProviderTranscriptAuthor::Model,
        serde_json::json!({"type":"reasoning","id":"native-private","summary":[{"type":"summary_text","text":"PRIVATE_NATIVE_REASONING"}]}),
    ).unwrap();
    let search = ProviderTranscriptItem::try_from_payload(
        ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1,
        ProviderTranscriptOrigin::Provider,
        ProviderTranscriptAuthor::Model,
        serde_json::json!({
            "type": "tool_search_call",
            "id": "native-private-search",
            "execution": "client",
            "call_id": "native-private-search-call",
            "status": "completed",
            "arguments": {"query": "PRIVATE_NATIVE_SEARCH"}
        }),
    )
    .unwrap();
    session
        .append_provider_transcript_group(&originals[2].id, None, vec![item, search])
        .unwrap();
    let (provider, requests) = prompt_capture_llm();
    let (persistence, checkpoints) = RetrievalCheckpointPersistence::succeeding();
    let mut config = message_summary_config(provider);
    config.persistence = Some(persistence);
    assert!(run_message_summary(&mut session, &config).await.unwrap());

    let captured = requests.lock().unwrap().clone();
    let sent = serde_json::to_string(&captured).unwrap();
    for prose in [
        "OLDER_USER_PROSE",
        "ASSISTANT_WITH_CALL_PROSE",
        "RECENT_ASSISTANT_PROSE",
    ] {
        assert!(sent.contains(prose), "missing eligible prose: {prose}");
    }
    for excluded in [
        "PRIVATE_TOOL_NAME",
        "PRIVATE_TOOL_ARGUMENT",
        "PRIVATE_TOOL_RESULT",
        "PRIVATE_REASONING",
        "PRIVATE_SIGNATURE",
        "PRIVATE_METADATA",
        "PRIVATE_NATIVE_REASONING",
        "PRIVATE_MANUAL_TOOL_ARGUMENT",
    ] {
        assert!(!sent.contains(excluded), "summary source leaked {excluded}");
    }
    assert!(captured
        .iter()
        .flatten()
        .all(|message| message.tool_calls.is_none() && message.tool_call_id.is_none()));
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
    assert_eq!(
        session.provider_transcript.last_reset_reason(),
        Some(ProviderTranscriptResetReason::Compression)
    );
    assert!(session
        .provider_transcript
        .replayable_groups(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            &boundary
        )
        .is_empty());
    assert_eq!(
        session.compression_events.last().unwrap().kind,
        bamboo_domain::CompressionEventKind::MessageSummary
    );
    assert!(session.force_manual_compression.is_none());
    assert_eq!(checkpoints.lock().unwrap().len(), 1);
    for original in originals {
        let stored = session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap();
        assert_eq!(stored.content, original.content);
        assert_eq!(stored.tool_calls, original.tool_calls);
        assert_eq!(stored.reasoning, original.reasoning);
    }
    assert!(session
        .messages
        .iter()
        .any(|message| message.id == current.id
            && !message.compressed
            && message.content == current.content));
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "message-summary-1827",
        &[],
        &noop_llm(),
        None,
    )
    .await
    .unwrap();
    assert!(prepared
        .prepared_context
        .messages
        .iter()
        .all(|message| message.role != Role::Tool
            && message.tool_calls.is_none()
            && message.tool_call_id.is_none()));
    assert!(!serde_json::to_string(&prepared.prepared_context.messages)
        .unwrap()
        .contains("PRIVATE_TOOL"));
}

#[tokio::test]
async fn message_summary_waits_for_every_result_in_the_active_batch() {
    let mut session = message_summary_session();
    let mut calls = session.messages[2].tool_calls.clone().unwrap();
    calls[0].id = "first-call".into();
    calls.push(ToolCall {
        id: "second-call".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Read".into(),
            arguments: "{}".into(),
        },
    });
    session.add_message(Message::assistant("", Some(calls)));
    session.add_message(Message::tool_result("first-call", "FIRST_PRIVATE_RESULT"));
    let before = archive_state(&session);
    let (provider, requests) = prompt_capture_llm();
    let config = message_summary_config(provider);
    let (tx, mut rx) = mpsc::channel(8);
    assert!(!maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "message-summary-1827",
        &[],
        &noop_llm(),
        Some(&tx),
        "pre-turn",
    )
    .await
    .unwrap());
    assert!(matches!(
        rx.try_recv(),
        Ok(AgentEvent::ContextCompressionStatus { status, .. }) if status == "deferred_tool_protocol"
    ));
    assert_eq!(archive_state(&session), before);
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.force_manual_compression.is_some());
    session.add_message(Message::tool_result("second-call", "SECOND_PRIVATE_RESULT"));
    assert!(run_message_summary(&mut session, &config).await.unwrap());
    assert!(session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .all(|message| message.role != Role::Tool && message.tool_calls.is_none()));
}

#[tokio::test]
async fn automatic_message_summary_compresses_once_after_the_last_tool_result() {
    use super::super::super::maybe_apply_mid_turn_context_compression;
    use bamboo_agent_core::Storage;

    let mut session = message_summary_session();
    session.force_manual_compression = None;
    let mut calls = session.messages[2].tool_calls.clone().unwrap();
    let abandoned_poll = Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "abandoned-poll".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "write_stdin".into(),
                arguments: r#"{"session_id":"previous-shell","chars":""}"#.into(),
            },
        }]),
    );
    let previous_turn_end = session.messages.len() - 1;
    session
        .messages
        .insert(previous_turn_end, abandoned_poll.clone());
    let mut second = calls[0].clone();
    second.id = "last-call".into();
    calls.push(second);
    session.add_message(Message::assistant("", Some(calls)));
    session.add_message(Message::tool_result(
        "private-tool-call",
        "tool output ".repeat(14_500),
    ));
    let before = archive_state(&session);
    let budget = session.token_budget.as_ref().unwrap();
    assert!(
        TiktokenTokenCounter::default().count_messages(&session.messages)
            > budget.compression_trigger_context_tokens()
    );
    let (provider, requests) = prompt_capture_llm();
    let mut config = message_summary_config(provider);
    let directory = tempfile::tempdir().unwrap();
    let storage = Arc::new(
        bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
            .await
            .unwrap(),
    );
    storage.save_session(&session).await.unwrap();
    config.persistence = Some(Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    )));
    let llm = noop_llm();
    let (tx, mut rx) = mpsc::channel(32);

    assert!(!maybe_apply_mid_turn_context_compression(
        &mut session,
        &config,
        &llm,
        &tx,
        "message-summary-1827",
        "test-model",
        &[],
    )
    .await
    .unwrap());
    assert_eq!(archive_state(&session), before);
    assert!(requests.lock().unwrap().is_empty());
    assert!(
        rx.try_recv().is_err(),
        "no compaction attempt for a partial batch"
    );

    session.add_message(Message::tool_result("last-call", "LAST_PRIVATE_RESULT"));
    assert!(maybe_apply_mid_turn_context_compression(
        &mut session,
        &config,
        &llm,
        &tx,
        "message-summary-1827",
        "test-model",
        &[],
    )
    .await
    .unwrap());
    let summary_requests = requests.lock().unwrap().len();
    assert!(summary_requests > 0);
    assert!(!maybe_apply_host_context_compression(
        &mut session,
        &config,
        "test-model",
        "message-summary-1827",
        &[],
        &llm,
        Some(&tx),
        "pre-turn",
    )
    .await
    .unwrap());
    assert_eq!(requests.lock().unwrap().len(), summary_requests);
    assert_eq!(session.compression_events.len(), 1);
    let applied = &session.compression_events[0];
    assert!(applied.usage_after_percent <= config.context_management.target_usage_ratio * 100.0);
    assert!(session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .all(|message| message.role != Role::Tool && message.tool_calls.is_none()));
    let statuses = std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|event| match event {
            AgentEvent::ContextCompressionStatus { status, .. }
                if matches!(
                    status.as_str(),
                    "started" | "completed" | "deferred_tool_protocol"
                ) =>
            {
                Some(status)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(statuses, ["started", "completed"]);

    let saved = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(saved.compression_events.len(), 1);
    assert_eq!(saved.compression_events[0].id, applied.id);
    assert_eq!(saved.messages.len(), session.messages.len());
    let mut saved_poll = saved.messages[previous_turn_end].clone();
    assert!(saved_poll.compressed);
    assert_eq!(
        saved_poll.compressed_by_event_id.take(),
        Some(applied.id.clone())
    );
    saved_poll.compressed = false;
    assert_eq!(
        serde_json::to_value(&saved_poll).unwrap(),
        serde_json::to_value(&abandoned_poll).unwrap(),
        "archive the exact historical call without fabricating a result"
    );
}

#[tokio::test]
async fn message_summary_rebuilds_a_mixed_legacy_summary_from_saved_prose() {
    let mut session = message_summary_session();
    session.messages[1].compressed = true;
    session.conversation_summary = Some(bamboo_domain::ConversationSummary::new(
        "PRIVATE_LEGACY_TOOL_SUMMARY",
        1,
        10,
    ));
    let mut recovery = Message::assistant(
        "[post-compaction-recovery]\nPRIVATE_RECOVERY_TOOL_PATH",
        None,
    );
    recovery.never_compress = true;
    session.messages.insert(2, recovery);
    let (provider, requests) = prompt_capture_llm();
    assert!(
        run_message_summary(&mut session, &message_summary_config(provider))
            .await
            .unwrap()
    );
    let sent = serde_json::to_string(&*requests.lock().unwrap()).unwrap();
    assert!(sent.contains("OLDER_USER_PROSE"));
    assert!(!sent.contains("PRIVATE_LEGACY_TOOL_SUMMARY"));
    assert!(!sent.contains("PRIVATE_RECOVERY_TOOL_PATH"));
    assert!(session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .all(|message| !message.content.contains("PRIVATE_RECOVERY_TOOL_PATH")));
}

#[tokio::test]
async fn message_summary_drops_tool_only_history_without_calling_a_model() {
    let mut session = message_summary_session();
    session.messages.retain(|message| {
        message.role == Role::System
            || message.tool_calls.is_some()
            || message.role == Role::Tool
            || message.content.starts_with("CURRENT_USER_REQUEST")
    });
    session
        .messages
        .iter_mut()
        .find(|message| message.tool_calls.is_some())
        .unwrap()
        .content
        .clear();
    let (provider, requests) = prompt_capture_llm();
    let mut config = message_summary_config(provider);
    config.summarization_model_name = None;
    assert!(run_message_summary(&mut session, &config).await.unwrap());
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.conversation_summary.is_none());
    assert!(session
        .messages
        .iter()
        .filter(|message| !message.compressed)
        .all(|message| message.role != Role::Tool && message.tool_calls.is_none()));
}

#[tokio::test]
async fn message_summary_model_failure_preserves_history_and_retry_request() {
    let mut session = message_summary_session();
    let before = archive_state(&session);
    let (provider, _) = bounded_compression_llm(BoundedFailureMode::Call(1));
    assert!(
        run_message_summary(&mut session, &message_summary_config(provider))
            .await
            .is_err()
    );
    assert_eq!(archive_state(&session), before);
    assert!(session.conversation_summary.is_none());
    assert!(session.compression_events.is_empty());
    assert!(session.force_manual_compression.is_some());
    assert!(session
        .metadata
        .contains_key("responses.previous_response_id"));
}

async fn run_chat_message_summary_retry(
    session: &mut Session,
    model: &str,
    fallback: ResponseTemplate,
) -> (Result<bool, AgentError>, Session) {
    use bamboo_agent_core::Storage;

    let server = MockServer::start().await;
    let calls = AtomicUsize::new(0);
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = request.body_json().unwrap();
            match calls.fetch_add(1, Ordering::Relaxed) {
                0 => chat_summary_stream("The chosen design is recorded."),
                1 => chat_summary_stream(""),
                2 if body["reasoning_effort"] == "none" => ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({
                        "message": "The value of reasoning_effort should be one of low, medium, high, xhigh, and max.",
                    })),
                3 if body.get("reasoning_effort").is_none() && body["enable_thinking"] == false => {
                    fallback.clone()
                }
                _ => ResponseTemplate::new(400)
                    .set_body_string("expected enable_thinking=false without reasoning_effort"),
            }
        })
        .expect(4)
        .mount(&server)
        .await;

    let directory = tempfile::tempdir().unwrap();
    tokio::fs::write(
        directory.path().join("model_limits.json"),
        serde_json::to_vec(&serde_json::json!([{
            "model_pattern": model,
            "max_context_tokens": 32_000,
            "max_output_tokens": 1_000,
        }]))
        .unwrap(),
    )
    .await
    .unwrap();
    let storage = Arc::new(
        bamboo_storage::SessionStoreV2::new(directory.path().to_path_buf())
            .await
            .unwrap(),
    );
    storage.save_session(session).await.unwrap();
    let provider = bamboo_llm::OpenAIProvider::new("test-key")
        .with_base_url(server.uri())
        .with_client(reqwest::Client::builder().no_proxy().build().unwrap());
    let mut config = message_summary_config(Arc::new(provider));
    config.app_data_dir = Some(directory.path().to_path_buf());
    config.summarization_model_name = Some(model.into());
    config.persistence = Some(Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    )));
    let result = run_message_summary(session, &config).await;
    let saved = storage.load_session(&session.id).await.unwrap().unwrap();

    let requests = server.received_requests().await.unwrap();
    let bodies = requests
        .iter()
        .map(|request| request.body_json::<serde_json::Value>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        bodies.len(),
        4,
        "map, empty reduce, rejected disable, corrected disable"
    );
    for body in &bodies {
        assert_eq!(body["model"], model);
    }
    for body in &bodies[..2] {
        assert_eq!(body["reasoning_effort"], "low");
        assert!(body.get("enable_thinking").is_none());
    }
    assert_eq!(bodies[2]["reasoning_effort"], "none");
    assert_eq!(bodies[3]["enable_thinking"], false);
    assert!(bodies[3].get("reasoning_effort").is_none());
    for body in &bodies[2..] {
        assert_eq!(body["messages"], bodies[1]["messages"]);
        assert!(bodies[1]["max_completion_tokens"].as_u64().unwrap() > 0);
        assert_eq!(
            body["max_completion_tokens"],
            bodies[1]["max_completion_tokens"]
        );
    }
    (result, saved)
}

fn chat_summary_stream(text: &str) -> ResponseTemplate {
    let chunk = serde_json::json!({
        "choices": [{"delta": {"content": text}, "finish_reason": "stop"}],
    });
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
}

#[tokio::test]
async fn chat_message_summary_retry_corrects_disable_and_commits_on_success() {
    let mut session = message_summary_session();
    let (result, saved) = run_chat_message_summary_retry(
        &mut session,
        "summary-primary",
        chat_summary_stream("Final summary from the fallback request."),
    )
    .await;
    assert!(result.unwrap());
    assert_eq!(saved.compression_events.len(), 1);
    assert!(saved.messages.iter().any(|message| message.compressed));
    assert_eq!(
        saved.conversation_summary.unwrap().content,
        "Final summary from the fallback request."
    );
}

#[tokio::test]
async fn chat_message_summary_retry_failure_preserves_persisted_history() {
    let mut session = message_summary_session();
    session.messages[1].compressed = true;
    session.conversation_summary = Some(bamboo_domain::ConversationSummary::new(
        "Previous accepted summary",
        1,
        10,
    ));
    let messages = serde_json::to_value(&session.messages).unwrap();
    let summary = serde_json::to_value(&session.conversation_summary).unwrap();
    let pending = session.force_manual_compression.clone();
    let (result, saved) = run_chat_message_summary_retry(
        &mut session,
        "summary-secondary",
        ResponseTemplate::new(400)
            .set_body_string("Invalid reasoning_effort 'none': mock rejected the summary retry"),
    )
    .await;
    let error = result.unwrap_err().to_string();
    assert!(error.contains("mock rejected the summary retry"), "{error}");
    assert_eq!(session.force_manual_compression, pending);
    for state in [&session, &saved] {
        assert_eq!(serde_json::to_value(&state.messages).unwrap(), messages);
        assert_eq!(
            serde_json::to_value(&state.conversation_summary).unwrap(),
            summary
        );
        assert!(state.compression_events.is_empty());
    }
}

#[tokio::test]
async fn message_summary_save_failure_does_not_publish_the_archive() {
    let mut session = message_summary_session();
    let before = archive_state(&session);
    let (provider, _) = prompt_capture_llm();
    let mut config = message_summary_config(provider);
    config.persistence = Some(RetrievalCheckpointPersistence::failing().0);
    let error = run_message_summary(&mut session, &config)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("message-summary checkpoint failed"));
    assert_eq!(archive_state(&session), before);
    assert!(session.conversation_summary.is_none());
    assert!(session.compression_events.is_empty());
    assert!(session.force_manual_compression.is_some());
    assert!(session
        .metadata
        .contains_key("responses.previous_response_id"));
}

#[tokio::test]
async fn message_summary_runs_for_automatic_pressure_and_critical_overflow() {
    for critical_overflow in [false, true] {
        let mut session = message_summary_session();
        session.force_manual_compression = None;
        session
            .messages
            .iter_mut()
            .find(|message| message.role == Role::Tool)
            .unwrap()
            .content = "tool output ".repeat(20_000);
        let (provider, _) = prompt_capture_llm();
        let config = message_summary_config(provider);
        let applied = if critical_overflow {
            force_overflow_context_recovery(
                &mut session,
                &config,
                "test-model",
                "message-summary-1827",
                &[],
                &noop_llm(),
                None,
            )
            .await
        } else {
            maybe_apply_host_context_compression(
                &mut session,
                &config,
                "test-model",
                "message-summary-1827",
                &[],
                &noop_llm(),
                None,
                "pre-turn",
            )
            .await
        };
        assert!(applied.unwrap());
        assert_eq!(
            session.compression_events.last().unwrap().kind,
            bamboo_domain::CompressionEventKind::MessageSummary
        );
        assert!(session
            .messages
            .iter()
            .filter(|message| !message.compressed)
            .all(|message| message.role != Role::Tool && message.tool_calls.is_none()));
    }
}
