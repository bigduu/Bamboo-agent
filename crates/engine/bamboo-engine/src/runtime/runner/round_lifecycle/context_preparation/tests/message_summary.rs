use super::super::{
    force_overflow_context_recovery, maybe_apply_summary_context_compression_with_budget,
    AgentError,
};
use super::*;

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
        &noop_llm(),
        &budget,
        None,
        "pre-turn",
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
    session.messages[2]
        .tool_calls
        .as_mut()
        .unwrap()
        .push(ToolCall {
            id: "second-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: "{}".into(),
            },
        });
    let before = archive_state(&session);
    let (provider, requests) = prompt_capture_llm();
    let config = message_summary_config(provider);
    assert!(!run_message_summary(&mut session, &config).await.unwrap());
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
