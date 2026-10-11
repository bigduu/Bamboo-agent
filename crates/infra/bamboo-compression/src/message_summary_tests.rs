use super::*;
use bamboo_domain::{FunctionCall, Role, ToolCall};

fn budget() -> TokenBudget {
    let mut budget = TokenBudget::for_model(16_000);
    budget.compression_target_percent = 40;
    budget.working_reserve_tokens = 0;
    budget
}

fn candidate(session: &Session) -> Result<CompressionCandidatePlan, CompressionPlanError> {
    build_message_summary_candidate_plan_with_fixed_tokens(
        session,
        "test-model",
        Some(&budget()),
        0.20,
        CompressionTriggerType::Manual,
        0,
    )
}

fn tool_call(id: &str, content: &str, name: &str) -> Message {
    Message::assistant(
        content,
        Some(vec![ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: r#"{"file_path":"/secret/tool-argument.rs"}"#.into(),
            },
        }]),
    )
}

fn event(kind: CompressionEventKind) -> CompressionEvent {
    let mut event = CompressionEvent::new(
        0,
        0,
        0.0,
        0.0,
        0,
        CompressionTriggerType::Manual,
        0.0,
        None,
        0,
    );
    event.kind = kind;
    event
}

#[test]
fn archives_all_eligible_history_including_recent_completed_skill_tools() {
    let mut session = Session::new("message-summary-all", "test-model");
    session.add_message(Message::system("system"));
    session.add_message(Message::user("old requirement"));
    session.add_message(Message::assistant("old answer", None));
    let mut anchor = Message::assistant("required child boundary", None);
    anchor.never_compress = true;
    let anchor_id = anchor.id.clone();
    session.add_message(anchor);
    let current = Message::user("current request");
    let current_id = current.id.clone();
    session.add_message(current);
    session.add_message(tool_call("skill", "loading guidance", "load_skill"));
    session.add_message(Message::tool_result("skill", "tool-only-result-sentinel"));
    session.add_message(Message::assistant("most recent prose", None));

    let plan = candidate(&session).unwrap();
    assert_eq!(plan.kind, CompressionEventKind::MessageSummary);
    assert_eq!(plan.compressed_message_ids.len(), 5);
    assert!(!plan.compressed_message_ids.contains(&anchor_id));
    assert!(!plan.compressed_message_ids.contains(&current_id));
    assert_eq!(
        plan.messages_to_summarize
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        [
            "old requirement",
            "old answer",
            "loading guidance",
            "most recent prose"
        ]
    );
    assert!(!plan.merge_existing_summary);
}

#[test]
fn source_contains_only_prose_and_budget_excludes_tool_payloads() {
    let mut session = Session::new("message-summary-fields", "test-model");
    let mut assistant = tool_call("call", "assistant prose", "Write");
    assistant.reasoning = Some("hidden-reasoning-sentinel".into());
    assistant.reasoning_signature = Some("signature-sentinel".into());
    assistant.content_parts = Some(vec![]);
    assistant.image_ocr = Some(vec![]);
    assistant.metadata = Some(serde_json::json!({"private": "metadata-sentinel"}));
    session.add_message(Message::user("past user prose"));
    session.add_message(assistant);
    session.add_message(Message::tool_result(
        "call",
        "tool-output-sentinel ".repeat(8_000),
    ));
    session.add_message(Message::user("current input"));

    let plan = candidate(&session).unwrap();
    let counter = TiktokenTokenCounter::default();
    assert_eq!(
        plan.source_tokens,
        counter.count_messages(&plan.messages_to_summarize)
    );
    assert_eq!(
        plan.target_summary_tokens,
        ((plan.source_tokens as f64) * 0.20).ceil() as u32
    );
    for source in &plan.messages_to_summarize {
        assert!(matches!(source.role, Role::User | Role::Assistant));
        assert!(source.reasoning.is_none());
        assert!(source.reasoning_signature.is_none());
        assert!(source.content_parts.is_none());
        assert!(source.image_ocr.is_none());
        assert!(source.metadata.is_none());
        assert!(source.phase.is_none());
        assert!(source.tool_calls.is_none());
        assert!(source.tool_call_id.is_none());
        assert!(source.tool_success.is_none());
    }
    let serialized = serde_json::to_string(&plan.messages_to_summarize).unwrap();
    assert!(!serialized.contains("sentinel"));
    assert!(!serialized.contains("/secret/tool-argument.rs"));
}

#[test]
fn tool_only_pass_archives_without_llm_summary_or_tool_derived_recovery() {
    let mut session = Session::new("message-summary-tool-only", "test-model");
    session.add_message(tool_call("call", "", "Write"));
    session.add_message(Message::tool_result("call", "tool-only payload"));
    session.add_message(Message::user("current request"));
    let original_messages = session.messages.clone();
    let candidate = candidate(&session).unwrap();
    assert!(candidate.messages_to_summarize.is_empty());
    assert_eq!(candidate.source_tokens, 0);
    assert_eq!(candidate.target_summary_tokens, 0);
    let plan = finalize_compression_candidate_plan(&session, candidate, String::new()).unwrap();
    assert_eq!(plan.summary_tokens, 0);
    assert_eq!(apply_compression_plan(&mut session, plan), 2);
    assert!(session.conversation_summary.is_none());
    assert_eq!(session.messages.len(), original_messages.len());
    assert!(session.messages[..2].iter().all(|m| m.compressed));
    assert!(!session.messages[2].compressed);
    assert_eq!(session.messages[0].content, original_messages[0].content);
    assert_eq!(
        serde_json::to_value(&session.messages[0].tool_calls).unwrap(),
        serde_json::to_value(&original_messages[0].tool_calls).unwrap()
    );
    assert_eq!(session.messages[1].content, "tool-only payload");
    assert_eq!(
        session.compression_events.last().unwrap().kind,
        CompressionEventKind::MessageSummary
    );
    assert!(session
        .messages
        .iter()
        .all(|m| !is_post_compaction_recovery(m)));
}

#[test]
fn incomplete_or_orphan_tools_reject_the_whole_pass_without_mutation() {
    for messages in [
        vec![tool_call("pending", "pending prose", "Read")],
        vec![Message::tool_result("orphan", "orphan payload")],
        vec![
            tool_call("duplicate", "calling", "Read"),
            Message::tool_result("duplicate", "first result"),
            Message::tool_result("duplicate", "second result"),
        ],
    ] {
        let mut session = Session::new("message-summary-incomplete", "test-model");
        session.add_message(Message::user("old prose"));
        for message in messages {
            session.add_message(message);
        }
        session.add_message(Message::user("current request"));
        let original = serde_json::to_value(&session).unwrap();
        assert!(matches!(
            candidate(&session),
            Err(CompressionPlanError::MessageSummaryToolProtocol { .. })
        ));
        assert_eq!(serde_json::to_value(&session).unwrap(), original);
    }
}

#[test]
fn protected_tool_chain_defers_instead_of_leaving_tool_context_active() {
    for protect_call in [true, false] {
        let mut session = Session::new("message-summary-protected-tool", "test-model");
        let mut call = tool_call("protected", "calling", "load_skill");
        let mut result = Message::tool_result("protected", "guidance");
        call.never_compress = protect_call;
        result.never_compress = !protect_call;
        session.add_message(call);
        session.add_message(result);
        session.add_message(Message::user("current request"));
        assert!(matches!(
            candidate(&session),
            Err(CompressionPlanError::MessageSummaryToolProtocol { .. })
        ));
    }
}

#[test]
fn first_switch_rebuilds_legacy_summary_from_durable_prose_and_retires_recovery() {
    let mut session = Session::new("message-summary-migration", "test-model");
    let mut historical = Message::assistant("durable original prose", None);
    historical.compressed = true;
    session.add_message(historical);
    let mut old_tool = Message::tool_result("historic", "historical tool-output sentinel");
    old_tool.compressed = true;
    session.add_message(old_tool);
    let mut recovery =
        Message::assistant("[post-compaction-recovery]\n/secret/tool-argument.rs", None);
    recovery.never_compress = true;
    let recovery_id = recovery.id.clone();
    session.add_message(recovery);
    session.add_message(Message::assistant("new prose", None));
    session.add_message(Message::user("current request"));
    session.conversation_summary = Some(ConversationSummary::new(
        "legacy tool-derived sentinel",
        2,
        20,
    ));
    session
        .compression_events
        .push(event(CompressionEventKind::Summary));

    let candidate = candidate(&session).unwrap();
    assert!(!candidate.merge_existing_summary);
    assert_eq!(candidate.previous_represented_source_tokens, 0);
    assert!(candidate.compressed_message_ids.contains(&recovery_id));
    assert_eq!(
        candidate
            .messages_to_summarize
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["durable original prose", "new prose"]
    );
    let plan =
        finalize_compression_candidate_plan(&session, candidate, "prose summary".into()).unwrap();
    apply_compression_plan(&mut session, plan);
    assert!(session
        .messages
        .iter()
        .filter(|m| !m.compressed)
        .all(|m| !is_post_compaction_recovery(m)));
}

#[test]
fn later_pass_reuses_only_a_message_summary_and_counts_new_prose_once() {
    let mut session = Session::new("message-summary-reuse", "test-model");
    let mut historical = Message::assistant("already summarized prose", None);
    historical.compressed = true;
    session.add_message(historical);
    session.add_message(Message::assistant("new prose", None));
    session.add_message(Message::user("current request"));
    session.conversation_summary = Some(
        ConversationSummary::new("safe existing prose summary", 1, 20)
            .with_compression_metrics(100, 20, 0.20, false, None),
    );
    session
        .compression_events
        .push(event(CompressionEventKind::MessageSummary));
    session
        .compression_events
        .push(event(CompressionEventKind::RetrievalWindow));

    let plan = candidate(&session).unwrap();
    assert!(plan.merge_existing_summary);
    assert_eq!(plan.messages_to_summarize.len(), 1);
    assert_eq!(plan.messages_to_summarize[0].content, "new prose");
    assert_eq!(plan.previous_represented_source_tokens, 100);
    assert_eq!(plan.represented_source_tokens, 100 + plan.source_tokens);
    assert_eq!(
        plan.target_summary_tokens,
        ((plan.represented_source_tokens as f64) * 0.20).ceil() as u32
    );
}

#[test]
fn content_changed_after_selection_cannot_commit_a_stale_summary() {
    let mut session = Session::new("message-summary-stale", "test-model");
    session.add_message(Message::assistant("original prose", None));
    session.add_message(Message::user("current request"));
    let candidate = candidate(&session).unwrap();
    session.messages[0].content = "changed prose".into();
    assert!(matches!(
        finalize_compression_candidate_plan(&session, candidate, "original summary".into()),
        Err(CompressionPlanError::CandidateSetChanged)
    ));
    assert!(session.messages.iter().all(|m| !m.compressed));
}

#[test]
fn prose_archived_by_retrieval_after_the_last_message_summary_is_incorporated_once() {
    let mut session = Session::new("message-summary-after-retrieval", "test-model");
    let prior_summary_event = event(CompressionEventKind::MessageSummary);
    let retrieval_event = event(CompressionEventKind::RetrievalWindow);
    let mut represented = Message::assistant("already represented prose", None);
    represented.compressed = true;
    represented.compressed_by_event_id = Some(prior_summary_event.id.clone());
    session.add_message(represented);
    let mut hidden = Message::assistant("retrieval-archived prose", None);
    hidden.compressed = true;
    hidden.compressed_by_event_id = Some(retrieval_event.id.clone());
    session.add_message(hidden);
    session.add_message(Message::assistant("new prose", None));
    session.add_message(Message::user("current request"));
    session.conversation_summary = Some(ConversationSummary::new("safe prior summary", 1, 20));
    session
        .compression_events
        .extend([prior_summary_event, retrieval_event]);

    let plan = candidate(&session).unwrap();
    assert!(plan.merge_existing_summary);
    assert_eq!(
        plan.messages_to_summarize
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["retrieval-archived prose", "new prose"]
    );
    assert_eq!(plan.compressed_message_ids.len(), 1);
}

#[test]
fn adoption_without_previous_summary_recovers_archived_prose() {
    let mut session = Session::new("message-summary-no-prior-summary", "test-model");
    let mut hidden = Message::user("previously archived user requirement");
    hidden.compressed = true;
    session.add_message(hidden);
    session.add_message(Message::assistant("new prose", None));
    session.add_message(Message::user("current request"));
    session
        .compression_events
        .push(event(CompressionEventKind::RetrievalWindow));

    let plan = candidate(&session).unwrap();
    assert!(!plan.merge_existing_summary);
    assert_eq!(
        plan.messages_to_summarize
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["previously archived user requirement", "new prose"]
    );
}

#[test]
fn empty_summary_cannot_silently_discard_represented_prose() {
    let mut session = Session::new("message-summary-empty-output", "test-model");
    session.add_message(Message::assistant("important prose", None));
    session.add_message(Message::user("current request"));
    let candidate = candidate(&session).unwrap();
    assert!(matches!(
        finalize_compression_candidate_plan(&session, candidate, String::new()),
        Err(CompressionPlanError::EmptyMessageSummary)
    ));
    assert!(session.messages.iter().all(|m| !m.compressed));
}
