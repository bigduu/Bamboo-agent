use super::super::{
    maybe_apply_summary_context_compression_with_budget, retrieval_window_preflight,
};
use super::*;

pub(super) fn history_schema() -> ToolSchema {
    let mut schema = retrieval_history_tool_schema();
    schema.function.parameters["properties"]["action"]["enum"] =
        serde_json::json!(["search_current", "read_slice"]);
    schema
}

pub(super) fn add_batch(
    session: &mut Session,
    id: &str,
    tool: &str,
    args: serde_json::Value,
    content: String,
) {
    let mut assistant = Message::assistant("", None);
    assistant.tool_calls = Some(vec![ToolCall {
        id: format!("call-{id}"),
        tool_type: "function".into(),
        function: FunctionCall {
            name: tool.into(),
            arguments: args.to_string(),
        },
    }]);
    let mut output = Message::tool_result(format!("call-{id}"), content);
    output.id = id.into();
    output.tool_success = Some(true);
    session.messages.extend([assistant, output]);
}

fn config(pruning: bool) -> AgentLoopConfig {
    let (persistence, _) = RetrievalCheckpointPersistence::succeeding();
    AgentLoopConfig {
        model_name: Some("test-model".into()),
        background_model_name: Some("test-model".into()),
        token_budget: Some(TokenBudget {
            max_output_tokens: 800,
            safety_margin: 100,
            ..TokenBudget::for_model(16_000)
        }),
        context_management: ContextManagementConfig {
            prune_old_tool_outputs: pruning,
            target_usage_ratio: 0.40,
            ..Default::default()
        },
        persistence: Some(persistence),
        ..Default::default()
    }
}

fn output(messages: &[Message], id: &str) -> String {
    messages
        .iter()
        .find(|message| message.id == id)
        .unwrap()
        .content
        .clone()
}

fn transcript(log_lines: usize) -> Session {
    let mut session = Session::new("old-tools", "test-model");
    session
        .messages
        .push(Message::user("Check the build and inspect matches."));
    add_batch(
        &mut session,
        "build",
        "Bash",
        serde_json::json!({"command": "cargo build"}),
        "compiler package instruction object source artifact\n".repeat(log_lines),
    );
    add_batch(
        &mut session,
        "tests",
        "Bash",
        serde_json::json!({"command": "cargo test --locked"}),
        "test workspace integration module assertion passed\n".repeat(log_lines),
    );
    add_batch(
        &mut session,
        "search",
        "Grep",
        serde_json::json!({}),
        "workspace module definition match marker source file\n".repeat(log_lines),
    );
    add_batch(
        &mut session,
        "read",
        "Read",
        serde_json::json!({}),
        "source lines kept intact\n".repeat(180),
    );
    add_batch(
        &mut session,
        "recent-one",
        "Grep",
        serde_json::json!({}),
        "recent match details\n".repeat(180),
    );
    add_batch(
        &mut session,
        "recent-two",
        "Grep",
        serde_json::json!({}),
        "more recent match details\n".repeat(180),
    );
    session
}

#[tokio::test]
async fn pruning_reaches_its_own_target_without_summary_and_reuses_saved_preview() {
    let mut session = transcript(900);
    let config = config(true);
    let (llm, calls) = recording_llm();
    let tools = [history_schema()];
    let source = output(&session.messages, "search");
    let read = output(&session.messages, "read");
    let recent = output(&session.messages, "recent-one");
    let prepared = prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "old-tools",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("the eligible old output should leave room");
    let preview = output(&prepared.prepared_context.messages, "search");
    assert!(preview.contains("read_slice, message_id search"));
    assert!(preview.len() < source.len());
    assert_eq!(output(&session.messages, "search"), source);
    assert_eq!(output(&prepared.prepared_context.messages, "read"), read);
    assert_eq!(
        output(&prepared.prepared_context.messages, "recent-one"),
        recent
    );
    assert!(calls.lock().unwrap().is_empty());
    assert!(session.compression_events.is_empty());
    assert!(!prepared.prepared_context.truncation_occurred);
    let state = session.model_context_state.clone().unwrap();
    assert!(state.tool_output_previews.contains_key("search"));
    let mut resumed: Session =
        serde_json::from_slice(&serde_json::to_vec(&session).unwrap()).unwrap();
    resumed.messages.push(Message::user("Continue checking."));
    let prepared = prepare_round_context(
        &mut resumed,
        &config,
        "test-model",
        "old-tools",
        &tools,
        &llm,
        None,
    )
    .await
    .expect("resume should reuse the same rendering");
    assert_eq!(
        output(&prepared.prepared_context.messages, "search"),
        preview
    );
    assert_eq!(resumed.model_context_state.unwrap(), state);
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pruning_uses_fresh_provider_pressure_and_invalidates_usage_only_after_rewrite() {
    let mut session = transcript(240);
    let config = config(true);
    let raw_budget = config.token_budget.as_ref().unwrap();
    let budget = config
        .context_management
        .apply_compression_policy(raw_budget.clone());
    let counter = TiktokenTokenCounter::default();
    let tools = [history_schema()];
    let (llm, calls) = recording_llm();

    for expected_previews in 1..=2 {
        let projection = retrieval_window_preflight(
            &session,
            &config,
            "test-model",
            &tools,
            &llm,
            &budget,
            &counter,
        )
        .await
        .unwrap();
        assert!(projection.active_tokens < budget.compression_thresholds().trigger_tokens);
        session.token_usage = Some(pressure_usage(14_000, 16_000));

        let summarized = maybe_apply_summary_context_compression_with_budget(
            &mut session,
            &config,
            "test-model",
            "old-tools",
            &tools,
            &llm,
            raw_budget,
            None,
            "test",
            None,
            None,
        )
        .await
        .unwrap();

        assert!(!summarized);
        assert_eq!(
            session
                .model_context_state
                .as_ref()
                .unwrap()
                .tool_output_previews
                .len(),
            expected_previews
        );
        assert!(session.token_usage.is_none());
    }
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pruning_that_cannot_fit_falls_back_to_summary_without_committing_previews() {
    let mut session = transcript(900);
    for index in 0..6 {
        session.messages.insert(
            0,
            Message::assistant(
                "analysis decisions notes on the implementation and its acceptance criteria\n"
                    .repeat(520),
                None,
            ),
        );
        session
            .messages
            .insert(0, Message::user(format!("Earlier request {index}")));
    }
    let (llm, calls) = recording_llm();
    let config = config(true);
    prepare_round_context(
        &mut session,
        &config,
        "test-model",
        "old-tools",
        &[history_schema()],
        &llm,
        None,
    )
    .await
    .expect("the summary should fit the remaining request");
    assert!(!calls.lock().unwrap().is_empty());
    assert!(session.conversation_summary.is_some());
    assert!(session
        .model_context_state
        .unwrap()
        .tool_output_previews
        .is_empty());
}
