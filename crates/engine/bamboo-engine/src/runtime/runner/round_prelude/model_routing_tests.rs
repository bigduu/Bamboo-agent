use super::*;
use crate::app_context::model_routing_test_support::RoutingContext;
use crate::runtime::config::PromptMemoryFlags;
use std::sync::Arc;

#[tokio::test]
async fn routing_contract_round_prelude_leaves_background_model_idle() {
    let state = RoutingContext::new(Some(true));
    let config = AgentLoopConfig {
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: true,
            relevant_recall: true,
            relevant_recall_rerank: true,
            project_first_dream: true,
            ledger_agenda: false,
        },
        background_model_name: Some("background-c".into()),
        background_model_provider: Some(state.providers["memory-c"].clone()),
        auxiliary_model_resolver: Some(Arc::new(|| {
            panic!("preparing a task request should never resolve a background memory model")
        })),
        ..Default::default()
    };
    let mut session = RoutingContext::session();
    session.messages.clear();
    session.add_message(Message::system("Synthetic base"));
    session.add_message(Message::user("synthetic routing marker"));
    let session_id = session.id.clone();
    let mut runtime = AgentRuntimeState::new("synthetic-routing-rounds");
    prepare_round(
        &mut session,
        &mut None,
        &mut runtime,
        &config,
        &bamboo_tools::BuiltinToolExecutor::new(),
        &RoundPreludeFrame {
            execution_id: "synthetic-execution",
            round: 0,
            max_rounds: None,
            debug_enabled: false,
            cancel_token: &CancellationToken::new(),
            metrics_collector: None,
            session_id: &session_id,
            model_name: "main-a",
        },
    )
    .await
    .unwrap();
    assert!(state.requests("memory-c").is_empty());
    assert!(state.requests("chat-a").is_empty());
}
