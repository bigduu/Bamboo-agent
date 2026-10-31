use super::*;
use crate::app_context::model_routing_test_support::RoutingContext;
use crate::runtime::config::{AuxiliaryModelConfig, PromptMemoryFlags};
use bamboo_agent_core::Message;
use bamboo_memory::memory_store::{DurableMemoryType, MemoryScope, MemoryStore};
use std::sync::Mutex;

#[tokio::test]
async fn routing_contract_round_prelude_uses_live_background_pair_and_authoritative_unset() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryStore::new(directory.path());
    for (title, content) in [
        (
            "Synthetic routing marker one",
            "Synthetic routing marker for a first memory",
        ),
        (
            "Synthetic routing marker two",
            "Synthetic routing marker for a different memory",
        ),
    ] {
        memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Feedback,
                title,
                content,
                &["synthetic".into(), "routing".into(), "marker".into()],
                None,
                "fixture",
                false,
                None,
            )
            .await
            .unwrap();
    }
    let state = RoutingContext::new(Some(true));
    let live = Arc::new(Mutex::new(AuxiliaryModelConfig::default()));
    let resolver_live = live.clone();
    let mut config = AgentLoopConfig {
        memory_store: memory,
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: true,
            relevant_recall_rerank: true,
            project_first_dream: false,
            ledger_agenda: false,
        },
        background_model_name: Some("legacy-fast-a".into()),
        background_model_provider: Some(state.providers["chat-a"].clone()),
        auxiliary_model_resolver: Some(Arc::new(move || resolver_live.lock().unwrap().clone())),
        ..Default::default()
    };
    let mut session = RoutingContext::session();
    session.messages.clear();
    session.add_message(Message::system("Synthetic base"));
    session.add_message(Message::user("synthetic routing marker"));
    let session_id = session.id.clone();
    let mut runtime = AgentRuntimeState::new("synthetic-routing-rounds");
    let cancellation = CancellationToken::new();
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    for (round, selected) in [
        Some(("memory-c", "background-c")),
        Some(("fast-b", "cheap-b")),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        *live.lock().unwrap() = AuxiliaryModelConfig {
            background_model_name: selected.map(|(_, model)| model.to_string()),
            background_model_provider: selected
                .map(|(provider, _)| state.providers[provider].clone() as Arc<dyn LLMProvider>),
            ..Default::default()
        };
        prepare_round(
            &mut session,
            &mut None,
            &mut runtime,
            &config,
            state.providers["chat-a"].clone(),
            &tools,
            &RoundPreludeFrame {
                execution_id: "synthetic-execution",
                round,
                max_rounds: None,
                debug_enabled: false,
                cancel_token: &cancellation,
                metrics_collector: None,
                session_id: &session_id,
                model_name: "main-a",
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(
        state.requests("memory-c"),
        vec![("background-c".into(), "memory_rerank".into())]
    );
    assert_eq!(
        state.requests("fast-b"),
        vec![("cheap-b".into(), "memory_rerank".into())]
    );
    assert!(
        state.requests("chat-a").is_empty(),
        "live None must not resurrect the startup target"
    );

    // Standalone SDK loops without a live resolver retain their supplied target.
    config.auxiliary_model_resolver = None;
    prepare_round(
        &mut session,
        &mut None,
        &mut runtime,
        &config,
        state.providers["chat-a"].clone(),
        &tools,
        &RoundPreludeFrame {
            execution_id: "synthetic-execution",
            round: 3,
            max_rounds: None,
            debug_enabled: false,
            cancel_token: &cancellation,
            metrics_collector: None,
            session_id: &session_id,
            model_name: "main-a",
        },
    )
    .await
    .unwrap();
    assert_eq!(
        state.requests("chat-a"),
        vec![("legacy-fast-a".into(), "memory_rerank".into())]
    );
}
