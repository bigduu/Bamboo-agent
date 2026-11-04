use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::{tools::ToolSchema, Message, Session};
use bamboo_config::ContextManagementStrategy;
use bamboo_domain::{CompressionEventKind, CompressionTriggerType, ProviderModelRef};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};

use bamboo_server::{routes::agent_routes, AppState};

const PROVIDER: &str = "compact-fixture";
const CHAT: &str = "compact-chat";
const SUMMARY: &str = "compact-summary";
const WINDOW: u32 = 256_000;
const HOOK_INSTRUCTION: &str = "Retain the synthetic compaction fixture marker";
const ID: &str = "compact-ordinary";
const ENDPOINT: &str = "/api/v1/sessions/compact-ordinary/compact";

#[derive(Default)]
struct SummaryOnly {
    summaries: AtomicUsize,
    chat_turns: AtomicUsize,
    hook_instruction_seen: AtomicBool,
    fail_once: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl LLMProvider for SummaryOnly {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        if model != SUMMARY {
            self.chat_turns.fetch_add(1, Ordering::SeqCst);
            return Err(LLMError::Api("Unexpected ordinary model turn".into()));
        }
        self.summaries.fetch_add(1, Ordering::SeqCst);
        if messages
            .iter()
            .any(|message| message.content.contains(HOOK_INSTRUCTION))
        {
            self.hook_instruction_seen.store(true, Ordering::SeqCst);
        }
        if self.fail_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
            return Err(LLMError::Api("Synthetic summarization failure".into()));
        }
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token(
                "The previous turns collected supporting context.".into(),
            )),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn fixture(
    strategy: ContextManagementStrategy,
) -> (
    tempfile::TempDir,
    web::Data<AppState>,
    Arc<SummaryOnly>,
    Session,
) {
    let home = tempfile::tempdir().unwrap();
    let mut config = Config::from_data_dir(Some(home.path().into()));
    config.provider = PROVIDER.into();
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat": { "provider": PROVIDER, "model": CHAT },
            "task_summary": { "provider": PROVIDER, "model": SUMMARY },
        }))
        .unwrap(),
    );
    config.context_management.strategy = strategy;
    config
        .context_management
        .retrieval_window
        .min_recent_user_turns = 2;
    if cfg!(unix) {
        config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
            enabled: true,
            pre_compact: vec![bamboo_config::LifecycleHookGroup {
                hooks: vec![bamboo_config::LifecycleHookHandler::Command {
                    command: format!(
                        r#"printf '%s' '{{"additional_context":"{HOOK_INSTRUCTION}"}}'"#
                    ),
                    timeout_ms: 1_000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
    }
    std::fs::write(home.path().join("model_limits.json"), json!([
        {"model_pattern": CHAT, "max_context_tokens": WINDOW, "max_output_tokens": 4_096, "safety_margin": 1_000},
        {"model_pattern": SUMMARY, "max_context_tokens": WINDOW, "max_output_tokens": 4_096, "safety_margin": 1_000},
    ]).to_string()).unwrap();
    *config.memory_mut() = Some(bamboo_config::MemoryConfig {
        auto_dream_enabled: false,
        gardener_enabled: false,
        dedup_gardener_enabled: false,
        ..Default::default()
    });
    let probe = Arc::new(SummaryOnly::default());
    let state = AppState::new_with_provider(home.path().into(), config, probe.clone())
        .await
        .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.to_owned(), probe.clone() as Arc<dyn LLMProvider>)]),
        PROVIDER.to_owned(),
    ));
    let mut session = Session::new(ID, CHAT);
    session.model_ref = Some(ProviderModelRef::new(PROVIDER, CHAT));
    // Put pressure in archiveable turns, leaving the three protected turns
    // small enough to fit even with the ordinary Root's full tool catalog.
    for turn in 0..10 {
        let context = if turn < 7 {
            " context".repeat(10_000)
        } else {
            String::new()
        };
        session.add_message(Message::user(format!("Turn {turn}{context}")));
        session.add_message(Message::assistant(format!("Completed turn {turn}."), None));
    }
    session.set_last_run_status("completed");
    session.synchronize_terminal_runtime_status();
    state.storage.save_session(&session).await.unwrap();
    (home, web::Data::new(state), probe, session)
}

#[actix_web::test]
async fn compact_session_dispatches_every_strategy_and_commits_without_a_chat_turn() {
    let _traces = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_env_filter("bamboo_engine::runtime::runner::round_lifecycle=debug")
            .with_test_writer()
            .finish(),
    );
    for (strategy, kind, prior_status) in [
        (
            ContextManagementStrategy::Summary,
            CompressionEventKind::Summary,
            "error",
        ),
        (
            ContextManagementStrategy::MessageSummary,
            CompressionEventKind::MessageSummary,
            "cancelled",
        ),
        (
            ContextManagementStrategy::RetrievalWindow,
            CompressionEventKind::RetrievalWindow,
            "completed",
        ),
    ] {
        let (_home, state, probe, mut before) = fixture(strategy).await;
        before.set_last_run_status(prior_status);
        if prior_status != "completed" {
            before.set_last_run_error("Previous chat diagnostic");
        }
        before.synchronize_terminal_runtime_status();
        state.storage.save_session(&before).await.unwrap();
        let before = state.storage.load_session(ID).await.unwrap().unwrap();
        let mut events = state.get_session_event_sender(ID).await.subscribe();
        let app =
            test::init_service(App::new().app_data(state.clone()).configure(agent_routes)).await;
        let response =
            test::call_service(&app, test::TestRequest::post().uri(ENDPOINT).to_request()).await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::OK, "{strategy:?}: {body}");
        assert_eq!(body["status"], "compacted", "{strategy:?}: {body}");
        let previous = body["usage_before_percent"].as_f64().unwrap();
        let after = body["usage_after_percent"].as_f64().unwrap();
        assert!(previous > 25.0 && previous < 80.0, "{strategy:?}: {body}");
        assert!(after > 0.0 && after <= 25.0, "{strategy:?}: {body}");
        assert_eq!(body["token_usage"]["max_context_tokens"], WINDOW);
        let durable = state.storage.load_session(ID).await.unwrap().unwrap();
        assert_previous_run(&durable, &before);
        let [event] = durable.compression_events.as_slice() else {
            panic!("expected one durable compression event");
        };
        assert_eq!(event.kind, kind);
        assert!(matches!(event.trigger_type, CompressionTriggerType::Manual));
        assert_eq!(json!(durable.token_usage), body["token_usage"]);
        let bamboo_agent_core::AgentEvent::TokenBudgetUpdated { usage } =
            events.try_recv().unwrap()
        else {
            panic!("expected the committed budget event");
        };
        assert_eq!(json!(usage), body["token_usage"]);
        for original in &before.messages {
            let saved = durable
                .messages
                .iter()
                .find(|message| message.id == original.id)
                .unwrap();
            assert_eq!(saved.content, original.content);
        }
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|m| m.role == bamboo_domain::Role::User)
                .count(),
            10
        );
        assert_eq!(probe.chat_turns.load(Ordering::SeqCst), 0);
        let summary_calls = probe.summaries.load(Ordering::SeqCst);
        assert_eq!(
            summary_calls > 0,
            strategy != ContextManagementStrategy::RetrievalWindow
        );
        if cfg!(unix) {
            assert_eq!(
                probe.hook_instruction_seen.load(Ordering::SeqCst),
                strategy == ContextManagementStrategy::Summary,
                "only the summary strategy uses the configured hook instructions",
            );
        }
        assert!(state.agent_runners.read().await.is_empty());
        assert!(state
            .session_activation_router
            .current_run_id(ID)
            .await
            .is_none());

        let response: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post().uri(ENDPOINT).to_request(),
        )
        .await;
        assert_eq!(response["status"], "unchanged", "{strategy:?}: {response}");
        assert_eq!(probe.summaries.load(Ordering::SeqCst), summary_calls);
        let unchanged = state.storage.load_session(ID).await.unwrap().unwrap();
        assert_previous_run(&unchanged, &durable);
    }
}

#[actix_web::test]
async fn compact_session_refuses_pending_work_before_calling_a_model() {
    let (_home, state, probe, mut session) = fixture(ContextManagementStrategy::Summary).await;
    session.set_last_run_status("pending");
    state.storage.save_session(&session).await.unwrap();
    let app = test::init_service(App::new().app_data(state.clone()).configure(agent_routes)).await;
    let response =
        test::call_service(&app, test::TestRequest::post().uri(ENDPOINT).to_request()).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let stored = state.storage.load_session(ID).await.unwrap().unwrap();
    assert_eq!(stored.last_run_status().as_deref(), Some("pending"));
    assert!(stored.compression_events.is_empty());
    assert!(stored.messages.iter().all(|message| !message.compressed));
    assert_eq!(probe.summaries.load(Ordering::SeqCst), 0);
    assert_eq!(probe.chat_turns.load(Ordering::SeqCst), 0);
    assert!(state
        .session_activation_router
        .current_run_id(ID)
        .await
        .is_none());
}

#[actix_web::test]
async fn compact_session_fences_duplicate_requests_and_leaves_failed_summary_unpublished() {
    let (_home, state, probe, _) = fixture(ContextManagementStrategy::Summary).await;
    let before = state.storage.load_session(ID).await.unwrap().unwrap();
    probe.fail_once.store(true, Ordering::SeqCst);
    let app = test::init_service(App::new().app_data(state.clone()).configure(agent_routes)).await;
    let first = test::call_service(&app, test::TestRequest::post().uri(ENDPOINT).to_request());
    let during = async {
        tokio::time::timeout(std::time::Duration::from_secs(15), probe.entered.notified())
            .await
            .expect("first request reached the summary provider");
        let duplicate =
            test::call_service(&app, test::TestRequest::post().uri(ENDPOINT).to_request()).await;
        probe.release.notify_one();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    };
    let (failed, ()) = tokio::join!(first, during);
    assert_eq!(failed.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let stored = state.storage.load_session(ID).await.unwrap().unwrap();
    assert_previous_run(&stored, &before);
    assert_eq!(stored.agent_runtime_state, before.agent_runtime_state);
    assert!(stored.compression_events.is_empty());
    assert!(stored.messages.iter().all(|message| !message.compressed));
    assert!(stored.conversation_summary.is_none());
    assert_eq!(probe.summaries.load(Ordering::SeqCst), 1);
    assert_eq!(probe.chat_turns.load(Ordering::SeqCst), 0);
    assert!(state
        .session_activation_router
        .current_run_id(ID)
        .await
        .is_none());
}

fn assert_previous_run(current: &Session, before: &Session) {
    assert_eq!(current.last_run_status(), before.last_run_status());
    assert_eq!(current.last_run_error(), before.last_run_error());
    let mut current_runtime = current.agent_runtime_state.clone();
    if let (Some(current), Some(before)) = (&mut current_runtime, &before.agent_runtime_state) {
        // A successful compaction can append its own hook checkpoint.
        let added = current
            .checkpoints
            .strip_prefix(before.checkpoints.as_slice())
            .expect("prior hook checkpoints are retained");
        assert!(added
            .iter()
            .all(|checkpoint| checkpoint.hook_point == "BeforeCompression"));
        current.checkpoints.truncate(before.checkpoints.len());
    }
    assert_eq!(current_runtime, before.agent_runtime_state);
    for key in [
        "runtime.root_writer_observation_v1",
        "runtime.reconciled_activation_fence",
    ] {
        assert_eq!(current.metadata.get(key), before.metadata.get(key), "{key}");
    }
}

#[actix_web::test]
async fn dropping_compaction_preserves_the_run_and_releases_its_writer() {
    let (_home, state, probe, _) = fixture(ContextManagementStrategy::Summary).await;
    let before = state.storage.load_session(ID).await.unwrap().unwrap();
    probe.fail_once.store(true, Ordering::SeqCst);
    let app = test::init_service(App::new().app_data(state.clone()).configure(agent_routes)).await;
    let mut request = Box::pin(test::call_service(
        &app,
        test::TestRequest::post().uri(ENDPOINT).to_request(),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::select! {
            _ = &mut request => panic!("compaction finished before the provider blocked"),
            _ = probe.entered.notified() => {}
        }
    })
    .await
    .expect("compaction reached its summary provider");
    drop(request);

    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while state
            .session_activation_router
            .current_run_id(ID)
            .await
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached cleanup released the logical owner");
    let after_drop = state.storage.load_session(ID).await.unwrap().unwrap();
    assert_previous_run(&after_drop, &before);
    assert_eq!(after_drop.agent_runtime_state, before.agent_runtime_state);
    assert!(after_drop.compression_events.is_empty());

    let recovered = state
        .storage
        .reconcile_session_runtime(
            &after_drop,
            chrono::Utc::now(),
            None,
            Arc::new(|_| panic!("compaction is not a chat outcome")),
        )
        .await
        .unwrap();
    assert!(recovered.is_none());
    let retry: Value =
        test::call_and_read_body_json(&app, test::TestRequest::post().uri(ENDPOINT).to_request())
            .await;
    assert_eq!(retry["status"], "compacted", "{retry}");
    let durable = state.storage.load_session(ID).await.unwrap().unwrap();
    assert_previous_run(&durable, &before);
    assert_eq!(probe.chat_turns.load(Ordering::SeqCst), 0);
}
