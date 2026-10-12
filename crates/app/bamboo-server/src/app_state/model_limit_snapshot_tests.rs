use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_engine::execution::{
    reserve_session_execution, AgentStatus, SessionExecutionReserveOutcome,
};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::stream;
use serde_json::json;

use super::AppState;
use crate::handlers::agent::execute::runtime::{
    spawn_agent_execution, spawn_event_forwarder_with_root_actor, SpawnAgentExecution,
};

#[derive(Default)]
struct ModelLimitProbe {
    output_limits: Mutex<Vec<Option<u32>>>,
}

#[async_trait]
impl LLMProvider for ModelLimitProbe {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.output_limits.lock().unwrap().push(max_output_tokens);
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::Token("finished".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn run_hosted_probe(
    state: &web::Data<AppState>,
    mut session: Session,
    probe: Arc<ModelLimitProbe>,
) -> u32 {
    state
        .agent
        .prepare_external_session_for_execution(&mut session)
        .await
        .unwrap();
    state.storage.save_session(&session).await.unwrap();
    let session_id = session.id.clone();
    let session_tx = state.get_session_event_sender(&session_id).await;
    let mut events = session_tx.subscribe();
    let mut reservation = match reserve_session_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        &session_id,
        &session_tx,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        SessionExecutionReserveOutcome::AlreadyRunning { run_id } => {
            panic!("fresh hosted probe already has a run: {run_id}")
        }
    };
    reservation
        .bind_root_actor(&state.agent, &session)
        .await
        .unwrap();
    let writer = reservation
        .root_actor_writer()
        .expect("hosted Root must have its real Actor writer");
    let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel(128);
    let history_commit_barrier = spawn_event_forwarder_with_root_actor(
        state.clone(),
        session_id.clone(),
        reservation.run_id().to_string(),
        mpsc_rx,
        session_tx,
        None,
        Some(writer),
    );
    spawn_agent_execution(SpawnAgentExecution {
        state: state.clone(),
        session_id: session_id.clone(),
        session,
        execution_reservation: reservation,
        is_child_session: false,
        provider_name: state.config.read().await.provider.clone(),
        provider_override: Some(probe),
        model_roster: bamboo_engine::ModelRoster {
            model: Some("legacy-hosted-model".into()),
            ..Default::default()
        },
        reasoning_effort: None,
        reasoning_effort_source: "test".into(),
        disabled_tools: Default::default(),
        disabled_skill_ids: Default::default(),
        mpsc_tx,
        history_commit_barrier,
        image_fallback: None,
        gold_config: None,
        app_data_dir: Some(state.app_data_dir.clone()),
        run_budget: None,
    });
    // Resolved budgets are deliberately not persisted. Observe the actual
    // hosted event route, then wait for its durable history and finalization.
    let context = tokio::time::timeout(Duration::from_secs(60), async {
        let mut context = None;
        loop {
            match events.recv().await.unwrap() {
                AgentEvent::TokenBudgetUpdated { usage } => {
                    context = Some(usage.max_context_tokens);
                }
                AgentEvent::SessionHistoryCommitted {
                    session_id: committed,
                } => {
                    assert_eq!(committed, session_id);
                    break context.expect("hosted provider run must publish its resolved budget");
                }
                AgentEvent::Error { message } => panic!("hosted probe failed: {message}"),
                _ => {}
            }
        }
    })
    .await
    .expect("hosted probe must publish its durable history");
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = state
                .agent_runners
                .read()
                .await
                .get(&session_id)
                .map(|runner| runner.status.clone());
            match status {
                Some(AgentStatus::Completed) => break,
                Some(AgentStatus::Error(error)) => panic!("hosted probe failed: {error}"),
                Some(AgentStatus::Cancelled) => panic!("hosted probe was cancelled"),
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("hosted probe must finalize its reserved runner");
    context
}

#[tokio::test]
async fn hosted_restart_loads_legacy_raw_model_limits_and_retains_them_until_repair() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
    let dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let installed = bamboo_config::ConfigFacade::open_or_migrate(dir.path()).unwrap();
    let mut config = installed.effective_config();
    config.proxy_auth = Some(bamboo_config::ProxyAuth {
        username: "test-user".into(),
        password: "test-password".into(),
    });
    bamboo_config::persist_proxy_auth_credential_transaction_at_revision(
        dir.path(),
        &mut config,
        installed.registry().core.snapshot().revision,
    )
    .unwrap();
    drop(installed);
    let path = dir.path().join("model_limits.json");
    let write_limit = |context, output| {
        std::fs::write(
            &path,
            json!([{
                "model_pattern": "legacy-hosted-model",
                "max_context_tokens": context,
                "max_output_tokens": output
            }])
            .to_string(),
        )
        .unwrap();
    };
    write_limit(48_000, 4_000);

    // Use the real hosted constructor: it must inject the section reopened
    // from an already-installed modular layout into its assembled Agent.
    let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
    let section = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .model_limits
        .clone();
    assert_eq!(
        section.snapshot().status,
        bamboo_config::SectionStatus::Healthy
    );
    let probe = Arc::new(ModelLimitProbe::default());
    let project = state
        .project_store
        .create_with_project_path(
            "Legacy model limits",
            None,
            workspace.path().to_string_lossy().into_owned(),
            Vec::new(),
        )
        .unwrap();
    let mut contexts = Vec::new();

    for (run, invalid) in [None, Some("{invalid"), Some("[{}]"), None]
        .into_iter()
        .enumerate()
    {
        if let Some(invalid) = invalid {
            std::fs::write(&path, invalid).unwrap();
            section.reload();
            assert_ne!(
                section.snapshot().status,
                bamboo_config::SectionStatus::Healthy
            );
        } else if run > 0 {
            write_limit(96_000, 8_000);
            section.reload();
            assert_eq!(
                section.snapshot().status,
                bamboo_config::SectionStatus::Healthy
            );
        }
        let mut session = Session::new(format!("legacy-model-limit-{run}"), "legacy-hosted-model");
        session.set_project_id_meta(project.id.to_string());
        session.add_message(Message::user("finish this request"));
        contexts.push(Box::pin(run_hosted_probe(&state, session, probe.clone())).await);
    }

    assert_eq!(contexts, [48_000, 48_000, 48_000, 96_000]);
    assert_eq!(
        *probe.output_limits.lock().unwrap(),
        [Some(4_000), Some(4_000), Some(4_000), Some(8_000)]
    );
    state.shutdown().await;
}

#[actix_web::test]
async fn compatibility_model_limit_writes_and_clear_reach_hosted_rounds_without_watcher() {
    use bamboo_compression::limits::{DEFAULT_MAX_CONTEXT_TOKENS, DEFAULT_MAX_OUTPUT_TOKENS};

    let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
    let dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    state.stop_config_watcher_for_test();
    let state = web::Data::new(state);
    let section = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .model_limits
        .clone();
    let initial_revision = section.snapshot().revision;
    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/bamboo/config",
        web::post().to(crate::handlers::settings::set_bamboo_config),
    ))
    .await;
    let project = state
        .project_store
        .create_with_project_path(
            "Compatibility model limits",
            None,
            workspace.path().to_string_lossy().into_owned(),
            Vec::new(),
        )
        .unwrap();
    let probe = Arc::new(ModelLimitProbe::default());

    for (run, (rows, expected_context)) in [
        (
            json!([{
                "model_pattern": "legacy-hosted-model",
                "max_context_tokens": 48_000,
                "max_output_tokens": 4_000
            }]),
            48_000,
        ),
        (
            json!([{
                "model_pattern": "legacy-hosted-model",
                "max_context_tokens": 96_000,
                "max_output_tokens": 8_000
            }]),
            96_000,
        ),
        (json!([]), DEFAULT_MAX_CONTEXT_TOKENS),
    ]
    .into_iter()
    .enumerate()
    {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/bamboo/config")
                .set_json(json!({"model_limits": rows}))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success());
        let snapshot = section.snapshot();
        assert_eq!(snapshot.revision, initial_revision + run as u64 + 1);
        assert_eq!(snapshot.data.0.len(), if run == 2 { 0 } else { 1 });

        let mut session = Session::new(
            format!("compatibility-model-limit-{run}"),
            "legacy-hosted-model",
        );
        session.set_project_id_meta(project.id.to_string());
        session.add_message(Message::user("finish this request"));
        assert_eq!(
            Box::pin(run_hosted_probe(&state, session, probe.clone())).await,
            expected_context
        );
    }

    // Queue overlapping compatibility mutations behind the same IO guard. The
    // middle clear matches the pre-lock snapshot, but must still run after the
    // first replacement. Both equality and revision reads belong inside IO.
    let rows = |context, output| {
        vec![json!({
            "model_pattern": "legacy-hosted-model",
            "max_context_tokens": context,
            "max_output_tokens": output
        })]
    };
    let io = state.config_io_lock.lock().await;
    let (first, clear, last, ()) = tokio::join!(
        biased;
        state.replace_compatibility_model_limits(rows(96_000, 8_000)),
        state.replace_compatibility_model_limits(Vec::new()),
        state.replace_compatibility_model_limits(rows(144_000, 12_000)),
        async {
            tokio::task::yield_now().await;
            drop(io);
        },
    );
    first.unwrap();
    clear.unwrap();
    last.unwrap();
    let snapshot = section.snapshot();
    assert_eq!(snapshot.revision, initial_revision + 6);
    assert_eq!(snapshot.data.0, rows(144_000, 12_000));
    let mut session = Session::new(
        "compatibility-model-limit-concurrent",
        "legacy-hosted-model",
    );
    session.set_project_id_meta(project.id.to_string());
    session.add_message(Message::user("finish this request"));
    assert_eq!(
        Box::pin(run_hosted_probe(&state, session, probe.clone())).await,
        144_000
    );
    assert_eq!(
        *probe.output_limits.lock().unwrap(),
        [
            Some(4_000),
            Some(8_000),
            Some(DEFAULT_MAX_OUTPUT_TOKENS),
            Some(12_000)
        ]
    );
    state.shutdown().await;
}
