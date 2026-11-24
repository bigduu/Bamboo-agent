use super::{managed_shutdown::ManagedShutdown, planned_update_handoff};
use crate::app_state::AppState;
use actix_web::{App, HttpServer};
use bamboo_agent_core::{FunctionCall, Message, Session, ToolCall, ToolSchema};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorActivationStatus, ActorDirectoryPort,
    AgentRuntimeState, AgentStatusState, PendingQuestion, PlanModeState, PlanModeStatus,
    ProviderModelRef, RootActorRuntimeWrite, RuntimeSessionPersistence, Storage, SuspensionState,
};
use bamboo_engine::session_activation::ParkedUpdateRoot;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use chrono::Utc;
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const PROVIDER: &str = "update-fixture-provider";
const MODEL: &str = "update-fixture-model";

struct LiveRoundProbe {
    in_model: Semaphore,
    finish_round: Semaphore,
    path: std::path::PathBuf,
}

#[async_trait::async_trait]
impl LLMProvider for LiveRoundProbe {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        self.in_model.add_permits(1);
        self.finish_round.acquire().await.unwrap().forget();
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: "planned-update-read".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Read".into(),
                    arguments: serde_json::json!({"file_path": self.path}).to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn planned_update_parks_concurrent_live_roots_after_their_real_tool_calls() {
    use actix_web::test;

    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().canonicalize().unwrap();
    let path = workspace.join("update-read-fixture.txt");
    std::fs::write(&path, "canonical result before replacement").unwrap();
    let probe = Arc::new(LiveRoundProbe {
        in_model: Semaphore::new(0),
        finish_round: Semaphore::new(0),
        path,
    });
    let mut config = Config::from_data_dir(Some(workspace.clone()));
    config.provider = "openai".into();
    config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
        model: Some(MODEL.into()),
        ..Default::default()
    });
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = AppState::new_with_provider_for_legacy_ingress_test(
        workspace.clone(),
        config,
        probe.clone(),
    )
    .await
    .unwrap()
    .into_http_data();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([("openai".into(), probe.clone() as Arc<dyn LLMProvider>)]),
        "openai".into(),
    ));
    let app = test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let ids = ["planned-live-first", "planned-live-second"];
    for id in ids {
        let mut session = Session::new(id, MODEL);
        session.title_generated = true;
        session.set_workspace_path_meta(workspace.to_string_lossy());
        session.set_provider_name("openai");
        session.metadata.insert(
            bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
            "explicit".into(),
        );
        session.add_message(Message::user("Read the fixture."));
        crate::handlers::agent::events::mark_pending_turn(&mut session);
        assert!(session.host_run_id().is_none());
        state.storage.save_session(&session).await.unwrap();
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model": MODEL}))
                .to_request(),
        )
        .await;
        let code = response.status();
        let response: serde_json::Value = test::read_body_json(response).await;
        assert!(code.is_success(), "{id}: {response}");
    }
    tokio::time::timeout(Duration::from_secs(12), probe.in_model.acquire_many(2))
        .await
        .expect("the live Roots reached their provider")
        .unwrap()
        .forget();
    let admission = state
        .session_activation_router
        .hold_admissions_for_update()
        .await
        .unwrap();
    assert_eq!(admission.outstanding_sessions().await, ids);
    assert!(admission.parked_roots().await.is_empty());
    probe.finish_round.add_permits(2);
    let parked = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let parked = admission.parked_roots().await;
            if parked.len() == ids.len() {
                return parked;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("each Root reached a persisted model-round boundary");

    for root in &parked {
        let saved = state
            .storage
            .load_session(&root.session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            bamboo_storage::SessionStoreV2::root_runtime_matches_activation(&saved, &root.fence)
                .unwrap()
        );
        assert!(saved.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some("planned-update-read")
                && message
                    .content
                    .contains("canonical result before replacement")
        }));
    }
    admission
        .commit_parked_roots(|roots| {
            assert_eq!(roots, parked);
            Ok(())
        })
        .await
        .unwrap();
    for root in parked {
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let entry = state
                    .session_store
                    .inspect_actor(&root.session_id)
                    .await
                    .unwrap();
                let owner = entry.activation.unwrap();
                assert_eq!(owner.fence(), root.fence);
                if owner.status == ActorActivationStatus::Succeeded {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the old Actor closed cleanly after the update commit");
        let saved = state
            .storage
            .load_session(&root.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.last_run_status().as_deref(), Some("suspended"));
    }
    assert_eq!(probe.in_model.available_permits(), 0);
    state.shutdown().await;
}

#[derive(Default)]
struct ProviderProbe(AtomicUsize);

#[async_trait::async_trait]
impl LLMProvider for ProviderProbe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.tool_call_id.as_deref() == Some("original-call"))
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>(),
            ["already finished"]
        );
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Continued using the saved result.".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn planned_update_desktop_protocol_resumes_only_the_recorded_run_once() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = std::fs::canonicalize(dir.path()).unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(workspace.clone())
            .await
            .unwrap(),
    );
    let mut session = Session::new("planned-update-root", MODEL);
    session.title_generated = true;
    session.set_workspace_path_meta(workspace.to_string_lossy());
    session.set_provider_name(PROVIDER);
    session.set_last_run_status("suspended");
    session
        .metadata
        .insert("runtime.suspend_reason".into(), "planned_update".into());
    let mut runtime = AgentRuntimeState::new(&session.id);
    runtime.status = AgentStatusState::Suspended;
    runtime.suspension = Some(SuspensionState {
        reason: "planned_update".into(),
        suspended_at: Utc::now(),
        resumable: true,
        hook_point: None,
    });
    session.metadata.insert(
        "agent.runtime.state".into(),
        serde_json::to_string(&runtime).unwrap(),
    );
    session.agent_runtime_state = Some(runtime);
    session.add_message(Message::user("Read the fixture and report what you found."));
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "original-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: r#"{"file_path":"fixture.txt"}"#.into(),
            },
        }]),
    ));
    session.add_message(Message::tool_result("original-call", "already finished"));
    store.save_session(&session).await.unwrap();

    let now = Utc::now();
    let owner = store
        .claim_activation(&ActorActivationClaim {
            actor_id: session.id.as_str().into(),
            run_id: "previous-root".into(),
            lease_owner: "predecessor-host".into(),
            lease_expires_at: now + chrono::Duration::minutes(1),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    store.start_activation(&owner.fence(), now).await.unwrap();
    let repository = bamboo_engine::SessionRepository::new(
        Default::default(),
        store.clone(),
        Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
    )
    .with_root_actor_directory(store.clone())
    .bind_root_response_writer(RootActorRuntimeWrite {
        fence: owner.fence(),
        created_at: session.created_at,
        conversation_generation: session.conversation_generation_observation(),
    })
    .unwrap();
    repository
        .checkpoint_runtime_session(&mut session)
        .await
        .unwrap();
    drop(repository);
    store
        .finish_activation(&owner.fence(), Utc::now(), ActorActivationFinish::Succeeded)
        .await
        .unwrap();
    let checkpoint = ParkedUpdateRoot {
        update_id: uuid::Uuid::new_v4(),
        session_id: session.id.clone(),
        created_at: session.created_at,
        fence: owner.fence(),
        conversation_reset_sha256: session.conversation_reset_sha256().unwrap(),
        transcript_sha256: ParkedUpdateRoot::hash_transcript(&session.messages).unwrap(),
        root_tool_authority_revision: session.root_tool_authority_revision,
        project_id: session.project_id_meta(),
        workspace_path: session.workspace_path_meta(),
        message_id: session.messages.last().unwrap().id.clone(),
        message_count: session.messages.len(),
        model: ProviderModelRef::new(PROVIDER, MODEL),
    };
    let checkpoint: ParkedUpdateRoot =
        serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();

    let mut waiting = Session::new("waiting-for-plan-approval", MODEL);
    waiting.set_provider_name(PROVIDER);
    waiting.set_last_run_status("suspended");
    let question = PendingQuestion {
        tool_call_id: "plan-call".into(),
        tool_name: "ExitPlanMode".into(),
        question: "Approve the plan?".into(),
        options: vec!["Approve".into(), "Revise".into()],
        allow_custom: false,
        source: Default::default(),
    };
    waiting.pending_question = Some(question.clone());
    let mut state = AgentRuntimeState::new(&waiting.id);
    state.status = AgentStatusState::Suspended;
    state.plan_mode = Some(PlanModeState {
        entered_at: now,
        pre_permission_mode: "default".into(),
        plan_file_path: None,
        status: PlanModeStatus::AwaitingApproval,
    });
    waiting.agent_runtime_state = Some(state);
    store.save_session(&waiting).await.unwrap();
    drop(store);

    let request = uuid::Uuid::new_v4();
    planned_update_handoff::publish(&workspace, request, std::slice::from_ref(&checkpoint))
        .unwrap();

    let probe = Arc::new(ProviderProbe::default());
    let mut config = Config::from_data_dir(Some(workspace.clone()));
    config.provider = PROVIDER.into();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state =
        AppState::new_with_provider_for_legacy_ingress_test(workspace, config, probe.clone())
            .await
            .unwrap()
            .into_http_data();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), probe.clone() as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    let before = state
        .session_store
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.last_run_status().as_deref(), Some("suspended"));
    assert!(
        bamboo_storage::SessionStoreV2::root_runtime_matches_activation(&before, &checkpoint.fence)
            .unwrap()
    );
    let mut stale = checkpoint.clone();
    let mut changed_messages = session.messages.clone();
    changed_messages[0].content = "Changed original request.".into();
    stale.transcript_sha256 = ParkedUpdateRoot::hash_transcript(&changed_messages).unwrap();
    assert!(!state
        .child_completion_coordinator
        .resume_planned_update_root(&stale)
        .await
        .unwrap());
    let mut stale_actor = checkpoint.clone();
    stale_actor.fence.lease_owner = "another-owner".into();
    assert!(state
        .child_completion_coordinator
        .resume_planned_update_root(&stale_actor)
        .await
        .is_err());
    let still_previous = state
        .session_store
        .inspect_actor(&session.id)
        .await
        .unwrap();
    assert_eq!(still_previous.activation.unwrap().fence(), checkpoint.fence);
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);

    let control = tempfile::tempdir().unwrap();
    let managed = ManagedShutdown::new(control.path().to_path_buf()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server = HttpServer::new(App::new)
        .disable_signals()
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
    let managed = actix_web::rt::spawn(managed.run(server, Some(state.clone())));
    let read_marker = |name: &str| {
        let path = control.path().join(name);
        async move {
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    match std::fs::read(&path) {
                        Ok(value) => return value,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                        Err(error) => panic!("cannot read managed update marker: {error}"),
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap()
        }
    };
    assert!(read_marker("ready").await.is_empty());
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    let mut marker = tempfile::NamedTempFile::new_in(control.path()).unwrap();
    write!(marker, "{request}").unwrap();
    marker
        .persist(control.path().join("resume-update"))
        .unwrap();
    let result: serde_json::Value =
        serde_json::from_slice(&read_marker("resume-update-result.json").await).unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "request": request,
            "report": {"resumed": 1, "advanced": 0, "pending_sessions": []}
        })
    );
    let saved = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let saved = state
                .session_store
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            if saved.last_run_status().as_deref() == Some("completed") {
                return saved;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the successor ran the continuation from persisted history");
    assert_eq!(saved.created_at, session.created_at);
    assert_eq!(
        saved.messages.last().unwrap().content,
        "Continued using the saved result."
    );
    let new_owner = state
        .session_store
        .inspect_actor(&session.id)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_ne!(new_owner.run_id, checkpoint.fence.run_id);
    assert_eq!(new_owner.lease_epoch, checkpoint.fence.lease_epoch + 1);
    assert!(
        bamboo_storage::SessionStoreV2::root_runtime_matches_activation(&saved, &new_owner.fence())
            .unwrap()
    );
    let report = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(report).unwrap(),
        serde_json::json!({"resumed": 0, "advanced": 1, "pending_sessions": []})
    );
    let still_waiting = state
        .session_store
        .load_session(&waiting.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_waiting.pending_question, Some(question));
    assert!(still_waiting.has_runtime_wait());
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);

    std::fs::File::create_new(control.path().join("stop")).unwrap();
    managed.await.unwrap().unwrap();
    state.shutdown().await;
}
