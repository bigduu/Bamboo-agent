use super::{managed_shutdown::ManagedShutdown, planned_update_handoff};
use crate::app_state::AppState;
use actix_web::{App, HttpServer};
use bamboo_agent_core::{FunctionCall, Message, Session, ToolCall, ToolSchema};
use bamboo_domain::{
    new_root_planned_update_lease_owner, ActorActivationClaim, ActorActivationFence,
    ActorActivationFinish, ActorActivationStatus, ActorDirectoryPort, AgentRuntimeState,
    AgentStatusState, PendingQuestion, PlanModeState, PlanModeStatus, ProviderModelRef,
    RootActorRuntimeWrite, Storage, SuspensionState,
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
    let state = fixture_openai_host(&workspace, probe.clone()).await;
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

struct ProviderProbe {
    calls: AtomicUsize,
    continue_model: Semaphore,
    reject: bool,
    expected_read_result: String,
}

struct InterruptedStreamProbe {
    in_stream: Arc<Semaphore>,
    calls: AtomicUsize,
    path: std::path::PathBuf,
}

#[async_trait::async_trait]
impl LLMProvider for InterruptedStreamProbe {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        use futures::StreamExt;

        assert_eq!(model, MODEL);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let tool = ToolCall {
            id: if call == 0 {
                "original-call"
            } else {
                "discarded-tool"
            }
            .into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: serde_json::json!({"file_path": self.path}).to_string(),
            },
        };
        if call == 0 {
            return Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::ToolCalls(vec![tool])),
                Ok(LLMChunk::Done),
            ])));
        }
        assert_eq!(
            call, 1,
            "the predecessor must not dispatch a replacement response"
        );
        let in_stream = self.in_stream.clone();
        let response = futures::stream::iter([
            Ok(LLMChunk::Token("discarded reply".into())),
            Ok(LLMChunk::ReasoningToken("discarded reasoning".into())),
            Ok(LLMChunk::ToolCalls(vec![tool])),
            Ok(LLMChunk::ResponseId("discarded-response-id".into())),
        ]);
        Ok(Box::pin(response.chain(futures::stream::once(
            async move {
                in_stream.add_permits(1);
                std::future::pending::<Result<LLMChunk, LLMError>>().await
            },
        ))))
    }
}

async fn fixture_openai_host(
    workspace: &std::path::Path,
    probe: Arc<dyn LLMProvider>,
) -> actix_web::web::Data<AppState> {
    let mut config = Config::from_data_dir(Some(workspace.to_owned()));
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
        workspace.to_owned(),
        config,
        probe.clone(),
    )
    .await
    .unwrap()
    .into_http_data();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([("openai".into(), probe)]),
        "openai".into(),
    ));
    state
}

#[actix_web::test]
async fn immediate_update_discards_partial_reply_and_resumes_from_saved_tool_history() {
    use actix_web::test;

    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().canonicalize().unwrap();
    let path = workspace.join("immediate-update-read.txt");
    std::fs::write(&path, "already finished").unwrap();
    let probe = Arc::new(InterruptedStreamProbe {
        in_stream: Arc::new(Semaphore::new(0)),
        calls: AtomicUsize::new(0),
        path,
    });
    let state = fixture_openai_host(&workspace, probe.clone()).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let mut session = Session::new("immediate-update-live-root", MODEL);
    session.title_generated = true;
    session.set_workspace_path_meta(workspace.to_string_lossy());
    session.set_provider_name("openai");
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
        "explicit".into(),
    );
    session.add_message(Message::user("Read the fixture and report what you found."));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    state.storage.save_session(&session).await.unwrap();
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{}", session.id))
            .set_json(serde_json::json!({"model":MODEL}))
            .to_request(),
    )
    .await;
    let code = response.status();
    let response: serde_json::Value = test::read_body_json(response).await;
    assert!(code.is_success(), "{}: {response}", session.id);
    tokio::time::timeout(Duration::from_secs(12), probe.in_stream.acquire())
        .await
        .expect("the unfinished provider stream was polled")
        .unwrap()
        .forget();

    let admission = state
        .session_activation_router
        .hold_admissions_for_immediate_update()
        .await
        .unwrap();
    assert_eq!(admission.outstanding_sessions().await, [session.id.clone()]);
    let checkpoint = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Some(root) = admission.parked_roots().await.pop() {
                return root;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the Root parked without waiting for the stream to complete");
    let request = uuid::Uuid::new_v4();
    let mut handoff = None;
    admission
        .commit_parked_roots(|roots| {
            assert_eq!(roots, std::slice::from_ref(&checkpoint));
            handoff = Some(planned_update_handoff::publish(&workspace, request, roots)?);
            Ok(())
        })
        .await
        .unwrap();

    let saved = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let owner = state
                .session_store
                .inspect_actor(&session.id)
                .await
                .unwrap()
                .activation
                .unwrap();
            if owner.status == ActorActivationStatus::Succeeded {
                assert_eq!(owner.fence(), checkpoint.fence);
                return state
                    .session_store
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the old Root released its durable writer");
    assert!(checkpoint.matches_checkpoint(&saved));
    assert_eq!(saved.messages.len(), 3);
    assert_eq!(saved.messages[0].id, session.messages[0].id);
    assert_eq!(
        saved.messages[2].tool_call_id.as_deref(),
        Some("original-call")
    );
    assert!(saved.messages[2].content.contains("already finished"));
    assert!(!serde_json::to_string(&saved).unwrap().contains("discarded"));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    state.shutdown().await;
    handoff.unwrap().authorize().unwrap();
    drop(app);
    drop(state);

    let probe = Arc::new(ProviderProbe {
        expected_read_result: saved.messages[2].content.clone(),
        ..ProviderProbe::new()
    });
    let successor = fixture_openai_host(&workspace, probe.clone()).await;
    probe.continue_model.add_permits(1);
    planned_update_handoff::resume(&successor, request)
        .await
        .unwrap();
    let resumed = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let saved = successor
                .session_store
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            if bamboo_storage::SessionStoreV2::root_completed_planned_update(
                &saved,
                &checkpoint.fence,
            ) {
                return saved;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the successor completed from the saved transcript");
    assert_eq!(resumed.created_at, session.created_at);
    assert_eq!(resumed.messages.len(), saved.messages.len() + 1);
    assert_eq!(
        ParkedUpdateRoot::hash_transcript(&resumed.messages[..saved.messages.len()]).unwrap(),
        checkpoint.transcript_sha256
    );
    assert_eq!(
        resumed.messages.last().unwrap().content,
        "Continued using the saved result."
    );
    planned_update_handoff::resume(&successor, request)
        .await
        .unwrap();
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    successor.shutdown().await;
}

impl ProviderProbe {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            continue_model: Semaphore::new(0),
            reject: false,
            expected_read_result: "already finished".into(),
        }
    }
}

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
        let mut results = messages
            .iter()
            .filter(|message| message.tool_call_id.as_deref() == Some("original-call"));
        assert_eq!(
            results.next().map(|message| message.content.as_str()),
            Some(self.expected_read_result.as_str())
        );
        assert!(results.next().is_none());
        assert!(messages
            .iter()
            .all(|message| !message.content.contains("discarded reply")));
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.continue_model.acquire().await.unwrap().forget();
        if self.reject {
            return Err(LLMError::RequestFailed(
                "The fixture provider rejected this continuation.".into(),
            ));
        }
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Continued using the saved result.".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn persist_parked_root(
    store: &bamboo_storage::SessionStoreV2,
    workspace: &std::path::Path,
    id: &str,
) -> (Session, ParkedUpdateRoot) {
    let mut session = Session::new(id, MODEL);
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
    persist_owner_runtime(store, &session, owner.fence()).await;
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
    (session, checkpoint)
}

async fn persist_owner_runtime(
    store: &bamboo_storage::SessionStoreV2,
    session: &Session,
    fence: ActorActivationFence,
) {
    store
        .save_root_actor_runtime(
            &RootActorRuntimeWrite {
                fence,
                created_at: session.created_at,
                conversation_generation: session.conversation_generation_observation(),
            },
            session,
            false,
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
}

fn set_runtime_status(session: &mut Session, status: AgentStatusState) {
    let mut runtime = session.agent_runtime_state.take().unwrap();
    runtime.status = status;
    runtime.suspension = None;
    session.metadata.insert(
        "agent.runtime.state".into(),
        serde_json::to_string(&runtime).unwrap(),
    );
    session.agent_runtime_state = Some(runtime);
}

async fn persist_initializing_projection(
    store: &bamboo_storage::SessionStoreV2,
    session: &mut Session,
    fence: ActorActivationFence,
) {
    session.set_last_run_status("running");
    session.metadata.remove("runtime.suspend_reason");
    set_runtime_status(session, AgentStatusState::Initializing);
    persist_owner_runtime(store, session, fence).await;
}

async fn fixture_successor(
    workspace: std::path::PathBuf,
    probe: Arc<ProviderProbe>,
) -> actix_web::web::Data<AppState> {
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
        HashMap::from([(PROVIDER.into(), probe as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    state
}

#[actix_web::test]
async fn planned_update_desktop_retries_an_expired_claim_before_startup() {
    assert_planned_update_desktop_retries_expired_claim(false).await;
}

#[actix_web::test]
async fn planned_update_desktop_retries_an_expired_startup_before_dispatch() {
    assert_planned_update_desktop_retries_expired_claim(true).await;
}

async fn assert_planned_update_desktop_retries_expired_claim(startup_saved: bool) {
    let dir = tempfile::tempdir().unwrap();
    let workspace = std::fs::canonicalize(dir.path()).unwrap();
    let store = bamboo_storage::SessionStoreV2::new(workspace.clone())
        .await
        .unwrap();
    let (session, checkpoint) =
        persist_parked_root(&store, &workspace, "planned-update-root").await;

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
    let now = Utc::now();
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
        .unwrap()
        .authorize()
        .unwrap();

    let probe = Arc::new(ProviderProbe::new());
    let state = fixture_successor(workspace, probe.clone()).await;
    let mut before = state
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
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);

    // Leave the durable state of a replacement that died before it could
    // dispatch. The receipt still points at the original parked member.
    let claimed_at = if startup_saved {
        Utc::now()
    } else {
        Utc::now() - chrono::Duration::minutes(1)
    };
    let expires = claimed_at + chrono::Duration::seconds(4);
    let abandoned = state
        .session_store
        .claim_root_activation_observed_after(
            &ActorActivationClaim {
                actor_id: session.id.clone(),
                run_id: "crashed-before-dispatch".into(),
                lease_owner: new_root_planned_update_lease_owner(&checkpoint.fence),
                lease_expires_at: expires,
                inbox_generation: 0,
                placement_ref: None,
                now: claimed_at,
            },
            &before,
            Some(&checkpoint.fence),
        )
        .await
        .unwrap();
    let abandoned = state
        .session_store
        .start_activation(&abandoned.fence(), claimed_at)
        .await
        .unwrap();
    assert_eq!(abandoned.lease_epoch, checkpoint.fence.lease_epoch + 1);
    if startup_saved {
        persist_initializing_projection(&state.session_store, &mut before, abandoned.fence()).await;
        let saved = state
            .session_store
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            bamboo_storage::SessionStoreV2::root_has_unstarted_planned_update_projection(
                &saved, &abandoned,
            )
            .unwrap()
        );
        if let Ok(remaining) = (expires - Utc::now()).to_std() {
            tokio::time::sleep(remaining + Duration::from_millis(1)).await;
        }
        let mut wake_candidate = saved.clone();
        set_runtime_status(&mut wake_candidate, AgentStatusState::Idle);
        assert!(
            state
                .session_store
                .save_session(&wake_candidate)
                .await
                .is_err(),
            "an ordinary pre-claim Inbox write must not clear the reserved startup"
        );
        let saved = state
            .session_store
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            bamboo_storage::SessionStoreV2::root_has_unstarted_planned_update_projection(
                &saved, &abandoned,
            )
            .unwrap()
        );
        let now = Utc::now();
        assert!(matches!(
            state
                .session_store
                .claim_root_activation_observed_after(
                    &ActorActivationClaim {
                        actor_id: session.id.clone(),
                        run_id: "ordinary-wake".into(),
                        lease_owner: "unrelated".into(),
                        lease_expires_at: now + chrono::Duration::minutes(1),
                        inbox_generation: 0,
                        placement_ref: None,
                        now,
                    },
                    &saved,
                    None,
                )
                .await,
            Err(bamboo_domain::ActorDirectoryError::Busy)
        ));
    }

    let control = tempfile::tempdir().unwrap();
    let managed = ManagedShutdown::new(control.path().to_path_buf()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server = HttpServer::new(App::new)
        .disable_signals()
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
    let managed_state = state.clone();
    let managed = actix_web::rt::spawn(async move {
        let mut committed = None;
        managed
            .run(server, Some(managed_state), &mut committed)
            .await
    });
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
    assert!(read_marker("planned-update-v1").await.is_empty());
    assert!(read_marker("immediate-update-v1").await.is_empty());
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    let mut marker = tempfile::NamedTempFile::new_in(control.path()).unwrap();
    write!(
        marker,
        "{}",
        serde_json::json!({"version":1,"request_id":request,"sequence":1,"command":"resume"})
    )
    .unwrap();
    marker
        .persist(control.path().join("planned-update-command.json"))
        .unwrap();
    let result: serde_json::Value =
        serde_json::from_slice(&read_marker("planned-update-result.json").await).unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "version": 1,
            "request_id": request,
            "sequence": 1,
            "status": "resumed",
            "report": {"resumed": 0, "advanced": 0, "pending_sessions": [&session.id]}
        })
    );
    probe.continue_model.add_permits(1);
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
    assert_eq!(new_owner.lease_epoch, abandoned.lease_epoch + 1);
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
    let mut completed = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let owner = state
                .session_store
                .inspect_actor(&session.id)
                .await
                .unwrap()
                .activation
                .unwrap();
            if owner.status == ActorActivationStatus::Succeeded {
                let saved = state
                    .session_store
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    bamboo_storage::SessionStoreV2::root_completed_planned_update(
                        &saved,
                        &checkpoint.fence,
                    )
                );
                return saved;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the real continuation records its exact completed handoff");
    let now = Utc::now();
    let ordinary = state
        .session_store
        .claim_root_activation_observed_after(
            &ActorActivationClaim {
                actor_id: session.id.clone(),
                run_id: "ordinary-next-run".into(),
                lease_owner: "ordinary-next-owner".into(),
                lease_expires_at: now + chrono::Duration::minutes(1),
                inbox_generation: 0,
                placement_ref: None,
                now,
            },
            &completed,
            None,
        )
        .await
        .unwrap();
    state
        .session_store
        .start_activation(&ordinary.fence(), now)
        .await
        .unwrap();
    completed.add_message(Message::assistant("A later ordinary run.", None));
    persist_owner_runtime(&state.session_store, &completed, ordinary.fence()).await;
    state
        .session_store
        .finish_activation(
            &ordinary.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
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
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);

    std::fs::File::create_new(control.path().join("stop")).unwrap();
    managed.await.unwrap().unwrap();
    state.shutdown().await;
}

#[actix_web::test]
async fn planned_update_keeps_uncertain_dispatch_and_unrelated_newer_progress_pending() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().canonicalize().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(workspace.clone())
        .await
        .unwrap();
    let (session, checkpoint) = persist_parked_root(&store, &workspace, "resume-attribution").await;
    drop(store);
    let request = uuid::Uuid::new_v4();
    planned_update_handoff::publish(&workspace, request, std::slice::from_ref(&checkpoint))
        .unwrap()
        .authorize()
        .unwrap();
    let probe = Arc::new(ProviderProbe::new());
    let state = fixture_successor(workspace, probe.clone()).await;
    let store = &state.session_store;
    let mut saved = store.load_session(&session.id).await.unwrap().unwrap();
    let now = Utc::now();
    let mut claim = ActorActivationClaim {
        actor_id: session.id.clone(),
        run_id: "startup-without-dispatch".into(),
        lease_owner: new_root_planned_update_lease_owner(&checkpoint.fence),
        lease_expires_at: now + chrono::Duration::minutes(1),
        inbox_generation: 0,
        placement_ref: None,
        now,
    };
    let owner = store
        .claim_root_activation_observed_after(&claim, &saved, Some(&checkpoint.fence))
        .await
        .unwrap();
    store.start_activation(&owner.fence(), now).await.unwrap();
    persist_initializing_projection(store, &mut saved, owner.fence()).await;

    let expected =
        serde_json::json!({"resumed": 0, "advanced": 0, "pending_sessions": [&session.id]});
    let report = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(report).unwrap(), expected);

    let dispatch = store
        .checkpoint_activation(&owner.fence(), Utc::now(), 0)
        .await
        .unwrap();
    assert_eq!(dispatch.checkpoint_revision, 1);
    store
        .finish_activation(&owner.fence(), Utc::now(), ActorActivationFinish::Failed)
        .await
        .unwrap();
    let report = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(report).unwrap(), expected);

    claim.run_id = "unrelated-execution".into();
    claim.lease_owner = "unrelated-owner".into();
    let ordinary = store
        .claim_root_activation_observed_after(&claim, &saved, None)
        .await
        .unwrap();
    store
        .start_activation(&ordinary.fence(), now)
        .await
        .unwrap();
    saved.add_message(Message::assistant("Unrelated Assistant progress.", None));
    saved.set_last_run_status("completed");
    set_runtime_status(&mut saved, AgentStatusState::Completed);
    persist_owner_runtime(store, &saved, ordinary.fence()).await;
    store
        .finish_activation(
            &ordinary.fence(),
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let report = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(report).unwrap(), expected);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    state.shutdown().await;
}

#[actix_web::test]
async fn planned_update_recognizes_a_durable_provider_rejection_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().canonicalize().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(workspace.clone())
        .await
        .unwrap();
    let (session, checkpoint) = persist_parked_root(&store, &workspace, "resume-rejected").await;
    drop(store);
    let request = uuid::Uuid::new_v4();
    planned_update_handoff::publish(&workspace, request, std::slice::from_ref(&checkpoint))
        .unwrap()
        .authorize()
        .unwrap();
    let probe = Arc::new(ProviderProbe {
        reject: true,
        ..ProviderProbe::new()
    });
    let state = fixture_successor(workspace, probe.clone()).await;
    let _ = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    probe.continue_model.add_permits(1);
    let saved = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let entry = state
                .session_store
                .inspect_actor(&session.id)
                .await
                .unwrap();
            let activation = entry.activation.unwrap();
            if activation.status == ActorActivationStatus::Failed {
                assert_eq!(activation.lease_epoch, checkpoint.fence.lease_epoch + 1);
                assert_eq!(activation.checkpoint_revision, 1);
                return state
                    .session_store
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the actual provider rejection has a durable terminal Actor");
    assert_eq!(saved.last_run_status().as_deref(), Some("error"));
    assert!(
        bamboo_storage::SessionStoreV2::root_completed_planned_update(&saved, &checkpoint.fence)
    );
    let report = planned_update_handoff::resume(&state, request)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(report).unwrap(),
        serde_json::json!({"resumed": 0, "advanced": 1, "pending_sessions": []})
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    state.shutdown().await;
}
