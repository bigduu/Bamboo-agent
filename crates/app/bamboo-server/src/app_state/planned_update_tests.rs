use crate::app_state::AppState;
use bamboo_agent_core::{FunctionCall, Message, Session, ToolCall, ToolSchema};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorActivationStatus, ActorDirectoryPort,
    AgentRuntimeState, AgentStatusState, ProviderModelRef, Storage, SuspensionState,
};
use bamboo_engine::session_activation::ParkedUpdateRoot;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use chrono::Utc;
use std::collections::HashMap;
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
        assert_eq!(saved.host_run_id(), Some(root.run_id.as_str()));
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
                assert_eq!(owner.run_id, root.run_id);
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
async fn planned_update_resume_uses_the_saved_tool_result_under_a_new_owner_once() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = std::fs::canonicalize(dir.path()).unwrap();
    let store = bamboo_storage::SessionStoreV2::new(workspace.clone())
        .await
        .unwrap();
    let mut session = Session::new("planned-update-root", MODEL);
    session.title_generated = true;
    session.set_workspace_path_meta(workspace.to_string_lossy());
    session.set_provider_name(PROVIDER);
    session.set_host_run_id("previous-root");
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
    store
        .finish_activation(&owner.fence(), Utc::now(), ActorActivationFinish::Succeeded)
        .await
        .unwrap();
    let checkpoint = ParkedUpdateRoot {
        update_id: uuid::Uuid::new_v4(),
        session_id: session.id.clone(),
        created_at: session.created_at,
        run_id: owner.run_id,
        lease_epoch: owner.lease_epoch,
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
    drop(store);

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
            .unwrap();
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
    assert_eq!(before.host_run_id(), Some(checkpoint.run_id.as_str()));
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
    stale_actor.lease_epoch += 1;
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
    assert_eq!(
        still_previous.activation.unwrap().lease_epoch,
        checkpoint.lease_epoch
    );
    assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    assert!(state
        .child_completion_coordinator
        .resume_planned_update_root(&checkpoint)
        .await
        .unwrap());
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
    assert_ne!(new_owner.run_id, checkpoint.run_id);
    assert_eq!(new_owner.lease_epoch, checkpoint.lease_epoch + 1);
    assert_eq!(saved.host_run_id(), Some(new_owner.run_id.as_str()));
    assert!(!state
        .child_completion_coordinator
        .resume_planned_update_root(&checkpoint)
        .await
        .unwrap());
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
}
