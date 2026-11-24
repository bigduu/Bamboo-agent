use crate::app_state::AppState;
use bamboo_agent_core::{FunctionCall, Message, Session, ToolCall, ToolSchema};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, AgentRuntimeState,
    AgentStatusState, ProviderModelRef, Storage, SuspensionState,
};
use bamboo_engine::session_activation::ParkedUpdateRoot;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PROVIDER: &str = "update-fixture-provider";
const MODEL: &str = "update-fixture-model";

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
    assert!(!state
        .child_completion_coordinator
        .resume_planned_update_root(&checkpoint)
        .await
        .unwrap());
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
}
