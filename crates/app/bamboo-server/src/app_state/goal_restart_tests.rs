use super::{TerminalRecovery, Utc};
use crate::app_state::AppState;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{FunctionCall, Message, Session, ToolCall, ToolSchema};
use bamboo_domain::{ActorActivationClaim, ActorDirectoryPort};
use bamboo_engine::runtime::goal_restart::eligible_checkpoint;
use bamboo_engine::runtime::goal_state::{read_goal_state, GoalRuntimeStatus};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PROVIDER: &str = "goal-restart-test-provider";
const MODEL: &str = "goal-restart-test-model";
const OLD_RUN: &str = "interrupted-goal-owner";
const OBJECTIVE: &str = "Finish the verified local fixture";
const READY: &str = "Continue from the committed Goal checkpoint";

#[derive(Default)]
struct Probe(AtomicUsize);

#[async_trait::async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        let chunk = if tools
            .iter()
            .any(|tool| tool.function.name == "report_gold_evaluation")
        {
            LLMChunk::ToolCalls(vec![ToolCall {
                id: "verify-resumed-goal".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "report_gold_evaluation".into(),
                    arguments: json!({
                        "decision": "achieved",
                        "confidence": "high",
                        "reasoning": "The existing fixture was verified."
                    })
                    .to_string(),
                },
            }])
        } else {
            assert!(messages.iter().any(|message| message.content == READY));
            self.0.fetch_add(1, Ordering::SeqCst);
            LLMChunk::Token("Verified the fixture after resuming.".into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
async fn goal_restart_uses_one_expired_owned_checkpoint_and_skips_newer_input() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = std::fs::canonicalize(directory.path()).unwrap();
    let store = bamboo_storage::SessionStoreV2::new(workspace.clone())
        .await
        .unwrap();
    let mut ids = Vec::new();

    for (id, newer_input) in [("restart-able-goal", false), ("restart-stale-goal", true)] {
        let mut session = Session::new(id, MODEL);
        session.title_generated = true;
        session.set_workspace_path_meta(workspace.to_string_lossy());
        session.metadata.insert("provider".into(), PROVIDER.into());
        session.set_last_run_status("running");
        session.metadata.insert(
            "gold_config".into(),
            json!({
                "enabled": true,
                "auto_continue_enabled": true,
                "resume_after_restart": true,
                "goal": OBJECTIVE,
            })
            .to_string(),
        );
        let goal = bamboo_engine::runtime::goal_state::ensure_goal_state(&session, OBJECTIVE);
        let mut goal = goal;
        goal.continuation_count = 1;
        let goal_created_at = goal.created_at.clone();
        bamboo_engine::runtime::goal_state::write_goal_state(&mut session, goal);
        let mut continuation = Message::user(READY);
        continuation.metadata = Some(json!({"runtime_kind":"goal_continue","hidden_from_ui":true}));
        session.metadata.insert(
            "goal.restart_checkpoint".into(),
            json!({
                "run_id": OLD_RUN,
                "lease_epoch": 1,
                "session_created_at": session.created_at,
                "goal_created_at": goal_created_at,
                "root_tool_authority_revision": session.root_tool_authority_revision,
                "message_id": continuation.id,
            })
            .to_string(),
        );
        session.add_message(continuation);
        if newer_input {
            session.add_message(Message::user("Use the newer instruction first."));
        }
        store.save_session(&session).await.unwrap();
        let now = Utc::now();
        store
            .claim_activation(&ActorActivationClaim {
                actor_id: id.into(),
                run_id: OLD_RUN.into(),
                lease_owner: "predecessor-host".into(),
                lease_expires_at: now + chrono::Duration::milliseconds(150),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        ids.push((id, session.created_at));
    }
    let cutoff = Utc::now();
    drop(store);

    let probe = Arc::new(Probe::default());
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
    let recovery = TerminalRecovery {
        store: state.session_store.clone(),
        cutoff,
        cache: Some(state.sessions.clone()),
        goal_resume: Some(state.child_completion_coordinator.clone()),
    };
    let before = recovery
        .store
        .load_session(ids[0].0)
        .await
        .unwrap()
        .unwrap();
    assert!(eligible_checkpoint(&before, cutoff).is_some());
    recovery.inspect(ids[1].0).await;
    recovery.inspect(ids[0].0).await;
    let saved = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let session = recovery
                .store
                .load_session(ids[0].0)
                .await
                .unwrap()
                .unwrap();
            if read_goal_state(&session)
                .is_some_and(|goal| goal.status == GoalRuntimeStatus::Complete)
            {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("checkpoint must execute through the reopened Host");
    assert_eq!(saved.created_at, ids[0].1);
    assert_eq!(read_goal_state(&saved).unwrap().continuation_count, 1);
    assert_eq!(saved.metadata["goal.restart_checkpoint"], "null");
    let active = recovery
        .store
        .inspect_actor(ids[0].0)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_ne!(active.run_id, OLD_RUN);
    assert_eq!(active.lease_epoch, 2);
    recovery.inspect(ids[0].0).await;
    recovery.inspect(ids[1].0).await;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    let stale = recovery
        .store
        .inspect_actor(ids[1].0)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_eq!(stale.run_id, OLD_RUN);
}
