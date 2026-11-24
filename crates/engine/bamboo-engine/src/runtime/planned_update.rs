//! Preparing a running local Root for a planned host update.

use bamboo_agent_core::{AgentError, Session};
use bamboo_domain::{AgentStatusState, SessionKind};
use tokio_util::sync::CancellationToken;

use super::config::AgentLoopConfig;
use crate::session_activation::ParkedUpdateRoot;
use crate::session_app::provider_model::session_effective_model_ref;

pub(crate) const SUSPEND_REASON: &str = "planned_update";

/// Called between complete rounds. Save through the old exact writer before
/// reporting a parked round, then keep the original loop alive for an abort.
pub(super) async fn pause_before_model_round(
    session: &mut Session,
    config: &AgentLoopConfig,
    cancel: &CancellationToken,
) -> Result<bool, AgentError> {
    let Some(router) = config.session_update_router.as_ref() else {
        return Ok(false);
    };
    let Some(update) = router.preparing_update() else {
        return Ok(false);
    };
    if cancel.is_cancelled() {
        return Err(AgentError::Cancelled);
    }
    if session.kind != SessionKind::Root
        || !session.authority_identity.is_ordinary()
        || crate::ticket_runtime::is_managed_work_root(session)
        || session.has_runtime_wait()
        || config.run_budget != Default::default()
        || config.run_deadline.is_some()
        || config.effective_counted_loop().is_some()
        || config.paced_loop_turn.is_some()
    {
        return Ok(false);
    }
    let Some(persistence) = config.persistence.as_ref() else {
        return Ok(false);
    };
    let Some(writer) = persistence.root_actor_writer() else {
        return Ok(false);
    };
    if writer.created_at != session.created_at
        || writer.fence.actor_id.as_str() != session.id
        || config.guidance_active_run_id.as_deref() != Some(&writer.fence.run_id)
    {
        return Ok(false);
    }
    let Some(mut model) = session_effective_model_ref(session).filter(|target| {
        config.provider_name.as_deref() == Some(target.provider.as_str())
            && config.model_name.as_deref() == Some(target.model.as_str())
    }) else {
        return Ok(false);
    };
    model.reasoning_effort = config.reasoning_effort;
    let Ok(conversation_reset_sha256) = session.conversation_reset_sha256() else {
        return Ok(false);
    };
    let Some(message) = session.messages.last() else {
        return Ok(false);
    };
    let Ok(transcript_sha256) = ParkedUpdateRoot::hash_transcript(&session.messages) else {
        return Ok(false);
    };
    let parked = ParkedUpdateRoot {
        update_id: update,
        session_id: session.id.clone(),
        created_at: session.created_at,
        fence: writer.fence.clone(),
        conversation_reset_sha256,
        transcript_sha256,
        root_tool_authority_revision: session.root_tool_authority_revision,
        project_id: session.project_id_meta(),
        workspace_path: session.workspace_path_meta(),
        message_id: message.id.clone(),
        message_count: session.messages.len(),
        model,
    };
    if !crate::session_app::truncation::unresolved_tool_call_ids(&session.messages).is_empty() {
        return Ok(false);
    }
    if let Err(error) = persistence.checkpoint_runtime_session(session).await {
        tracing::warn!(
            session_id = %session.id,
            %error,
            "Planned update round checkpoint was not confirmed"
        );
        return Ok(false);
    }
    if !matches_checkpoint(&parked, session) || session.has_runtime_wait() {
        return Ok(false);
    }
    router.park_root_for_update(parked, cancel).await
}

/// Match the persisted history against the planned update controller's exact
/// prepared member. The successor still needs to acquire the genuine writer.
pub(crate) fn matches_checkpoint(root: &ParkedUpdateRoot, session: &Session) -> bool {
    session.kind == SessionKind::Root
        && session.authority_identity.is_ordinary()
        && !crate::ticket_runtime::is_managed_work_root(session)
        && session.id == root.session_id
        && session.created_at == root.created_at
        && session.root_tool_authority_revision == root.root_tool_authority_revision
        && session.project_id_meta() == root.project_id
        && session.workspace_path_meta() == root.workspace_path
        && session
            .conversation_reset_sha256()
            .is_ok_and(|hash| hash == root.conversation_reset_sha256)
        && session.messages.len() == root.message_count
        && session
            .messages
            .last()
            .is_some_and(|message| message.id == root.message_id)
        && ParkedUpdateRoot::hash_transcript(&session.messages)
            .is_ok_and(|hash| hash == root.transcript_sha256)
        && session_effective_model_ref(session)
            .is_some_and(|model| model.to_pair() == root.model.to_pair())
}

pub(crate) fn prepare_resuming_session(
    root: &ParkedUpdateRoot,
    saved: &Session,
) -> Option<Session> {
    use crate::runtime::runner::state_bridge::{read_runtime_state, write_runtime_state};

    if !matches_checkpoint(root, saved)
        || saved.last_run_status().as_deref() != Some("suspended")
        || saved
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str)
            != Some(SUSPEND_REASON)
    {
        return None;
    }
    let mut runtime = read_runtime_state(saved)?;
    if runtime.status != AgentStatusState::Suspended
        || runtime
            .suspension
            .as_ref()
            .map(|suspension| suspension.reason.as_str())
            != Some(SUSPEND_REASON)
    {
        return None;
    }
    let mut resumed = saved.clone();
    runtime.status = AgentStatusState::Initializing;
    runtime.suspension = None;
    write_runtime_state(&mut resumed, &runtime);
    resumed.metadata.remove("runtime.suspend_reason");
    if resumed.has_runtime_wait() {
        return None;
    }
    resumed.reasoning_effort = root.model.reasoning_effort;
    resumed.set_last_run_status("running");
    resumed.clear_last_run_error();
    Some(resumed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{FunctionCall, Message, Role, ToolCall};
    use bamboo_domain::{ActorActivationClaim, ActorDirectoryPort, RootActorRuntimeWrite, Storage};
    use bamboo_storage::{LockedSessionStore, SessionStoreV2};
    use chrono::{Duration, Utc};
    use std::sync::Arc;

    #[tokio::test]
    async fn update_parks_only_after_checkpointing_the_completed_tool_round() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(dir.path().into()).await.unwrap());
        let router = crate::session_activation::SessionActivationRouter::new();
        let mut session = Session::new("update-fixture", "local-model");
        session.set_provider_name("local-provider");
        session.add_message(Message::user("Read the fixture."));
        session.set_last_run_status("running");
        store.save_session(&session).await.unwrap();

        let now = Utc::now();
        let actor = store
            .claim_activation(&ActorActivationClaim {
                actor_id: session.id.as_str().into(),
                run_id: "owned-root".into(),
                lease_owner: "local-host".into(),
                lease_expires_at: now + Duration::minutes(1),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        store.start_activation(&actor.fence(), now).await.unwrap();
        let repository = crate::SessionRepository::new(
            Default::default(),
            store.clone(),
            Arc::new(LockedSessionStore::new(store.clone())),
        );
        let persistence = Arc::new(
            repository
                .with_root_actor_directory(store.clone())
                .bind_root_response_writer(RootActorRuntimeWrite {
                    fence: actor.fence(),
                    created_at: session.created_at,
                    conversation_generation: session.conversation_generation_observation(),
                })
                .unwrap(),
        );
        let registration = router
            .register_session_run(&session, "owned-root")
            .await
            .unwrap();
        let config = AgentLoopConfig {
            guidance_active_run_id: Some("owned-root".into()),
            provider_name: Some("local-provider".into()),
            model_name: Some("local-model".into()),
            session_update_router: Some(router.clone()),
            persistence: Some(persistence),
            ..Default::default()
        };
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "tool-1".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Read".into(),
                    arguments: "{}".into(),
                },
            }]),
        ));
        let result = Message::tool_result("tool-1", "durable tool result");
        let result_id = result.id.clone();
        session.add_message(result);

        let update = router.hold_admissions_for_update().await.unwrap();
        let task = tokio::spawn(async move {
            assert!(
                !pause_before_model_round(&mut session, &config, &CancellationToken::new())
                    .await
                    .unwrap()
            );
            session
        });
        let parked = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(root) = update.parked_roots().await.into_iter().next() {
                    return root;
                }
                assert!(
                    !task.is_finished(),
                    "the owned Root did not reach a checkpoint"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(parked.fence.run_id, "owned-root");
        let saved = store
            .load_session(&parked.session_id)
            .await
            .unwrap()
            .unwrap();
        let tail = saved.messages.last().unwrap();
        assert_eq!(tail.role, Role::Tool);
        assert_eq!(tail.id, result_id);
        assert!(matches_checkpoint(&parked, &saved));
        assert!(prepare_resuming_session(&parked, &saved).is_none());
        assert!(!task.is_finished());
        let failed_write = update
            .commit_parked_roots(|roots| {
                assert_eq!(roots, std::slice::from_ref(&parked));
                Err(std::io::Error::other("could not publish the handoff"))
            })
            .await;
        assert!(failed_write.is_err());
        assert!(!task.is_finished());

        drop(update);
        let resumed = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.created_at, parked.created_at);
        assert_eq!(resumed.messages.last().unwrap().id, result_id);
        assert!(router.owns_run(&resumed.id, "owned-root").await);
        registration.abandon().await;
    }
}
