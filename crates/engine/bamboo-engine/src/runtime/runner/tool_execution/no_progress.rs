use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{AgentRuntimeState, SessionKind};

pub(crate) fn quiet_poll_guard_active(
    session: &Session,
    runtime: &AgentRuntimeState,
    config: &AgentLoopConfig,
) -> bool {
    session.kind == SessionKind::Root && !runtime.no_human_approver
        && session.pending_question.is_none()
        && !crate::runtime::loop_control::suspension::active(config)
        // Ending an owned local task must not inadvertently stop its command.
        && bamboo_tools::LocalTaskResources::current().is_none()
}

#[cfg(test)]
pub(crate) async fn pause_for_quiet_polling(
    session: &mut Session,
    runtime: &mut AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
) -> Result<bool, AgentError> {
    if !quiet_poll_guard_active(session, runtime, config) {
        return Ok(false);
    }
    pause_with_question(
        session,
        runtime,
        event_tx,
        config,
        Some(crate::session_app::no_progress::QUIET_POLL_QUESTION_TEXT),
    )
    .await
}

/// Pause only a Human-controlled Root, after every real tool result is paired.
/// Publish the existing question event only after its checkpoint succeeds.
#[cfg(test)]
pub(crate) async fn pause_for_no_progress(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
) -> Result<bool, AgentError> {
    pause_with_question(session, runtime_state, event_tx, config, None).await
}

#[cfg(test)]
async fn pause_with_question(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    question: Option<&str>,
) -> Result<bool, AgentError> {
    if crate::runtime::loop_control::suspension::active(config) {
        return Err(AgentError::Tool(
            "managed loop cannot pause for progress clarification".into(),
        ));
    }
    if session.kind != SessionKind::Root
        || runtime_state.no_human_approver
        || session.pending_question.is_some()
    {
        return Ok(false);
    }
    if let Some(text) = question {
        crate::session_app::no_progress::create_question_with_text(session, runtime_state, text);
    } else {
        crate::session_app::no_progress::create_question(session, runtime_state);
    }
    if let Some(persistence) = config.persistence.as_ref() {
        persistence
            .save_runtime_session(session)
            .await
            .map_err(|error| {
                AgentError::Tool(format!("Progress pause could not be saved: {error}"))
            })?;
    }
    let pending = session.pending_question.as_ref().expect("created question");
    let _ = event_tx
        .send(AgentEvent::message_appended(
            &session.id,
            session.messages.last().expect("question message"),
        ))
        .await;
    let _ = event_tx
        .send(AgentEvent::NeedClarification {
            question: pending.question.clone(),
            options: Some(pending.options.clone()),
            tool_call_id: Some(pending.tool_call_id.clone()),
            tool_name: Some(pending.tool_name.clone()),
            allow_custom: true,
            source: Some(pending.source.clone()),
        })
        .await;
    Ok(true)
}

/// Finish this LLM recovery, leaving real command lifetime and approvals alone.
/// Stage the ordinary notice so a rejected checkpoint emits no phantom message.
pub(crate) async fn finish_exhausted_progress(
    session: &mut Session,
    runtime: &AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
    feedback: &str,
    summary: &str,
) -> Result<(), AgentError> {
    if !quiet_poll_guard_active(session, runtime, config) {
        return Err(AgentError::Tool(
            "progress feedback cannot end this owned execution".into(),
        ));
    }
    let mut candidate = session.clone();
    let mut notice = bamboo_agent_core::Message::assistant(summary, None);
    notice.metadata = Some(serde_json::json!({
        "runtime_kind": "observation_progress_exhausted",
        "source": "runtime",
        "progress_feedback": feedback,
    }));
    candidate.add_message(notice.clone());
    candidate.metadata.insert(
        "runtime.completion_reason".into(),
        "observation_progress_exhausted".into(),
    );
    crate::runtime::runner::state_bridge::write_runtime_state(&mut candidate, runtime);
    if let Some(persistence) = config.persistence.as_ref() {
        persistence
            .save_runtime_session(&mut candidate)
            .await
            .map_err(|error| {
                AgentError::Tool(format!(
                    "Progress feedback summary could not be saved: {error}"
                ))
            })?;
    }
    *session = candidate;
    let _ = event_tx
        .send(AgentEvent::message_appended(&session.id, &notice))
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct QuestionPersistence {
        saved: Mutex<Option<Session>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for QuestionPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            if self.fail {
                return Err(std::io::Error::other("question save failed"));
            }
            *self.saved.lock().unwrap() = Some(session.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn quiet_poll_pause_preserves_noninteractive_and_owned_task_lifecycles() {
        let (tx, mut rx) = mpsc::channel(4);
        let config = AgentLoopConfig::default();
        let mut root = Session::new("quiet-pause-root", "model");
        let mut runtime = AgentRuntimeState::default();
        assert!(quiet_poll_guard_active(&root, &runtime, &config));
        let mut child = Session::new_child("quiet-child", "quiet-pause-root", "model", "Inspect");
        assert!(
            !pause_for_quiet_polling(&mut child, &mut runtime, &tx, &config)
                .await
                .unwrap()
        );
        assert!(child.pending_question.is_none());
        runtime.no_human_approver = true;
        assert!(
            !pause_for_quiet_polling(&mut root, &mut runtime, &tx, &config)
                .await
                .unwrap()
        );
        runtime.no_human_approver = false;
        let resources = bamboo_tools::LocalTaskResources::new(
            "quiet-pause-root",
            "quiet-run",
            chrono::Utc::now(),
        );
        resources
            .scope(async {
                assert!(
                    !pause_for_quiet_polling(&mut root, &mut runtime, &tx, &config)
                        .await
                        .unwrap()
                );
            })
            .await;
        resources.close_and_drain().await.unwrap();
        assert!(root.pending_question.is_none());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn no_progress_pause_persists_before_publishing_question() {
        let persistence = Arc::new(QuestionPersistence {
            saved: Mutex::new(None),
            fail: false,
        });
        let config = AgentLoopConfig {
            persistence: Some(persistence.clone()),
            ..Default::default()
        };
        let mut session = Session::new("progress-persist", "model");
        let mut runtime = AgentRuntimeState::default();
        let (tx, mut rx) = mpsc::channel(4);
        assert!(
            pause_for_no_progress(&mut session, &mut runtime, &tx, &config)
                .await
                .unwrap()
        );
        let durable = persistence.saved.lock().unwrap().clone().unwrap();
        assert_eq!(
            durable.pending_question.as_ref().unwrap().tool_call_id,
            session.pending_question.as_ref().unwrap().tool_call_id
        );
        assert_eq!(
            durable.agent_runtime_state.unwrap().status,
            bamboo_domain::AgentStatusState::Suspended
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            AgentEvent::MessageAppended { .. }
        ));
        assert!(matches!(
            rx.try_recv().unwrap(),
            AgentEvent::NeedClarification {
                allow_custom: true,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn no_progress_pause_save_failure_never_publishes_question() {
        let persistence = Arc::new(QuestionPersistence {
            saved: Mutex::new(None),
            fail: true,
        });
        let config = AgentLoopConfig {
            persistence: Some(persistence.clone()),
            ..Default::default()
        };
        let mut session = Session::new("progress-persist-failure", "model");
        let mut runtime = AgentRuntimeState::default();
        let (tx, mut rx) = mpsc::channel(4);
        assert!(
            pause_for_no_progress(&mut session, &mut runtime, &tx, &config)
                .await
                .is_err()
        );
        assert!(persistence.saved.lock().unwrap().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn no_progress_pause_does_not_change_child_headless_root_or_existing_question() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut runtime = AgentRuntimeState::default();
        let mut child = Session::new_child("child", "root", "model", "Inspect");
        assert!(
            !pause_for_no_progress(&mut child, &mut runtime, &tx, &AgentLoopConfig::default())
                .await
                .unwrap()
        );
        assert!(child.pending_question.is_none());
        let mut headless = Session::new("headless-root", "model");
        let headless_before = serde_json::to_value(&headless).unwrap();
        runtime.no_human_approver = true;
        assert!(!pause_for_no_progress(
            &mut headless,
            &mut runtime,
            &tx,
            &AgentLoopConfig::default()
        )
        .await
        .unwrap());
        assert_eq!(serde_json::to_value(&headless).unwrap(), headless_before);
        runtime.no_human_approver = false;
        let mut session = Session::new("existing-question", "model");
        session.set_pending_question(
            "real-tool".into(),
            "Bash".into(),
            "Approve?".into(),
            vec![],
            false,
        );
        assert!(!pause_for_no_progress(
            &mut session,
            &mut runtime,
            &tx,
            &AgentLoopConfig::default()
        )
        .await
        .unwrap());
        assert_eq!(session.pending_question.unwrap().tool_call_id, "real-tool");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn model_progress_exhaustion_checkpoints_normal_notice_without_question() {
        let store = Arc::new(QuestionPersistence {
            saved: Mutex::new(None),
            fail: false,
        });
        let config = AgentLoopConfig {
            persistence: Some(store.clone()),
            ..Default::default()
        };
        let mut session = Session::new("feedback-finish", "model");
        let runtime = AgentRuntimeState::default();
        let (tx, mut rx) = mpsc::channel(4);
        finish_exhausted_progress(
            &mut session,
            &runtime,
            &tx,
            &config,
            "[runtime_progress_feedback_v1]\n{}",
            "The task is not confirmed complete.",
        )
        .await
        .unwrap();
        let saved = store.saved.lock().unwrap().clone().unwrap();
        assert!(saved.pending_question.is_none());
        assert!(!saved.metadata.contains_key("runtime.suspend_reason"));
        assert_eq!(
            saved.metadata["runtime.completion_reason"],
            "observation_progress_exhausted"
        );
        assert_eq!(
            saved.messages.last().unwrap().metadata.as_ref().unwrap()["runtime_kind"],
            "observation_progress_exhausted"
        );
        assert!(matches!(
            rx.try_recv().unwrap(),
            AgentEvent::MessageAppended { .. }
        ));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn model_progress_exhaustion_failed_checkpoint_keeps_caller_and_events() {
        let store = Arc::new(QuestionPersistence {
            saved: Mutex::new(None),
            fail: true,
        });
        let config = AgentLoopConfig {
            persistence: Some(store),
            ..Default::default()
        };
        let mut session = Session::new("feedback-finish-failure", "model");
        let before = serde_json::to_value(&session).unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let error = finish_exhausted_progress(
            &mut session,
            &AgentRuntimeState::default(),
            &tx,
            &config,
            "[runtime_progress_feedback_v1]\n{}",
            "The task is not confirmed complete.",
        )
        .await
        .expect_err("rejected checkpoint must not publish a completion notice");
        assert!(error.to_string().contains("question save failed"));
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert!(rx.try_recv().is_err());
    }
}
