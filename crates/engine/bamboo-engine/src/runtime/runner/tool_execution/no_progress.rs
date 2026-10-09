use tokio::sync::mpsc;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{AgentRuntimeState, SessionKind};

/// Pause only a Human-controlled Root, after every real tool result is paired.
/// Publish the existing question event only after its checkpoint succeeds.
pub(crate) async fn pause_for_no_progress(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    event_tx: &mpsc::Sender<AgentEvent>,
    config: &AgentLoopConfig,
) -> Result<bool, AgentError> {
    if session.kind != SessionKind::Root || session.pending_question.is_some() {
        return Ok(false);
    }
    crate::session_app::no_progress::create_question(session, runtime_state);
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
    async fn no_progress_pause_does_not_change_child_or_existing_question() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut runtime = AgentRuntimeState::default();
        let mut child = Session::new_child("child", "root", "model", "Inspect");
        assert!(
            !pause_for_no_progress(&mut child, &mut runtime, &tx, &AgentLoopConfig::default())
                .await
                .unwrap()
        );
        assert!(child.pending_question.is_none());
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
}
