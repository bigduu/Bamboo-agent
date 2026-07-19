use chrono::Utc;
use tokio::sync::mpsc;

use super::finalize_session;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState, TaskItem, TaskItemStatus, TaskList};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use std::sync::Arc;

fn completed_task_list(session_id: &str) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Done".to_string(),
        items: vec![TaskItem {
            id: "done-1".to_string(),
            description: "Completed task".to_string(),
            status: TaskItemStatus::Completed,
            depends_on: Vec::new(),
            notes: String::new(),
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn finalize_session_emits_complete_when_not_sent_yet() {
    let mut session = Session::new("finalize-session-1", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        None,
        &mut session,
        &event_tx,
        "finalize-session-1",
        &AgentLoopConfig::default(),
        None,
        false,
        &mut AgentRuntimeState::new("finalize-session-1"),
    )
    .await;

    let event = event_rx.recv().await.expect("complete event expected");
    match event {
        AgentEvent::Complete { usage } => {
            assert_eq!(usage.prompt_tokens, 0);
            assert_eq!(usage.completion_tokens, 0);
            assert_eq!(usage.total_tokens, 0);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn finalize_session_skips_complete_when_already_sent() {
    let mut session = Session::new("finalize-session-2", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        None,
        &mut session,
        &event_tx,
        "finalize-session-2",
        &AgentLoopConfig::default(),
        None,
        true,
        &mut AgentRuntimeState::new("finalize-session-2"),
    )
    .await;

    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn finalize_session_syncs_task_context_and_emits_task_completed() {
    let mut session = Session::new("finalize-session-3", "test-model");
    session.set_task_list(completed_task_list("finalize-session-3"));
    let mut task_context =
        TaskLoopContext::from_session(&session).expect("task context should exist");
    task_context.current_round = 3;

    let (event_tx, mut event_rx) = mpsc::channel(8);

    finalize_session(
        Some(task_context),
        &mut session,
        &event_tx,
        "finalize-session-3",
        &AgentLoopConfig::default(),
        None,
        true,
        &mut AgentRuntimeState::new("finalize-session-3"),
    )
    .await;

    let event = event_rx
        .recv()
        .await
        .expect("task completed event expected");
    match event {
        AgentEvent::TaskListCompleted {
            session_id,
            total_rounds,
            ..
        } => {
            assert_eq!(session_id, "finalize-session-3");
            assert_eq!(total_rounds, 4);
        }
        other => panic!("unexpected event: {other:?}"),
    }

    assert!(session.task_list.is_some());
    assert_eq!(
        session
            .metadata
            .get("task_list_version")
            .map(String::as_str),
        Some("0")
    );
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn finalize_releases_completed_activation_but_retains_suspended_activation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let skill_root = directory.path().join("skills/finalize-demo");
    tokio::fs::create_dir_all(&skill_root)
        .await
        .expect("skill root");
    tokio::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: finalize-demo\ndescription: finalize\n---\nfinalize\n",
    )
    .await
    .expect("skill");
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: directory.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.expect("initialize");
    let ids = vec!["finalize-demo".to_string()];
    for session_id in ["completed-pin", "suspended-pin"] {
        manager
            .store()
            .pin_current_activation(session_id, &ids, None)
            .await
            .expect("pin activation");
    }
    let config = AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        ..Default::default()
    };
    let (event_tx, _event_rx) = mpsc::channel(8);
    let mut completed_session = Session::new("completed-pin", "model");
    finalize_session(
        None,
        &mut completed_session,
        &event_tx,
        "completed-pin",
        &config,
        None,
        true,
        &mut AgentRuntimeState::new("completed-pin"),
    )
    .await;
    assert!(manager
        .store()
        .activation_descriptor("completed-pin")
        .await
        .is_none());

    let mut suspended_session = Session::new("suspended-pin", "model");
    let mut suspended_state = AgentRuntimeState::new("suspended-pin");
    suspended_state.status = AgentStatusState::Suspended;
    finalize_session(
        None,
        &mut suspended_session,
        &event_tx,
        "suspended-pin",
        &config,
        None,
        true,
        &mut suspended_state,
    )
    .await;
    assert!(manager
        .store()
        .activation_descriptor("suspended-pin")
        .await
        .is_some());
}
