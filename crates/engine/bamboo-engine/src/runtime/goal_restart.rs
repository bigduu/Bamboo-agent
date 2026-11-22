//! The persisted handoff between one verified Goal stop point and the next
//! provider round. The running owner withdraws it before the next round begins.

use crate::model_config_helper::{parse_session_gold_config, GOLD_CONFIG_METADATA_KEY};
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::goal_state::{read_goal_state, GoalState};
use bamboo_agent_core::{AgentError, Message, Role, Session};
use bamboo_domain::{AgentHookPoint, SessionKind};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const CHECKPOINT: &str = "goal.restart_checkpoint";
const DISARMED: &str = "null";
const ATTEMPTED_GOAL: &str = "goal.restart_attempted";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRestartCheckpoint {
    pub run_id: String,
    pub lease_epoch: u64,
    session_created_at: DateTime<Utc>,
    goal_created_at: String,
    root_tool_authority_revision: u64,
    message_id: String,
}

fn supported_root(session: &Session) -> bool {
    session.kind == SessionKind::Root
        && session.authority_identity.is_ordinary()
        && !crate::ticket_runtime::is_managed_work_root(session)
        && !session.has_runtime_wait()
}

fn explicitly_enabled(session: &Session, goal: &GoalState) -> bool {
    parse_session_gold_config(
        session
            .metadata
            .get(GOLD_CONFIG_METADATA_KEY)
            .map(String::as_str),
    )
    .is_some_and(|config| {
        config.enabled
            && config.auto_continue_enabled
            && config.resume_after_restart
            && config.effective_goal() == Some(goal.objective.as_str())
    })
}

pub(crate) async fn arm(session: &mut Session, config: &AgentLoopConfig) {
    let Some(goal) = read_goal_state(session) else {
        return;
    };
    if !config
        .gold_config
        .as_ref()
        .is_some_and(|gold| gold.resume_after_restart)
        || !supported_root(session)
        || config.run_budget != Default::default()
        || config.run_deadline.is_some()
        || config.effective_counted_loop().is_some()
        || config.paced_loop_turn.is_some()
        || config.hook_runner.has_hooks_for(AgentHookPoint::AfterRound)
        || !explicitly_enabled(session, &goal)
        || session.metadata.get(ATTEMPTED_GOAL) == Some(&goal.created_at)
    {
        return;
    }
    let Some(persistence) = config.persistence.as_ref() else {
        return;
    };
    let Some(owner) = persistence.root_actor_writer() else {
        return;
    };
    let Some(message) = session
        .messages
        .last()
        .filter(|message| is_continuation(message))
    else {
        return;
    };
    let checkpoint = GoalRestartCheckpoint {
        run_id: owner.fence.run_id.clone(),
        lease_epoch: owner.fence.lease_epoch,
        session_created_at: owner.created_at,
        goal_created_at: goal.created_at.clone(),
        root_tool_authority_revision: session.root_tool_authority_revision,
        message_id: message.id.clone(),
    };
    if let Ok(value) = serde_json::to_string(&checkpoint) {
        session.metadata.insert(CHECKPOINT.into(), value);
        if let Err(error) = persistence.checkpoint_runtime_session(session).await {
            // A failed acknowledgement can follow a successful write. Keep
            // the local marker so the next round still has to withdraw it.
            tracing::warn!(session_id = %session.id, %error, "Goal restart checkpoint unconfirmed");
        }
    }
}

fn is_continuation(message: &Message) -> bool {
    message.role == Role::User
        && message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("runtime_kind"))
            .and_then(serde_json::Value::as_str)
            == Some("goal_continue")
}

pub(crate) async fn disarm_before_round(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> Result<(), AgentError> {
    if session
        .metadata
        .get(CHECKPOINT)
        .is_some_and(|value| value != DISARMED)
    {
        // Persist a tombstone because runtime metadata merges preserve absent keys.
        session.metadata.insert(CHECKPOINT.into(), DISARMED.into());
        if let Some(persistence) = config.persistence.as_ref() {
            persistence
                .checkpoint_runtime_session(session)
                .await
                .map_err(|error| {
                    AgentError::Tool(format!(
                        "Could not fence the Goal restart checkpoint: {error}"
                    ))
                })?;
        }
    }
    Ok(())
}

/// Return only a checkpoint written by a preceding process, for the exact
/// current Goal and continuation message. The Host must still obtain the
/// ordinary Root writer before consuming it or making any provider call.
pub fn eligible_checkpoint(
    session: &Session,
    startup: DateTime<Utc>,
) -> Option<GoalRestartCheckpoint> {
    if session.updated_at >= startup
        || session.last_run_status().as_deref() != Some("running")
        || !supported_root(session)
    {
        return None;
    }
    let goal = read_goal_state(session)?;
    if !goal.status.is_active()
        || goal.declared_status.is_some()
        || !explicitly_enabled(session, &goal)
        || session.metadata.get(ATTEMPTED_GOAL) == Some(&goal.created_at)
    {
        return None;
    }
    let checkpoint: GoalRestartCheckpoint =
        serde_json::from_str(session.metadata.get(CHECKPOINT)?).ok()?;
    let message = session.messages.last()?;
    (checkpoint.session_created_at == session.created_at
        && checkpoint.goal_created_at == goal.created_at
        && checkpoint.root_tool_authority_revision == session.root_tool_authority_revision
        && message.id == checkpoint.message_id
        && is_continuation(message))
    .then_some(checkpoint)
}

pub(crate) fn consume(session: &mut Session, checkpoint: &GoalRestartCheckpoint) {
    session.metadata.insert(CHECKPOINT.into(), DISARMED.into());
    session
        .metadata
        .insert(ATTEMPTED_GOAL.into(), checkpoint.goal_created_at.clone());
}
