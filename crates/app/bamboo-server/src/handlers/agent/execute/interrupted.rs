use std::collections::HashSet;

use actix_web::{web, HttpResponse};
use bamboo_agent_core::{MessagePhase, Role, Session};
use bamboo_engine::execution::SessionExecutionReservation;
use bamboo_engine::session_app::execute::{
    has_pending_clarification_resume, has_pending_retry_resume,
};

use bamboo_engine::session_app::respond::{
    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY, PERMISSION_REEXECUTE_METADATA_KEY,
};

use super::runtime::{reserve_runner, RunnerReservation};
use crate::app_state::{AgentStatus, AppState};

pub(super) fn unavailable(session_id: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "session_id": session_id,
        "error": {
            "type": "api_error",
            "code": "interrupted_resume_unavailable",
            "message": "This conversation has no available interrupted run to resume."
        }
    }))
}

async fn runner_busy(state: &AppState, session_id: &str) -> bool {
    state
        .agent_runners
        .read()
        .await
        .get(session_id)
        .is_some_and(|runner| matches!(runner.status, AgentStatus::Pending | AgentStatus::Running))
}

pub(crate) async fn can_offer(state: &web::Data<AppState>, session: &Session) -> bool {
    !runner_busy(state, &session.id).await
        && !crate::handlers::agent::events::execute_startup_is_in_flight(state, &session.id)
        && eligible(state, session).await
        && !crate::handlers::agent::events::has_running_child(state, &session.id).await
}

async fn eligible(state: &AppState, session: &Session) -> bool {
    if !crate::skill_runtime::registered_main(session)
        || !session.execution_compatibility().execution_allowed
        || session.has_runtime_wait()
        || session.has_pending_injected_messages()
        || has_pending_retry_resume(session)
        || has_pending_clarification_resume(session)
        || session
            .metadata
            .contains_key(bamboo_domain::ONCE_RESPONSE_HANDOFF_KEY)
        || session
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY)
        || session
            .metadata
            .contains_key(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
        || session.metadata.contains_key("chat.queued_ingress.v1")
        || session.metadata.contains_key("created_by_schedule_id")
        || session
            .metadata
            .contains_key(bamboo_domain::PACED_LOOP_METADATA_KEY)
        || session
            .metadata
            .contains_key(bamboo_domain::CONNECT_SESSION_ORIGIN_KEY)
        || !session
            .messages
            .iter()
            .any(bamboo_engine::session_app::execute::is_billable_user_turn)
    {
        return false;
    }
    match session.last_run_status().as_deref() {
        Some("error" | "failed" | "timeout" | "cancelled") => {}
        Some("running")
            if session.messages.last().is_some_and(|tail| {
                matches!(tail.role, Role::User | Role::Tool)
                    || tail.phase == Some(MessagePhase::Commentary)
            }) =>
        {
            if bamboo_engine::runtime::goal_restart::eligible_checkpoint(
                session,
                chrono::Utc::now(),
            )
            .is_some()
                || !state
                    .session_store
                    .has_expired_interrupted_root(session)
                    .await
                    .unwrap_or(false)
            {
                return false;
            }
        }
        _ => return false,
    }
    complete_active_tool_chains(session)
        && state
            .session_inbox
            .inspect(&session.id)
            .await
            .is_ok_and(|backlog| backlog.pending == 0 && backlog.claimed == 0)
}

// Manual resume preserves the transcript. A malformed tool exchange needs
// the separate explicit repair flow, even if the normal projection could trim it.
fn complete_active_tool_chains(session: &Session) -> bool {
    let mut seen = HashSet::new();
    let mut pending = HashSet::new();
    for message in session
        .messages
        .iter()
        .filter(|message| !message.compressed)
    {
        if let Some(calls) = &message.tool_calls {
            for call in calls {
                if message.role != Role::Assistant
                    || call.id.trim().is_empty()
                    || call.function.name.trim().is_empty()
                    || !seen.insert(call.id.as_str())
                {
                    return false;
                }
                pending.insert(call.id.as_str());
            }
        }
        if message.role == Role::Tool
            && !message
                .tool_call_id
                .as_deref()
                .is_some_and(|id| pending.remove(id))
        {
            return false;
        }
    }
    pending.is_empty()
}

pub(super) async fn reserve(
    state: &web::Data<AppState>,
    session_id: &str,
) -> Result<(SessionExecutionReservation, Session), Box<HttpResponse>> {
    let Some(observed) = state.storage.load_session(session_id).await.ok().flatten() else {
        return Err(Box::new(unavailable(session_id)));
    };
    if runner_busy(state, session_id).await || !eligible(state, &observed).await {
        return Err(Box::new(unavailable(session_id)));
    }
    let sender = state.get_session_event_sender(session_id).await;
    let RunnerReservation::Started(reservation) = reserve_runner(state, session_id, &sender).await
    else {
        return Err(Box::new(unavailable(session_id)));
    };

    // The runner slot now excludes competing execution admission. Work already
    // waiting in storage must reconcile through its own path.
    let result = match state.storage.load_session(session_id).await {
        Ok(Some(session))
            if session.created_at == observed.created_at
                && eligible(state, &session).await
                && !crate::handlers::agent::events::has_running_child(state, session_id).await =>
        {
            Some(session)
        }
        _ => None,
    };
    if let Some(session) = result {
        Ok((reservation, session))
    } else {
        reservation.abandon().await;
        Err(Box::new(unavailable(session_id)))
    }
}
