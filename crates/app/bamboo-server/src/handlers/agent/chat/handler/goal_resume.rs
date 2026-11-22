use actix_web::HttpResponse;
use bamboo_domain::{ActorDirectoryPort, Message, Role, Session, SessionKind};
use bamboo_engine::config::GoldConfig;
use bamboo_engine::model_config_helper::{resolve_gold_config, GOLD_CONFIG_METADATA_KEY};
use bamboo_engine::runtime::goal_state::{read_goal_state, GoalRuntimeStatus};

use super::{
    save_and_cache_session_locked, workflow_runner_is_active, ChatResponse, GoalCommandResponse,
};
use crate::app_state::AppState;

const RESUME_KIND: &str = "goal_recovery_resume";

pub(super) async fn handle(state: &AppState, session_id: &str) -> HttpResponse {
    let _guard = state.persistence.acquire_lock(session_id).await;
    let mut session = match state.storage.load_session(session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return HttpResponse::NotFound().json(
                serde_json::json!({"error": crate::error::error_value("Session not found")}),
            );
        }
        Err(error) => {
            tracing::warn!(%session_id, %error, "failed to load saved Goal");
            return unavailable();
        }
    };
    let config = {
        let current = state.config.read().await;
        resolve_gold_config(
            &current,
            session
                .metadata
                .get(GOLD_CONFIG_METADATA_KEY)
                .map(String::as_str),
        )
    };
    let Some(config) = config.filter(|config| config.enabled && config.auto_continue_enabled)
    else {
        return conflict("Enable the existing Goal before resuming.");
    };
    let Some(goal) = read_goal_state(&session)
        .filter(|goal| config.effective_goal() == Some(goal.objective.as_str()))
    else {
        return conflict("No matching Goal checkpoint is available.");
    };
    if goal.status == GoalRuntimeStatus::Complete {
        return response(session_id, config, false);
    }
    if session.kind != SessionKind::Root
        || !session.authority_identity.is_ordinary()
        || session
            .metadata
            .contains_key(bamboo_engine::ticket_runtime::TICKET_DISPATCH_KEY)
    {
        return conflict("This Goal must be resumed by its execution owner.");
    }
    if session.has_runtime_wait() || goal.status == GoalRuntimeStatus::NeedInput {
        return conflict("Resolve the pending input or wait before resuming the Goal.");
    }

    let runners = state.agent_runners.read().await;
    if workflow_runner_is_active(runners.get(session_id))
        || crate::handlers::agent::events::execute_startup_is_in_flight(state, session_id)
    {
        return conflict("An execution is already active in this session.");
    }
    let actor = match state.session_store.inspect_actor(session_id).await {
        Ok(actor) if actor.actor.matches_session(&session) => actor,
        _ => return unavailable(),
    };
    if actor.activation.is_some_and(|activation| {
        activation.status.is_live() && activation.lease_expires_at > chrono::Utc::now()
    }) {
        return conflict(
            "The previous execution still owns this session. Try again when its lease expires.",
        );
    }
    match state.session_inbox.inspect(session_id).await {
        Ok(inbox) if inbox.activation_pending() => {
            return conflict("An input is already pending in this session.");
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%session_id, %error, "failed to inspect Goal session input");
            return unavailable();
        }
    }
    if session.last_run_status().as_deref() == Some("pending") {
        return if session.messages.last().is_some_and(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("runtime_kind"))
                .and_then(|kind| kind.as_str())
                == Some(RESUME_KIND)
        }) {
            response(session_id, config, true)
        } else {
            conflict("An input is already pending in this session.")
        };
    }
    if has_unresolved_tool_call(&session) {
        return conflict("The previous tool call has no confirmed result. Check its outcome before resuming the Goal.");
    }

    let mut message = Message::user(
        "The user asked to resume the saved Goal. Continue from the committed conversation. Verify the current state of prior outputs and side effects before taking action. Keep the same objective, remaining continuation budget, and existing permissions.",
    );
    message.metadata = Some(serde_json::json!({
        "hidden_from_ui": true,
        "runtime_kind": RESUME_KIND,
    }));
    session.add_message(message);
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    if let Err(response) = save_and_cache_session_locked(state, &session).await {
        return *response;
    }
    response(session_id, config, true)
}

fn has_unresolved_tool_call(session: &Session) -> bool {
    use std::collections::HashSet;

    let mut open = HashSet::new();
    for message in &session.messages {
        if message.role == Role::Assistant {
            if let Some(calls) = &message.tool_calls {
                open.extend(calls.iter().map(|call| call.id.as_str()));
            }
        } else if message.role == Role::Tool {
            if let Some(call_id) = &message.tool_call_id {
                open.remove(call_id.as_str());
            }
        }
    }
    !open.is_empty()
}

fn response(session_id: &str, config: GoldConfig, should_execute: bool) -> HttpResponse {
    HttpResponse::Ok().json(ChatResponse {
        message_id: None,
        ingress_seq: None,
        session_id: session_id.to_owned(),
        stream_url: format!("/api/v1/events/{session_id}"),
        status: "accepted".to_owned(),
        goal_command: Some(GoalCommandResponse {
            action: if should_execute { "resume" } else { "status" }.to_owned(),
            should_execute,
            gold_config: Some(config),
        }),
    })
}

fn conflict(message: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({"error": crate::error::error_value(message)}))
}

fn unavailable() -> HttpResponse {
    HttpResponse::ServiceUnavailable().json(serde_json::json!({
        "error": crate::error::error_value("The saved Goal could not be verified."),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{FunctionCall, ToolCall};
    use serde_json::json;

    #[actix_web::test]
    async fn interrupted_goal_resumes_the_same_checkpoint_once_and_retains_its_budget() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(directory.path().to_path_buf()).await.unwrap();
        let mut session = Session::new("interrupted-goal", "model");
        let config = json!({"enabled": true, "auto_continue_enabled": true, "goal": "Ship the verified result", "max_auto_continuations": 3});
        let goal = json!({
            "objective": "Ship the verified result",
            "status": "active",
            "continuation_count": 2,
            "created_at": "2026-11-22T00:00:00Z",
            "updated_at": "2026-11-22T00:05:00Z",
        });
        session
            .metadata
            .insert(GOLD_CONFIG_METADATA_KEY.into(), config.to_string());
        session
            .metadata
            .insert("goal.state".into(), goal.to_string());
        session.set_last_run_status("running");
        session.add_message(Message::user("Ship the verified result"));
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "confirm-artifact".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Read".into(),
                    arguments: "{}".into(),
                },
            }]),
        ));
        state.storage.save_session(&session).await.unwrap();
        let rejected = handle(&state, &session.id).await;
        assert_eq!(rejected.status(), actix_web::http::StatusCode::CONFLICT);
        let outcome = actix_web::body::to_bytes(rejected.into_body())
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&outcome).contains("no confirmed result"));

        session.add_message(Message::tool_result(
            "confirm-artifact",
            "Existing artifact evidence",
        ));
        state.storage.save_session(&session).await.unwrap();
        for _ in 0..2 {
            let response = handle(&state, &session.id).await;
            assert_eq!(response.status(), actix_web::http::StatusCode::OK);
            let bytes = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["goal_command"]["action"], "resume");
            assert_eq!(body["goal_command"]["should_execute"], true);
        }
        let saved = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.metadata["goal.state"], goal.to_string());
        assert_eq!(saved.metadata[GOLD_CONFIG_METADATA_KEY], config.to_string());
        assert_eq!(saved.messages.len(), session.messages.len() + 1);
        assert_eq!(saved.messages[2].content, "Existing artifact evidence");
        assert_eq!(saved.last_run_status().as_deref(), Some("pending"));
    }
}
