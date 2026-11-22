use actix_web::{web, HttpRequest, HttpResponse, ResponseError};
use bamboo_domain::{ActorDirectoryPort, Message, Role, Session};
use bamboo_engine::config::GoldConfig;
use bamboo_engine::model_config_helper::{resolve_gold_config, GOLD_CONFIG_METADATA_KEY};
use bamboo_engine::runtime::goal_state::{read_goal_state, GoalRuntimeStatus};

use super::{workflow_runner_is_active, ChatResponse, GoalCommandResponse};
use crate::app_state::AppState;
use serde_json::json;

pub(crate) async fn resume_goal(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
) -> HttpResponse {
    use crate::handlers::agent::tickets::{require_supervisor_owner, TicketHttpError};

    let session_id = path.into_inner();
    if let Err(error) = require_supervisor_owner(&state, &request, &session_id).await {
        return TicketHttpError::from(error).error_response();
    }
    handle(&state, &session_id).await
}

pub(super) async fn handle(state: &AppState, session_id: &str) -> HttpResponse {
    let guard = state.persistence.acquire_lock(session_id).await;
    let session = match state.storage.load_session(session_id).await {
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
        return response(session_id, config, "status");
    }
    if !crate::skill_runtime::registered_main(&session)
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
    if actor.activation.as_ref().is_some_and(|activation| {
        activation.status.is_live() && activation.lease_expires_at > chrono::Utc::now()
    }) {
        return conflict(
            "The previous execution still owns this session. Try again when its lease expires.",
        );
    }
    if session.last_run_status().as_deref() == Some("pending") {
        return conflict("An input is already pending in this session.");
    }
    if has_unresolved_tool_call(&session) {
        return conflict("The previous tool call has no confirmed result. Check its outcome before resuming the Goal.");
    }

    let mut instruction = Message::user(
        "The user asked to resume the saved Goal. Continue from the committed conversation. Verify the current state of prior outputs and side effects before taking action. Keep the same objective, remaining continuation budget, and existing permissions.",
    );
    instruction.metadata = Some(json!({
        "hidden_from_ui": true,
        "runtime_kind": "goal_recovery_resume",
    }));
    drop(runners);
    drop(guard);
    match state
        .child_completion_coordinator
        .resume_requested_goal(&session, instruction)
        .await
    {
        Ok(()) => response(session_id, config, "resume"),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            conflict(&error.to_string())
        }
        Err(error) => {
            tracing::warn!(%session_id, %error, "failed to admit requested Goal continuation");
            unavailable()
        }
    }
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

fn response(session_id: &str, config: GoldConfig, action: &str) -> HttpResponse {
    HttpResponse::Ok().json(ChatResponse {
        message_id: None,
        ingress_seq: None,
        session_id: session_id.to_owned(),
        stream_url: format!("/api/v1/events/{session_id}"),
        status: "accepted".to_owned(),
        goal_command: Some(GoalCommandResponse {
            action: action.to_owned(),
            should_execute: false,
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
    use bamboo_domain::ActorActivationClaim;

    #[actix_web::test]
    async fn interrupted_goal_resumes_the_same_checkpoint_once_and_retains_its_budget() {
        let directory = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
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
        let now = chrono::Utc::now() - chrono::Duration::seconds(5);
        let owner = state
            .session_store
            .claim_activation(&ActorActivationClaim {
                actor_id: session.id.clone(),
                run_id: "interrupted-owner".into(),
                lease_owner: "previous-host".into(),
                lease_expires_at: now + chrono::Duration::seconds(1),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        state
            .session_store
            .start_activation(&owner.fence(), now)
            .await
            .unwrap();

        let app = actix_web::test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let uri = format!("/api/v1/sessions/{}/goal/resume", session.id);
        let request = actix_web::test::TestRequest::post().uri(&uri).to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body: serde_json::Value = actix_web::test::read_body_json(response).await;
        assert_eq!(body["goal_command"]["action"], "resume");
        assert_eq!(body["goal_command"]["should_execute"], false);
        let saved = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.metadata["goal.state"], goal.to_string());
        assert_eq!(saved.metadata[GOLD_CONFIG_METADATA_KEY], config.to_string());
        assert!(saved.messages.iter().any(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|value| value["runtime_kind"].as_str())
                == Some("goal_recovery_resume")
        }));
        assert!(saved
            .messages
            .iter()
            .any(|message| { message.content == "Existing artifact evidence" }));
        let inbox = state.session_inbox.inspect(&session.id).await.unwrap();
        assert!(inbox.oldest_generation.is_none());
    }

    #[actix_web::test]
    async fn requested_goal_resume_rejects_a_goal_paused_after_validation() {
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new(directory.path().to_path_buf()).await.unwrap();
        let mut session = Session::new("goal-paused-before-resume", "model");
        session.metadata.insert(
            GOLD_CONFIG_METADATA_KEY.into(),
            json!({"enabled":true,"auto_continue_enabled":true,"goal":"Verify the build"})
                .to_string(),
        );
        let goal =
            bamboo_engine::runtime::goal_state::ensure_goal_state(&session, "Verify the build");
        bamboo_engine::runtime::goal_state::write_goal_state(&mut session, goal);
        state.storage.save_session(&session).await.unwrap();
        let source = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        let mut paused = source.clone();
        paused.metadata.insert(
            GOLD_CONFIG_METADATA_KEY.into(),
            json!({"enabled":false,"auto_continue_enabled":false,"goal":"Verify the build"})
                .to_string(),
        );
        state.storage.save_session(&paused).await.unwrap();

        let result = state
            .child_completion_coordinator
            .resume_requested_goal(&source, Message::user("obsolete resume"))
            .await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        let saved = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert!(saved.messages.is_empty());
        assert_eq!(
            saved.metadata[GOLD_CONFIG_METADATA_KEY],
            paused.metadata[GOLD_CONFIG_METADATA_KEY]
        );
        assert!(state
            .session_inbox
            .inspect(&session.id)
            .await
            .unwrap()
            .oldest_generation
            .is_none());
    }
}
