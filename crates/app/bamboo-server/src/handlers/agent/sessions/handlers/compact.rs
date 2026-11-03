use actix_web::{web, HttpResponse, Result};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{ActorActivationFinish, SessionKind};
use bamboo_engine::events::RootActorEventPublication;
use bamboo_engine::execution::{
    reserve_session_preparation, AgentStatus, SessionPreparationReservation,
};
use bamboo_engine::session_app::{
    errors::RespondError, execute::has_pending_user_message, repository::SessionAccess,
};
use serde_json::json;
use std::time::Duration;

use crate::app_state::AppState;
use crate::error::{error_value, json_internal_server_error};
use crate::handlers::agent::events;
use crate::tools::ToolSurface;

const COMPACTION_DEADLINE: Duration = Duration::from_secs(300);

fn conflict(detail: impl Into<String>) -> HttpResponse {
    HttpResponse::Conflict().json(json!({ "error": error_value(detail) }))
}

fn not_found() -> HttpResponse {
    HttpResponse::NotFound().json(json!({ "error": error_value("Session not found") }))
}

/// Compress an idle ordinary chat through the Session's fenced history writer.
pub async fn compact_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let _startup = {
        let _lock = state.persistence.acquire_lock(&id).await;
        if events::execute_startup_is_in_flight(&state, &id) {
            return Ok(conflict(
                "Wait for the current Session operation before compressing",
            ));
        }
        events::begin_execute_startup(&state, &id)
    };
    let mut owner = match reserve_session_preparation(&state.agent, &state.agent_runners, &id).await
    {
        Ok(owner) => owner,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            return Ok(conflict(
                "Wait for the current Session operation before compressing",
            ));
        }
        Err(error) => return Err(json_internal_server_error(error.to_string())),
    };
    let response = compact_owned_session(&state, &id, &mut owner).await;
    if response
        .as_ref()
        .is_ok_and(|response| response.status().is_success())
    {
        owner.finish(ActorActivationFinish::Succeeded).await;
    } else {
        owner.abandon().await;
    }
    response
}

async fn compact_owned_session(
    state: &AppState,
    id: &str,
    owner: &mut SessionPreparationReservation,
) -> Result<HttpResponse> {
    let Some(observed) = state.storage.load_session(id).await.map_err(|error| {
        json_internal_server_error(format!("Could not load the Session: {error}"))
    })?
    else {
        return Ok(not_found());
    };
    if let Some(response) = super::super::types::SessionSummary::execution_conflict(&observed) {
        return Ok(response);
    }
    if observed.kind != SessionKind::Root || !observed.authority_identity.is_ordinary() {
        return Ok(conflict(
            "Direct compression requires an ordinary chat Session",
        ));
    }
    if let Err(error) = owner.bind_root_actor(&state.agent, &observed).await {
        return Ok(conflict(format!("Session ownership changed: {error}")));
    }
    let writer = owner.root_actor_writer();
    let Some(access) = state
        .bind_response_writer(writer.clone())
        .map_err(|error| {
            json_internal_server_error(format!("Could not bind the Session writer: {error}"))
        })?
    else {
        return Ok(HttpResponse::ServiceUnavailable().json(json!({
            "error": error_value("Direct compression requires the Session's Root writer"),
        })));
    };
    let Some(mut staged) = access.inspect_for_response(id).await.map_err(|error| {
        json_internal_server_error(format!("Could not inspect the Session: {error}"))
    })?
    else {
        return Ok(not_found());
    };
    if staged.created_at != observed.created_at
        || staged.authority_identity != observed.authority_identity
        || staged.kind != observed.kind
        || staged.check_conversation_generation(&observed).is_err()
    {
        return Ok(conflict("The Session changed before compression"));
    }
    if matches!(
        staged.last_run_status().as_deref(),
        Some("pending" | "running" | "suspended")
    ) || has_pending_user_message(&staged)
        || bamboo_domain::conversation_compaction_pending_rejection(&staged).is_some()
    {
        return Ok(conflict(
            "Wait for pending Session work or responses before compressing",
        ));
    }
    let inbox = state.session_inbox.inspect(id).await.map_err(|error| {
        json_internal_server_error(format!("Could not inspect pending Session input: {error}"))
    })?;
    if inbox.pending > 0 || inbox.claimed > 0 {
        return Ok(conflict("Wait for queued Session input before compressing"));
    }
    let snapshot = serde_json::to_value(&staged)
        .map_err(|error| json_internal_server_error(error.to_string()))?;
    let tools = state.tools_for(ToolSurface::Root);
    let result = tokio::time::timeout(
        COMPACTION_DEADLINE,
        state.agent.stage_context_compaction(
            &mut staged,
            &state.provider_registry,
            tools.as_ref(),
            &state.app_data_dir,
        ),
    )
    .await;
    let Ok(result) = result else {
        return Ok(HttpResponse::GatewayTimeout().json(json!({
            "error": error_value("Context compression timed out before saving"),
        })));
    };
    match result {
        Ok(true) => {}
        Ok(false) => {
            return Ok(HttpResponse::Ok().json(json!({
                "session_id": id, "status": "unchanged",
            })))
        }
        Err(error) => {
            return Ok(HttpResponse::UnprocessableEntity().json(json!({
                "error": error_value(format!("Could not compress the Session: {error}")),
            })))
        }
    }

    let event_id = staged
        .compression_events
        .last()
        .expect("compaction event")
        .id
        .clone();
    staged.updated_at = chrono::Utc::now();
    let saved = access
        .mutate_for_response(
            id,
            Box::new(move |current| {
                if serde_json::to_value(&*current).ok().as_ref() != Some(&snapshot) {
                    return Err(RespondError::InvalidResponse(
                        "The Session changed during compression".into(),
                    ));
                }
                *current = staged;
                Ok(())
            }),
        )
        .await;
    match saved {
        Ok(Some(saved)) => {
            let receipt = saved
                .compression_events
                .last()
                .zip(saved.token_usage.as_ref())
                .filter(|(event, usage)| event.id == event_id && usage.max_context_tokens > 0);
            let Some((event, usage)) = receipt else {
                return Err(json_internal_server_error(
                    "Could not confirm the compression checkpoint",
                ));
            };
            let budget_event = AgentEvent::TokenBudgetUpdated {
                usage: usage.clone(),
            };
            let live_event = budget_event.clone();
            let sender = state.get_session_event_sender(id).await;
            let mut runners = state.agent_runners.clone().write_owned().await;
            let session_id = id.to_owned();
            // Update replay and broadcast at the same boundary a subscriber
            // uses to capture its receiver and retained runner.
            let publication = RootActorEventPublication::new(
                state.storage.clone(),
                state.account_sink.clone(),
                writer.expect("bound Root writer"),
            );
            if let Err(error) = publication
                .publish(
                    id,
                    &budget_event,
                    Box::new(move || {
                        if let Some(runner) = runners.get_mut(&session_id) {
                            if matches!(runner.status, AgentStatus::Pending | AgentStatus::Running)
                            {
                                return false;
                            }
                            runner.last_budget_event = Some(live_event.clone());
                        }
                        let _ = sender.send(live_event);
                        true
                    }),
                )
                .await
            {
                tracing::debug!(session_id = id, %error, "Compaction was committed but token event publication was unavailable");
            }
            Ok(HttpResponse::Ok().json(json!({
                "session_id": id,
                "status": "compacted",
                "usage_before_percent": event.usage_before_percent,
                "usage_after_percent": f64::from(usage.total_tokens) * 100.0
                    / f64::from(usage.max_context_tokens),
                "token_usage": usage,
            })))
        }
        Ok(None) => Ok(not_found()),
        Err(RespondError::InvalidResponse(reason)) => Ok(conflict(reason)),
        Err(error) => Err(json_internal_server_error(format!(
            "Could not confirm the compression checkpoint: {error}"
        ))),
    }
}
