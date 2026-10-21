use actix_web::{web, HttpResponse, Result};
use bamboo_agent_core::Session;
use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
use bamboo_engine::session_app::errors::RespondError;
use bamboo_engine::session_app::repository::SessionAccess;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::app_state::AppState;

const CHANGED_LIFETIME: &str = "Session identity changed before Retry preparation";

fn conflict(session_id: &str, reason: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "session_id": session_id,
        "error": crate::error::error_value(reason),
    }))
}

pub(super) async fn prepare(
    state: &web::Data<AppState>,
    observed: Session,
) -> Result<HttpResponse> {
    let id = observed.id.clone();
    // Coordinate with the same startup lock used by Execute. No model is
    // dispatched by this operation; the following Execute remains separate.
    let _startup = {
        let _lock = state.persistence.acquire_lock(&id).await;
        if crate::handlers::agent::events::execute_startup_is_in_flight(state, &id) {
            return Ok(conflict(
                &id,
                "Session is preparing another operation; wait before Retry",
            ));
        }
        crate::handlers::agent::events::begin_execute_startup(state, &id)
    };
    let sender = state.get_session_event_sender(&id).await;
    let mut reservation = match reserve_session_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        &id,
        &sender,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        SessionExecutionReserveOutcome::AlreadyRunning { .. } => {
            return Ok(conflict(&id, "Session is running; wait before Retry"));
        }
    };
    if let Err(error) = reservation.bind_root_actor(&state.agent, &observed).await {
        reservation.abandon().await;
        return Ok(conflict(&id, &format!("Retry ownership changed: {error}")));
    }
    let access = match SessionAccess::bind_response_writer(
        state.as_ref(),
        reservation.root_actor_writer(),
    ) {
        Ok(Some(access)) => access,
        result => {
            reservation.abandon().await;
            let detail = match result {
                Err(error) => format!("Retry writer unavailable: {error}"),
                _ => "Retry requires its exact Root writer".to_owned(),
            };
            return Ok(HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "session_id": id, "error": crate::error::error_value(detail),
            })));
        }
    };
    let outcome = Arc::new(AtomicUsize::new(0));
    let captured = outcome.clone();
    let saved = access
        .mutate_for_response(
            &id,
            Box::new(move |session| {
                if session.created_at != observed.created_at
                    || session.authority_identity != observed.authority_identity
                    || session.kind != observed.kind
                {
                    return Err(RespondError::InvalidResponse(CHANGED_LIFETIME.to_owned()));
                }
                let (removed, _, clear, _) = super::prepare_error_retry(session)
                    .map_err(|reason| RespondError::InvalidResponse(reason.to_owned()))?;
                if clear {
                    super::clear_derived_context_state(session);
                }
                session.updated_at = chrono::Utc::now();
                captured.store(removed, Ordering::Relaxed);
                Ok(())
            }),
        )
        .await;
    if matches!(saved, Ok(Some(_))) {
        // The atomic mutation has awaited the durable save and fenced cache
        // publication. There are no queued model/forwarder events to replay.
        reservation
            .finish_root_actor(bamboo_domain::ActorActivationFinish::Succeeded)
            .await;
    }
    reservation.abandon().await;
    match saved {
        Ok(Some(session)) => {
            let removed = outcome.load(Ordering::Relaxed);
            let length = session.messages.len();
            Ok(HttpResponse::Ok().json(serde_json::json!({
                "success": true, "session_id": id,
                "messages_removed": removed, "message_count": length,
            })))
        }
        Ok(None) => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "session_id": id, "error": crate::error::error_value("Session not found"),
        }))),
        Err(RespondError::InvalidResponse(reason)) if reason == CHANGED_LIFETIME => {
            Ok(conflict(&id, CHANGED_LIFETIME))
        }
        Err(RespondError::InvalidResponse(reason)) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "session_id": id, "error": crate::error::error_value(reason),
            })))
        }
        Err(error) => Err(crate::error::json_internal_server_error(format!(
            "Retry preparation was not committed; no new execution started: {error}"
        ))),
    }
}
