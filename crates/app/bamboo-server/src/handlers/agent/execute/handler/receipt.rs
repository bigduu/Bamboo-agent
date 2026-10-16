//! Explicit Native transport selection; receipt data never supplies a writer.
use crate::{app_state::AppState, error::ResponseResult};
use actix_web::{
    http::{header::HeaderValue, StatusCode},
    web, HttpRequest, HttpResponse,
};
use bamboo_domain::{SessionInboxReceipt, SessionMessageId};
use bamboo_engine::execution::agent_spawn::reserve_selected_input_execution;
use bamboo_engine::execution::{SessionExecutionReservation, SessionExecutionReserveOutcome};

pub(super) const INPUT_ID: &str = "Bamboo-Input-Id";
pub(super) const INGRESS_SEQ: &str = "Bamboo-Ingress-Seq";
pub(super) const ROOT_BIRTH: &str = "Bamboo-Session-Created-At";

pub(super) struct Selection {
    pub receipt: SessionInboxReceipt,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

fn conflict(message: &str) -> HttpResponse {
    crate::error::json_error(StatusCode::CONFLICT, message)
}

pub(super) fn selection(http: &HttpRequest) -> ResponseResult<Option<Selection>> {
    let headers = http.headers();
    if ![INPUT_ID, INGRESS_SEQ, ROOT_BIRTH]
        .iter()
        .any(|name| headers.contains_key(*name))
    {
        return Ok(None);
    }
    let bad = || {
        crate::error::json_error(StatusCode::BAD_REQUEST,
        "Receipt execute requires single Bamboo-Input-Id, positive Bamboo-Ingress-Seq, Bamboo-Session-Created-At and Idempotency-Key headers")
    };
    if [INPUT_ID, INGRESS_SEQ, ROOT_BIRTH, "Idempotency-Key"]
        .iter()
        .any(|name| headers.get_all(*name).count() != 1)
    {
        return Err(bad().into());
    }
    let id = headers
        .get(INPUT_ID)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(bad)?;
    let generation = headers
        .get(INGRESS_SEQ)
        .and_then(|h| h.to_str().ok())
        .filter(|s| s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .ok_or_else(bad)?;
    let id = SessionMessageId::parse(id.to_owned()).map_err(|_| bad())?;
    let created_at = headers
        .get(ROOT_BIRTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .ok_or_else(bad)?;
    Ok(Some(Selection {
        receipt: SessionInboxReceipt { id, generation },
        created_at,
    }))
}

pub(super) async fn reserve(
    state: &web::Data<AppState>,
    id: &str,
    selected: &Selection,
) -> ResponseResult<SessionExecutionReservation> {
    let session = state
        .storage
        .load_session(id)
        .await
        .map_err(|_| conflict("Root receipt target unavailable"))?
        .ok_or_else(|| conflict("Root receipt target unavailable"))?;
    if session.created_at != selected.created_at {
        return Err(conflict("Root lifetime differs from the accepted input request").into());
    }
    if !crate::skill_runtime::registered_main(&session)
        || session.metadata.contains_key("chat.queued_ingress.v1")
        || session.pending_injected_messages().is_some()
        || session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|s| s.waiting_for_children.is_some() || s.waiting_for_bash.is_some())
    {
        return Err(conflict("Receipt execute requires an ordinary Native Root without a specific wait or legacy ingress").into());
    }
    let sender = state.get_session_event_sender(id).await;
    let mut reservation = match reserve_selected_input_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        id,
        &sender,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(r) => r,
        SessionExecutionReserveOutcome::AlreadyRunning { .. } => {
            return Err(conflict("Receipt execute cannot join an existing owner").into())
        }
    };
    let binding = reservation
        .bind_root_actor(&state.agent, &session)
        .await
        .and_then(|_| reservation.bind_root_inbox_receipt(selected.receipt.clone()));
    if binding.is_err() {
        reservation.abandon().await;
        return Err(conflict("Root receipt execution binding rejected").into());
    }
    Ok(reservation)
}

pub(super) fn annotate(mut response: HttpResponse, receipt: Option<Selection>) -> HttpResponse {
    if receipt.is_some()
        && response.status().is_success()
        && response.status() != StatusCode::ACCEPTED
    {
        return conflict("Selected execution did not start a new run");
    }
    if response.status() == StatusCode::ACCEPTED {
        if let Some(selected) = receipt {
            let receipt = selected.receipt;
            response.headers_mut().insert(
                actix_web::http::header::HeaderName::from_static("bamboo-session-created-at"),
                HeaderValue::from_str(&selected.created_at.to_rfc3339()).expect("RFC3339"),
            );
            if let Ok(id) = HeaderValue::from_str(receipt.id.as_str()) {
                response.headers_mut().insert(
                    actix_web::http::header::HeaderName::from_static("bamboo-input-id"),
                    id,
                );
            }
            response.headers_mut().insert(
                actix_web::http::header::HeaderName::from_static("bamboo-ingress-seq"),
                HeaderValue::from_str(&receipt.generation.to_string()).expect("decimal u64"),
            );
            response.headers_mut().insert(
                actix_web::http::header::HeaderName::from_static("bamboo-output-scope"),
                HeaderValue::from_static("activation-receipt"),
            );
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{ActorDirectoryPort, SessionActivationPolicy, SessionMessageEnvelope};
    #[actix_web::test]
    async fn receipt_run_dropped_startup_reservation_releases_actual_actor_and_registry() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let id = "receipt-drop-before-admission";
        state
            .storage
            .save_session(&bamboo_domain::Session::new(id, "test-model"))
            .await
            .unwrap();
        let receipt = state
            .session_inbox
            .deliver_with_activation_intent(
                &SessionMessageEnvelope::user_input(id, "accepted current input"),
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();
        let created_at = state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .created_at;
        let reservation = reserve(
            &state,
            id,
            &Selection {
                receipt: receipt.clone(),
                created_at,
            },
        )
        .await
        .unwrap();
        let token = reservation.cancel_token().clone();
        assert!(reservation
            .execution_persistence()
            .unwrap()
            .root_inbox_receipt()
            .is_some());
        drop(reservation);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let running = state.agent_runners.read().await.get(id).is_some_and(|r| {
                    matches!(
                        r.status,
                        crate::AgentStatus::Running | crate::AgentStatus::Pending
                    )
                });
                let actor = state
                    .session_store
                    .inspect_actor(id)
                    .await
                    .unwrap()
                    .activation
                    .unwrap();
                if token.is_cancelled() && !running && !actor.status.is_live() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!state
            .session_inbox
            .was_admitted(id, &receipt.id)
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
    }
}
