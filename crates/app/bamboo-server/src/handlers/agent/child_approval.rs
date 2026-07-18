//! Phase 2: route a human's approval decision to a child sub-agent's blocked
//! gated tool.
//!
//! When an out-of-process child worker hits a gated tool, the host surfaces an
//! `AgentEvent::ChildApprovalRequested { child_session_id, request_id, .. }` on
//! the agent stream. The frontend posts the human's choice here, and we deliver
//! it over the child's live actor connection — the worker's gated tool is
//! parked on the matching `host.approval_call`, so approve lets it proceed and
//! deny fails it closed. See `bamboo_engine::external_agents::live`.

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;

/// Body for `POST /api/v1/child-approval/{child_session_id}`.
#[derive(Debug, Deserialize)]
pub struct ChildApprovalDecision {
    /// The `request_id` carried by the surfaced `ChildApprovalRequested` event.
    pub request_id: String,
    /// Whether the human approved (`true`) or denied (`false`) the gated action.
    pub approved: bool,
}

#[derive(Debug, Serialize)]
struct ChildApprovalResponse {
    delivered: bool,
}

/// Deliver a human approval decision to a live child sub-agent (Phase 2).
///
/// `POST /api/v1/child-approval/{child_session_id}`
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<ChildApprovalDecision>,
) -> impl Responder {
    let child_session_id = path.into_inner();
    let ChildApprovalDecision {
        request_id,
        approved,
    } = body.into_inner();

    let delivered = bamboo_engine::external_agents::live::deliver_approval_checked(
        Some(&state.approval_registry),
        &child_session_id,
        &request_id,
        approved,
    );

    if delivered {
        tracing::info!(
            "[{}] child approval delivered (request_id={}, approved={})",
            child_session_id,
            request_id,
            approved
        );
        HttpResponse::Ok().json(ChildApprovalResponse { delivered: true })
    } else {
        // Not delivered: the request_id isn't a currently-pending human-loop
        // approval — unknown/guessed, already answered, timed out, or the child
        // isn't live (finished/disconnected). The checked entry consumes pending
        // ids one-shot, so a replay also lands here.
        tracing::warn!(
            "[{}] child approval not delivered (request_id={}): not a pending request",
            child_session_id,
            request_id
        );
        HttpResponse::NotFound().json(ChildApprovalResponse { delivered: false })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_body_deserializes() {
        let v: ChildApprovalDecision =
            serde_json::from_str(r#"{"request_id":"r1","approved":true}"#).unwrap();
        assert_eq!(v.request_id, "r1");
        assert!(v.approved);
    }
}
