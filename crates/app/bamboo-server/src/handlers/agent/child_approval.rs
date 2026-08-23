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
    /// Parent session that surfaced the durable approval record.
    pub parent_session_id: String,
    /// Child execution attempt that created this request.
    pub child_attempt: u32,
    /// The `request_id` carried by the surfaced `ChildApprovalRequested` event.
    pub request_id: String,
    /// Exact durable approval version displayed to the operator.
    pub expected_version: u64,
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
        parent_session_id,
        child_attempt,
        request_id,
        expected_version,
        approved,
    } = body.into_inner();

    if parent_session_id.trim().is_empty() || request_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(ChildApprovalResponse { delivered: false });
    }

    let outcome = bamboo_engine::external_agents::live::deliver_approval_checked_cas(
        Some(&state.approval_registry),
        &parent_session_id,
        &child_session_id,
        child_attempt,
        &request_id,
        expected_version,
        approved,
    );

    match outcome {
        bamboo_engine::external_agents::live::ApprovalDeliveryResult::Delivered => {
            tracing::info!(
                "[{}] child approval delivered (parent_session_id={}, child_attempt={}, request_id={}, expected_version={}, approved={})",
                child_session_id,
                parent_session_id,
                child_attempt,
                request_id,
                expected_version,
                approved
            );
            HttpResponse::Ok().json(ChildApprovalResponse { delivered: true })
        }
        bamboo_engine::external_agents::live::ApprovalDeliveryResult::Conflict => {
            tracing::warn!(
                "[{}] child approval rejected due to stale durable identity (parent_session_id={}, child_attempt={}, request_id={}, expected_version={})",
                child_session_id,
                parent_session_id,
                child_attempt,
                request_id,
                expected_version
            );
            HttpResponse::Conflict().json(ChildApprovalResponse { delivered: false })
        }
        bamboo_engine::external_agents::live::ApprovalDeliveryResult::NotFound
        | bamboo_engine::external_agents::live::ApprovalDeliveryResult::DeliveryFailed => {
            // Unknown/already-resolved requests and children whose live
            // transport disappeared remain non-deliverable and fail closed.
            tracing::warn!(
                "[{}] child approval not delivered (parent_session_id={}, child_attempt={}, request_id={}, expected_version={})",
                child_session_id,
                parent_session_id,
                child_attempt,
                request_id,
                expected_version
            );
            HttpResponse::NotFound().json(ChildApprovalResponse { delivered: false })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};
    use bamboo_subagent::proto::ParentFrame;

    #[actix_web::test]
    async fn decision_body_deserializes() {
        let v: ChildApprovalDecision = serde_json::from_str(
            r#"{"parent_session_id":"p1","child_attempt":2,"request_id":"r1","expected_version":7,"approved":true}"#,
        )
        .unwrap();
        assert_eq!(v.parent_session_id, "p1");
        assert_eq!(v.child_attempt, 2);
        assert_eq!(v.request_id, "r1");
        assert_eq!(v.expected_version, 7);
        assert!(v.approved);
    }

    #[actix_web::test]
    async fn legacy_decision_body_without_cas_identity_fails_closed() {
        assert!(serde_json::from_str::<ChildApprovalDecision>(
            r#"{"request_id":"r1","approved":true}"#
        )
        .is_err());
    }

    #[actix_web::test]
    async fn http_handler_conflicts_on_parent_or_version_mismatch_without_consuming() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let child_id = "child-http-cas";
        let request_id = "request-http-cas";
        let (wire_tx, mut wire_rx) = tokio::sync::mpsc::unbounded_channel();
        let _guard = bamboo_engine::external_agents::live::register(
            child_id,
            wire_tx,
            4,
            Some(state.approval_registry.clone()),
        );
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(4);
        let (version, _) = bamboo_engine::external_agents::live::observe_pending_approval(
            bamboo_engine::external_agents::live::PendingApprovalObservation {
                registry: Some(&state.approval_registry),
                parent_session_id: "parent-http-cas",
                child_id,
                child_attempt: 4,
                request_id,
                tool_name: "Bash",
                permission: "execute",
                resource: "cargo test",
                event_tx,
            },
        );
        let app = test::init_service(App::new().app_data(state).route(
            "/api/v1/child-approval/{child_session_id}",
            web::post().to(handler),
        ))
        .await;

        let missing_identity = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/child-approval/{child_id}"))
                .set_json(serde_json::json!({
                    "request_id": request_id,
                    "approved": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(missing_identity.status(), StatusCode::BAD_REQUEST);

        let wrong_parent = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/child-approval/{child_id}"))
                .set_json(serde_json::json!({
                    "parent_session_id": "parent-stale",
                    "child_attempt": 4,
                    "request_id": request_id,
                    "expected_version": version,
                    "approved": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(wrong_parent.status(), StatusCode::CONFLICT);

        let wrong_version = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/child-approval/{child_id}"))
                .set_json(serde_json::json!({
                    "parent_session_id": "parent-http-cas",
                    "child_attempt": 4,
                    "request_id": request_id,
                    "expected_version": version.saturating_add(1),
                    "approved": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(wrong_version.status(), StatusCode::CONFLICT);
        assert!(wire_rx.try_recv().is_err());

        let accepted = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/child-approval/{child_id}"))
                .set_json(serde_json::json!({
                    "parent_session_id": "parent-http-cas",
                    "child_attempt": 4,
                    "request_id": request_id,
                    "expected_version": version,
                    "approved": false
                }))
                .to_request(),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::OK);
        assert!(matches!(
            wire_rx.try_recv(),
            Ok(ParentFrame::ApprovalReply { id, approved }) if id == request_id && !approved
        ));
    }
}
