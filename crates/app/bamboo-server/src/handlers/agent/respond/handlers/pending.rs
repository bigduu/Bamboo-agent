use actix_web::{web, HttpResponse, Result};
use bamboo_agent_core::{PendingQuestion, Role, Session};
use bamboo_tools::permission::{
    PermissionConfig, PermissionDecisionKind, PermissionDecisionReceipt, PermissionEvaluation,
    PermissionOutcome, PermissionRequest,
};

use crate::app_state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingInteractionKind {
    Permission,
    Clarification,
}

impl PendingInteractionKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Clarification => "clarification",
        }
    }
}

pub(super) struct PendingInteraction {
    pub(super) kind: PendingInteractionKind,
    pub(super) permission_request: Option<PermissionRequest>,
}

fn request_matches_pending(
    request: &PermissionRequest,
    session: &Session,
    pending: &PendingQuestion,
) -> bool {
    request.session_id == session.id
        && request.request_id == pending.tool_call_id
        && !request.request_generation.trim().is_empty()
}

fn persisted_permission_payload(
    session: &Session,
    tool_call_id: &str,
) -> Option<serde_json::Value> {
    let message = session.messages.iter().rev().find(|message| {
        matches!(&message.role, Role::Tool) && message.tool_call_id.as_deref() == Some(tool_call_id)
    })?;
    let payload = serde_json::from_str::<serde_json::Value>(&message.content).ok()?;
    (payload.get("status").and_then(serde_json::Value::as_str)
        == Some("awaiting_permission_approval"))
    .then_some(payload)
}

pub(super) fn persisted_permission_request(
    session: &Session,
    tool_call_id: &str,
) -> Option<PermissionRequest> {
    let message = session.messages.iter().rev().find(|message| {
        matches!(&message.role, Role::Tool) && message.tool_call_id.as_deref() == Some(tool_call_id)
    })?;
    let from_metadata = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("permission_request"))
        .cloned();
    from_metadata
        .or_else(|| {
            serde_json::from_str::<serde_json::Value>(&message.content)
                .ok()
                .as_ref()
                .and_then(|payload| payload.get("permission_request"))
                .cloned()
        })
        .and_then(|request| serde_json::from_value::<PermissionRequest>(request).ok())
        .filter(|request| {
            request.session_id == session.id
                && request.request_id == tool_call_id
                && !request.request_generation.trim().is_empty()
        })
}

pub(super) fn persisted_permission_decision_receipt(
    session: &Session,
    tool_call_id: &str,
) -> Option<PermissionDecisionReceipt> {
    let message = session.messages.iter().rev().find(|message| {
        matches!(&message.role, Role::Tool) && message.tool_call_id.as_deref() == Some(tool_call_id)
    })?;
    let receipt = message
        .metadata
        .as_ref()?
        .get("permission_decision_receipt")?
        .clone();
    serde_json::from_value::<PermissionDecisionReceipt>(receipt)
        .ok()
        .filter(|receipt| {
            receipt.session_id == session.id
                && receipt.decision.request_id == tool_call_id
                && !receipt.decision.request_generation.trim().is_empty()
        })
}

pub(super) fn resolve_pending_interaction(
    session: &Session,
    pending: &PendingQuestion,
    in_memory_request: Option<PermissionRequest>,
) -> PendingInteraction {
    let in_memory_request =
        in_memory_request.filter(|request| request_matches_pending(request, session, pending));
    let persisted_payload = persisted_permission_payload(session, &pending.tool_call_id);
    let persisted_request = persisted_permission_request(session, &pending.tool_call_id)
        .filter(|request| request_matches_pending(request, session, pending));
    let permission_request = in_memory_request.or(persisted_request);
    let kind = if permission_request.is_some() || persisted_payload.is_some() {
        PendingInteractionKind::Permission
    } else {
        PendingInteractionKind::Clarification
    };

    PendingInteraction {
        kind,
        permission_request,
    }
}

const MAX_PENDING_TOOL_ARGUMENT_BYTES: usize = 16 * 1024;

fn pending_tool_argument_text<'a>(session: &'a Session, tool_call_id: &str) -> Option<&'a str> {
    session.messages.iter().rev().find_map(|message| {
        if !matches!(&message.role, Role::Assistant) {
            return None;
        }
        message
            .tool_calls
            .as_ref()?
            .iter()
            .find(|tool_call| tool_call.id == tool_call_id)
            .map(|tool_call| tool_call.function.arguments.as_str())
    })
}

pub(super) fn pending_tool_arguments_exact(
    session: &Session,
    tool_call_id: &str,
) -> Option<serde_json::Value> {
    let arguments = pending_tool_argument_text(session, tool_call_id)?;
    Some(
        serde_json::from_str(arguments)
            .unwrap_or_else(|_| serde_json::Value::String(arguments.to_string())),
    )
}

/// Re-evaluate a parked operation against the current policy without consuming
/// any grant. A stale contract is never revision-rebased blindly. A fresh
/// `Ask` replaces it normally; an `Allow` outcome still requires the user's
/// explicit choice, while a `Deny` outcome narrows the contract to denial-only
/// choices. This keeps the parked operation recoverable without auto-approval
/// or allowing a stale prompt to override a newer deny.
pub(super) fn refresh_request_for_current_policy(
    config: &PermissionConfig,
    session: &Session,
    pending: &PendingQuestion,
    request: &PermissionRequest,
) -> Option<PermissionRequest> {
    if request.policy_revision == config.policy_revision() {
        return Some(request.clone());
    }
    let tool_args = pending_tool_arguments_exact(session, pending.tool_call_id.as_str())
        .unwrap_or(serde_json::Value::Null);
    match config.evaluate(PermissionEvaluation {
        request_id: request.request_id.clone(),
        session_id: request.session_id.clone(),
        workspace_path: request.workspace_path.clone(),
        tool_name: request.tool_name.clone(),
        tool_args,
        permission_type: request.permission_type,
        resource: request.resource.clone(),
        operation_summary: request.operation_summary.clone(),
        risk_level: request.risk_level,
        bypass_requested: request.bypass_requested,
        auto_approve_requested: request.auto_approve_requested,
        platform_hard_deny: None,
        consume_once: false,
        // Never widen a parked request merely because policy changed. The
        // executor's original capability boundary remains authoritative.
        supported_decisions: request.allowed_decisions.clone(),
    }) {
        PermissionOutcome::Ask(mut refreshed) => {
            // This is still the same parked operation; only its policy view is
            // refreshed. Rotating generation here would invalidate the exact
            // decision identity shown to the operator.
            refreshed.request_generation = request.request_generation.clone();
            Some(refreshed)
        }
        PermissionOutcome::Allow {
            effective_policy, ..
        } => {
            let mut refreshed = request.clone();
            refreshed.policy_revision = effective_policy.revision;
            refreshed.effective_mode = effective_policy.mode;
            Some(refreshed)
        }
        PermissionOutcome::Deny {
            reason,
            effective_policy,
        } => {
            let mut refreshed = request.clone();
            refreshed.policy_revision = effective_policy.revision;
            refreshed.effective_mode = effective_policy.mode;
            refreshed.reason_code = reason.code;
            refreshed.matched_rule = reason.matched_rule;
            refreshed.allowed_decisions.retain(|decision| {
                matches!(
                    decision,
                    PermissionDecisionKind::DenyOnce | PermissionDecisionKind::DenySession
                )
            });
            (!refreshed.allowed_decisions.is_empty()).then_some(refreshed)
        }
    }
}

fn pending_tool_arguments(
    session: &Session,
    tool_call_id: &str,
) -> Option<(serde_json::Value, bool)> {
    let arguments = pending_tool_argument_text(session, tool_call_id)?;
    if arguments.len() <= MAX_PENDING_TOOL_ARGUMENT_BYTES {
        return Some((
            serde_json::from_str(arguments)
                .unwrap_or_else(|_| serde_json::Value::String(arguments.to_string())),
            false,
        ));
    }

    let mut boundary = MAX_PENDING_TOOL_ARGUMENT_BYTES.min(arguments.len());
    while boundary > 0 && !arguments.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut preview = arguments[..boundary].to_string();
    preview.push('…');
    Some((serde_json::Value::String(preview), true))
}

/// Get the pending question for a session (if any).
///
/// This endpoint retrieves the current pending question that the agent
/// is waiting for the user to answer.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/question`
pub async fn get_pending_question(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    let Some(session) = state.load_session_merged(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found")
        })));
    };

    match session.pending_question.as_ref() {
        Some(pending) => {
            let permission_config = state.permission_checker.permission_config();
            let in_memory_request = permission_config
                .as_ref()
                .and_then(|config| {
                    config.pending_request(&session_id, pending.tool_call_id.as_str())
                })
                .filter(|request| request_matches_pending(request, &session, pending));
            let mut interaction =
                resolve_pending_interaction(&session, pending, in_memory_request.clone());
            if let (Some(config), Some(request)) = (
                permission_config.as_ref(),
                interaction.permission_request.as_ref(),
            ) {
                if request.policy_revision != config.policy_revision() {
                    interaction.permission_request =
                        refresh_request_for_current_policy(config, &session, pending, request);
                }
            }
            if let (Some(config), Some(request)) = (
                permission_config.as_ref(),
                interaction.permission_request.as_ref(),
            ) {
                // Rehydrate or replace only a fully decoded request whose
                // embedded session/request identities matched the durable
                // pending question. Replacement is required after safe policy
                // re-evaluation so a 409 refresh can actually advance.
                config.register_pending_request(request.clone());
            }
            let bounded_tool_arguments = (interaction.kind == PendingInteractionKind::Permission)
                .then(|| pending_tool_arguments(&session, pending.tool_call_id.as_str()))
                .flatten();
            let tool_arguments = bounded_tool_arguments
                .as_ref()
                .map(|(arguments, _)| arguments.clone());
            let tool_arguments_truncated = bounded_tool_arguments
                .as_ref()
                .is_some_and(|(_, truncated)| *truncated);

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "has_pending_question": true,
                "question": pending.question,
                "options": pending.options,
                "allow_custom": pending.allow_custom,
                "tool_call_id": pending.tool_call_id,
                "tool_name": pending.tool_name,
                "source": pending.source,
                "interaction_kind": interaction.kind.as_str(),
                "permission_request": interaction.permission_request,
                "tool_arguments": tool_arguments,
                "tool_arguments_truncated": tool_arguments_truncated,
            })))
        }
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": false,
            "interaction_kind": null,
        }))),
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use bamboo_agent_core::{FunctionCall, Message, PendingQuestionSource, Session, ToolCall};
    use bamboo_tools::permission::{
        PermissionDecisionKind, PermissionMatcher, PermissionMatcherKind, PermissionMode,
        PermissionReasonCode, PermissionRequest, PermissionType, RiskLevel,
    };

    use crate::routes::configure_routes;
    use crate::AppState;

    fn permission_request(session_id: &str, request_id: &str) -> PermissionRequest {
        PermissionRequest {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            session_id: session_id.to_string(),
            workspace_path: Some("/workspace".to_string()),
            tool_name: "Bash".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            resource: "cargo test".to_string(),
            operation_summary: "Run cargo test".to_string(),
            risk_level: RiskLevel::High,
            reason_code: PermissionReasonCode::RiskThreshold,
            effective_mode: PermissionMode::Default,
            bypass_requested: false,
            auto_approve_requested: false,
            policy_revision: 0,
            matched_rule: None,
            allowed_decisions: vec![
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::DenyOnce,
            ],
            suggested_matchers: vec![PermissionMatcher {
                id: "exact_resource".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: "cargo test".to_string(),
            }],
        }
    }

    fn assistant_tool_call(tool_call_id: &str, arguments: &str) -> Message {
        Message::assistant(
            "",
            Some(vec![ToolCall {
                id: tool_call_id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Bash".to_string(),
                    arguments: arguments.to_string(),
                },
            }]),
        )
    }

    fn permission_tool_result(
        tool_call_id: &str,
        permission_request: serde_json::Value,
    ) -> Message {
        Message::tool_result(
            tool_call_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "question": "Allow cargo test?",
                "permission_type": "execute_command",
                "resource": "cargo test",
                "options": ["Approve", "Deny"],
                "allow_custom": false,
                "permission_request": permission_request,
            })
            .to_string(),
        )
    }

    /// `GET /api/v1/sessions/{id}/respond/pending` for an unknown session must
    /// use the canonical nested error envelope (`{"error": {"message",
    /// "type"}}`), not the old flat `{"error": "<string>"}` shape. #251/#507.
    #[actix_web::test]
    async fn get_pending_question_not_found_uses_canonical_error_envelope() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions/does-not-exist/respond/pending")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
    }

    #[actix_web::test]
    async fn get_pending_question_includes_matching_typed_request_and_json_arguments() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "typed-permission-pending";
        let tool_call_id = "permission-call-1";
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(assistant_tool_call(
            tool_call_id,
            r#"{"command":"cargo test","timeout":30}"#,
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "Bash".to_string(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;
        state
            .permission_checker
            .permission_config()
            .expect("typed permission config")
            .register_pending_request(permission_request(session_id, tool_call_id));

        let response = get_pending_question(state, web::Path::from(session_id.to_string()))
            .await
            .expect("pending response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: Value = serde_json::from_slice(&body).expect("response JSON");

        assert_eq!(body["interaction_kind"], "permission");
        assert_eq!(body["permission_request"]["request_id"], tool_call_id);
        assert_eq!(
            body["permission_request"]["allowed_decisions"],
            serde_json::json!(["allow_once", "deny_once"])
        );
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({"command": "cargo test", "timeout": 30})
        );
    }

    #[actix_web::test]
    async fn ordinary_clarification_omits_typed_request_and_tool_arguments() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "ordinary-clarification-pending";
        let tool_call_id = "clarification-call-1";
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(assistant_tool_call(
            tool_call_id,
            r#"{"question":"Choose one"}"#,
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "conclusion_with_options".to_string(),
            "Choose one".to_string(),
            vec!["A".to_string(), "B".to_string()],
            false,
            PendingQuestionSource::AgenticClarification,
        );
        state.save_and_cache_session(&mut session).await;

        let response = get_pending_question(state, web::Path::from(session_id.to_string()))
            .await
            .expect("pending response");
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: Value = serde_json::from_slice(&body).expect("response JSON");

        assert_eq!(body["interaction_kind"], "clarification");
        assert!(body["permission_request"].is_null());
        assert!(body["tool_arguments"].is_null());
    }

    #[actix_web::test]
    async fn get_pending_question_recovers_typed_request_after_map_loss() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_id = "typed-permission-restart";
        let tool_call_id = "permission-call-after-restart";
        let request = permission_request(session_id, tool_call_id);

        let state_before = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("initial app state"),
        );
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(assistant_tool_call(
            tool_call_id,
            r#"{"command":"cargo test"}"#,
        ));
        session.messages.push(permission_tool_result(
            tool_call_id,
            serde_json::to_value(&request).expect("permission request JSON"),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "Bash".to_string(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state_before.save_and_cache_session(&mut session).await;
        assert!(state_before
            .permission_checker
            .permission_config()
            .expect("initial permission config")
            .pending_request(session_id, tool_call_id)
            .is_none());
        drop(state_before);

        let state_after = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("restarted app state"),
        );
        assert!(state_after
            .permission_checker
            .permission_config()
            .expect("restarted permission config")
            .pending_request(session_id, tool_call_id)
            .is_none());

        let response =
            get_pending_question(state_after.clone(), web::Path::from(session_id.to_string()))
                .await
                .expect("pending response");
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: Value = serde_json::from_slice(&body).expect("response JSON");

        assert_eq!(body["interaction_kind"], "permission");
        assert_eq!(body["permission_request"]["request_id"], tool_call_id);
        assert_eq!(body["permission_request"]["session_id"], session_id);
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({"command": "cargo test"})
        );
        let rehydrated = state_after
            .permission_checker
            .permission_config()
            .expect("restarted permission config")
            .pending_request(session_id, tool_call_id)
            .expect("validated request is rehydrated");
        assert_eq!(rehydrated, request);
    }

    #[actix_web::test]
    async fn reused_tool_call_id_reads_only_latest_persisted_generation() {
        let session_id = "reused-persisted-generation";
        let request_id = "provider-reused-id";
        let mut old_request = permission_request(session_id, request_id);
        old_request.request_generation = "generation-old".to_string();
        old_request.resource = "cargo test --old".to_string();
        let mut current_request = permission_request(session_id, request_id);
        current_request.request_generation = "generation-current".to_string();
        current_request.resource = "cargo test --current".to_string();

        let mut session = Session::new(session_id, "test-model");
        let mut old_result =
            Message::tool_result_with_status(request_id, "Selected response: Approve", true);
        old_result.metadata = Some(serde_json::json!({
            "permission_request": old_request,
            "permission_decision_receipt": {
                "session_id": session_id,
                "decision": {
                    "request_id": request_id,
                    "request_generation": "generation-old",
                    "decision": "allow_once",
                    "confirm_global": false
                },
                "decided_at": "2026-08-14T00:00:00Z"
            }
        }));
        session.messages.push(old_result);
        session.messages.push(permission_tool_result(
            request_id,
            serde_json::to_value(&current_request).expect("current request JSON"),
        ));

        assert_eq!(
            persisted_permission_request(&session, request_id),
            Some(current_request)
        );
        assert!(persisted_permission_decision_receipt(&session, request_id).is_none());
        assert_eq!(
            persisted_permission_payload(&session, request_id)
                .and_then(|payload| payload.get("permission_request").cloned())
                .and_then(|request| request.get("request_generation").cloned()),
            Some(serde_json::json!("generation-current"))
        );
    }

    #[actix_web::test]
    async fn malformed_typed_payload_remains_a_fail_closed_permission_interaction() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "malformed-typed-permission";
        let tool_call_id = "malformed-permission-call";
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(assistant_tool_call(
            tool_call_id,
            r#"{"command":"cargo test"}"#,
        ));
        session.messages.push(permission_tool_result(
            tool_call_id,
            serde_json::json!({
                "request_id": tool_call_id,
                "session_id": session_id,
                "allowed_decisions": "not-an-array"
            }),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "Bash".to_string(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;

        let response = get_pending_question(state, web::Path::from(session_id.to_string()))
            .await
            .expect("pending response");
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: Value = serde_json::from_slice(&body).expect("response JSON");

        assert_eq!(body["interaction_kind"], "permission");
        assert!(body["permission_request"].is_null());
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({"command": "cargo test"})
        );
    }

    #[actix_web::test]
    async fn pending_tool_arguments_preserve_invalid_json_as_a_string() {
        let mut session = Session::new("invalid-arguments", "test-model");
        session
            .messages
            .push(assistant_tool_call("call-invalid", "{not-json"));

        assert_eq!(
            pending_tool_arguments(&session, "call-invalid"),
            Some((Value::String("{not-json".to_string()), false))
        );
    }

    #[actix_web::test]
    async fn pending_tool_arguments_are_utf8_safely_bounded_before_json_formatting() {
        let raw = format!(
            r#"{{"payload":"{}"}}"#,
            "界".repeat(MAX_PENDING_TOOL_ARGUMENT_BYTES)
        );
        let mut session = Session::new("large-arguments", "test-model");
        session
            .messages
            .push(assistant_tool_call("call-large", &raw));

        let (preview, truncated) =
            pending_tool_arguments(&session, "call-large").expect("bounded arguments");
        assert!(truncated);
        let preview = preview.as_str().expect("large preview is a string");
        assert!(preview.ends_with('…'));
        assert!(preview.len() <= MAX_PENDING_TOOL_ARGUMENT_BYTES + '…'.len_utf8());
        assert!(std::str::from_utf8(preview.as_bytes()).is_ok());
    }
}
