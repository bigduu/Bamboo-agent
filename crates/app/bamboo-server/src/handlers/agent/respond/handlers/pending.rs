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

fn is_browser_display_tool_name(tool_name: &str) -> bool {
    tool_name
        .trim()
        .rsplit("::")
        .next()
        .is_some_and(|name| name.trim().eq_ignore_ascii_case("browser"))
}

fn is_browser_eval_display_tool_name(tool_name: &str) -> bool {
    tool_name
        .trim()
        .rsplit("::")
        .next()
        .is_some_and(|name| name.trim().eq_ignore_ascii_case("browser_eval"))
}

/// Return only a display copy. The original assistant arguments stay in the
/// session for exact decision validation and approved replay, but private
/// browser input and page script source must not reach permission previews.
fn pending_tool_arguments_for_display(
    session: &Session,
    pending: &PendingQuestion,
    request: Option<&PermissionRequest>,
) -> Option<(serde_json::Value, bool, bool)> {
    if is_browser_eval_display_tool_name(&pending.tool_name)
        || request.is_some_and(|request| is_browser_eval_display_tool_name(&request.tool_name))
    {
        let raw = pending_tool_argument_text(session, pending.tool_call_id.as_str())?;
        if raw.len() > MAX_PENDING_TOOL_ARGUMENT_BYTES {
            return Some((serde_json::json!({"arguments":"[omitted]"}), true, false));
        }
        let parsed: serde_json::Value = match serde_json::from_str(raw) {
            Ok(parsed) => parsed,
            Err(_) => return Some((serde_json::json!({"arguments":"[omitted]"}), true, false)),
        };
        return Some((
            serde_json::json!({
                "code":"[redacted]",
                "expected_url":"[redacted]",
                "expected_epoch":parsed.get("expected_epoch").and_then(serde_json::Value::as_u64),
            }),
            false,
            false,
        ));
    }
    if !is_browser_display_tool_name(&pending.tool_name) {
        return pending_tool_arguments(session, pending.tool_call_id.as_str())
            .map(|(args, truncated)| (args, truncated, false));
    }
    let raw = pending_tool_argument_text(session, pending.tool_call_id.as_str())?;
    let parked_focused = request.is_some_and(PermissionRequest::is_focused_browser_input);
    if raw.len() > MAX_PENDING_TOOL_ARGUMENT_BYTES {
        return Some((
            serde_json::json!({"arguments":"[omitted]"}),
            true,
            parked_focused,
        ));
    }
    let parsed: serde_json::Value = match serde_json::from_str(raw) {
        Ok(parsed) => parsed,
        Err(_) => {
            return Some((
                serde_json::json!({"arguments":"[omitted]"}),
                true,
                parked_focused,
            ));
        }
    };
    if parked_focused || bamboo_tools::permission::is_focused_browser_input("browser", &parsed) {
        return Some((
            match parsed.get("action").and_then(serde_json::Value::as_str) {
                Some("type") => serde_json::json!({"action":"type","text":"[redacted]"}),
                Some("key" | "press") => serde_json::json!({
                    "action":parsed["action"],
                    "key":"[redacted]",
                }),
                Some("dialog_respond") => serde_json::json!({
                    "action":"dialog_respond",
                    "dialog_id":parsed.get("dialog_id"),
                    "accept":parsed.get("accept"),
                    "text":"[redacted]",
                }),
                _ => serde_json::json!({"arguments":"[omitted]"}),
            },
            false,
            true,
        ));
    }
    if bamboo_tools::permission::is_native_browser_select("browser", &parsed) {
        return Some((serde_json::json!({"action":"select_option"}), false, false));
    }
    Some((parsed, false, false))
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
                .then(|| {
                    pending_tool_arguments_for_display(
                        &session,
                        pending,
                        interaction.permission_request.as_ref(),
                    )
                })
                .flatten();
            let tool_arguments = bounded_tool_arguments
                .as_ref()
                .map(|(arguments, _, _)| arguments.clone());
            let tool_arguments_truncated = bounded_tool_arguments
                .as_ref()
                .is_some_and(|(_, truncated, _)| *truncated);
            // A parked browser approval may outlive a missing or undecodable
            // request. Never reuse its original question when the action
            // cannot be inspected safely; it may quote private input.
            let browser_arguments_unavailable = interaction.kind
                == PendingInteractionKind::Permission
                && (is_browser_display_tool_name(&pending.tool_name)
                    || is_browser_eval_display_tool_name(&pending.tool_name)
                    || interaction
                        .permission_request
                        .as_ref()
                        .is_some_and(|request| {
                            is_browser_eval_display_tool_name(&request.tool_name)
                        }))
                && bounded_tool_arguments
                    .as_ref()
                    .is_none_or(|(_, truncated, _)| *truncated);
            let focused_browser_input = interaction
                .permission_request
                .as_ref()
                .is_some_and(PermissionRequest::is_focused_browser_input)
                || bounded_tool_arguments
                    .as_ref()
                    .is_some_and(|(_, _, focused)| *focused);
            let native_browser_select = tool_arguments.as_ref().is_some_and(|arguments| {
                bamboo_tools::permission::is_native_browser_select("browser", arguments)
            });
            let dialog_response = tool_arguments
                .as_ref()
                .and_then(|arguments| arguments.get("action").and_then(serde_json::Value::as_str))
                == Some("dialog_respond");
            let browser_eval = is_browser_eval_display_tool_name(&pending.tool_name)
                || interaction
                    .permission_request
                    .as_ref()
                    .is_some_and(|request| is_browser_eval_display_tool_name(&request.tool_name));
            let permission_request_for_display =
                interaction.permission_request.map(|mut request| {
                    if request.has_private_browser_resource()
                        || native_browser_select
                        || dialog_response
                        || browser_eval
                        || browser_arguments_unavailable
                    {
                        // Keep the exact request registered for receipt matching,
                        // but do not send its private resource to approval UIs.
                        request.resource = "[redacted]".to_string();
                        request.operation_summary = if dialog_response {
                            "Answer pending browser dialog"
                        } else if browser_eval {
                            "Execute browser page JavaScript"
                        } else if native_browser_select {
                            "Select native browser options"
                        } else {
                            "Focused browser input"
                        }
                        .to_string();
                        request.matched_rule = None;
                        request.suggested_matchers.clear();
                    }
                    request
                });

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "has_pending_question": true,
                "question": if browser_arguments_unavailable {
                    "Approve browser action?"
                } else if dialog_response {
                    "Approve browser dialog response?"
                } else if focused_browser_input {
                    "Approve focused browser input?"
                } else if native_browser_select {
                    "Approve native browser selection?"
                } else if browser_eval {
                    "Approve browser page JavaScript on the active page?"
                } else {
                    pending.question.as_str()
                },
                "options": pending.options,
                "allow_custom": pending.allow_custom,
                "tool_call_id": pending.tool_call_id,
                "tool_name": pending.tool_name,
                "source": pending.source,
                "interaction_kind": interaction.kind.as_str(),
                "permission_request": permission_request_for_display,
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

    fn assistant_browser_call(tool_call_id: &str, arguments: &str) -> Message {
        assistant_named_browser_call(tool_call_id, "browser", arguments)
    }

    fn assistant_named_browser_call(
        tool_call_id: &str,
        tool_name: &str,
        arguments: &str,
    ) -> Message {
        assistant_named_tool_call(tool_name, tool_call_id, arguments)
    }

    fn assistant_named_tool_call(tool_name: &str, tool_call_id: &str, arguments: &str) -> Message {
        Message::assistant(
            "",
            Some(vec![ToolCall {
                id: tool_call_id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: tool_name.to_string(),
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
    async fn get_pending_question_redacts_focused_input_and_preserves_selector_literal() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let private_input = "private browser input";
        for (action, argument, resource, selector) in [
            ("type", "text", "browser:17:type:focused:opaque", None),
            ("key", "key", "browser:17:key:opaque", None),
            ("press", "key", "browser:17:press:focused:key:opaque", None),
            ("fill", "text", "browser:17:fill:#account", Some("#account")),
            (
                "press",
                "key",
                "browser:17:press:#account:key:opaque",
                Some("#account"),
            ),
        ] {
            let focused = selector.is_none();
            let input = if focused { private_input } else { "[redacted]" };
            let session_id = format!("browser-{action}-{focused}-display");
            let tool_call_id = format!("browser-{action}-{focused}-call");
            let mut args = serde_json::json!({"action":action,"expected_epoch":17});
            args[argument] = serde_json::json!(input);
            if let Some(selector) = selector {
                args["selector"] = serde_json::json!(selector);
            }
            let original_args = args.to_string();
            let mut request = permission_request(&session_id, &tool_call_id);
            request.tool_name = "browser".to_string();
            request.permission_type = PermissionType::BrowserInteraction;
            request.resource = resource.to_string();
            request.operation_summary = format!("Send {input} to browser element");
            request.suggested_matchers[0].value = resource.to_string();
            let mut session = Session::new(session_id.as_str(), "test-model");
            session.messages.push(assistant_browser_call(
                tool_call_id.as_str(),
                &original_args,
            ));
            session.messages.push(Message::tool_result(
                tool_call_id.as_str(),
                serde_json::json!({
                    "status":"awaiting_permission_approval",
                    "question":format!("Approve {input}?"),
                    "permission_request":request,
                })
                .to_string(),
            ));
            session.set_pending_question_with_source(
                tool_call_id.clone(),
                "browser".to_string(),
                format!("Approve {input}?"),
                vec!["Approve".to_string(), "Deny".to_string()],
                false,
                PendingQuestionSource::PauseTool,
            );
            state.save_and_cache_session(&mut session).await;

            let response = get_pending_question(state.clone(), web::Path::from(session_id.clone()))
                .await
                .expect("pending response");
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .expect("response body");
            let body: Value = serde_json::from_slice(&body).expect("response JSON");
            assert_eq!(
                body["question"],
                if focused {
                    "Approve focused browser input?".to_string()
                } else {
                    format!("Approve {input}?")
                }
            );
            let mut expected = serde_json::json!({"action":action});
            expected[argument] = serde_json::json!("[redacted]");
            if let Some(selector) = selector {
                expected["selector"] = serde_json::json!(selector);
                expected["expected_epoch"] = serde_json::json!(17);
            }
            assert_eq!(body["tool_arguments"], expected);
            if focused {
                assert_eq!(body["permission_request"]["resource"], "[redacted]");
                assert_eq!(
                    body["permission_request"]["operation_summary"],
                    "Focused browser input"
                );
                assert!(body["permission_request"]["suggested_matchers"]
                    .as_array()
                    .is_some_and(Vec::is_empty));
                assert!(!body.to_string().contains(private_input));
                assert!(!body.to_string().contains("opaque"));
            } else {
                assert_eq!(body["permission_request"]["resource"], resource);
            }
            assert_eq!(
                pending_tool_arguments_exact(&session, &tool_call_id).unwrap()[argument],
                input,
                "presentation redaction must preserve the parked invocation"
            );
        }
    }

    #[actix_web::test]
    async fn get_pending_question_hides_native_select_values_but_keeps_parked_call() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "native-select-private-display";
        let tool_call_id = "native-select-call";
        let private_value = "private-option-value";
        let private_selector = "select[data-private='account']";
        let args = serde_json::json!({
            "action":"select_option",
            "selector":private_selector,
            "values":[private_value],
            "expected_epoch":17,
        });
        let mut request = permission_request(session_id, tool_call_id);
        request.tool_name = "browser".to_string();
        request.permission_type = PermissionType::BrowserInteraction;
        request.resource = "browser:17:select_option:options:private-fingerprint".to_string();
        request.operation_summary = "Select native browser options".to_string();
        request.suggested_matchers[0].value = request.resource.clone();
        let mut session = Session::new(session_id, "test-model");
        session
            .messages
            .push(assistant_browser_call(tool_call_id, &args.to_string()));
        session.messages.push(Message::tool_result(
            tool_call_id,
            serde_json::json!({
                "status":"awaiting_permission_approval",
                "question":format!("Approve {private_value}?"),
                "permission_request":request,
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "browser".to_string(),
            format!("Approve {private_value}?"),
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
        assert_eq!(body["question"], "Approve native browser selection?");
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({"action":"select_option"})
        );
        assert_eq!(body["permission_request"]["resource"], "[redacted]");
        assert_eq!(
            body["permission_request"]["operation_summary"],
            "Select native browser options"
        );
        assert_eq!(
            body["permission_request"]["suggested_matchers"],
            serde_json::json!([])
        );
        for private in [private_value, private_selector, "private-fingerprint"] {
            assert!(!body.to_string().contains(private));
        }
        assert_eq!(
            pending_tool_arguments_exact(&session, tool_call_id).unwrap(),
            args
        );
    }

    #[actix_web::test]
    async fn namespaced_native_select_pending_projection_hides_values_and_resource() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "namespaced-native-select";
        let tool_call_id = "namespaced-select-call";
        let args = serde_json::json!({
            "action":"select_option",
            "selector":"select[data-private='account']",
            "values":["private-option-value"],
            "expected_epoch":17,
            "extra":{"secret":"private-extra"},
        });
        let mut request = permission_request(session_id, tool_call_id);
        request.tool_name = "default::browser".to_string();
        request.permission_type = PermissionType::BrowserInteraction;
        request.resource = "browser:17:select_option:options:private-fingerprint".to_string();
        request.suggested_matchers[0].value = request.resource.clone();
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(assistant_named_browser_call(
            tool_call_id,
            "default::browser",
            &args.to_string(),
        ));
        session.messages.push(Message::tool_result(
            tool_call_id,
            serde_json::json!({
                "status":"awaiting_permission_approval",
                "question":"Approve private-option-value?",
                "permission_request":request,
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "default::browser".to_string(),
            "Approve private-option-value?".to_string(),
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
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({"action":"select_option"})
        );
        assert_eq!(body["permission_request"]["resource"], "[redacted]");
        assert_eq!(body["question"], "Approve native browser selection?");
        for private in [
            "private-option-value",
            "data-private",
            "private-extra",
            "private-fingerprint",
        ] {
            assert!(!body.to_string().contains(private));
        }
        assert_eq!(
            pending_tool_arguments_exact(&session, tool_call_id),
            Some(args)
        );
    }

    #[actix_web::test]
    async fn pending_browser_dialog_response_hides_prompt_text_and_receipt_resource() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "browser-dialog-display";
        let tool_call_id = "browser-dialog-call";
        let dialog_id = "a".repeat(24);
        let private_text = "private dialog answer";
        let args = serde_json::json!({
            "action":"dialog_respond","dialog_id":dialog_id,
            "accept":true,"text":private_text,"expected_epoch":17
        });
        let mut request = permission_request(session_id, tool_call_id);
        request.tool_name = "browser".to_string();
        request.permission_type = PermissionType::BrowserInteraction;
        request.resource =
            format!("browser:17:dialog_respond:{dialog_id}:accept:private-fingerprint");
        request.operation_summary = format!("Answer {private_text}");
        request.suggested_matchers[0].value = request.resource.clone();
        let mut session = Session::new(session_id, "test-model");
        session
            .messages
            .push(assistant_browser_call(tool_call_id, &args.to_string()));
        session.messages.push(Message::tool_result(
            tool_call_id,
            serde_json::json!({
                "status":"awaiting_permission_approval",
                "question":format!("Approve {private_text}?"),
                "permission_request":request,
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            "browser".to_string(),
            format!("Approve {private_text}?"),
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
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["question"], "Approve browser dialog response?");
        assert_eq!(
            body["tool_arguments"],
            serde_json::json!({
                "action":"dialog_respond","dialog_id":dialog_id,
                "accept":true,"text":"[redacted]"
            })
        );
        assert_eq!(body["permission_request"]["resource"], "[redacted]");
        assert_eq!(
            body["permission_request"]["operation_summary"],
            "Answer pending browser dialog"
        );
        assert!(body["permission_request"]["suggested_matchers"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(!body.to_string().contains(private_text));
        assert!(!body.to_string().contains("private-fingerprint"));
        assert_eq!(
            pending_tool_arguments_exact(&session, tool_call_id).unwrap()["text"],
            private_text
        );
    }

    #[actix_web::test]
    async fn browser_permission_without_decodable_arguments_uses_safe_question() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        for (case, raw_arguments) in [
            ("malformed", Some("{private input".to_string())),
            (
                "oversized",
                Some(format!(
                    "private{}",
                    "x".repeat(MAX_PENDING_TOOL_ARGUMENT_BYTES)
                )),
            ),
            ("missing", None),
        ] {
            let session_id = format!("browser-{case}-without-request");
            let tool_call_id = format!("browser-{case}-call");
            let mut session = Session::new(session_id.as_str(), "test-model");
            if let Some(raw_arguments) = raw_arguments {
                session.messages.push(assistant_browser_call(
                    tool_call_id.as_str(),
                    raw_arguments.as_str(),
                ));
            }
            session.messages.push(Message::tool_result(
                tool_call_id.as_str(),
                serde_json::json!({"status":"awaiting_permission_approval"}).to_string(),
            ));
            session.set_pending_question_with_source(
                tool_call_id.clone(),
                "browser".to_string(),
                "Approve private input?".to_string(),
                vec!["Approve".to_string(), "Deny".to_string()],
                false,
                PendingQuestionSource::PauseTool,
            );
            state.save_and_cache_session(&mut session).await;

            let response = get_pending_question(state.clone(), web::Path::from(session_id))
                .await
                .expect("pending response");
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .expect("response body");
            let body: Value = serde_json::from_slice(&body).expect("response JSON");
            assert_eq!(body["interaction_kind"], "permission");
            assert_eq!(body["question"], "Approve browser action?");
            assert!(body["permission_request"].is_null());
            assert!(!body.to_string().contains("private input"));
            if case == "missing" {
                assert!(body["tool_arguments"].is_null());
            } else {
                assert_eq!(
                    body["tool_arguments"],
                    serde_json::json!({"arguments":"[omitted]"})
                );
            }
        }
    }

    #[actix_web::test]
    async fn get_pending_question_redacts_browser_eval_source_url_and_resource() {
        let temp_dir = tempdir().expect("tempdir");
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        for (tool_name, session_id) in [
            ("browser_eval", "browser-eval-pending-display"),
            (
                "default::browser_eval",
                "namespaced-browser-eval-pending-display",
            ),
        ] {
            let tool_call_id = "browser-eval-call";
            let source = "document.querySelector('#password').value = 'private-source'";
            let url = "https://example.com/account?token=private-query";
            let original_args = serde_json::json!({
                "code":source,
                "expected_url":url,
                "expected_epoch":17,
            })
            .to_string();
            let mut request = permission_request(session_id, tool_call_id);
            request.tool_name = tool_name.to_string();
            request.permission_type = PermissionType::BrowserInteraction;
            request.resource = "browser_eval:17:private-fingerprint".to_string();
            request.operation_summary =
                "Execute browser page JavaScript on https://example.com".into();
            request.suggested_matchers[0].value = request.resource.clone();
            let mut session = Session::new(session_id, "test-model");
            session.messages.push(assistant_named_tool_call(
                tool_name,
                tool_call_id,
                &original_args,
            ));
            session.messages.push(Message::tool_result(
                tool_call_id,
                serde_json::json!({
                    "status":"awaiting_permission_approval",
                    "question":"Approve browser page JavaScript?",
                    "permission_request":request,
                })
                .to_string(),
            ));
            session.set_pending_question_with_source(
                tool_call_id.to_string(),
                tool_name.to_string(),
                "Approve browser page JavaScript?".to_string(),
                vec!["Approve".to_string(), "Deny".to_string()],
                false,
                PendingQuestionSource::PauseTool,
            );
            state.save_and_cache_session(&mut session).await;

            let response =
                get_pending_question(state.clone(), web::Path::from(session_id.to_string()))
                    .await
                    .expect("pending response");
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .expect("response body");
            let body: Value = serde_json::from_slice(&body).expect("response JSON");
            assert_eq!(
                body["question"],
                "Approve browser page JavaScript on the active page?"
            );
            assert_eq!(
                body["tool_arguments"],
                serde_json::json!({
                    "code":"[redacted]",
                    "expected_url":"[redacted]",
                    "expected_epoch":17,
                })
            );
            assert_eq!(body["permission_request"]["resource"], "[redacted]");
            assert_eq!(
                body["permission_request"]["suggested_matchers"],
                serde_json::json!([])
            );
            for secret in [
                "#password",
                "private-source",
                "private-query",
                "private-fingerprint",
            ] {
                assert!(!body.to_string().contains(secret));
            }
            assert_eq!(
                pending_tool_arguments_exact(&session, tool_call_id).unwrap()["code"],
                source
            );
            assert_eq!(
                pending_tool_arguments_exact(&session, tool_call_id).unwrap()["expected_url"],
                url
            );
        }
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

    #[actix_web::test]
    async fn browser_pending_display_omits_oversized_or_malformed_raw_arguments() {
        let display = |raw: &str| {
            let mut session = Session::new("browser-arguments", "test-model");
            session
                .messages
                .push(assistant_browser_call("browser-call", raw));
            session.set_pending_question_with_source(
                "browser-call".to_string(),
                "browser".to_string(),
                "Approve browser action?".to_string(),
                vec!["Approve".to_string(), "Deny".to_string()],
                false,
                PendingQuestionSource::PauseTool,
            );
            pending_tool_arguments_for_display(
                &session,
                session.pending_question.as_ref().unwrap(),
                None,
            )
            .unwrap()
        };
        let oversized = serde_json::json!({
            "action":"type",
            "text":format!("private{}", "x".repeat(MAX_PENDING_TOOL_ARGUMENT_BYTES)),
        })
        .to_string();
        let (preview, truncated, _) = display(&oversized);
        assert_eq!(preview, serde_json::json!({"arguments":"[omitted]"}));
        assert!(truncated);
        let (preview, truncated, _) = display(r#"{"action":"type","text":"private"#);
        assert_eq!(preview, serde_json::json!({"arguments":"[omitted]"}));
        assert!(truncated);
        let (preview, truncated, focused) = display(r##"{"action":"click","selector":"#save"}"##);
        assert_eq!(
            preview,
            serde_json::json!({"action":"click","selector":"#save"})
        );
        assert!(!truncated);
        assert!(!focused);
    }
}
