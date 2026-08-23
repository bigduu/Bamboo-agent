//! Authoritative permission refresh for approved-tool replay.
//!
//! An approval only authorizes the posture that is still active when the
//! suspended tool is about to re-enter the executor. All replay surfaces use
//! this helper so a stale in-memory Auto/Bypass snapshot cannot outrun a newer
//! durable Default/Plan transition.

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    plan_mode_allows_tool, ToolCall, ToolExecutionSessionFlags, ToolResult,
};
use bamboo_agent_core::{AgentError, PendingQuestionSource, Session};
use bamboo_domain::{permission_request_generation, AgentRuntimeState, PermissionMode};
use bamboo_tools::permission::{
    PermissionConfig, PermissionDecision, PermissionDecisionKind, PermissionDecisionReceipt,
    PermissionRequest,
};
use serde::{Deserialize, Serialize};

const PERMISSION_REPLAY_APPROVALS_METADATA_KEY: &str = "permission.replay_approvals.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PermissionReplayApproval {
    request: PermissionRequest,
    decision: PermissionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReparkedPermissionApproval {
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

/// Result of refreshing permission posture immediately before approval replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalReplayDecision {
    /// The approved call may enter the executor with these exact live flags.
    Execute(ToolExecutionSessionFlags),
    /// The approval became stale because the latest posture is Plan and the
    /// original tool mutates state. Callers consume the marker and record a
    /// failed result, but must not emit `ToolStart` or enter the executor.
    BlockedByPlan(ToolExecutionSessionFlags),
}

/// Exact history target of one approved tool replay.
///
/// The result-message position is captured together with the closest preceding
/// tool call so a provider-reused tool-call id cannot redirect replay to an
/// older operation or overwrite an older result.
pub struct PermissionReplayTarget {
    tool_call: ToolCall,
    result_message_index: usize,
    result_message_id: String,
    request_generation: Option<String>,
}

impl PermissionReplayTarget {
    pub fn tool_call(&self) -> &ToolCall {
        &self.tool_call
    }

    pub fn request_generation(&self) -> Option<&str> {
        self.request_generation.as_deref()
    }
}

/// Locate the concrete approved invocation, newest-first and optionally bound
/// to the server-issued permission generation.
pub fn find_permission_replay_target(
    session: &Session,
    tool_call_id: &str,
    request_generation: Option<&str>,
) -> Option<PermissionReplayTarget> {
    let (result_message_index, result_message) =
        session
            .messages
            .iter()
            .enumerate()
            .rev()
            .find(|(_, message)| {
                message.tool_call_id.as_deref() == Some(tool_call_id)
                    && match request_generation {
                        Some(generation) => {
                            permission_request_generation(message).as_deref() == Some(generation)
                        }
                        None => true,
                    }
            })?;
    let tool_call = session.messages[..result_message_index]
        .iter()
        .rev()
        .find_map(|message| {
            message.tool_calls.as_ref().and_then(|calls| {
                calls
                    .iter()
                    .rev()
                    .find(|call| call.id == tool_call_id)
                    .cloned()
            })
        })?;
    let result_generation = permission_request_generation(result_message);
    Some(PermissionReplayTarget {
        tool_call,
        result_message_index,
        result_message_id: result_message.id.clone(),
        request_generation: result_generation,
    })
}

/// Replace only the tool-result message captured by
/// [`find_permission_replay_target`].
pub fn apply_permission_replay_result(
    session: &mut Session,
    target: &PermissionReplayTarget,
    content: String,
    success: bool,
) -> bool {
    let Some(message) = session.messages.get_mut(target.result_message_index) else {
        return false;
    };
    if message.id != target.result_message_id
        || message.tool_call_id.as_deref() != Some(target.tool_call.id.as_str())
        || target
            .request_generation
            .as_deref()
            .is_some_and(|generation| {
                permission_request_generation(message).as_deref() != Some(generation)
            })
    {
        return false;
    }
    message.content = content;
    message.tool_success = Some(success);
    true
}

fn message_permission_contract(
    session: &Session,
    target: &PermissionReplayTarget,
) -> Result<(PermissionRequest, PermissionDecisionReceipt), AgentError> {
    let message = session
        .messages
        .get(target.result_message_index)
        .filter(|message| message.id == target.result_message_id)
        .ok_or_else(|| {
            AgentError::Tool("permission replay result occurrence changed".to_string())
        })?;
    let metadata = message.metadata.as_ref().ok_or_else(|| {
        AgentError::Tool("permission replay is missing durable typed metadata".to_string())
    })?;
    let request = metadata
        .get("permission_request")
        .cloned()
        .and_then(|value| serde_json::from_value::<PermissionRequest>(value).ok())
        .ok_or_else(|| {
            AgentError::Tool("permission replay is missing its durable request".to_string())
        })?;
    let receipt = metadata
        .get("permission_decision_receipt")
        .cloned()
        .and_then(|value| serde_json::from_value::<PermissionDecisionReceipt>(value).ok())
        .ok_or_else(|| {
            AgentError::Tool(
                "permission replay is missing its durable decision receipt".to_string(),
            )
        })?;
    Ok((request, receipt))
}

fn validate_replay_approval(
    session: &Session,
    tool_call_id: &str,
    approval: &PermissionReplayApproval,
) -> Result<(), AgentError> {
    let request = &approval.request;
    let decision = &approval.decision;
    if request.session_id != session.id
        || request.request_id != tool_call_id
        || request.request_generation.trim().is_empty()
        || decision.request_id != request.request_id
        || decision.request_generation != request.request_generation
        || !request.allowed_decisions.contains(&decision.decision)
    {
        return Err(AgentError::Tool(
            "permission replay approval identity is invalid".to_string(),
        ));
    }
    if decision.decision == PermissionDecisionKind::AllowGlobal && !decision.confirm_global {
        return Err(AgentError::Tool(
            "permission replay global approval lacks explicit confirmation".to_string(),
        ));
    }
    Ok(())
}

fn selected_replay_matcher(
    approval: &PermissionReplayApproval,
) -> Result<bamboo_tools::permission::PermissionMatcher, AgentError> {
    let matcher_id = approval.decision.matcher_id.as_deref().ok_or_else(|| {
        AgentError::Tool("remembered permission replay lacks matcher identity".to_string())
    })?;
    approval
        .request
        .suggested_matchers
        .iter()
        .find(|matcher| matcher.id == matcher_id)
        .cloned()
        .ok_or_else(|| {
            AgentError::Tool("remembered permission replay matcher is unavailable".to_string())
        })
}

/// Rebuild the exact runtime capabilities needed by a typed approved replay.
///
/// The current receipt and any earlier contexts approved for the same concrete
/// invocation live on the generation-bound result message. This lets a daemon
/// restart recover an AllowOnce/AllowSession without broadening it, and lets a
/// tool with multiple permission contexts restart its checks while retaining
/// only the contexts the operator already approved.
pub fn restore_permission_replay_authorization(
    config: &PermissionConfig,
    session: &Session,
    target: &PermissionReplayTarget,
) -> Result<(), AgentError> {
    let Some(replay_generation) = target.request_generation() else {
        return Ok(());
    };
    let workspace = session
        .workspace
        .as_deref()
        .map(str::trim)
        .filter(|workspace| !workspace.is_empty())
        .map(ToOwned::to_owned);
    config.set_session_workspace(session.id.clone(), workspace);

    let message = session
        .messages
        .get(target.result_message_index)
        .filter(|message| message.id == target.result_message_id)
        .ok_or_else(|| {
            AgentError::Tool("permission replay result occurrence changed".to_string())
        })?;
    let mut approvals = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(PERMISSION_REPLAY_APPROVALS_METADATA_KEY))
        .cloned()
        .map(serde_json::from_value::<Vec<PermissionReplayApproval>>)
        .transpose()
        .map_err(|error| {
            AgentError::Tool(format!(
                "permission replay approval ledger is invalid: {error}"
            ))
        })?
        .unwrap_or_default();
    let (request, receipt) = message_permission_contract(session, target)?;
    if receipt.session_id != session.id || request.request_generation != replay_generation {
        return Err(AgentError::Tool(
            "permission replay receipt does not match the active generation".to_string(),
        ));
    }
    approvals.push(PermissionReplayApproval {
        request,
        decision: receipt.decision,
    });
    if approvals.len() > 64 {
        return Err(AgentError::Tool(
            "permission replay approval ledger exceeded its safety bound".to_string(),
        ));
    }

    for approval in &approvals {
        validate_replay_approval(session, target.tool_call.id.as_str(), approval)?;
        match approval.decision.decision {
            PermissionDecisionKind::AllowOnce => config
                .grant_once_for_generation(
                    session.id.as_str(),
                    target.tool_call.id.as_str(),
                    replay_generation,
                    approval.request.permission_type,
                    approval.request.resource.clone(),
                )
                .map_err(AgentError::Tool)?,
            PermissionDecisionKind::AllowSession => {
                config
                    .grant_typed_scoped_session_permission(
                        session.id.as_str(),
                        approval.request.permission_type,
                        selected_replay_matcher(approval)?,
                    )
                    .map_err(AgentError::Tool)?;
            }
            PermissionDecisionKind::AllowWorkspace => {
                let authoritative = session
                    .workspace
                    .as_deref()
                    .map(str::trim)
                    .filter(|workspace| !workspace.is_empty());
                if authoritative != approval.request.workspace_path.as_deref() {
                    return Err(AgentError::Tool(
                        "workspace permission replay does not match the durable session workspace"
                            .to_string(),
                    ));
                }
                selected_replay_matcher(approval)?;
            }
            PermissionDecisionKind::AllowGlobal => {
                selected_replay_matcher(approval)?;
            }
            PermissionDecisionKind::DenyOnce | PermissionDecisionKind::DenySession => {
                return Err(AgentError::Tool(
                    "denied permission unexpectedly carried an execution replay marker".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Convert a second typed permission gate encountered during replay into a new
/// durable pending question. The prior approval is appended to a bounded
/// per-invocation ledger; no model successor may start until every context has
/// been separately approved and the tool returns a real result.
pub fn repark_permission_replay(
    session: &mut Session,
    target: &PermissionReplayTarget,
    result: &ToolResult,
) -> Result<Option<ReparkedPermissionApproval>, AgentError> {
    let payload = match serde_json::from_str::<serde_json::Value>(&result.result) {
        Ok(payload)
            if result.display_preference.as_deref() == Some("request_permissions")
                && payload.get("status").and_then(serde_json::Value::as_str)
                    == Some("awaiting_permission_approval") =>
        {
            payload
        }
        _ => return Ok(None),
    };
    let new_request = payload
        .get("permission_request")
        .cloned()
        .and_then(|value| serde_json::from_value::<PermissionRequest>(value).ok())
        .ok_or_else(|| {
            AgentError::Tool("replayed permission gate omitted typed request".to_string())
        })?;
    if new_request.session_id != session.id
        || new_request.request_id != target.tool_call.id
        || new_request.request_generation.trim().is_empty()
        || target.request_generation() == Some(new_request.request_generation.as_str())
    {
        return Err(AgentError::Tool(
            "replayed permission gate returned an invalid next-generation identity".to_string(),
        ));
    }
    let question = payload
        .get("question")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Permission required")
        .to_string();
    let options = payload
        .get("options")
        .and_then(serde_json::Value::as_array)
        .map(|options| {
            options
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let allow_custom = payload
        .get("allow_custom")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let (old_request, old_receipt) = message_permission_contract(session, target)?;
    if old_receipt.session_id != session.id {
        return Err(AgentError::Tool(
            "permission replay receipt session identity changed".to_string(),
        ));
    }
    let old_approval = PermissionReplayApproval {
        request: old_request,
        decision: old_receipt.decision,
    };
    validate_replay_approval(session, target.tool_call.id.as_str(), &old_approval)?;

    let message = session
        .messages
        .get_mut(target.result_message_index)
        .filter(|message| {
            message.id == target.result_message_id
                && message.tool_call_id.as_deref() == Some(target.tool_call.id.as_str())
        })
        .ok_or_else(|| {
            AgentError::Tool("permission replay result occurrence changed".to_string())
        })?;
    let metadata = message
        .metadata
        .get_or_insert_with(|| serde_json::Value::Object(Default::default()));
    let object = metadata.as_object_mut().ok_or_else(|| {
        AgentError::Tool("permission replay result metadata is malformed".to_string())
    })?;
    let mut approvals = object
        .get(PERMISSION_REPLAY_APPROVALS_METADATA_KEY)
        .cloned()
        .map(serde_json::from_value::<Vec<PermissionReplayApproval>>)
        .transpose()
        .map_err(|error| {
            AgentError::Tool(format!(
                "permission replay approval ledger is invalid: {error}"
            ))
        })?
        .unwrap_or_default();
    if !approvals.iter().any(|approval| approval == &old_approval) {
        approvals.push(old_approval);
    }
    if approvals.len() > 64 {
        return Err(AgentError::Tool(
            "permission replay approval ledger exceeded its safety bound".to_string(),
        ));
    }
    object.insert(
        PERMISSION_REPLAY_APPROVALS_METADATA_KEY.to_string(),
        serde_json::to_value(approvals).expect("permission replay approvals serialize"),
    );
    object.insert(
        "permission_request".to_string(),
        serde_json::to_value(&new_request).expect("permission request serializes"),
    );
    object.remove("permission_decision_receipt");
    message.content.clone_from(&result.result);
    message.tool_success = Some(result.success);

    session.set_pending_question_with_source(
        target.tool_call.id.clone(),
        target.tool_call.function.name.clone(),
        question.clone(),
        options.clone(),
        allow_custom,
        PendingQuestionSource::PauseTool,
    );
    session
        .metadata
        .remove(crate::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY);
    session
        .metadata
        .remove(crate::session_app::respond::PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
    for stale_resume_marker in [
        "clarification_resume_pending",
        "conclusion_with_options_resume_pending",
        "execute.startup_handoff_at",
    ] {
        session.metadata.remove(stale_resume_marker);
    }
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );
    Ok(Some(ReparkedPermissionApproval {
        question,
        options,
        allow_custom,
    }))
}

/// Strictly reload and adopt the authoritative permission posture for an
/// approved tool call.
///
/// Storage errors, missing sessions, and malformed typed posture fail closed.
/// This function does not touch the replay marker, so callers can retain it
/// when returning the error. The coherent typed-mode/audit pair and Plan state
/// are committed to `session` only after every fallible validation succeeds.
pub async fn refresh_approval_replay_posture(
    storage: &dyn Storage,
    session: &mut Session,
    configured_mode: PermissionMode,
    tool_name: &str,
) -> Result<ApprovalReplayDecision, AgentError> {
    let latest = storage
        .load_runtime_control_plane(&session.id)
        .await
        .map_err(|error| {
            AgentError::Tool(format!(
                "authoritative approval replay posture refresh failed closed: {error}"
            ))
        })?
        .ok_or_else(|| {
            AgentError::Tool(
                "authoritative approval replay posture refresh failed closed: session missing"
                    .to_string(),
            )
        })?;
    let latest_runtime = latest.agent_runtime_state.as_ref().ok_or_else(|| {
        AgentError::Tool(
            "authoritative approval replay posture refresh failed closed: typed mode missing"
                .to_string(),
        )
    })?;
    let disk_mode = latest_runtime.effective_permission_mode();
    let current_mode = session
        .agent_runtime_state
        .as_ref()
        .map(AgentRuntimeState::effective_permission_mode)
        .unwrap_or_default();

    let should_adopt_audit = bamboo_domain::disk_permission_posture_is_fresher(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    let fresher_audit = bamboo_domain::fresher_disk_permission_audit(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    if should_adopt_audit && fresher_audit.is_none() {
        return Err(AgentError::Tool(
            "authoritative approval replay posture refresh failed closed: complete audit unavailable"
                .to_string(),
        ));
    }

    if let Some(audit) = fresher_audit {
        audit.write_to(&mut session.metadata);
    }
    let runtime = session
        .agent_runtime_state
        .get_or_insert_with(|| AgentRuntimeState::new(latest_runtime.run_id.clone()));
    runtime.set_permission_mode(disk_mode);
    runtime.plan_mode = latest_runtime.plan_mode.clone();

    let flags =
        ToolExecutionSessionFlags::from_session_and_configured_mode(session, configured_mode);
    if flags.plan_read_only && !plan_mode_allows_tool(tool_name) {
        Ok(ApprovalReplayDecision::BlockedByPlan(flags))
    } else {
        Ok(ApprovalReplayDecision::Execute(flags))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};
    use bamboo_agent_core::{Message, Session};
    use bamboo_domain::{
        record_permission_audit, resolve_permission_mode, AgentRuntimeState, PermissionAuditSeed,
        PermissionMode, PlanModeState, PlanModeStatus, SessionPermissionMode,
    };
    use bamboo_tools::permission::{
        PermissionConfig, PermissionDecision, PermissionDecisionKind, PermissionDecisionReceipt,
        PermissionMatcher, PermissionMatcherKind, PermissionReasonCode, PermissionRequest,
        PermissionType, RiskLevel,
    };
    use tokio::sync::RwLock;

    use super::*;

    struct ReplayStorage {
        session: RwLock<Option<Session>>,
        fail_load: bool,
    }

    #[async_trait::async_trait]
    impl Storage for ReplayStorage {
        async fn save_session(&self, _session: &Session) -> std::io::Result<()> {
            Ok(())
        }

        async fn load_session(&self, _session_id: &str) -> std::io::Result<Option<Session>> {
            unreachable!("approval replay must use load_runtime_control_plane")
        }

        async fn load_runtime_control_plane(
            &self,
            _session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            if self.fail_load {
                return Err(std::io::Error::other("injected posture read failure"));
            }
            Ok(self.session.read().await.clone())
        }

        async fn delete_session(&self, _session_id: &str) -> std::io::Result<bool> {
            Ok(false)
        }
    }

    fn session_with_mode(id: &str, mode: SessionPermissionMode) -> Session {
        let mut session = Session::new(id, "model");
        let mut runtime = AgentRuntimeState::new("run");
        runtime.set_permission_mode(mode);
        session.agent_runtime_state = Some(runtime);
        let resolution = resolve_permission_mode(mode, PermissionMode::Default);
        record_permission_audit(
            &mut session.metadata,
            &PermissionAuditSeed::bamboo_runtime(1, resolution),
            Some("2026-07-31T00:00:00Z"),
        )
        .unwrap();
        session
    }

    fn permission_request(
        session_id: &str,
        generation: &str,
        resource: &str,
        decisions: Vec<PermissionDecisionKind>,
    ) -> PermissionRequest {
        PermissionRequest {
            request_id: "call-1".to_string(),
            request_generation: generation.to_string(),
            session_id: session_id.to_string(),
            workspace_path: Some("/workspace/a".to_string()),
            tool_name: "multi_context".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            resource: resource.to_string(),
            operation_summary: format!("execute {resource}"),
            risk_level: RiskLevel::High,
            reason_code: PermissionReasonCode::RiskThreshold,
            effective_mode: PermissionMode::Default,
            bypass_requested: false,
            auto_approve_requested: false,
            policy_revision: 0,
            matched_rule: None,
            allowed_decisions: decisions,
            suggested_matchers: vec![PermissionMatcher {
                id: "exact_resource".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: resource.to_string(),
            }],
        }
    }

    fn decision_receipt(
        session_id: &str,
        generation: &str,
        decision: PermissionDecisionKind,
    ) -> PermissionDecisionReceipt {
        PermissionDecisionReceipt {
            session_id: session_id.to_string(),
            decision: PermissionDecision {
                request_id: "call-1".to_string(),
                request_generation: generation.to_string(),
                decision,
                matcher_id: matches!(
                    decision,
                    PermissionDecisionKind::AllowSession
                        | PermissionDecisionKind::AllowWorkspace
                        | PermissionDecisionKind::AllowGlobal
                        | PermissionDecisionKind::DenySession
                )
                .then(|| "exact_resource".to_string()),
                expected_policy_revision: None,
                confirm_global: decision == PermissionDecisionKind::AllowGlobal,
            },
            decided_at: chrono::Utc::now(),
        }
    }

    fn approved_replay_session(
        generation: &str,
        resource: &str,
        decision: PermissionDecisionKind,
    ) -> Session {
        let mut session = session_with_mode("replay", SessionPermissionMode::Default);
        session.workspace = Some("/workspace/a".to_string());
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "call-1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "multi_context".to_string(),
                    arguments: "{}".to_string(),
                },
            }]),
        ));
        let request = permission_request(
            &session.id,
            generation,
            resource,
            vec![
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::AllowSession,
                PermissionDecisionKind::AllowWorkspace,
                PermissionDecisionKind::AllowGlobal,
                PermissionDecisionKind::DenyOnce,
                PermissionDecisionKind::DenySession,
            ],
        );
        let receipt = decision_receipt(&session.id, generation, decision);
        let mut result = Message::tool_result("call-1", "Selected response: Approve");
        result.id = "result-1".to_string();
        result.metadata = Some(serde_json::json!({
            "permission_request": request,
            "permission_decision_receipt": receipt,
        }));
        session.add_message(result);
        session.metadata.insert(
            crate::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "call-1".to_string(),
        );
        session.metadata.insert(
            crate::session_app::respond::PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.to_string(),
            generation.to_string(),
        );
        session.metadata.insert(
            "clarification_resume_pending".to_string(),
            "true".to_string(),
        );
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "true".to_string(),
        );
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            "2026-08-14T00:00:00Z".to_string(),
        );
        session
    }

    #[tokio::test]
    async fn latest_plan_blocks_mutating_replay_before_executor_entry() {
        let mut latest = session_with_mode("plan", SessionPermissionMode::Auto);
        latest.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "auto".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        });
        let storage: Arc<dyn Storage> = Arc::new(ReplayStorage {
            session: RwLock::new(Some(latest)),
            fail_load: false,
        });
        let mut owned = session_with_mode("plan", SessionPermissionMode::Bypass);

        let decision = refresh_approval_replay_posture(
            storage.as_ref(),
            &mut owned,
            PermissionMode::Default,
            "Write",
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            ApprovalReplayDecision::BlockedByPlan(
                bamboo_agent_core::tools::ToolExecutionSessionFlags {
                    bypass_permissions: false,
                    auto_approve_permissions: true,
                    plan_read_only: true,
                }
            )
        );
    }

    #[tokio::test]
    async fn latest_auto_and_bypass_map_to_distinct_executor_flags() {
        for (mode, expected_bypass, expected_auto) in [
            (SessionPermissionMode::Auto, false, true),
            (SessionPermissionMode::Bypass, true, false),
        ] {
            let latest = session_with_mode("flags", mode);
            let storage: Arc<dyn Storage> = Arc::new(ReplayStorage {
                session: RwLock::new(Some(latest)),
                fail_load: false,
            });
            let mut owned = session_with_mode("flags", SessionPermissionMode::Default);

            let decision = refresh_approval_replay_posture(
                storage.as_ref(),
                &mut owned,
                PermissionMode::Default,
                "Write",
            )
            .await
            .unwrap();
            let ApprovalReplayDecision::Execute(flags) = decision else {
                panic!("non-Plan replay must remain executable");
            };
            assert_eq!(flags.bypass_permissions, expected_bypass);
            assert_eq!(flags.auto_approve_permissions, expected_auto);
            assert!(!flags.plan_read_only);
        }
    }

    #[tokio::test]
    async fn missing_or_failed_authoritative_load_fails_closed_without_mutation() {
        for storage in [
            ReplayStorage {
                session: RwLock::new(None),
                fail_load: false,
            },
            ReplayStorage {
                session: RwLock::new(None),
                fail_load: true,
            },
        ] {
            let mut owned = session_with_mode("missing", SessionPermissionMode::Auto);
            let before = owned.clone();
            let error = refresh_approval_replay_posture(
                &storage,
                &mut owned,
                PermissionMode::Default,
                "Write",
            )
            .await
            .expect_err("unavailable durable posture must fail closed");
            assert!(error.to_string().contains("failed closed"));
            assert_eq!(owned.agent_runtime_state, before.agent_runtime_state);
            assert_eq!(owned.metadata, before.metadata);
        }
    }

    #[test]
    fn restart_restores_exact_allow_once_and_session_authorizations() {
        let allow_once = approved_replay_session(
            "generation-1",
            "context-one",
            PermissionDecisionKind::AllowOnce,
        );
        let target =
            find_permission_replay_target(&allow_once, "call-1", Some("generation-1")).unwrap();
        let restarted = PermissionConfig::new();
        restore_permission_replay_authorization(&restarted, &allow_once, &target).unwrap();
        assert!(restarted.consume_once_for_generation(
            "replay",
            "call-1",
            "generation-1",
            PermissionType::ExecuteCommand,
            "context-one"
        ));
        assert!(!restarted.consume_once_for_generation(
            "replay",
            "call-1",
            "generation-1",
            PermissionType::ExecuteCommand,
            "different-context"
        ));

        let allow_session = approved_replay_session(
            "generation-session",
            "git status",
            PermissionDecisionKind::AllowSession,
        );
        let target =
            find_permission_replay_target(&allow_session, "call-1", Some("generation-session"))
                .unwrap();
        let restarted = PermissionConfig::new();
        restore_permission_replay_authorization(&restarted, &allow_session, &target).unwrap();
        assert!(restarted.is_scoped_session_granted(
            "replay",
            PermissionType::ExecuteCommand,
            "git status"
        ));
    }

    #[test]
    fn workspace_replay_uses_only_durable_session_workspace() {
        let session = approved_replay_session(
            "generation-workspace",
            "cargo test",
            PermissionDecisionKind::AllowWorkspace,
        );
        let target =
            find_permission_replay_target(&session, "call-1", Some("generation-workspace"))
                .unwrap();
        let restarted = PermissionConfig::new();
        restore_permission_replay_authorization(&restarted, &session, &target).unwrap();
        assert_eq!(
            restarted.session_workspace("replay").as_deref(),
            Some("/workspace/a")
        );

        let mut mismatched = session;
        mismatched.workspace = Some("/workspace/b".to_string());
        let target =
            find_permission_replay_target(&mismatched, "call-1", Some("generation-workspace"))
                .unwrap();
        assert!(restore_permission_replay_authorization(
            &PermissionConfig::new(),
            &mismatched,
            &target
        )
        .is_err());
    }

    #[test]
    fn second_permission_context_reparks_and_accumulates_exact_approvals() {
        let mut session = approved_replay_session(
            "generation-1",
            "context-one",
            PermissionDecisionKind::AllowOnce,
        );
        let target =
            find_permission_replay_target(&session, "call-1", Some("generation-1")).unwrap();
        let next_request = permission_request(
            &session.id,
            "generation-2",
            "context-two",
            vec![
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::DenyOnce,
            ],
        );
        let next_result = ToolResult {
            success: true,
            result: serde_json::json!({
                "status": "awaiting_permission_approval",
                "question": "Approve context two?",
                "options": ["Approve", "Deny"],
                "allow_custom": false,
                "permission_request": next_request,
            })
            .to_string(),
            display_preference: Some("request_permissions".to_string()),
            images: Vec::new(),
        };

        let reparked = repark_permission_replay(&mut session, &target, &next_result)
            .unwrap()
            .expect("second context must park");
        assert_eq!(reparked.question, "Approve context two?");
        assert_eq!(
            session
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.as_str()),
            Some("call-1")
        );
        for marker in [
            crate::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY,
            crate::session_app::respond::PERMISSION_REEXECUTE_GENERATION_METADATA_KEY,
            "clarification_resume_pending",
            "conclusion_with_options_resume_pending",
            "execute.startup_handoff_at",
        ] {
            assert!(
                !session.metadata.contains_key(marker),
                "stale marker {marker}"
            );
        }

        let message = session.messages.last_mut().unwrap();
        message.content = "Selected response: Approve".to_string();
        message
            .metadata
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                "permission_decision_receipt".to_string(),
                serde_json::to_value(decision_receipt(
                    "replay",
                    "generation-2",
                    PermissionDecisionKind::AllowOnce,
                ))
                .unwrap(),
            );
        let target =
            find_permission_replay_target(&session, "call-1", Some("generation-2")).unwrap();
        let restarted = PermissionConfig::new();
        restore_permission_replay_authorization(&restarted, &session, &target).unwrap();
        for resource in ["context-one", "context-two"] {
            assert!(restarted.consume_once_for_generation(
                "replay",
                "call-1",
                "generation-2",
                PermissionType::ExecuteCommand,
                resource,
            ));
        }
    }
}
