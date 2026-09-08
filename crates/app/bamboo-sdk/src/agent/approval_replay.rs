//! SDK entry-point adapter for durable, occurrence-bound permission replay.

use super::{Agent, AgentError, AgentEvent, Message, Session};
use bamboo_agent_core::tools::{ToolExecutor, ToolOutcome};
use bamboo_agent_core::{PendingQuestion, PendingQuestionSource};
use bamboo_domain::resolve_tool_reference_name;
use bamboo_engine::session_app::approval_replay::{
    apply_permission_replay_result, find_permission_replay_target, refresh_approval_replay_posture,
    repark_permission_replay, restore_permission_replay_authorization, ApprovalReplayDecision,
    PermissionReplayTarget,
};
use bamboo_engine::session_app::respond::{
    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY, PERMISSION_REEXECUTE_METADATA_KEY,
};
use bamboo_tools::permission::{with_permission_replay_generation, PermissionRequest};
use tokio::sync::mpsc;

#[derive(Debug)]
pub(super) enum ReplayDisposition {
    Continue,
    AwaitingApproval(PendingQuestion),
}

fn invalid(message: &str) -> AgentError {
    AgentError::Tool(format!("permission replay {message}"))
}

fn latest_result<'a>(session: &'a Session, id: &str) -> Option<&'a Message> {
    session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(id))
}

/// Field presence decides whether a result is typed. A malformed/blank request
/// must never fall through the legacy path because generation extraction failed.
fn typed_request(message: &Message) -> Result<Option<PermissionRequest>, AgentError> {
    let payload = serde_json::from_str::<serde_json::Value>(&message.content).ok();
    let metadata_request = message
        .metadata
        .as_ref()
        .and_then(|value| value.get("permission_request"));
    let payload_request = payload
        .as_ref()
        .and_then(|value| value.get("permission_request"));
    let decode = |value: &serde_json::Value| {
        serde_json::from_value::<PermissionRequest>(value.clone())
            .map_err(|_| invalid("request is malformed"))
    };
    let durable = metadata_request.map(decode).transpose()?;
    let waiting = payload_request.map(decode).transpose()?;
    if matches!((&durable, &waiting), (Some(a), Some(b)) if a != b) {
        return Err(invalid("request payload and metadata disagree"));
    }
    let request = durable.or(waiting);
    if request.is_some() && !matches!(message.role, super::Role::Tool) {
        return Err(invalid("typed result does not have the Tool role"));
    }
    if request.is_none()
        && (message
            .metadata
            .as_ref()
            .is_some_and(|value| value.get("permission_decision_receipt").is_some())
            || payload
                .as_ref()
                .is_some_and(|value| value.get("permission_decision_receipt").is_some()))
    {
        return Err(invalid("receipt is missing its typed request"));
    }
    Ok(request)
}

fn validate_request(
    session: &Session,
    target: &PermissionReplayTarget,
    request: &PermissionRequest,
    executor: &dyn ToolExecutor,
) -> Result<String, AgentError> {
    // Requests name the selected execution identity, while history and pending
    // questions retain the caller's spelling. Exact custom registrations must
    // win before builtin aliases, just as they do in the live executor.
    let execution_name = resolve_tool_reference_name(&target.tool_call().function.name, |name| {
        executor.owns_exact_tool(name)
    })
    .ok_or_else(|| invalid("current tool is unavailable"))?;
    if request.session_id != session.id
        || request.request_id != target.tool_call().id
        || request.tool_name != execution_name
        || request.request_generation.trim().is_empty()
        || target.request_generation() != Some(request.request_generation.as_str())
    {
        return Err(invalid("request does not match the current operation"));
    }
    Ok(execution_name)
}

fn clear_markers(session: &mut Session) {
    session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
    session
        .metadata
        .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
}

impl Agent {
    /// Resolve only the newest same-ID occurrence. An unanswered typed request
    /// remains waiting across new SDK calls/restarts; plain User text is not an
    /// authorization response. Successful terminal events follow durable save.
    pub(super) async fn reexecute_approved_tool_if_pending(
        &self,
        session: &mut Session,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) -> Result<ReplayDisposition, AgentError> {
        let call_id = session
            .metadata
            .get(PERMISSION_REEXECUTE_METADATA_KEY)
            .cloned();
        let generation = session
            .metadata
            .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
            .cloned();
        if call_id.is_none() && generation.is_some() {
            return Err(invalid("generation marker has no tool-call marker"));
        }
        let executor = self.inner.default_tools();

        if let Some(pending) = session.pending_question.as_ref() {
            if let Some(message) = latest_result(session, &pending.tool_call_id) {
                if let Some(request) = typed_request(message)? {
                    let target =
                        find_permission_replay_target(session, &pending.tool_call_id, None)
                            .ok_or_else(|| invalid("pending operation is missing its tool call"))?;
                    validate_request(session, &target, &request, executor.as_ref())?;
                    let payload = serde_json::from_str::<serde_json::Value>(&message.content).ok();
                    if call_id.is_some()
                        || generation.is_some()
                        || pending.source != PendingQuestionSource::PauseTool
                        || pending.tool_name != target.tool_call().function.name
                        || message.metadata.as_ref().is_some_and(|metadata| {
                            metadata.get("permission_decision_receipt").is_some()
                        })
                        || payload
                            .as_ref()
                            .is_some_and(|value| value.get("permission_decision_receipt").is_some())
                        || payload
                            .as_ref()
                            .and_then(|value| value.get("status"))
                            .and_then(serde_json::Value::as_str)
                            != Some("awaiting_permission_approval")
                        || session
                            .metadata
                            .get("runtime.suspend_reason")
                            .map(String::as_str)
                            != Some("awaiting_clarification")
                    {
                        return Err(invalid("pending approval state is contradictory"));
                    }
                    return Ok(ReplayDisposition::AwaitingApproval(pending.clone()));
                }
            }
        }

        let Some(call_id) = call_id else {
            return Ok(ReplayDisposition::Continue);
        };
        if call_id.trim().is_empty()
            || generation
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(invalid("marker is empty"));
        }
        let request = latest_result(session, &call_id)
            .map(typed_request)
            .transpose()?
            .flatten();
        let Some(target) = find_permission_replay_target(session, &call_id, None) else {
            if generation.is_some() || request.is_some() {
                return Err(invalid("typed target is missing"));
            }
            // Preserve the old SDK's call-ID-only ghost-marker compatibility.
            clear_markers(session);
            return Ok(ReplayDisposition::Continue);
        };
        if generation.as_deref() != target.request_generation() {
            return Err(invalid("generation does not match the newest operation"));
        }
        let execution_name = if let Some(request) = request.as_ref() {
            let name = validate_request(session, &target, request, executor.as_ref())?;
            if generation.as_deref() != Some(request.request_generation.as_str())
                || session.pending_question.is_some()
            {
                return Err(invalid(
                    "typed operation has not been answered for this generation",
                ));
            }
            Some(name)
        } else {
            if generation.is_some() || target.request_generation().is_some() {
                return Err(invalid("generation is missing its typed request"));
            }
            None
        };

        let tool_call = target.tool_call();
        let tool_name = &tool_call.function.name;
        let decision = refresh_approval_replay_posture(
            self.storage().as_ref(),
            session,
            self.permission_mode,
            execution_name.as_deref().unwrap_or(tool_name),
        )
        .await?;
        if request.is_some() {
            let config = self
                .permission_checker
                .as_ref()
                .and_then(|checker| checker.permission_config())
                .ok_or_else(|| invalid("typed authorization requires a PermissionConfig"))?;
            restore_permission_replay_authorization(&config, session, &target)?;
        }
        let flags = match decision {
            ApprovalReplayDecision::Execute(flags) => flags,
            ApprovalReplayDecision::BlockedByPlan(_) => {
                if !apply_permission_replay_result(session, &target, format!(
                    "Plan mode blocked approved mutating tool '{tool_name}'; the stale approval was not executed"), false) {
                    return Err(invalid("result occurrence changed"));
                }
                clear_markers(session);
                self.save_permission_replay(session).await?;
                return Ok(ReplayDisposition::Continue);
            }
        };

        let is_mutating = bamboo_tools::orchestrator::classify_tool(tool_name)
            == bamboo_tools::orchestrator::ToolMutability::Mutating;
        let mut emitter = bamboo_tools::ToolEmitter::new(&call_id, tool_name, is_mutating);
        emitter.set_auto_approved(true);
        let _ = event_tx
            .send(emitter.begin().clone().into_agent_event())
            .await;
        let completed = async {
            let ctx = bamboo_agent_core::tools::ToolExecutionContext {
                executing_supervisor: None,
                session_id: Some(session.id.as_str()),
                root_session_id: Some(if session.root_session_id.trim().is_empty() {
                    session.id.as_str()
                } else {
                    session.root_session_id.as_str()
                }),
                tool_call_id: &call_id,
                event_tx: Some(event_tx),
                available_tool_schemas: None,
                bypass_permissions: flags.bypass_permissions,
                auto_approve_permissions: flags.auto_approve_permissions,
                plan_read_only: flags.plan_read_only,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            };
            let dispatch = async {
                match execution_name.as_deref() {
                    Some(name) => executor
                        .execute_exact_with_context_outcome(tool_call, name, ctx)
                        .await
                        .map(ToolOutcome::into_tool_result),
                    None => executor.execute_with_context(tool_call, ctx).await,
                }
            };
            let (result, execution_error) = match with_permission_replay_generation(
                &session.id,
                &call_id,
                generation.as_deref(),
                dispatch,
            )
            .await
            {
                Ok(result) => (result, None),
                Err(error) => {
                    // A real tool failure after valid admission remains input
                    // for normal model recovery, once that failed result saves.
                    let message = format!("Tool re-execution after approval failed: {error}");
                    (
                        bamboo_agent_core::tools::ToolResult::text(false, message.clone()),
                        Some(message),
                    )
                }
            };
            let reparked = repark_permission_replay(session, &target, &result)?;
            if reparked.is_none()
                && !apply_permission_replay_result(
                    session,
                    &target,
                    result.result.clone(),
                    result.success,
                )
            {
                return Err(invalid("result occurrence changed"));
            }
            clear_markers(session);
            self.save_permission_replay(session).await?;
            Ok((result, reparked, execution_error))
        }
        .await;
        let (result, reparked, execution_error) = match completed {
            Ok(completed) => completed,
            Err(error) => {
                let _ = event_tx
                    .send(emitter.error(error.to_string()).clone().into_agent_event())
                    .await;
                return Err(error);
            }
        };
        if let Some(message) = execution_error {
            let _ = event_tx
                .send(emitter.error(message).clone().into_agent_event())
                .await;
            return Ok(ReplayDisposition::Continue);
        }
        let _ = event_tx
            .send(
                emitter
                    .finish(Some(
                        if reparked.is_some() {
                            "Awaiting additional permission approval"
                        } else {
                            "Re-executed after approval"
                        }
                        .into(),
                    ))
                    .clone()
                    .into_agent_event(),
            )
            .await;
        let _ = event_tx
            .send(AgentEvent::ToolComplete {
                tool_call_id: call_id.clone(),
                result,
            })
            .await;
        if let Some(reparked) = reparked {
            return Ok(ReplayDisposition::AwaitingApproval(PendingQuestion {
                question: reparked.question,
                options: reparked.options,
                tool_call_id: call_id,
                tool_name: tool_name.clone(),
                allow_custom: reparked.allow_custom,
                source: PendingQuestionSource::PauseTool,
            }));
        }
        Ok(ReplayDisposition::Continue)
    }

    async fn save_permission_replay(&self, session: &mut Session) -> Result<(), AgentError> {
        self.persistence()
            .save_runtime_session(session)
            .await
            .map_err(|error| invalid(&format!("persistence failed: {error}")))
    }
}
