//! Compact Root → direct Child caller; the existing actions remain internal
//! compatibility routes. This module grants no placement or lifecycle authority.

use bamboo_agent_core::tools::{ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{
    is_matching_session_message, ActorSession, Message, ParentRequest, Session, SessionKind,
    SessionMessageBody, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource,
};
use bamboo_engine::session_app::child_session::{self, ChildSessionPort};
use bamboo_tools::permission::{PermissionReasonCode, PermissionType};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};
use uuid::Uuid;

const MAX_RESULT_BYTES: usize = child_session::MAX_CHILD_RESULT_BYTES;
#[cfg(test)]
const MAX_TREE_NODES: usize = 32;
#[cfg(test)]
const MAX_TREE_CURSOR_BYTES: usize = 128;
#[cfg(test)]
const MAX_TREE_DEPTH: u32 = 4;
const MAX_AUDIT_ROWS: usize = 8;
const AUDIT_SUBSYSTEM: &str = "direct_parent_permission_review";
const AUDIT_REQUEST: &str = "direct_parent_forced_permission_request_v1";
const AUDIT_TERMINAL: &str = "direct_parent_forced_permission_terminal_v1";

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Projection {
    Chat,
    Overview,
    Diagnostics,
    Messages,
    Content,
    Error,
    Tree,
    ForcedPermissionAudit,
    ParentRequestReply,
    ParentQuestionAsk,
    Control,
}

pub(super) struct NormalizedCall {
    pub args: Value,
    pub projection: Option<Projection>,
}

#[derive(Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Intent {
    #[default]
    Chat,
    Inspect,
    Control,
    AskParent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FacadeArgs {
    #[serde(default)]
    intent: Intent,
    target: Option<String>,
    role: Option<String>,
    message: Option<String>,
    reply_to: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct InspectionQuery {
    view: Option<String>,
    cursor: Option<String>,
    message_id: Option<String>,
}

fn invalid(message: &'static str) -> ToolError {
    ToolError::InvalidArguments(message.into())
}

pub(super) fn parameters_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "properties": {
            "intent": {"type":"string", "enum":["chat","inspect","control","ask_parent"], "description":"Defaults to chat. ask_parent is Child-only and uses message as a bounded clarification question; it suspends until the canonical direct parent replies."},
            "target": {"type":"string", "description":"Logical Child ActorId returned by this tool. Omit for chat to create a durable child, or inspect to request a tree scoped to this session. Root sees its full tree; an active Child with a Host canonical tree route sees only its owned subtree. The forced permission audit remains Root-only."},
            "role": {"type":"string", "description":"Only chat without target: select a named profile. Defaults include explorer (read-only exploration), implementer (bounded implementation), and reviewer (independent read-only review). Project overrides Global, then the builtin default. Omit for worker; no builtin role is implicitly selected. Invalid or duplicate known catalog entries fail closed. The selected profile freezes its model, prompt, and read-only/tool posture; the child uses only the tools the runtime exposes to the child. Role cannot change an existing target."},
            "message": {"type":"string", "description":"Chat: complete natural-language task or correction. Inspect without target: tree, a JSON {view:tree,cursor} page request, or forced_permission_audit (Root-only read-only audit). Inspect with target: overview, diagnostics, messages, result, error, or a JSON query with view/cursor/message_id for pagination. Control: cancel or retry. No host, worker, model, or mailbox parameters."},
            "reply_to": {"type":"string", "description":"Exact pending direct-parent ParentRequest id. For forced permission use message exactly approve_once or deny; for a clarification use message as a bounded answer. Omit target and role. The Host checks the canonical request kind and deadline."}
        }
    })
}

pub(super) fn description() -> &'static str {
    "Delegate bounded work to a durable child ActorId. Use for parallel or context-heavy tasks; handle simple tasks directly. A child uses only the tools and permissions exposed to it by the runtime. With default chat intent, message is the full assignment: omit target to create, include target to correct or continue. role=explorer, implementer, reviewer, or a catalog name applies only on creation; omitted role is worker. intent=inspect shows the tree without target (Root full tree; Host-routed Child its owned subtree) or target overview, diagnostics, messages, result, or error. Paginate with a JSON message containing view/cursor/message_id; changed trees reject stale cursors. intent=control uses target and message=cancel or retry. Children use intent=ask_parent with a bounded question in message. Direct parents answer an exact pending ParentRequest using reply_to and message, omitting target and role; forced permission accepts only approve_once or deny. Host verifies kind, direct-parent lineage, Project, and deadline. Root audit: intent=inspect, message=forced_permission_audit. Runtime handles activation and waiting; never pass physical worker or mailbox ids."
}

/// Must run before launch-owner classification. Only legacy calls retain the
/// previous missing-action → create behavior.
pub(super) fn normalize(args: Value) -> Result<NormalizedCall, ToolError> {
    if !args.is_object() {
        return Err(invalid("SubAgent arguments must be an object"));
    }
    let compact_fields = ["intent", "target", "role", "reply_to"]
        .iter()
        .any(|key| args.get(key).is_some());
    if args.get("action").is_some() || (!compact_fields && args.get("prompt").is_some()) {
        if compact_fields {
            return Err(invalid(
                "Do not combine compact and legacy SubAgent arguments",
            ));
        }
        return Ok(NormalizedCall {
            args,
            projection: None,
        });
    }
    let has_role = args.get("role").is_some();
    let has_target = args.get("target").is_some();
    let has_reply_to = args.get("reply_to").is_some();
    let parsed: FacadeArgs =
        serde_json::from_value(args).map_err(|_| invalid("Invalid compact SubAgent arguments"))?;
    if has_reply_to {
        if parsed.intent != Intent::Chat || has_target || has_role {
            return Err(invalid(
                "ParentRequest reply requires chat intent and omits target and role",
            ));
        }
        let request = parsed
            .reply_to
            .as_deref()
            .and_then(|id| SessionMessageId::parse(id).ok())
            .ok_or_else(|| invalid("reply_to must be an exact ParentRequest id"))?;
        let message = parsed
            .message
            .as_deref()
            .ok_or_else(|| invalid("ParentRequest reply requires message"))?;
        crate::parent_request_reply::validate_parent_answer_input(message).map_err(invalid)?;
        return Ok(NormalizedCall {
            args: json!({"reply_to":request.as_str(), "message":message}),
            projection: Some(Projection::ParentRequestReply),
        });
    }
    if has_role {
        if parsed.intent != Intent::Chat || parsed.target.is_some() {
            return Err(invalid(
                "role can only select a new child; it cannot rebind target",
            ));
        }
        let role = parsed
            .role
            .as_deref()
            .ok_or_else(|| invalid("role must be an exact non-empty catalog name"))?;
        if role.trim().is_empty() || role.trim() != role || role.len() > 128 {
            return Err(invalid("role must be an exact non-empty catalog name"));
        }
    }
    if parsed.target.as_ref().is_some_and(|target| {
        target.trim().is_empty() || target.trim() != target || target.len() > 128
    }) {
        return Err(invalid("target must be an exact logical Child ActorId"));
    }
    let (args, projection) = match parsed.intent {
        Intent::AskParent => {
            if has_target || has_role {
                return Err(invalid("ask_parent requires omitted target and role"));
            }
            let question = parsed
                .message
                .as_deref()
                .ok_or_else(|| invalid("ask_parent requires message"))?;
            if question.trim().is_empty()
                || question.len() > bamboo_domain::PARENT_QUESTION_TEXT_MAX_BYTES
                || question.contains('\0')
            {
                return Err(invalid("ask_parent requires a bounded non-empty question"));
            }
            (json!({"question":question}), Projection::ParentQuestionAsk)
        }
        Intent::Chat => {
            let message = parsed
                .message
                .filter(|message| !message.trim().is_empty())
                .ok_or_else(|| invalid("chat requires a non-empty complete message"))?;
            let args = if let Some(target) = parsed.target {
                json!({"action":"send_message", "child_session_id":target, "message":message})
            } else {
                let title: String = message
                    .lines()
                    .find(|line| !line.trim().is_empty())
                    .unwrap_or("Delegated task")
                    .trim()
                    .chars()
                    .take(80)
                    .collect();
                json!({"action":"create", "title":title,
                    "responsibility":"Carry out the delegated task in the complete message.",
                    "prompt":message, "subagent_type":parsed.role.unwrap_or_else(|| "worker".into())})
            };
            (args, Projection::Chat)
        }
        Intent::Inspect => {
            let Some(target) = parsed.target else {
                if parsed.message.as_deref() == Some("forced_permission_audit") {
                    if has_target {
                        return Err(invalid("forced_permission_audit requires omitted target"));
                    }
                    return Ok(NormalizedCall {
                        args: json!({}),
                        projection: Some(Projection::ForcedPermissionAudit),
                    });
                }
                let cursor =
                    match parsed.message.as_deref() {
                        None | Some("tree") => None,
                        Some(message) if message.trim().is_empty() => None,
                        Some(message) if message.starts_with('{') && message.len() <= 4096 => {
                            let query: InspectionQuery = serde_json::from_str(message)
                                .map_err(|_| invalid("Invalid tree inspection query"))?;
                            if query.view.as_deref() != Some("tree") || query.message_id.is_some() {
                                return Err(invalid("Invalid tree inspection query"));
                            }
                            query.cursor
                        }
                        _ => return Err(invalid(
                            "inspect without target accepts only tree or forced_permission_audit",
                        )),
                    };
                if cursor.as_ref().is_some_and(|cursor| cursor.is_empty()) {
                    return Err(invalid("Invalid tree inspection cursor"));
                }
                let mut args = json!({"action":"list"});
                if let Some(cursor) = cursor {
                    args["cursor"] = json!(cursor);
                }
                return Ok(NormalizedCall {
                    args,
                    projection: Some(Projection::Tree),
                });
            };
            let message = parsed.message.as_deref().unwrap_or("overview").trim();
            let query = if message.starts_with('{') {
                if message.len() > 4096 {
                    return Err(invalid("inspection query exceeds its limit"));
                }
                serde_json::from_str::<InspectionQuery>(message)
                    .map_err(|_| invalid("Invalid inspection query"))?
            } else {
                InspectionQuery {
                    view: Some(message.into()),
                    ..Default::default()
                }
            };
            let view = query.view.as_deref().unwrap_or("overview");
            let projection = match view {
                "overview" => Projection::Overview,
                "diagnostics" => Projection::Diagnostics,
                "messages" => Projection::Messages,
                "message" | "result" => Projection::Content,
                "error" => Projection::Error,
                _ => return Err(invalid(
                    "inspect supports overview, diagnostics, messages, message, result, or error",
                )),
            };
            if projection == Projection::Diagnostics
                && (query.cursor.is_some() || query.message_id.is_some())
            {
                return Err(invalid(
                    "diagnostics does not accept a cursor or message_id",
                ));
            }
            let mut args = json!({"action":"get", "child_session_id":target, "view":view});
            if let Some(cursor) = query.cursor {
                args["cursor"] = json!(cursor);
            }
            if let Some(message_id) = query.message_id {
                args["message_id"] = json!(message_id);
            }
            match projection {
                Projection::Messages => args["limit"] = json!(1),
                Projection::Content | Projection::Error => args["max_bytes"] = json!(512),
                _ => {}
            }
            (args, projection)
        }
        Intent::Control => {
            let target = parsed
                .target
                .ok_or_else(|| invalid("control requires target"))?;
            let action = match parsed.message.as_deref().map(str::trim) {
                Some("cancel") => "cancel",
                Some("retry") => "run",
                _ => {
                    return Err(invalid(
                        "control supports cancel or retry; use chat for corrections",
                    ))
                }
            };
            (
                json!({"action":action, "child_session_id":target}),
                Projection::Control,
            )
        }
    };
    Ok(NormalizedCall {
        args,
        projection: Some(projection),
    })
}

fn observed_status(raw: &Value) -> &'static str {
    match raw.as_str() {
        Some("created") => "created",
        Some("pending") => "pending",
        Some("queued") => "queued",
        Some("running") => "running",
        Some("running_in_background") => "running_in_background",
        Some("already_running") => "already_running",
        Some("completed") => "completed",
        Some("error") => "error",
        Some("timeout") => "timeout",
        Some("cancelled") => "cancelled",
        Some("message_delivered_live") => "message_delivered_live",
        Some("message_queued") => "message_queued",
        Some("activation_pending") => "activation_pending",
        Some("activation_retry_required") => "activation_retry_required",
        _ => "unknown",
    }
}

fn copy_fields(output: &mut Value, input: &Value, fields: &[&str]) {
    for field in fields {
        if let Some(value) = input.get(field) {
            output[*field] = value.clone();
        }
    }
}

fn project(projection: Projection, input: &Value) -> Value {
    let mut output = json!({"actor_id":input["child_session_id"]});
    match projection {
        Projection::Chat | Projection::Control => {
            let status = if input["status"] == "already_terminal" {
                &input["last_run_status"]
            } else {
                &input["status"]
            };
            output["observed_status"] = json!(observed_status(status));
            copy_fields(&mut output, input, &["message_count", "messages_removed"]);
            if let Some(id) = input.get("message_id") {
                output["delivery_message_id"] = id.clone();
            }
            let diagnostic = match input["status"].as_str() {
                Some("activation_pending") => Some("Input is durable; activation is pending."),
                Some("activation_retry_required") => Some("Input is durable; activation authorization is unconfirmed. Inspect before sending more work."),
                _ => None,
            };
            if let Some(diagnostic) = diagnostic {
                output["diagnostic"] = json!(diagnostic);
            }
        }
        Projection::Overview => {
            output["observed_status"] = json!(observed_status(&input["last_run_status"]));
            copy_fields(
                &mut output,
                input,
                &[
                    "title",
                    "message_count",
                    "is_running",
                    "has_pending_injected_messages",
                    "runner_started_at",
                    "runner_completed_at",
                    "last_event_at",
                    "round_count",
                    "last_tool_name",
                    "last_tool_phase",
                ],
            );
            output["has_error"] = json!(input["last_run_error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()));
        }
        Projection::Diagnostics => {
            copy_fields(
                &mut output,
                input,
                &[
                    "view",
                    "available",
                    "observed_status",
                    "queue",
                    "leases",
                    "dead_letters",
                    "activation",
                    "wait",
                    "question",
                    "permission",
                    "heartbeat",
                    "error",
                    "reason",
                ],
            );
        }
        Projection::Messages => {
            copy_fields(
                &mut output,
                input,
                &["view", "snapshot_message_count", "next_cursor"],
            );
            output["messages"] = json!(input["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|message| {
                    let mut preview = json!({});
                    copy_fields(
                        &mut preview,
                        message,
                        &[
                            "index",
                            "message_id",
                            "role",
                            "content_preview",
                            "content_utf8_bytes",
                            "content_complete",
                        ],
                    );
                    preview
                })
                .collect::<Vec<_>>());
        }
        Projection::Content => {
            copy_fields(
                &mut output,
                input,
                &[
                    "view",
                    "available",
                    "message_id",
                    "message_index",
                    "role",
                    "current_run_final",
                    "content_utf8_bytes",
                    "byte_start",
                    "byte_end",
                    "text",
                    "next_cursor",
                ],
            );
            if input.get("last_run_status").is_some() {
                output["observed_status"] = json!(observed_status(&input["last_run_status"]));
            }
        }
        Projection::Error => {
            output["has_error"] =
                json!(input["content_utf8_bytes"].as_u64().unwrap_or_default() > 0);
            output["diagnostic"] = json!("Check the child's observed status; detailed execution errors remain available to the authenticated inspector.");
        }
        Projection::Tree
        | Projection::ForcedPermissionAudit
        | Projection::ParentRequestReply
        | Projection::ParentQuestionAsk => {
            unreachable!("direct inspections are read and projected separately")
        }
    }
    // Preserve only the exact existing runtime wait signal. Dropping it would
    // change retry/idle-message coordination even though its display is compact.
    if input["runtime_control"] == "waiting_for_children" {
        output["runtime_control"] = json!("waiting_for_children");
        output["wait_for"] = json!("all");
    }
    output
}

fn bounded_result(mut result: ToolResult, value: Value) -> Result<ToolOutcome, ToolError> {
    result.result = value.to_string();
    result.images.clear();
    if serde_json::to_vec(&result).map_or(true, |bytes| bytes.len() > MAX_RESULT_BYTES) {
        return Err(invalid(
            "SubAgent observation exceeds its limit; request a smaller history slice",
        ));
    }
    Ok(ToolOutcome::Completed(result))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForcedAuditData {
    version: u8,
    child_session_id: String,
    child_created_at: DateTime<Utc>,
    parent_session_id: String,
    parent_created_at: DateTime<Utc>,
    root_session_id: String,
    project_id: Option<String>,
    request_id: String,
    request_generation: String,
    operation_digest: String,
    policy_revision: u64,
    reason: PermissionReasonCode,
    tool: String,
    permission: PermissionType,
    resource: String,
    live: Value,
    lineage: Vec<ActorSession>,
    #[serde(default)]
    parent_request: Option<ParentRequest>,
}

struct ForcedAuditMarker {
    envelope_id: String,
    child: ActorSession,
    display: String,
}

fn invalid_audit() -> ToolError {
    ToolError::Execution("The forced permission audit has an invalid canonical proof".into())
}

fn forced_audit_marker(
    message: &Message,
    root: &Session,
    root_actor: &ActorSession,
) -> Result<Option<ForcedAuditMarker>, ToolError> {
    let Some(marker) = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("session_message"))
    else {
        return Ok(None);
    };
    let subsystem = marker.pointer("/source/subsystem").and_then(Value::as_str);
    let instruction = marker.pointer("/body/instruction").and_then(Value::as_str);
    if subsystem == Some(AUDIT_SUBSYSTEM) && instruction == Some(AUDIT_TERMINAL) {
        return Ok(None);
    }
    if subsystem != Some(AUDIT_SUBSYSTEM) && instruction != Some(AUDIT_REQUEST) {
        return Ok(None);
    }
    if subsystem != Some(AUDIT_SUBSYSTEM) || instruction != Some(AUDIT_REQUEST) {
        return Err(invalid_audit());
    }
    let envelope: SessionMessageEnvelope =
        serde_json::from_value(marker.clone()).map_err(|_| invalid_audit())?;
    if envelope.source
        != (SessionMessageSource::Runtime {
            subsystem: AUDIT_SUBSYSTEM.into(),
        })
        || envelope.kind != SessionMessageKind::RuntimeInstruction
        || envelope.target_session_id != root.id
        || envelope.thread_id.is_some()
        || envelope.in_reply_to.is_some()
        || envelope.attempt.is_some()
        || envelope.correlation_id.is_some()
    {
        return Err(invalid_audit());
    }
    let SessionMessageBody::RuntimeInstruction(instruction) = &envelope.body else {
        return Err(invalid_audit());
    };
    if instruction.instruction != AUDIT_REQUEST {
        return Err(invalid_audit());
    }
    let raw_data = instruction.data.as_ref().ok_or_else(invalid_audit)?;
    if !raw_data
        .as_object()
        .is_some_and(|object| object.contains_key("project_id"))
    {
        return Err(invalid_audit());
    }
    let data: ForcedAuditData =
        serde_json::from_value(raw_data.clone()).map_err(|_| invalid_audit())?;
    // Older durable audits have no typed ParentRequest. New ones must carry a
    // proof that matches the complete canonical envelope, not merely a field
    // accepted by the audit projection's strict decoder.
    if raw_data.get("parent_request").is_some() {
        let typed = data.parent_request.as_ref().ok_or_else(invalid_audit)?;
        if ParentRequest::from_forced_permission_envelope(&envelope).as_ref() != Some(typed) {
            return Err(invalid_audit());
        }
    }
    let generation = Uuid::parse_str(&data.request_generation).map_err(|_| invalid_audit())?;
    let digest = data
        .operation_digest
        .strip_prefix("stable-")
        .ok_or_else(invalid_audit)?;
    if data.version != 1
        || data.parent_session_id != root.id
        || data.parent_created_at != root.created_at
        || data.root_session_id != root.id
        || data.project_id != root_actor.project_id
        || data.child_session_id == root.id
        || data.request_id.trim().is_empty()
        || data.request_id.len() > 256
        || generation.to_string() != data.request_generation
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !matches!(
            data.reason,
            PermissionReasonCode::HardDangerous | PermissionReasonCode::ConfiguredAlwaysAsk
        )
        || data.tool.chars().count() > 128
        || data.resource.chars().count() > 800
        || !data.live.is_object()
        || data.lineage.len() != 2
        || data.lineage[1].project_id != root_actor.project_id
        || !data.lineage[1].matches_session(root)
    {
        return Err(invalid_audit());
    }
    let child = &data.lineage[0];
    if child.actor_id != data.child_session_id
        || child.session_created_at != data.child_created_at
        || child.parent_actor_id.as_deref() != Some(root.id.as_str())
        || child.root_actor_id != root.id
        || child.project_id != root_actor.project_id
        || child.spawn_depth != 1
        || child.session_created_at < root.created_at
    {
        return Err(invalid_audit());
    }
    let expected_id = SessionMessageId::stable(
        "direct-parent-forced-approval-v1",
        &json!({"child":child.actor_id, "birth":child.session_created_at,
            "parent":root.id, "parent_birth":root.created_at,
            "generation":data.request_generation}),
    );
    if envelope.id != expected_id {
        return Err(invalid_audit());
    }
    let redact_resource = data.tool.eq_ignore_ascii_case("Bash")
        || !matches!(
            data.permission,
            PermissionType::WriteFile | PermissionType::DeleteOperation
        )
        || data.resource.contains("://");
    if redact_resource && data.resource != "[redacted]" {
        return Err(invalid_audit());
    }
    let display = format!(
        "Child {} asks to use {} ({}) on {}. Direct parent: reply SubAgent(reply_to=\"{}\", message=\"approve_once\") or message=\"deny\" by deadline. No grant.",
        data.child_session_id, data.tool, data.permission.description(), data.resource, envelope.id
    );
    // Already persisted audit messages use the earlier exact display text.
    // Accept both canonical spellings without trusting arbitrary transcript text.
    let legacy_display = format!(
        "Child {} requests a forced permission decision. Tool: {}; permission: {}; resource: {}. This request is an audit record, not a grant or an instruction to bypass policy.",
        data.child_session_id, data.tool, data.permission.description(), data.resource
    );
    let content = instruction.content.as_ref().ok_or_else(invalid_audit)?;
    let provider = instruction
        .provider_message
        .as_ref()
        .ok_or_else(invalid_audit)?;
    if (content.text != display && content.text != legacy_display)
        || !content.parts.is_empty()
        || provider.content != *content
        || !provider.metadata.is_empty()
        || !provider.never_compress
        || !is_matching_session_message(message, &envelope)
        || serde_json::to_value(message).map_err(|_| invalid_audit())?
            != serde_json::to_value(
                envelope
                    .to_provider_message()
                    .map_err(|_| invalid_audit())?,
            )
            .map_err(|_| invalid_audit())?
    {
        return Err(invalid_audit());
    }
    let _ = data.policy_revision;
    Ok(Some(ForcedAuditMarker {
        envelope_id: envelope.id.to_string(),
        child: child.clone(),
        display: content.text.clone(),
    }))
}

fn unique_audit_marker(
    message: &Message,
    root: &Session,
    root_actor: &ActorSession,
    id_counts: &HashMap<String, usize>,
    generations: &mut HashSet<String>,
) -> Result<Option<ForcedAuditMarker>, ToolError> {
    if let Some(generation) = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.pointer("/session_message/body/data/request_generation"))
        .and_then(Value::as_str)
    {
        if !generations.insert(generation.into()) {
            return Err(invalid_audit());
        }
    }
    let marker = forced_audit_marker(message, root, root_actor)?;
    if marker.is_some() && id_counts.get(&message.id) != Some(&1) {
        return Err(invalid_audit());
    }
    Ok(marker)
}

fn audit_output(records: &[Value], truncated: bool) -> Value {
    json!({"observation":"forced_permission_audit", "records":records,
        "truncated":truncated})
}

fn owned_audit_child(
    root: &Session,
    root_actor: &ActorSession,
    marker: &ForcedAuditMarker,
    child: &Session,
) -> bool {
    let Ok(child_actor) = ActorSession::from_session(child) else {
        return false;
    };
    child.kind == SessionKind::Child
        && child.created_at == marker.child.session_created_at
        && child.parent_session_id.as_deref() == Some(root.id.as_str())
        && child.root_session_id == root.id
        && child.spawn_depth == 1
        && child_actor.project_id == root_actor.project_id
        && marker.child.project_id == child_actor.project_id
        && marker.child.matches_session(child)
}

pub(super) async fn inspect_forced_permission_audit(
    port: &dyn ChildSessionPort,
    caller_id: &str,
) -> Result<ToolOutcome, ToolError> {
    let root = port
        .load_root_session(caller_id)
        .await
        .map_err(|error| match error {
            child_session::ChildSessionError::NotRootSession(_) => {
                invalid("forced_permission_audit is available only to the current Root")
            }
            _ => ToolError::Execution("The current Root session is unavailable".into()),
        })?;
    if root.kind != SessionKind::Root || root.id != caller_id {
        return Err(invalid(
            "forced_permission_audit is available only to the current Root",
        ));
    }
    let root_actor = ActorSession::from_session(&root).map_err(|_| invalid_audit())?;
    let mut id_counts = HashMap::new();
    for message in &root.messages {
        *id_counts.entry(message.id.clone()).or_insert(0) += 1;
    }
    let mut generations = HashSet::new();
    let mut records = Vec::new();
    let mut truncated = false;
    for message in &root.messages {
        let Some(marker) =
            unique_audit_marker(message, &root, &root_actor, &id_counts, &mut generations)?
        else {
            continue;
        };
        let child = port
            .load_child_for_parent(&root.id, &marker.child.actor_id)
            .await
            .map_err(|_| invalid_audit())?;
        if !owned_audit_child(&root, &root_actor, &marker, &child) {
            return Err(invalid_audit());
        }
        if records.len() == MAX_AUDIT_ROWS {
            truncated = true;
            continue;
        }
        records.push(json!({"audit_envelope_id":marker.envelope_id,
            "actor_id":child.id, "observed_status":"audit_recorded",
            "display":marker.display}));
        if serde_json::to_vec(&ToolResult::text(
            true,
            audit_output(&records, true).to_string(),
        ))
        .map_or(true, |bytes| bytes.len() > MAX_RESULT_BYTES)
        {
            records.pop();
            truncated = true;
        }
    }
    let current = port
        .load_root_session(caller_id)
        .await
        .map_err(|_| ToolError::Execution("The current Root session is unavailable".into()))?;
    if current.created_at != root.created_at || current.project_id_meta() != root.project_id_meta()
    {
        return Err(ToolError::Execution(
            "The current Root lifetime or Project changed; start a new inspection".into(),
        ));
    }
    bounded_result(
        ToolResult::text(true, String::new()),
        audit_output(&records, truncated),
    )
}

pub(super) fn finish(
    projection: Option<Projection>,
    result: Result<ToolOutcome, ToolError>,
) -> Result<ToolOutcome, ToolError> {
    let Some(projection) = projection else {
        return result;
    };
    match result {
        Ok(ToolOutcome::Completed(result)) => {
            let value: Value = serde_json::from_str(&result.result)
                .map_err(|_| ToolError::Execution("SubAgent returned an invalid logical observation".into()))?;
            bounded_result(result, project(projection, &value))
        }
        Ok(_) => Err(ToolError::Execution("Unexpected SubAgent operation disposition".into())),
        Err(ToolError::InvalidArguments(_)) => Err(invalid("Invalid SubAgent request or inspection cursor; start a new inspection")),
        Err(_) => Err(ToolError::Execution("SubAgent operation failed. Inspect the current child before retrying; execution details are available to the authenticated inspector.".into())),
    }
}

#[cfg(test)]
fn tree_cursor(offset: usize, digest: &str) -> String {
    format!("tp1:{offset}:{digest}")
}

#[cfg(test)]
fn tree_page(
    caller: &Session,
    tree: &child_session::SessionTreeNode,
    raw_cursor: Option<&str>,
    scope_digest: &str,
    max_depth: u32,
) -> Result<Value, ToolError> {
    // Stable traversal makes a cursor meaningful even when the index returns
    // sibling rows in a different order on the next read.
    let mut pending = vec![(tree, None)];
    let mut nodes = Vec::new();
    let mut depth_limited = false;
    while let Some((node, parent)) = pending.pop() {
        let title: String = node.title.chars().take(80).collect();
        nodes.push(json!({"actor_id":node.session_id, "parent_actor_id":parent,
            "title":title, "depth":node.depth,
            "observed_status":observed_status(&json!(node.last_run_status))}));
        depth_limited |= node.depth >= max_depth;
        let mut children = node.children.iter().collect::<Vec<_>>();
        children.sort_unstable_by(|a, b| a.session_id.cmp(&b.session_id));
        for child in children.into_iter().rev() {
            pending.push((child, Some(node.session_id.as_str())));
        }
    }
    let fingerprint = json!({"caller":caller.id, "birth":caller.created_at,
        "project":caller.project_id_meta(), "metadata_version":caller.metadata_version,
        "canonical_scope":scope_digest, "nodes":nodes});
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&fingerprint)
            .map_err(|_| invalid("Unable to encode the tree inspection"))?,
    ));
    let start = match raw_cursor {
        None => 0,
        Some(raw) => {
            if raw.len() > MAX_TREE_CURSOR_BYTES {
                return Err(invalid("Invalid or stale tree inspection cursor"));
            }
            let mut fields = raw.split(':');
            let (Some("tp1"), Some(offset), Some(proof), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return Err(invalid("Invalid or stale tree inspection cursor"));
            };
            if offset.starts_with('0') || proof != digest {
                return Err(invalid("Invalid or stale tree inspection cursor"));
            }
            let offset = offset
                .parse::<usize>()
                .map_err(|_| invalid("Invalid or stale tree inspection cursor"))?;
            if offset >= nodes.len() {
                return Err(invalid("Invalid or stale tree inspection cursor"));
            }
            offset
        }
    };
    let mut page = Vec::new();
    for node in nodes.iter().skip(start).take(MAX_TREE_NODES) {
        page.push(node.clone());
        let end = start + page.len();
        let next_cursor = (end < nodes.len()).then(|| tree_cursor(end, &digest));
        let candidate = json!({"actor_id":caller.id, "nodes":page,
            "truncated":depth_limited || next_cursor.is_some(), "next_cursor":next_cursor,
            "observation":"Durable index tree; run statuses are observations, not activation leases."});
        if serde_json::to_vec(&ToolResult::text(true, candidate.to_string()))
            .map_or(true, |bytes| bytes.len() > MAX_RESULT_BYTES)
        {
            page.pop();
            if page.is_empty() {
                return Err(invalid("Tree node exceeds the SubAgent observation limit"));
            }
            break;
        }
    }
    let end = start + page.len();
    let next_cursor = (end < nodes.len()).then(|| tree_cursor(end, &digest));
    Ok(json!({"actor_id":caller.id, "nodes":page,
        "truncated":depth_limited || next_cursor.is_some(), "next_cursor":next_cursor,
        "observation":"Durable index tree; run statuses are observations, not activation leases."}))
}

struct FacadeTreePort<'a>(&'a dyn ChildSessionPort);

#[async_trait::async_trait]
impl child_session::owned_tree::OwnedTreePort for FacadeTreePort<'_> {
    async fn load(&self, id: &str) -> Result<Session, child_session::owned_tree::OwnedTreeError> {
        self.0
            .load_tree_caller_session(id)
            .await
            .map_err(|_| child_session::owned_tree::OwnedTreeError::InvalidLineage)
    }

    async fn child_ids(
        &self,
        parent_id: &str,
    ) -> Result<Vec<String>, child_session::owned_tree::OwnedTreeError> {
        Ok(self
            .0
            .list_children(parent_id)
            .await
            .into_iter()
            .map(|entry| entry.child_session_id)
            .collect())
    }

    async fn child_index(
        &self,
    ) -> Result<Option<Vec<(String, String)>>, child_session::owned_tree::OwnedTreeError> {
        self.0
            .tree_index_snapshot()
            .await
            .map_err(|_| child_session::owned_tree::OwnedTreeError::InvalidLineage)
    }
}

pub(super) fn inspect_tree<'a>(
    port: &'a dyn ChildSessionPort,
    caller_id: &'a str,
    raw_cursor: Option<&'a str>,
) -> Pin<Box<dyn Future<Output = Result<ToolOutcome, ToolError>> + Send + 'a>> {
    Box::pin(async move {
        use child_session::owned_tree::{inspect_owned_tree, OwnedTreeError};
        let page = inspect_owned_tree(&FacadeTreePort(port), caller_id, raw_cursor)
            .await
            .map_err(|error| match error {
                OwnedTreeError::InvalidCursor => invalid("Invalid or stale tree inspection cursor"),
                OwnedTreeError::InvalidLineage => ToolError::Execution(
                    "The owned tree has an invalid durable lineage or Project scope".into(),
                ),
                OwnedTreeError::ResultTooLarge => {
                    invalid("Tree node exceeds the SubAgent observation limit")
                }
            })?;
        bounded_result(ToolResult::text(true, String::new()), page)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_message_and_target_normalize_before_owner_classification() {
        let message = "  Keep this full task 🪷\n\nDo not trim the ending.  ";
        let create = normalize(json!({"message":message})).unwrap();
        assert_eq!(create.args["action"], "create");
        assert_eq!(create.args["prompt"], message);
        assert_eq!(create.projection, Some(Projection::Chat));
        assert_eq!(create.args["subagent_type"], "worker");
        for role in ["explorer", "implementer", "reviewer", "unknown-label"] {
            let selected = normalize(json!({"role":role,"message":message})).unwrap();
            assert_eq!(selected.args["subagent_type"], role);
            assert_eq!(selected.args["prompt"], message);
        }
        let chat = normalize(json!({"target":"child", "message":message})).unwrap();
        assert_eq!(chat.args["action"], "send_message");
        assert_eq!(chat.args["message"], message);
        let retry =
            normalize(json!({"intent":"control", "target":"child", "message":"retry"})).unwrap();
        assert_eq!(retry.args["action"], "run");
        let inspect = normalize(json!({"intent":"inspect", "target":"child"})).unwrap();
        assert_eq!(inspect.args["action"], "get");
        assert_eq!(inspect.projection, Some(Projection::Overview));
    }

    #[test]
    fn legacy_calls_are_unchanged_and_compact_runtime_parameters_are_rejected() {
        for args in [
            json!({"action":"list"}),
            json!({"title":"task", "responsibility":"work", "prompt":"body"}),
            json!({"action":"create", "title":"task", "responsibility":"work", "prompt":"body", "subagent_type":"explorer"}),
        ] {
            let normalized = normalize(args.clone()).unwrap();
            assert_eq!(normalized.args, args);
            assert_eq!(normalized.projection, None);
        }
        for args in [
            json!({"message":"task", "model":"physical"}),
            json!({"message":"task", "reply_to":"unsafe/request"}),
            json!({"action":"create", "intent":"chat"}),
            json!({"target":" child ", "message":"task"}),
            json!({"intent":"control", "target":"child", "message":"retire"}),
            json!({"role":" explorer ", "message":"task"}),
            json!({"role":null, "message":"task"}),
            json!({"target":"child", "role":"explorer", "message":"task"}),
            json!({"intent":"inspect", "role":"reviewer"}),
            json!({"intent":"inspect", "role":null}),
            json!({"intent":"control", "role":"implementer", "target":"child", "message":"retry"}),
        ] {
            assert!(normalize(args).is_err());
        }
    }

    #[test]
    fn parent_request_reply_keeps_bounded_message_for_canonical_kind_check() {
        let request = SessionMessageId::new();
        let approve =
            normalize(json!({"reply_to":request.as_str(),"message":"approve_once"})).unwrap();
        assert_eq!(approve.projection, Some(Projection::ParentRequestReply));
        assert_eq!(approve.args["reply_to"], request.as_str());
        assert_eq!(approve.args["message"], "approve_once");
        let deny = normalize(json!({"reply_to":request.as_str(),"message":"deny"})).unwrap();
        assert_eq!(deny.args["message"], "deny");
        let answer =
            normalize(json!({"reply_to":request.as_str(),"message":"Use option A"})).unwrap();
        assert_eq!(answer.args["message"], "Use option A");
        for bad in [
            json!({"reply_to":null,"message":"deny"}),
            json!({"reply_to":request.as_str(),"message":" "}),
            json!({"reply_to":request.as_str(),"message":"x".repeat(bamboo_domain::PARENT_ANSWER_MAX_BYTES + 1)}),
            json!({"reply_to":request.as_str(),"message":"deny ","target":"child"}),
            json!({"reply_to":request.as_str(),"message":"deny","role":"reviewer"}),
            json!({"intent":"inspect","reply_to":request.as_str(),"message":"deny"}),
            json!({"action":"send_message","reply_to":request.as_str(),"message":"deny"}),
        ] {
            assert!(normalize(bad).is_err());
        }
    }

    #[test]
    fn ask_parent_keeps_existing_compact_fields_and_bounded_question() {
        let call = normalize(json!({"intent":"ask_parent","message":"Which option?"})).unwrap();
        assert_eq!(call.projection, Some(Projection::ParentQuestionAsk));
        assert_eq!(call.args, json!({"question":"Which option?"}));
        let maximum = "x".repeat(bamboo_domain::PARENT_QUESTION_TEXT_MAX_BYTES);
        assert_eq!(
            normalize(json!({"intent":"ask_parent","message":maximum.clone()}))
                .unwrap()
                .args["question"]
                .as_str(),
            Some(maximum.as_str())
        );
        for bad in [
            json!({"intent":"ask_parent","message":" "}),
            json!({"intent":"ask_parent","message":"x".repeat(bamboo_domain::PARENT_QUESTION_TEXT_MAX_BYTES + 1)}),
            json!({"intent":"ask_parent","message":"a\0b"}),
            json!({"intent":"ask_parent","message":"Question?","target":"child"}),
            json!({"intent":"ask_parent","message":"Question?","role":"worker"}),
            json!({"intent":"ask_parent","message":"Question?","reply_to":"request"}),
        ] {
            assert!(normalize(bad).is_err());
        }
    }

    #[test]
    fn inspection_queries_keep_existing_cursor_and_message_selectors() {
        let call = normalize(json!({"intent":"inspect", "target":"child",
            "message":r#"{"view":"message","cursor":"cursor","message_id":"message"}"#}))
        .unwrap();
        assert_eq!(call.args["cursor"], "cursor");
        assert_eq!(call.args["message_id"], "message");
        assert_eq!(call.args["max_bytes"], 512);
        assert_eq!(
            normalize(json!({"intent":"inspect"})).unwrap().projection,
            Some(Projection::Tree)
        );
        assert_eq!(
            normalize(json!({"intent":"inspect","message":"forced_permission_audit"}))
                .unwrap()
                .projection,
            Some(Projection::ForcedPermissionAudit)
        );
        for args in [
            json!({"intent":"inspect","target":null,"message":"forced_permission_audit"}),
            json!({"intent":"inspect","target":"other","message":"forced_permission_audit"}),
            json!({"intent":"inspect","message":"forced_permission_audit ","reply_to":null}),
            json!({"intent":"inspect","message":"forced_permission_audit","parent_session_id":"other"}),
        ] {
            assert!(normalize(args).is_err());
        }
    }

    #[test]
    fn root_tree_pages_recover_every_descendant_and_reject_stale_cursors() {
        let root = Session::new("tree-root", "model");
        let mut tree = child_session::SessionTreeNode {
            session_id: root.id.clone(),
            title: "Root".into(),
            last_run_status: None,
            depth: 0,
            children: (0..129)
                .rev()
                .map(|index| child_session::SessionTreeNode {
                    session_id: format!("child-{index:03}"),
                    title: format!("Child {index}"),
                    last_run_status: Some("completed".into()),
                    depth: 1,
                    children: vec![],
                })
                .collect(),
        };
        let first = tree_page(&root, &tree, None, "", MAX_TREE_DEPTH).unwrap();
        assert!(first["next_cursor"].is_string());
        let stale = first["next_cursor"].as_str().unwrap().to_owned();
        let normalized = normalize(json!({"intent":"inspect",
            "message":json!({"view":"tree","cursor":stale}).to_string()}))
        .unwrap();
        assert_eq!(normalized.projection, Some(Projection::Tree));
        assert_eq!(normalized.args["cursor"], stale);

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = tree_page(&root, &tree, cursor.as_deref(), "", MAX_TREE_DEPTH).unwrap();
            let nodes = page["nodes"].as_array().unwrap();
            assert!(!nodes.is_empty() && nodes.len() <= MAX_TREE_NODES);
            assert!(
                serde_json::to_vec(&ToolResult::text(true, page.to_string()))
                    .unwrap()
                    .len()
                    <= MAX_RESULT_BYTES
            );
            seen.extend(
                nodes
                    .iter()
                    .map(|node| node["actor_id"].as_str().unwrap().to_owned()),
            );
            cursor = page["next_cursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                assert_eq!(page["truncated"], false);
                break;
            }
            assert_eq!(page["truncated"], true);
        }
        assert_eq!(seen.len(), 130);
        assert_eq!(seen.first().unwrap(), "tree-root");
        assert_eq!(seen.last().unwrap(), "child-128");
        assert!(tree_page(&root, &tree, Some("tp1:0:bad"), "", MAX_TREE_DEPTH).is_err());

        tree.children.push(child_session::SessionTreeNode {
            session_id: "child-new".into(),
            title: "New child".into(),
            last_run_status: None,
            depth: 1,
            children: vec![],
        });
        assert!(tree_page(&root, &tree, Some(&stale), "", MAX_TREE_DEPTH).is_err());
    }

    fn audit_fixture() -> (Session, Session, Message) {
        use bamboo_domain::{
            SessionMessageContent, SessionProviderMessage, SessionRuntimeInstruction,
        };
        let root = Session::new("audit-root", "model");
        let child = Session::new_child("audit-child", &root.id, "model", "child");
        let generation = Uuid::new_v4().to_string();
        let id = SessionMessageId::stable(
            "direct-parent-forced-approval-v1",
            &json!({"child":child.id,"birth":child.created_at,
                "parent":root.id,"parent_birth":root.created_at,"generation":generation}),
        );
        let display = format!(
            "Child {} requests a forced permission decision. Tool: Write; permission: Write files to disk; resource: file.txt. This request is an audit record, not a grant or an instruction to bypass policy.",
            child.id
        );
        let envelope = SessionMessageEnvelope {
            id,
            source: SessionMessageSource::Runtime {
                subsystem: AUDIT_SUBSYSTEM.into(),
            },
            target_session_id: root.id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: AUDIT_REQUEST.into(),
                content: Some(SessionMessageContent::text(display.clone())),
                data: Some(json!({
                    "version":1,"child_session_id":child.id,"child_created_at":child.created_at,
                    "parent_session_id":root.id,"parent_created_at":root.created_at,
                    "root_session_id":root.id,"project_id":null,"request_id":" request-1 ",
                    "request_generation":generation,
                    "operation_digest":SessionMessageId::stable("permission-operation",&json!({"x":1})).to_string(),
                    "policy_revision":1,"reason":"configured_always_ask","tool":"Write",
                    "permission":"write_file","resource":"file.txt","live":{},
                    "lineage":[ActorSession::from_session(&child).unwrap(),ActorSession::from_session(&root).unwrap()]
                })),
                provider_message: Some(SessionProviderMessage {
                    content: SessionMessageContent::text(display),
                    metadata: Default::default(),
                    never_compress: true,
                }),
            }),
            created_at: Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        (root, child, envelope.to_provider_message().unwrap())
    }

    #[test]
    fn audit_marker_requires_canonical_proof_and_direct_owned_birth() {
        let (root, child, message) = audit_fixture();
        let root_actor = ActorSession::from_session(&root).unwrap();
        let marker = forced_audit_marker(&message, &root, &root_actor)
            .unwrap()
            .unwrap();
        assert!(owned_audit_child(&root, &root_actor, &marker, &child));
        let mut modern: SessionMessageEnvelope =
            serde_json::from_value(message.metadata.as_ref().unwrap()["session_message"].clone())
                .unwrap();
        let modern_display = format!(
            "Child {} asks to use Write (Write files to disk) on file.txt. Direct parent: reply SubAgent(reply_to=\"{}\", message=\"approve_once\") or message=\"deny\" by deadline. No grant.",
            child.id, modern.id
        );
        let SessionMessageBody::RuntimeInstruction(instruction) = &mut modern.body else {
            panic!("audit instruction");
        };
        instruction.content = Some(bamboo_domain::SessionMessageContent::text(
            modern_display.clone(),
        ));
        instruction.provider_message.as_mut().unwrap().content =
            bamboo_domain::SessionMessageContent::text(modern_display.clone());
        let modern_message = modern.to_provider_message().unwrap();
        assert_eq!(
            forced_audit_marker(&modern_message, &root, &root_actor)
                .unwrap()
                .unwrap()
                .display,
            modern_display
        );
        let mut different_birth = child.clone();
        different_birth.created_at += chrono::Duration::seconds(1);
        assert!(!owned_audit_child(
            &root,
            &root_actor,
            &marker,
            &different_birth
        ));
        let mut foreign_child = child.clone();
        foreign_child.parent_session_id = Some("other-root".into());
        assert!(!owned_audit_child(
            &root,
            &root_actor,
            &marker,
            &foreign_child
        ));
        let mut forged = message.clone();
        forged.content.push_str(" raw operation");
        assert!(forced_audit_marker(&forged, &root, &root_actor).is_err());
        let mut wrong_project = message.clone();
        wrong_project.metadata.as_mut().unwrap()["session_message"]["body"]["data"]["project_id"] =
            json!("foreign");
        assert!(forced_audit_marker(&wrong_project, &root, &root_actor).is_err());
        let mut invalid_typed = message;
        invalid_typed.metadata.as_mut().unwrap()["session_message"]["body"]["data"]
            ["parent_request"] = json!(null);
        assert!(forced_audit_marker(&invalid_typed, &root, &root_actor).is_err());
    }

    #[test]
    fn duplicate_or_colliding_audit_transcript_proofs_fail_closed() {
        let (root, _, message) = audit_fixture();
        let root_actor = ActorSession::from_session(&root).unwrap();
        let mut id_counts = HashMap::from([(message.id.clone(), 1)]);
        let mut generations = HashSet::new();
        assert!(
            unique_audit_marker(&message, &root, &root_actor, &id_counts, &mut generations)
                .unwrap()
                .is_some()
        );
        id_counts.insert(message.id.clone(), 2);
        assert!(
            unique_audit_marker(&message, &root, &root_actor, &id_counts, &mut generations)
                .is_err()
        );
        let mut collision = message.clone();
        collision.id = "other-transcript-id".into();
        assert!(
            unique_audit_marker(&collision, &root, &root_actor, &id_counts, &mut generations)
                .is_err()
        );
    }

    #[test]
    fn projection_removes_runtime_identity_and_errors_but_preserves_wait_signal() {
        let input = json!({"child_session_id":"logical", "status":"queued",
            "runtime_control":"waiting_for_children", "external_agent_id":"physical",
            "endpoint":"https://broker.invalid", "activation_error":"secret", "message_id":"delivery"});
        let projected = project(Projection::Chat, &input);
        assert_eq!(projected["actor_id"], "logical");
        assert_eq!(projected["runtime_control"], "waiting_for_children");
        assert_eq!(projected["delivery_message_id"], "delivery");
        assert!(!projected.to_string().contains("physical"));
        assert!(!projected.to_string().contains("broker.invalid"));
        assert!(!projected.to_string().contains("secret"));
        let error = project(
            Projection::Error,
            &json!({"child_session_id":"logical", "content_utf8_bytes":99, "text":"secret"}),
        );
        assert_eq!(error["has_error"], true);
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn overview_exposes_bounded_progress_without_physical_identity() {
        let input = json!({"child_session_id":"logical", "last_run_status":"running",
            "is_running":true, "round_count":7,
            "runner_started_at":"2026-09-29T01:00:00Z",
            "last_event_at":"2026-09-29T01:01:00Z",
            "last_tool_name":"read_file", "last_tool_phase":"end",
            "external_agent_id":"worker-secret", "endpoint":"wss://internal.invalid",
            "last_run_error":"private diagnostic"});
        let projected = project(Projection::Overview, &input);
        assert_eq!(projected["actor_id"], "logical");
        assert_eq!(projected["round_count"], 7);
        assert_eq!(projected["last_tool_name"], "read_file");
        assert_eq!(projected["last_event_at"], "2026-09-29T01:01:00Z");
        assert_eq!(projected["has_error"], true);
        let observation = projected.to_string();
        for private in ["worker-secret", "internal.invalid", "private diagnostic"] {
            assert!(!observation.contains(private));
        }
    }

    #[test]
    fn diagnostics_is_read_only_and_keeps_physical_topology_out_of_model_output() {
        let call = normalize(json!({
            "intent": "inspect",
            "target": "logical-child",
            "message": "diagnostics",
        }))
        .unwrap();
        assert_eq!(call.args["action"], "get");
        assert_eq!(call.args["view"], "diagnostics");
        assert_eq!(call.projection, Some(Projection::Diagnostics));
        let projected = project(
            Projection::Diagnostics,
            &json!({
                "child_session_id": "logical-child",
                "view": "diagnostics",
                "available": true,
                "queue": {"available": true, "pending": 2},
                "activation": {"available": true, "attempt": 3},
                "endpoint": "wss://secret.invalid",
                "worker_id": "private-worker",
            }),
        );
        assert_eq!(projected["queue"]["pending"], 2);
        assert_eq!(projected["activation"]["attempt"], 3);
        assert!(!projected.to_string().contains("secret.invalid"));
        assert!(!projected.to_string().contains("private-worker"));
        assert!(
            normalize(json!({"intent":"inspect","target":"logical-child",
            "message":"{\"view\":\"diagnostics\",\"cursor\":\"older\"}"}))
            .is_err()
        );
    }

    #[test]
    fn escaped_result_capacity_is_measured_on_the_complete_tool_result() {
        let value =
            json!({"child_session_id":"logical", "view":"message", "text":"\0".repeat(512)});
        let result = finish(
            Some(Projection::Content),
            Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                value.to_string(),
            ))),
        )
        .unwrap()
        .into_tool_result();
        assert!(serde_json::to_vec(&result).unwrap().len() <= MAX_RESULT_BYTES);
        let excessive = json!({"child_session_id":"logical", "text":"\0".repeat(8192)});
        assert!(finish(
            Some(Projection::Content),
            Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                excessive.to_string()
            )))
        )
        .is_err());
    }
}
