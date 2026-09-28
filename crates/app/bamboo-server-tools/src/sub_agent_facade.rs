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
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};
use uuid::Uuid;

const MAX_RESULT_BYTES: usize = child_session::MAX_CHILD_RESULT_BYTES;
const MAX_TREE_NODES: usize = 32;
const MAX_AUDIT_ROWS: usize = 8;
const AUDIT_SUBSYSTEM: &str = "direct_parent_permission_review";
const AUDIT_REQUEST: &str = "direct_parent_forced_permission_request_v1";
const AUDIT_TERMINAL: &str = "direct_parent_forced_permission_terminal_v1";

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Projection {
    Chat,
    Overview,
    Messages,
    Content,
    Error,
    Tree,
    ForcedPermissionAudit,
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
            "intent": {"type":"string", "enum":["chat","inspect","control"], "description":"Defaults to chat. Runtime manages activation and waiting."},
            "target": {"type":"string", "description":"Logical Child ActorId returned by this tool. Omit for chat to create a durable child, or inspect to read the owned tree or Root-owned forced permission audit."},
            "role": {"type":"string", "description":"Only chat without target: select a named profile. Defaults include explorer (read-only exploration), implementer (bounded implementation), and reviewer (independent read-only review). Project overrides Global, then the builtin default. Omit for worker; no builtin role is implicitly selected. Unknown labels retain legacy behavior; invalid or duplicate known catalog entries fail closed. The selected profile freezes its model, prompt, and read-only/tool posture; the child uses only the tools the runtime exposes to the child. Role cannot change an existing target."},
            "message": {"type":"string", "description":"Chat: complete natural-language task or correction. Inspect without target: tree or forced_permission_audit (Root-only read-only audit). Inspect with target: overview, messages, result, error, or a JSON query with view/cursor/message_id for pagination. Control: cancel or retry. No host, worker, model, or mailbox parameters."},
            "reply_to": {"type":"string", "description":"Reserved; ParentRequest resolution is not supported by this caller."}
        }
    })
}

pub(super) fn description() -> &'static str {
    "Delegate to durable child sessions with one logical identity. Use delegation when the user requests parallel work or a separate bounded task would crowd the current context; handle simple tasks directly. A child uses only the tools and permissions exposed to it by the runtime. Send a complete task in message (intent defaults to chat) to create a child; optionally select role=explorer, implementer, reviewer, or another catalog name. Omitted role keeps worker behavior. Include target to correct or continue that same child; role cannot rebind it. Use intent=inspect without target for a bounded owned tree, or message=forced_permission_audit for the current Root's read-only audit records. This audit is not approval or a ParentRequest. Use target for child overview, messages, result, or error. Paginate child inspection with a JSON message containing view/cursor/message_id. Use intent=control, target, and message=cancel or retry to control that same child. Runtime manages activation and waiting. This caller does not resolve ParentRequests or perform remote reassignment; do not pass physical worker or mailbox ids. Legacy action calls remain compatible but are not part of this compact interface."
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
    if has_reply_to || parsed.reply_to.is_some() {
        return Err(invalid(
            "ParentRequest replies are not supported by this caller",
        ));
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
                if parsed
                    .message
                    .as_deref()
                    .is_some_and(|message| !message.trim().is_empty() && message.trim() != "tree")
                {
                    return Err(invalid(
                        "inspect without target accepts only tree or forced_permission_audit",
                    ));
                }
                return Ok(NormalizedCall {
                    args: json!({"action":"list"}),
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
                "messages" => Projection::Messages,
                "message" | "result" => Projection::Content,
                "error" => Projection::Error,
                _ => {
                    return Err(invalid(
                        "inspect supports overview, messages, message, result, or error",
                    ))
                }
            };
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
                ],
            );
            output["has_error"] = json!(input["last_run_error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()));
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
        Projection::Tree | Projection::ForcedPermissionAudit => {
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
        "Child {} requests a forced permission decision. Tool: {}; permission: {}; resource: {}. This request is an audit record, not a grant or an instruction to bypass policy.",
        data.child_session_id, data.tool, data.permission.description(), data.resource
    );
    let content = instruction.content.as_ref().ok_or_else(invalid_audit)?;
    let provider = instruction
        .provider_message
        .as_ref()
        .ok_or_else(invalid_audit)?;
    if content.text != display
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
        display,
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
        Err(_) => Err(ToolError::Execution("SubAgent operation failed. Inspect the owned tree and current child before retrying; execution details are available to the authenticated inspector.".into())),
    }
}

pub(super) fn inspect_tree<'a>(
    port: &'a dyn ChildSessionPort,
    parent_id: &'a str,
) -> Pin<Box<dyn Future<Output = Result<ToolOutcome, ToolError>> + Send + 'a>> {
    Box::pin(async move {
        let root = port
            .load_root_session(parent_id)
            .await
            .map_err(|_| ToolError::Execution("The current Root session is unavailable".into()))?;
        let tree = child_session::build_session_tree_action(port, parent_id, 4).await;
        let mut pending = vec![(&tree, None)];
        let mut nodes = Vec::new();
        let mut truncated = false;
        while let Some((node, parent)) = pending.pop() {
            if nodes.len() >= MAX_TREE_NODES {
                truncated = true;
                break;
            }
            let title: String = node.title.chars().take(80).collect();
            let candidate = json!({"actor_id":node.session_id, "parent_actor_id":parent,
            "title":title, "depth":node.depth,
            "observed_status":observed_status(&json!(node.last_run_status))});
            nodes.push(candidate);
            truncated |= node.depth >= 4;
            let value = json!({"actor_id":parent_id, "nodes":nodes, "truncated":true,
            "observation":"Durable index tree; run statuses are observations, not activation leases."});
            if serde_json::to_vec(&ToolResult::text(true, value.to_string()))
                .map_or(true, |bytes| bytes.len() > MAX_RESULT_BYTES)
            {
                nodes.pop();
                truncated = true;
                break;
            }
            for child in node.children.iter().rev() {
                pending.push((child, Some(node.session_id.as_str())));
            }
        }
        let current = port
            .load_root_session(parent_id)
            .await
            .map_err(|_| ToolError::Execution("The current Root session is unavailable".into()))?;
        if current.created_at != root.created_at {
            return Err(ToolError::Execution(
                "The current Root lifetime changed; start a new inspection".into(),
            ));
        }
        bounded_result(
            ToolResult::text(true, String::new()),
            json!({
                "actor_id":parent_id, "nodes":nodes, "truncated":truncated,
                "observation":"Durable index tree; run statuses are observations, not activation leases."
            }),
        )
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
            json!({"message":"task", "reply_to":"request"}),
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
