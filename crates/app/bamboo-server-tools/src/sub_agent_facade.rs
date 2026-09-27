//! Compact Root → direct Child caller; the existing actions remain internal
//! compatibility routes. This module grants no placement or lifecycle authority.

use bamboo_agent_core::tools::{ToolError, ToolOutcome, ToolResult};
use bamboo_engine::session_app::child_session::{self, ChildSessionPort};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{future::Future, pin::Pin};

const MAX_RESULT_BYTES: usize = child_session::MAX_CHILD_RESULT_BYTES;
const MAX_TREE_NODES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Projection {
    Chat,
    Overview,
    Messages,
    Content,
    Error,
    Tree,
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
            "target": {"type":"string", "description":"Logical Child ActorId returned by this tool. Omit for chat to create a durable child, or inspect to read the owned tree."},
            "role": {"type":"string", "description":"Only chat without target: select a named profile. Defaults include explorer (read-only exploration), implementer (bounded implementation), and reviewer (independent read-only review). Project overrides Global, then the builtin default. Omit for worker; no builtin role is implicitly selected. Unknown labels retain legacy behavior; invalid or duplicate known catalog entries fail closed. The selected profile freezes its model, prompt, and read-only/tool posture; the child uses only the tools the runtime exposes to the child. Role cannot change an existing target."},
            "message": {"type":"string", "description":"Chat: complete natural-language task or correction. Inspect: overview, messages, result, error, or a JSON query with view/cursor/message_id for pagination. Control: cancel or retry. No host, worker, model, or mailbox parameters."},
            "reply_to": {"type":"string", "description":"Reserved; ParentRequest resolution is not supported by this caller."}
        }
    })
}

pub(super) fn description() -> &'static str {
    "Delegate to durable child sessions with one logical identity. Use delegation when the user requests parallel work or a separate bounded task would crowd the current context; handle simple tasks directly. A child uses only the tools and permissions exposed to it by the runtime. Send a complete task in message (intent defaults to chat) to create a child; optionally select role=explorer, implementer, reviewer, or another catalog name. Omitted role keeps worker behavior. Include target to correct or continue that same child; role cannot rebind it. Use intent=inspect without target for a bounded owned tree, or with target for overview, messages, result, or error. Paginate with message containing a JSON object with view/cursor/message_id from the prior inspection. Use intent=control, target, and message=cancel or retry to control that same child. Runtime manages activation and waiting. This caller does not resolve ParentRequests or perform remote reassignment; do not pass physical worker or mailbox ids. Legacy action calls remain compatible but are not part of this compact interface."
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
    let parsed: FacadeArgs =
        serde_json::from_value(args).map_err(|_| invalid("Invalid compact SubAgent arguments"))?;
    if parsed.reply_to.is_some() {
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
                if parsed
                    .message
                    .as_deref()
                    .is_some_and(|message| !message.trim().is_empty() && message.trim() != "tree")
                {
                    return Err(invalid("inspect without target accepts only tree"));
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
        Projection::Tree => unreachable!("tree is read and projected separately"),
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
