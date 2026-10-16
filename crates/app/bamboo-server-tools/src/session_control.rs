//! Narrow model control over already attached independent Roots.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{LoopControlRequest, SessionActivationDisposition, SupervisorReference};
use bamboo_engine::{SessionMessenger, SessionMessengerError};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SessionControlArgs {
    Followup {
        target_session_id: String,
        operation_id: String,
        message: String,
    },
    Loop {
        target_session_id: String,
        #[serde(default)]
        expected_link_revision: Option<u64>,
        command: LoopControlRequest,
        #[serde(default)]
        receipt_operation_id: Option<String>,
    },
}

pub struct SessionControlTool {
    messenger: Arc<SessionMessenger>,
}

impl SessionControlTool {
    pub fn new(messenger: Arc<SessionMessenger>) -> Self {
        Self { messenger }
    }
}

#[async_trait]
impl Tool for SessionControlTool {
    fn name(&self) -> &str {
        "session_control"
    }

    fn description(&self) -> &str {
        "Supervisor-only control of an already attached independent Root, preserving its original context and authority. followup: use a stable operation_id per target and exactly the same message on retry; returns durable admission/activation status, not task completion. loop: requires a separate explicit host grant for the target's current active execution and its existing finite loop opt-in. command is status/start/update/stop; only status may omit expected_link_revision or query receipt_operation_id. Mutations require the exact nonzero link revision and command generation/control CAS. A target-owner receipt distinguishes queued delivery, committed control and any objective still pending its next iteration. No new session, attachment, successor, cadence, approval answer or permission is created. Plan and approval replay cannot issue loop commands. Pending human questions remain unanswered; use session_history.read_messages for bounded history."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "action": {"type": "string", "enum": ["followup", "loop"],
                    "description": "followup requires target_session_id, operation_id and message. loop requires target_session_id and command; mutations also require expected_link_revision. Only loop status may query receipt_operation_id or omit the link revision."},
                "target_session_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "operation_id": {"type": "string", "minLength": 1, "maxLength": 256,
                    "description": "followup only: stable operation id for this target; no whitespace padding, slashes or '..'. Reuse it only with the same message."},
                "message": {"type": "string", "minLength": 1, "maxLength": 16384,
                    "description": "followup only: one instruction, at most 16 KiB in UTF-8."},
                "expected_link_revision": {"type": ["integer", "null"], "minimum": 1,
                    "description": "loop only: exact current nonzero Supervisor link revision; required for start/update/stop. Status alone may omit it to bootstrap after full canonical authority and host-grant checks."},
                "command": scoped_loop_command_schema(),
                "receipt_operation_id": {"type": ["string", "null"], "format": "uuid",
                    "description": "loop status only: exact operation UUID for bounded target-owner receipt lookup; never a grant or a new command."}
            },
            "required": ["action", "target_session_id"],
            "oneOf": [
                {
                    "properties": {
                        "action": {"const": "followup"},
                        "command": false,
                        "expected_link_revision": false,
                        "receipt_operation_id": false
                    },
                    "required": ["operation_id", "message"]
                },
                {
                    "properties": {
                        "action": {"const": "loop"},
                        "operation_id": false,
                        "message": false
                    },
                    "required": ["command"],
                    "oneOf": [
                        {
                            "properties": {
                                "command": {"properties": {"action": {"const": "status"}}}
                            }
                        },
                        {
                            "properties": {
                                "command": {"properties": {"action": {"enum": ["start", "update", "stop"]}}},
                                "expected_link_revision": {"type": "integer"},
                                "receipt_operation_id": {"type": "null"}
                            },
                            "required": ["expected_link_revision"]
                        }
                    ]
                }
            ]
        })
    }

    fn classify(&self, _args: &Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if ctx.plan_read_only {
            return Err(ToolError::Execution(
                "session_control is unavailable in Plan mode".into(),
            ));
        }
        let caller = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("session_control requires a session_id in tool context".into())
        })?;
        let supervisor = ctx
            .executing_supervisor_for(caller)
            .ok_or_else(|| {
                ToolError::Execution(
                    "session_control requires the original executing Supervisor identity".into(),
                )
            })?
            .supervisor_reference();
        let (target_session_id, operation_id, message) = match serde_json::from_value(args)
            .map_err(|error| {
                ToolError::InvalidArguments(format!("invalid session_control arguments: {error}"))
            })? {
            SessionControlArgs::Followup {
                target_session_id,
                operation_id,
                message,
            } => (target_session_id, operation_id, message),
            SessionControlArgs::Loop {
                target_session_id,
                expected_link_revision,
                command,
                receipt_operation_id,
            } => {
                validate_loop_arguments(
                    &supervisor,
                    &target_session_id,
                    expected_link_revision,
                    &command,
                    receipt_operation_id.as_deref(),
                )?;
                if bamboo_tools::permission::permission_replay_active(caller, &ctx.tool_call_id) {
                    return Err(ToolError::Execution(
                        "session_control loop requires a fresh command, not approval replay".into(),
                    ));
                }
                let receipt = self
                    .messenger
                    .supervisor_loop_control(
                        &supervisor,
                        &target_session_id,
                        expected_link_revision,
                        &command,
                        receipt_operation_id.as_deref(),
                    )
                    .await
                    .map_err(|error| ToolError::Execution(error.to_string()))?;
                // Preserve the host/target's actual queued or committed
                // result. Delivery alone never manufactures applied=true.
                return Ok(ToolOutcome::Completed(ToolResult::text(
                    true,
                    receipt.to_string(),
                )));
            }
        };
        let result = self
            .messenger
            .supervisor_followup(&supervisor, &target_session_id, &operation_id, &message)
            .await;
        let (delivery, activation, detail) = match result {
            Ok(receipt) => {
                let activation = match receipt.activation {
                    SessionActivationDisposition::ActiveNotified => "active_notified",
                    SessionActivationDisposition::ActivationReserved => "activation_reserved",
                    SessionActivationDisposition::ActivationCoalesced => "activation_coalesced",
                };
                (receipt.delivery, activation, None)
            }
            Err(SessionMessengerError::Activation {
                receipt, source, ..
            }) => (receipt, "activation_pending", Some(source.to_string())),
            Err(SessionMessengerError::ActivationEligibility {
                receipt, source, ..
            }) => (
                receipt,
                "activation_retry_required",
                Some(source.to_string()),
            ),
            Err(error) => return Err(ToolError::Execution(error.to_string())),
        };
        let activation_retry_required = activation == "activation_retry_required";
        Ok(ToolOutcome::Completed(ToolResult::text(!activation_retry_required, json!({
            "action": "followup", "target_session_id": target_session_id,
            "operation_id": operation_id, "admitted": true,
            "receipt_id": delivery.id, "generation": delivery.generation,
            "activation": activation, "activation_error": detail,
            "note": "Admission is durable; this is not completion. A waiting target remains waiting. Retry with the same operation_id and message if activation needs recovery; read results with session_history.read_messages."
        }).to_string())))
    }
}

/// Project the shared bounded fields into the remote adapter's strict actions.
/// The standalone self-tool schema and its public API remain unchanged.
fn scoped_loop_command_schema() -> Value {
    let mut schema = bamboo_tools::tools::loop_control::LoopControlTool::new().parameters_schema();
    // Match Rust str::trim/char::is_whitespace exactly. ECMAScript \s
    // includes FEFF but excludes NEL, so its shorthand is not equivalent.
    // This local wrapper leaves the shared self-tool schema and DTO unchanged.
    schema["properties"]["objective"]["pattern"] = json!(
        r"[^\u0009-\u000D\u0020\u0085\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]"
    );
    schema["properties"]["objective"]["maxLength"] =
        json!(bamboo_domain::MAX_SCOPED_LOOP_OBJECTIVE_CHARS);
    let patch_fields = [
        ("objective", "string"),
        ("max_iterations", "integer"),
        ("max_total_rounds", "integer"),
    ];
    // Serde Option patches accept null as omission. Start requires actual values
    // below; Update must still carry at least one non-null patch.
    for (field, kind) in patch_fields {
        schema["properties"][field]["type"] = json!([kind, "null"]);
    }
    schema["properties"]["objective"]["description"] = json!(
        "Required for start; optional update patch. Non-whitespace text, at most 4096 Unicode scalar values, \
         counting the original text including surrounding whitespace, and at most 16384 UTF-8 bytes. \
         This remote command contract is narrower than standalone self loop_control; no trimming or normalization occurs. \
         The same limit applies to start and objective updates. \
         An edit applies no earlier than the next iteration admission; at most one objective edit may be pending."
    );
    let variant = |action: &str, required: &[&str], optional: &[&str]| {
        let mut properties = serde_json::Map::new();
        properties.insert("action".into(), json!({"const": action}));
        for field in schema["properties"].as_object().unwrap().keys() {
            if field != "action"
                && !required.contains(&field.as_str())
                && !optional.contains(&field.as_str())
            {
                properties.insert(field.clone(), Value::Bool(false));
            }
        }
        json!({"properties": properties, "required": required})
    };
    let status = variant("status", &["action"], &[]);
    let mut start = variant(
        "start",
        &[
            "action",
            "operation_id",
            "expected_revision",
            "objective",
            "max_iterations",
            "max_total_rounds",
        ],
        &["expected_generation"],
    );
    let mut update = variant(
        "update",
        &["action", "operation_id", "generation", "expected_revision"],
        &["objective", "max_iterations", "max_total_rounds"],
    );
    let stop = variant(
        "stop",
        &["action", "operation_id", "generation", "expected_revision"],
        &[],
    );
    for (field, kind) in patch_fields {
        start["properties"][field] = json!({"type": kind});
    }
    update["anyOf"] = json!([
        {"required": ["objective"], "properties": {"objective": {"type": "string"}}},
        {"required": ["max_iterations"], "properties": {"max_iterations": {"type": "integer"}}},
        {"required": ["max_total_rounds"], "properties": {"max_total_rounds": {"type": "integer"}}}
    ]);
    schema["oneOf"] = json!([status, start, update, stop]);
    schema
}

fn validate_loop_arguments(
    supervisor: &SupervisorReference,
    target_session_id: &str,
    expected_link_revision: Option<u64>,
    command: &LoopControlRequest,
    receipt_operation_id: Option<&str>,
) -> Result<(), ToolError> {
    bamboo_domain::validate_supervisor_target_id(target_session_id)
        .map_err(|error| ToolError::InvalidArguments(error.into()))?;
    if target_session_id == supervisor.session_id {
        return Err(ToolError::InvalidArguments(
            "Supervisor loop control cannot target itself".into(),
        ));
    }
    bamboo_domain::validate_scoped_loop_control_command(command)
        .map_err(ToolError::InvalidArguments)?;
    if expected_link_revision == Some(0)
        || (!matches!(command, LoopControlRequest::Status) && expected_link_revision.is_none())
    {
        return Err(ToolError::InvalidArguments(
            "loop expected_link_revision must be nonzero and is required for mutations".into(),
        ));
    }
    if let Some(operation_id) = receipt_operation_id {
        if !matches!(command, LoopControlRequest::Status) {
            return Err(ToolError::InvalidArguments(
                "receipt_operation_id is only valid for loop status".into(),
            ));
        }
        uuid::Uuid::parse_str(operation_id).map_err(|_| {
            ToolError::InvalidArguments("receipt_operation_id must be a valid UUID".into())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_bootstrap_and_receipt_lookup_are_valid_structures_not_authority_grants() {
        let supervisor = SupervisorReference {
            session_id: bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::from_u128(1),
        };
        for receipt in [None, Some("A4B85B5B-BA44-488D-9900-20DD7E016E88")] {
            let mut arguments = json!({
                "action":"loop", "target_session_id":"target-a", "command":{"action":"status"},
            });
            if let Some(operation_id) = receipt {
                arguments["receipt_operation_id"] = json!(operation_id);
            }
            let SessionControlArgs::Loop {
                target_session_id,
                expected_link_revision,
                command,
                receipt_operation_id,
            } = serde_json::from_value(arguments).unwrap()
            else {
                panic!("expected loop arguments");
            };
            assert_eq!(expected_link_revision, None);
            assert_eq!(receipt_operation_id.as_deref(), receipt);
            assert_eq!(command, LoopControlRequest::Status);
            // This validates wire structure only. Live authority/host-grant
            // checks remain mandatory in supervisor_loop_control.
            validate_loop_arguments(
                &supervisor,
                &target_session_id,
                expected_link_revision,
                &command,
                receipt_operation_id.as_deref(),
            )
            .unwrap();
        }
    }
}
