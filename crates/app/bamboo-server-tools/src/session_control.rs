//! Narrow model control over already attached independent Roots.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::SessionActivationDisposition;
use bamboo_engine::{SessionMessenger, SessionMessengerError};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SessionControlArgs {
    Followup {
        target_session_id: String,
        operation_id: String,
        message: String,
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
        "Supervisor-only followup to an already attached existing independent Root. The target continues with its original context. Use a stable operation_id per target; retry the same operation with exactly the same message to recover an uncertain result, never a new id. Returns durable admission and activation-request status, not task completion. Pending human questions remain unanswered. Use session_history action=read_messages for bounded result readback. This tool cannot create or attach sessions, answer approvals, or cancel work."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "action": {"type": "string", "enum": ["followup"]},
                "target_session_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "operation_id": {"type": "string", "minLength": 1, "maxLength": 256,
                    "description": "Stable operation id for this target; no whitespace padding, slashes or '..'. Reuse it only with the same message."},
                "message": {"type": "string", "minLength": 1, "maxLength": 16384,
                    "description": "One followup instruction, at most 16 KiB in UTF-8."}
            },
            "required": ["action", "target_session_id", "operation_id", "message"]
        })
    }

    fn classify(&self, _args: &Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if ctx.plan_read_only {
            return Err(ToolError::Execution(
                "session_control followup is unavailable in Plan mode".into(),
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
        let SessionControlArgs::Followup {
            target_session_id,
            operation_id,
            message,
        } = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("invalid session_control arguments: {error}"))
        })?;
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
            Err(error) => return Err(ToolError::Execution(error.to_string())),
        };
        Ok(ToolOutcome::Completed(ToolResult::text(true, json!({
            "action": "followup", "target_session_id": target_session_id,
            "operation_id": operation_id, "admitted": true,
            "receipt_id": delivery.id, "generation": delivery.generation,
            "activation": activation, "activation_error": detail,
            "note": "Admission is durable; this is not completion. A waiting target remains waiting. Retry with the same operation_id and message if activation needs recovery; read results with session_history.read_messages."
        }).to_string())))
    }
}
