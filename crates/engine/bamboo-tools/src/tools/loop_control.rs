//! Typed, stateless commands for the current session's bounded loop.
//!
//! This tool validates the command only. The owning serial runner checks
//! authority, host ceilings and compare-and-swap state, then persists the
//! operation before replacing this placeholder with a durable receipt.

use async_trait::async_trait;
use bamboo_agent_core::{
    Tool, ToolClass, ToolCtx, ToolError, ToolMutability, ToolOutcome, ToolResult,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use uuid::Uuid;

pub const LOOP_CONTROL_TOOL_NAME: &str = "loop_control";
const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;
const MAX_ITERATIONS: u32 = 100;
const MAX_TOTAL_ROUNDS: u32 = 1000;

/// A command for the executing session only; no target-session routing exists.
///
/// Deserialization rejects extra fields for every action, including `status`.
/// UUIDs and objectives retain their supplied spelling for owner-side operation
/// fingerprints. State-dependent checks belong to the owning runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum LoopControlRequest {
    Status,
    Start {
        operation_id: String,
        expected_generation: Option<String>,
        expected_revision: u64,
        objective: String,
        max_iterations: u32,
        max_total_rounds: u32,
    },
    Update {
        operation_id: String,
        generation: String,
        expected_revision: u64,
        objective: Option<String>,
        max_iterations: Option<u32>,
        max_total_rounds: Option<u32>,
    },
    Stop {
        operation_id: String,
        generation: String,
        expected_revision: u64,
    },
}

// An empty struct variant makes deny_unknown_fields apply to status as well as
// the data-bearing variants. A unit variant's ignored content can otherwise
// accept extra fields in an internally tagged enum.
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum LoopControlWireRequest {
    Status {},
    Start {
        operation_id: String,
        #[serde(default)]
        expected_generation: Option<String>,
        expected_revision: u64,
        objective: String,
        max_iterations: u32,
        max_total_rounds: u32,
    },
    Update {
        operation_id: String,
        generation: String,
        expected_revision: u64,
        #[serde(default)]
        objective: Option<String>,
        #[serde(default)]
        max_iterations: Option<u32>,
        #[serde(default)]
        max_total_rounds: Option<u32>,
    },
    Stop {
        operation_id: String,
        generation: String,
        expected_revision: u64,
    },
}

impl<'de> Deserialize<'de> for LoopControlRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match LoopControlWireRequest::deserialize(deserializer)? {
            LoopControlWireRequest::Status {} => Self::Status,
            LoopControlWireRequest::Start {
                operation_id,
                expected_generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            } => Self::Start {
                operation_id,
                expected_generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            },
            LoopControlWireRequest::Update {
                operation_id,
                generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            } => Self::Update {
                operation_id,
                generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            },
            LoopControlWireRequest::Stop {
                operation_id,
                generation,
                expected_revision,
            } => Self::Stop {
                operation_id,
                generation,
                expected_revision,
            },
        })
    }
}

impl LoopControlRequest {
    fn action(&self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Start { .. } => "start",
            Self::Update { .. } => "update",
            Self::Stop { .. } => "stop",
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Status => Ok(()),
            Self::Start {
                operation_id,
                expected_generation,
                objective,
                max_iterations,
                max_total_rounds,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                if let Some(generation) = expected_generation {
                    validate_uuid("expected_generation", generation)?;
                }
                validate_objective(objective)?;
                validate_bound("max_iterations", *max_iterations, MAX_ITERATIONS)?;
                validate_bound("max_total_rounds", *max_total_rounds, MAX_TOTAL_ROUNDS)
            }
            Self::Update {
                operation_id,
                generation,
                objective,
                max_iterations,
                max_total_rounds,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                validate_uuid("generation", generation)?;
                if objective.is_none() && max_iterations.is_none() && max_total_rounds.is_none() {
                    return Err("loop_control.update requires at least one patch field".into());
                }
                if let Some(objective) = objective {
                    validate_objective(objective)?;
                }
                if let Some(max_iterations) = max_iterations {
                    validate_bound("max_iterations", *max_iterations, MAX_ITERATIONS)?;
                }
                if let Some(max_total_rounds) = max_total_rounds {
                    validate_bound("max_total_rounds", *max_total_rounds, MAX_TOTAL_ROUNDS)?;
                }
                Ok(())
            }
            Self::Stop {
                operation_id,
                generation,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                validate_uuid("generation", generation)
            }
        }
    }
}

fn validate_uuid(field: &str, value: &str) -> Result<(), String> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| format!("loop_control.{field} must be a valid UUID"))
}

fn validate_objective(objective: &str) -> Result<(), String> {
    if objective.trim().is_empty() || objective.len() > MAX_OBJECTIVE_BYTES {
        return Err(format!(
            "loop_control.objective must contain non-whitespace text and be at most {MAX_OBJECTIVE_BYTES} UTF-8 bytes"
        ));
    }
    Ok(())
}

fn validate_bound(field: &str, value: u32, ceiling: u32) -> Result<(), String> {
    if !(1..=ceiling).contains(&value) {
        return Err(format!(
            "loop_control.{field} must be between 1 and {ceiling}"
        ));
    }
    Ok(())
}

/// Parse strict action fields and validate only state-independent constraints.
pub fn parse_loop_control_request(arguments: &str) -> Result<LoopControlRequest, String> {
    let request: LoopControlRequest = serde_json::from_str(arguments)
        .map_err(|error| format!("invalid loop_control arguments: {error}"))?;
    request.validate()?;
    Ok(request)
}

/// Validation-only front end; the session owner applies every command serially.
pub struct LoopControlTool;

impl LoopControlTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LoopControlTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for LoopControlTool {
    fn name(&self) -> &str {
        LOOP_CONTROL_TOOL_NAME
    }

    fn description(&self) -> &str {
        "Read or control this session's bounded loop only when the host explicitly enabled ExecuteRequest.loop_control(config). \
         status requires only action. start requires operation_id (UUID), expected_revision, objective, max_iterations and max_total_rounds; expected_generation is optional. \
         update requires operation_id, generation (UUID), expected_revision and at least one of objective/max_iterations/max_total_rounds. stop requires operation_id, generation and expected_revision. \
         Mutations use generation/control-revision compare-and-swap and an idempotent operation_id; read status before constructing a command. Receipts are bounded to 64 operations in the current generation; retired generations are stale. \
         Host ceilings, consumed budgets and user cancellation always take priority: a command cannot grant authority, overlap execution, restore a stopped run or increase the host's finite limits. \
         Updates may only tighten current limits without dropping below consumption. A new objective applies no earlier than the next iteration admission; only one pending objective is allowed. max_total_rounds is the absolute parent-round cap for this entire activation, including rounds already consumed before start, and grants no new rounds. Start is only for work that can complete without human questions, new approvals, asynchronous child work or background Bash; incompatible waits are refused without approval or a waiter. Interactive work requires a later ordinary activation outside this generation. This tool alone validates a command; only a durable owner receipt confirms application. No pause, resume, cadence or other-session target is supported."
    }

    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        match parse_loop_control_request(&args.to_string()) {
            Ok(LoopControlRequest::Status) => ToolClass {
                mutability: ToolMutability::ReadOnly,
                parallel_safe: false,
                promotable: false,
            },
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["status", "start", "update", "stop"],
                    "description": "status: no other fields. start: operation_id, expected_revision, objective, max_iterations, max_total_rounds; optional expected_generation. update: operation_id, generation, expected_revision, and at least one patch field. stop: operation_id, generation, expected_revision."
                },
                "operation_id": {
                    "type": "string",
                    "format": "uuid",
                    "description": "Required for start/update/stop. Reuse only for an identical command in the current generation; the owner checks idempotency before CAS. At most 64 operation receipts; retired generations are stale."
                },
                "expected_generation": {
                    "type": ["string", "null"],
                    "format": "uuid",
                    "description": "start only: expected prior generation UUID, or absent/null if none. The owner verifies current generation and revision."
                },
                "generation": {
                    "type": "string",
                    "format": "uuid",
                    "description": "Required for update/stop: exact current generation from status. Retired generations are stale; stop cannot be undone by start."
                },
                "expected_revision": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Required for start/update/stop: exact current control revision from status, independent of ordinary checkpoint state revisions. Stale commands are refused."
                },
                "objective": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_OBJECTIVE_BYTES,
                    "description": "Required for start; optional update patch. Non-whitespace text, at most 16384 UTF-8 bytes. An edit applies no earlier than the next iteration admission; at most one objective edit may be pending."
                },
                "max_iterations": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_ITERATIONS,
                    "description": "Required for start; optional update patch. Total iteration limit within the immutable host ceiling. Updates may only tighten the current limit and cannot drop below consumed iterations."
                },
                "max_total_rounds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOTAL_ROUNDS,
                    "description": "Required for start; optional update patch. Absolute parent-round cap for the entire current activation, including rounds consumed before start; never an additional allowance. Within immutable host ceilings; updates may only tighten and cannot drop below consumption."
                }
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let request =
            parse_loop_control_request(&args.to_string()).map_err(ToolError::InvalidArguments)?;
        let session_id = ctx
            .session_id()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ToolError::Execution("loop_control requires the current session context".into())
            })?;
        if ctx.plan_read_only && !matches!(request, LoopControlRequest::Status) {
            return Err(ToolError::Execution(
                "Plan mode: loop_control mutation blocked".into(),
            ));
        }
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "action": request.action(),
                "session_id": session_id,
                "status": "validated",
                "applied": false,
                "requires_owner_apply": true,
                "message": "loop_control command validated only; the current session runner must apply it and return a durable receipt before ToolComplete."
            })
            .to_string(),
            display_preference: Some("json".into()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const OPERATION_ID: &str = "A4B85B5B-BA44-488D-9900-20DD7E016E88";
    const GENERATION: &str = "b23bc011-5424-4b5e-8fc9-3c861b89874b";

    fn start() -> serde_json::Value {
        json!({
            "action": "start",
            "operation_id": OPERATION_ID,
            "expected_revision": 0,
            "objective": "  Verify the bounded loop  ",
            "max_iterations": 2,
            "max_total_rounds": 4
        })
    }

    fn update() -> serde_json::Value {
        json!({
            "action": "update",
            "operation_id": OPERATION_ID,
            "generation": GENERATION,
            "expected_revision": 1,
            "objective": "Use the updated objective"
        })
    }

    fn stop() -> serde_json::Value {
        json!({
            "action": "stop",
            "operation_id": OPERATION_ID,
            "generation": GENERATION,
            "expected_revision": 2
        })
    }

    fn context() -> ToolCtx {
        let mut context = ToolCtx::none("loop-control-test");
        context.session_id = Some(Arc::from("current-session"));
        context
    }

    #[test]
    fn parses_all_actions_and_preserves_fingerprint_inputs() {
        assert_eq!(
            parse_loop_control_request(r#"{"action":"status"}"#).unwrap(),
            LoopControlRequest::Status
        );
        let parsed = parse_loop_control_request(&start().to_string()).unwrap();
        assert!(matches!(
            &parsed,
            LoopControlRequest::Start {
                operation_id,
                expected_generation: None,
                objective,
                ..
            } if operation_id == OPERATION_ID && objective == "  Verify the bounded loop  "
        ));
        assert_eq!(serde_json::to_value(&parsed).unwrap()["action"], "start");
        assert_eq!(
            serde_json::from_str::<LoopControlRequest>(&serde_json::to_string(&parsed).unwrap())
                .unwrap(),
            parsed
        );
        assert!(matches!(
            parse_loop_control_request(&update().to_string()).unwrap(),
            LoopControlRequest::Update {
                max_iterations: None,
                max_total_rounds: None,
                ..
            }
        ));
        assert!(matches!(
            parse_loop_control_request(&stop().to_string()).unwrap(),
            LoopControlRequest::Stop { .. }
        ));
    }

    #[test]
    fn rejects_unknown_fields_for_every_action_including_direct_deserialization() {
        for mut args in [json!({"action": "status"}), start(), update(), stop()] {
            args["session_id"] = json!("another-session");
            assert!(parse_loop_control_request(&args.to_string()).is_err());
            assert!(serde_json::from_value::<LoopControlRequest>(args).is_err());
        }
        let mut args = start();
        args["generation"] = json!(GENERATION);
        assert!(parse_loop_control_request(&args.to_string()).is_err());
        let mut args = stop();
        args["objective"] = json!("not a stop field");
        assert!(parse_loop_control_request(&args.to_string()).is_err());
    }

    #[test]
    fn rejects_missing_fields_unknown_actions_and_wrong_types() {
        for field in [
            "operation_id",
            "expected_revision",
            "objective",
            "max_iterations",
            "max_total_rounds",
        ] {
            let mut args = start();
            args.as_object_mut().unwrap().remove(field);
            assert!(
                parse_loop_control_request(&args.to_string()).is_err(),
                "{field}"
            );
        }
        for args in [
            json!({}),
            json!({"action": "Status"}),
            json!({"action": "pause"}),
            json!({"action": "resume"}),
            json!({"action": 1}),
            json!(null),
            json!([]),
        ] {
            assert!(parse_loop_control_request(&args.to_string()).is_err());
        }
        for value in [json!(-1), json!(1.5), json!("1"), json!(null)] {
            let mut args = start();
            args["expected_revision"] = value;
            assert!(parse_loop_control_request(&args.to_string()).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_fields_and_trailing_input() {
        for arguments in [
            r#"{"action":"status","action":"status"}"#,
            r#"{"action":"status"} {}"#,
            r#"{"action":"stop","operation_id":"A4B85B5B-BA44-488D-9900-20DD7E016E88","generation":"b23bc011-5424-4b5e-8fc9-3c861b89874b","expected_revision":1,"expected_revision":2}"#,
        ] {
            assert!(parse_loop_control_request(arguments).is_err());
        }
    }

    #[test]
    fn validates_uuid_fields_without_normalizing_them() {
        for (mut args, field) in [
            (start(), "operation_id"),
            (start(), "expected_generation"),
            (update(), "generation"),
            (stop(), "generation"),
        ] {
            args[field] = json!("not-a-uuid");
            assert!(
                parse_loop_control_request(&args.to_string()).is_err(),
                "{field}"
            );
        }
        let mut args = start();
        args["expected_generation"] = json!(GENERATION);
        let parsed = parse_loop_control_request(&args.to_string()).unwrap();
        assert_eq!(
            serde_json::to_value(parsed).unwrap()["expected_generation"],
            GENERATION
        );
    }

    #[test]
    fn enforces_objective_utf8_byte_limit_and_non_whitespace() {
        for mut args in [start(), update()] {
            for objective in [
                "".into(),
                " \n\t".into(),
                "界".repeat(MAX_OBJECTIVE_BYTES / 3 + 1),
            ] {
                args["objective"] = json!(objective);
                assert!(parse_loop_control_request(&args.to_string()).is_err());
            }
            args["objective"] = json!("x".repeat(MAX_OBJECTIVE_BYTES));
            assert!(parse_loop_control_request(&args.to_string()).is_ok());
        }
    }

    #[test]
    fn enforces_positive_iteration_and_round_bounds_for_start_and_update() {
        for mut args in [start(), update()] {
            for (field, ceiling) in [
                ("max_iterations", MAX_ITERATIONS),
                ("max_total_rounds", MAX_TOTAL_ROUNDS),
            ] {
                for value in [json!(0), json!(ceiling + 1), json!(-1), json!(1.5)] {
                    args[field] = value;
                    assert!(
                        parse_loop_control_request(&args.to_string()).is_err(),
                        "{field}"
                    );
                }
                args[field] = json!(ceiling);
                assert!(parse_loop_control_request(&args.to_string()).is_ok());
            }
        }
    }

    #[test]
    fn update_requires_a_non_null_patch() {
        let mut args = update();
        args.as_object_mut().unwrap().remove("objective");
        assert!(parse_loop_control_request(&args.to_string()).is_err());
        args["objective"] = json!(null);
        args["max_iterations"] = json!(null);
        args["max_total_rounds"] = json!(null);
        assert!(parse_loop_control_request(&args.to_string()).is_err());
        args["max_total_rounds"] = json!(8);
        assert!(parse_loop_control_request(&args.to_string()).is_ok());
    }

    #[test]
    fn status_is_readonly_but_all_calls_are_serial_and_nonpromotable() {
        let tool = LoopControlTool::new();
        assert_eq!(
            tool.classify(&json!({"action": "status"})),
            ToolClass {
                mutability: ToolMutability::ReadOnly,
                parallel_safe: false,
                promotable: false,
            }
        );
        for args in [
            start(),
            update(),
            stop(),
            json!({"action": "status", "session_id": "foreign"}),
            json!({"action": "update"}),
            json!({"action": "unknown"}),
            json!(null),
        ] {
            assert_eq!(tool.classify(&args), ToolClass::MUTATING_SERIAL);
        }
    }

    #[test]
    fn schema_is_flat_bounded_and_requires_action() {
        let schema = LoopControlTool::new().parameters_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(
            schema["properties"]["max_iterations"]["maximum"],
            MAX_ITERATIONS
        );
        assert_eq!(
            schema["properties"]["max_total_rounds"]["maximum"],
            MAX_TOTAL_ROUNDS
        );
        assert!(schema.get("oneOf").is_none());
        assert!(schema["properties"].get("session_id").is_none());
        assert!(schema["properties"].get("target_session_id").is_none());
    }

    #[tokio::test]
    async fn invocation_is_validation_only_for_every_action() {
        let tool = LoopControlTool::new();
        for args in [json!({"action": "status"}), start(), update(), stop()] {
            let ToolOutcome::Completed(result) = tool.invoke(args, context()).await.unwrap() else {
                panic!("expected a validation-only completion")
            };
            assert!(result.success);
            let receipt: serde_json::Value = serde_json::from_str(&result.result).unwrap();
            assert_eq!(receipt["status"], "validated");
            assert_eq!(receipt["applied"], false);
            assert_eq!(receipt["requires_owner_apply"], true);
            assert_eq!(receipt["session_id"], "current-session");
            assert!(receipt.get("generation").is_none());
            assert!(receipt.get("revision").is_none());
            assert!(receipt.get("objective").is_none());
            assert!(receipt["message"]
                .as_str()
                .unwrap()
                .contains("validated only"));
        }
    }

    #[tokio::test]
    async fn invocation_requires_current_session_and_rejects_invalid_arguments() {
        let tool = LoopControlTool::new();
        assert!(matches!(
            tool.invoke(json!({"action": "status"}), ToolCtx::none("missing"))
                .await,
            Err(ToolError::Execution(_))
        ));
        let mut empty_context = context();
        empty_context.session_id = Some(Arc::from(" "));
        assert!(matches!(
            tool.invoke(start(), empty_context).await,
            Err(ToolError::Execution(_))
        ));
        assert!(matches!(
            tool.invoke(json!({"action": "start"}), context()).await,
            Err(ToolError::InvalidArguments(_))
        ));
    }

    #[tokio::test]
    async fn plan_posture_allows_status_and_blocks_all_mutations_even_with_bypass() {
        let tool = LoopControlTool::new();
        let mut context = context();
        context.plan_read_only = true;
        context.bypass_permissions = true;
        context.auto_approve_permissions = true;
        assert!(tool
            .invoke(json!({"action": "status"}), context.clone())
            .await
            .is_ok());
        for args in [start(), update(), stop()] {
            assert!(matches!(
                tool.invoke(args, context.clone()).await,
                Err(ToolError::Execution(message)) if message.contains("Plan mode")
            ));
        }
    }
}
