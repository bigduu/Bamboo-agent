//! `update_goal` — the agent's explicit completion signal for the goal loop.
//!
//! Mirrors OpenAI Codex's `update_goal` tool: while a session goal is active the
//! runtime keeps re-injecting a rigorous completion-audit continuation prompt,
//! and the *main agent itself* declares the objective finished (or genuinely
//! blocked) by calling this tool. The tool is intentionally minimal — it only
//! records the declared status. The engine's post-execution handler persists it
//! into the durable goal state, and a side-channel Gold evaluator double-checks
//! the claim at the terminal point before the run actually stops.
//!
//! The tool is only surfaced to the model when the goal loop is active (see
//! `resolve_available_tool_schemas_for_session`), so it never appears in
//! ordinary sessions.

use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::json;

/// Canonical tool name. Kept identical to Codex so prompts transfer cleanly.
pub const UPDATE_GOAL_TOOL_NAME: &str = "update_goal";

#[derive(Debug, Deserialize)]
struct UpdateGoalArgs {
    status: String,
}

/// Record the agent's self-reported goal status (`complete` | `blocked`).
pub struct UpdateGoalTool;

impl UpdateGoalTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for UpdateGoalTool {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse and normalize the `status` argument, returning the canonical lowercase
/// form on success. Shared with the engine handler so validation stays in one
/// place.
pub fn parse_update_goal_status(arguments: &str) -> Result<String, String> {
    let parsed: UpdateGoalArgs = serde_json::from_str(arguments)
        .map_err(|e| format!("invalid update_goal arguments: {e}"))?;
    let status = parsed.status.trim().to_ascii_lowercase();
    match status.as_str() {
        "complete" | "blocked" => Ok(status),
        other => Err(format!(
            "update_goal.status must be 'complete' or 'blocked', got '{other}'"
        )),
    }
}

#[async_trait]
impl Tool for UpdateGoalTool {
    fn name(&self) -> &str {
        UPDATE_GOAL_TOOL_NAME
    }

    fn description(&self) -> &str {
        "Update the active session goal. Use this ONLY to mark the goal `complete` or `blocked`.\n\
         Set status to `complete` only when the objective has actually been achieved and no required work remains — not because the budget is nearly exhausted or because you are stopping. Completion is reverified against the current state before the run ends, so do not claim it on weak, indirect, or unverified evidence.\n\
         Set status to `blocked` only when the same blocking condition has repeated for at least three consecutive goal turns (counting the original turn and any automatic continuations) and you genuinely cannot make progress without user input or an external-state change. Never use `blocked` merely because work is hard, slow, uncertain, or incomplete."
    }

    /// Treated as read-only for approval/scheduling purposes: it touches no
    /// filesystem or external state. The durable goal-state mutation happens in
    /// the engine post-execution handler (same pattern as `session_note`).
    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["complete", "blocked"],
                    "description": "`complete` when the full objective is achieved and verified; `blocked` only after the strict blocked audit (same blocker ≥3 consecutive goal turns)."
                }
            },
            "required": ["status"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let arguments = args.to_string();
        let status = parse_update_goal_status(&arguments).map_err(ToolError::InvalidArguments)?;

        let message = match status.as_str() {
            "complete" => {
                "Recorded goal status: complete. Before the run ends the runtime will verify the \
                 objective against the current state; if anything remains unproven you will be \
                 asked to keep working."
            }
            // Only "complete" | "blocked" reach here (validated above).
            _ => "Recorded goal status: blocked.",
        };

        Ok(ToolOutcome::Completed(ToolResult::text(true, message)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn complete_status_is_recorded() {
        let tool = UpdateGoalTool::new();
        let out = tool
            .invoke(json!({"status": "complete"}), ToolCtx::none("t"))
            .await
            .expect("complete accepted");
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
        assert!(result.result.to_lowercase().contains("complete"));
    }

    #[tokio::test]
    async fn blocked_status_is_recorded() {
        let tool = UpdateGoalTool::new();
        let out = tool
            .invoke(json!({"status": "BLOCKED"}), ToolCtx::none("t"))
            .await
            .expect("blocked accepted (case-insensitive)");
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
        assert!(result.result.to_lowercase().contains("blocked"));
    }

    #[tokio::test]
    async fn rejects_unknown_status() {
        let tool = UpdateGoalTool::new();
        let result = tool
            .invoke(json!({"status": "paused"}), ToolCtx::none("t"))
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn rejects_missing_status() {
        let tool = UpdateGoalTool::new();
        assert!(tool.invoke(json!({}), ToolCtx::none("t")).await.is_err());
    }

    #[test]
    fn parse_helper_normalizes_case() {
        assert_eq!(
            parse_update_goal_status(r#"{"status":"Complete"}"#).unwrap(),
            "complete"
        );
        assert!(parse_update_goal_status(r#"{"status":"nope"}"#).is_err());
    }
}
