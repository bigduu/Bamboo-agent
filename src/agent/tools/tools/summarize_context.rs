//! On-demand context summarization tool.
//!
//! This tool allows the LLM to explicitly request a re-summarization of the
//! conversation context. It is conditionally injected into the tool schema
//! only when context usage exceeds 75%, to save tokens in normal operation.
//!
//! The tool itself is a lightweight signal — actual summarization happens
//! in the context preparation pipeline. When called, it reads the current
//! conversation summary (if any) and returns it, signaling that the model
//! is aware of context pressure and wants continuity information.

use async_trait::async_trait;
use serde_json::json;

use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};

/// Tool name constant for conditional injection checks.
pub const SUMMARIZE_CONTEXT_TOOL_NAME: &str = "summarize_context";

#[derive(Debug, Default)]
pub struct SummarizeContextTool;

impl SummarizeContextTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for SummarizeContextTool {
    fn name(&self) -> &str {
        SUMMARIZE_CONTEXT_TOOL_NAME
    }

    fn description(&self) -> &str {
        "Request a summary of earlier conversation context that was compressed due to context window limits. \
         Use this when you feel you are missing important context from earlier in the conversation. \
         Returns the current conversation summary if available."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Optional: why you need the context summary (helps improve future summaries)."
                }
            },
            "required": []
        })
    }

    async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
        // This tool needs context to access the session's conversation summary.
        Err(ToolError::Execution(
            "summarize_context must be executed with ToolExecutionContext".to_string(),
        ))
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let reason = args
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("not specified");

        let session_id = ctx.session_id.unwrap_or("unknown");

        tracing::info!(
            "[{}] summarize_context tool called (reason: {})",
            session_id,
            reason
        );

        // The actual summary content is injected into the context by the preparation
        // pipeline. This tool serves as a signal that the model is aware of context
        // pressure. We return a helpful message indicating the summary is already
        // in the context, or that no summary is available yet.
        //
        // Note: We don't have direct access to the session here. The summary is
        // already injected into prepared_context by prepare_hybrid_context().
        // This tool acts as acknowledgment that the model wants context continuity.
        Ok(ToolResult {
            success: true,
            result: "Context summary has been injected into your conversation context automatically. \
                     If you see a '## Previous Conversation Summary' section near the start of your context, \
                     that contains the summary of earlier compressed messages. \
                     If no summary section is visible, no messages have been compressed yet.\n\n\
                     The summary is automatically updated each time messages are compressed. \
                     You can continue your work with the available context."
                .to_string(),
            display_preference: None,
        })
    }
}
