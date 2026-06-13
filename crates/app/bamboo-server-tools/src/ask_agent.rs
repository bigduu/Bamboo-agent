//! `ask_agent` — the in-loop "command another agent" tool.
//!
//! Lets a running (root) agent ask another agent — deployed as a local
//! subprocess, in Docker, or on a remote host — a question over the central
//! message broker, and judge the answer. The caller's session id is the asker
//! (replies route back to it); the `target` is the other agent's broker mailbox
//! key. Two modes mirror `AskMode`: `query` (read-only summarize/extract) and
//! `steer` (insert into the target's conversation to redirect its work).
//!
//! Only registered on the Root surface when a broker is configured
//! (`subagents.broker` in config).

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_subagent::{AgentRef, AskMode};

/// Default / max wait for an answer.
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const MAX_TIMEOUT_SECS: u64 = 300;

pub struct AskAgentTool {
    endpoint: String,
    token: String,
}

impl AskAgentTool {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: token.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct AskArgs {
    target: String,
    question: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[async_trait]
impl Tool for AskAgentTool {
    fn name(&self) -> &str {
        "ask_agent"
    }

    fn description(&self) -> &str {
        "Ask another agent (deployed locally, in Docker, or on a remote host) a question over the \
         message broker and get its answer back. `target` is the agent's id (its broker mailbox \
         key). mode=query (default) is read-only — the target summarizes/extracts from its current \
         state without changing it. mode=steer inserts your question into the target's live \
         conversation to redirect or advance its work. Returns the target's answer."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "target": { "type": "string", "description": "The target agent's id (broker mailbox key)." },
                "question": { "type": "string", "description": "What to ask the target agent." },
                "mode": {
                    "type": "string",
                    "enum": ["query", "steer"],
                    "description": "query = read-only summarize/extract (default); steer = insert into the target's conversation / redirect its work."
                },
                "timeout_secs": { "type": "number", "description": "Max seconds to wait for the answer (default 60, max 300)." }
            },
            "required": ["target", "question"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let caller = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("ask_agent requires a session_id in tool context".to_string())
        })?;
        let parsed: AskArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid ask_agent args: {e}")))?;

        let mode = match parsed.mode.as_deref() {
            Some("steer") => AskMode::Steer,
            Some("query") | None => AskMode::Query,
            Some(other) => {
                return Err(ToolError::InvalidArguments(format!(
                    "unknown mode '{other}' (use 'query' or 'steer')"
                )))
            }
        };
        let timeout = Duration::from_secs(
            parsed
                .timeout_secs
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .clamp(1, MAX_TIMEOUT_SECS),
        );
        let me = AgentRef {
            session_id: caller.to_string(),
            role: None,
        };

        let answer = bamboo_broker::ask_agent(
            &self.endpoint,
            me,
            &self.token,
            &parsed.target,
            &parsed.question,
            mode,
            timeout,
        )
        .await
        .map_err(|e| ToolError::Execution(format!("ask_agent failed: {e}")))?;

        let mode_str = if matches!(mode, AskMode::Steer) {
            "steer"
        } else {
            "query"
        };
        Ok(ToolResult {
            success: true,
            result: json!({ "from": parsed.target, "mode": mode_str, "answer": answer })
                .to_string(),
            display_preference: None,
            images: Vec::new(),
        })
    }
}
