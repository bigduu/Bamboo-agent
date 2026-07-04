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

use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
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
        "Ask another agent — already running locally, in a Docker container, or on a remote host — \
         a question over the message broker, and get its answer back synchronously. This is how you \
         COMMAND a worker you (or a teammate) deployed: `target` is that agent's id (the broker \
         mailbox key returned by deploy_agent, or a peer's session id). Replies route back to you \
         automatically.\n\
         \n\
         TWO MODES (pick deliberately):\n\
         - mode=query (default) — READ-ONLY. The target inspects its OWN current state and \
         summarizes/extracts an answer WITHOUT changing what it is doing. Use it to poll progress, \
         pull a result, or ask 'what did you find?'. Safe to call repeatedly.\n\
         - mode=steer — WRITE. Your question is injected into the target's LIVE conversation, so it \
         redirects or advances the target's work (a command, not a peek). Use it to assign the next \
         task, change priorities, or hand off new context.\n\
         \n\
         WORKED EXAMPLE (deploy → poll → steer):\n\
         1. deploy_agent(action=deploy, env=docker, image=\"bamboo:latest\", role=\"researcher\") \
         → returns id \"agent-1a2b3c\".\n\
         2. ask_agent(target=\"agent-1a2b3c\", question=\"Summarize the auth flow in this repo.\", \
         mode=query) → wait for its findings.\n\
         3. ask_agent(target=\"agent-1a2b3c\", question=\"Now write the fix to src/auth.rs and run \
         the tests.\", mode=steer) → reassigns it to do the work.\n\
         4. ask_agent(target=\"agent-1a2b3c\", question=\"Are the tests green yet?\", mode=query) → \
         poll until done.\n\
         \n\
         Blocks until the target answers or `timeout_secs` elapses (default 60, max 300) — raise it \
         for slow work. The target must be reachable on the broker; deploy it with deploy_agent \
         first if it does not exist yet."
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

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL.promotable()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let caller = ctx.session_id().ok_or_else(|| {
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
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({ "from": parsed.target, "mode": mode_str, "answer": answer })
                .to_string(),
            display_preference: None,
            images: Vec::new(),
        }))
    }
}
