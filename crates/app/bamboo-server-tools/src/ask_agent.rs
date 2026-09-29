//! `ask_agent` — the in-loop "command another agent" tool.
//!
//! Lets a running (root) agent ask another agent — deployed as a local
//! subprocess, in Docker, or on a remote host — a question over the central
//! message broker, and judge the answer. The caller's session id is the asker
//! (replies route back to it). The Host-wired tool resolves a returned ActorId,
//! live deployment alias, or registered cluster node worker id before dispatch;
//! standalone callers may still address a broker peer directly. Two modes mirror
//! `AskMode`: `query` (read-only summarize/extract) and
//! `steer` (insert into the target's conversation to redirect its work).
//!
//! Only registered on the Root surface when a broker is configured
//! (`subagents.broker` in config).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_storage::SessionStoreV2;
use bamboo_subagent::{AgentRef, AskMode};

use crate::deploy_agent::{resolve_ask_target, DeployedRegistry};

/// Default / max wait for an answer.
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const MAX_TIMEOUT_SECS: u64 = 300;

pub struct AskAgentTool {
    endpoint: String,
    token: String,
    deployments: Option<(DeployedRegistry, Arc<SessionStoreV2>)>,
}

impl AskAgentTool {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: token.into(),
            deployments: None,
        }
    }
    pub fn with_deployments(
        mut self,
        registry: DeployedRegistry,
        store: Arc<SessionStoreV2>,
    ) -> Self {
        self.deployments = Some((registry, store));
        self
    }
}

/// Host-bound query reads only the saved logical Session. It supports exact
/// status/progress selectors; the physical peer's private conversation cannot
/// answer an arbitrary question about the canonical Actor.
async fn canonical_query(
    store: &SessionStoreV2,
    actor: &bamboo_domain::ActorSession,
    question: &str,
) -> Result<serde_json::Value, ToolError> {
    if !matches!(question.trim(), "status" | "progress") {
        return Err(ToolError::InvalidArguments(
            "Host Actor query supports only question=status or question=progress; use SubAgent inspect for canonical details".into(),
        ));
    }
    let session = store
        .load_session(&actor.actor_id)
        .await
        .map_err(|_| ToolError::Execution("canonical Actor state is unavailable".into()))?
        .filter(|session| {
            actor.matches_session(session)
                && actor.project_id.as_deref() == session.project_id_meta().as_deref()
        })
        .ok_or_else(|| ToolError::Execution("canonical Actor identity changed".into()))?;
    let recent_messages = session
        .messages
        .iter()
        .rev()
        .take(8)
        .map(|message| {
            json!({
                "role": message.role,
                "content_utf8_bytes": message.content.len(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "last_run_status": session.last_run_status(),
        "message_count": session.messages.len(),
        "recent_messages": recent_messages,
        "updated_at": session.updated_at,
    }))
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
        "Inspect a Host-owned Actor or ask a physical cluster worker through the broker. \
         `target` is a logical ActorId, its live alias, or a registered cluster node worker id.\n\
         \n\
         TWO MODES (pick deliberately):\n\
         - mode=query (default) — READ-ONLY. For a Host-owned ActorId, use question=status or \
         question=progress to receive a bounded canonical Session snapshot; no model answers a \
         free-form question on this path. For a physical cluster worker, the broker forwards your \
         question and returns that worker's answer.\n\
         - mode=steer — WRITE. Supported for a physical cluster worker: your question is injected \
         into its live conversation. Host-owned ActorIds require canonical SessionInbox delivery, \
         so steer currently returns an explicit unsupported error for them.\n\
         \n\
         EXAMPLES:\n\
         - ask_agent(target=\"actor-…\", question=\"status\", mode=query) reads saved Actor progress.\n\
         - ask_agent(target=<worker_id>, question=\"Summarize the auth flow\", mode=query) \
         asks a physical cluster worker.\n\
         \n\
         Physical worker calls block until that worker answers or `timeout_secs` elapses \
         (default 60, max 300). Host Actor status/progress reads return from canonical storage."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "target": { "type": "string", "description": "An existing Host ActorId, its live deployment alias, or a registered cluster node worker id." },
                "question": { "type": "string", "description": "What to ask the target agent." },
                "mode": {
                    "type": "string",
                    "enum": ["query", "steer"],
                    "description": "Host Actor query = bounded canonical status/progress snapshot; Host Actor steer is unsupported. Physical worker query/steer uses broker replies."
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

        let resolved = if let Some((registry, store)) = &self.deployments {
            resolve_ask_target(registry, Some(store), Some(caller), &parsed.target).await?
        } else {
            None
        };
        // Production wiring always has a Host store. Never let a missing or
        // stale registry entry turn into a raw broker mailbox address. Direct
        // callers using `new` retain the standalone legacy peer contract.
        if self.deployments.is_some() && resolved.is_none() {
            return Err(ToolError::Execution(
                "target is not a live deployment owned by the caller".into(),
            ));
        }
        let target = resolved
            .as_ref()
            .map(|target| target.worker_id.as_str())
            .unwrap_or(&parsed.target);
        let public_id = resolved
            .as_ref()
            .and_then(|target| target.actor.as_ref())
            .map(|actor| actor.actor_id.as_str())
            .unwrap_or(&parsed.target);
        if matches!(mode, AskMode::Steer)
            && resolved
                .as_ref()
                .is_some_and(|target| target.actor.is_some())
        {
            return Err(ToolError::Execution(
                "Host Actor steer requires canonical SessionInbox delivery; no broker-private transcript mutation was made".into(),
            ));
        }
        if let (AskMode::Query, Some(bound), Some((registry, store))) =
            (mode, resolved.as_ref(), self.deployments.as_ref())
        {
            if let Some(actor) = bound.actor.as_ref() {
                let snapshot = canonical_query(store, actor, &parsed.question).await?;
                let current =
                    resolve_ask_target(registry, Some(store), Some(caller), &parsed.target).await?;
                if current.is_none_or(|current| {
                    current.worker_id != bound.worker_id || current.activation != bound.activation
                }) {
                    return Err(ToolError::Execution(
                        "deployment changed while ask_agent was in flight".into(),
                    ));
                }
                return Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({"from":public_id,"mode":"query","snapshot":snapshot})
                        .to_string(),
                    display_preference: None,
                    images: Vec::new(),
                }));
            }
        }
        let answer = bamboo_broker::ask_agent(
            &self.endpoint,
            me,
            &self.token,
            target,
            &parsed.question,
            mode,
            timeout,
        )
        .await
        .map_err(|error| {
            tracing::warn!(%error, "ask_agent transport failed");
            ToolError::Execution("ask_agent could not obtain a reply from the deployment".into())
        })?;

        // A deployment may be stopped or its alias reused while the broker
        // call is in flight. Do not return a reply from an owner that has lost
        // its Host activation since dispatch.
        if let (Some(bound), Some((registry, store))) = (&resolved, &self.deployments) {
            let current =
                resolve_ask_target(registry, Some(store), Some(caller), &parsed.target).await?;
            if current.is_none_or(|current| {
                current.worker_id != bound.worker_id || current.activation != bound.activation
            }) {
                return Err(ToolError::Execution(
                    "deployment changed while ask_agent was in flight".into(),
                ));
            }
        }

        let mode_str = if matches!(mode, AskMode::Steer) {
            "steer"
        } else {
            "query"
        };
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({ "from": public_id, "mode": mode_str, "answer": answer }).to_string(),
            display_preference: None,
            images: Vec::new(),
        }))
    }
}
