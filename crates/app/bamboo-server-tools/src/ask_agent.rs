//! `ask_agent` — the in-loop "command another agent" tool.
//!
//! The Host-wired tool inspects a durable Child Session or steers it through
//! SessionMessenger. Physical cluster workers still use the central broker.
//! The caller's session id is the asker (physical replies route back to it).
//! The tool resolves a returned ActorId, live deployment alias, or registered
//! cluster node worker id before dispatch; standalone callers may still address
//! a broker peer directly. Two modes mirror
//! `AskMode`: `query` (read-only summarize/extract) and
//! `steer` (durably redirect a logical Child, or command a physical peer).
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
use bamboo_domain::{
    ActorDirectoryPort, ActorLogicalState, ActorSession, Session, SessionKind, SessionMessageBody,
    SessionMessageContent, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource,
};
use bamboo_engine::{SessionMessenger, SessionMessengerError};
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
    messenger: Option<Arc<SessionMessenger>>,
}

impl AskAgentTool {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: token.into(),
            deployments: None,
            messenger: None,
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

    /// Production routes a legacy Child steer through the same durable
    /// logical inbox used by SubAgent, never through a worker's private chat.
    pub fn with_messenger(mut self, messenger: Arc<SessionMessenger>) -> Self {
        self.messenger = Some(messenger);
        self
    }
}

async fn canonical_steer(
    messenger: &SessionMessenger,
    caller: &str,
    actor: &ActorSession,
    tool_call_id: &str,
    message: &str,
) -> Result<serde_json::Value, ToolError> {
    if message.trim().is_empty() || message.len() > 16 * 1024 {
        return Err(ToolError::InvalidArguments(
            "Child steer message must contain 1..=16384 UTF-8 bytes".into(),
        ));
    }
    let envelope = SessionMessageEnvelope {
        id: SessionMessageId::stable(
            "legacy-ask-agent-steer-v1",
            &json!({
                "caller": caller,
                "actor_id": actor.actor_id,
                "actor_birth": actor.session_created_at,
                "tool_call_id": tool_call_id,
            }),
        ),
        source: SessionMessageSource::Session {
            session_id: caller.to_string(),
        },
        target_session_id: actor.actor_id.clone(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text(message)),
        created_at: chrono::Utc::now(),
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: None,
    };
    let result = messenger.send(envelope).await;
    let (receipt, activation, activation_error) = match result {
        Ok(receipt) => (
            receipt.delivery,
            match receipt.activation {
                bamboo_domain::SessionActivationDisposition::ActiveNotified => "active_notified",
                bamboo_domain::SessionActivationDisposition::ActivationReserved => {
                    "activation_reserved"
                }
                bamboo_domain::SessionActivationDisposition::ActivationCoalesced => {
                    "activation_coalesced"
                }
            },
            None,
        ),
        Err(SessionMessengerError::Activation {
            receipt, source, ..
        }) => {
            tracing::warn!(%source, actor_id = %actor.actor_id, "legacy Child steer activation pending");
            (
                receipt,
                "activation_pending",
                Some("receiver_activation_pending"),
            )
        }
        Err(SessionMessengerError::ActivationEligibility {
            receipt, source, ..
        }) => {
            tracing::warn!(%source, actor_id = %actor.actor_id, "legacy Child steer activation ineligible");
            (
                receipt,
                "activation_retry_required",
                Some("receiver_activation_ineligible"),
            )
        }
        Err(error) => {
            tracing::warn!(%error, actor_id = %actor.actor_id, "legacy Child steer admission failed");
            return Err(ToolError::Execution(
                "Child steer was not admitted; inspect Child diagnostics before retrying".into(),
            ));
        }
    };
    Ok(json!({
        "from": actor.actor_id,
        "mode": "steer",
        "admitted": true,
        "message_id": receipt.id.to_string(),
        "inbox_generation": receipt.generation,
        "activation": activation,
        "activation_error": activation_error,
    }))
}

/// A saved Session id is an authority-bearing logical address, not a broker
/// mailbox. Verify the direct parent and both durable births before allowing
/// a Root to inspect its existing Child through the shared Session.
async fn authorize_direct_child(
    store: &SessionStoreV2,
    caller: &str,
    child: &Session,
) -> Result<ActorSession, ToolError> {
    let unauthorized = || {
        ToolError::Execution(
            "target is not an existing direct Child Actor owned by this Root".into(),
        )
    };
    let parent = store
        .load_session(caller)
        .await
        .map_err(|_| ToolError::Execution("canonical caller state is unavailable".into()))?
        .ok_or_else(unauthorized)?;
    if parent.kind != SessionKind::Root
        || parent.id != caller
        || parent.root_session_id != caller
        || parent.parent_session_id.is_some()
        || child.kind != SessionKind::Child
        || child.parent_session_id.as_deref() != Some(caller)
        || child.root_session_id != caller
        || child.spawn_depth != parent.spawn_depth.saturating_add(1)
        || child.project_id_meta() != parent.project_id_meta()
    {
        return Err(unauthorized());
    }
    let parent_actor = store
        .inspect_actor(caller)
        .await
        .map_err(|_| unauthorized())?
        .actor;
    let child_entry = store
        .inspect_actor(&child.id)
        .await
        .map_err(|_| unauthorized())?;
    let child_actor = child_entry.actor;
    let direct_parent = child_actor.ancestor_observations.first();
    if !parent_actor.matches_session(&parent)
        || parent_actor.project_id.as_deref() != parent.project_id_meta().as_deref()
        || parent_actor.state == ActorLogicalState::Retired
        || !child_actor.matches_session(child)
        || child_actor.project_id.as_deref() != child.project_id_meta().as_deref()
        || child_actor.parent_actor_id.as_deref() != Some(caller)
        || child_actor.root_actor_id != parent_actor.actor_id
        || child_actor.state == ActorLogicalState::Retired
        || direct_parent.is_none_or(|ancestor| {
            ancestor.actor_id != parent_actor.actor_id
                || ancestor.session_created_at != parent_actor.session_created_at
        })
    {
        return Err(unauthorized());
    }
    Ok(child_actor)
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
        "Inspect an existing direct Child Actor through its durable Session, or ask a \
         physical cluster worker through the broker. `target` is a logical Child ActorId, a live \
         deployment alias, or a registered cluster node worker id.\n\
         \n\
         TWO MODES (pick deliberately):\n\
         - mode=query (default) — READ-ONLY. For a Host-owned Child ActorId, use question=status or \
         question=progress to receive a bounded canonical Session snapshot; no model answers a \
         free-form question on this path. For a physical cluster worker, the broker forwards your \
         question and returns that worker's answer.\n\
         - mode=steer — WRITE. Existing direct Child ActorIds receive a durable SessionInbox \
         message and activation request. A physical cluster worker receives the question through the broker.\n\
         \n\
         EXAMPLES:\n\
         - ask_agent(target=\"actor-…\", question=\"status\", mode=query) reads saved Actor progress.\n\
         - ask_agent(target=\"actor-…\", question=\"Focus on the assigned tests\", mode=steer) durably corrects a Child.\n\
         - ask_agent(target=<worker_id>, question=\"Summarize the auth flow\", mode=query) \
         asks a physical cluster worker.\n\
         \n\
         Physical worker calls block until that worker answers or `timeout_secs` elapses \
         (default 60, max 300). Host Child reads use canonical storage."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "target": { "type": "string", "description": "An existing direct Child ActorId, a live deployment alias, or a registered cluster node worker id." },
                "question": { "type": "string", "description": "What to ask the target agent." },
                "mode": {
                    "type": "string",
                    "enum": ["query", "steer"],
                    "description": "Host Child query = bounded canonical status/progress snapshot; Host Child steer = durable SessionInbox delivery. Physical worker query/steer uses broker replies."
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
        if ctx.plan_read_only && matches!(mode, AskMode::Steer) {
            return Err(ToolError::Execution(
                "ask_agent steer is unavailable in Plan mode".into(),
            ));
        }
        let timeout = Duration::from_secs(
            parsed
                .timeout_secs
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .clamp(1, MAX_TIMEOUT_SECS),
        );

        // Production supplies a Host store.
        // An exact saved Session id always belongs to the logical authority
        // plane. A known but unauthorized Root/Child must never fall through to
        // a fabric worker whose mailbox happens to share its string id.
        if let Some((_, store)) = &self.deployments {
            let saved = store.load_session(&parsed.target).await.map_err(|_| {
                ToolError::Execution("canonical target state is unavailable".into())
            })?;
            if let Some(child) = saved {
                let actor = authorize_direct_child(store, caller, &child).await?;
                return match mode {
                    AskMode::Query => {
                        let snapshot = canonical_query(store, &actor, &parsed.question).await?;
                        let current = authorize_direct_child(store, caller, &child).await?;
                        if current.session_created_at != actor.session_created_at
                            || current.project_id != actor.project_id
                        {
                            return Err(ToolError::Execution(
                                "canonical Child identity changed while ask_agent was in flight"
                                    .into(),
                            ));
                        }
                        Ok(ToolOutcome::Completed(ToolResult::text(
                            true,
                            json!({"from": actor.actor_id, "mode": "query", "snapshot": snapshot})
                                .to_string(),
                        )))
                    }
                    AskMode::Steer => {
                        authorize_direct_child(store, caller, &child).await?;
                        let messenger = self.messenger.as_ref().ok_or_else(|| {
                            ToolError::Execution("logical SessionMessenger is unavailable".into())
                        })?;
                        let delivered = canonical_steer(
                            messenger,
                            caller,
                            &actor,
                            ctx.tool_call_id.as_ref(),
                            &parsed.question,
                        )
                        .await?;
                        let success = delivered["activation"] != "activation_retry_required";
                        Ok(ToolOutcome::Completed(ToolResult::text(
                            success,
                            delivered.to_string(),
                        )))
                    }
                };
            }
        }
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
        if let (AskMode::Steer, Some(bound), Some((_, store))) =
            (mode, resolved.as_ref(), self.deployments.as_ref())
        {
            if let Some(actor) = bound.actor.as_ref() {
                let child = store
                    .load_session(&actor.actor_id)
                    .await
                    .map_err(|_| ToolError::Execution("canonical Child is unavailable".into()))?
                    .ok_or_else(|| ToolError::Execution("canonical Child is unavailable".into()))?;
                let current = authorize_direct_child(store, caller, &child).await?;
                if current != *actor {
                    return Err(ToolError::Execution(
                        "deployment Actor identity changed before steer".into(),
                    ));
                }
                let messenger = self.messenger.as_ref().ok_or_else(|| {
                    ToolError::Execution("logical SessionMessenger is unavailable".into())
                })?;
                let delivered = canonical_steer(
                    messenger,
                    caller,
                    actor,
                    ctx.tool_call_id.as_ref(),
                    &parsed.question,
                )
                .await?;
                let success = delivered["activation"] != "activation_retry_required";
                return Ok(ToolOutcome::Completed(ToolResult::text(
                    success,
                    delivered.to_string(),
                )));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        SessionActivationDisposition, SessionActivationError, SessionActivationPort,
        SessionInboxLimits, SessionInboxPort,
    };
    use bamboo_storage::FileSessionInbox;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingActivation(Mutex<Vec<(String, u64)>>);

    #[async_trait]
    impl SessionActivationPort for RecordingActivation {
        async fn request_activation(
            &self,
            target_session_id: &str,
            inbox_generation: u64,
        ) -> Result<SessionActivationDisposition, SessionActivationError> {
            self.0
                .lock()
                .await
                .push((target_session_id.to_string(), inbox_generation));
            Ok(SessionActivationDisposition::ActivationReserved)
        }
    }

    fn caller(root: &str, call: &str) -> ToolCtx {
        let mut ctx = ToolCtx::none(call);
        ctx.session_id = Some(Arc::from(root));
        ctx
    }

    async fn steer(
        tool: &AskAgentTool,
        root: &str,
        child: &str,
        call: &str,
        message: &str,
    ) -> Result<serde_json::Value, ToolError> {
        let result = tool
            .invoke(
                json!({"target":child,"question":message,"mode":"steer"}),
                caller(root, call),
            )
            .await?;
        let ToolOutcome::Completed(result) = result else {
            panic!("legacy Child steer must complete with an admission receipt")
        };
        Ok(serde_json::from_str(&result.result).unwrap())
    }

    #[tokio::test]
    async fn host_child_steer_is_durable_idempotent_and_direct_parent_only() {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let root = Session::new("ask-root", "model");
        let child = Session::new_child_of("ask-child", &root, "model", "task");
        let other = Session::new("other-root", "model");
        store.save_session(&root).await.unwrap();
        store.save_session(&child).await.unwrap();
        store.save_session(&other).await.unwrap();

        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let activation = Arc::new(RecordingActivation::default());
        let messenger = Arc::new(SessionMessenger::new(
            store.clone(),
            inbox.clone(),
            activation.clone(),
        ));
        let tool = AskAgentTool::new("ws://127.0.0.1:1", "unused")
            .with_deployments(Arc::new(Mutex::new(Default::default())), store)
            .with_messenger(messenger);

        let first = steer(&tool, &root.id, &child.id, "call-1", "continue narrowly")
            .await
            .unwrap();
        assert_eq!(first["from"], child.id);
        assert_eq!(first["activation"], "activation_reserved");
        assert_eq!(inbox.inspect(&child.id).await.unwrap().pending, 1);
        assert_eq!(
            activation.0.lock().await.as_slice(),
            &[(child.id.clone(), 1)]
        );

        let retry = steer(&tool, &root.id, &child.id, "call-1", "continue narrowly")
            .await
            .unwrap();
        assert_eq!(retry["message_id"], first["message_id"]);
        assert_eq!(retry["inbox_generation"], first["inbox_generation"]);
        assert_eq!(inbox.inspect(&child.id).await.unwrap().pending, 1);

        assert!(steer(&tool, &root.id, &child.id, "call-1", "broaden scope")
            .await
            .is_err());
        assert!(steer(&tool, &other.id, &child.id, "call-2", "take over")
            .await
            .is_err());
        assert_eq!(inbox.inspect(&child.id).await.unwrap().pending, 1);

        let mut plan = caller(&root.id, "call-plan");
        plan.plan_read_only = true;
        assert!(tool
            .invoke(
                json!({"target":child.id,"question":"mutate","mode":"steer"}),
                plan,
            )
            .await
            .is_err());
    }
}
