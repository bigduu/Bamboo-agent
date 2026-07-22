//! Wire protocol: discovery record + parent/child WebSocket frames.
//!
//! The session/event payloads are kept opaque (`serde_json::Value`) so this crate stays a leaf;
//! the real `AgentEvent` serializes into [`ChildFrame::Event`] verbatim (design §6, zero mapping).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Tier-1 discovery record an actor publishes into the file fabric so others can find it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRecord {
    pub agent_id: String,
    pub role: String,
    #[serde(default)]
    pub labels: Vec<String>,
    /// `ws://127.0.0.1:<port>` reachable endpoint.
    pub endpoint: String,
    pub pid: u32,
    #[serde(default)]
    pub version: String,
    pub started_at: DateTime<Utc>,
    /// Lease: a reader treats the record as stale once `now > lease_expires_at`.
    pub lease_expires_at: DateTime<Utc>,
}

/// A unit of work a parent assigns to an actor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSpec {
    pub assignment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Effective permission policy captured by the host at this activation
    /// boundary. Keeping it on `RunSpec` (rather than only provisioning) lets
    /// warm, broker and remote workers observe policy revisions and bypass
    /// changes on their next activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_policy: Option<PermissionPolicyContext>,
    /// Full prior conversation (serialized domain `Message`s, oldest first),
    /// INCLUDING the assignment's user message when present. The actor's
    /// durable state lives in the parent's store; each activation rehydrates
    /// from here — this is what makes send_message/update/rerun carry context
    /// across one-shot actor processes. Empty = first activation, no history.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<serde_json::Value>,
    /// Secrets minted for this activation only. They are delivered in-memory
    /// over the actor transport and must never be persisted by the worker.
    #[serde(default, skip_serializing_if = "RunSecrets::is_empty")]
    pub secrets: RunSecrets,
}

/// Per-activation secret envelope. A Bamboo-routed Codex token lives here so a
/// warm worker never reuses a credential from an earlier run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunSecrets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_provider_token: Option<SecretValue>,
}

impl RunSecrets {
    pub fn is_empty(&self) -> bool {
        self.codex_provider_token.is_none()
    }
}

/// Serializable secret whose debug representation is always redacted.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

/// Host-computed permission state for one actor activation. The policy payload
/// is opaque here so `bamboo-subagent` remains a transport leaf.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionPolicyContext {
    pub revision: u64,
    pub bypass_permissions: bool,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Session grants are deliberately not inherited across an actor boundary;
    /// a future opt-in protocol can set this and carry explicit scoped grants.
    #[serde(default)]
    pub inherit_session_grants: bool,
    pub policy: serde_json::Value,
}

/// Parent → child control/in-band frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParentFrame {
    Run(RunSpec),
    Cancel,
    Message {
        text: String,
    },
    /// Reply to a [`ChildFrame::ApprovalRequest`] — the host's human/policy
    /// decision on a gated tool the worker proxied back (Phase 2 child→parent
    /// approval delegation). `id` correlates to the request. When
    /// `approved == true` the worker records the grant locally and proceeds;
    /// `false` denies the tool.
    ApprovalReply {
        id: String,
        approved: bool,
    },
}

/// Child → parent event/terminal frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChildFrame {
    /// One agent event, serialized verbatim (the real `AgentEvent` lands here as JSON).
    Event { event: serde_json::Value },
    /// The worker hit a tool needing human approval (Phase 2 child→parent
    /// approval delegation). Proxied to the host — which surfaces it to the
    /// human via the parent session's pending-question / notification path. The
    /// host answers with [`ParentFrame::ApprovalReply`] carrying the same `id`.
    /// `body` carries `{tool_name, permission_type, resource, question}`.
    ApprovalRequest { id: String, body: serde_json::Value },
    Terminal {
        status: TerminalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// Full worker transcript (serialized domain `Message`s) shipped on
        /// suspend so the host can persist it onto the child session and
        /// rehydrate the worker on resume. Empty for non-suspend terminals.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transcript: Vec<serde_json::Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    Completed,
    Error,
    Cancelled,
    /// The worker's loop suspended (it spawned its own sub-agents and is waiting
    /// on them). Non-terminal to the host: the completion coordinator resumes
    /// the worker (re-dispatch) once its children finish.
    Suspended,
}

impl ParentFrame {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
    pub fn from_text(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

impl ChildFrame {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
    pub fn from_text(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_frames_round_trip() {
        for f in [
            ParentFrame::Run(RunSpec {
                assignment: "do x".into(),
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                secrets: Default::default(),
            }),
            ParentFrame::Cancel,
            ParentFrame::Message { text: "hi".into() },
        ] {
            assert_eq!(ParentFrame::from_text(&f.to_text()).unwrap(), f);
        }
    }

    #[test]
    fn child_frames_round_trip() {
        let e = ChildFrame::Event {
            event: serde_json::json!({"type":"token","content":"hi"}),
        };
        assert_eq!(ChildFrame::from_text(&e.to_text()).unwrap(), e);
        let t = ChildFrame::Terminal {
            status: TerminalStatus::Completed,
            result: Some("done".into()),
            error: None,
            transcript: Vec::new(),
        };
        assert_eq!(ChildFrame::from_text(&t.to_text()).unwrap(), t);

        // Suspend terminal carries the worker transcript.
        let s = ChildFrame::Terminal {
            status: TerminalStatus::Suspended,
            result: None,
            error: None,
            transcript: vec![serde_json::json!({"role":"assistant","content":"x"})],
        };
        assert_eq!(ChildFrame::from_text(&s.to_text()).unwrap(), s);

        // Phase 2 approval request/reply round-trip over the per-child WS.
        let areq = ChildFrame::ApprovalRequest {
            id: "a1".into(),
            body: serde_json::json!({
                "tool_name": "Write",
                "permission_type": "WriteFile",
                "resource": "/tmp/x",
                "question": "approve?",
            }),
        };
        assert_eq!(ChildFrame::from_text(&areq.to_text()).unwrap(), areq);
        let areply = ParentFrame::ApprovalReply {
            id: "a1".into(),
            approved: true,
        };
        assert_eq!(ParentFrame::from_text(&areply.to_text()).unwrap(), areply);
    }

    #[test]
    fn run_frame_tag_is_stable() {
        let f = ParentFrame::Run(RunSpec {
            assignment: "a".into(),
            reasoning_effort: Some("high".into()),
            permission_policy: None,
            messages: Vec::new(),
            secrets: Default::default(),
        });
        let v: serde_json::Value = serde_json::from_str(&f.to_text()).unwrap();
        assert_eq!(v["kind"], "run");
        assert_eq!(v["assignment"], "a");
        assert!(v.get("secrets").is_none());
    }

    #[test]
    fn run_secret_round_trips_but_debug_output_is_redacted() {
        let secret = SecretValue::new("bcx1_secret-570");
        assert_eq!(format!("{secret:?}"), "SecretValue([REDACTED])");
        assert!(!format!(
            "{:?}",
            RunSecrets {
                codex_provider_token: Some(secret.clone()),
            }
        )
        .contains("secret-570"));

        let frame = ParentFrame::Run(RunSpec {
            assignment: "a".into(),
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            secrets: RunSecrets {
                codex_provider_token: Some(secret),
            },
        });
        let decoded = ParentFrame::from_text(&frame.to_text()).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn permission_policy_context_round_trips_at_run_boundary() {
        let context = PermissionPolicyContext {
            revision: 9,
            bypass_permissions: true,
            session_id: "child-1".into(),
            workspace_path: Some("/workspace/project".into()),
            inherit_session_grants: false,
            policy: serde_json::json!({"enabled":true,"durable_rules":[]}),
        };
        let frame = ParentFrame::Run(RunSpec {
            assignment: "work".into(),
            reasoning_effort: None,
            permission_policy: Some(context.clone()),
            messages: Vec::new(),
            secrets: Default::default(),
        });
        let decoded = ParentFrame::from_text(&frame.to_text()).unwrap();
        assert_eq!(decoded, frame);
        let ParentFrame::Run(run) = decoded else {
            panic!("expected run frame");
        };
        assert_eq!(run.permission_policy, Some(context));
    }

    #[test]
    fn run_frame_without_messages_parses_backward_compat() {
        // An old-style frame (no `messages` field) must still parse.
        let parsed = ParentFrame::from_text(r#"{"kind":"run","assignment":"x"}"#).unwrap();
        match parsed {
            ParentFrame::Run(spec) => {
                assert_eq!(spec.assignment, "x");
                assert!(spec.messages.is_empty());
            }
            other => panic!("expected run frame, got {other:?}"),
        }
    }
}
