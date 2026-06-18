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
    /// Full prior conversation (serialized domain `Message`s, oldest first),
    /// INCLUDING the assignment's user message when present. The actor's
    /// durable state lives in the parent's store; each activation rehydrates
    /// from here — this is what makes send_message/update/rerun carry context
    /// across one-shot actor processes. Empty = first activation, no history.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<serde_json::Value>,
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
    /// Reply to a [`ChildFrame::SubagentRequest`] — the host's result for a
    /// SubAgent tool call the worker proxied back over this same WS. `id`
    /// correlates to the request; `body` is the proxied tool result JSON.
    SubagentReply {
        id: String,
        body: serde_json::Value,
    },
    /// Reply to a [`ChildFrame::ApprovalRequest`] — the host's human/policy
    /// decision on a gated tool the worker proxied back (Phase 2 child→parent
    /// approval delegation). `id` correlates to the request. When
    /// `approved == true` the worker records the grant locally and proceeds;
    /// `false` denies the tool. Mirrors the `SubagentReply` proxy pattern.
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
    /// The worker's `SubAgent` tool call, proxied to the host over this WS so a
    /// nested sub-agent's grandchildren are created in the host store (parented
    /// to this worker's host session). The host answers with
    /// [`ParentFrame::SubagentReply`] carrying the same `id`.
    SubagentRequest { id: String, body: serde_json::Value },
    /// The worker hit a tool needing human approval (Phase 2 child→parent
    /// approval delegation). Proxied to the host — which surfaces it to the
    /// human via the parent session's pending-question / notification path —
    /// mirroring [`ChildFrame::SubagentRequest`]. The host answers with
    /// [`ParentFrame::ApprovalReply`] carrying the same `id`. `body` carries
    /// `{tool_name, permission_type, resource, question}`.
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
        serde_json::to_string(self).expect("ParentFrame serializes")
    }
    pub fn from_text(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

impl ChildFrame {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).expect("ChildFrame serializes")
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
                messages: Vec::new(),
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

        // SubAgent request/reply round-trip over the per-child WS.
        let req = ChildFrame::SubagentRequest {
            id: "r1".into(),
            body: serde_json::json!({"action":"create"}),
        };
        assert_eq!(ChildFrame::from_text(&req.to_text()).unwrap(), req);
        let reply = ParentFrame::SubagentReply {
            id: "r1".into(),
            body: serde_json::json!({"success":true}),
        };
        assert_eq!(ParentFrame::from_text(&reply.to_text()).unwrap(), reply);

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
            messages: Vec::new(),
        });
        let v: serde_json::Value = serde_json::from_str(&f.to_text()).unwrap();
        assert_eq!(v["kind"], "run");
        assert_eq!(v["assignment"], "a");
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
