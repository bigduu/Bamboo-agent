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
}

/// Parent → child control/in-band frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParentFrame {
    Run(RunSpec),
    Cancel,
    Message { text: String },
}

/// Child → parent event/terminal frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChildFrame {
    /// One agent event, serialized verbatim (the real `AgentEvent` lands here as JSON).
    Event { event: serde_json::Value },
    Terminal {
        status: TerminalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    Completed,
    Error,
    Cancelled,
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
        };
        assert_eq!(ChildFrame::from_text(&t.to_text()).unwrap(), t);
    }

    #[test]
    fn run_frame_tag_is_stable() {
        let f = ParentFrame::Run(RunSpec {
            assignment: "a".into(),
            reasoning_effort: Some("high".into()),
        });
        let v: serde_json::Value = serde_json::from_str(&f.to_text()).unwrap();
        assert_eq!(v["kind"], "run");
        assert_eq!(v["assignment"], "a");
    }
}
