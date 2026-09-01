//! Wire protocol: discovery record + parent/child WebSocket frames.
//!
//! The session/event payloads are kept opaque (`serde_json::Value`) so this crate stays a leaf;
//! the real `AgentEvent` serializes into a sequenced [`ActorEventBatch`]. The legacy
//! [`ChildFrame::Event`] remains decodable during rolling upgrades.

use bamboo_domain::{ProjectId, SessionActivationPolicy, SessionMessageEnvelope};
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
    /// Stable domain identity for the session being activated. Actor process,
    /// mailbox, and pooled-worker ids are transport details and must never
    /// replace these values in worker persistence or message routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_session: Option<LogicalSessionIdentity>,
    /// Stable Project identity inherited from the parent session. The typed
    /// wire value rejects unsafe/invalid identifiers during deserialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
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
    /// Independently authoritative id of the host activation whose execution
    /// this RunSpec starts. Initial and mid-run typed deliveries must match it;
    /// a delivery's own run-id field is never accepted as self-authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_run_id: Option<String>,
    /// Host-issued fencing epoch for this concrete execution attempt. A retry
    /// on a different worker gets a newer epoch even when it belongs to the
    /// same logical activation, so late frames from the replaced worker can be
    /// rejected. Zero is reserved for legacy senders and selects the legacy
    /// one-event wire shape on a new worker during rolling upgrades.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub execution_epoch: u64,
    /// Canonical logical-session deliveries that caused this idle actor
    /// activation. The worker durably enqueues these before entering its first
    /// provider boundary, then confirms admission over the child frame stream.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_session_messages: Vec<SessionMessageDelivery>,
    /// Secrets minted for this activation only. They are delivered in-memory
    /// over the actor transport and must never be persisted by the worker.
    #[serde(default, skip_serializing_if = "RunSecrets::is_empty")]
    pub secrets: RunSecrets,
}

/// Logical session ancestry carried across every actor placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSessionIdentity {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Delivery semantics for one actor event batch.
///
/// `Durable` batches must use the broker's acknowledged mailbox lane.
/// `Snapshot` and `Ephemeral` batches may use the bounded live lane: sequence
/// gaps tell a consumer to reload the authoritative session snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorEventQos {
    Durable,
    Snapshot,
    Ephemeral,
}

impl ActorEventQos {
    /// Classify opaque serialized `AgentEvent` JSON without making this leaf
    /// crate depend on `bamboo-agent-core`.
    pub fn classify(event: &serde_json::Value) -> Self {
        match event.get("type").and_then(serde_json::Value::as_str) {
            Some("token" | "reasoning_token" | "tool_token" | "sub_agent_heartbeat") => {
                Self::Ephemeral
            }
            // A raw child projection inherits the inner event's semantics. New
            // hosts no longer recursively project these to parents, but this is
            // needed for rolling-upgrade workers that still do.
            Some("sub_agent_event") => event
                .get("event")
                .map(Self::classify)
                .unwrap_or(Self::Durable),
            Some("runner_progress" | "token_budget_updated" | "context_pressure_notification") => {
                Self::Snapshot
            }
            // This is a versioned delta, not a reconstructable full snapshot.
            // Losing it can leave task state behind even when later unrelated
            // events arrive, and core exposes it on the durable account feed.
            Some("task_list_item_progress") => Self::Durable,
            // Unknown events are never silently downgraded onto a lossy lane.
            _ => Self::Durable,
        }
    }
}

/// Maximum event count accepted in one actor wire batch. This bounds decode
/// and fan-out work per frame independently of payload byte limits enforced by
/// WebSocket implementations.
pub const MAX_ACTOR_EVENT_BATCH_EVENTS: usize = 64;

/// A compact, ordered actor event batch. Route/fencing metadata is common to
/// the batch; `first_seq..=last_seq` assigns one sequence number per item in
/// `events` and is scoped to `(logical session, activation, execution epoch)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorEventBatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_session: Option<LogicalSessionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub execution_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_actor_id: Option<String>,
    pub first_seq: u64,
    pub last_seq: u64,
    pub qos: ActorEventQos,
    pub events: Vec<serde_json::Value>,
}

impl ActorEventBatch {
    /// Reject malformed ranges, oversized batches, and QoS downgrades before a
    /// broker routes the frame onto a lossy lane.
    pub fn validate(&self) -> Result<(), String> {
        if self.events.is_empty() {
            return Err("actor event batch is empty".to_string());
        }
        if self.events.len() > MAX_ACTOR_EVENT_BATCH_EVENTS {
            return Err(format!(
                "actor event batch has {} events; maximum is {MAX_ACTOR_EVENT_BATCH_EVENTS}",
                self.events.len()
            ));
        }
        let expected_last = self
            .first_seq
            .checked_add(self.events.len() as u64 - 1)
            .ok_or_else(|| "actor event batch sequence range overflows".to_string())?;
        if self.first_seq == 0 || self.last_seq != expected_last {
            return Err("actor event batch has an invalid sequence range".to_string());
        }
        if self
            .events
            .iter()
            .any(|event| ActorEventQos::classify(event) != self.qos)
        {
            return Err("actor event batch QoS does not match its events".to_string());
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PendingActorEventBatch {
    first_seq: u64,
    qos: ActorEventQos,
    events: Vec<serde_json::Value>,
}

/// Per-run event batch builder shared by direct and broker transports. Durable
/// events flush immediately; snapshot/ephemeral events coalesce until a QoS
/// boundary, size bound, or the caller's latency timer fires.
#[derive(Debug)]
pub struct ActorEventBatcher {
    logical_session: Option<LogicalSessionIdentity>,
    activation_id: Option<String>,
    execution_epoch: u64,
    source_node_id: Option<String>,
    source_actor_id: Option<String>,
    next_seq: u64,
    pending: Option<PendingActorEventBatch>,
}

impl ActorEventBatcher {
    pub fn for_run(
        spec: &RunSpec,
        source_node_id: Option<String>,
        source_actor_id: Option<String>,
    ) -> Self {
        Self {
            logical_session: spec.logical_session.clone(),
            activation_id: spec.activation_run_id.clone(),
            execution_epoch: spec.execution_epoch,
            source_node_id,
            source_actor_id,
            next_seq: 1,
            pending: None,
        }
    }

    /// Add one event and return every batch that became ready. At most two are
    /// returned: an older lossy batch followed by an immediate durable event.
    pub fn push(&mut self, event: serde_json::Value) -> Vec<ActorEventBatch> {
        let qos = ActorEventQos::classify(&event);
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let mut ready = Vec::with_capacity(2);

        if qos == ActorEventQos::Durable {
            if let Some(batch) = self.flush() {
                ready.push(batch);
            }
            ready.push(self.build(seq, qos, vec![event]));
            return ready;
        }

        let boundary = self.pending.as_ref().is_some_and(|pending| {
            pending.qos != qos || pending.events.len() >= MAX_ACTOR_EVENT_BATCH_EVENTS
        });
        if boundary {
            if let Some(batch) = self.flush() {
                ready.push(batch);
            }
        }
        let pending = self.pending.get_or_insert_with(|| PendingActorEventBatch {
            first_seq: seq,
            qos,
            events: Vec::with_capacity(MAX_ACTOR_EVENT_BATCH_EVENTS),
        });
        pending.events.push(event);
        if pending.events.len() >= MAX_ACTOR_EVENT_BATCH_EVENTS {
            if let Some(batch) = self.flush() {
                ready.push(batch);
            }
        }
        ready
    }

    pub fn flush(&mut self) -> Option<ActorEventBatch> {
        let pending = self.pending.take()?;
        Some(self.build(pending.first_seq, pending.qos, pending.events))
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    fn build(
        &self,
        first_seq: u64,
        qos: ActorEventQos,
        events: Vec<serde_json::Value>,
    ) -> ActorEventBatch {
        let last_seq = first_seq + events.len() as u64 - 1;
        ActorEventBatch {
            logical_session: self.logical_session.clone(),
            activation_id: self.activation_id.clone(),
            execution_epoch: self.execution_epoch,
            source_node_id: self.source_node_id.clone(),
            source_actor_id: self.source_actor_id.clone(),
            first_seq,
            last_seq,
            qos,
            events,
        }
    }
}

/// One canonical inbox claim forwarded to an active actor. The activation run
/// id and claim generation make the worker's confirmation unambiguous even if
/// a stale connection delivers a late frame after a successor has taken over.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessageDelivery {
    pub target_session_id: String,
    pub envelope: SessionMessageEnvelope,
    pub canonical_claim_generation: u64,
    pub activation_run_id: String,
    /// Durable host policy associated with the authorized claim prefix. The
    /// worker mirrors it onto its local receipt before the safe-turn boundary.
    #[serde(default)]
    pub activation_policy: SessionActivationPolicy,
}

/// Worker proof that its local safe-turn path durably checkpointed and acked a
/// forwarded envelope. The host still has to checkpoint the canonical logical
/// transcript before it may ack the canonical claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessageAdmissionConfirmation {
    pub target_session_id: String,
    pub envelope_id: String,
    pub canonical_claim_generation: u64,
    pub activation_run_id: String,
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
    /// Exact typed session request (`default`, `bypass`, or `auto`). Empty is a
    /// rolling-upgrade legacy payload and is derived from the booleans below.
    #[serde(default)]
    pub requested_mode: String,
    /// Host-resolved effective mode, including Plan/read-only hard overlays.
    /// Empty is accepted only for legacy payloads.
    #[serde(default)]
    pub effective_mode: String,
    pub bypass_permissions: bool,
    #[serde(default)]
    pub auto_approve_permissions: bool,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    /// Session grants are deliberately not inherited across an actor boundary;
    /// a future opt-in protocol can set this and carry explicit scoped grants.
    #[serde(default)]
    pub inherit_session_grants: bool,
    pub policy: serde_json::Value,
}

impl PermissionPolicyContext {
    /// Validate a newly-produced wire posture and decode rolling-upgrade
    /// payloads. Dual permissive flags are always contradictory: Auto never
    /// borrows Bypass semantics.
    pub fn resolved_modes(
        &self,
    ) -> Result<
        (
            bamboo_domain::SessionPermissionMode,
            bamboo_domain::PermissionMode,
        ),
        String,
    > {
        if self.auto_approve_permissions && self.bypass_permissions {
            return Err(
                "permission_policy auto_approve_permissions and bypass_permissions are mutually exclusive"
                    .to_string(),
            );
        }
        let has_requested_mode = !self.requested_mode.is_empty();
        let has_effective_mode = !self.effective_mode.is_empty();
        if has_requested_mode != has_effective_mode {
            return Err(
                "permission_policy requested_mode and effective_mode must be provided together"
                    .to_string(),
            );
        }
        let requested = if self.requested_mode.is_empty() {
            if self.auto_approve_permissions {
                bamboo_domain::SessionPermissionMode::Auto
            } else if self.bypass_permissions {
                bamboo_domain::SessionPermissionMode::Bypass
            } else {
                bamboo_domain::SessionPermissionMode::Default
            }
        } else {
            match self.requested_mode.as_str() {
                "default" => bamboo_domain::SessionPermissionMode::Default,
                "bypass" => bamboo_domain::SessionPermissionMode::Bypass,
                "auto" => bamboo_domain::SessionPermissionMode::Auto,
                other => return Err(format!("invalid requested permission mode '{other}'")),
            }
        };
        let effective = if self.effective_mode.is_empty() {
            bamboo_domain::resolve_permission_mode(
                requested,
                bamboo_domain::PermissionMode::Default,
            )
            .effective
        } else {
            bamboo_domain::PermissionMode::from_audit_str(&self.effective_mode).ok_or_else(
                || {
                    format!(
                        "invalid effective permission mode '{}'",
                        self.effective_mode
                    )
                },
            )?
        };
        let resolution = bamboo_domain::PermissionModeResolution {
            requested,
            effective,
        };
        if !resolution.is_consistent() {
            return Err("permission_policy requested/effective modes are inconsistent".into());
        }
        if has_requested_mode {
            if self.bypass_permissions != resolution.bypass_permissions() {
                return Err("permission_policy bypass flag disagrees with effective mode".into());
            }
            if self.auto_approve_permissions != resolution.suppress_approval_prompts() {
                return Err(
                    "permission_policy auto flag disagrees with no-prompt resolution".into(),
                );
            }
        }
        Ok((requested, effective))
    }
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
    SessionMessage {
        delivery: SessionMessageDelivery,
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
    /// Sequenced event batch used by current workers. Route metadata prevents a
    /// stale/replaced Cluster actor from updating the wrong logical activation.
    EventBatch { batch: ActorEventBatch },
    /// The worker hit a tool needing human approval (Phase 2 child→parent
    /// approval delegation). Proxied to the host — which surfaces it to the
    /// human via the parent session's pending-question / notification path. The
    /// host answers with [`ParentFrame::ApprovalReply`] carrying the same `id`.
    /// `body` carries `{tool_name, permission_type, resource, question}`.
    ApprovalRequest { id: String, body: serde_json::Value },
    /// Emitted only after the worker's local SessionInbox transcript + cursor
    /// checkpoint and admitted receipt are durable.
    SessionMessageAdmitted {
        confirmation: SessionMessageAdmissionConfirmation,
    },
    Terminal {
        status: TerminalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// Compatibility-only suspend payload. Current hosts reject Suspended
        /// and never consume this field; canonical session checkpoints are the
        /// transcript authority. Retained for rolling wire compatibility.
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
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: None,
                execution_epoch: 0,
                initial_session_messages: Vec::new(),
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
        let batch = ChildFrame::EventBatch {
            batch: ActorEventBatch {
                logical_session: Some(LogicalSessionIdentity {
                    session_id: "child".into(),
                    parent_session_id: Some("parent".into()),
                    root_session_id: "root".into(),
                }),
                activation_id: Some("run-7".into()),
                execution_epoch: 9,
                source_node_id: Some("node-a".into()),
                source_actor_id: Some("worker-2".into()),
                first_seq: 1,
                last_seq: 2,
                qos: ActorEventQos::Ephemeral,
                events: vec![
                    serde_json::json!({"type":"token","content":"a"}),
                    serde_json::json!({"type":"token","content":"b"}),
                ],
            },
        };
        assert_eq!(ChildFrame::from_text(&batch.to_text()).unwrap(), batch);
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
            logical_session: None,
            project_id: None,
            reasoning_effort: Some("high".into()),
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
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
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
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
            requested_mode: "bypass".into(),
            effective_mode: "bypass".into(),
            bypass_permissions: true,
            auto_approve_permissions: false,
            session_id: "child-1".into(),
            workspace_path: Some("/workspace/project".into()),
            inherit_session_grants: false,
            policy: serde_json::json!({"enabled":true,"durable_rules":[]}),
        };
        let frame = ParentFrame::Run(RunSpec {
            assignment: "work".into(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: Some(context.clone()),
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
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
    fn legacy_permission_policy_context_defaults_auto_to_false() {
        let frame = ParentFrame::from_text(
            r#"{"kind":"run","assignment":"work","permission_policy":{"revision":8,"bypass_permissions":true,"session_id":"legacy-child","inherit_session_grants":false,"policy":{}}}"#,
        )
        .unwrap();
        let ParentFrame::Run(run) = frame else {
            panic!("expected run frame");
        };
        let context = run.permission_policy.expect("permission policy");
        assert!(context.bypass_permissions);
        assert!(!context.auto_approve_permissions);
        assert_eq!(
            context.resolved_modes().unwrap(),
            (
                bamboo_domain::SessionPermissionMode::Bypass,
                bamboo_domain::PermissionMode::BypassPermissions,
            )
        );
    }

    #[test]
    fn permission_policy_rejects_partial_typed_mode_pairs() {
        let context = PermissionPolicyContext {
            revision: 1,
            requested_mode: "auto".to_string(),
            effective_mode: String::new(),
            bypass_permissions: false,
            auto_approve_permissions: true,
            session_id: "partial-policy".to_string(),
            workspace_path: None,
            inherit_session_grants: false,
            policy: serde_json::json!({}),
        };
        assert!(context
            .resolved_modes()
            .unwrap_err()
            .contains("provided together"));

        let effective_only = PermissionPolicyContext {
            requested_mode: String::new(),
            effective_mode: "auto".to_string(),
            ..context
        };
        assert!(effective_only
            .resolved_modes()
            .unwrap_err()
            .contains("provided together"));
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

    #[test]
    fn run_frame_round_trips_typed_project_identity() {
        let frame = ParentFrame::Run(RunSpec {
            assignment: "work".into(),
            logical_session: None,
            project_id: Some(ProjectId::parse("project-1").unwrap()),
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        });

        let decoded = ParentFrame::from_text(&frame.to_text()).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn run_frame_rejects_unsafe_project_identity() {
        let error =
            ParentFrame::from_text(r#"{"kind":"run","assignment":"x","project_id":"../other"}"#)
                .unwrap_err();

        assert!(error.to_string().contains("invalid project id"));
    }

    #[test]
    fn actor_event_batcher_sequences_and_separates_qos() {
        let spec = RunSpec {
            assignment: "work".into(),
            logical_session: Some(LogicalSessionIdentity {
                session_id: "child".into(),
                parent_session_id: Some("parent".into()),
                root_session_id: "root".into(),
            }),
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: Some("activation-1".into()),
            execution_epoch: 4,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        };
        let mut batcher =
            ActorEventBatcher::for_run(&spec, Some("node-a".into()), Some("actor-a".into()));
        assert!(batcher
            .push(serde_json::json!({"type":"token","content":"a"}))
            .is_empty());
        assert!(batcher
            .push(serde_json::json!({"type":"token","content":"b"}))
            .is_empty());

        let ready = batcher.push(serde_json::json!({
            "type":"tool_start",
            "tool_call_id":"t1",
            "tool_name":"Read",
            "arguments":{}
        }));
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].qos, ActorEventQos::Ephemeral);
        assert_eq!((ready[0].first_seq, ready[0].last_seq), (1, 2));
        assert_eq!(ready[1].qos, ActorEventQos::Durable);
        assert_eq!((ready[1].first_seq, ready[1].last_seq), (3, 3));
        assert!(ready.iter().all(|batch| batch.validate().is_ok()));
        assert_eq!(ready[1].activation_id.as_deref(), Some("activation-1"));
        assert_eq!(ready[1].execution_epoch, 4);
        assert!(!batcher.has_pending());
    }

    #[test]
    fn actor_event_batch_rejects_qos_downgrade_and_bad_range() {
        let mut batch = ActorEventBatch {
            logical_session: None,
            activation_id: None,
            execution_epoch: 0,
            source_node_id: None,
            source_actor_id: None,
            first_seq: 1,
            last_seq: 1,
            qos: ActorEventQos::Ephemeral,
            events: vec![serde_json::json!({"type":"tool_start"})],
        };
        assert!(batch.validate().unwrap_err().contains("QoS"));
        batch.qos = ActorEventQos::Durable;
        batch.last_seq = 2;
        assert!(batch.validate().unwrap_err().contains("sequence"));
    }

    #[test]
    fn task_item_progress_delta_is_never_put_on_a_lossy_lane() {
        let event = serde_json::json!({
            "type": "task_list_item_progress",
            "session_id": "child",
            "item_id": "task-1",
            "status": "in_progress",
            "tool_calls_count": 2,
            "version": 3
        });
        assert_eq!(ActorEventQos::classify(&event), ActorEventQos::Durable);
    }
}
