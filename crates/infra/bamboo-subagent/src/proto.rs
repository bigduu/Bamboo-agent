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

/// Actual worker message suffix, carried in the existing sequenced event lane.
/// This cache observation grants nothing; the Host separately validates its
/// current callable ceiling, event trace and fenced canonical append.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ReadOnlyActorTranscript {
    #[serde(rename = "owned_readonly_transcript")]
    Complete { messages: Vec<serde_json::Value> },
}
impl ReadOnlyActorTranscript {
    pub const MAX_BYTES: usize = 64 * 1024;
    pub fn validate(&self) -> Result<(), &'static str> {
        let Self::Complete { messages } = self;
        if messages.len() != 3
            || serde_json::to_vec(self).map_or(true, |bytes| bytes.len() > Self::MAX_BYTES)
        {
            return Err("owned_readonly_transcript_unsupported");
        }
        Ok(())
    }
}

/// Completion DATA on the existing durable event lane, never Session authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum LocalToolMessages {
    #[serde(rename = "local_client_tool_messages_v1")]
    Complete {
        version: u32,
        messages: Vec<serde_json::Value>,
    },
}

impl LocalToolMessages {
    pub const TYPE: &'static str = "local_client_tool_messages_v1";
    pub const MAX_BYTES: usize = 64 * 1024;
    pub const MAX_MESSAGES: usize = 128;
    pub const MAX_PAIRS: usize = 32;

    pub fn supports_tools(tools: &[String], read_only: bool) -> bool {
        !tools.is_empty()
            && !(read_only && tools.len() == 1 && tools[0] == "Glob")
            && tools
                .iter()
                .all(|name| matches!(name.as_str(), "Read" | "Glob" | "Write"))
    }

    pub fn validate(&self) -> Result<Vec<bamboo_domain::Message>, &'static str> {
        use std::io::Write;
        struct Limit(usize);
        impl Write for Limit {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .filter(|n| *n <= LocalToolMessages::MAX_BYTES)
                    .ok_or_else(|| std::io::Error::other("local tool history limit"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let Self::Complete { version, messages } = self;
        if *version != 1
            || messages.is_empty()
            || messages.len() > Self::MAX_MESSAGES
            || serde_json::to_writer(Limit(0), self).is_err()
        {
            return Err("local_tool_history_unsupported");
        }
        let typed: Vec<bamboo_domain::Message> = messages
            .iter()
            .map(|value| {
                let message: bamboo_domain::Message = serde_json::from_value(value.clone())
                    .map_err(|_| "local_tool_history_unsupported")?;
                if serde_json::to_value(&message).map_err(|_| "local_tool_history_unsupported")?
                    != *value
                    || message.id.is_empty()
                    || message.id.len() > 128
                    || message.reasoning.is_some()
                    || message.reasoning_signature.is_some()
                    || message.content_parts.is_some()
                    || message.image_ocr.is_some()
                    || message.compressed
                    || message.compressed_by_event_id.is_some()
                    || message.compression_level != 0
                {
                    return Err("local_tool_history_unsupported");
                }
                Ok(message)
            })
            .collect::<Result<_, _>>()?;
        let mut ids = std::collections::HashSet::new();
        let mut calls = std::collections::HashSet::new();
        for message in &typed {
            if !ids.insert(&message.id) {
                return Err("local_tool_history_unsupported");
            }
            for call in message.tool_calls.iter().flatten() {
                if call.id.is_empty()
                    || call.id.len() > 128
                    || !calls.insert(&call.id)
                    || calls.len() > Self::MAX_PAIRS
                {
                    return Err("local_tool_history_unsupported");
                }
            }
        }
        Ok(typed)
    }
}

/// Logical session ancestry carried across every actor placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSessionIdentity {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    /// Host-authored immutable Child birth. Missing only on legacy routes;
    /// this is identity, not task or permission authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation: Option<ChildCreationIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildCreationIdentity {
    pub created_at: DateTime<Utc>,
    pub spawn_depth: u32,
}

/// Immutable one-shot native name ceiling. It is startup authority, not a
/// persisted profile or a grant for workspace/network/secret access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolCeiling {
    pub version: u32,
    pub child_session_id: String,
    pub parent_session_id: String,
    pub root_session_id: String,
    pub created_at: DateTime<Utc>,
    pub spawn_depth: u32,
    // Explicit null means unassigned; omission is not an authority observation.
    #[serde(deserialize_with = "deserialize_project_observation")]
    pub project_id: Option<ProjectId>,
    pub tools: Vec<String>,
}

fn deserialize_project_observation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ProjectId>, D::Error> {
    Option::<ProjectId>::deserialize(deserializer)
}

impl NativeToolCeiling {
    pub const NAMES: [&'static str; 5] = ["Bash", "Edit", "Glob", "Read", "Write"];
    pub const MAX_BYTES: usize = 16 * 1024;

    pub fn validate(&self) -> Result<(), &'static str> {
        let mut candidate = bamboo_domain::Session::new(self.child_session_id.clone(), "");
        candidate.kind = bamboo_domain::SessionKind::Child;
        candidate.parent_session_id = Some(self.parent_session_id.clone());
        candidate.root_session_id = self.root_session_id.clone();
        candidate.created_at = self.created_at;
        candidate.spawn_depth = self.spawn_depth;
        if self.version != 1
            || bamboo_domain::ActorSession::from_session(&candidate).is_err()
            || self.tools.len() > Self::NAMES.len()
            || self
                .tools
                .iter()
                .any(|name| !Self::NAMES.contains(&name.as_str()))
            || self.tools.windows(2).any(|pair| pair[0] >= pair[1])
            || serde_json::to_vec(self).map_or(true, |bytes| bytes.len() > Self::MAX_BYTES)
        {
            return Err("native_tool_ceiling_invalid");
        }
        Ok(())
    }

    pub fn matches_run(&self, run: &RunSpec) -> bool {
        self.validate().is_ok()
            && run.project_id == self.project_id
            && run.logical_session.as_ref().is_some_and(|identity| {
                identity.session_id == self.child_session_id
                    && identity.parent_session_id.as_deref()
                        == Some(self.parent_session_id.as_str())
                    && identity.root_session_id == self.root_session_id
                    && identity.creation.as_ref().is_some_and(|creation| {
                        creation.created_at == self.created_at
                            && creation.spawn_depth == self.spawn_depth
                    })
            })
    }
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

/// Closed initial-input barrier. This is not a tool grant or an approval reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialInputReleaseRequest {
    pub version: u32,
    pub nonce: String,
    pub child_id: String,
    pub parent_id: String,
    pub root_id: String,
    pub created_at: DateTime<Utc>,
    pub spawn_depth: u32,
    #[serde(deserialize_with = "deserialize_project_observation")]
    pub project_id: Option<ProjectId>,
    pub envelope_id: String,
    pub generation: u64,
    pub activation_run_id: String,
    pub execution_epoch: u64,
}
impl InitialInputReleaseRequest {
    pub fn from_run(
        run: &RunSpec,
        delivery: &SessionMessageDelivery,
        nonce: String,
    ) -> Result<Self, String> {
        let logical = run
            .logical_session
            .as_ref()
            .ok_or("initial release logical identity missing")?;
        let birth = logical
            .creation
            .as_ref()
            .ok_or("initial release birth missing")?;
        let parent = logical
            .parent_session_id
            .as_ref()
            .ok_or("initial release parent missing")?;
        if uuid::Uuid::parse_str(&nonce).is_err()
            || nonce.len() != 36
            || logical.session_id.is_empty()
            || parent.is_empty()
            || logical.root_session_id.is_empty()
            || birth.spawn_depth == 0
            || run.execution_epoch == 0
            || delivery.canonical_claim_generation == 0
            || run.activation_run_id.as_deref() != Some(delivery.activation_run_id.as_str())
            || delivery.target_session_id != logical.session_id
            || delivery.envelope.target_session_id != logical.session_id
        {
            return Err("initial release binding invalid".into());
        }
        let request = Self {
            version: 1,
            nonce,
            child_id: logical.session_id.clone(),
            parent_id: parent.clone(),
            root_id: logical.root_session_id.clone(),
            created_at: birth.created_at,
            spawn_depth: birth.spawn_depth,
            project_id: run.project_id.clone(),
            envelope_id: delivery.envelope.id.as_str().into(),
            generation: delivery.canonical_claim_generation,
            activation_run_id: delivery.activation_run_id.clone(),
            execution_epoch: run.execution_epoch,
        };
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<(), String> {
        let identity = NativeToolCeiling {
            version: self.version,
            child_session_id: self.child_id.clone(),
            parent_session_id: self.parent_id.clone(),
            root_session_id: self.root_id.clone(),
            created_at: self.created_at,
            spawn_depth: self.spawn_depth,
            project_id: self.project_id.clone(),
            tools: Vec::new(),
        };
        if identity.validate().is_err()
            || uuid::Uuid::parse_str(&self.nonce).is_err()
            || self.nonce.len() != 36
            || self.generation == 0
            || self.execution_epoch == 0
            || self.activation_run_id.is_empty()
            || self.activation_run_id.len() > 256
            || bamboo_domain::SessionMessageId::parse(self.envelope_id.clone()).is_err()
        {
            return Err("initial release binding invalid".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialInputRelease {
    pub request: InitialInputReleaseRequest,
    pub expires_at: DateTime<Utc>,
}
impl InitialInputRelease {
    pub fn permits(&self, request: &InitialInputReleaseRequest, now: DateTime<Utc>) -> bool {
        &self.request == request && request.version == 1 && now < self.expires_at
    }
}
/// Reserved discriminator: unknown or malformed values never become text steering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "initial_input_control",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum InitialInputControl {
    Request { request: InitialInputReleaseRequest },
    Release { release: InitialInputRelease },
}
impl InitialInputControl {
    pub fn decode(body: serde_json::Value) -> Result<Self, String> {
        if serde_json::to_vec(&body).map_or(true, |bytes| bytes.len() > 4096) {
            return Err("initial release control exceeds bound".into());
        }
        let control: Self =
            serde_json::from_value(body).map_err(|_| "initial release control malformed")?;
        match &control {
            Self::Request { request } => request.validate()?,
            Self::Release { release } => release.request.validate()?,
        }
        Ok(control)
    }
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
    /// A portable, admission-time Git snapshot for a fixed remote worker.
    /// Remote runs carry no host-absolute workspace path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_lease: Option<crate::environment::EnvironmentLease>,
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
    InitialInputRelease {
        release: InitialInputRelease,
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
    /// Bounded Host-owned tree page for the active logical Child. A missing
    /// page is a fail-closed denial; no Session authority travels to Worker.
    OwnedTreeReply {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        page: Option<serde_json::Value>,
    },
    /// Canonical Host result for a logical SubAgent operation in this Run.
    SubAgentReply {
        id: String,
        result: serde_json::Value,
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
    /// The Worker supplies only a page cursor. The Host binds this frame to
    /// the currently fenced logical Child; it never accepts a caller ID here.
    OwnedTreeRequest {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
    /// The Worker supplies only logical arguments and a transcript call id.
    /// Caller identity is bound to the active Host drive, not this frame.
    SubAgentRequest {
        id: String,
        tool_call_id: String,
        args: serde_json::Value,
    },
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
    fn local_tool_messages_are_closed_bounded_and_durable() {
        let raw = serde_json::to_value(bamboo_domain::Message::user("confirmed")).unwrap();
        let good = LocalToolMessages::Complete {
            version: 1,
            messages: vec![raw.clone()],
        };
        assert_eq!(
            serde_json::to_value(good.validate().unwrap()[0].clone()).unwrap(),
            raw
        );
        let event = serde_json::to_value(&good).unwrap();
        assert_eq!(ActorEventQos::classify(&event), ActorEventQos::Durable);
        for field in ["session", "permission", "version_extra"] {
            let mut changed = event.clone();
            changed[field] = serde_json::json!({});
            assert!(
                serde_json::from_value::<LocalToolMessages>(changed).is_err(),
                "{field}"
            );
        }
        for mutation in [
            "version",
            "missing",
            "unknown_message",
            "reasoning",
            "compressed",
            "duplicate",
            "bytes",
            "rows",
        ] {
            let mut changed = event.clone();
            match mutation {
                "version" => changed["version"] = 2.into(),
                "missing" => {
                    changed.as_object_mut().unwrap().remove("messages");
                }
                "unknown_message" => changed["messages"][0]["cursor"] = 1.into(),
                "reasoning" => changed["messages"][0]["reasoning"] = "private".into(),
                "compressed" => changed["messages"][0]["compression_level"] = 1.into(),
                "duplicate" => changed["messages"] = serde_json::json!([raw, raw]),
                "bytes" => {
                    changed["messages"][0]["content"] =
                        "\\\"".repeat(LocalToolMessages::MAX_BYTES).into()
                }
                "rows" => {
                    changed["messages"] =
                        serde_json::to_value(vec![raw.clone(); LocalToolMessages::MAX_MESSAGES + 1])
                            .unwrap()
                }
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<LocalToolMessages>(changed)
                    .map_or(true, |data| data.validate().is_err()),
                "{mutation}"
            );
        }
        assert!(LocalToolMessages::supports_tools(
            &["Read".into(), "Write".into()],
            false
        ));
        assert!(!LocalToolMessages::supports_tools(&["Glob".into()], true));
        assert!(!LocalToolMessages::supports_tools(&["Bash".into()], false));
    }

    #[test]
    fn initial_release_schema_is_closed_and_exact() {
        let request = InitialInputReleaseRequest {
            version: 1,
            nonce: uuid::Uuid::new_v4().to_string(),
            child_id: "child".into(),
            parent_id: "parent".into(),
            root_id: "parent".into(),
            created_at: Utc::now(),
            spawn_depth: 1,
            project_id: None,
            envelope_id: "input".into(),
            generation: 1,
            activation_run_id: "run".into(),
            execution_epoch: 7,
        };
        let release = InitialInputRelease {
            request: request.clone(),
            expires_at: Utc::now() + chrono::Duration::seconds(1),
        };
        assert!(release.permits(&request, Utc::now()));
        for change in [
            "nonce",
            "generation",
            "execution_epoch",
            "child_id",
            "created_at",
            "project_id",
        ] {
            let mut value = serde_json::to_value(&request).unwrap();
            value[change] = match change {
                "generation" | "execution_epoch" => serde_json::json!(2),
                "created_at" => serde_json::json!("2020-01-01T00:00:00Z"),
                _ => serde_json::json!("foreign"),
            };
            if let Ok(changed) = serde_json::from_value(value) {
                assert!(!release.permits(&changed, Utc::now()), "{change}");
            }
        }
        assert!(!release.permits(&request, release.expires_at));
        let mut absent_project = serde_json::to_value(&request).unwrap();
        absent_project.as_object_mut().unwrap().remove("project_id");
        assert!(serde_json::from_value::<InitialInputReleaseRequest>(absent_project).is_err());
        let bytes = serde_json::to_string(&request).unwrap();
        let duplicate = bytes.replacen("{", "{\"nonce\":\"foreign\",", 1);
        assert!(serde_json::from_str::<InitialInputReleaseRequest>(&duplicate).is_err());
        let mut value = serde_json::to_value(InitialInputControl::Release { release }).unwrap();
        value["text"] = "cannot become steer".into();
        assert!(serde_json::from_value::<InitialInputControl>(value.clone()).is_err());
        value.as_object_mut().unwrap().remove("text");
        value["initial_input_control"] = "unknown".into();
        assert!(serde_json::from_value::<InitialInputControl>(value).is_err());
    }

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
    fn logical_creation_is_atomic_and_preserves_exact_birth() {
        let legacy = serde_json::json!({"session_id":"child", "parent_session_id":"parent", "root_session_id":"root"});
        let mut identity: LogicalSessionIdentity = serde_json::from_value(legacy.clone()).unwrap();
        assert!(identity.creation.is_none());
        assert_eq!(serde_json::to_value(&identity).unwrap(), legacy);
        identity.creation = Some(ChildCreationIdentity {
            created_at: "2026-09-26T01:02:03.123456789Z".parse().unwrap(),
            spawn_depth: 3,
        });
        let wire = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            serde_json::from_value::<LogicalSessionIdentity>(wire.clone()).unwrap(),
            identity
        );
        for creation in [
            serde_json::json!({"created_at":"2026-09-26T01:02:03Z"}),
            serde_json::json!({"spawn_depth":3}),
            serde_json::json!({"created_at":"invalid", "spawn_depth":3}),
            serde_json::json!({"created_at":"2026-09-26T01:02:03Z", "spawn_depth":-1}),
            serde_json::json!({"created_at":"2026-09-26T01:02:03Z", "spawn_depth":3, "grant":true}),
        ] {
            let mut damaged = wire.clone();
            damaged["creation"] = creation;
            assert!(serde_json::from_value::<LogicalSessionIdentity>(damaged).is_err());
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
                    creation: None,
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
            environment_lease: None,
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
            environment_lease: None,
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
                creation: None,
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
