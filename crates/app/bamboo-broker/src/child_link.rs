//! `BrokerChildLink` — the parent side of running a child over the mailbox bus.
//!
//! It mirrors `bamboo_subagent::transport::ChildClient` (the direct-WS link) but
//! talks to the broker: `send(ParentFrame)` delivers to the child's mailbox and
//! `next_frame()` surfaces the child's streamed `Event`s + terminal `Outcome` as
//! `ChildFrame`s, demuxed by the run's correlation id. So the actor runner can
//! drive a local child over the in-process bus with the same calls it used for a
//! direct WS connection — one path for local and remote (P1.3 of the actor+
//! mailbox unification).

use bamboo_subagent::{
    AgentRef, ChildFrame, ChildLink, ChildOutcome, DurableChildDeliveryReceipt, InboxKind,
    InboxMessage, MsgId, ParentFrame, TerminalStatus, TransportError, TransportResult,
};
use chrono::Utc;
use std::collections::VecDeque;

use crate::client::{BrokerClient, BrokerStreamEvent};
use crate::error::{BrokerError, BrokerResult};
use crate::proto::{FencedRunEnvelope, WorkerHostObservation};

/// A parent→child link over the broker, addressing the child by its mailbox id.
pub struct BrokerChildLink {
    client: BrokerClient,
    /// The child's mailbox id (where `Run`/`Cancel` are delivered).
    child: String,
    /// This parent's ref (the `from` on outbound messages; replies route here).
    me: AgentRef,
    broker_identity: String,
    /// The current run's correlation id (the delivered `Run` message id). Set on
    /// `send(Run)`; `next_frame` only surfaces frames correlated to it.
    run_id: Option<MsgId>,
    /// True once a terminal frame has been surfaced for the current run.
    done: bool,
    selected_worker: Option<AgentRef>,
    selected_host_observation: Option<WorkerHostObservation>,
    expected_run: Option<bamboo_subagent::RunSpec>,
    require_environment_lease: bool,
    /// Recovery-only links may ACK Host-proven receipts but never dispatch Run.
    recovery_only: bool,
    /// Exact durable mailbox receipts accepted for this Run. The Host alone
    /// decides when its logical Session checkpoint permits their ACK.
    pending_durable: VecDeque<MsgId>,
    pending_terminal: Option<MsgId>,
    observed_terminal_status: Option<TerminalStatus>,
    accepted_terminal_status: Option<TerminalStatus>,
}

const MAX_PENDING_DURABLE_FRAMES: usize = 4096;

impl BrokerChildLink {
    /// Connect to the broker as `parent` and subscribe, ready to drive `child`.
    pub async fn connect(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
        child: impl Into<String>,
    ) -> BrokerResult<Self> {
        let mut client = BrokerClient::connect_actor(endpoint, parent.clone(), token).await?;
        client.subscribe().await?;
        let broker_identity = client.durable_broker_identity().await?;
        Ok(Self {
            client,
            child: child.into(),
            me: parent,
            broker_identity,
            run_id: None,
            done: false,
            selected_worker: None,
            selected_host_observation: None,
            expected_run: None,
            require_environment_lease: false,
            recovery_only: false,
            pending_durable: VecDeque::new(),
            pending_terminal: None,
            observed_terminal_status: None,
            accepted_terminal_status: None,
        })
    }

    /// Subscribe to the Host mailbox solely to replay-ACK its checkpointed
    /// Event/Outcome receipts. This never requires the old worker to be online
    /// and cannot be used to dispatch a new Run.
    pub async fn connect_receipt_recovery(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
    ) -> BrokerResult<Self> {
        // The legacy broker accepts an authenticated ACK without a subscription.
        // Avoid replacing an active parent subscription during periodic repair.
        let mut client = BrokerClient::connect_actor(endpoint, parent.clone(), token).await?;
        let broker_identity = client.durable_broker_identity().await?;
        Ok(Self {
            client,
            child: String::new(),
            me: parent,
            broker_identity,
            run_id: None,
            done: false,
            selected_worker: None,
            selected_host_observation: None,
            expected_run: None,
            require_environment_lease: false,
            recovery_only: true,
            pending_durable: VecDeque::new(),
            pending_terminal: None,
            observed_terminal_status: None,
            accepted_terminal_status: None,
        })
    }

    /// Scoped TLS counterpart of [`Self::connect_receipt_recovery`]. The
    /// authenticated peer policy, not a worker claim, binds the parent mailbox.
    pub async fn connect_receipt_recovery_with_tls(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
        tls: rustls::ClientConfig,
    ) -> BrokerResult<Self> {
        let mut client =
            BrokerClient::connect_actor_with_tls(endpoint, parent.clone(), token, Some(tls))
                .await?;
        let broker_identity = client.durable_broker_identity().await?;
        Ok(Self {
            client,
            child: String::new(),
            me: parent,
            broker_identity,
            run_id: None,
            done: false,
            selected_worker: None,
            selected_host_observation: None,
            expected_run: None,
            require_environment_lease: false,
            recovery_only: true,
            pending_durable: VecDeque::new(),
            pending_terminal: None,
            observed_terminal_status: None,
            accepted_terminal_status: None,
        })
    }

    pub fn parent_mailbox(&self) -> &str {
        &self.me.session_id
    }

    /// Explicit scoped peer route; the credential belongs only to this client.
    pub async fn connect_strict_with_tls(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
        worker: AgentRef,
        tls: rustls::ClientConfig,
    ) -> BrokerResult<Self> {
        let mut client =
            BrokerClient::connect_actor_with_tls(endpoint, parent.clone(), token, Some(tls))
                .await?;
        client.subscribe().await?;
        let broker_identity = client.durable_broker_identity().await?;
        // This connection's ordered control reply confirms subscription admission
        // and the selected role's current connection presence, not Host ownership.
        let role = worker.role.as_deref().ok_or_else(strict_link_error)?;
        if !client
            .list_connected(role)
            .await?
            .contains(&worker.session_id)
        {
            return Err(strict_link_error());
        }
        Ok(Self {
            client,
            child: worker.session_id.clone(),
            me: parent,
            broker_identity,
            run_id: None,
            done: false,
            selected_worker: Some(worker),
            selected_host_observation: None,
            expected_run: None,
            require_environment_lease: false,
            recovery_only: false,
            pending_durable: VecDeque::new(),
            pending_terminal: None,
            observed_terminal_status: None,
            accepted_terminal_status: None,
        })
    }

    /// Fixed Remote only: require an authenticated, current Worker claim before
    /// dispatch. A versioned inbox kind also rejects old Worker binaries if
    /// the subscriber changes after this observation.
    pub async fn connect_strict_with_tls_environment_lease(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
        worker: AgentRef,
        tls: rustls::ClientConfig,
    ) -> BrokerResult<Self> {
        let mut link =
            Self::connect_strict_with_tls(endpoint, parent, token, worker.clone(), tls).await?;
        let role = worker.role.as_deref().ok_or_else(strict_link_error)?;
        let observation = link.client.observe_host(&worker.session_id, role).await?;
        let Some(observation) = observation.filter(|current| {
            current.environment_lease_v1
                && current.mailbox == worker.session_id
                && current.role.as_deref() == Some(role)
        }) else {
            return Err(BrokerError::Protocol(
                "remote_environment_lease_unsupported".into(),
            ));
        };
        link.selected_host_observation = Some(observation);
        link.require_environment_lease = true;
        Ok(link)
    }

    fn validate_selected_batch(
        &self,
        batch: &bamboo_subagent::ActorEventBatch,
    ) -> BrokerResult<()> {
        if let Some(worker) = &self.selected_worker {
            let run = self.expected_run.as_ref().ok_or_else(strict_link_error)?;
            if batch.validate().is_err()
                || batch.logical_session != run.logical_session
                || batch.activation_id != run.activation_run_id
                || batch.execution_epoch != run.execution_epoch
                || batch.source_actor_id.as_deref() != Some(worker.session_id.as_str())
            {
                return Err(strict_link_error());
            }
        }
        Ok(())
    }

    fn msg(
        &self,
        kind: InboxKind,
        body: serde_json::Value,
        correlation: Option<MsgId>,
    ) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: self.me.clone(),
            kind,
            body,
            created_at: Utc::now(),
            correlation_id: correlation,
        }
    }

    fn hold_durable(&mut self, id: MsgId, terminal: bool) -> BrokerResult<()> {
        if self.pending_durable.len() >= MAX_PENDING_DURABLE_FRAMES {
            return Err(BrokerError::Protocol(
                "too many unconfirmed child durable frames".into(),
            ));
        }
        if terminal {
            self.pending_terminal = Some(id.clone());
        }
        self.pending_durable.push_back(id);
        Ok(())
    }

    /// ACK only after the Host has committed the Child's logical transcript
    /// and final status. Exact MsgIds are retained across a failed send; a
    /// reconnect may replay them and the Host must revalidate its checkpoint.
    pub async fn acknowledge_durable_frames(&mut self) -> BrokerResult<()> {
        if self.pending_terminal.is_none() || self.accepted_terminal_status.is_none() {
            return Err(BrokerError::Protocol(
                "child terminal receipt is not ready for durable ACK".into(),
            ));
        }
        while let Some(id) = self.pending_durable.front().cloned() {
            self.client.ack_confirmed(id.clone()).await?;
            self.pending_durable.pop_front();
            if self.pending_terminal.as_ref() == Some(&id) {
                self.pending_terminal = None;
                self.observed_terminal_status = None;
                self.accepted_terminal_status = None;
            }
        }
        Ok(())
    }

    /// Send a parent→child frame, mirroring `ChildClient::send`.
    pub async fn send(&mut self, frame: ParentFrame) -> BrokerResult<()> {
        if self.recovery_only {
            return Err(BrokerError::Protocol(
                "broker receipt recovery link cannot dispatch work".into(),
            ));
        }
        match frame {
            ParentFrame::Run(spec) => {
                if self.require_environment_lease
                    && !spec.permission_policy.as_ref().is_some_and(|policy| {
                        policy.workspace_path.is_none()
                            && policy.environment_lease.as_ref().is_some_and(|lease| {
                                lease.actor_id
                                    == spec
                                        .logical_session
                                        .as_ref()
                                        .map(|id| id.session_id.as_str())
                                        .unwrap_or("")
                                    && lease.activation_run_id
                                        == spec.activation_run_id.as_deref().unwrap_or("")
                                    && lease.execution_epoch == spec.execution_epoch
                            })
                    })
                {
                    return Err(BrokerError::Protocol(
                        "remote_environment_lease_missing".into(),
                    ));
                }
                if self.selected_worker.is_some() {
                    if spec
                        .logical_session
                        .as_ref()
                        .and_then(|id| id.creation.as_ref())
                        .is_none()
                        || spec.activation_run_id.as_deref().is_none_or(str::is_empty)
                        || spec.execution_epoch == 0
                    {
                        return Err(strict_link_error());
                    }
                    self.expected_run = Some(bamboo_subagent::RunSpec {
                        assignment: String::new(),
                        logical_session: spec.logical_session.clone(),
                        project_id: spec.project_id.clone(),
                        reasoning_effort: None,
                        permission_policy: None,
                        messages: Vec::new(),
                        activation_run_id: spec.activation_run_id.clone(),
                        execution_epoch: spec.execution_epoch,
                        initial_session_messages: Vec::new(),
                        secrets: Default::default(),
                    });
                }
                let body = if self.require_environment_lease {
                    let observed = self
                        .selected_host_observation
                        .as_ref()
                        .ok_or_else(strict_link_error)?;
                    let envelope = FencedRunEnvelope::for_observation(spec, observed)
                        .ok_or_else(strict_link_error)?;
                    serde_json::to_value(envelope)
                } else {
                    serde_json::to_value(spec)
                }
                .map_err(|e| BrokerError::Transport(format!("encode RunSpec: {e}")))?;
                let kind = if self.require_environment_lease {
                    InboxKind::FencedRun
                } else {
                    InboxKind::Run
                };
                let m = self.msg(kind, body, None);
                self.run_id = Some(m.id.clone());
                self.done = false;
                self.observed_terminal_status = None;
                self.accepted_terminal_status = None;
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::Cancel => {
                if let Some(rid) = self.run_id.clone() {
                    self.client.cancel(&self.child, &rid).await?;
                }
            }
            // In-band steering: deliver to the child's mailbox correlated to the
            // run, so the worker routes it to that run's steer inbox.
            ParentFrame::Message { text } => {
                let m = self.msg(
                    InboxKind::Steer,
                    serde_json::json!({ "text": text }),
                    self.run_id.clone(),
                );
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::InitialInputRelease { release } => {
                let body =
                    serde_json::to_value(bamboo_subagent::proto::InitialInputControl::Release {
                        release,
                    })
                    .map_err(|e| BrokerError::Transport(format!("encode initial release: {e}")))?;
                let m = self.msg(InboxKind::Steer, body, self.run_id.clone());
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::SessionMessage { delivery } => {
                let body = serde_json::to_value(delivery).map_err(|e| {
                    BrokerError::Transport(format!("encode SessionMessageDelivery: {e}"))
                })?;
                let m = self.msg(InboxKind::Steer, body, self.run_id.clone());
                self.client.deliver(&self.child, m).await?;
            }
            // Approval decision: the worker routes it to the waiting tool call by
            // the approval-request `id` carried in the body.
            ParentFrame::ApprovalReply { id, approved } => {
                let m = self.msg(
                    InboxKind::ApprovalReply,
                    serde_json::json!({ "id": id, "approved": approved }),
                    self.run_id.clone(),
                );
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::OwnedTreeReply { id, page } => {
                if id.is_empty()
                    || id.len() > 128
                    || serde_json::to_vec(&page).map_or(true, |bytes| bytes.len() > 8192)
                {
                    return Err(BrokerError::Protocol("invalid owned tree reply".into()));
                }
                let m = self.msg(
                    InboxKind::OwnedTreeReply,
                    serde_json::json!({ "id": id, "page": page }),
                    self.run_id.clone(),
                );
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::SubAgentReply { id, result } => {
                if id.is_empty()
                    || id.len() > 128
                    || serde_json::to_vec(&result).map_or(true, |bytes| bytes.len() > 16 * 1024)
                {
                    return Err(BrokerError::Protocol(
                        "invalid canonical SubAgent reply".into(),
                    ));
                }
                let m = self.msg(
                    InboxKind::SubAgentReply,
                    serde_json::json!({ "id": id, "result": result }),
                    self.run_id.clone(),
                );
                self.client.deliver(&self.child, m).await?;
            }
        }
        Ok(())
    }

    /// Receive the next child→parent frame for the current run, mirroring
    /// `ChildClient::next_frame`. Returns `None` once the run is terminal or the
    /// connection closes. Frames from other runs (or unsolicited) are skipped.
    pub async fn next_frame(&mut self) -> BrokerResult<Option<ChildFrame>> {
        if self.done {
            return Ok(None);
        }
        loop {
            let msg = match self.client.next_message_or_event_batch().await {
                BrokerStreamEvent::EventBatch(Some(delivery)) => {
                    if self.run_id.as_ref() != Some(&delivery.correlation_id) {
                        continue;
                    }
                    self.validate_selected_batch(&delivery.batch)?;
                    delivery.batch.validate().map_err(|error| {
                        BrokerError::Transport(format!("invalid actor event batch: {error}"))
                    })?;
                    return Ok(Some(ChildFrame::EventBatch {
                        batch: delivery.batch,
                    }));
                }
                BrokerStreamEvent::EventBatch(None) => continue,
                BrokerStreamEvent::Message(Some(message)) => message,
                BrokerStreamEvent::Message(None) => return Ok(None),
            };
            let id = msg.id.clone();
            // Never delete a different Run's durable message merely because it
            // arrived on this subscription. Its Host checkpoint is unknown.
            if self.run_id.is_none() || msg.correlation_id != self.run_id {
                continue;
            }
            if self
                .selected_worker
                .as_ref()
                .is_some_and(|worker| &msg.from != worker)
            {
                return Err(strict_link_error());
            }
            let frame = match msg.kind {
                InboxKind::Event => {
                    match serde_json::from_value::<bamboo_subagent::ActorEventBatch>(
                        msg.body.clone(),
                    ) {
                        Ok(batch) => {
                            self.validate_selected_batch(&batch)?;
                            Some(ChildFrame::EventBatch { batch })
                        }
                        Err(_) if self.selected_worker.is_some() => return Err(strict_link_error()),
                        Err(_) => Some(ChildFrame::Event { event: msg.body }),
                    }
                }
                InboxKind::SessionMessageAdmitted
                    if msg.body.get("initial_input_control").is_some() =>
                {
                    match bamboo_subagent::proto::InitialInputControl::decode(msg.body).map_err(
                        |e| BrokerError::Transport(format!("decode initial control: {e}")),
                    )? {
                        bamboo_subagent::proto::InitialInputControl::Request { request } => {
                            Some(ChildFrame::Event {
                                event: serde_json::to_value(
                                    bamboo_subagent::proto::InitialInputControl::Request {
                                        request,
                                    },
                                )
                                .map_err(|e| {
                                    BrokerError::Transport(format!("encode initial control: {e}"))
                                })?,
                            })
                        }
                        _ => {
                            return Err(BrokerError::Transport(
                                "unexpected initial release direction".into(),
                            ))
                        }
                    }
                }
                InboxKind::SessionMessageAdmitted => {
                    let confirmation = serde_json::from_value(msg.body).map_err(|e| {
                        BrokerError::Transport(format!(
                            "decode SessionMessageAdmissionConfirmation: {e}"
                        ))
                    })?;
                    Some(ChildFrame::SessionMessageAdmitted { confirmation })
                }
                InboxKind::ApprovalRequest => {
                    // body = {"id": "...", "request": {...}}: the worker proxied a
                    // gated-tool approval up. Surface it for the host to decide.
                    let id = msg
                        .body
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let body = msg
                        .body
                        .get("request")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    Some(ChildFrame::ApprovalRequest { id, body })
                }
                InboxKind::OwnedTreeRequest => {
                    let object = msg.body.as_object().ok_or_else(|| {
                        BrokerError::Protocol("invalid owned tree request".into())
                    })?;
                    if object.keys().any(|key| key != "id" && key != "cursor") {
                        return Err(BrokerError::Protocol("invalid owned tree request".into()));
                    }
                    let id = object
                        .get("id")
                        .and_then(|value| value.as_str())
                        .filter(|id| !id.is_empty() && id.len() <= 128)
                        .ok_or_else(|| BrokerError::Protocol("invalid owned tree request".into()))?
                        .to_owned();
                    let cursor = match object.get("cursor") {
                        None | Some(serde_json::Value::Null) => None,
                        Some(serde_json::Value::String(value))
                            if !value.is_empty() && value.len() <= 128 =>
                        {
                            Some(value.clone())
                        }
                        _ => {
                            return Err(BrokerError::Protocol("invalid owned tree request".into()))
                        }
                    };
                    Some(ChildFrame::OwnedTreeRequest { id, cursor })
                }
                InboxKind::SubAgentRequest => {
                    let object = msg.body.as_object().ok_or_else(|| {
                        BrokerError::Protocol("invalid canonical SubAgent request".into())
                    })?;
                    if object
                        .keys()
                        .any(|key| key != "id" && key != "tool_call_id" && key != "args")
                    {
                        return Err(BrokerError::Protocol(
                            "invalid canonical SubAgent request".into(),
                        ));
                    }
                    let text = |key: &str| {
                        object
                            .get(key)
                            .and_then(serde_json::Value::as_str)
                            .filter(|value| !value.is_empty() && value.len() <= 128)
                            .map(str::to_owned)
                            .ok_or_else(|| {
                                BrokerError::Protocol("invalid canonical SubAgent request".into())
                            })
                    };
                    let id = text("id")?;
                    let tool_call_id = text("tool_call_id")?;
                    let args = object
                        .get("args")
                        .filter(|value| value.is_object())
                        .filter(|value| {
                            serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 64 * 1024)
                        })
                        .cloned()
                        .ok_or_else(|| {
                            BrokerError::Protocol("invalid canonical SubAgent request".into())
                        })?;
                    Some(ChildFrame::SubAgentRequest {
                        id,
                        tool_call_id,
                        args,
                    })
                }
                InboxKind::Outcome => {
                    let oc: ChildOutcome = serde_json::from_value(msg.body)
                        .map_err(|e| BrokerError::Transport(format!("decode ChildOutcome: {e}")))?;
                    self.done = true;
                    self.observed_terminal_status = Some(oc.status);
                    Some(ChildFrame::Terminal {
                        status: oc.status,
                        result: oc.result,
                        error: oc.error,
                        transcript: oc.transcript,
                    })
                }
                _ => None,
            };
            if matches!(msg.kind, InboxKind::Event | InboxKind::Outcome) {
                self.hold_durable(id, msg.kind == InboxKind::Outcome)?;
            } else {
                self.client.ack(id).await?;
            }
            if let Some(f) = frame {
                return Ok(Some(f));
            }
        }
    }
}

fn strict_link_error() -> BrokerError {
    BrokerError::Transport("remote broker frame rejected".into())
}

/// Drive a child over the bus with the SAME interface as a direct-WS
/// `ChildClient`, so the actor runner's `drive()` is transport-agnostic.
#[async_trait::async_trait]
impl ChildLink for BrokerChildLink {
    async fn send(&mut self, frame: ParentFrame) -> TransportResult<()> {
        BrokerChildLink::send(self, frame)
            .await
            .map_err(|e| TransportError::Protocol(format!("broker link send: {e}")))
    }
    async fn next_frame(&mut self) -> TransportResult<Option<ChildFrame>> {
        BrokerChildLink::next_frame(self)
            .await
            .map_err(|e| TransportError::Protocol(format!("broker link recv: {e}")))
    }

    fn accept_durable_terminal(&mut self, status: TerminalStatus) {
        if self.pending_terminal.is_some() && self.observed_terminal_status == Some(status) {
            self.accepted_terminal_status = Some(status);
        }
    }

    fn has_pending_durable_terminal(&self) -> bool {
        self.pending_terminal.is_some() && self.accepted_terminal_status.is_some()
    }

    fn durable_delivery_receipt(&self) -> Option<DurableChildDeliveryReceipt> {
        self.pending_terminal.as_ref()?;
        let terminal_status = self.accepted_terminal_status?;
        Some(DurableChildDeliveryReceipt {
            broker_identity: self.broker_identity.clone(),
            parent_mailbox: self.me.session_id.clone(),
            correlation_id: self.run_id.as_ref()?.as_str().to_owned(),
            message_ids: self
                .pending_durable
                .iter()
                .map(|id| id.as_str().to_owned())
                .collect(),
            terminal_status,
        })
    }

    fn broker_parent_mailbox(&self) -> Option<&str> {
        Some(&self.me.session_id)
    }

    async fn acknowledge_durable_frames(&mut self) -> TransportResult<()> {
        BrokerChildLink::acknowledge_durable_frames(self)
            .await
            .map_err(|e| TransportError::Protocol(format!("broker link ack: {e}")))
    }

    async fn acknowledge_recovered_durable_frames(
        &mut self,
        broker_identity: &str,
        message_ids: &[String],
    ) -> TransportResult<()> {
        if broker_identity != self.broker_identity {
            return Err(TransportError::Protocol(
                "broker mailbox identity changed before recovered ACK".into(),
            ));
        }
        if message_ids.is_empty() || message_ids.len() > MAX_PENDING_DURABLE_FRAMES {
            return Err(TransportError::Protocol(
                "invalid recovered durable ACK set".into(),
            ));
        }
        for id in message_ids {
            if id.is_empty() || id.len() > 128 {
                return Err(TransportError::Protocol(
                    "invalid recovered durable ACK id".into(),
                ));
            }
            self.client
                .ack_confirmed(MsgId(id.clone()))
                .await
                .map_err(|error| {
                    TransportError::Protocol(format!("broker recovered ack: {error}"))
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::serve::serve_executor;
    use crate::server::BrokerServer;
    use bamboo_subagent::{EchoExecutor, RunSpec};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn durable_frames_replay_until_explicit_host_confirmation() {
        let (endpoint, dir) = start_broker().await;
        let mut link = connect_parent(&endpoint).await;
        let run_id = MsgId::new();
        link.run_id = Some(run_id.clone());
        let worker_ref = AgentRef {
            session_id: "child".into(),
            role: None,
        };
        let mut worker = BrokerClient::connect(&endpoint, worker_ref.clone(), "t")
            .await
            .unwrap();
        let event_id = MsgId::new();
        let outcome_id = MsgId::new();
        for (id, kind, body) in [
            (
                event_id,
                InboxKind::Event,
                serde_json::json!({"type":"ready"}),
            ),
            (
                outcome_id,
                InboxKind::Outcome,
                serde_json::to_value(ChildOutcome::completed("done")).unwrap(),
            ),
        ] {
            worker
                .deliver(
                    "parent",
                    InboxMessage {
                        id,
                        from: worker_ref.clone(),
                        kind,
                        body,
                        created_at: Utc::now(),
                        correlation_id: Some(run_id.clone()),
                    },
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            link.next_frame().await.unwrap(),
            Some(ChildFrame::Event { .. })
        ));
        assert!(matches!(
            link.next_frame().await.unwrap(),
            Some(ChildFrame::Terminal { .. })
        ));
        let mailbox = bamboo_subagent::Mailbox::at(dir.path().join("mailboxes/parent"));
        assert_eq!(mailbox.pending_count().await.unwrap(), 2);
        drop(link);

        // A successor's different correlation must not delete either old
        // receipt. It may confirm only its own new Outcome after its own Host
        // save; the previous Run still needs separate checkpoint proof.
        let mut successor = connect_parent(&endpoint).await;
        let successor_run = MsgId::new();
        successor.run_id = Some(successor_run.clone());
        worker
            .deliver(
                "parent",
                InboxMessage {
                    id: MsgId::new(),
                    from: worker_ref.clone(),
                    kind: InboxKind::Outcome,
                    body: serde_json::to_value(ChildOutcome::completed("successor")).unwrap(),
                    created_at: Utc::now(),
                    correlation_id: Some(successor_run),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            successor.next_frame().await.unwrap(),
            Some(ChildFrame::Terminal { .. })
        ));
        assert!(!successor.has_pending_durable_terminal());
        successor.accept_durable_terminal(TerminalStatus::Completed);
        successor.acknowledge_durable_frames().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while mailbox.pending_count().await.unwrap() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(successor);

        // The same Run's unconfirmed frames are re-pushed on reconnect. A
        // failed ACK does not remove a token or turn a receipt into success.
        let mut replay = connect_parent(&endpoint).await;
        replay.run_id = Some(run_id);
        assert!(matches!(
            replay.next_frame().await.unwrap(),
            Some(ChildFrame::Event { .. })
        ));
        assert!(matches!(
            replay.next_frame().await.unwrap(),
            Some(ChildFrame::Terminal { .. })
        ));
        replay.accept_durable_terminal(TerminalStatus::Completed);
        let fail_ack = replay.client.fail_next_ack_handle();
        fail_ack.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(replay.acknowledge_durable_frames().await.is_err());
        assert_eq!(mailbox.pending_count().await.unwrap(), 2);
        replay.acknowledge_durable_frames().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while mailbox.pending_count().await.unwrap() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn successor_rehydrates_only_the_same_broker_maildir_receipt() {
        let (endpoint, dir) = start_broker().await;
        let mut first = connect_parent(&endpoint).await;
        let old_run = MsgId::new();
        first.run_id = Some(old_run.clone());
        let worker_ref = AgentRef {
            session_id: "child".into(),
            role: None,
        };
        let mut worker = BrokerClient::connect(&endpoint, worker_ref.clone(), "t")
            .await
            .unwrap();
        let outcome_id = MsgId::new();
        worker
            .deliver(
                "parent",
                InboxMessage {
                    id: outcome_id.clone(),
                    from: worker_ref,
                    kind: InboxKind::Outcome,
                    body: serde_json::to_value(ChildOutcome::completed("done")).unwrap(),
                    created_at: Utc::now(),
                    correlation_id: Some(old_run),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            first.next_frame().await.unwrap(),
            Some(ChildFrame::Terminal { .. })
        ));
        first.accept_durable_terminal(TerminalStatus::Completed);
        let receipt = first.durable_delivery_receipt().unwrap();
        drop(first);

        let mut successor = connect_parent(&endpoint).await;
        successor.run_id = Some(MsgId::new());
        let wrong_root = uuid::Uuid::new_v4().to_string();
        assert!(successor
            .acknowledge_recovered_durable_frames(&wrong_root, &receipt.message_ids)
            .await
            .is_err());
        let mailbox = bamboo_subagent::Mailbox::at(dir.path().join("mailboxes/parent"));
        assert_eq!(mailbox.pending_count().await.unwrap(), 1);
        successor
            .acknowledge_recovered_durable_frames(&receipt.broker_identity, &receipt.message_ids)
            .await
            .unwrap();
        assert_eq!(mailbox.pending_count().await.unwrap(), 0);
        // A lost Host cleanup response may retry the same exact MsgId.
        successor
            .acknowledge_recovered_durable_frames(&receipt.broker_identity, &receipt.message_ids)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_worker_terminals_wait_for_explicit_host_acceptance() {
        for (outcome, status) in [
            (ChildOutcome::error("worker failed"), TerminalStatus::Error),
            (ChildOutcome::cancelled(), TerminalStatus::Cancelled),
        ] {
            let (endpoint, dir) = start_broker().await;
            let mut link = connect_parent(&endpoint).await;
            let run_id = MsgId::new();
            link.run_id = Some(run_id.clone());
            let worker_ref = AgentRef {
                session_id: "child".into(),
                role: None,
            };
            let mut worker = BrokerClient::connect(&endpoint, worker_ref.clone(), "t")
                .await
                .unwrap();
            worker
                .deliver(
                    "parent",
                    InboxMessage {
                        id: MsgId::new(),
                        from: worker_ref,
                        kind: InboxKind::Outcome,
                        body: serde_json::to_value(outcome).unwrap(),
                        created_at: Utc::now(),
                        correlation_id: Some(run_id),
                    },
                )
                .await
                .unwrap();
            assert!(matches!(
                link.next_frame().await.unwrap(),
                Some(ChildFrame::Terminal { status: received, .. }) if received == status
            ));
            let mailbox = bamboo_subagent::Mailbox::at(dir.path().join("mailboxes/parent"));
            assert_eq!(mailbox.pending_count().await.unwrap(), 1);
            assert!(link.durable_delivery_receipt().is_none());
            link.accept_durable_terminal(TerminalStatus::Completed);
            assert!(link.durable_delivery_receipt().is_none());
            link.accept_durable_terminal(status);
            assert_eq!(
                link.durable_delivery_receipt().unwrap().terminal_status,
                status
            );
            link.acknowledge_durable_frames().await.unwrap();
            assert_eq!(mailbox.pending_count().await.unwrap(), 0);
        }
    }

    /// Full round trip: a parent drives a child over the bus via `BrokerChildLink`
    /// and the P1.3b worker streams `Event`s then a `Terminal` — proving local
    /// child execution works end-to-end over the mailbox bus.
    #[tokio::test]
    async fn drives_a_child_run_over_the_bus() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "child".into(),
                    role: None,
                },
                "t",
                Arc::new(EchoExecutor),
            )
            .await;
        });

        let mut link = BrokerChildLink::connect(
            &endpoint,
            AgentRef {
                session_id: "parent".into(),
                role: None,
            },
            "t",
            "child",
        )
        .await
        .unwrap();

        link.send(ParentFrame::Run(RunSpec {
            assignment: "hello world".into(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: None,
            execution_epoch: 1,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .unwrap();

        let mut events = 0usize;
        let mut saw_batch = false;
        let mut terminal = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
                .await
                .expect("a frame arrives")
                .expect("link ok")
            {
                Some(ChildFrame::Event { .. }) => events += 1,
                Some(ChildFrame::EventBatch { batch }) => {
                    saw_batch = true;
                    events += batch.events.len();
                }
                Some(ChildFrame::Terminal { status, result, .. }) => {
                    terminal = Some((status, result));
                    break;
                }
                Some(_) => {}
                None => break,
            }
        }

        assert!(events >= 1, "expected streamed events, got {events}");
        assert!(
            saw_batch,
            "a non-zero execution epoch must select event batches"
        );
        let (status, result) = terminal.expect("a terminal frame");
        assert_eq!(status, bamboo_subagent::TerminalStatus::Completed);
        assert_eq!(result.as_deref(), Some("echo: hello world"));

        // After the terminal, the link is drained.
        assert!(link.next_frame().await.unwrap().is_none());
    }

    async fn start_broker() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    fn spawn_worker(endpoint: &str, exec: Arc<dyn bamboo_subagent::ChildExecutor>) {
        let ep = endpoint.to_string();
        tokio::spawn(async move {
            let _ = serve_executor(
                &ep,
                AgentRef {
                    session_id: "child".into(),
                    role: None,
                },
                "t",
                exec,
            )
            .await;
        });
    }

    async fn connect_parent(endpoint: &str) -> BrokerChildLink {
        BrokerChildLink::connect(
            endpoint,
            AgentRef {
                session_id: "parent".into(),
                role: None,
            },
            "t",
            "child",
        )
        .await
        .expect("connect link")
    }

    /// An executor that emits a `ready` event then returns the FIRST steer it
    /// receives — proving an in-band steer reaches a running bus child.
    struct SteerEcho;
    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for SteerEcho {
        async fn run(
            &self,
            _spec: RunSpec,
            events: bamboo_subagent::EventSink,
            mut steer: bamboo_subagent::SteerInbox,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            events.emit(serde_json::json!({ "type": "ready" })).await;
            let s = steer.recv().await.unwrap_or_default();
            bamboo_subagent::ChildOutcome::completed(format!("steered: {s}"))
        }
    }

    struct TypedSteer;
    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for TypedSteer {
        async fn run(
            &self,
            spec: RunSpec,
            events: bamboo_subagent::EventSink,
            mut steer: bamboo_subagent::SteerInbox,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            assert_eq!(
                spec.logical_session,
                Some(bamboo_subagent::LogicalSessionIdentity {
                    creation: None,
                    session_id: "logical-child".to_string(),
                    parent_session_id: Some("logical-parent".to_string()),
                    root_session_id: "logical-root".to_string(),
                })
            );
            events.emit(serde_json::json!({ "type": "ready" })).await;
            let bamboo_subagent::SteerMessage::SessionMessage(delivery) =
                steer.recv_message().await.expect("typed delivery")
            else {
                panic!("expected typed delivery");
            };
            events
                .confirm_session_message(bamboo_subagent::SessionMessageAdmissionConfirmation {
                    target_session_id: delivery.target_session_id,
                    envelope_id: delivery.envelope.id.as_str().to_string(),
                    canonical_claim_generation: delivery.canonical_claim_generation,
                    activation_run_id: delivery.activation_run_id,
                })
                .await;
            bamboo_subagent::ChildOutcome::completed("confirmed")
        }
    }

    /// An executor that proxies one gated-tool approval to the host and reports
    /// the decision — proving the approval round-trip works over the bus.
    struct AskApproval;
    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for AskApproval {
        async fn run(
            &self,
            _spec: RunSpec,
            events: bamboo_subagent::EventSink,
            _steer: bamboo_subagent::SteerInbox,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            let approved = match events.host().cloned() {
                Some(host) => host
                    .approval_call(
                        serde_json::json!({ "tool_name": "Bash", "resource": "rm -rf /" }),
                    )
                    .await
                    .ok()
                    .and_then(|v| v.get("approved").and_then(|b| b.as_bool()))
                    .unwrap_or(false),
                None => false,
            };
            bamboo_subagent::ChildOutcome::completed(
                if approved { "approved" } else { "denied" }.to_string(),
            )
        }
    }

    #[tokio::test]
    async fn carries_an_in_band_steer_over_the_bus() {
        let (endpoint, _dir) = start_broker().await;
        spawn_worker(&endpoint, Arc::new(SteerEcho));
        let mut link = connect_parent(&endpoint).await;

        link.send(ParentFrame::Run(RunSpec {
            assignment: "go".into(),
            logical_session: Some(bamboo_subagent::LogicalSessionIdentity {
                creation: None,
                session_id: "logical-child".to_string(),
                parent_session_id: Some("logical-parent".to_string()),
                root_session_id: "logical-root".to_string(),
            }),
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .unwrap();

        // Wait for the worker's `ready` event (run started ⇒ steer inbox armed),
        // THEN steer it. (Single match, not a loop — the first frame is expected
        // to be the ready Event; anything else is a test failure.)
        match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
            .await
            .expect("a frame")
            .expect("ok")
        {
            Some(ChildFrame::Event { .. } | ChildFrame::EventBatch { .. }) => {}
            other => panic!("expected ready event first, got {other:?}"),
        }
        link.send(ParentFrame::Message {
            text: "turn-left".into(),
        })
        .await
        .unwrap();

        let result = loop {
            match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
                .await
                .expect("a frame")
                .expect("ok")
            {
                Some(ChildFrame::Terminal { result, .. }) => break result,
                _ => continue,
            }
        };
        assert_eq!(result.as_deref(), Some("steered: turn-left"));
    }

    #[tokio::test]
    async fn carries_typed_session_message_confirmation_over_the_bus() {
        let (endpoint, _dir) = start_broker().await;
        spawn_worker(&endpoint, Arc::new(TypedSteer));
        let mut link = connect_parent(&endpoint).await;
        link.send(ParentFrame::Run(RunSpec {
            assignment: "go".into(),
            logical_session: Some(bamboo_subagent::LogicalSessionIdentity {
                creation: None,
                session_id: "logical-child".to_string(),
                parent_session_id: Some("logical-parent".to_string()),
                root_session_id: "logical-root".to_string(),
            }),
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), link.next_frame())
                .await
                .unwrap()
                .unwrap(),
            Some(ChildFrame::Event { .. } | ChildFrame::EventBatch { .. })
        ));
        let mut envelope =
            bamboo_domain::SessionMessageEnvelope::user_input("logical-child", "typed");
        envelope.id = bamboo_domain::SessionMessageId::parse("broker-typed-id").unwrap();
        link.send(ParentFrame::SessionMessage {
            delivery: bamboo_subagent::SessionMessageDelivery {
                target_session_id: "logical-child".to_string(),
                envelope,
                canonical_claim_generation: 9,
                activation_run_id: "run-broker-9".to_string(),
                activation_policy: bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            },
        })
        .await
        .unwrap();

        match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
            .await
            .unwrap()
            .unwrap()
        {
            Some(ChildFrame::SessionMessageAdmitted { confirmation }) => {
                assert_eq!(confirmation.target_session_id, "logical-child");
                assert_eq!(confirmation.envelope_id, "broker-typed-id");
                assert_eq!(confirmation.canonical_claim_generation, 9);
                assert_eq!(confirmation.activation_run_id, "run-broker-9");
            }
            other => panic!("expected typed confirmation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn carries_an_approval_round_trip_over_the_bus() {
        let (endpoint, _dir) = start_broker().await;
        spawn_worker(&endpoint, Arc::new(AskApproval));
        let mut link = connect_parent(&endpoint).await;

        link.send(ParentFrame::Run(RunSpec {
            assignment: "do the dangerous thing".into(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: None,
            execution_epoch: 0,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .unwrap();

        let mut saw_request = false;
        let result = loop {
            match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
                .await
                .expect("a frame")
                .expect("ok")
            {
                Some(ChildFrame::ApprovalRequest { id, body }) => {
                    saw_request = true;
                    assert_eq!(body.get("tool_name").and_then(|v| v.as_str()), Some("Bash"));
                    link.send(ParentFrame::ApprovalReply { id, approved: true })
                        .await
                        .unwrap();
                }
                Some(ChildFrame::Terminal { result, .. }) => break result,
                _ => continue,
            }
        };
        assert!(saw_request, "the worker must proxy an approval request up");
        assert_eq!(result.as_deref(), Some("approved"));
    }
}
