//! The `ChildExecutor` seam: how an actor actually runs a task.
//!
//! This crate never depends on the agent runtime. The worker process implements
//! [`ChildExecutor`] backed by the real `agent.execute()`; the transport layer drives it.
//! [`EchoExecutor`] is a dependency-free stand-in used by the demo worker and tests.

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::proto::{
    RunSpec, SessionMessageAdmissionConfirmation, SessionMessageDelivery, TerminalStatus,
};

/// Non-AgentEvent signals an executor sends to its transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorControl {
    SessionMessageAdmitted(SessionMessageAdmissionConfirmation),
}

/// Which kind of host callback a [`HostRequest`] is — selects the wire frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostRequestKind {
    /// Proxy a gated-tool approval to the host (→ `ChildFrame::ApprovalRequest`).
    Approval,
}

/// A single host-callback request: an executor proxies a gated-tool approval
/// back to the host over the run's WS and awaits the reply.
pub struct HostRequest {
    pub kind: HostRequestKind,
    pub body: serde_json::Value,
    pub reply: oneshot::Sender<serde_json::Value>,
}

/// Host-callback bridge handed to an executor (via [`EventSink`]) so a nested
/// sub-agent can proxy a gated-tool approval to the host — over the same
/// per-child WebSocket, no broker needed. Absent for tests/[`EchoExecutor`].
#[derive(Clone)]
pub struct HostBridge {
    req_tx: mpsc::UnboundedSender<HostRequest>,
}

impl HostBridge {
    /// Create a bridge + the receiver the transport pumps to the wire.
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<HostRequest>) {
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        (HostBridge { req_tx }, req_rx)
    }

    /// Proxy one gated-tool approval to the host and await the decision JSON
    /// (`{"approved": bool}`). The worker's permission flow blocks on this so the
    /// human decides on the parent (Phase 2).
    pub async fn approval_call(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.call(HostRequestKind::Approval, body).await
    }

    async fn call(
        &self,
        kind: HostRequestKind,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let (reply, rx) = oneshot::channel();
        self.req_tx
            .send(HostRequest { kind, body, reply })
            .map_err(|_| "host bridge closed".to_string())?;
        rx.await
            .map_err(|_| "host bridge dropped reply".to_string())
    }
}

/// Sink an executor emits events into; the transport forwards each as a `ChildFrame::Event`.
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::UnboundedSender<serde_json::Value>,
    control_tx: Option<mpsc::UnboundedSender<ExecutorControl>>,
    host: Option<HostBridge>,
}

impl EventSink {
    /// Create a sink + the receiver the transport pumps to the wire.
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<serde_json::Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            EventSink {
                tx,
                control_tx: None,
                host: None,
            },
            rx,
        )
    }
    /// Create a sink with a control channel for protocol-level confirmations.
    pub fn channel_with_control() -> (
        Self,
        mpsc::UnboundedReceiver<serde_json::Value>,
        mpsc::UnboundedReceiver<ExecutorControl>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        (
            EventSink {
                tx,
                control_tx: Some(control_tx),
                host: None,
            },
            rx,
            control_rx,
        )
    }
    /// Attach a host-callback bridge (the transport wires this for real runs).
    pub fn with_host_bridge(mut self, bridge: HostBridge) -> Self {
        self.host = Some(bridge);
        self
    }
    /// The host-callback bridge, if this run was wired with one.
    pub fn host(&self) -> Option<&HostBridge> {
        self.host.as_ref()
    }
    /// Emit one event (serialized agent event). Dropped silently if the peer is gone.
    pub fn emit(&self, event: serde_json::Value) {
        let _ = self.tx.send(event);
    }
    /// Confirm a forwarded SessionInbox message only after the executor has
    /// observed its durable local admitted receipt.
    pub fn confirm_session_message(&self, confirmation: SessionMessageAdmissionConfirmation) {
        if let Some(tx) = &self.control_tx {
            let _ = tx.send(ExecutorControl::SessionMessageAdmitted(confirmation));
        }
    }
}

/// Result of running a task to completion (or suspension).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChildOutcome {
    pub status: TerminalStatus,
    pub result: Option<String>,
    pub error: Option<String>,
    /// Rolling-wire compatibility field. The current actor host never consumes
    /// it: canonical session checkpoints are the transcript authority and
    /// suspend/resume dispatch is not implemented. Keep serializing the empty
    /// field until the protocol is versioned; do not build new state transfer on
    /// this payload.
    #[serde(default)]
    pub transcript: Vec<serde_json::Value>,
}

impl ChildOutcome {
    pub fn completed(result: impl Into<String>) -> Self {
        Self {
            status: TerminalStatus::Completed,
            result: Some(result.into()),
            error: None,
            transcript: Vec::new(),
        }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            status: TerminalStatus::Error,
            result: None,
            error: Some(msg.into()),
            transcript: Vec::new(),
        }
    }
    pub fn cancelled() -> Self {
        Self {
            status: TerminalStatus::Cancelled,
            result: None,
            error: None,
            transcript: Vec::new(),
        }
    }
    /// Compatibility constructor for the unimplemented suspend wire path.
    /// Current hosts reject `Suspended` and do not persist this transcript.
    pub fn suspended(transcript: Vec<serde_json::Value>) -> Self {
        Self {
            status: TerminalStatus::Suspended,
            result: None,
            error: None,
            transcript,
        }
    }
}

/// Mid-run steering inbox: `ParentFrame::Message` texts arriving while a run is
/// active. Executors that support in-band steering admit them at a safe point
/// (the engine's round boundary); others may simply ignore the inbox.
pub struct SteerInbox {
    rx: mpsc::UnboundedReceiver<SteerMessage>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SteerMessage {
    /// Direct legacy WS text has no durable ingress identity and remains
    /// explicitly best-effort.
    Text(String),
    /// Broker Maildir ingress retains its durable message id so at-least-once
    /// replay converges on one typed SessionInbox envelope.
    DurableText {
        message_id: String,
        text: String,
    },
    SessionMessage(Box<SessionMessageDelivery>),
}

impl From<String> for SteerMessage {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for SteerMessage {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl SteerInbox {
    /// Create a sender + inbox pair (the transport holds the sender).
    pub fn channel() -> (mpsc::UnboundedSender<SteerMessage>, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, SteerInbox { rx })
    }
    /// An already-closed inbox (for tests / executors that don't steer).
    pub fn disconnected() -> Self {
        let (_tx, rx) = mpsc::unbounded_channel();
        SteerInbox { rx }
    }
    /// Next steering message, or `None` once the run's sender is gone.
    pub async fn recv(&mut self) -> Option<String> {
        match self.rx.recv().await? {
            SteerMessage::Text(text) => Some(text),
            SteerMessage::DurableText { text, .. } => Some(text),
            SteerMessage::SessionMessage(delivery) => serde_json::to_string(&delivery).ok(),
        }
    }
    /// Receive the typed steering value. Runtime-backed workers use this path
    /// so SessionInbox envelopes never collapse into ad-hoc text.
    pub async fn recv_message(&mut self) -> Option<SteerMessage> {
        self.rx.recv().await
    }
}

/// What runs inside an actor. Implemented by the worker with the real runtime.
#[async_trait]
pub trait ChildExecutor: Send + Sync + 'static {
    /// Maximum number of independent Run/Ask/Task executions this instance may
    /// execute at once. The safe default is one: production executors often
    /// own mutable permission/provider/child-runner state that must not cross
    /// session boundaries. An implementation may opt into more slots only when
    /// all per-run state is isolated.
    fn max_parallel_executions(&self) -> usize {
        1
    }

    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome;
}

/// Dependency-free executor: streams one `token` event per word, then completes with an echo.
/// Used by the demo worker and tests to exercise the full transport without a real LLM.
///
/// Test hook: an assignment starting with `__sleep_ms:<n>` sleeps (cancellably)
/// for `n` milliseconds before echoing the rest — this is what lets cancel /
/// concurrency e2e tests hold a run open deterministically without an LLM.
pub struct EchoExecutor;

/// Assignment prefix recognized by [`EchoExecutor`] for a cancellable delay.
pub const ECHO_SLEEP_PREFIX: &str = "__sleep_ms:";

#[async_trait]
impl ChildExecutor for EchoExecutor {
    fn max_parallel_executions(&self) -> usize {
        // Echo has no mutable execution state; keep the high-concurrency fabric
        // E2E honest without weakening the safe default for real runtimes.
        256
    }

    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        _steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        // Optional cancellable delay: any token `__sleep_ms:<n>` in the
        // assignment (scanned, not just the prefix — child creation may wrap
        // the prompt in a template). The marker token itself is not echoed.
        let mut sleep_ms: Option<u64> = None;
        let mut words: Vec<&str> = Vec::new();
        for word in spec.assignment.split_whitespace() {
            match word
                .strip_prefix(ECHO_SLEEP_PREFIX)
                .and_then(|n| n.parse::<u64>().ok())
            {
                Some(ms) if sleep_ms.is_none() => sleep_ms = Some(ms),
                _ => words.push(word),
            }
        }
        if let Some(ms) = sleep_ms {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
                _ = cancel.cancelled() => return ChildOutcome::cancelled(),
            }
        }

        for word in &words {
            if cancel.is_cancelled() {
                return ChildOutcome::cancelled();
            }
            events.emit(serde_json::json!({ "type": "token", "content": format!("{word} ") }));
            // tiny yield so cancellation can interleave; not a real delay
            tokio::task::yield_now().await;
        }
        events.emit(serde_json::json!({ "type": "complete" }));
        ChildOutcome::completed(format!("echo: {}", words.join(" ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn echo_explicitly_opts_into_high_parallelism() {
        assert!(EchoExecutor.max_parallel_executions() >= 200);
    }

    #[tokio::test]
    async fn echo_streams_then_completes() {
        let (sink, mut rx) = EventSink::channel();
        let outcome = EchoExecutor
            .run(
                RunSpec {
                    assignment: "alpha beta".into(),
                    logical_session: None,
                    project_id: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    messages: Vec::new(),
                    activation_run_id: None,
                    execution_epoch: 0,
                    initial_session_messages: Vec::new(),
                    secrets: Default::default(),
                },
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("echo: alpha beta"));

        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }
        // two tokens + one complete
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["content"], "alpha ");
    }

    #[tokio::test]
    async fn echo_honors_cancel() {
        let (sink, _rx) = EventSink::channel();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = EchoExecutor
            .run(
                RunSpec {
                    assignment: "a b c".into(),
                    logical_session: None,
                    project_id: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    messages: Vec::new(),
                    activation_run_id: None,
                    execution_epoch: 0,
                    initial_session_messages: Vec::new(),
                    secrets: Default::default(),
                },
                sink,
                SteerInbox::disconnected(),
                cancel,
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
    }

    #[tokio::test]
    async fn approval_call_sends_approval_kind_and_round_trips_reply() {
        let (bridge, mut req_rx) = HostBridge::channel();
        let caller = tokio::spawn(async move {
            bridge
                .approval_call(serde_json::json!({"resource": "/tmp/x"}))
                .await
        });
        let req = req_rx.recv().await.expect("a host request");
        assert_eq!(req.kind, HostRequestKind::Approval);
        assert_eq!(req.body["resource"], "/tmp/x");
        let _ = req.reply.send(serde_json::json!({"approved": true}));
        let reply = caller.await.unwrap().expect("decision");
        assert_eq!(reply["approved"], true);
    }
}
