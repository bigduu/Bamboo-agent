//! The `ChildExecutor` seam: how an actor actually runs a task.
//!
//! This crate never depends on the agent runtime. The worker process implements
//! [`ChildExecutor`] backed by the real `agent.execute()`; the transport layer drives it.
//! [`EchoExecutor`] is a dependency-free stand-in used by the demo worker and tests.

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::proto::{RunSpec, TerminalStatus};

/// Sink an executor emits events into; the transport forwards each as a `ChildFrame::Event`.
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::UnboundedSender<serde_json::Value>,
}

impl EventSink {
    /// Create a sink + the receiver the transport pumps to the wire.
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<serde_json::Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (EventSink { tx }, rx)
    }
    /// Emit one event (serialized agent event). Dropped silently if the peer is gone.
    pub fn emit(&self, event: serde_json::Value) {
        let _ = self.tx.send(event);
    }
}

/// Result of running a task to completion.
#[derive(Debug, Clone, PartialEq)]
pub struct ChildOutcome {
    pub status: TerminalStatus,
    pub result: Option<String>,
    pub error: Option<String>,
}

impl ChildOutcome {
    pub fn completed(result: impl Into<String>) -> Self {
        Self {
            status: TerminalStatus::Completed,
            result: Some(result.into()),
            error: None,
        }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            status: TerminalStatus::Error,
            result: None,
            error: Some(msg.into()),
        }
    }
    pub fn cancelled() -> Self {
        Self {
            status: TerminalStatus::Cancelled,
            result: None,
            error: None,
        }
    }
}

/// Mid-run steering inbox: `ParentFrame::Message` texts arriving while a run is
/// active. Executors that support in-band steering admit them at a safe point
/// (the engine's round boundary); others may simply ignore the inbox.
pub struct SteerInbox {
    rx: mpsc::UnboundedReceiver<String>,
}

impl SteerInbox {
    /// Create a sender + inbox pair (the transport holds the sender).
    pub fn channel() -> (mpsc::UnboundedSender<String>, Self) {
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
        self.rx.recv().await
    }
}

/// What runs inside an actor. Implemented by the worker with the real runtime.
#[async_trait]
pub trait ChildExecutor: Send + Sync + 'static {
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome;
}

/// Dependency-free executor: streams one `token` event per word, then completes with an echo.
/// Used by the demo worker and the e2e test to exercise the full transport without a real LLM.
pub struct EchoExecutor;

#[async_trait]
impl ChildExecutor for EchoExecutor {
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        _steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        for word in spec.assignment.split_whitespace() {
            if cancel.is_cancelled() {
                return ChildOutcome::cancelled();
            }
            events.emit(serde_json::json!({ "type": "token", "content": format!("{word} ") }));
            // tiny yield so cancellation can interleave; not a real delay
            tokio::task::yield_now().await;
        }
        events.emit(serde_json::json!({ "type": "complete" }));
        ChildOutcome::completed(format!("echo: {}", spec.assignment.trim()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_streams_then_completes() {
        let (sink, mut rx) = EventSink::channel();
        let outcome = EchoExecutor
            .run(
                RunSpec {
                    assignment: "alpha beta".into(),
                    reasoning_effort: None,
                messages: Vec::new(),
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
                    reasoning_effort: None,
                messages: Vec::new(),
                },
                sink,
                SteerInbox::disconnected(),
                cancel,
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
    }
}
