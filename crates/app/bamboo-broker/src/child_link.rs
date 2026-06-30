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
    AgentRef, ChildFrame, ChildOutcome, InboxKind, InboxMessage, MsgId, ParentFrame,
};
use chrono::Utc;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

/// A parent→child link over the broker, addressing the child by its mailbox id.
pub struct BrokerChildLink {
    client: BrokerClient,
    /// The child's mailbox id (where `Run`/`Cancel` are delivered).
    child: String,
    /// This parent's ref (the `from` on outbound messages; replies route here).
    me: AgentRef,
    /// The current run's correlation id (the delivered `Run` message id). Set on
    /// `send(Run)`; `next_frame` only surfaces frames correlated to it.
    run_id: Option<MsgId>,
    /// True once a terminal frame has been surfaced for the current run.
    done: bool,
}

impl BrokerChildLink {
    /// Connect to the broker as `parent` and subscribe, ready to drive `child`.
    pub async fn connect(
        endpoint: &str,
        parent: AgentRef,
        token: &str,
        child: impl Into<String>,
    ) -> BrokerResult<Self> {
        let mut client = BrokerClient::connect(endpoint, parent.clone(), token).await?;
        client.subscribe().await?;
        Ok(Self {
            client,
            child: child.into(),
            me: parent,
            run_id: None,
            done: false,
        })
    }

    fn msg(&self, kind: InboxKind, body: serde_json::Value, correlation: Option<MsgId>) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: self.me.clone(),
            kind,
            body,
            created_at: Utc::now(),
            correlation_id: correlation,
        }
    }

    /// Send a parent→child frame, mirroring `ChildClient::send`.
    pub async fn send(&mut self, frame: ParentFrame) -> BrokerResult<()> {
        match frame {
            ParentFrame::Run(spec) => {
                let body = serde_json::to_value(spec)
                    .map_err(|e| BrokerError::Transport(format!("encode RunSpec: {e}")))?;
                let m = self.msg(InboxKind::Run, body, None);
                self.run_id = Some(m.id.clone());
                self.done = false;
                self.client.deliver(&self.child, m).await?;
            }
            ParentFrame::Cancel => {
                if let Some(rid) = self.run_id.clone() {
                    self.client.cancel(&self.child, &rid).await?;
                }
            }
            // Steer / approval-delegation over the bus land in a later phase
            // (the worker's Run handler uses a disconnected steer inbox today).
            ParentFrame::Message { .. } | ParentFrame::ApprovalReply { .. } => {
                tracing::debug!("BrokerChildLink: steer/approval not yet carried over the bus");
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
            let Some(msg) = self.client.next_message().await else {
                return Ok(None);
            };
            let id = msg.id.clone();
            // Only this run's frames; ack + skip anything else so the mailbox drains.
            if self.run_id.is_some() && msg.correlation_id != self.run_id {
                self.client.ack(id).await.ok();
                continue;
            }
            let frame = match msg.kind {
                InboxKind::Event => Some(ChildFrame::Event { event: msg.body }),
                InboxKind::Outcome => {
                    let oc: ChildOutcome = serde_json::from_value(msg.body)
                        .map_err(|e| BrokerError::Transport(format!("decode ChildOutcome: {e}")))?;
                    self.done = true;
                    Some(ChildFrame::Terminal {
                        status: oc.status,
                        result: oc.result,
                        error: oc.error,
                        transcript: oc.transcript,
                    })
                }
                // ApprovalRequest etc. are not produced over the bus yet.
                _ => None,
            };
            self.client.ack(id).await.ok();
            if let Some(f) = frame {
                return Ok(Some(f));
            }
        }
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
                AgentRef { session_id: "child".into(), role: None },
                "t",
                Arc::new(EchoExecutor),
            )
            .await;
        });

        let mut link = BrokerChildLink::connect(
            &endpoint,
            AgentRef { session_id: "parent".into(), role: None },
            "t",
            "child",
        )
        .await
        .unwrap();

        link.send(ParentFrame::Run(RunSpec {
            assignment: "hello world".into(),
            reasoning_effort: None,
            messages: vec![],
        }))
        .await
        .unwrap();

        let mut events = 0usize;
        let mut terminal = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(5), link.next_frame())
                .await
                .expect("a frame arrives")
                .expect("link ok")
            {
                Some(ChildFrame::Event { .. }) => events += 1,
                Some(ChildFrame::Terminal { status, result, .. }) => {
                    terminal = Some((status, result));
                    break;
                }
                Some(_) => {}
                None => break,
            }
        }

        assert!(events >= 1, "expected streamed events, got {events}");
        let (status, result) = terminal.expect("a terminal frame");
        assert_eq!(status, bamboo_subagent::TerminalStatus::Completed);
        assert_eq!(result.as_deref(), Some("echo: hello world"));

        // After the terminal, the link is drained.
        assert!(link.next_frame().await.unwrap().is_none());
    }
}
