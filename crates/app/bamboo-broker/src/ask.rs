//! Orchestrator-side ask: deliver an `Ask` to a target agent over the broker and
//! await its correlated `Reply`. This is the primitive the `SubAgent` "ask"
//! action (and tests) build on — `me` asks `target` a question and judges the
//! answer, regardless of where `target` physically runs.

use std::time::Duration;

use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

/// Connect as `me`, ask `target` a `question` in `mode`, and return the answer.
/// Convenience wrapper over [`ask_over`] for a one-shot ask.
pub async fn ask_agent(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    target: &str,
    question: &str,
    mode: AskMode,
    timeout: Duration,
) -> BrokerResult<String> {
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    ask_over(&mut client, &me, target, question, mode, timeout).await
}

/// Ask over an already-connected, already-subscribed client — so an orchestrator
/// can reuse one connection for many asks. Delivers the `Ask` and waits for the
/// `Reply` whose `correlation_id` matches (skipping any unrelated messages), up
/// to `timeout`.
pub async fn ask_over(
    client: &mut BrokerClient,
    me: &AgentRef,
    target: &str,
    question: &str,
    mode: AskMode,
    timeout: Duration,
) -> BrokerResult<String> {
    let msg = InboxMessage {
        id: MsgId::new(),
        from: me.clone(),
        kind: InboxKind::Ask,
        body: serde_json::to_value(AskBody {
            question: question.to_string(),
            mode,
        })
        .expect("AskBody serializes"),
        created_at: Utc::now(),
        correlation_id: None,
    };
    let qid = msg.id.clone();
    client.deliver(target, msg).await?;

    loop {
        match tokio::time::timeout(timeout, client.next_message()).await {
            Ok(Some(reply)) if reply.correlation_id.as_ref() == Some(&qid) => {
                let body: ReplyBody = serde_json::from_value(reply.body)
                    .map_err(|e| BrokerError::Protocol(format!("bad reply body: {e}")))?;
                return Ok(body.answer);
            }
            Ok(Some(_)) => continue, // unrelated message; keep waiting for ours
            Ok(None) => {
                return Err(BrokerError::Transport(
                    "connection closed before reply".into(),
                ))
            }
            Err(_) => {
                // We're abandoning this ask — tell the worker to stop the
                // (expensive) in-flight run instead of letting it burn the whole
                // LLM call for a result no one will read. Best-effort + out-of-band
                // (no receipt); a worker that's offline or doesn't understand
                // Cancel just ignores it. #50.
                let _ = client.cancel(target, &qid).await;
                return Err(BrokerError::Transport(format!(
                    "ask to '{target}' timed out after {timeout:?}"
                )));
            }
        }
    }
}

/// Generic correlated request/reply over an existing connected + subscribed
/// client: deliver a message of `kind` carrying `body` to `target`, then wait
/// for the reply whose `correlation_id` matches and return its body. Up to
/// `timeout`. (The MCP proxy and `ask` both build on this.)
pub async fn request_over(
    client: &mut BrokerClient,
    me: &AgentRef,
    target: &str,
    kind: InboxKind,
    body: serde_json::Value,
    timeout: Duration,
) -> BrokerResult<serde_json::Value> {
    let msg = InboxMessage {
        id: MsgId::new(),
        from: me.clone(),
        kind,
        body,
        created_at: Utc::now(),
        correlation_id: None,
    };
    let qid = msg.id.clone();
    client.deliver(target, msg).await?;

    loop {
        match tokio::time::timeout(timeout, client.next_message()).await {
            Ok(Some(reply)) if reply.correlation_id.as_ref() == Some(&qid) => return Ok(reply.body),
            Ok(Some(_)) => continue,
            Ok(None) => {
                return Err(BrokerError::Transport(
                    "connection closed before reply".into(),
                ))
            }
            Err(_) => {
                // Abandoning this request — cancel the worker's in-flight run so
                // it doesn't burn the whole call for an unread result. Best-effort,
                // out-of-band. #50.
                let _ = client.cancel(target, &qid).await;
                return Err(BrokerError::Transport(format!(
                    "request to '{target}' timed out after {timeout:?}"
                )));
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
    use std::sync::Arc;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn ask_agent_round_trip_against_echo_executor() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        // Echo-executor agent on the bus.
        let ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &ep,
                AgentRef {
                    session_id: "w".into(),
                    role: None,
                },
                "t",
                Arc::new(bamboo_subagent::EchoExecutor),
            )
            .await;
        });

        let answer = ask_agent(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            "t",
            "w",
            "ping pong",
            AskMode::Query,
            Duration::from_secs(5),
        )
        .await
        .expect("ask returns an answer");
        assert_eq!(answer, "echo: ping pong");
    }

    #[tokio::test]
    async fn ask_timeout_cancels_the_worker_run() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio_util::sync::CancellationToken;

        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        // A worker that parks on its cancel token forever and records the moment
        // it is cancelled.
        let was_cancelled = Arc::new(AtomicBool::new(false));
        // Completes a non-"park" ask immediately (used to confirm subscription);
        // parks forever on its cancel token for a "park" ask, recording the cancel.
        struct ProbeOrPark(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl ChildExecutor for ProbeOrPark {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                cancel: CancellationToken,
            ) -> ChildOutcome {
                if spec.assignment.contains("park") {
                    cancel.cancelled().await;
                    self.0.store(true, Ordering::SeqCst);
                    ChildOutcome::cancelled()
                } else {
                    ChildOutcome::completed("ready")
                }
            }
        }
        let ep = endpoint.clone();
        let flag = was_cancelled.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &ep,
                AgentRef {
                    session_id: "w".into(),
                    role: None,
                },
                "t",
                Arc::new(ProbeOrPark(flag)),
            )
            .await;
        });

        // Probe round-trip FIRST: an out-of-band cancel is dropped if the target
        // isn't subscribed, so confirm the worker is connected + subscribed before
        // the timing-sensitive park ask (otherwise a slow worker startup could
        // race the cancel — the poll loop below can't rescue a dropped cancel).
        let ready = ask_agent(
            &endpoint,
            AgentRef {
                session_id: "probe".into(),
                role: None,
            },
            "t",
            "w",
            "ping",
            AskMode::Query,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(ready.expect("probe answered"), "ready");

        // Now the park ask against the (confirmed-subscribed) worker times out...
        let result = ask_agent(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            "t",
            "w",
            "park on slow work",
            AskMode::Query,
            Duration::from_millis(300),
        )
        .await;
        assert!(result.is_err(), "ask times out (the worker never replies)");

        // ...and the timeout's out-of-band cancel aborted the worker's in-flight
        // run (end-to-end: ask_over -> ClientFrame::Cancel -> broker -> worker
        // next_cancel -> the run's token). #50.
        let mut observed = false;
        for _ in 0..100 {
            if was_cancelled.load(Ordering::SeqCst) {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            observed,
            "ask_agent timeout cancelled the worker's in-flight run"
        );
    }
}
