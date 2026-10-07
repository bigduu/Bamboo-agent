//! Orchestrator-side ask: deliver an `Ask` to a target agent over the broker and
//! await its correlated `Reply`. This is the primitive the `SubAgent` "ask"
//! action (and tests) build on — `me` asks `target` a question and judges the
//! answer, regardless of where `target` physically runs.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};
use crate::mux::MultiplexedClient;

const MAX_ACTIVE_CALLER_ROUTES: usize = 4096;

#[derive(Eq, PartialEq)]
struct CallerRouteKey {
    endpoint: String,
    session_id: String,
    role: Option<String>,
    credential_digest: [u8; 32],
}

impl Hash for CallerRouteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.session_id.hash(state);
        self.role.hash(state);
        self.credential_digest.hash(state);
    }
}

type CallerRoute = Mutex<Weak<MultiplexedClient>>;

fn caller_route(endpoint: &str, me: &AgentRef, token: &str) -> BrokerResult<Arc<CallerRoute>> {
    static ROUTES: OnceLock<StdMutex<HashMap<CallerRouteKey, Arc<CallerRoute>>>> = OnceLock::new();
    let key = CallerRouteKey {
        endpoint: endpoint.to_owned(),
        session_id: me.session_id.clone(),
        role: me.role.clone(),
        credential_digest: Sha256::digest(token.as_bytes()).into(),
    };
    let mut routes = ROUTES
        .get_or_init(|| StdMutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    routes.retain(|_, route| {
        Arc::strong_count(route) > 1
            || route
                .try_lock()
                .map(|current| current.upgrade().is_some())
                .unwrap_or(true)
    });
    if let Some(route) = routes.get(&key) {
        return Ok(Arc::clone(route));
    }
    if routes.len() >= MAX_ACTIVE_CALLER_ROUTES {
        return Err(BrokerError::Transport(
            "too many active broker caller routes".into(),
        ));
    }
    let route = Arc::new(Mutex::new(Weak::new()));
    routes.insert(key, Arc::clone(&route));
    Ok(route)
}

async fn shared_caller_mux(
    endpoint: &str,
    me: &AgentRef,
    token: &str,
) -> BrokerResult<Arc<MultiplexedClient>> {
    let route = caller_route(endpoint, me, token)?;
    let mut current = route.lock().await;
    if let Some(mux) = current.upgrade().filter(|mux| mux.reader_alive()) {
        return Ok(mux);
    }
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    let mux = Arc::new(client.into_multiplexed(me.clone()));
    *current = Arc::downgrade(&mux);
    Ok(mux)
}

/// Connect as `me`, ask `target` a `question` in `mode`, and return the answer.
/// Concurrent asks from one authenticated caller share a single subscribed
/// connection, so a later ask cannot replace an earlier ask's reply route.
pub async fn ask_agent(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    target: &str,
    question: &str,
    mode: AskMode,
    timeout: Duration,
) -> BrokerResult<String> {
    let deadline = Instant::now() + timeout;
    let mux = tokio::time::timeout_at(deadline, shared_caller_mux(endpoint, &me, token))
        .await
        .map_err(|_| {
            BrokerError::Transport(format!("ask to '{target}' timed out after {timeout:?}"))
        })??;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(BrokerError::Transport(format!(
            "ask to '{target}' timed out after {timeout:?}"
        )));
    }
    mux.ask(target, question, mode, remaining).await
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
    // One deadline covers the delivery receipt, unrelated traffic, the reply,
    // and its durable ACK. Repeated unrelated messages cannot renew the ask.
    let result = tokio::time::timeout(timeout, async {
        client.deliver(target, msg).await?;
        loop {
            match client.next_message().await {
                Some(reply)
                    if reply.correlation_id.as_ref() == Some(&qid)
                        && reply.kind == InboxKind::Reply
                        && reply.from.session_id == target =>
                {
                    let body: ReplyBody = serde_json::from_value(reply.body.clone())
                        .map_err(|e| BrokerError::Protocol(format!("bad reply body: {e}")))?;
                    client.ack_confirmed(reply.id).await?;
                    return Ok(body.answer);
                }
                // Leave unrelated durable messages unacknowledged. A later
                // owner can recover them after this connection closes.
                Some(_) => continue,
                None => {
                    return Err(BrokerError::Transport(
                        "connection closed before reply".into(),
                    ))
                }
            }
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            // The worker may still be running after an uncertain delivery.
            // Cancellation is best effort and must not make timeout unbounded.
            let _ = tokio::time::timeout(Duration::from_secs(1), client.cancel(target, &qid)).await;
            Err(BrokerError::Transport(format!(
                "ask to '{target}' timed out after {timeout:?}"
            )))
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
    let expected_reply = if kind == InboxKind::McpRequest {
        InboxKind::McpReply
    } else {
        InboxKind::Reply
    };
    let result = tokio::time::timeout(timeout, async {
        client.deliver(target, msg).await?;
        loop {
            match client.next_message().await {
                Some(reply)
                    if reply.correlation_id.as_ref() == Some(&qid)
                        && reply.kind == expected_reply
                        && reply.from.session_id == target =>
                {
                    let body = reply.body.clone();
                    client.ack_confirmed(reply.id).await?;
                    return Ok(body);
                }
                Some(_) => continue,
                None => {
                    return Err(BrokerError::Transport(
                        "connection closed before reply".into(),
                    ))
                }
            }
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = tokio::time::timeout(Duration::from_secs(1), client.cancel(target, &qid)).await;
            Err(BrokerError::Transport(format!(
                "request to '{target}' timed out after {timeout:?}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::serve::serve_executor;
    use crate::server::BrokerServer;
    use crate::{BrokerLimits, PeerPolicy};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn agent(session_id: &str) -> AgentRef {
        AgentRef {
            session_id: session_id.into(),
            role: None,
        }
    }

    fn answer(from: &str, request: &InboxMessage, value: &str) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: agent(from),
            kind: InboxKind::Reply,
            body: serde_json::to_value(ReplyBody {
                answer: value.into(),
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: Some(request.id.clone()),
        }
    }

    async fn start_test_broker() -> (String, tempfile::TempDir) {
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

    #[tokio::test]
    async fn overlapping_asks_from_one_caller_route_reversed_replies_exactly() {
        let (endpoint, _dir) = start_test_broker().await;
        let mut worker = BrokerClient::connect(&endpoint, agent("worker"), "t")
            .await
            .unwrap();
        worker.subscribe().await.unwrap();
        let responder = tokio::spawn(async move {
            let first = worker.next_message().await.unwrap();
            let second = worker.next_message().await.unwrap();
            assert_eq!(first.kind, InboxKind::Ask);
            assert_eq!(second.kind, InboxKind::Ask);
            for request in [second, first] {
                let question: AskBody = serde_json::from_value(request.body.clone()).unwrap();
                worker
                    .deliver("caller", answer("worker", &request, &question.question))
                    .await
                    .unwrap();
            }
        });

        let (first, second) = tokio::time::timeout(Duration::from_secs(4), async {
            tokio::join!(
                ask_agent(
                    &endpoint,
                    agent("caller"),
                    "t",
                    "worker",
                    "first",
                    AskMode::Query,
                    Duration::from_secs(3),
                ),
                ask_agent(
                    &endpoint,
                    agent("caller"),
                    "t",
                    "worker",
                    "second",
                    AskMode::Query,
                    Duration::from_secs(3),
                )
            )
        })
        .await
        .unwrap();
        assert_eq!(first.unwrap(), "first");
        assert_eq!(second.unwrap(), "second");
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn scoped_overlapping_asks_use_only_the_authorized_caller_mailbox() {
        const CALLER_TOKEN: &str = "caller-opaque-credential-00000000001";
        const WORKER_TOKEN: &str = "worker-opaque-credential-00000000001";
        let dir = tempfile::tempdir().unwrap();
        let policy = PeerPolicy::from_json(
            &serde_json::to_vec(&serde_json::json!({"peers":[
                {
                    "credential": CALLER_TOKEN, "host": "caller-host", "mailbox": "caller",
                    "role": "parent", "expires_at": Utc::now() + chrono::Duration::minutes(5),
                    "destinations": [{"mailbox":"worker","kinds":["ask"]}],
                    "cancel": ["worker"], "presence": []
                },
                {
                    "credential": WORKER_TOKEN, "host": "worker-host", "mailbox": "worker",
                    "role": "worker", "expires_at": Utc::now() + chrono::Duration::minutes(5),
                    "destinations": [{"mailbox":"caller","kinds":["reply"]}],
                    "cancel": [], "presence": []
                }
            ]}))
            .unwrap(),
        )
        .unwrap();
        let core = Arc::new(BrokerCore::new_scoped(dir.path()));
        let server = Arc::new(BrokerServer::with_peer_policy(
            core,
            policy,
            BrokerLimits::default(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let caller = AgentRef {
            session_id: "caller".into(),
            role: Some("parent".into()),
        };
        assert!(BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "caller-per-request".into(),
                role: Some("parent".into()),
            },
            CALLER_TOKEN,
        )
        .await
        .is_err());
        let worker_ref = AgentRef {
            session_id: "worker".into(),
            role: Some("worker".into()),
        };
        let mut worker = BrokerClient::connect(&endpoint, worker_ref.clone(), WORKER_TOKEN)
            .await
            .unwrap();
        worker.subscribe().await.unwrap();
        let responder = tokio::spawn(async move {
            let first = worker.next_message().await.unwrap();
            let second = worker.next_message().await.unwrap();
            for request in [second, first] {
                let question: AskBody = serde_json::from_value(request.body.clone()).unwrap();
                let mut reply = answer("worker", &request, &question.question);
                reply.from = worker_ref.clone();
                worker.deliver("caller", reply).await.unwrap();
            }
        });
        let (first, second) = tokio::join!(
            ask_agent(
                &endpoint,
                caller.clone(),
                CALLER_TOKEN,
                "worker",
                "alpha",
                AskMode::Query,
                Duration::from_secs(3),
            ),
            ask_agent(
                &endpoint,
                caller,
                CALLER_TOKEN,
                "worker",
                "beta",
                AskMode::Query,
                Duration::from_secs(3),
            )
        );
        assert_eq!(first.unwrap(), "alpha");
        assert_eq!(second.unwrap(), "beta");
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn timed_out_ask_cancels_and_reconnect_retires_only_its_stale_reply() {
        let (endpoint, _dir) = start_test_broker().await;
        let mut worker = BrokerClient::connect(&endpoint, agent("worker"), "t")
            .await
            .unwrap();
        worker.subscribe().await.unwrap();
        let unrelated = InboxMessage {
            id: MsgId::new(),
            from: agent("worker"),
            kind: InboxKind::Task,
            body: serde_json::json!({"keep":"for another consumer"}),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let unrelated_id = unrelated.id.clone();
        let (stale_sent, stale_ready) = tokio::sync::oneshot::channel();
        let responder = tokio::spawn(async move {
            let first = worker.next_message().await.unwrap();
            assert_eq!(first.kind, InboxKind::Ask);
            let cancelled = tokio::time::timeout(Duration::from_secs(2), worker.next_cancel())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cancelled, first.id);
            worker
                .deliver("caller", answer("worker", &first, "too late"))
                .await
                .unwrap();
            worker.deliver("caller", unrelated).await.unwrap();
            stale_sent.send(()).unwrap();
            let second = worker.next_message().await.unwrap();
            worker
                .deliver("caller", answer("worker", &second, "fresh"))
                .await
                .unwrap();
        });

        let error = ask_agent(
            &endpoint,
            agent("caller"),
            "t",
            "worker",
            "park",
            AskMode::Query,
            Duration::from_millis(150),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        tokio::time::timeout(Duration::from_secs(2), stale_ready)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ask_agent(
                &endpoint,
                agent("caller"),
                "t",
                "worker",
                "current",
                AskMode::Query,
                Duration::from_secs(3),
            )
            .await
            .unwrap(),
            "fresh"
        );
        responder.await.unwrap();

        // Both replies were ACKed. Other durable traffic remains recoverable.
        let mut replacement = BrokerClient::connect(&endpoint, agent("caller"), "t")
            .await
            .unwrap();
        replacement.subscribe().await.unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(1), replacement.next_message())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id, unrelated_id);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), replacement.next_message())
                .await
                .is_err()
        );
    }

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

        // A successful answer is durably ACKed before ask_agent returns.
        // Replacing its subscription must not replay the consumed Reply.
        let mut replacement = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            "t",
        )
        .await
        .unwrap();
        replacement.subscribe().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), replacement.next_message())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unrelated_traffic_does_not_extend_the_ask_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");
        let me = AgentRef {
            session_id: "orch".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), "t")
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let sender_endpoint = endpoint.clone();
        let spam = tokio::spawn(async move {
            let from = AgentRef {
                session_id: "noise".into(),
                role: None,
            };
            let mut sender = BrokerClient::connect(&sender_endpoint, from.clone(), "t")
                .await
                .unwrap();
            for _ in 0..60 {
                let message = InboxMessage {
                    id: MsgId::new(),
                    from: from.clone(),
                    kind: InboxKind::Reply,
                    body: serde_json::json!({ "answer": "unrelated" }),
                    created_at: Utc::now(),
                    correlation_id: Some(MsgId::new()),
                };
                sender.deliver("orch", message).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let result = tokio::time::timeout(
            Duration::from_millis(800),
            ask_over(
                &mut client,
                &me,
                "nobody",
                "waiting",
                AskMode::Query,
                Duration::from_millis(150),
            ),
        )
        .await;
        spam.abort();
        let error = result
            .expect("unrelated messages must not renew the deadline")
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
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
