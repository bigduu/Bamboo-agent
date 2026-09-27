//! Real WSS frames and physical ACK effects; no Echo executor or mock core.
use bamboo_broker::{
    client_config_trusting_cert, BrokerCore, BrokerFrame, BrokerLimits, BrokerServer, ClientFrame,
    PeerPolicy,
};
use bamboo_subagent::{ActorEventBatch, ActorEventQos, AgentRef, InboxKind, InboxMessage, MsgId};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite::Message, Connector, MaybeTlsStream, WebSocketStream,
};
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
const A: &str = "host-opaque-fixture-credential-00000001";
const B: &str = "worker-opaque-fixture-credential-000001";
fn peer(token: &str, mailbox: &str, expiry: chrono::DateTime<Utc>) -> Value {
    json!({"credential":token,"host":format!("host-{mailbox}"),"mailbox":mailbox,"role":"worker","expires_at":expiry,
        "destinations":[{"mailbox":"b","kinds":["ask","event"]}],"cancel":[],"presence":["worker"]})
}
fn policy() -> Value {
    json!({"peers":[peer(A,"a",Utc::now()+ChronoDuration::hours(1)),peer(B,"b",Utc::now()+ChronoDuration::hours(1))]})
}
fn cert(dir: &Path) -> (PathBuf, PathBuf) {
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    assert!(Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"])
        .args(["-subj", "/CN=127.0.0.1"])
        .args(["-addext", "subjectAltName=IP:127.0.0.1"])
        .args(["-addext", "basicConstraints=critical,CA:FALSE"])
        .arg("-keyout")
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .expect("required openssl unavailable")
        .status
        .success());
    (cert, key)
}
async fn socket(url: &str, cert: &Path) -> Ws {
    connect_async_tls_with_config(
        url,
        None,
        false,
        Some(Connector::Rustls(Arc::new(
            client_config_trusting_cert(cert).unwrap(),
        ))),
    )
    .await
    .unwrap()
    .0
}
async fn send(ws: &mut Ws, frame: ClientFrame) {
    ws.send(Message::text(frame.to_text())).await.unwrap();
}
async fn recv(ws: &mut Ws) -> BrokerFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                break BrokerFrame::from_text(&text).unwrap();
            }
        }
    })
    .await
    .unwrap()
}
fn agent(id: &str) -> AgentRef {
    AgentRef {
        session_id: id.into(),
        role: Some("worker".into()),
    }
}
async fn login(url: &str, cert: &Path, id: &str, token: &str) -> Ws {
    let mut ws = socket(url, cert).await;
    send(
        &mut ws,
        ClientFrame::Hello {
            agent: agent(id),
            token: token.into(),
        },
    )
    .await;
    assert!(matches!(recv(&mut ws).await, BrokerFrame::Welcome));
    ws
}
fn message() -> InboxMessage {
    InboxMessage {
        id: MsgId::new(),
        from: agent("a"),
        kind: InboxKind::Ask,
        body: json!({"question":"bounded ask"}),
        created_at: Utc::now(),
        correlation_id: None,
    }
}
fn delivery(to: &str, message: InboxMessage) -> ClientFrame {
    ClientFrame::Deliver {
        to: to.into(),
        message,
    }
}
async fn listen(
    core: Arc<BrokerCore>,
    policy: Value,
    cert: &Path,
    key: &Path,
    limits: BrokerLimits,
) -> (
    String,
    tokio::task::JoinHandle<bamboo_broker::BrokerResult<()>>,
) {
    let server = Arc::new(
        BrokerServer::with_peer_policy(
            core,
            PeerPolicy::from_json(&serde_json::to_vec(&policy).unwrap()).unwrap(),
            limits,
        )
        .with_tls(cert, key)
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://{}", listener.local_addr().unwrap());
    (
        url,
        tokio::spawn(async move { server.serve(listener).await }),
    )
}
fn batch(durable: bool) -> ActorEventBatch {
    ActorEventBatch {
        logical_session: None,
        activation_id: Some("run-1".into()),
        execution_epoch: 1,
        source_node_id: None,
        source_actor_id: Some("a".into()),
        first_seq: 1,
        last_seq: 1,
        qos: if durable {
            ActorEventQos::Durable
        } else {
            ActorEventQos::Ephemeral
        },
        events: vec![if durable {
            json!({"type":"complete"})
        } else {
            json!({"type":"token","content":"x"})
        }],
    }
}
fn files(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    fn walk(root: &Path, p: &Path, rows: &mut Vec<(PathBuf, Option<Vec<u8>>)>) {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries {
                let p = e.unwrap().path();
                let dir = p.is_dir();
                rows.push((
                    p.strip_prefix(root).unwrap().into(),
                    if dir {
                        None
                    } else {
                        Some(std::fs::read(&p).unwrap())
                    },
                ));
                if dir {
                    walk(root, &p, rows);
                }
            }
        }
    }
    let mut rows = Vec::new();
    walk(root, root, &mut rows);
    rows.sort();
    rows
}
async fn refused(url: &str, cert: &Path, frame: ClientFrame) {
    let mut ws = login(url, cert, "a", A).await;
    send(&mut ws, frame).await;
    assert!(
        matches!(recv(&mut ws).await,BrokerFrame::Error{reason,..} if reason.contains("scoped peer admission denied")&&!reason.contains(A))
    );
}

#[test]
fn bounded_closed_policy_rejects_ambiguity_without_echo() {
    let good = policy();
    PeerPolicy::from_json(&serde_json::to_vec(&good).unwrap()).unwrap();
    let mut cases = Vec::new();
    for field in ["mailbox", "host", "role"] {
        let mut v = good.clone();
        v["peers"][0][field] = json!("../escape");
        cases.push(v);
    }
    for (field, value) in [("unknown", json!(A)), ("credential", json!("short"))] {
        let mut v = good.clone();
        v["peers"][0][field] = value;
        cases.push(v);
    }
    let mut v = good.clone();
    v["peers"][1]["mailbox"] = json!("a");
    cases.push(v);
    let mut v = good.clone();
    v["peers"][1]["credential"] = json!(A);
    cases.push(v);
    let mut v = good.clone();
    v["peers"] = json!(vec![good["peers"][0].clone(); 65]);
    cases.push(v);
    let mut v = good.clone();
    v["peers"][0]["destinations"] = json!((0..17)
        .map(|i| json!({"mailbox":format!("d{i}"),"kinds":["ask"]}))
        .collect::<Vec<_>>());
    cases.push(v);
    for selector in ["mailbox", "destinations", "cancel"] {
        let mut v = good.clone();
        match selector {
            "mailbox" => v["peers"][1]["mailbox"] = json!("A"),
            "destinations" => v["peers"][0]["destinations"][0]["mailbox"] = json!("B"),
            _ => v["peers"][0]["cancel"] = json!(["B"]),
        }
        cases.push(v);
    }
    for v in cases {
        let error = PeerPolicy::from_json(&serde_json::to_vec(&v).unwrap())
            .err()
            .unwrap();
        assert!(!error.to_string().contains(A));
    }
    let raw = good
        .to_string()
        .replacen("\"peers\":", "\"peers\":[],\"peers\":", 1);
    assert!(PeerPolicy::from_json(raw.as_bytes()).is_err());
    let raw = good
        .to_string()
        .replacen("\"host\":", "\"host\":\"duplicate\",\"host\":", 1);
    assert!(PeerPolicy::from_json(raw.as_bytes()).is_err());
    assert!(PeerPolicy::from_json(&vec![b' '; 65537]).is_err());
}

#[tokio::test]
async fn wss_scoped_frames_current_ack_and_expiry_preserve_real_maildir() {
    let tmp = tempfile::tempdir().unwrap();
    let (cert, key) = cert(tmp.path());
    let core = Arc::new(BrokerCore::new_scoped(tmp.path()));
    let mut p = policy();
    p["peers"].as_array_mut().unwrap().push(peer(
        "expired-fixture-credential-00000000001",
        "expired",
        Utc::now() - ChronoDuration::seconds(1),
    ));
    let (url, task) = listen(core.clone(), p, &cert, &key, BrokerLimits::default()).await;
    let wrong = tempfile::tempdir().unwrap();
    let (wrong_cert, _) = self::cert(wrong.path());
    assert!(connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(Connector::Rustls(Arc::new(
            client_config_trusting_cert(&wrong_cert).unwrap()
        )))
    )
    .await
    .is_err());
    for (id, token, role) in [
        ("a", "wrong-credential", Some("worker")),
        ("b", A, Some("worker")),
        ("a", A, None),
        (
            "expired",
            "expired-fixture-credential-00000000001",
            Some("worker"),
        ),
    ] {
        let mut ws = socket(&url, &cert).await;
        send(
            &mut ws,
            ClientFrame::Hello {
                agent: AgentRef {
                    session_id: id.into(),
                    role: role.map(str::to_owned),
                },
                token: token.into(),
            },
        )
        .await;
        assert!(matches!(recv(&mut ws).await, BrokerFrame::Error { .. }));
    }
    assert!(!tmp.path().join("scoped-peers-v1/mailboxes").exists());
    let mut old = login(&url, &cert, "b", B).await;
    send(&mut old, ClientFrame::Subscribe).await;
    let mut a = login(&url, &cert, "a", A).await;
    let msg = message();
    let id = msg.id.clone();
    send(&mut a, delivery("b", msg)).await;
    assert!(matches!(recv(&mut a).await, BrokerFrame::Delivered { .. }));
    assert!(matches!(recv(&mut old).await,BrokerFrame::Message{message} if message.id==id));
    let root = tmp.path().join("scoped-peers-v1");
    let before = files(&root);
    assert!(before
        .iter()
        .any(|(p, _)| p.to_string_lossy().contains("/cur/")));
    let mut no_sub = login(&url, &cert, "b", B).await;
    send(&mut no_sub, ClientFrame::Ack { id: id.clone() }).await;
    assert!(matches!(recv(&mut no_sub).await, BrokerFrame::Error { .. }));
    assert_eq!(files(&root), before);
    let mut current = login(&url, &cert, "b", B).await;
    send(&mut current, ClientFrame::Subscribe).await;
    assert!(matches!(recv(&mut current).await,BrokerFrame::Message{message} if message.id==id));
    send(&mut old, ClientFrame::Ack { id: id.clone() }).await;
    assert!(matches!(recv(&mut old).await, BrokerFrame::Error { .. }));
    assert_eq!(files(&root), before);
    old.close(None).await.ok();
    assert!(core.is_subscribed("b").await);
    let mut bad = Vec::new();
    for i in 0..7 {
        let mut m = message();
        let mut to = "b".to_owned();
        match i {
            0 => m.from.session_id = "b".into(),
            1 => m.from.role = None,
            2 => to = "other".into(),
            3 => m.kind = InboxKind::Run,
            4 => m.id = MsgId("x/../../../../outside".into()),
            5 => m.correlation_id = Some(MsgId("../unsafe".into())),
            _ => to = "B".into(),
        };
        bad.push(delivery(&to, m));
    }
    for i in 0..4 {
        let mut b = batch(true);
        match i {
            0 => b.source_actor_id = None,
            1 => b.source_actor_id = Some("b".into()),
            2 => b.source_node_id = Some("other-host".into()),
            _ => b.qos = ActorEventQos::Ephemeral,
        };
        let mut m = message();
        m.kind = InboxKind::Event;
        m.body = serde_json::to_value(b).unwrap();
        bad.push(delivery("b", m));
    }
    let mut live = batch(false);
    live.source_actor_id = Some("b".into());
    bad.push(ClientFrame::PublishEventBatch {
        to: "b".into(),
        correlation_id: MsgId::new(),
        batch: live,
    });
    bad.push(ClientFrame::PublishEventBatch {
        to: "other".into(),
        correlation_id: MsgId::new(),
        batch: batch(false),
    });
    bad.extend([
        ClientFrame::Cancel {
            to: "b".into(),
            correlation_id: MsgId::new(),
        },
        ClientFrame::ListConnected {
            role: "private".into(),
        },
        ClientFrame::Hello {
            agent: agent("b"),
            token: B.into(),
        },
    ]);
    for frame in bad {
        refused(&url, &cert, frame).await;
        assert_eq!(files(&root), before);
        assert!(core.is_subscribed("b").await);
    }
    send(&mut current, ClientFrame::Ack { id }).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while files(&root)
            .iter()
            .any(|(p, _)| p.to_string_lossy().contains("/cur/"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut event = message();
    event.kind = InboxKind::Event;
    event.body = serde_json::to_value(batch(true)).unwrap();
    send(&mut a, delivery("b", event)).await;
    assert!(matches!(recv(&mut a).await, BrokerFrame::Delivered { .. }));
    assert!(
        matches!(recv(&mut current).await,BrokerFrame::Message{message} if message.kind==InboxKind::Event)
    );
    send(
        &mut a,
        ClientFrame::PublishEventBatch {
            to: "b".into(),
            correlation_id: MsgId::new(),
            batch: batch(false),
        },
    )
    .await;
    assert!(matches!(
        recv(&mut current).await,
        BrokerFrame::EventBatch { .. }
    ));
    task.abort();
    // Fresh listener/root: both idle and outbound-active peers expire. A frame
    // blocked by the actual limiter must not reach Core after its deadline.
    let expiry_root = tmp.path().join("expiry");
    let expiry_core = Arc::new(BrokerCore::new_scoped(&expiry_root));
    let mut expiring = policy();
    let expiry = Utc::now() + ChronoDuration::milliseconds(1800);
    for peer in expiring["peers"].as_array_mut().unwrap() {
        peer["expires_at"] = json!(expiry);
    }
    const IDLE: &str = "idle-fixture-credential-0000000000001";
    expiring["peers"]
        .as_array_mut()
        .unwrap()
        .push(peer(IDLE, "idle", expiry));
    let (url, task) = listen(
        expiry_core.clone(),
        expiring,
        &cert,
        &key,
        BrokerLimits {
            messages_per_second: std::num::NonZeroU32::new(1).unwrap(),
            message_burst: std::num::NonZeroU32::new(1).unwrap(),
            ..BrokerLimits::default()
        },
    )
    .await;
    let mut a = login(&url, &cert, "a", A).await;
    let mut b = login(&url, &cert, "b", B).await;
    let mut idle = login(&url, &cert, "idle", IDLE).await;
    send(&mut b, ClientFrame::Subscribe).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !expiry_core.is_subscribed("b").await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for _ in 0..2 {
        send(&mut a, delivery("b", message())).await;
        assert!(matches!(recv(&mut a).await, BrokerFrame::Delivered { .. }));
        assert!(matches!(recv(&mut b).await, BrokerFrame::Message { .. }));
    }
    let blocked = message();
    let blocked_id = blocked.id.as_str().to_owned();
    send(&mut a, delivery("b", blocked)).await;
    for ws in [&mut a, &mut b, &mut idle] {
        let end = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap();
        assert!(matches!(
            end,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
    }
    assert!(!files(&expiry_root)
        .iter()
        .any(|(p, _)| p.file_name().unwrap() == blocked_id.as_str()));
    let mut ws = socket(&url, &cert).await;
    send(
        &mut ws,
        ClientFrame::Hello {
            agent: agent("b"),
            token: B.into(),
        },
    )
    .await;
    assert!(matches!(recv(&mut ws).await, BrokerFrame::Error { .. }));
    task.abort();
}

#[tokio::test]
async fn strict_actor_link_fences_source_run_birth_and_order_over_real_wss() {
    use bamboo_broker::{BrokerChildLink, BrokerClient};
    use bamboo_subagent::{
        proto::ChildCreationIdentity, ChildFrame, LogicalSessionIdentity, ParentFrame, RunSpec,
    };
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = cert(dir.path());
    let expiry = Utc::now() + ChronoDuration::minutes(5);
    let mk = |token, mailbox, role| {
        json!({"credential":token,"mailbox":mailbox,"role":role,
        "host":format!("host-{mailbox}"),"expires_at":expiry,
        "destinations":[{"mailbox":if mailbox=="a" {"b"}else{"a"},"kinds":if mailbox=="a" {vec!["run"]}else{vec!["event","outcome"]}}],
        "cancel":if mailbox=="a" {vec!["b"]}else{vec![]},"presence":if mailbox=="a" {vec!["worker"]}else{vec![]}})
    };
    let core = Arc::new(BrokerCore::new(dir.path().join("broker")));
    let (url, server) = listen(
        core,
        json!({"peers":[mk(A,"a","host"),mk(B,"b","worker"),
        mk("rogue-opaque-fixture-credential-000001","c","worker")]}),
        &cert,
        &key,
        BrokerLimits::default(),
    )
    .await;
    let host = AgentRef {
        session_id: "a".into(),
        role: Some("host".into()),
    };
    let mut worker = BrokerClient::connect_with_tls(
        &url,
        agent("b"),
        B,
        Some(client_config_trusting_cert(&cert).unwrap()),
    )
    .await
    .unwrap();
    worker.subscribe().await.unwrap();
    let mut rogue = BrokerClient::connect_with_tls(
        &url,
        agent("c"),
        "rogue-opaque-fixture-credential-000001",
        Some(client_config_trusting_cert(&cert).unwrap()),
    )
    .await
    .unwrap();
    let identity = LogicalSessionIdentity {
        session_id: "logical-child".into(),
        parent_session_id: Some("logical-root".into()),
        root_session_id: "logical-root".into(),
        creation: Some(ChildCreationIdentity {
            created_at: Utc::now(),
            spawn_depth: 1,
        }),
    };
    for case in [
        "order",
        "source",
        "birth",
        "parent",
        "root",
        "activation",
        "epoch",
        "missing",
        "untyped",
    ] {
        eprintln!("strict scoped WSS case={case}");
        let mut link = BrokerChildLink::connect_strict_with_tls(
            &url,
            host.clone(),
            A,
            agent("b"),
            client_config_trusting_cert(&cert).unwrap(),
        )
        .await
        .unwrap();
        let mut run = RunSpec {
            assignment: "task".into(),
            logical_session: Some(identity.clone()),
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: Some(format!("run-{case}")),
            execution_epoch: 9,
            initial_session_messages: vec![],
            secrets: Default::default(),
        };
        if case == "missing" {
            run.logical_session.as_mut().unwrap().creation = None;
            assert!(link.send(ParentFrame::Run(run)).await.is_err());
            continue;
        }
        link.send(ParentFrame::Run(run.clone())).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(3), worker.next_message())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.kind, InboxKind::Run);
        worker.ack(received.id.clone()).await.unwrap();
        let mut batch = batch(true);
        batch.logical_session = Some(identity.clone());
        batch.activation_id = run.activation_run_id;
        batch.execution_epoch = run.execution_epoch;
        batch.source_actor_id = Some(if case == "source" { "c" } else { "b" }.into());
        batch.source_node_id = None;
        match case {
            "birth" => {
                batch
                    .logical_session
                    .as_mut()
                    .unwrap()
                    .creation
                    .as_mut()
                    .unwrap()
                    .created_at += ChronoDuration::nanoseconds(1)
            }
            "parent" => {
                batch.logical_session.as_mut().unwrap().parent_session_id = Some("foreign".into())
            }
            "root" => batch.logical_session.as_mut().unwrap().root_session_id = "foreign".into(),
            "activation" => batch.activation_id = Some("stale".into()),
            "epoch" => batch.execution_epoch += 1,
            _ => {}
        }
        let body = serde_json::to_value(&batch).unwrap();
        let msg = |body, from, correlation| InboxMessage {
            id: MsgId::new(),
            from,
            kind: InboxKind::Event,
            body,
            created_at: Utc::now(),
            correlation_id: Some(correlation),
        };
        if case == "source" {
            rogue
                .deliver("a", msg(body, agent("c"), received.id))
                .await
                .unwrap();
        } else if case == "untyped" {
            assert!(
                worker
                    .deliver(
                        "a",
                        msg(json!({"type":"complete"}), agent("b"), received.id)
                    )
                    .await
                    .is_err(),
                "{case}: scoped Broker rejects untyped Event"
            );
            continue;
        } else {
            if case == "order" {
                worker
                    .deliver("a", msg(body.clone(), agent("b"), MsgId::new()))
                    .await
                    .unwrap();
                let mut live = batch.clone();
                live.qos = ActorEventQos::Ephemeral;
                live.events = vec![json!({"type":"token","content":"wire order"})];
                worker
                    .publish_event_batch("a", &received.id, live)
                    .await
                    .unwrap();
            }
            worker
                .deliver("a", msg(body, agent("b"), received.id.clone()))
                .await
                .unwrap();
            if case == "order" {
                worker
                    .deliver(
                        "a",
                        InboxMessage {
                            id: MsgId::new(),
                            from: agent("b"),
                            kind: InboxKind::Outcome,
                            body: json!({"status":"completed","result":"done","error":null}),
                            created_at: Utc::now(),
                            correlation_id: Some(received.id),
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        let next = tokio::time::timeout(Duration::from_secs(3), link.next_frame())
            .await
            .unwrap();
        if case == "order" {
            assert!(
                matches!(next.unwrap(),Some(ChildFrame::EventBatch{batch}) if batch.qos==ActorEventQos::Ephemeral)
            );
            assert!(
                matches!(link.next_frame().await.unwrap(),Some(ChildFrame::EventBatch{batch}) if batch.qos==ActorEventQos::Durable)
            );
            assert!(matches!(
                link.next_frame().await.unwrap(),
                Some(ChildFrame::Terminal { .. })
            ));
        } else {
            assert!(next.is_err(), "{case}");
        }
    }
    server.abort();
}
