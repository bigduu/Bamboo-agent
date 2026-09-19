//! End-to-end: spawn a REAL worker subprocess that dials the mailbox bus and
//! drive it with `BrokerChildLink` — exactly what `ActorChildRunner` now does
//! for local children. Validates the actor+mailbox flip's integration:
//! `spawn_worker_on_bus` (no file-discovery) + the worker's bus-dial +
//! `serve_executor` over the bus + the parent-side link, over a live broker.

use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::core::BrokerCore;
use bamboo_broker::server::BrokerServer;
use bamboo_broker::{BrokerChildLink, BrokerLimits};
use bamboo_subagent::proto::{
    ActorEventQos, ChildFrame, LogicalSessionIdentity, ParentFrame, RunSpec, TerminalStatus,
};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec};
use bamboo_subagent::{spawn_worker_on_bus, AgentRef, BusEndpoint, ProvisionSpec};
use futures_util::future::join_all;
use tokio::net::TcpListener;

#[tokio::test]
async fn runner_style_child_run_over_the_bus_with_a_real_subprocess() {
    // 1. A live broker on loopback.
    let dir = tempfile::tempdir().unwrap();
    let core = Arc::new(BrokerCore::new(dir.path()));
    let token = "e2e-token";
    let server = Arc::new(BrokerServer::new(core, token));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    // 2. Provision a child to run on the bus (Echo executor, no LLM).
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "child-e2e".into(),
            parent_id: Some("parent-e2e".into()),
            project_key: None,
            role: "worker".into(),
            depth: 1,
        },
        ExecutorSpec::Echo,
        dir.path().join("fabric").to_string_lossy().into_owned(),
    );
    spec.bus = Some(BusEndpoint {
        endpoint: endpoint.clone(),
        token: token.into(),
    });

    // 3. Spawn the REAL worker subprocess (it dials the bus, no rendezvous wait).
    let worker_bin = std::path::Path::new(env!("CARGO_BIN_EXE_subagent_bus_demo"));
    let _spawned = spawn_worker_on_bus(worker_bin, &[], &spec)
        .await
        .expect("spawn worker on bus");

    // 4. Drive it exactly like ActorChildRunner does: BrokerChildLink + Run.
    let mut link = BrokerChildLink::connect(
        &endpoint,
        AgentRef {
            session_id: "p-child-e2e".into(),
            role: None,
        },
        token,
        "child-e2e",
    )
    .await
    .expect("connect child link");

    link.send(ParentFrame::Run(RunSpec {
        assignment: "ping pong".into(),
        logical_session: Some(LogicalSessionIdentity {
            session_id: "session-child-e2e".into(),
            parent_session_id: Some("session-parent-e2e".into()),
            root_session_id: "session-parent-e2e".into(),
        }),
        project_id: None,
        reasoning_effort: None,
        permission_policy: None,
        messages: vec![],
        activation_run_id: Some("activation-child-e2e".into()),
        execution_epoch: 7,
        initial_session_messages: Vec::new(),
        secrets: Default::default(),
    }))
    .await
    .expect("send run");

    // 5. Collect streamed events then the terminal outcome.
    let mut events = Vec::new();
    let mut batches = 0usize;
    let mut next_seq = 1u64;
    let mut terminal = None;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(15), link.next_frame())
            .await
            .expect("a frame arrives within 15s")
            .expect("link ok");
        match frame {
            Some(ChildFrame::Event { event }) => {
                panic!("modern execution unexpectedly used the legacy event wire: {event}")
            }
            Some(ChildFrame::EventBatch { batch }) => {
                batch.validate().expect("worker emitted a valid batch");
                assert_eq!(
                    batch.first_seq, next_seq,
                    "event sequence must be contiguous"
                );
                next_seq = batch.last_seq + 1;
                assert_eq!(batch.execution_epoch, 7);
                assert_eq!(batch.activation_id.as_deref(), Some("activation-child-e2e"));
                assert_eq!(batch.source_actor_id.as_deref(), Some("child-e2e"));
                assert_eq!(
                    batch
                        .logical_session
                        .as_ref()
                        .map(|identity| identity.session_id.as_str()),
                    Some("session-child-e2e")
                );
                batches += 1;
                events.extend(batch.events);
            }
            Some(ChildFrame::Terminal { status, result, .. }) => {
                terminal = Some((status, result));
                break;
            }
            Some(_) => {}
            None => break,
        }
    }

    assert!(
        batches >= 2,
        "token and durable completion boundaries should produce separate batches"
    );
    assert!(events
        .iter()
        .any(|event| { event["type"] == "token" && event["content"] == "ping " }));
    assert!(events
        .iter()
        .any(|event| { event["type"] == "token" && event["content"] == "pong " }));
    assert!(events.iter().any(|event| {
        event["type"] == "complete" && ActorEventQos::classify(event) == ActorEventQos::Durable
    }));
    let (status, result) = terminal.expect("a terminal frame");
    assert_eq!(status, TerminalStatus::Completed);
    assert_eq!(result.as_deref(), Some("echo: ping pong"));
}

/// Warm-pool reuse: ONE worker subprocess (one spawn) handles TWO sequential
/// child Runs delivered to its mailbox — exactly what the runner's pool does when
/// it parks a worker and a second interchangeable child reuses it. Each Run is
/// driven by its OWN BrokerChildLink (distinct parent id), and replies route back
/// per run. Proves a parked worker stays subscribed and serves run-after-run.
#[tokio::test]
async fn one_warm_worker_serves_two_sequential_runs() {
    let dir = tempfile::tempdir().unwrap();
    let core = Arc::new(BrokerCore::new(dir.path()));
    let token = "warm-token";
    let server = Arc::new(BrokerServer::new(core, token));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "warm-worker".into(),
            parent_id: None,
            project_key: None,
            role: "worker".into(),
            depth: 1,
        },
        ExecutorSpec::Echo,
        dir.path().join("fabric").to_string_lossy().into_owned(),
    );
    spec.bus = Some(BusEndpoint {
        endpoint: endpoint.clone(),
        token: token.into(),
    });

    // Spawn ONE worker; it dials the bus and subscribes to "warm-worker".
    let worker_bin = std::path::Path::new(env!("CARGO_BIN_EXE_subagent_bus_demo"));
    let _spawned = spawn_worker_on_bus(worker_bin, &[], &spec)
        .await
        .expect("spawn warm worker");

    // Drive two DIFFERENT child runs through the SAME worker mailbox, each via its
    // own link (as the pool's reusing children do).
    for (i, word) in [(0u8, "alpha"), (1, "beta")].iter().map(|(i, w)| (*i, *w)) {
        let mut link = BrokerChildLink::connect(
            &endpoint,
            AgentRef {
                session_id: format!("p-child-{i}"),
                role: None,
            },
            token,
            "warm-worker",
        )
        .await
        .expect("connect link");

        link.send(ParentFrame::Run(RunSpec {
            assignment: word.to_string(),
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
        .expect("send run");

        let mut legacy_events = 0usize;
        let mut modern_batches = 0usize;
        let result = loop {
            let frame = tokio::time::timeout(Duration::from_secs(15), link.next_frame())
                .await
                .expect("a frame within 15s")
                .expect("link ok");
            match frame {
                Some(ChildFrame::Event { .. }) => legacy_events += 1,
                Some(ChildFrame::EventBatch { .. }) => modern_batches += 1,
                Some(ChildFrame::Terminal { status, result, .. }) => {
                    assert_eq!(status, TerminalStatus::Completed);
                    break result;
                }
                _ => continue,
            }
        };
        assert_eq!(
            result.as_deref(),
            Some(format!("echo: {word}").as_str()),
            "run {i} ({word}) handled by the warm worker"
        );
        assert!(
            legacy_events >= 2,
            "epoch zero must retain the rolling-upgrade one-event wire"
        );
        assert_eq!(
            modern_batches, 0,
            "epoch zero must not surprise a legacy parent with EventBatch"
        );
    }
}

/// High-parallelism Cluster path: one real worker process multiplexes 200
/// logical child activations through its fixed inbound/control/event broker
/// connections. Every activation has its own parent connection and correlation
/// id, so this exercises the real WebSocket broker, Maildir boundaries, shared
/// actor uplinks, per-run demux, event batches, and terminal ordering together.
#[tokio::test]
async fn one_cluster_worker_multiplexes_200_concurrent_runs_end_to_end() {
    const RUNS: u64 = 200;

    let dir = tempfile::tempdir().unwrap();
    let core = Arc::new(BrokerCore::new(dir.path()));
    let token = "parallel-token";
    // 200 parent links + the worker's fixed inbound/control/event trio fit,
    // while the old per-run worker-uplink topology would deterministically hit
    // this ceiling. This turns the connection-count invariant into an E2E guard.
    let server = Arc::new(BrokerServer::with_limits(
        core,
        token,
        BrokerLimits {
            max_connections: RUNS as usize + 8,
            ..BrokerLimits::default()
        },
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    let worker_id = "cluster-worker-e2e";
    let mut provision = ProvisionSpec::new(
        ChildIdentity {
            child_id: worker_id.into(),
            parent_id: None,
            project_key: None,
            role: "cluster-worker".into(),
            depth: 1,
        },
        ExecutorSpec::Echo,
        dir.path().join("fabric").to_string_lossy().into_owned(),
    );
    provision.bus = Some(BusEndpoint {
        endpoint: endpoint.clone(),
        token: token.into(),
    });
    let worker_bin = std::path::Path::new(env!("CARGO_BIN_EXE_subagent_bus_demo"));
    let worker = spawn_worker_on_bus(worker_bin, &[], &provision)
        .await
        .expect("spawn shared cluster worker");

    // Establish every parent link before dispatching any work. Keeping all 200
    // sockets live simultaneously makes the connection-budget assertion
    // deterministic rather than relying on runs overlapping by timing alone.
    let links = tokio::time::timeout(
        Duration::from_secs(30),
        join_all((0..RUNS).map(|run| {
            let endpoint = endpoint.clone();
            async move {
                BrokerChildLink::connect(
                    &endpoint,
                    AgentRef {
                        session_id: format!("parallel-parent-{run}"),
                        role: None,
                    },
                    token,
                    worker_id,
                )
                .await
            }
        })),
    )
    .await
    .expect("200 parent links connect within 30 seconds")
    .into_iter()
    .map(|link| link.expect("parallel parent fits the fixed connection budget"))
    .collect::<Vec<_>>();
    assert_eq!(links.len(), RUNS as usize);

    let runs = links.into_iter().enumerate().map(|(run, mut link)| {
        let run = run as u64;
        async move {
            let session_id = format!("parallel-child-{run}");
            let activation_id = format!("parallel-activation-{run}");
            // 250ms × 200 would take at least 50s if the mailbox worker
            // serialized runs. The 30s aggregate deadline below therefore
            // proves actual overlapping execution, not merely 200 accepted
            // connections that eventually drain one by one.
            let assignment = format!("__sleep_ms:250 payload-{run}");
            let expected_result = format!("echo: payload-{run}");

            link.send(ParentFrame::Run(RunSpec {
                assignment,
                logical_session: Some(LogicalSessionIdentity {
                    session_id: session_id.clone(),
                    parent_session_id: Some("parallel-root".into()),
                    root_session_id: "parallel-root".into(),
                }),
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: Vec::new(),
                activation_run_id: Some(activation_id.clone()),
                execution_epoch: run + 1,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            }))
            .await
            .expect("send parallel run");

            let mut next_seq = 1u64;
            let mut saw_token = false;
            let mut saw_complete = false;
            loop {
                let frame = link
                    .next_frame()
                    .await
                    .expect("parallel child stream remains connected")
                    .expect("parallel child reaches terminal");
                match frame {
                    ChildFrame::Event { event } => {
                        panic!("epoch {} used legacy event wire: {event}", run + 1)
                    }
                    ChildFrame::EventBatch { batch } => {
                        batch.validate().expect("valid parallel actor batch");
                        assert_eq!(batch.first_seq, next_seq, "run {run} sequence gap");
                        next_seq = batch.last_seq + 1;
                        assert_eq!(batch.execution_epoch, run + 1);
                        assert_eq!(batch.activation_id.as_deref(), Some(activation_id.as_str()));
                        assert_eq!(batch.source_actor_id.as_deref(), Some(worker_id));
                        assert_eq!(
                            batch
                                .logical_session
                                .as_ref()
                                .map(|identity| identity.session_id.as_str()),
                            Some(session_id.as_str())
                        );
                        for event in batch.events {
                            saw_token |= event["type"] == "token"
                                && event["content"] == format!("payload-{run} ");
                            saw_complete |= event["type"] == "complete";
                        }
                    }
                    ChildFrame::Terminal { status, result, .. } => {
                        assert_eq!(status, TerminalStatus::Completed, "run {run}");
                        assert_eq!(
                            result.as_deref(),
                            Some(expected_result.as_str()),
                            "run {run}"
                        );
                        assert!(saw_token, "run {run} lost its token batch");
                        assert!(
                            saw_complete,
                            "run {run} terminal overtook its durable completion event"
                        );
                        break;
                    }
                    ChildFrame::ApprovalRequest { .. }
                    | ChildFrame::SessionMessageAdmitted { .. } => {
                        panic!("echo run {run} emitted an unexpected control frame")
                    }
                }
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(30), join_all(runs))
        .await
        .expect("200 real broker runs overlap and complete within 30 seconds");
    worker.kill().await;
}
