//! End-to-end: spawn a REAL worker subprocess that dials the mailbox bus and
//! drive it with `BrokerChildLink` — exactly what `ActorChildRunner` now does
//! for local children. Validates the actor+mailbox flip's integration:
//! `spawn_worker_on_bus` (no file-discovery) + the worker's bus-dial +
//! `serve_executor` over the bus + the parent-side link, over a live broker.

use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::core::BrokerCore;
use bamboo_broker::server::BrokerServer;
use bamboo_broker::BrokerChildLink;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec};
use bamboo_subagent::{spawn_worker_on_bus, AgentRef, BusEndpoint, ProvisionSpec};
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
        reasoning_effort: None,
        permission_policy: None,
        messages: vec![],
    }))
    .await
    .expect("send run");

    // 5. Collect streamed events then the terminal outcome.
    let mut events = 0usize;
    let mut terminal = None;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(15), link.next_frame())
            .await
            .expect("a frame arrives within 15s")
            .expect("link ok");
        match frame {
            Some(ChildFrame::Event { .. }) => events += 1,
            Some(ChildFrame::Terminal { status, result, .. }) => {
                terminal = Some((status, result));
                break;
            }
            Some(_) => {}
            None => break,
        }
    }

    assert!(
        events >= 1,
        "expected streamed events from the subprocess, got {events}"
    );
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
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
        }))
        .await
        .expect("send run");

        let result = loop {
            let frame = tokio::time::timeout(Duration::from_secs(15), link.next_frame())
                .await
                .expect("a frame within 15s")
                .expect("link ok");
            match frame {
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
    }
}
