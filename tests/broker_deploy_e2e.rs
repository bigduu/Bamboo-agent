//! End-to-end of the full deployable vertical: an in-process broker, a REAL
//! `bamboo broker-agent serve --echo` subprocess brought up by
//! `LocalProcessDeployer`, and an orchestrator asking it over the bus. Proves
//! binary + broker + deploy + ask (query & steer) wired together, deterministic
//! (echo executor, no LLM). The same path with DockerDeployer / SshDeployer
//! reaches an agent in a container or on a remote host.

use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{
    ask_agent, AgentDeployment, BrokerCore, BrokerServer, Deployer, DockerDeployer,
    LocalProcessDeployer,
};
use bamboo_subagent::{AgentRef, AskMode};
use tokio::net::TcpListener;

const TOKEN: &str = "e2e-token";

/// Start an in-process broker on loopback; returns its ws endpoint + the
/// mailbox-root guard (hold it for the test's lifetime).
async fn start_broker() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (format!("ws://{addr}"), dir)
}

fn orchestrator() -> AgentRef {
    AgentRef {
        session_id: "orchestrator".into(),
        role: None,
    }
}

#[tokio::test]
async fn deploy_local_broker_agent_and_ask_it_query_and_steer() {
    // 1. Broker (the bus) in-process.
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    // 2. Deploy a REAL broker-agent subprocess (echo executor) at "worker".
    let deployer = LocalProcessDeployer::new(env!("CARGO_BIN_EXE_bamboo"));
    let agent = deployer
        .deploy(&AgentDeployment {
            id: "worker".into(),
            role: Some("echo".into()),
            broker_endpoint: endpoint.clone(),
            token: TOKEN.into(),
            model: None,
            workspace: None,
            echo: true,
            mcp_proxy: None,
        })
        .await
        .expect("deploy local broker-agent subprocess");

    // 3. Ask it (query). The Ask is durable, so even if it lands before the
    //    freshly-spawned agent subscribes, it is delivered once the agent is up —
    //    a generous timeout is all that's needed.
    let answer = ask_agent(
        &endpoint,
        orchestrator(),
        TOKEN,
        "worker",
        "remote hello",
        AskMode::Query,
        Duration::from_secs(30),
    )
    .await
    .expect("query answered by deployed agent");
    assert_eq!(answer, "echo: remote hello");

    // 4. Steer also round-trips (the agent advances its context; echo replies).
    let steered = ask_agent(
        &endpoint,
        orchestrator(),
        TOKEN,
        "worker",
        "now do this",
        AskMode::Steer,
        Duration::from_secs(30),
    )
    .await
    .expect("steer answered by deployed agent");
    assert_eq!(steered, "echo: now do this");

    agent.shutdown().await;
}

#[tokio::test]
async fn orchestrator_commands_two_deployed_agents_independently() {
    let (endpoint, _dir) = start_broker().await;
    let deployer = LocalProcessDeployer::new(env!("CARGO_BIN_EXE_bamboo"));

    // Deploy two independent agents on the same bus.
    let mut agents = Vec::new();
    for id in ["alpha", "beta"] {
        agents.push(
            deployer
                .deploy(&AgentDeployment {
                    id: id.into(),
                    role: None,
                    broker_endpoint: endpoint.clone(),
                    token: TOKEN.into(),
                    model: None,
                    workspace: None,
                    echo: true,
                    mcp_proxy: None,
                })
                .await
                .expect("deploy agent"),
        );
    }

    // The orchestrator commands each independently; answers are isolated/correct.
    let a = ask_agent(
        &endpoint,
        orchestrator(),
        TOKEN,
        "alpha",
        "to alpha",
        AskMode::Query,
        Duration::from_secs(30),
    )
    .await
    .expect("alpha answers");
    let b = ask_agent(
        &endpoint,
        orchestrator(),
        TOKEN,
        "beta",
        "to beta",
        AskMode::Query,
        Duration::from_secs(30),
    )
    .await
    .expect("beta answers");
    assert_eq!(a, "echo: to alpha");
    assert_eq!(b, "echo: to beta");

    for agent in agents {
        agent.shutdown().await;
    }
}

/// Live Docker e2e — opt-in. Set `BAMBOO_E2E_DOCKER_IMAGE` to a bamboo image
/// (one that contains a `bamboo` binary) to run it; otherwise it skips. Assumes
/// Linux `--network host` so the container can reach a `127.0.0.1` broker.
#[tokio::test]
async fn docker_deploy_gated() {
    let Some(image) = std::env::var("BAMBOO_E2E_DOCKER_IMAGE")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("skipping docker e2e: set BAMBOO_E2E_DOCKER_IMAGE=<bamboo image> to run it");
        return;
    };

    let (endpoint, _dir) = start_broker().await;
    let deployer = DockerDeployer::new(image).network("host");
    let agent = deployer
        .deploy(&AgentDeployment {
            id: "dockerworker".into(),
            role: None,
            broker_endpoint: endpoint.clone(),
            token: TOKEN.into(),
            model: None,
            workspace: None,
            echo: true,
            mcp_proxy: None,
        })
        .await
        .expect("docker run broker-agent");

    let answer = ask_agent(
        &endpoint,
        orchestrator(),
        TOKEN,
        "dockerworker",
        "from container",
        AskMode::Query,
        Duration::from_secs(60),
    )
    .await
    .expect("containerized agent answers");
    assert_eq!(answer, "echo: from container");

    agent.shutdown().await;
}
