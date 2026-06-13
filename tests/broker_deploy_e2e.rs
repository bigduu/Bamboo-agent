//! End-to-end of the full deployable vertical: an in-process broker, a REAL
//! `bamboo broker-agent serve --echo` subprocess brought up by
//! `LocalProcessDeployer`, and an orchestrator asking it over the bus. Proves
//! binary + broker + deploy + ask (query & steer) wired together, deterministic
//! (echo executor, no LLM). The same path with DockerDeployer / SshDeployer
//! reaches an agent in a container or on a remote host.

use std::sync::Arc;
use std::time::Duration;

use bamboo_broker::{
    ask_agent, AgentDeployment, BrokerCore, BrokerServer, Deployer, LocalProcessDeployer,
};
use bamboo_subagent::{AgentRef, AskMode};
use tokio::net::TcpListener;

const TOKEN: &str = "e2e-token";

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
