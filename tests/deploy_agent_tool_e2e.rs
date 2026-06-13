//! End-to-end of the AI-callable deploy flow: the `deploy_agent` tool spins up a
//! REAL broker-agent worker subprocess (via the bamboo binary), wired to a
//! broker, then the orchestrator asks the freshly-deployed worker over the bus.
//! Proves "bamboo deploys a worker itself, then commands it" — deterministic
//! (echo executor, no LLM). list + stop round out the lifecycle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
use bamboo_broker::{ask_agent, BrokerCore, BrokerServer};
use bamboo_server_tools::DeployAgentTool;
use bamboo_subagent::{AgentRef, AskMode};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const TOKEN: &str = "deploy-e2e";

fn ctx() -> ToolExecutionContext<'static> {
    ToolExecutionContext {
        session_id: Some("root"),
        tool_call_id: "tc",
        event_tx: None,
        available_tool_schemas: None,
    }
}

#[tokio::test]
async fn agent_deploys_a_worker_then_asks_lists_and_stops_it() {
    // Broker on loopback.
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    // The AI-callable deploy tool, wired to the REAL bamboo binary + this broker.
    // Holding `tool` keeps its registry (and the deployed handle) alive.
    let registry = Arc::new(Mutex::new(HashMap::new()));
    let tool = DeployAgentTool::new(
        endpoint.clone(),
        TOKEN,
        env!("CARGO_BIN_EXE_bamboo"),
        registry,
    );

    // 1. The agent deploys a worker (local subprocess, echo executor).
    let r = tool
        .execute_with_context(
            serde_json::json!({ "action": "deploy", "id": "w1", "env": "local", "echo": true }),
            ctx(),
        )
        .await
        .expect("deploy succeeds");
    let v: serde_json::Value = serde_json::from_str(&r.result).unwrap();
    assert_eq!(v["status"], "deployed");
    assert_eq!(v["id"], "w1");

    // 2. The just-deployed worker is reachable over the broker.
    let answer = ask_agent(
        &endpoint,
        AgentRef {
            session_id: "root".into(),
            role: None,
        },
        TOKEN,
        "w1",
        "hi there",
        AskMode::Query,
        Duration::from_secs(30),
    )
    .await
    .expect("deployed worker answers");
    assert_eq!(answer, "echo: hi there");

    // 3. list shows it.
    let r = tool
        .execute_with_context(serde_json::json!({ "action": "list" }), ctx())
        .await
        .expect("list succeeds");
    assert!(
        r.result.contains("w1"),
        "list should include w1: {}",
        r.result
    );

    // 4. stop tears it down.
    let r = tool
        .execute_with_context(serde_json::json!({ "action": "stop", "id": "w1" }), ctx())
        .await
        .expect("stop succeeds");
    let v: serde_json::Value = serde_json::from_str(&r.result).unwrap();
    assert_eq!(v["status"], "stopped");
}
