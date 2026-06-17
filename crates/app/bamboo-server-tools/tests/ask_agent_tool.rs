//! The in-server `ask_agent` tool, end-to-end: a real broker + an echo agent on
//! the bus + the tool invoked as the agent loop would call it. Proves a running
//! (root) agent can command another broker-deployed agent and get its answer.

use std::sync::Arc;

use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
use bamboo_broker::{serve_executor, BrokerCore, BrokerServer};
use bamboo_server_tools::AskAgentTool;
use bamboo_subagent::{AgentRef, EchoExecutor};
use tokio::net::TcpListener;

#[tokio::test]
async fn ask_agent_tool_queries_a_broker_agent() {
    // Broker on loopback.
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, "tok"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

    // An echo agent named "worker" on the bus.
    let ep = endpoint.clone();
    tokio::spawn(async move {
        let _ = serve_executor(
            &ep,
            AgentRef {
                session_id: "worker".into(),
                role: None,
            },
            "tok",
            Arc::new(EchoExecutor),
        )
        .await;
    });

    // The in-server tool, as a root session would invoke it.
    let tool = AskAgentTool::new(endpoint, "tok");
    let ctx = ToolExecutionContext {
        session_id: Some("root-session"),
        tool_call_id: "tc1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
    };
    let result = tool
        .execute_with_context(
            serde_json::json!({
                "target": "worker",
                "question": "hi there",
                "mode": "query"
            }),
            ctx,
        )
        .await
        .expect("ask_agent tool returns an answer");

    assert!(result.success);
    let v: serde_json::Value = serde_json::from_str(&result.result).expect("json result");
    assert_eq!(v["from"], "worker");
    assert_eq!(v["mode"], "query");
    assert_eq!(v["answer"], "echo: hi there");
}

#[tokio::test]
async fn ask_agent_tool_rejects_unknown_mode() {
    let tool = AskAgentTool::new("ws://127.0.0.1:1", "tok");
    let ctx = ToolExecutionContext {
        session_id: Some("root-session"),
        tool_call_id: "tc2",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
    };
    let err = tool
        .execute_with_context(
            serde_json::json!({ "target": "w", "question": "q", "mode": "bogus" }),
            ctx,
        )
        .await
        .expect_err("unknown mode is rejected");
    assert!(format!("{err}").contains("mode"));
}
