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
    // Broker on loopback + an echo agent named "worker" on the bus.
    let (endpoint, _dir) = broker_with_echo_worker().await;

    // The in-server tool, as a root session would invoke it.
    let tool = AskAgentTool::new(endpoint, "tok");
    let ctx = ToolExecutionContext {
        session_id: Some("root-session"),
        tool_call_id: "tc1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
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
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
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

/// Start a broker + a single echo worker named `worker`; returns the ws endpoint.
async fn broker_with_echo_worker() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let core = Arc::new(BrokerCore::new(dir.path()));
    let server = Arc::new(BrokerServer::new(core, "tok"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let endpoint = format!("ws://{addr}");

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

    (endpoint, dir)
}

/// Many orchestrators ask the SAME worker concurrently — every ask must come
/// back with its OWN answer (no message loss, no cross-correlation), even though
/// the worker's serve loop processes them serially.
#[tokio::test]
async fn concurrent_asks_to_one_worker_are_each_answered_correctly() {
    let (endpoint, _dir) = broker_with_echo_worker().await;

    const M: usize = 16;
    let tool = Arc::new(AskAgentTool::new(endpoint, "tok"));

    let start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(M);
    for i in 0..M {
        let tool = tool.clone();
        handles.push(tokio::spawn(async move {
            // A distinct orchestrator identity per task, so their reply mailboxes
            // never collide.
            let sid = format!("root-{i}");
            let tc = format!("tc-{i}");
            let ctx = ToolExecutionContext {
                session_id: Some(&sid),
                tool_call_id: &tc,
                event_tx: None,
                available_tool_schemas: None,
                bypass_permissions: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            };
            let result = tool
                .execute_with_context(
                    serde_json::json!({
                        "target": "worker",
                        "question": format!("q-{i}"),
                        "mode": "query"
                    }),
                    ctx,
                )
                .await
                .expect("concurrent ask returns an answer");
            let v: serde_json::Value = serde_json::from_str(&result.result).expect("json result");
            (i, v["answer"].as_str().unwrap_or_default().to_string())
        }));
    }

    let mut answers = std::collections::HashMap::new();
    for h in handles {
        let (i, answer) = h.await.expect("ask task did not panic");
        answers.insert(i, answer);
    }
    let elapsed = start.elapsed();

    // Every orchestrator got exactly its own correlated echo — none lost, none crossed.
    assert_eq!(answers.len(), M, "all {M} concurrent asks were answered");
    for i in 0..M {
        assert_eq!(
            answers.get(&i).map(String::as_str),
            Some(format!("echo: q-{i}").as_str()),
            "ask {i} must receive its OWN answer (correct correlation, no cross-wiring)"
        );
    }
    eprintln!("#59 baseline: {M} concurrent asks answered in {elapsed:?}");
}
