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
        executing_supervisor: None,
        session_id: Some("root"),
        root_session_id: None,
        tool_call_id: "tc",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
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
    let config = Arc::new(tokio::sync::RwLock::new(bamboo_config::Config::default()));
    let tool = DeployAgentTool::new(
        endpoint.clone(),
        TOKEN,
        env!("CARGO_BIN_EXE_bamboo"),
        registry,
        config,
    );

    // 1. The agent deploys a worker (local subprocess, echo executor).
    let r = tool
        .invoke(
            serde_json::json!({ "action": "deploy", "id": "w1", "env": "local", "echo": true }),
            ctx().to_tool_ctx(),
        )
        .await
        .map(|o| o.into_tool_result())
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
        .invoke(serde_json::json!({ "action": "list" }), ctx().to_tool_ctx())
        .await
        .map(|o| o.into_tool_result())
        .expect("list succeeds");
    assert!(
        r.result.contains("w1"),
        "list should include w1: {}",
        r.result
    );

    // 4. stop tears it down.
    let r = tool
        .invoke(
            serde_json::json!({ "action": "stop", "id": "w1" }),
            ctx().to_tool_ctx(),
        )
        .await
        .map(|o| o.into_tool_result())
        .expect("stop succeeds");
    let v: serde_json::Value = serde_json::from_str(&r.result).unwrap();
    assert_eq!(v["status"], "stopped");
}

/// Normal production tool path with the actual Host Store; no manual Actor claim.
#[tokio::test]
async fn deployment_uses_host_actor_identity_and_rejects_stale_or_foreign_controls() {
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort, ActorLogicalState, Session};
    use bamboo_server_tools::AskAgentTool;
    use bamboo_storage::SessionStoreV2;

    let dir = tempfile::tempdir().unwrap();
    let core = Arc::new(BrokerCore::new(dir.path().join("broker")));
    let server = Arc::new(BrokerServer::new(core, TOKEN));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let broker_task = tokio::spawn(async move { server.serve(listener).await });
    let home = dir.path().join("host");
    let store = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
    let mut root = Session::new("root", "echo-model");
    root.set_project_id_meta("resident-project");
    store.save_session(&root).await.unwrap();
    let registry = Arc::new(Mutex::new(HashMap::new()));
    let tool = DeployAgentTool::new(
        endpoint.clone(),
        TOKEN,
        env!("CARGO_BIN_EXE_bamboo"),
        registry.clone(),
        Arc::new(tokio::sync::RwLock::new(bamboo_config::Config::default())),
    )
    .with_actor_store(store.clone());
    let ask = AskAgentTool::new(endpoint, TOKEN).with_deployments(registry.clone(), store.clone());
    let deployed = tool
        .invoke(
            serde_json::json!({"action":"deploy", "id":"resident-alias", "echo":true}),
            ctx().to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    let output: serde_json::Value = serde_json::from_str(&deployed.result).unwrap();
    let actor_id = output["id"].as_str().unwrap().to_string();
    let worker_id = registry
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .handle
        .id
        .clone();
    assert!(actor_id.starts_with("actor-"));
    assert_ne!(actor_id, worker_id);
    assert_ne!(actor_id, "resident-alias");
    assert!(!deployed.result.contains(&worker_id));
    assert!(!deployed.result.contains(TOKEN));
    let canonical = store.load_session(&actor_id).await.unwrap().unwrap();
    assert_eq!(canonical.parent_session_id.as_deref(), Some("root"));
    assert_eq!(canonical.root_session_id, "root");
    assert_eq!(canonical.spawn_depth, 1);
    assert_eq!(
        canonical.project_id_meta().as_deref(),
        Some("resident-project")
    );
    assert_eq!(
        canonical.metadata.get("lifecycle").map(String::as_str),
        Some("resident")
    );
    let entry = store.inspect_actor(&actor_id).await.unwrap();
    assert_eq!(entry.actor.session_created_at, canonical.created_at);
    assert_eq!(entry.actor.state, ActorLogicalState::Active);
    assert_eq!(entry.actor.current_attempt, 1);
    assert_eq!(
        entry.activation.unwrap().status,
        ActorActivationStatus::Running
    );

    let answer = ask
        .invoke(
            serde_json::json!({"target":actor_id, "question":"identity query", "timeout_secs":30}),
            ctx().to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    let answer: serde_json::Value = serde_json::from_str(&answer.result).unwrap();
    assert_eq!(answer["from"], actor_id);
    assert_eq!(answer["answer"], "echo: identity query");
    let listed = tool
        .invoke(serde_json::json!({"action":"list"}), ctx().to_tool_ctx())
        .await
        .unwrap()
        .into_tool_result();
    assert!(listed.result.contains(&actor_id));
    assert!(!listed.result.contains(&worker_id));
    assert!(tool
        .invoke(
            serde_json::json!({"action":"deploy", "id":"resident-alias", "echo":true}),
            ctx().to_tool_ctx(),
        )
        .await
        .is_err());
    assert_eq!(registry.lock().await.len(), 1);
    let mut foreign = ctx();
    foreign.session_id = Some("foreign-root");
    for target in [actor_id.as_str(), worker_id.as_str(), "resident-alias"] {
        let error = ask
            .invoke(
                serde_json::json!({"target":target, "question":"must not dispatch"}),
                foreign.to_tool_ctx(),
            )
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("identity or caller"));
    }
    assert!(tool
        .invoke(
            serde_json::json!({"action":"stop", "id":actor_id}),
            foreign.to_tool_ctx(),
        )
        .await
        .is_err());
    assert_eq!(registry.lock().await.len(), 1);

    // Change only the saved Actor's birth witness: the real Store rejects it
    // before broker dispatch; restore exact bytes to permit a legitimate stop.
    let authority = store
        .sessions_root_dir()
        .join("root")
        .join("children")
        .join(&actor_id)
        .join("actor-authority.json");
    let original = std::fs::read(&authority).unwrap();
    let mut altered: serde_json::Value = serde_json::from_slice(&original).unwrap();
    altered["actor"]["session_created_at"] =
        serde_json::json!(canonical.created_at + chrono::Duration::seconds(1));
    std::fs::write(&authority, serde_json::to_vec(&altered).unwrap()).unwrap();
    let error = ask
        .invoke(
            serde_json::json!({"target":actor_id, "question":"stale birth must not dispatch"}),
            ctx().to_tool_ctx(),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("identity or caller"));
    let invalid_list = tool
        .invoke(serde_json::json!({"action":"list"}), ctx().to_tool_ctx())
        .await
        .unwrap()
        .into_tool_result();
    let invalid_list: serde_json::Value = serde_json::from_str(&invalid_list.result).unwrap();
    assert!(invalid_list["agents"].as_array().unwrap().is_empty());
    std::fs::write(&authority, &original).unwrap();

    let stopped = tool
        .invoke(
            serde_json::json!({"action":"stop", "id":actor_id}),
            ctx().to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    assert!(stopped.result.contains("stopped"));
    assert!(registry.lock().await.is_empty());
    for target in [actor_id.as_str(), "actor-unknown"] {
        assert!(ask
            .invoke(
                serde_json::json!({"target":target, "question":"stopped must not dispatch"}),
                ctx().to_tool_ctx(),
            )
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("not a live deployment"));
    }
    let reopened = SessionStoreV2::new(home).await.unwrap();
    let durable = reopened.load_session(&actor_id).await.unwrap().unwrap();
    assert_eq!(durable.created_at, canonical.created_at);
    assert_eq!(
        durable.project_id_meta().as_deref(),
        Some("resident-project")
    );
    assert_eq!(
        reopened.inspect_actor(&actor_id).await.unwrap().actor.state,
        ActorLogicalState::Retired
    );
    broker_task.abort();
}
