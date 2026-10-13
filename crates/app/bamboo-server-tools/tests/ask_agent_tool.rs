//! The in-server `ask_agent` tool, end-to-end: a real broker + an echo agent on
//! the bus + the tool invoked as the agent loop would call it. Proves a running
//! (root) agent can command another broker-deployed agent and get its answer.

use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
use bamboo_broker::{serve_executor, BrokerCore, BrokerServer, DeployedAgent};
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, ActorPlacementClass, ActorPlacementRef, Message,
    Session,
};
use bamboo_server_tools::AskAgentTool;
use bamboo_server_tools::{registry_keys, Deployed, DeployedRegistry};
use bamboo_storage::SessionStoreV2;
use bamboo_subagent::{AgentRef, EchoExecutor};
use tokio::net::TcpListener;

#[tokio::test]
async fn ask_agent_tool_queries_a_broker_agent() {
    // Broker on loopback + an echo agent named "worker" on the bus.
    let (endpoint, _dir) = broker_with_echo_worker().await;

    // The in-server tool, as a root session would invoke it.
    let tool = AskAgentTool::new(endpoint, "tok");
    let ctx = ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some("root-session"),
        root_session_id: None,
        tool_call_id: "tc1",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let result = tool
        .invoke(
            serde_json::json!({
                "target": "worker",
                "question": "hi there",
                "mode": "query"
            }),
            ctx.to_tool_ctx(),
        )
        .await
        .map(|o| o.into_tool_result())
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
        executing_supervisor: None,
        session_id: Some("root-session"),
        root_session_id: None,
        tool_call_id: "tc2",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let err = tool
        .invoke(
            serde_json::json!({ "target": "w", "question": "q", "mode": "bogus" }),
            ctx.to_tool_ctx(),
        )
        .await
        .expect_err("unknown mode is rejected");
    assert!(format!("{err}").contains("mode"));
}

fn root_ctx<'a>(session_id: &'a str, tool_call_id: &'a str) -> ToolExecutionContext<'a> {
    ToolExecutionContext {
        executing_supervisor: None,
        session_id: Some(session_id),
        root_session_id: None,
        tool_call_id,
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
async fn existing_direct_child_query_reads_durable_history_and_steer_requires_messenger() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let mut root = Session::new("canonical-root", "echo-model");
    root.set_project_id_meta("project-a");
    store.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of("canonical-child", &root, "echo-model", "resident");
    child.set_project_id_meta("project-a");
    child.add_message(Message::user("Existing canonical child history"));
    store.save_session(&child).await.unwrap();
    // No deployment registry entry or reachable broker: the query must read
    // the same durable Child Session as SubAgent.
    let tool = AskAgentTool::new("ws://127.0.0.1:1", "tok")
        .with_deployments(Arc::default(), store.clone());
    let query = tool
        .invoke(
            serde_json::json!({"target": child.id, "question":"status", "mode":"query"}),
            root_ctx(&root.id, "query-child").to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    let query: serde_json::Value = serde_json::from_str(&query.result).unwrap();
    assert_eq!(query["from"], child.id);
    assert_eq!(query["snapshot"]["message_count"], 1);
    assert!(query.get("answer").is_none());

    let error = tool
        .invoke(
            serde_json::json!({"target": child.id, "question":"Focus only on the assigned parser", "mode":"steer"}),
            root_ctx(&root.id, "steer-child-1").to_tool_ctx(),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("SessionMessenger is unavailable"));
    assert_eq!(
        store
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .len(),
        1
    );
}

#[tokio::test]
async fn known_non_direct_or_wrong_project_actor_never_falls_back_to_broker() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let mut root = Session::new("root-a", "echo-model");
    root.set_project_id_meta("project-a");
    store.save_session(&root).await.unwrap();
    let mut other = Session::new("root-b", "echo-model");
    other.set_project_id_meta("project-b");
    store.save_session(&other).await.unwrap();
    let mut direct = Session::new_child_of("direct-child", &root, "echo-model", "resident");
    direct.set_project_id_meta("project-a");
    store.save_session(&direct).await.unwrap();
    let mut grandchild = Session::new_child_of("nested-child", &direct, "echo-model", "resident");
    grandchild.set_project_id_meta("project-a");
    store.save_session(&grandchild).await.unwrap();
    let mut foreign = Session::new_child_of("foreign-child", &other, "echo-model", "resident");
    foreign.set_project_id_meta("project-b");
    store.save_session(&foreign).await.unwrap();
    let tool = AskAgentTool::new("ws://127.0.0.1:1", "tok").with_deployments(Arc::default(), store);
    for target in [&root.id, &other.id, &grandchild.id, &foreign.id] {
        for mode in ["query", "steer"] {
            let error = tool
                .invoke(
                    serde_json::json!({"target":target, "question":"status", "mode":mode}),
                    root_ctx(&root.id, "forbidden-child").to_tool_ctx(),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("not an existing direct Child Actor"),
                "{target}: {error}"
            );
        }
    }
}

async fn host_bound_deployment(store: &Arc<SessionStoreV2>, registry: &DeployedRegistry) -> String {
    let mut root = Session::new("owner-root", "echo-model");
    root.set_project_id_meta("project-a");
    store.save_session(&root).await.unwrap();
    let mut other = Session::new("other-root", "echo-model");
    other.set_project_id_meta("project-b");
    store.save_session(&other).await.unwrap();
    let mut child = Session::new_child_of("actor-owned", &root, "echo-model", "resident");
    child.set_project_id_meta("project-a");
    store.save_session(&child).await.unwrap();
    let actor = store.ensure_actor(&child.id).await.unwrap().actor;
    let now = chrono::Utc::now();
    let activation = store
        .claim_activation(&ActorActivationClaim {
            actor_id: actor.actor_id.clone(),
            run_id: "resident-run".into(),
            lease_owner: "resident-host".into(),
            lease_expires_at: now + chrono::Duration::minutes(5),
            inbox_generation: 0,
            placement_ref: Some(ActorPlacementRef {
                class: ActorPlacementClass::Local,
                lease_id: "worker".into(),
                slot_epoch: None,
            }),
            now,
        })
        .await
        .unwrap();
    let fence = activation.fence();
    store.start_activation(&fence, now).await.unwrap();
    let child_process = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    registry.lock().await.insert(
        registry_keys::agent_key("friendly-alias"),
        Deployed {
            actor: Some(actor),
            activation: Some(fence),
            env: "local".into(),
            handle: DeployedAgent::from_parts("worker", child_process, None),
        },
    );
    child.id
}

#[tokio::test]
async fn host_bound_ask_uses_saved_child_or_live_alias_before_broker_send() {
    // A live broker peer named "worker" makes a raw-mailbox fallback observable.
    let (endpoint, _broker_dir) = broker_with_echo_worker().await;
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let registry: DeployedRegistry = Arc::default();
    let actor_id = host_bound_deployment(&store, &registry).await;
    let unbound_process = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    registry.lock().await.insert(
        registry_keys::agent_key("unbound-alias"),
        Deployed {
            actor: None,
            activation: None,
            env: "local".into(),
            handle: DeployedAgent::from_parts("worker", unbound_process, None),
        },
    );
    // Fabric still exposes a worker id to direct callers. It is a registered
    // compatibility route, unlike the unregistered "worker" broker peer.
    let fabric_endpoint = endpoint.clone();
    tokio::spawn(async move {
        let _ = serve_executor(
            &fabric_endpoint,
            AgentRef {
                session_id: "node-n1".into(),
                role: None,
            },
            "tok",
            Arc::new(EchoExecutor),
        )
        .await;
    });
    let fabric_process = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    registry.lock().await.insert(
        registry_keys::node_key("n1"),
        Deployed {
            actor: None,
            activation: None,
            env: "local".into(),
            handle: DeployedAgent::from_parts("node-n1", fabric_process, None),
        },
    );
    let tool = AskAgentTool::new(endpoint, "tok").with_deployments(registry.clone(), store.clone());

    for target in [&actor_id, "friendly-alias"] {
        let result = tool
            .invoke(
                serde_json::json!({"target": target, "question": "status", "mode": "query", "timeout_secs": 10}),
                root_ctx("owner-root", "owned-ask").to_tool_ctx(),
            )
            .await
            .unwrap()
            .into_tool_result();
        let value: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(value["from"], actor_id);
        assert_eq!(value["mode"], "query");
        assert_eq!(value["snapshot"]["message_count"], 0);
        assert!(value.get("answer").is_none());
    }
    let before = store.load_session(&actor_id).await.unwrap().unwrap();
    for (question, mode, reason) in [
        ("tell me about your private transcript", "query", "status"),
        (
            "inject this into the worker",
            "steer",
            "SessionMessenger is unavailable",
        ),
    ] {
        let error = tool
            .invoke(
                serde_json::json!({"target": actor_id, "question": question, "mode": mode}),
                root_ctx("owner-root", "no-private-conversation").to_tool_ctx(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains(reason));
    }
    let after = store.load_session(&actor_id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(after.messages).unwrap(),
        serde_json::to_value(before.messages).unwrap()
    );

    for target in ["worker", "unknown-peer", "unbound-alias"] {
        let error = tool
            .invoke(
                serde_json::json!({"target": target, "question": "must not route"}),
                root_ctx("owner-root", "raw-peer").to_tool_ctx(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not a live deployment"));
    }

    let fabric_result = tool
        .invoke(
            serde_json::json!({"target":"node-n1", "question":"legacy fabric query", "timeout_secs":10}),
            root_ctx("owner-root", "fabric-ask").to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    let fabric_value: serde_json::Value = serde_json::from_str(&fabric_result.result).unwrap();
    assert_eq!(fabric_value["from"], "node-n1");
    assert_eq!(fabric_value["answer"], "echo: legacy fabric query");
    let invalid_caller = tool
        .invoke(
            serde_json::json!({"target":"node-n1", "question":"no caller"}),
            root_ctx("missing-root", "fabric-unknown-caller").to_tool_ctx(),
        )
        .await
        .unwrap_err();
    assert!(invalid_caller.to_string().contains("saved Root"));

    for (target, reason) in [
        (actor_id.as_str(), "not an existing direct Child Actor"),
        ("friendly-alias", "identity or caller"),
    ] {
        let error = tool
            .invoke(
                serde_json::json!({"target": target, "question": "cross-project"}),
                root_ctx("other-root", "cross-project").to_tool_ctx(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains(reason));
    }

    // Reopening the durable store retains exact Child query authority, but it
    // does not restore the former worker mailbox or ephemeral deployment alias.
    let restarted_store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    assert!(restarted_store.inspect_actor(&actor_id).await.is_ok());
    let restarted_tool = AskAgentTool::new("ws://127.0.0.1:1", "tok")
        .with_deployments(Arc::default(), restarted_store);
    let direct = restarted_tool
        .invoke(
            serde_json::json!({"target": actor_id, "question": "status"}),
            root_ctx("owner-root", "after-restart").to_tool_ctx(),
        )
        .await
        .unwrap()
        .into_tool_result();
    let direct: serde_json::Value = serde_json::from_str(&direct.result).unwrap();
    assert_eq!(direct["snapshot"]["message_count"], 0);
    let error = restarted_tool
        .invoke(
            serde_json::json!({"target": "friendly-alias", "question": "stale"}),
            root_ctx("owner-root", "after-restart-alias").to_tool_ctx(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not a live deployment"));

    let fence = registry
        .lock()
        .await
        .get(&registry_keys::agent_key("friendly-alias"))
        .unwrap()
        .activation
        .clone()
        .unwrap();
    store
        .retire_actor_if_activation(&fence, chrono::Utc::now())
        .await
        .unwrap();
    let error = tool
        .invoke(
            serde_json::json!({"target":"friendly-alias", "question":"stale"}),
            root_ctx("owner-root", "retired-alias").to_tool_ctx(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity or caller"));

    registry.lock().await.remove(&registry_keys::node_key("n1"));
    let stale_fabric = tool
        .invoke(
            serde_json::json!({"target":"node-n1", "question":"stale node"}),
            root_ctx("owner-root", "stale-fabric").to_tool_ctx(),
        )
        .await
        .unwrap_err();
    assert!(stale_fabric.to_string().contains("not a live deployment"));
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
                executing_supervisor: None,
                session_id: Some(&sid),
                root_session_id: None,
                tool_call_id: &tc,
                event_tx: None,
                available_tool_schemas: None,
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            };
            let result = tool
                .invoke(
                    serde_json::json!({
                        "target": "worker",
                        "question": format!("q-{i}"),
                        "mode": "query"
                    }),
                    ctx.to_tool_ctx(),
                )
                .await
                .map(|o| o.into_tool_result())
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
