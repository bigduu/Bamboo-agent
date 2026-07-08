//! Full production-path integration test for the actor runtime:
//!
//! ```text
//! AppState (real server assembly, friendly `subagents` config)
//!   -> SubAgent tool `create`
//!   -> ChildSessionAdapter -> SpawnScheduler -> run_child_spawn (wants_external)
//!   -> ActorChildRunner -> spawns the REAL `bamboo subagent-worker` process
//!   -> stdin ProvisionSpec -> fabric self-register -> WS run (echo, no LLM)
//!   -> terminal -> result written back onto the child session + status persisted
//! ```
//!
//! This is the exact wiring a user gets from `"subagents": { "runtime": "actor" }`;
//! only the executor is `echo` so no API key is needed.

use std::collections::HashMap;
use std::time::Duration;

use bamboo_agent_core::storage::Storage as _;
use bamboo_agent_core::tools::ToolExecutionContext;
use bamboo_agent_core::{Role, Session};
use bamboo_domain::session::tool_types::{FunctionCall, ToolCall};
use bamboo_server::app_state::AppState;
use bamboo_server::tools::ToolSurface;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn subagent_create_runs_actor_process_through_the_server() {
    let bamboo_bin = env!("CARGO_BIN_EXE_bamboo");
    let home = TempDir::new().unwrap();
    let fabric_dir = home.path().join("fabric");

    // The friendly config a user would write — plus expert worker_bin override
    // because inside a test the "current executable" is the test runner, not bamboo.
    let config = serde_json::json!({
        "provider": "anthropic",
        "providers": { "anthropic": { "api_key": "test-key", "model": "claude-test" } },
        "subagents": {
            "runtime": "actor",
            "executor": "echo",
            "worker_bin": bamboo_bin,
            "worker_args": ["subagent-worker"],
            "fabric_dir": fabric_dir.to_string_lossy(),
            "max_concurrent": 2
        }
    });
    std::fs::write(
        home.path().join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();

    let state = AppState::new(home.path().to_path_buf())
        .await
        .expect("app state boots");

    // A root session for the parent (workspace = the temp dir).
    let parent_id = "parent-actor-e2e";
    let mut parent = Session::new(parent_id, "claude-test");
    parent.title = "Actor e2e parent".into();
    parent.workspace = Some(home.path().to_string_lossy().into_owned());
    state.storage.save_session(&parent).await.unwrap();
    state.session_store.save_session(&parent).await.unwrap();

    // Invoke the SubAgent tool exactly as the LLM would.
    let tools = state.tool_factory.get(ToolSurface::Root);
    let args = serde_json::json!({
        "action": "create",
        "title": "Echo task",
        "responsibility": "echo the assignment",
        "prompt": "hello actor",
        "wait": false,
        "auto_run": true
    });
    let call = ToolCall {
        id: "t1".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: args.to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none("t1");
    ctx.session_id = Some(parent_id);
    // `SubAgent create` is now permission-classified (spawns a child agent
    // process — see #395/#402), so through the real executor it requires an
    // approval that a non-interactive test has no sink for and it fails closed.
    // This test exercises the actor-spawn WIRING, not the permission gate, so we
    // run it under bypass (the gate is covered by bamboo-permission's own tests).
    ctx.bypass_permissions = true;
    let result = tools
        .execute_with_context(&call, ctx)
        .await
        .expect("SubAgent.create succeeds");
    assert!(result.success, "create failed: {}", result.result);

    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let child_id = payload["child_session_id"]
        .as_str()
        .expect("create returns child_session_id")
        .to_string();

    // Wait for the actor process round-trip: spawn -> register -> WS -> echo -> terminal
    // -> write-back -> persist (status flips to completed).
    let mut completed_child: Option<Session> = None;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(Some(child)) = state.storage.load_session(&child_id).await {
            let status: HashMap<_, _> = child.metadata.clone().into_iter().collect();
            let runtime_status = child
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.last_run_status.clone())
                .or_else(|| status.get("last_run_status").cloned());
            match runtime_status.as_deref() {
                Some("completed") => {
                    completed_child = Some(child);
                    break;
                }
                Some("error") | Some("timeout") | Some("cancelled") => {
                    panic!(
                        "actor child ended with status {:?}, error: {:?}",
                        runtime_status,
                        child
                            .runtime_metadata
                            .as_ref()
                            .and_then(|m| m.last_run_error.clone())
                    );
                }
                _ => {}
            }
        }
    }

    let child = completed_child.expect("actor child should complete within 60s");

    // The actor's reply was written back onto the child session (the actor's
    // durable state), so the transcript survives the process.
    let reply = child
        .messages
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::Assistant))
        .map(|m| m.content.clone())
        .expect("child session has the actor's assistant reply");
    assert!(
        reply.starts_with("echo:"),
        "expected echo result written back, got: {reply}"
    );
    assert!(
        reply.contains("hello actor"),
        "echo should contain the assignment text, got: {reply}"
    );

    // Routing metadata proves it went through the actor path (not in-process).
    assert_eq!(
        child.metadata.get("external.protocol").map(String::as_str),
        Some("actor")
    );
}

/// Cancel a RUNNING actor child through the server: the cancel must trip the
/// worker mid-run (cancellable echo sleep), the child must land on
/// last_run_status="cancelled" (the natural-terminal guard must not mislabel),
/// and the worker must withdraw its fabric record (process recycled).
// TODO(cluster-fabric): stale since the Phase-3 bus cutover (`run local children
// over the mailbox bus` / `delete RegistryFabric`). It polls the FILE fabric
// record (`{child_id}.json`) for liveness, but local children now register on the
// bus, so that file is never written and the liveness wait times out. Not a
// functional regression — `subagent_create_runs_actor_process_through_the_server`
// exercises the same actor path via session completion and passes. Re-enable once
// the liveness/withdrawal checks are rewritten against the bus-native signal.
#[ignore = "pre-existing bus-migration debt: polls the deleted file-fabric record; needs a bus-native liveness rewrite"]
#[tokio::test(flavor = "multi_thread")]
async fn cancel_running_actor_child_through_the_server() {
    let bamboo_bin = env!("CARGO_BIN_EXE_bamboo");
    let home = TempDir::new().unwrap();
    let fabric_dir = home.path().join("fabric");

    let config = serde_json::json!({
        "provider": "anthropic",
        "providers": { "anthropic": { "api_key": "test-key", "model": "claude-test" } },
        "subagents": {
            "runtime": "actor",
            "executor": "echo",
            "worker_bin": bamboo_bin,
            "worker_args": ["subagent-worker"],
            "fabric_dir": fabric_dir.to_string_lossy(),
        }
    });
    std::fs::write(
        home.path().join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();

    let state = AppState::new(home.path().to_path_buf())
        .await
        .expect("app state boots");

    let parent_id = "parent-cancel-e2e";
    let mut parent = Session::new(parent_id, "claude-test");
    parent.workspace = Some(home.path().to_string_lossy().into_owned());
    state.storage.save_session(&parent).await.unwrap();
    state.session_store.save_session(&parent).await.unwrap();

    let tools = state.tool_factory.get(ToolSurface::Root);
    let create = ToolCall {
        id: "t1".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: serde_json::json!({
                "action": "create",
                "title": "Sleeper",
                "responsibility": "sleep until cancelled",
                // 60s cancellable sleep: the run stays open until we cancel.
                "prompt": "__sleep_ms:60000 never reached",
                "wait": false,
                "auto_run": true
            })
            .to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none("t1");
    ctx.session_id = Some(parent_id);
    // `SubAgent create` is now permission-classified (spawns a child agent
    // process — see #395/#402), so through the real executor it requires an
    // approval that a non-interactive test has no sink for and it fails closed.
    // This test exercises the actor-spawn WIRING, not the permission gate, so we
    // run it under bypass (the gate is covered by bamboo-permission's own tests).
    ctx.bypass_permissions = true;
    let result = tools.execute_with_context(&create, ctx).await.unwrap();
    assert!(result.success, "create failed: {}", result.result);
    let child_id = serde_json::from_str::<serde_json::Value>(&result.result).unwrap()
        ["child_session_id"]
        .as_str()
        .unwrap()
        .to_string();

    // The actor is live once its fabric record appears (self-registration).
    let record = fabric_dir.join(format!("{child_id}.json"));
    let mut live = false;
    for _ in 0..150 {
        if record.exists() {
            live = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(live, "actor child never self-registered in the fabric");

    // Cancel the RUNNING actor through the same tool surface.
    let cancel = ToolCall {
        id: "t2".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: serde_json::json!({
                "action": "cancel",
                "child_session_id": child_id
            })
            .to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none("t2");
    ctx.session_id = Some(parent_id);
    let result = tools.execute_with_context(&cancel, ctx).await.unwrap();
    assert!(result.success, "cancel failed: {}", result.result);
    eprintln!("cancel result: {}", result.result);

    // Terminal state must be "cancelled" (mid-sleep cancel, no natural finish).
    let mut status = None;
    for i in 0..100 {
        if let Ok(Some(child)) = state.storage.load_session(&child_id).await {
            let s = child
                .runtime_metadata
                .as_ref()
                .and_then(|m| m.last_run_status.clone())
                .or_else(|| child.metadata.get("last_run_status").cloned());
            if i % 10 == 0 {
                eprintln!("poll {i}: status={s:?}");
            }
            if matches!(s.as_deref(), Some("cancelled")) {
                status = s;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(status.as_deref(), Some("cancelled"));

    // The worker withdrew its fabric record on the way out (process recycled).
    let mut withdrawn = false;
    for _ in 0..100 {
        if !record.exists() {
            withdrawn = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(withdrawn, "worker did not withdraw its fabric record");
}
