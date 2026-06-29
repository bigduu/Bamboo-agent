//! Fabric deploy engine wiring (RFC v2 §6) — the server-side bridge from a
//! persisted [`Node`] to a running `broker-agent` worker.
//!
//! It reuses the existing push-broker machinery: it builds the right
//! [`bamboo_broker::Deployer`] for the node's placement, deploys a worker
//! pointed at the SAME broker the agent's `ask_agent` talks to (so any agent can
//! address it by `worker_id`), holds the kill-on-drop handle in a server-wide
//! registry, and persists the resulting [`NodeState`].
//!
//! P2.1 implements `placement = Local` (a localhost worker via
//! [`LocalProcessDeployer`]). SSH placements return a clear "not yet" error
//! until P2.2 (system-ssh + upload) and P2.3 (russh) land. The broker token is
//! the shared `subagents.broker` token (per-node tokens are a P4 hardening).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::Mutex;

use bamboo_broker::{AgentDeployment, ORCHESTRATOR_ID};
use bamboo_config::cluster_fabric::{NodePlacement, NodeState, NodeStatus};
use bamboo_server_tools::fabric_deploy::{
    build_deployer, log_path_for, placement_env, worker_id_for,
};
use bamboo_server_tools::{Deployed, DeployedRegistry};

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::error::AppError;

/// Server-wide registry of fabric-deployed worker handles, kept alive for the
/// process lifetime and keyed by node id. Distinct from the per-session
/// `deploy_agent` registry: fabric deployments are operator-driven and global.
fn fabric_registry() -> &'static DeployedRegistry {
    static REG: OnceLock<DeployedRegistry> = OnceLock::new();
    REG.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// Deploy a node's worker and persist its running state.
///
/// `echo=true` deploys the dependency-free echo executor (no LLM / provider
/// creds) — a connectivity smoke test, mirroring `deploy_agent`'s `echo` option.
pub async fn deploy_node(
    app_state: &AppState,
    node_id: &str,
    echo: bool,
) -> Result<NodeState, AppError> {
    let (node, broker) = {
        let cfg = app_state.config.read().await;
        let node = cfg
            .cluster_fabric
            .node(node_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("Node '{node_id}'")))?;
        (node, cfg.subagents.broker.clone())
    };

    if !node.enabled {
        return Err(AppError::BadRequest(format!(
            "Node '{node_id}' is disabled"
        )));
    }

    let broker = broker
        .filter(|b| !b.endpoint.trim().is_empty())
        .ok_or_else(|| {
            AppError::BadRequest(
                "No broker configured (subagents.broker) — a worker has nowhere to dial home"
                    .to_string(),
            )
        })?;

    let worker_id = worker_id_for(&node);
    let bamboo_bin = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));

    // Shared "node → deployer" construction (used by the agent `cluster` tool too).
    // `observed_fp` is the TOFU-observed host-key fingerprint (russh only); after
    // a successful first deploy it is pinned onto the node.
    let build = build_deployer(&node, &bamboo_bin).map_err(AppError::BadRequest)?;
    let observed_fp = build.observed_fp;
    let deployer = build.deployer;
    let log_path = log_path_for(&node);

    let deployment = AgentDeployment {
        id: worker_id.clone(),
        role: node.deploy.default_role.clone(),
        broker_endpoint: broker.endpoint.clone(),
        token: broker.token.clone(),
        model: node.deploy.model.clone(),
        workspace: node.deploy.workspace.clone(),
        echo,
        // Deployed workers proxy MCP to this orchestrator (single MCP host).
        mcp_proxy: Some(ORCHESTRATOR_ID.to_string()),
        log_path: Some(log_path.clone()),
    };

    // Release any prior deployment for this node FIRST, so its reverse tunnel
    // frees the broker port before the new deploy requests the same forward
    // (otherwise the redeploy's tcpip_forward collides and is rejected).
    if let Some(prev) = fabric_registry().lock().await.remove(node_id) {
        prev.handle.shutdown().await;
    }

    let deploy_result = deployer.deploy(&deployment).await;
    let handle = match deploy_result {
        Ok(h) => h,
        Err(e) => {
            // Record the failure on the node so the operator sees why.
            let failed = NodeState {
                status: NodeStatus::Failed,
                last_error: Some(e.to_string()),
                ..Default::default()
            };
            let _ = persist_state(app_state, node_id, Some(failed)).await;
            // Audit (no secrets): the deploy endpoints are authenticated RCE.
            tracing::warn!(
                audit = "cluster_fabric.deploy",
                node = node_id,
                placement = placement_env(&node),
                outcome = "failed",
                error = %e,
            );
            return Err(AppError::InternalError(anyhow::anyhow!(
                "deploy node '{node_id}' failed: {e}"
            )));
        }
    };
    let pid = handle.pid();
    tracing::info!(
        audit = "cluster_fabric.deploy",
        node = node_id,
        placement = placement_env(&node),
        worker_id = %worker_id,
        echo,
        outcome = "deployed",
    );

    fabric_registry().lock().await.insert(
        node_id.to_string(),
        Deployed {
            env: placement_env(&node).to_string(),
            handle,
        },
    );

    // TOFU: pin the observed host-key fingerprint on the node if not already set,
    // so a later key change is rejected as a MITM.
    if let Some(cell) = observed_fp {
        if let Some(fp) = cell.lock().await.clone() {
            pin_fingerprint_if_absent(app_state, node_id, &fp).await;
        }
    }

    let state = NodeState {
        status: NodeStatus::Running,
        worker_id: Some(worker_id),
        remote_pid: pid,
        log_path: Some(log_path),
        deployed_at: Some(now_rfc3339()),
        ..Default::default()
    };
    persist_state(app_state, node_id, Some(state.clone())).await?;
    Ok(state)
}

/// Tail a node's worker log (the last `lines` lines).
pub async fn read_logs(
    app_state: &AppState,
    node_id: &str,
    lines: usize,
) -> Result<String, AppError> {
    let node = {
        let cfg = app_state.config.read().await;
        cfg.cluster_fabric
            .node(node_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("Node '{node_id}'")))?
    };
    let log_path = node
        .state
        .as_ref()
        .and_then(|s| s.log_path.clone())
        .unwrap_or_else(|| log_path_for(&node));
    let bamboo_bin = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));
    let build = build_deployer(&node, &bamboo_bin).map_err(AppError::BadRequest)?;
    build
        .deployer
        .tail_log(&log_path, lines)
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("read logs failed: {e}")))
}

/// Pin `fp` onto the node's SSH target if it has no fingerprint yet (TOFU).
async fn pin_fingerprint_if_absent(app_state: &AppState, node_id: &str, fp: &str) {
    let node_id = node_id.to_string();
    let fp = fp.to_string();
    let _ = app_state
        .update_config(
            move |cfg| {
                if let Some(node) = cfg.cluster_fabric.node_mut(&node_id) {
                    if let NodePlacement::Ssh(target) = &mut node.placement {
                        if target.host_key_fingerprint.is_none() {
                            target.host_key_fingerprint = Some(fp.clone());
                        }
                    }
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await;
}

/// Connectivity preflight for a node: connect + auth + `uname` WITHOUT deploying.
pub async fn test_node(app_state: &AppState, node_id: &str) -> Result<String, AppError> {
    let node = {
        let cfg = app_state.config.read().await;
        cfg.cluster_fabric
            .node(node_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("Node '{node_id}'")))?
    };
    let bamboo_bin = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("bamboo"));
    let build = build_deployer(&node, &bamboo_bin).map_err(AppError::BadRequest)?;
    let result = build.deployer.preflight().await;
    tracing::info!(
        audit = "cluster_fabric.test",
        node = node_id,
        placement = placement_env(&node),
        outcome = if result.is_ok() { "ok" } else { "failed" },
    );
    result.map_err(|e| AppError::InternalError(anyhow::anyhow!("preflight failed: {e}")))
}

/// Stop a node's worker (if running) and persist the stopped state.
pub async fn stop_node(app_state: &AppState, node_id: &str) -> Result<NodeState, AppError> {
    // Verify the node exists (clear 404 rather than a silent no-op).
    {
        let cfg = app_state.config.read().await;
        if cfg.cluster_fabric.node(node_id).is_none() {
            return Err(AppError::NotFound(format!("Node '{node_id}'")));
        }
    }

    let was_running = fabric_registry().lock().await.remove(node_id);
    if let Some(d) = was_running {
        d.handle.shutdown().await;
    }
    tracing::info!(
        audit = "cluster_fabric.stop",
        node = node_id,
        outcome = "stopped",
    );

    let state = NodeState {
        status: NodeStatus::Stopped,
        ..Default::default()
    };
    persist_state(app_state, node_id, Some(state.clone())).await?;
    Ok(state)
}

/// Persist `state` onto the node in config (engine-owned field).
async fn persist_state(
    app_state: &AppState,
    node_id: &str,
    state: Option<NodeState>,
) -> Result<(), AppError> {
    let node_id = node_id.to_string();
    app_state
        .update_config(
            move |cfg| {
                let node = cfg
                    .cluster_fabric
                    .node_mut(&node_id)
                    .ok_or_else(|| AppError::NotFound(format!("Node '{node_id}'")))?;
                node.state = state.clone();
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;
    Ok(())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}
