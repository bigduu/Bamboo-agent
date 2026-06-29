//! `cluster` — the agent's read-only window into the operator-managed Remote
//! Cluster Fabric (RFC v2 §5, the progressive-disclosure ladder).
//!
//! Rung 0 (this tool's description) advertises the capability in the cached
//! prompt prefix. Rungs 1–2 are tool-pull: `list` (inventory), `describe` (one
//! node's capabilities), `status` (a node's live deploy state). Everything
//! volatile is returned by the CALL — nothing is injected into the prompt
//! prefix — so the 1h cache is never busted.
//!
//! It NEVER exposes credentials. Dispatch (deploying a worker onto a node,
//! driving it) is a separate concern: the operator deploys from the UI, and the
//! agent commands the resulting worker with `ask_agent` by the `worker_id` this
//! tool surfaces.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_broker::{AgentDeployment, ORCHESTRATOR_ID};
use bamboo_config::cluster_fabric::{Node, NodePlacement};
use bamboo_config::Config;

use crate::deploy_agent::{Deployed, DeployedRegistry};
use crate::fabric_deploy::{build_deployer, log_path_for, placement_env, worker_id_for};

pub struct ClusterTool {
    config: Arc<RwLock<Config>>,
    /// Broker the deployed worker dials home to (shared with `ask_agent`).
    broker_endpoint: String,
    broker_token: String,
    /// Local `bamboo` binary path (used for `placement = Local`).
    bamboo_bin: PathBuf,
    /// Shared with `deploy_agent` so `deploy_agent list/stop` see these workers.
    registry: DeployedRegistry,
}

impl ClusterTool {
    pub fn new(
        config: Arc<RwLock<Config>>,
        broker_endpoint: impl Into<String>,
        broker_token: impl Into<String>,
        bamboo_bin: impl Into<PathBuf>,
        registry: DeployedRegistry,
    ) -> Self {
        Self {
            config,
            broker_endpoint: broker_endpoint.into(),
            broker_token: broker_token.into(),
            bamboo_bin: bamboo_bin.into(),
            registry,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClusterArgs {
    /// Compact inventory of all nodes + clusters.
    List,
    /// One node's capabilities (model/role/workspace/placement/status).
    Describe { node: String },
    /// One node's live deploy state.
    Status { node: String },
    /// Deploy a worker onto a managed node (credentials resolved server-side).
    Deploy {
        node: String,
        #[serde(default)]
        echo: bool,
    },
    /// Stop a worker previously deployed onto a node.
    Stop { node: String },
}

/// A node's address line for display (NEVER includes credentials).
fn node_target(node: &Node) -> String {
    match &node.placement {
        NodePlacement::Local => "local".to_string(),
        NodePlacement::Ssh(t) => format!("{}@{}:{}", t.username, t.host, t.port),
    }
}

fn node_status(node: &Node) -> &'static str {
    match node.state.as_ref().map(|s| s.status) {
        Some(bamboo_config::cluster_fabric::NodeStatus::NotDeployed) | None => "not_deployed",
        Some(bamboo_config::cluster_fabric::NodeStatus::Deploying) => "deploying",
        Some(bamboo_config::cluster_fabric::NodeStatus::Running) => "running",
        Some(bamboo_config::cluster_fabric::NodeStatus::Unreachable) => "unreachable",
        Some(bamboo_config::cluster_fabric::NodeStatus::Stopped) => "stopped",
        Some(bamboo_config::cluster_fabric::NodeStatus::Failed) => "failed",
    }
}

/// Compact one-line-per-node summary for `list`.
fn node_brief(node: &Node, cluster: Option<&str>) -> Value {
    json!({
        "id": node.id,
        "label": node.label,
        "target": node_target(node),
        "status": node_status(node),
        "worker_id": node.state.as_ref().and_then(|s| s.worker_id.clone()),
        "cluster": cluster,
        "enabled": node.enabled,
    })
}

impl ClusterTool {
    async fn list(&self) -> Result<ToolResult, ToolError> {
        let cfg = self.config.read().await;
        let fabric = &cfg.cluster_fabric;

        // node id → first cluster name (for the brief).
        let cluster_of = |id: &str| -> Option<String> {
            fabric
                .clusters
                .iter()
                .find(|c| c.node_ids.iter().any(|n| n == id))
                .map(|c| c.name.clone())
        };

        let nodes: Vec<Value> = fabric
            .nodes
            .iter()
            .map(|n| node_brief(n, cluster_of(&n.id).as_deref()))
            .collect();
        let clusters: Vec<Value> = fabric
            .clusters
            .iter()
            .map(|c| {
                json!({
                    "name": c.name,
                    "description": c.description,
                    "node_ids": c.node_ids,
                })
            })
            .collect();

        Ok(tool_json(json!({
            "nodes": nodes,
            "clusters": clusters,
            "hint": "Use action=describe node=<id> for capabilities, then drive a running worker with ask_agent(target=<worker_id>, …).",
        })))
    }

    async fn describe(&self, node_id: &str) -> Result<ToolResult, ToolError> {
        let cfg = self.config.read().await;
        let node = cfg
            .cluster_fabric
            .node(node_id)
            .ok_or_else(|| ToolError::InvalidArguments(format!("unknown node '{node_id}'")))?;

        Ok(tool_json(json!({
            "id": node.id,
            "label": node.label,
            "target": node_target(node),
            "placement": match &node.placement {
                NodePlacement::Local => "local",
                NodePlacement::Ssh(_) => "ssh",
            },
            "trust_level": format!("{:?}", node.trust_level).to_lowercase(),
            "status": node_status(node),
            "worker_id": node.state.as_ref().and_then(|s| s.worker_id.clone()),
            "enabled": node.enabled,
            "role": node.deploy.default_role,
            "model": node.deploy.model,
            "workspace": node.deploy.workspace,
        })))
    }

    async fn status(&self, node_id: &str) -> Result<ToolResult, ToolError> {
        let cfg = self.config.read().await;
        let node = cfg
            .cluster_fabric
            .node(node_id)
            .ok_or_else(|| ToolError::InvalidArguments(format!("unknown node '{node_id}'")))?;
        Ok(tool_json(json!({
            "id": node.id,
            "status": node_status(node),
            "state": node.state,
        })))
    }

    async fn deploy(&self, node_id: &str, echo: bool) -> Result<ToolResult, ToolError> {
        // Snapshot the node (its SSH secrets are hydrated to plaintext in memory).
        let node: Node = {
            let cfg = self.config.read().await;
            cfg.cluster_fabric
                .node(node_id)
                .cloned()
                .ok_or_else(|| ToolError::InvalidArguments(format!("unknown node '{node_id}'")))?
        };
        if !node.enabled {
            return Err(ToolError::InvalidArguments(format!(
                "node '{node_id}' is disabled"
            )));
        }

        let worker_id = worker_id_for(&node);
        let build = build_deployer(&node, &self.bamboo_bin)
            .map_err(|e| ToolError::Execution(format!("cannot deploy node '{node_id}': {e}")))?;

        let deployment = AgentDeployment {
            id: worker_id.clone(),
            role: node.deploy.default_role.clone(),
            broker_endpoint: self.broker_endpoint.clone(),
            token: self.broker_token.clone(),
            model: node.deploy.model.clone(),
            workspace: node.deploy.workspace.clone(),
            echo,
            mcp_proxy: Some(ORCHESTRATOR_ID.to_string()),
            log_path: Some(log_path_for(&node)),
        };

        // Release any prior worker for this node first (frees its tunnel port).
        if let Some(prev) = self.registry.lock().await.remove(node_id) {
            prev.handle.shutdown().await;
        }
        let handle = build
            .deployer
            .deploy(&deployment)
            .await
            .map_err(|e| ToolError::Execution(format!("deploy node '{node_id}' failed: {e}")))?;
        self.registry.lock().await.insert(
            node_id.to_string(),
            Deployed {
                env: placement_env(&node).to_string(),
                handle,
            },
        );
        // Audit (no secrets): agent-initiated dispatch onto a managed node.
        tracing::info!(
            audit = "cluster_fabric.agent_deploy",
            node = node_id,
            placement = placement_env(&node),
            worker_id = %worker_id,
            echo,
            outcome = "deployed",
        );

        Ok(tool_json(json!({
            "node": node_id,
            "worker_id": worker_id,
            "status": "deployed",
            "note": format!(
                "worker '{worker_id}' is dialing the broker; command it with ask_agent(target=\"{worker_id}\", …)."
            ),
        })))
    }

    async fn stop(&self, node_id: &str) -> Result<ToolResult, ToolError> {
        let removed = self.registry.lock().await.remove(node_id);
        match removed {
            Some(d) => {
                d.handle.shutdown().await;
                Ok(tool_json(json!({ "node": node_id, "status": "stopped" })))
            }
            None => Ok(tool_json(json!({ "node": node_id, "status": "not_running" }))),
        }
    }
}

fn tool_json(value: Value) -> ToolResult {
    ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: None,
        images: Vec::new(),
    }
}

#[async_trait]
impl Tool for ClusterTool {
    fn name(&self) -> &str {
        "cluster"
    }

    fn description(&self) -> &str {
        "Inspect your operator-managed remote clusters: machines (\"nodes\") grouped into clusters \
         that you can run work on. Use this to DISCOVER what compute you have, then dispatch to it.\n\
         \n\
         ACTIONS:\n\
         - action=list — compact inventory: every node's id, label, target (user@host or local), \
         status, its worker_id if deployed, and cluster membership. Start here.\n\
         - action=describe node=<id> — one node's capabilities: placement, role, model, workspace, \
         status, worker_id.\n\
         - action=status node=<id> — one node's live deploy state (deployed_at, pid, last error).\n\
         - action=deploy node=<id> [echo=true] — deploy a worker onto the node (credentials are \
         resolved by the backend; you never see them). Returns a worker_id. Use echo=true for a \
         no-LLM connectivity smoke test.\n\
         - action=stop node=<id> — stop the worker you deployed on that node.\n\
         \n\
         DISPATCH: after deploy (or for an already-running node), command its worker_id with \
         ask_agent(target=<worker_id>, question=…, mode=query|steer). For PARALLEL work: list the \
         cluster, deploy to several nodes, then ask_agent each and gather. You address nodes by id \
         and never handle credentials."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["list", "describe", "status", "deploy", "stop"] },
                "node": { "type": "string", "description": "node id (required for describe/status/deploy/stop)." },
                "echo": { "type": "boolean", "description": "deploy: run the no-LLM echo executor (connectivity smoke)." }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: Value,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ClusterArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid cluster args: {e}")))?;
        match parsed {
            ClusterArgs::List => self.list().await,
            ClusterArgs::Describe { node } => self.describe(&node).await,
            ClusterArgs::Status { node } => self.status(&node).await,
            ClusterArgs::Deploy { node, echo } => self.deploy(&node, echo).await,
            ClusterArgs::Stop { node } => self.stop(&node).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::cluster_fabric::{
        Cluster, DeployProfile, NodeState, NodeStatus, SshAuth, SshTarget, TrustLevel,
    };

    fn config_with(nodes: Vec<Node>, clusters: Vec<Cluster>) -> Arc<RwLock<Config>> {
        let mut cfg = Config::default();
        cfg.cluster_fabric.nodes = nodes;
        cfg.cluster_fabric.clusters = clusters;
        Arc::new(RwLock::new(cfg))
    }

    fn tool(config: Arc<RwLock<Config>>) -> ClusterTool {
        ClusterTool::new(
            config,
            "ws://127.0.0.1:9600",
            "tok",
            "/bin/true",
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        )
    }

    fn ssh_node(id: &str, running: bool) -> Node {
        Node {
            id: id.to_string(),
            label: format!("label-{id}"),
            placement: NodePlacement::Ssh(SshTarget {
                host: "10.0.0.9".into(),
                port: 22,
                username: "deploy".into(),
                auth: SshAuth::Password {
                    password: "SECRET".into(),
                    password_encrypted: None,
                },
                host_key_fingerprint: None,
            }),
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile {
                default_role: Some("worker".into()),
                ..Default::default()
            },
            state: running.then(|| NodeState {
                status: NodeStatus::Running,
                worker_id: Some(format!("node-{id}")),
                ..Default::default()
            }),
            enabled: true,
        }
    }

    fn parse(r: ToolResult) -> Value {
        serde_json::from_str(&r.result).unwrap()
    }

    #[tokio::test]
    async fn list_summarizes_nodes_without_secrets() {
        let cfg = config_with(
            vec![ssh_node("n1", true)],
            vec![Cluster {
                name: "prod".into(),
                description: None,
                node_ids: vec!["n1".into()],
            }],
        );
        let tool = tool(cfg);
        let out = parse(tool.list().await.unwrap());
        let node = &out["nodes"][0];
        assert_eq!(node["target"], "deploy@10.0.0.9:22");
        assert_eq!(node["status"], "running");
        assert_eq!(node["worker_id"], "node-n1");
        assert_eq!(node["cluster"], "prod");
        // No credential material anywhere in the serialized output.
        assert!(!out.to_string().contains("SECRET"));
        assert!(!out.to_string().contains("password"));
    }

    #[tokio::test]
    async fn describe_exposes_capabilities_not_creds() {
        let cfg = config_with(vec![ssh_node("n1", true)], vec![]);
        let tool = tool(cfg);
        let out = parse(tool.describe("n1").await.unwrap());
        assert_eq!(out["placement"], "ssh");
        assert_eq!(out["role"], "worker");
        assert_eq!(out["worker_id"], "node-n1");
        assert!(!out.to_string().contains("SECRET"));
    }

    #[tokio::test]
    async fn describe_unknown_node_errors() {
        let cfg = config_with(vec![], vec![]);
        let tool = tool(cfg);
        assert!(tool.describe("nope").await.is_err());
    }

    #[tokio::test]
    async fn status_reports_not_deployed_for_fresh_node() {
        let cfg = config_with(vec![ssh_node("n1", false)], vec![]);
        let tool = tool(cfg);
        let out = parse(tool.status("n1").await.unwrap());
        assert_eq!(out["status"], "not_deployed");
    }

    fn local_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            label: id.to_string(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn deploy_local_node_registers_worker_then_stop_clears_it() {
        // Use a harmless binary as "bamboo": LocalProcessDeployer spawns it with
        // broker-agent args (ignored by /bin/true), exercising deploy/register/stop.
        let cfg = config_with(vec![local_node("n1")], vec![]);
        let registry: DeployedRegistry =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let t = ClusterTool::new(
            cfg,
            "ws://127.0.0.1:9600",
            "tok",
            "/usr/bin/true",
            registry.clone(),
        );

        let out = parse(t.deploy("n1", true).await.unwrap());
        assert_eq!(out["worker_id"], "node-n1");
        assert_eq!(out["status"], "deployed");
        assert!(registry.lock().await.contains_key("n1"), "handle registered");

        let stopped = parse(t.stop("n1").await.unwrap());
        assert_eq!(stopped["status"], "stopped");
        assert!(!registry.lock().await.contains_key("n1"), "handle removed");
    }

    #[tokio::test]
    async fn deploy_unknown_node_errors() {
        let cfg = config_with(vec![], vec![]);
        let tool = tool(cfg);
        assert!(tool.deploy("nope", true).await.is_err());
    }
}
