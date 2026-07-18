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

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_config::cluster_fabric::{Node, NodePlacement};
use bamboo_config::Config;

use crate::fabric_deploy::{FabricDeployer, FabricError};

pub struct ClusterTool {
    /// Read-only access for the inventory actions (list/describe/status).
    config: Arc<RwLock<Config>>,
    /// Shared deploy engine (one registry across HTTP + agent surfaces).
    deployer: Arc<FabricDeployer>,
}

impl ClusterTool {
    pub fn new(config: Arc<RwLock<Config>>, deployer: Arc<FabricDeployer>) -> Self {
        Self { config, deployer }
    }
}

/// Map a fabric engine error to a tool error.
fn to_tool_error(e: FabricError) -> ToolError {
    match e {
        FabricError::NotFound(m) | FabricError::BadRequest(m) => ToolError::InvalidArguments(m),
        FabricError::Internal(m) => ToolError::Execution(m),
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
        // Delegate to the shared engine (one registry across HTTP + agent), which
        // resolves stored creds server-side and persists NodeState.
        let state = self
            .deployer
            .deploy(node_id, echo)
            .await
            .map_err(to_tool_error)?;
        let worker_id = state.worker_id.clone().unwrap_or_default();
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
        self.deployer.stop(node_id).await.map_err(to_tool_error)?;
        Ok(tool_json(json!({ "node": node_id, "status": "stopped" })))
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
         PREFER LOCAL: default to a local `SubAgent` for delegation. Dispatch to a cluster node ONLY \
         when the task genuinely needs THAT machine (its data, GPU, network location). Deploying a \
         node uploads a binary and adds network latency — don't route here by default.\n\
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

    async fn invoke(&self, args: Value, _ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let parsed: ClusterArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid cluster args: {e}")))?;
        let result = match parsed {
            ClusterArgs::List => self.list().await,
            ClusterArgs::Describe { node } => self.describe(&node).await,
            ClusterArgs::Status { node } => self.status(&node).await,
            ClusterArgs::Deploy { node, echo } => self.deploy(&node, echo).await,
            ClusterArgs::Stop { node } => self.stop(&node).await,
        }?;
        Ok(ToolOutcome::Completed(result))
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

    fn deployer_for(config: Arc<RwLock<Config>>) -> Arc<crate::fabric_deploy::FabricDeployer> {
        Arc::new(crate::fabric_deploy::FabricDeployer::new(
            config,
            Arc::new(tokio::sync::Mutex::new(())),
            std::env::temp_dir(),
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            "/usr/bin/true",
        ))
    }

    fn tool(config: Arc<RwLock<Config>>) -> ClusterTool {
        let deployer = deployer_for(config.clone());
        ClusterTool::new(config, deployer)
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

    /// A throwaway in-process broker on a random port (token `"t"`).
    async fn start_broker() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(bamboo_broker::BrokerCore::new(dir.path()));
        let server = Arc::new(bamboo_broker::BrokerServer::new(core, "t"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    /// Join a worker to the bus so the post-deploy presence verify can see it
    /// (mailbox id == worker id, registered under `role`). Keep the handle alive.
    async fn join_worker(endpoint: &str, id: &str, role: &str) -> bamboo_broker::BrokerClient {
        let mut c = bamboo_broker::BrokerClient::connect(
            endpoint,
            bamboo_subagent::AgentRef {
                session_id: id.into(),
                role: Some(role.into()),
            },
            "t",
        )
        .await
        .unwrap();
        c.subscribe().await.unwrap();
        c
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
        // Exercises the register/stop plumbing through the tool. `/usr/bin/true`
        // stands in for the `bamboo` binary (spawned with broker-agent args it
        // ignores), so it drives the spawn/register path only. Deploy now runs a
        // post-deploy presence verify for NON-echo workers too, so we stand up a
        // real broker and join a matching worker (id "node-n1" under the resident
        // default role "general-purpose") to stand in for the process dialing home.
        let (endpoint, _broker_dir) = start_broker().await;
        let mut config = Config::default();
        config.cluster_fabric.nodes = vec![local_node("n1")];
        config.subagents_mut().broker = Some(bamboo_config::BrokerClientConfig {
            endpoint: endpoint.clone(),
            token: "t".into(),
            token_encrypted: None,
        });
        let cfg = Arc::new(RwLock::new(config));

        // The worker "dials home": a live bus client under the node's resolved role.
        let _worker = join_worker(&endpoint, "node-n1", "general-purpose").await;

        // Unique data dir so the persisted config.json doesn't collide with peers.
        let data_dir =
            std::env::temp_dir().join(format!("bamboo-clustertool-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&data_dir);
        let deployer = Arc::new(crate::fabric_deploy::FabricDeployer::new(
            cfg.clone(),
            Arc::new(tokio::sync::Mutex::new(())),
            &data_dir,
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            "/usr/bin/true",
        ));
        let registry = deployer.registry();
        let t = ClusterTool::new(cfg, deployer);

        let key = crate::registry_keys::node_key("n1");
        let out = parse(t.deploy("n1", false).await.unwrap());
        assert_eq!(out["worker_id"], "node-n1");
        assert_eq!(out["status"], "deployed");
        assert!(
            registry.lock().await.contains_key(&key),
            "handle registered"
        );

        let stopped = parse(t.stop("n1").await.unwrap());
        assert_eq!(stopped["status"], "stopped");
        assert!(!registry.lock().await.contains_key(&key), "handle removed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn deploy_unknown_node_errors() {
        let cfg = config_with(vec![], vec![]);
        let tool = tool(cfg);
        assert!(tool.deploy("nope", true).await.is_err());
    }
}
