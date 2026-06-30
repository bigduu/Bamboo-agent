//! Remote Cluster Fabric deploy engine.
//!
//! [`FabricDeployer`] is the SINGLE orchestration path — `deploy` / `stop` /
//! `test` / `read_logs` — shared by the operator HTTP handlers and the agent
//! [`crate::cluster_tool::ClusterTool`]. Both hold the same `Arc<FabricDeployer>`
//! so they share ONE worker registry (stop from either side sees the same
//! workers) and one persistence path. Living here (the one crate that sees both
//! `bamboo-config`'s `Node` and `bamboo-broker`'s deployers) keeps placement/
//! auth handling in one place.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use bamboo_broker::{
    AgentDeployment, Deployer, LocalProcessDeployer, RusshAuth, RusshDeployer, SshDeployer,
    UploadSpec, ORCHESTRATOR_ID,
};
use bamboo_config::cluster_fabric::{Node, NodePlacement, NodeState, NodeStatus, SshAuth, SshTarget};
use bamboo_config::Config;

use crate::deploy_agent::{Deployed, DeployedRegistry};

/// Typed error so callers (HTTP / agent tool) can map to the right status/kind.
#[derive(Debug)]
pub enum FabricError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for FabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FabricError::NotFound(m) | FabricError::BadRequest(m) | FabricError::Internal(m) => {
                write!(f, "{m}")
            }
        }
    }
}

type FabricResult<T> = Result<T, FabricError>;

/// The shared fabric deploy engine: turns persisted nodes into running
/// `broker-agent` workers, holding their handles + persisting `NodeState`.
pub struct FabricDeployer {
    config: Arc<RwLock<Config>>,
    /// Serializes the mutate+persist of a fabric config write (same guarantee as
    /// `AppState::update_config`'s io-lock — #126).
    config_io_lock: Arc<Mutex<()>>,
    data_dir: PathBuf,
    /// Worker handles, keyed by node id — SHARED with `deploy_agent` so both
    /// surfaces see/manage the same workers.
    registry: DeployedRegistry,
    bamboo_bin: PathBuf,
}

impl FabricDeployer {
    pub fn new(
        config: Arc<RwLock<Config>>,
        config_io_lock: Arc<Mutex<()>>,
        data_dir: impl Into<PathBuf>,
        registry: DeployedRegistry,
        bamboo_bin: impl Into<PathBuf>,
    ) -> Self {
        Self {
            config,
            config_io_lock,
            data_dir: data_dir.into(),
            registry,
            bamboo_bin: bamboo_bin.into(),
        }
    }

    /// The shared worker registry (so `deploy_agent` can reuse it).
    pub fn registry(&self) -> DeployedRegistry {
        self.registry.clone()
    }

    fn node_snapshot(&self, cfg: &Config, node_id: &str) -> FabricResult<Node> {
        cfg.cluster_fabric
            .node(node_id)
            .cloned()
            .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))
    }

    /// Deploy a worker onto a node and persist its running state.
    ///
    /// `echo=true` runs the dependency-free echo executor (no LLM) — a
    /// connectivity smoke test.
    pub async fn deploy(&self, node_id: &str, echo: bool) -> FabricResult<NodeState> {
        let (node, broker) = {
            let cfg = self.config.read().await;
            (self.node_snapshot(&cfg, node_id)?, cfg.subagents.broker.clone())
        };
        if !node.enabled {
            return Err(FabricError::BadRequest(format!("Node '{node_id}' is disabled")));
        }
        let broker = broker.filter(|b| !b.endpoint.trim().is_empty()).ok_or_else(|| {
            FabricError::BadRequest(
                "No broker configured (subagents.broker) — a worker has nowhere to dial home"
                    .to_string(),
            )
        })?;

        let worker_id = worker_id_for(&node);
        let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
        let log_path = log_path_for(&node);

        let deployment = AgentDeployment {
            id: worker_id.clone(),
            role: node.deploy.default_role.clone(),
            broker_endpoint: broker.endpoint.clone(),
            token: broker.token.clone(),
            model: node.deploy.model.clone(),
            workspace: node.deploy.workspace.clone(),
            echo,
            mcp_proxy: Some(ORCHESTRATOR_ID.to_string()),
            log_path: Some(log_path.clone()),
            spec_json: None,
        };

        // Release any prior worker FIRST so its reverse tunnel frees the broker
        // port before the new deploy requests the same forward.
        if let Some(prev) = self.registry.lock().await.remove(&crate::registry_keys::node_key(node_id)) {
            prev.handle.shutdown().await;
        }

        let handle = match build.deployer.deploy(&deployment).await {
            Ok(h) => h,
            Err(e) => {
                let failed = NodeState {
                    status: NodeStatus::Failed,
                    last_error: Some(e.to_string()),
                    ..Default::default()
                };
                let _ = self.persist_state(node_id, Some(failed)).await;
                tracing::warn!(
                    audit = "cluster_fabric.deploy",
                    node = node_id,
                    placement = placement_env(&node),
                    outcome = "failed",
                    error = %e,
                );
                return Err(FabricError::Internal(format!("deploy node '{node_id}' failed: {e}")));
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

        self.registry.lock().await.insert(
            crate::registry_keys::node_key(node_id),
            Deployed {
                env: placement_env(&node).to_string(),
                handle,
            },
        );

        // TOFU: pin the observed host-key fingerprint if not already set.
        if let Some(cell) = build.observed_fp {
            if let Some(fp) = cell.lock().await.clone() {
                self.pin_fingerprint_if_absent(node_id, &fp).await;
            }
        }

        let state = NodeState {
            status: NodeStatus::Running,
            worker_id: Some(worker_id),
            remote_pid: pid,
            log_path: Some(log_path),
            deployed_at: Some(chrono::Utc::now().to_rfc3339()),
            ..Default::default()
        };
        self.persist_state(node_id, Some(state.clone())).await?;
        Ok(state)
    }

    /// Stop a node's worker (if running) and persist the stopped state.
    pub async fn stop(&self, node_id: &str) -> FabricResult<NodeState> {
        {
            let cfg = self.config.read().await;
            self.node_snapshot(&cfg, node_id)?;
        }
        if let Some(d) = self.registry.lock().await.remove(&crate::registry_keys::node_key(node_id)) {
            d.handle.shutdown().await;
        }
        tracing::info!(audit = "cluster_fabric.stop", node = node_id, outcome = "stopped");
        let state = NodeState {
            status: NodeStatus::Stopped,
            ..Default::default()
        };
        self.persist_state(node_id, Some(state.clone())).await?;
        Ok(state)
    }

    /// Connectivity preflight: connect + auth + `uname`, WITHOUT deploying.
    pub async fn test(&self, node_id: &str) -> FabricResult<String> {
        let node = {
            let cfg = self.config.read().await;
            self.node_snapshot(&cfg, node_id)?
        };
        let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
        let result = build.deployer.preflight().await;
        tracing::info!(
            audit = "cluster_fabric.test",
            node = node_id,
            placement = placement_env(&node),
            outcome = if result.is_ok() { "ok" } else { "failed" },
        );
        result.map_err(|e| FabricError::Internal(format!("preflight failed: {e}")))
    }

    /// Tail the last `lines` lines of a node worker's log.
    pub async fn read_logs(&self, node_id: &str, lines: usize) -> FabricResult<String> {
        let node = {
            let cfg = self.config.read().await;
            self.node_snapshot(&cfg, node_id)?
        };
        let log_path = node
            .state
            .as_ref()
            .and_then(|s| s.log_path.clone())
            .unwrap_or_else(|| log_path_for(&node));
        let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
        build
            .deployer
            .tail_log(&log_path, lines)
            .await
            .map_err(|e| FabricError::Internal(format!("read logs failed: {e}")))
    }

    /// Persist `state` onto a node (engine-owned field): io-lock + atomic save,
    /// mirroring `AppState::update_config` minus the provider/MCP side effects.
    async fn persist_state(&self, node_id: &str, state: Option<NodeState>) -> FabricResult<()> {
        let _io = self.config_io_lock.lock().await;
        let snapshot = {
            let mut cfg = self.config.write().await;
            let node = cfg
                .cluster_fabric
                .node_mut(node_id)
                .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
            node.state = state;
            cfg.clone()
        };
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || snapshot.save_to_dir(data_dir))
            .await
            .map_err(|e| FabricError::Internal(format!("persist task: {e}")))?
            .map_err(|e| FabricError::Internal(format!("save config: {e}")))?;
        Ok(())
    }

    /// Pin `fp` onto a node's SSH target if it has no fingerprint yet (TOFU).
    async fn pin_fingerprint_if_absent(&self, node_id: &str, fp: &str) {
        let _io = self.config_io_lock.lock().await;
        let snapshot = {
            let mut cfg = self.config.write().await;
            if let Some(node) = cfg.cluster_fabric.node_mut(node_id) {
                if let NodePlacement::Ssh(target) = &mut node.placement {
                    if target.host_key_fingerprint.is_some() {
                        return;
                    }
                    target.host_key_fingerprint = Some(fp.to_string());
                }
            }
            cfg.clone()
        };
        let data_dir = self.data_dir.clone();
        let _ = tokio::task::spawn_blocking(move || snapshot.save_to_dir(data_dir)).await;
    }
}

/// Shared handle to the russh TOFU-observed fingerprint cell (read after deploy).
pub type FingerprintCell = Arc<Mutex<Option<String>>>;

/// The chosen deployer + (russh only) the observed-fingerprint cell for pinning.
pub struct DeployerBuild {
    pub deployer: Box<dyn Deployer>,
    pub observed_fp: Option<FingerprintCell>,
}

/// The broker mailbox id for a node's worker (the `ask_agent` target).
pub fn worker_id_for(node: &Node) -> String {
    let short: String = node.id.chars().filter(|c| *c != '-').take(8).collect();
    format!("node-{short}")
}

/// Short label for which environment a node deploys into.
pub fn placement_env(node: &Node) -> &'static str {
    match &node.placement {
        NodePlacement::Local => "local",
        NodePlacement::Ssh(_) => "ssh",
    }
}

/// Where the worker writes its log: a LOCAL path under the bamboo data dir for
/// `Local` nodes, a REMOTE path under `remote_dir` for SSH nodes. Read back by
/// `Deployer::tail_log`.
pub fn log_path_for(node: &Node) -> String {
    let worker = worker_id_for(node);
    match &node.placement {
        NodePlacement::Local => bamboo_config::paths::resolve_bamboo_dir()
            .join("fabric-logs")
            .join(format!("{worker}.log"))
            .to_string_lossy()
            .into_owned(),
        NodePlacement::Ssh(_) => {
            let dir = node
                .deploy
                .remote_dir
                .clone()
                .unwrap_or_else(|| ".bamboo-deploy".to_string());
            format!("{dir}/{worker}.log")
        }
    }
}

/// Remote path to install an uploaded binary at: `<remote_dir>/bamboo[-<sha8>]`.
/// A relative `remote_dir` resolves to the remote home over scp/ssh/sftp.
pub fn remote_artifact_path(node: &Node) -> String {
    let dir = node
        .deploy
        .remote_dir
        .clone()
        .unwrap_or_else(|| ".bamboo-deploy".to_string());
    let name = node
        .deploy
        .artifact_sha256
        .as_deref()
        .filter(|h| h.len() >= 8)
        .map(|h| format!("bamboo-{}", &h[..8]))
        .unwrap_or_else(|| "bamboo".to_string());
    format!("{dir}/{name}")
}

/// Build the deployer for a node. `bamboo_bin` is the local `bamboo` path used
/// for `placement = Local`. Returns a human-readable error for misconfigured
/// nodes (missing secret, etc.).
pub fn build_deployer(node: &Node, bamboo_bin: &Path) -> Result<DeployerBuild, String> {
    match &node.placement {
        NodePlacement::Local => Ok(DeployerBuild {
            deployer: Box::new(LocalProcessDeployer::new(bamboo_bin.to_path_buf())),
            observed_fp: None,
        }),
        NodePlacement::Ssh(target) => match &target.auth {
            // "Use my ssh config" → system `ssh` (agent/config keys) + upload.
            SshAuth::SystemSshConfig => Ok(DeployerBuild {
                deployer: Box::new(build_system_ssh(node, target)),
                observed_fp: None,
            }),
            // Stored password / inline key → russh.
            SshAuth::Password { .. } | SshAuth::PrivateKey { .. } => {
                let russh = build_russh(node, target)?;
                let observed_fp = Some(russh.observed_cell());
                Ok(DeployerBuild {
                    deployer: Box::new(russh),
                    observed_fp,
                })
            }
        },
    }
}

fn build_system_ssh(node: &Node, target: &SshTarget) -> SshDeployer {
    let host = format!("{}@{}", target.username, target.host);
    let upload = node.deploy.artifact_path.as_ref().map(|local| UploadSpec {
        local_path: local.clone(),
        remote_path: remote_artifact_path(node),
    });
    SshDeployer::new(host)
        .with_port(Some(target.port))
        .with_upload(upload)
}

fn build_russh(node: &Node, target: &SshTarget) -> Result<RusshDeployer, String> {
    let auth = match &target.auth {
        SshAuth::Password { password, .. } => {
            if password.trim().is_empty() {
                return Err("node has no stored SSH password".to_string());
            }
            RusshAuth::Password(password.clone())
        }
        SshAuth::PrivateKey {
            private_key,
            private_key_path,
            passphrase,
            ..
        } => {
            let pem = if !private_key.trim().is_empty() {
                private_key.clone()
            } else if let Some(path) = private_key_path {
                std::fs::read_to_string(path)
                    .map_err(|e| format!("read private key '{path}': {e}"))?
            } else {
                return Err("node has neither an inline private key nor a key path".to_string());
            };
            RusshAuth::PrivateKey {
                pem,
                passphrase: Some(passphrase.clone()).filter(|p| !p.trim().is_empty()),
            }
        }
        SshAuth::SystemSshConfig => {
            return Err("build_russh called for SystemSshConfig".to_string());
        }
    };

    let upload = node.deploy.artifact_path.as_ref().map(|local| UploadSpec {
        local_path: local.clone(),
        remote_path: remote_artifact_path(node),
    });

    Ok(
        RusshDeployer::new(target.host.clone(), target.port, target.username.clone(), auth)
            .with_fingerprint(target.host_key_fingerprint.clone())
            .with_upload(upload),
    )
}
