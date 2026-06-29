//! Shared "node → deployer" construction for the Remote Cluster Fabric.
//!
//! Both the operator HTTP path (`bamboo-server`) and the agent-initiated
//! dispatch tool ([`crate::cluster_tool::ClusterTool`]) turn a persisted
//! [`Node`] into a [`bamboo_broker::Deployer`]. Keeping that logic here (the one
//! crate that sees both `bamboo-config`'s `Node` and `bamboo-broker`'s
//! deployers) keeps the two callers in agreement on placement/auth handling.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use bamboo_broker::{
    Deployer, LocalProcessDeployer, RusshAuth, RusshDeployer, SshDeployer, UploadSpec,
};
use bamboo_config::cluster_fabric::{Node, NodePlacement, SshAuth, SshTarget};

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
