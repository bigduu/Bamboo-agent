//! Remote Cluster Fabric configuration: operator-managed nodes & clusters.
//!
//! A **node** is one machine (local or SSH-reachable) that bamboo can deploy a
//! `broker-agent` worker onto; a **cluster** is a named group of node ids used
//! for disclosure/grouping. This is the L1 *operator* data model (RFC v2 §3):
//! persistent, additive, back-compat (absent ⇒ empty).
//!
//! Secrets (SSH password / private key / passphrase) are encrypted at rest with
//! the same AES-256-GCM pattern as [`crate::config::EnvVarEntry`]: a plaintext
//! field hydrated in memory, an `*_encrypted` field on disk, plaintext cleared
//! before serialization. See [`Config::hydrate_cluster_fabric_from_encrypted`],
//! [`Config::refresh_cluster_fabric_encrypted`],
//! [`Config::sanitize_cluster_fabric_for_disk`].
//!
//! NOTE: the deploy engine (P2) is not wired here — `NodeState` is engine-owned
//! and stays `None` until a deploy runs. This module is purely the persisted
//! registry + its crypto.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Config;

/// The persisted cluster fabric: clusters (groups) + nodes (machines).
///
/// Additive and back-compat: an absent `cluster_fabric` key deserializes to the
/// empty default and never appears on disk (`skip_serializing_if`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterFabricConfig {
    /// Named groups of node ids (the disclosure/grouping unit).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<Cluster>,
    /// The registered machines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,
}

impl ClusterFabricConfig {
    /// True when there are no clusters and no nodes (the serialize-skip gate).
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty() && self.nodes.is_empty()
    }

    /// Look up a node by id.
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Mutable node lookup by id.
    pub fn node_mut(&mut self, id: &str) -> Option<&mut Node> {
        self.nodes.iter_mut().find(|n| n.id == id)
    }

    /// Look up a cluster by name.
    pub fn cluster(&self, name: &str) -> Option<&Cluster> {
        self.clusters.iter().find(|c| c.name == name)
    }
}

/// A named group of node ids. Clusters carry no credentials — they are pure
/// grouping for disclosure and operator organization.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cluster {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_ids: Vec<String>,
}

/// One machine: where it is (`placement`), how much we trust it (`trust_level`),
/// what to launch (`deploy`), and engine-owned live `state`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    /// Stable unique id (uuid). The `ask_agent`/dispatch handle the agent uses.
    pub id: String,
    /// Human label, e.g. "gpu-1".
    pub label: String,
    /// Local (localhost, no SSH) or Ssh (remote) — the ONLY local/remote diff.
    pub placement: NodePlacement,
    /// Default `Trusted` (own infra ⇒ ship creds). `Untrusted` ⇒ proxy-home (future).
    #[serde(default)]
    pub trust_level: TrustLevel,
    /// What to launch + which artifact to upload.
    #[serde(default)]
    pub deploy: DeployProfile,
    /// Engine-owned live state (status/worker/health). `None` until first deploy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<NodeState>,
    /// Operator on/off switch (a disabled node is hidden from dispatch).
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Where a node lives. `remote = local + {ssh connect, upload, reverse tunnel}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodePlacement {
    /// localhost → `LocalProcessDeployer`; no ssh/upload/tunnel.
    Local,
    /// remote → russh/system-ssh deployer + binary upload + reverse tunnel.
    Ssh(SshTarget),
}

impl Default for NodePlacement {
    fn default() -> Self {
        NodePlacement::Local
    }
}

/// Trust posture for credential handling (RFC v2 §7).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    /// Own infra: OK to sync provider/MCP creds to the node (default).
    #[default]
    Trusted,
    /// Future: proxy LLM/MCP calls home so no secret leaves the orchestrator.
    Untrusted,
}

/// SSH connection target for a remote node. Secrets live in `auth`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SshTarget {
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    pub username: String,
    pub auth: SshAuth,
    /// TOFU-pinned host key fingerprint; a changed key is rejected as MITM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_key_fingerprint: Option<String>,
}

fn default_ssh_port() -> u16 {
    22
}

/// How to authenticate the SSH connection. Secret material is encrypted at rest
/// (the `*_encrypted` fields); plaintext is hydrated in memory and cleared
/// before disk, exactly like [`crate::config::EnvVarEntry`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum SshAuth {
    /// Use the bamboo host's own ssh agent/config (→ system-ssh deployer). No
    /// stored secret — delegated to the host. This is the only auth method the
    /// existing system-`ssh` `SshDeployer` can serve.
    SystemSshConfig,
    /// Stored password.
    Password {
        /// Plaintext — hydrated in memory, empty on disk.
        #[serde(default)]
        password: String,
        /// Ciphertext on disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        password_encrypted: Option<String>,
    },
    /// Private key — either inline PEM (secret, encrypted) or an on-host path
    /// (not a secret). An optional passphrase is always a secret.
    PrivateKey {
        /// Inline PEM plaintext — hydrated in memory, empty on disk.
        #[serde(default)]
        private_key: String,
        /// Ciphertext of the inline PEM on disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_encrypted: Option<String>,
        /// Path to a key file on the bamboo host (NOT a secret).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_path: Option<String>,
        /// Optional key passphrase plaintext — hydrated in memory, empty on disk.
        #[serde(default)]
        passphrase: String,
        /// Ciphertext of the passphrase on disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        passphrase_encrypted: Option<String>,
    },
}

/// What to launch on the node + which artifact to upload (RFC v2 §6).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeployProfile {
    /// Local-on-bamboo-host binary path to SFTP-upload (correct remote arch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    /// Expected sha256 of the artifact (idempotent redeploy / integrity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_sha256: Option<String>,
    /// Remote install dir (default `~/.bamboo-deploy`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_dir: Option<String>,
    /// Role the broker-agent runs as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_role: Option<String>,
    /// Model override for the deployed worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Workspace override for the deployed worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

/// Engine-owned live state. Written by the deploy engine (P2), not the operator.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeState {
    pub status: NodeStatus,
    /// Broker mailbox id (the `ask_agent` target) once deployed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Name of the encrypted env var holding this node's broker token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    /// RFC3339 timestamps + last error, all optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_health: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Lifecycle status of a node's worker.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    #[default]
    NotDeployed,
    Deploying,
    Running,
    Unreachable,
    Stopped,
    Failed,
}

// ── Crypto: mirror the env-vars AES-256-GCM at-rest pattern ────────────────

impl Config {
    /// Decrypt SSH secrets into in-memory plaintext after loading config.
    ///
    /// Mirrors [`Config::hydrate_env_vars_from_encrypted`]: only fills a
    /// plaintext field that is currently empty, from its `*_encrypted`
    /// counterpart.
    pub fn hydrate_cluster_fabric_from_encrypted(&mut self) {
        for node in &mut self.cluster_fabric.nodes {
            let NodePlacement::Ssh(target) = &mut node.placement else {
                continue;
            };
            match &mut target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password {
                    password,
                    password_encrypted,
                } => {
                    hydrate_field(password, password_encrypted.as_deref(), &node.id, "password");
                }
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } => {
                    hydrate_field(
                        private_key,
                        private_key_encrypted.as_deref(),
                        &node.id,
                        "private_key",
                    );
                    hydrate_field(
                        passphrase,
                        passphrase_encrypted.as_deref(),
                        &node.id,
                        "passphrase",
                    );
                }
            }
        }
    }

    /// Re-encrypt SSH secrets from current in-memory plaintext before persisting.
    ///
    /// Mirrors [`Config::refresh_env_vars_encrypted`]: a non-empty plaintext is
    /// (re-)encrypted; an empty plaintext leaves any existing ciphertext intact
    /// (so a redacted round-trip where the client never re-sent the secret keeps
    /// it). To CLEAR a secret, the caller swaps the whole `auth` variant.
    pub fn refresh_cluster_fabric_encrypted(&mut self) -> Result<()> {
        for node in &mut self.cluster_fabric.nodes {
            let node_id = node.id.clone();
            let NodePlacement::Ssh(target) = &mut node.placement else {
                continue;
            };
            match &mut target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password {
                    password,
                    password_encrypted,
                } => {
                    refresh_field(password, password_encrypted, &node_id, "password")?;
                }
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } => {
                    refresh_field(private_key, private_key_encrypted, &node_id, "private_key")?;
                    refresh_field(passphrase, passphrase_encrypted, &node_id, "passphrase")?;
                }
            }
        }
        Ok(())
    }

    /// Clear plaintext SSH secrets before serialization to disk.
    pub fn sanitize_cluster_fabric_for_disk(&mut self) {
        for node in &mut self.cluster_fabric.nodes {
            let NodePlacement::Ssh(target) = &mut node.placement else {
                continue;
            };
            match &mut target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password { password, .. } => password.clear(),
                SshAuth::PrivateKey {
                    private_key,
                    passphrase,
                    ..
                } => {
                    private_key.clear();
                    passphrase.clear();
                }
            }
        }
    }
}

/// Decrypt `encrypted` into `plaintext` when `plaintext` is currently empty.
fn hydrate_field(plaintext: &mut String, encrypted: Option<&str>, node_id: &str, what: &str) {
    if !plaintext.trim().is_empty() {
        return;
    }
    let Some(encrypted) = encrypted else {
        return;
    };
    match crate::encryption::decrypt(encrypted) {
        Ok(value) => *plaintext = value,
        Err(e) => tracing::warn!("Failed to decrypt node '{node_id}' {what}: {e}"),
    }
}

/// (Re-)encrypt `plaintext` into `encrypted` when `plaintext` is non-empty.
/// An empty plaintext is left untouched so a redacted update keeps the secret.
fn refresh_field(
    plaintext: &str,
    encrypted: &mut Option<String>,
    node_id: &str,
    what: &str,
) -> Result<()> {
    if plaintext.trim().is_empty() {
        return Ok(());
    }
    *encrypted = Some(
        crate::encryption::encrypt(plaintext)
            .with_context(|| format!("Failed to encrypt node '{node_id}' {what}"))?,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ssh_node(id: &str, auth: SshAuth) -> Node {
        Node {
            id: id.to_string(),
            label: id.to_string(),
            placement: NodePlacement::Ssh(SshTarget {
                host: "10.0.0.1".to_string(),
                port: 22,
                username: "deploy".to_string(),
                auth,
                host_key_fingerprint: None,
            }),
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        }
    }

    #[test]
    fn empty_fabric_is_skipped_on_serialize() {
        let cfg = ClusterFabricConfig::default();
        assert!(cfg.is_empty());
    }

    #[test]
    fn password_secret_round_trips_through_encrypt_sanitize_hydrate() {
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "n1",
            SshAuth::Password {
                password: "hunter2".to_string(),
                password_encrypted: None,
            },
        ));

        // Persist path: encrypt then sanitize (what save_to_dir does).
        config.refresh_cluster_fabric_encrypted().unwrap();
        config.sanitize_cluster_fabric_for_disk();

        // After sanitize: plaintext gone, ciphertext present.
        let NodePlacement::Ssh(t) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected ssh");
        };
        let SshAuth::Password {
            password,
            password_encrypted,
        } = &t.auth
        else {
            panic!("expected password auth");
        };
        assert!(password.is_empty(), "plaintext must be cleared for disk");
        assert!(password_encrypted.is_some(), "ciphertext must be stored");

        // Load path: hydrate restores plaintext.
        config.hydrate_cluster_fabric_from_encrypted();
        let NodePlacement::Ssh(t) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected ssh");
        };
        let SshAuth::Password { password, .. } = &t.auth else {
            panic!("expected password auth");
        };
        assert_eq!(password, "hunter2", "plaintext restored on hydrate");
    }

    #[test]
    fn private_key_and_passphrase_round_trip() {
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "n2",
            SshAuth::PrivateKey {
                private_key: "-----BEGIN KEY-----xyz".to_string(),
                private_key_encrypted: None,
                private_key_path: None,
                passphrase: "pp".to_string(),
                passphrase_encrypted: None,
            },
        ));

        config.refresh_cluster_fabric_encrypted().unwrap();
        config.sanitize_cluster_fabric_for_disk();
        config.hydrate_cluster_fabric_from_encrypted();

        let NodePlacement::Ssh(t) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected ssh");
        };
        let SshAuth::PrivateKey {
            private_key,
            passphrase,
            ..
        } = &t.auth
        else {
            panic!("expected private key auth");
        };
        assert_eq!(private_key, "-----BEGIN KEY-----xyz");
        assert_eq!(passphrase, "pp");
    }

    #[test]
    fn empty_plaintext_keeps_existing_ciphertext_on_refresh() {
        // Simulates a redacted update: the client returns an empty secret, and
        // we must NOT wipe the stored ciphertext.
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "n3",
            SshAuth::Password {
                password: "secret".to_string(),
                password_encrypted: None,
            },
        ));
        config.refresh_cluster_fabric_encrypted().unwrap();
        config.sanitize_cluster_fabric_for_disk();

        // Now plaintext is empty (as if loaded but not re-hydrated); refresh again.
        config.refresh_cluster_fabric_encrypted().unwrap();
        config.hydrate_cluster_fabric_from_encrypted();

        let NodePlacement::Ssh(t) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected ssh");
        };
        let SshAuth::Password { password, .. } = &t.auth else {
            panic!("expected password auth");
        };
        assert_eq!(password, "secret", "ciphertext preserved across empty refresh");
    }

    #[test]
    fn local_node_has_no_secrets_to_touch() {
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(Node {
            id: "local".to_string(),
            label: "local".to_string(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        });
        // Should be a no-op, no panic.
        config.refresh_cluster_fabric_encrypted().unwrap();
        config.sanitize_cluster_fabric_for_disk();
        config.hydrate_cluster_fabric_from_encrypted();
        assert_eq!(config.cluster_fabric.nodes.len(), 1);
    }
}
