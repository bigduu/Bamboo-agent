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

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::config_store::ConfigStoreResult;
use crate::credential_store::{credential_ref, CredentialRef};

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
    /// Stable references to SSH secrets, keyed by the node's immutable id.
    ///
    /// Runtime plaintext remains in [`SshAuth`], while ordinary configuration
    /// persists only these references and truthful configured metadata. The
    /// legacy `*_encrypted` fields remain readable during migration but are not
    /// the authority for newly isolated credentials.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credential_refs: BTreeMap<String, ClusterNodeCredentialRefs>,
    /// Seconds between background health probes of Running/Unreachable nodes.
    /// Omitted → the built-in default (30s); `0` disables the health monitor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_interval_secs: Option<u64>,
}

/// Default health-probe cadence when `health_interval_secs` is unset.
pub const DEFAULT_HEALTH_INTERVAL_SECS: u64 = 30;

impl ClusterFabricConfig {
    /// True when there are no clusters and no nodes (the serialize-skip gate).
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty() && self.nodes.is_empty() && self.credential_refs.is_empty()
    }

    /// Resolve the health-monitor cadence: `None` when disabled (`0`), else the
    /// configured seconds or the [`DEFAULT_HEALTH_INTERVAL_SECS`] default.
    pub fn health_interval(&self) -> Option<std::time::Duration> {
        match self.health_interval_secs {
            Some(0) => None,
            Some(s) => Some(std::time::Duration::from_secs(s)),
            None => Some(std::time::Duration::from_secs(DEFAULT_HEALTH_INTERVAL_SECS)),
        }
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

    /// Drop metadata for nodes that no longer exist. Credential refs remain an
    /// inert persistence seam until the cluster exact transaction is wired;
    /// callers must prune them whenever a node collection is replaced.
    pub fn prune_orphaned_credential_refs(&mut self) {
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        self.credential_refs
            .retain(|node_id, _| node_ids.contains(node_id.as_str()));
    }
}

/// Metadata for one node's isolated SSH credentials.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeCredentialRefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_credential_ref: Option<CredentialRef>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub password_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_credential_ref: Option<CredentialRef>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub private_key_configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passphrase_credential_ref: Option<CredentialRef>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub passphrase_configured: bool,
}

impl ClusterNodeCredentialRefs {
    pub fn references(&self) -> impl Iterator<Item = &CredentialRef> {
        [
            self.password_credential_ref.as_ref(),
            self.private_key_credential_ref.as_ref(),
            self.passphrase_credential_ref.as_ref(),
        ]
        .into_iter()
        .flatten()
    }

    pub fn is_empty(&self) -> bool {
        self.references().next().is_none()
            && !self.password_configured
            && !self.private_key_configured
            && !self.passphrase_configured
    }
}

/// Explicit operator intent for one isolated cluster credential.
///
/// Deliberately does not implement `Debug` or serialization: replacement
/// values are request-only secret material and must never enter ordinary
/// configuration documents, logs, events, or diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub enum ClusterCredentialAction {
    Keep,
    Replace(String),
    Clear,
}

/// Explicit password/private-key/passphrase actions for one node mutation.
///
/// Every HTTP mutation supplies all three actions, so an omitted field can
/// never ambiguously mean both "keep" and "clear".
#[derive(Clone, PartialEq, Eq)]
pub struct ClusterNodeCredentialIntents {
    pub password: ClusterCredentialAction,
    pub private_key: ClusterCredentialAction,
    pub passphrase: ClusterCredentialAction,
}

impl ClusterNodeCredentialIntents {
    pub fn clear_all() -> Self {
        Self {
            password: ClusterCredentialAction::Clear,
            private_key: ClusterCredentialAction::Clear,
            passphrase: ClusterCredentialAction::Clear,
        }
    }
}

pub fn cluster_password_credential_ref(node_id: &str) -> ConfigStoreResult<CredentialRef> {
    credential_ref("cluster", node_id, "password")
}

pub fn cluster_private_key_credential_ref(node_id: &str) -> ConfigStoreResult<CredentialRef> {
    credential_ref("cluster", node_id, "private_key")
}

pub fn cluster_passphrase_credential_ref(node_id: &str) -> ConfigStoreResult<CredentialRef> {
    credential_ref("cluster", node_id, "passphrase")
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodePlacement {
    /// localhost → `LocalProcessDeployer`; no ssh/upload/tunnel.
    #[default]
    Local,
    /// remote → russh/system-ssh deployer + binary upload + reverse tunnel.
    Ssh(SshTarget),
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
        #[serde(default, skip_serializing_if = "String::is_empty")]
        password: String,
        /// Ciphertext on disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        password_encrypted: Option<String>,
    },
    /// Private key — either inline PEM (secret, encrypted) or an on-host path
    /// (not a secret). An optional passphrase is always a secret.
    PrivateKey {
        /// Inline PEM plaintext — hydrated in memory, empty on disk.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        private_key: String,
        /// Ciphertext of the inline PEM on disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_encrypted: Option<String>,
        /// Path to a key file on the bamboo host (NOT a secret).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_path: Option<String>,
        /// Optional key passphrase plaintext — hydrated in memory, empty on disk.
        #[serde(default, skip_serializing_if = "String::is_empty")]
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
    /// Auto-redeploy this node when the health monitor finds its worker gone
    /// (opt-in; only recovers a node that WAS Running, never a user-Stopped one).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub auto_recover: bool,
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
    /// Resolve isolated SSH credentials after the legacy migration has
    /// completed. Metadata is server-owned: every reference must be the
    /// canonical node-scoped reference for its auth field, must match the SSH
    /// auth variant, and must not be shared with another configuration
    /// consumer. Any validation or store failure leaves every cluster runtime
    /// secret empty.
    pub fn hydrate_cluster_credentials_from_store(
        &mut self,
        data_dir: &std::path::Path,
    ) -> ConfigStoreResult<()> {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum Field {
            Password,
            PrivateKey,
            Passphrase,
        }
        let mut prior_plaintext = BTreeMap::<(usize, Field), String>::new();
        let legacy_ciphertext_present =
            self.cluster_fabric
                .nodes
                .iter()
                .enumerate()
                .any(|(index, node)| {
                    let NodePlacement::Ssh(target) = &node.placement else {
                        return false;
                    };
                    match &target.auth {
                        SshAuth::SystemSshConfig => false,
                        SshAuth::Password {
                            password,
                            password_encrypted,
                        } => {
                            if !password.trim().is_empty() {
                                prior_plaintext.insert((index, Field::Password), password.clone());
                            }
                            password_encrypted
                                .as_deref()
                                .is_some_and(|value| !value.trim().is_empty())
                        }
                        SshAuth::PrivateKey {
                            private_key,
                            private_key_encrypted,
                            passphrase,
                            passphrase_encrypted,
                            ..
                        } => {
                            if !private_key.trim().is_empty() {
                                prior_plaintext
                                    .insert((index, Field::PrivateKey), private_key.clone());
                            }
                            if !passphrase.trim().is_empty() {
                                prior_plaintext
                                    .insert((index, Field::Passphrase), passphrase.clone());
                            }
                            private_key_encrypted
                                .as_deref()
                                .is_some_and(|value| !value.trim().is_empty())
                                || passphrase_encrypted
                                    .as_deref()
                                    .is_some_and(|value| !value.trim().is_empty())
                        }
                    }
                });
        self.clear_cluster_runtime_credentials();
        if legacy_ciphertext_present {
            return Err(crate::ConfigStoreError::Validation(
                "legacy cluster credential appeared after migration".to_string(),
            ));
        }

        let mut node_ids = BTreeSet::new();
        for node in &self.cluster_fabric.nodes {
            if node.id.trim().is_empty() {
                return Err(crate::ConfigStoreError::Validation(
                    "cluster node id is empty".to_string(),
                ));
            }
            if !node_ids.insert(node.id.as_str()) {
                return Err(crate::ConfigStoreError::Validation(
                    "cluster node ids must be unique".to_string(),
                ));
            }
        }
        for node_id in self.cluster_fabric.credential_refs.keys() {
            if !node_ids.contains(node_id.as_str()) {
                return Err(crate::ConfigStoreError::Validation(
                    "cluster credential metadata references an unknown node".to_string(),
                ));
            }
        }

        let mut requested = Vec::<(usize, Field, CredentialRef)>::new();
        let mut seen_refs = BTreeSet::new();
        let consumer_counts = crate::credential_store::config_credential_ref_counts(self)?;
        for (index, node) in self.cluster_fabric.nodes.iter().enumerate() {
            let metadata = self.cluster_fabric.credential_refs.get(&node.id);
            let empty = ClusterNodeCredentialRefs::default();
            let metadata = metadata.unwrap_or(&empty);
            let mut validate = |field: Field,
                                reference: Option<&CredentialRef>,
                                configured: bool,
                                canonical: CredentialRef|
             -> ConfigStoreResult<()> {
                if configured != reference.is_some() {
                    return Err(crate::ConfigStoreError::Validation(
                        "cluster credential configured metadata is inconsistent".to_string(),
                    ));
                }
                let Some(reference) = reference else {
                    return Ok(());
                };
                if reference != &canonical {
                    return Err(crate::ConfigStoreError::Validation(
                        "cluster credential reference is not canonical".to_string(),
                    ));
                }
                // The shared inventory includes this cluster metadata slot.
                // Exactly one consumer is therefore the expected exclusive
                // state; any additional consumer remains fail-closed.
                if consumer_counts.get(reference).copied().unwrap_or(0) != 1
                    || !seen_refs.insert(reference.clone())
                {
                    return Err(crate::ConfigStoreError::Validation(
                        "cluster credential reference is shared by another config consumer"
                            .to_string(),
                    ));
                }
                requested.push((index, field, reference.clone()));
                Ok(())
            };

            match &node.placement {
                NodePlacement::Local => {
                    if !metadata.is_empty() {
                        return Err(crate::ConfigStoreError::Validation(
                            "local cluster node carries SSH credential metadata".to_string(),
                        ));
                    }
                }
                NodePlacement::Ssh(target) => match &target.auth {
                    SshAuth::SystemSshConfig => {
                        if !metadata.is_empty() {
                            return Err(crate::ConfigStoreError::Validation(
                                "system SSH node carries stored credential metadata".to_string(),
                            ));
                        }
                    }
                    SshAuth::Password { .. } => {
                        if metadata.private_key_credential_ref.is_some()
                            || metadata.private_key_configured
                            || metadata.passphrase_credential_ref.is_some()
                            || metadata.passphrase_configured
                        {
                            return Err(crate::ConfigStoreError::Validation(
                                "cluster credential metadata does not match password auth"
                                    .to_string(),
                            ));
                        }
                        validate(
                            Field::Password,
                            metadata.password_credential_ref.as_ref(),
                            metadata.password_configured,
                            cluster_password_credential_ref(&node.id)?,
                        )?;
                    }
                    SshAuth::PrivateKey { .. } => {
                        if metadata.password_credential_ref.is_some()
                            || metadata.password_configured
                        {
                            return Err(crate::ConfigStoreError::Validation(
                                "cluster credential metadata does not match private-key auth"
                                    .to_string(),
                            ));
                        }
                        validate(
                            Field::PrivateKey,
                            metadata.private_key_credential_ref.as_ref(),
                            metadata.private_key_configured,
                            cluster_private_key_credential_ref(&node.id)?,
                        )?;
                        validate(
                            Field::Passphrase,
                            metadata.passphrase_credential_ref.as_ref(),
                            metadata.passphrase_configured,
                            cluster_passphrase_credential_ref(&node.id)?,
                        )?;
                    }
                },
            }
        }

        let store = crate::CredentialStore::open(data_dir);
        let mut resolved = Vec::with_capacity(requested.len());
        for (index, field, reference) in requested {
            let secret = store.resolve(&reference)?.ok_or_else(|| {
                crate::ConfigStoreError::Validation(
                    "referenced cluster credential is unavailable".to_string(),
                )
            })?;
            let secret = secret.expose().to_string();
            if let Some(previous) = prior_plaintext.remove(&(index, field)) {
                if previous != secret {
                    return Err(crate::ConfigStoreError::Validation(
                        "legacy cluster credential appeared after migration".to_string(),
                    ));
                }
            }
            resolved.push((index, field, secret));
        }
        if !prior_plaintext.is_empty() {
            return Err(crate::ConfigStoreError::Validation(
                "legacy cluster credential appeared after migration".to_string(),
            ));
        }
        for (index, field, secret) in resolved {
            let NodePlacement::Ssh(target) = &mut self.cluster_fabric.nodes[index].placement else {
                unreachable!("validated SSH credential target")
            };
            match (&mut target.auth, field) {
                (SshAuth::Password { password, .. }, Field::Password) => *password = secret,
                (SshAuth::PrivateKey { private_key, .. }, Field::PrivateKey) => {
                    *private_key = secret
                }
                (SshAuth::PrivateKey { passphrase, .. }, Field::Passphrase) => *passphrase = secret,
                _ => unreachable!("validated cluster credential field"),
            }
        }
        Ok(())
    }

    /// Remove legacy/cached cluster secrets from a runtime snapshot. Used both
    /// before store hydration and when migration readiness is unavailable.
    pub(crate) fn clear_cluster_runtime_credentials(&mut self) {
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
                    password.clear();
                    *password_encrypted = None;
                }
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } => {
                    private_key.clear();
                    *private_key_encrypted = None;
                    passphrase.clear();
                    *passphrase_encrypted = None;
                }
            }
        }
    }

    /// Decrypt SSH secrets into in-memory plaintext after loading config.
    ///
    /// Mirrors [`Config::hydrate_env_vars_from_encrypted`]: only fills a
    /// plaintext field that is currently empty, from its `*_encrypted`
    /// counterpart.
    pub fn hydrate_cluster_fabric_from_encrypted(&mut self) {
        let credential_refs = self.cluster_fabric.credential_refs.clone();
        for node in &mut self.cluster_fabric.nodes {
            let metadata = credential_refs.get(&node.id);
            let NodePlacement::Ssh(target) = &mut node.placement else {
                continue;
            };
            match &mut target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password {
                    password,
                    password_encrypted,
                } => {
                    if metadata.is_some_and(|value| value.password_credential_ref.is_some()) {
                        *password_encrypted = None;
                        continue;
                    }
                    hydrate_field(
                        password,
                        password_encrypted.as_deref(),
                        &node.id,
                        "password",
                    );
                }
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } => {
                    if metadata.is_some_and(|value| value.private_key_credential_ref.is_some()) {
                        *private_key_encrypted = None;
                    } else {
                        hydrate_field(
                            private_key,
                            private_key_encrypted.as_deref(),
                            &node.id,
                            "private_key",
                        );
                    }
                    if metadata.is_some_and(|value| value.passphrase_credential_ref.is_some()) {
                        *passphrase_encrypted = None;
                    } else {
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
    }

    /// Re-encrypt SSH secrets from current in-memory plaintext before persisting.
    ///
    /// Mirrors [`Config::refresh_env_vars_encrypted`]: a non-empty plaintext is
    /// (re-)encrypted; an empty plaintext leaves any existing ciphertext intact
    /// (so a redacted round-trip where the client never re-sent the secret keeps
    /// it). To CLEAR a secret, the caller swaps the whole `auth` variant.
    pub fn refresh_cluster_fabric_encrypted(&mut self) -> Result<()> {
        let credential_refs = self.cluster_fabric.credential_refs.clone();
        for node in &mut self.cluster_fabric.nodes {
            let node_id = node.id.clone();
            let metadata = credential_refs.get(&node_id);
            let NodePlacement::Ssh(target) = &mut node.placement else {
                continue;
            };
            match &mut target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password {
                    password,
                    password_encrypted,
                } => {
                    if metadata.is_some_and(|value| value.password_credential_ref.is_some()) {
                        *password_encrypted = None;
                    } else {
                        refresh_field(password, password_encrypted, &node_id, "password")?;
                    }
                }
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } => {
                    if metadata.is_some_and(|value| value.private_key_credential_ref.is_some()) {
                        *private_key_encrypted = None;
                    } else {
                        refresh_field(private_key, private_key_encrypted, &node_id, "private_key")?;
                    }
                    if metadata.is_some_and(|value| value.passphrase_credential_ref.is_some()) {
                        *passphrase_encrypted = None;
                    } else {
                        refresh_field(passphrase, passphrase_encrypted, &node_id, "passphrase")?;
                    }
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
    fn credential_metadata_round_trips_without_secret_material() {
        let mut fabric = ClusterFabricConfig::default();
        fabric.credential_refs.insert(
            "node/with unsafe id".to_string(),
            ClusterNodeCredentialRefs {
                password_credential_ref: Some(
                    cluster_password_credential_ref("node/with unsafe id").unwrap(),
                ),
                password_configured: true,
                private_key_credential_ref: Some(
                    cluster_private_key_credential_ref("node/with unsafe id").unwrap(),
                ),
                private_key_configured: true,
                passphrase_credential_ref: Some(
                    cluster_passphrase_credential_ref("node/with unsafe id").unwrap(),
                ),
                passphrase_configured: true,
            },
        );

        let encoded = serde_json::to_string(&fabric).unwrap();
        assert!(encoded.contains("password_credential_ref"));
        assert!(encoded.contains("private_key_credential_ref"));
        assert!(encoded.contains("passphrase_credential_ref"));
        assert!(!encoded.contains("password_encrypted"));
        assert!(!encoded.contains("private_key_encrypted"));
        assert!(!encoded.contains("passphrase_encrypted"));
        let decoded: ClusterFabricConfig = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, fabric);
    }

    #[test]
    fn canonical_cluster_refs_are_node_scoped_and_injective() {
        let slash = cluster_password_credential_ref("node/a").unwrap();
        let dot = cluster_password_credential_ref("node.a").unwrap();
        assert_ne!(slash, dot);
        assert!(slash.as_str().starts_with("cluster."));
        assert!(slash.as_str().ends_with(".password"));
        assert_ne!(
            cluster_private_key_credential_ref("node/a").unwrap(),
            cluster_passphrase_credential_ref("node/a").unwrap()
        );
    }

    #[test]
    fn pruning_nodes_drops_orphaned_credential_metadata() {
        let mut fabric = ClusterFabricConfig::default();
        fabric
            .nodes
            .push(ssh_node("kept", SshAuth::SystemSshConfig));
        for id in ["kept", "deleted"] {
            fabric.credential_refs.insert(
                id.to_string(),
                ClusterNodeCredentialRefs {
                    password_credential_ref: Some(cluster_password_credential_ref(id).unwrap()),
                    password_configured: true,
                    ..ClusterNodeCredentialRefs::default()
                },
            );
        }

        fabric.prune_orphaned_credential_refs();
        assert!(fabric.credential_refs.contains_key("kept"));
        assert!(!fabric.credential_refs.contains_key("deleted"));
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
        assert_eq!(
            password, "secret",
            "ciphertext preserved across empty refresh"
        );
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

    #[test]
    fn isolated_cluster_credentials_hydrate_only_from_canonical_refs() {
        let _key = crate::encryption::set_test_encryption_key([0xb1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let password_ref = cluster_password_credential_ref("password-node").unwrap();
        let key_ref = cluster_private_key_credential_ref("key-node").unwrap();
        let passphrase_ref = cluster_passphrase_credential_ref("key-node").unwrap();
        let store = crate::CredentialStore::open(dir.path());
        store
            .replace(
                password_ref.clone(),
                "password-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        store
            .replace(
                key_ref.clone(),
                "private-key-secret",
                crate::CredentialSource::User,
                1,
            )
            .unwrap();
        store
            .replace(
                passphrase_ref.clone(),
                "passphrase-secret",
                crate::CredentialSource::User,
                2,
            )
            .unwrap();

        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "password-node",
            SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        config.cluster_fabric.nodes.push(ssh_node(
            "key-node",
            SshAuth::PrivateKey {
                private_key: String::new(),
                private_key_encrypted: None,
                private_key_path: None,
                passphrase: String::new(),
                passphrase_encrypted: None,
            },
        ));
        config.cluster_fabric.credential_refs.insert(
            "password-node".to_string(),
            ClusterNodeCredentialRefs {
                password_credential_ref: Some(password_ref),
                password_configured: true,
                ..ClusterNodeCredentialRefs::default()
            },
        );
        config.cluster_fabric.credential_refs.insert(
            "key-node".to_string(),
            ClusterNodeCredentialRefs {
                private_key_credential_ref: Some(key_ref),
                private_key_configured: true,
                passphrase_credential_ref: Some(passphrase_ref),
                passphrase_configured: true,
                ..ClusterNodeCredentialRefs::default()
            },
        );

        config
            .hydrate_cluster_credentials_from_store(dir.path())
            .unwrap();
        let NodePlacement::Ssh(password_target) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected SSH node")
        };
        let SshAuth::Password { password, .. } = &password_target.auth else {
            panic!("expected password auth")
        };
        assert_eq!(password, "password-secret");
        let NodePlacement::Ssh(key_target) = &config.cluster_fabric.nodes[1].placement else {
            panic!("expected SSH node")
        };
        let SshAuth::PrivateKey {
            private_key,
            passphrase,
            ..
        } = &key_target.auth
        else {
            panic!("expected private-key auth")
        };
        assert_eq!(private_key, "private-key-secret");
        assert_eq!(passphrase, "passphrase-secret");

        config
            .hydrate_cluster_credentials_from_store(dir.path())
            .unwrap();
        config.refresh_cluster_fabric_encrypted().unwrap();
        config.sanitize_cluster_fabric_for_disk();
        let durable = serde_json::to_string(&config).unwrap();
        for forbidden in [
            "password-secret",
            "private-key-secret",
            "passphrase-secret",
            "password_encrypted",
            "private_key_encrypted",
            "passphrase_encrypted",
        ] {
            assert!(!durable.contains(forbidden), "persisted {forbidden}");
        }
    }

    #[test]
    fn unavailable_or_shared_cluster_ref_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let reference = cluster_password_credential_ref("node").unwrap();
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "node",
            SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        config.cluster_fabric.credential_refs.insert(
            "node".to_string(),
            ClusterNodeCredentialRefs {
                password_credential_ref: Some(reference.clone()),
                password_configured: true,
                ..ClusterNodeCredentialRefs::default()
            },
        );

        let error = config
            .hydrate_cluster_credentials_from_store(dir.path())
            .unwrap_err();
        assert!(error.to_string().contains("unavailable"));

        config.notifications.ntfy.credential_ref = Some(reference);
        config.notifications.ntfy.configured = true;
        let error = config
            .hydrate_cluster_credentials_from_store(dir.path())
            .unwrap_err();
        assert!(error.to_string().contains("shared"));
    }

    #[test]
    fn late_legacy_cluster_secret_is_rejected_and_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("late-secret").unwrap();
        let mut config = Config::default();
        config.cluster_fabric.nodes.push(ssh_node(
            "node",
            SshAuth::Password {
                password: "late-plaintext".to_string(),
                password_encrypted: Some(ciphertext),
            },
        ));

        let error = config
            .hydrate_cluster_credentials_from_store(dir.path())
            .unwrap_err();
        assert!(error.to_string().contains("appeared after migration"));
        let NodePlacement::Ssh(target) = &config.cluster_fabric.nodes[0].placement else {
            panic!("expected SSH node")
        };
        let SshAuth::Password {
            password,
            password_encrypted,
        } = &target.auth
        else {
            panic!("expected password auth")
        };
        assert!(password.is_empty());
        assert!(password_encrypted.is_none());
    }
}
