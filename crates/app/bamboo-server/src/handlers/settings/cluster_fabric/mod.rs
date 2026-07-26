//! Remote Cluster Fabric operator API (RFC v2 §4): CRUD for nodes & clusters,
//! plus lifecycle endpoints (test/deploy/stop/status/logs).
//!
//! P1 scope: the persisted registry + redacted round-trip. The lifecycle
//! actions (`test`/`deploy`/`stop`/`logs`) are **stubbed `501 Not Implemented`**
//! until the deploy engine lands in P2; `status` returns the persisted state
//! (no live SSH probe yet).
//!
//! Secrets (SSH password / private key / passphrase) never enter ordinary node
//! payloads or responses. Dedicated request-only keep/replace/clear actions are
//! committed with node and membership metadata under one section revision.

use actix_web::{http::StatusCode, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use bamboo_config::cluster_fabric::{
    Cluster, ClusterCredentialAction, ClusterFabricConfig, ClusterNodeCredentialIntents,
    ClusterNodeCredentialRefs, DeployProfile, Node, NodePlacement, SshAuth, SshTarget, TrustLevel,
};
use bamboo_config::{
    patch::is_masked_api_key, CredentialSource, CredentialStatus, SectionEnvelope, SectionId,
    SectionStatus,
};
use bamboo_server_tools::FabricCommitSnapshot;

use crate::app_state::AppState;
use crate::error::AppError;

mod deploy;

// ─── Request / response types ──────────────────────────────────────────

/// Combined inventory returned by `GET /nodes` (nodes are redacted).
#[derive(Serialize)]
pub struct FabricListResponse {
    pub nodes: Vec<Value>,
    pub clusters: Vec<Cluster>,
}

/// Create/replace payload for a node. `id`/`state` are server-owned and ignored.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeUpsertRequest {
    pub expected_revision: u64,
    pub label: String,
    pub placement: NodePlacementRequest,
    #[serde(default)]
    pub trust_level: TrustLevel,
    #[serde(default)]
    pub deploy: DeployProfile,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub credential_changes: NodeCredentialChangesRequest,
    #[serde(default)]
    pub membership: Option<NodeMembershipRequest>,
}

/// Secret-free operator placement. SSH credential material is accepted only
/// through `credential_changes`, never through the ordinary node document.
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodePlacementRequest {
    Local,
    Ssh {
        host: String,
        #[serde(default = "default_ssh_port")]
        port: u16,
        username: String,
        auth: SshAuthRequest,
        #[serde(default)]
        host_key_fingerprint: Option<String>,
    },
}

#[derive(Clone, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum SshAuthRequest {
    SystemSshConfig {},
    Password {},
    PrivateKey {
        #[serde(default)]
        private_key_path: Option<String>,
    },
}

#[derive(Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialActionRequest {
    Keep,
    Replace { value: String },
    Clear,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCredentialChangesRequest {
    pub password: CredentialActionRequest,
    pub private_key: CredentialActionRequest,
    pub passphrase: CredentialActionRequest,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeMembershipRequest {
    /// Replace the node's complete cluster membership with these existing
    /// or newly-created cluster names as part of the same node transaction.
    #[serde(default)]
    pub cluster_names: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDeleteQuery {
    pub expected_revision: u64,
}

fn default_true() -> bool {
    true
}

fn default_ssh_port() -> u16 {
    22
}

/// Create/replace payload for a cluster.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterUpsertRequest {
    pub expected_revision: u64,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub node_ids: Vec<String>,
}

// ─── Validation ────────────────────────────────────────────────────────

fn validate_node(req: &NodeUpsertRequest) -> Result<(), AppError> {
    if req.label.trim().is_empty() {
        return Err(AppError::BadRequest("Node label is required".into()));
    }
    if let NodePlacementRequest::Ssh {
        host,
        port,
        username,
        ..
    } = &req.placement
    {
        if host.trim().is_empty() {
            return Err(AppError::BadRequest("SSH host is required".into()));
        }
        if username.trim().is_empty() {
            return Err(AppError::BadRequest("SSH username is required".into()));
        }
        if *port == 0 {
            return Err(AppError::BadRequest("SSH port must be non-zero".into()));
        }
    }
    validate_credential_actions(&req.placement, &req.credential_changes)?;
    Ok(())
}

fn validate_credential_actions(
    placement: &NodePlacementRequest,
    changes: &NodeCredentialChangesRequest,
) -> Result<(), AppError> {
    for action in [&changes.password, &changes.private_key, &changes.passphrase] {
        if let CredentialActionRequest::Replace { value } = action {
            if value.is_empty() || is_masked_api_key(value) {
                return Err(AppError::BadRequest(
                    "credential replacements must be nonempty and must not use a mask sentinel"
                        .to_string(),
                ));
            }
        }
    }
    let is_clear =
        |action: &CredentialActionRequest| matches!(action, CredentialActionRequest::Clear);
    match placement {
        NodePlacementRequest::Local
        | NodePlacementRequest::Ssh {
            auth: SshAuthRequest::SystemSshConfig {},
            ..
        } => {
            if !is_clear(&changes.password)
                || !is_clear(&changes.private_key)
                || !is_clear(&changes.passphrase)
            {
                return Err(AppError::BadRequest(
                    "this placement requires clearing all stored SSH credentials".to_string(),
                ));
            }
        }
        NodePlacementRequest::Ssh {
            auth: SshAuthRequest::Password {},
            ..
        } => {
            if is_clear(&changes.password) {
                return Err(AppError::BadRequest(
                    "password authentication requires keep or replace".to_string(),
                ));
            }
            if !is_clear(&changes.private_key) || !is_clear(&changes.passphrase) {
                return Err(AppError::BadRequest(
                    "password authentication requires clearing private-key credentials".to_string(),
                ));
            }
        }
        NodePlacementRequest::Ssh {
            auth: SshAuthRequest::PrivateKey { private_key_path },
            ..
        } => {
            if !is_clear(&changes.password) {
                return Err(AppError::BadRequest(
                    "private-key authentication requires clearing the password credential"
                        .to_string(),
                ));
            }
            let uses_path = private_key_path
                .as_deref()
                .is_some_and(|path| !path.trim().is_empty());
            if uses_path && !is_clear(&changes.private_key) {
                return Err(AppError::BadRequest(
                    "private-key-path authentication requires clearing the inline private key"
                        .to_string(),
                ));
            }
            if !uses_path && is_clear(&changes.private_key) {
                return Err(AppError::BadRequest(
                    "private-key authentication requires an inline key or key path".to_string(),
                ));
            }
        }
    }
    Ok(())
}

impl NodePlacementRequest {
    fn into_domain(self) -> NodePlacement {
        match self {
            Self::Local => NodePlacement::Local,
            Self::Ssh {
                host,
                port,
                username,
                auth,
                host_key_fingerprint,
            } => NodePlacement::Ssh(SshTarget {
                host,
                port,
                username,
                auth: match auth {
                    SshAuthRequest::SystemSshConfig {} => SshAuth::SystemSshConfig,
                    SshAuthRequest::Password {} => SshAuth::Password {
                        password: String::new(),
                        password_encrypted: None,
                    },
                    SshAuthRequest::PrivateKey { private_key_path } => SshAuth::PrivateKey {
                        private_key: String::new(),
                        private_key_encrypted: None,
                        private_key_path: private_key_path.filter(|path| !path.trim().is_empty()),
                        passphrase: String::new(),
                        passphrase_encrypted: None,
                    },
                },
                host_key_fingerprint,
            }),
        }
    }
}

impl CredentialActionRequest {
    fn into_domain(self) -> ClusterCredentialAction {
        match self {
            Self::Keep => ClusterCredentialAction::Keep,
            Self::Replace { value } => ClusterCredentialAction::Replace(value),
            Self::Clear => ClusterCredentialAction::Clear,
        }
    }
}

impl NodeCredentialChangesRequest {
    fn into_domain(self) -> ClusterNodeCredentialIntents {
        ClusterNodeCredentialIntents {
            password: self.password.into_domain(),
            private_key: self.private_key.into_domain(),
            passphrase: self.passphrase.into_domain(),
        }
    }
}

// ─── Secret-free section projection ────────────────────────────────────

/// Serialize a node without any secret fields, ciphertext, or mask sentinel.
fn secret_free_node_value(node: &Node) -> Value {
    let mut value = serde_json::to_value(node).unwrap_or(Value::Null);
    if let Some(auth) = value
        .get_mut("placement")
        .and_then(|p| p.get_mut("auth"))
        .and_then(|a| a.as_object_mut())
    {
        for field in [
            "password",
            "password_encrypted",
            "private_key",
            "private_key_encrypted",
            "passphrase",
            "passphrase_encrypted",
        ] {
            auth.remove(field);
        }
    }
    value
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ClusterCredentialState {
    Configured,
    FromEnv,
    Missing,
    Error,
}

#[derive(Serialize)]
struct ClusterCredentialFieldStatus {
    state: ClusterCredentialState,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<CredentialSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct ClusterNodeCredentialStatusView {
    password: ClusterCredentialFieldStatus,
    private_key: ClusterCredentialFieldStatus,
    passphrase: ClusterCredentialFieldStatus,
}

fn credential_field_status(
    configured: bool,
    reference: Option<&bamboo_config::CredentialRef>,
    statuses: &BTreeMap<String, CredentialStatus>,
    store_healthy: bool,
) -> ClusterCredentialFieldStatus {
    if !configured {
        return ClusterCredentialFieldStatus {
            state: ClusterCredentialState::Missing,
            source: None,
            updated_at: None,
        };
    }
    if !store_healthy {
        return ClusterCredentialFieldStatus {
            state: ClusterCredentialState::Error,
            source: None,
            updated_at: None,
        };
    }
    let Some(status) = reference.and_then(|reference| statuses.get(reference.as_str())) else {
        return ClusterCredentialFieldStatus {
            state: ClusterCredentialState::Error,
            source: None,
            updated_at: None,
        };
    };
    if !status.configured {
        return ClusterCredentialFieldStatus {
            state: ClusterCredentialState::Error,
            source: Some(status.source),
            updated_at: status.updated_at,
        };
    }
    ClusterCredentialFieldStatus {
        state: if status.source == CredentialSource::Environment {
            ClusterCredentialState::FromEnv
        } else {
            ClusterCredentialState::Configured
        },
        source: Some(status.source),
        updated_at: status.updated_at,
    }
}

fn node_credential_status(
    metadata: Option<&ClusterNodeCredentialRefs>,
    statuses: &BTreeMap<String, CredentialStatus>,
    store_healthy: bool,
) -> ClusterNodeCredentialStatusView {
    let metadata = metadata.cloned().unwrap_or_default();
    ClusterNodeCredentialStatusView {
        password: credential_field_status(
            metadata.password_configured,
            metadata.password_credential_ref.as_ref(),
            statuses,
            store_healthy,
        ),
        private_key: credential_field_status(
            metadata.private_key_configured,
            metadata.private_key_credential_ref.as_ref(),
            statuses,
            store_healthy,
        ),
        passphrase: credential_field_status(
            metadata.passphrase_configured,
            metadata.passphrase_credential_ref.as_ref(),
            statuses,
            store_healthy,
        ),
    }
}

fn project_cluster_section(
    fabric: ClusterFabricConfig,
    mut envelope: SectionEnvelope<Value>,
    statuses: Vec<CredentialStatus>,
    store_healthy: bool,
) -> Result<SectionEnvelope<Value>, AppError> {
    let statuses = statuses
        .into_iter()
        .map(|status| (status.credential_ref.as_str().to_string(), status))
        .collect::<BTreeMap<_, _>>();
    let mut credential_status = BTreeMap::new();
    for node in &fabric.nodes {
        credential_status.insert(
            node.id.clone(),
            node_credential_status(
                fabric.credential_refs.get(&node.id),
                &statuses,
                store_healthy,
            ),
        );
    }
    let mut data = serde_json::to_value(&fabric)
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let object = data.as_object_mut().ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "cluster section projection is not an object"
        ))
    })?;
    object.remove("credential_refs");
    object.insert(
        "nodes".to_string(),
        Value::Array(fabric.nodes.iter().map(secret_free_node_value).collect()),
    );
    object.insert(
        "credential_status".to_string(),
        serde_json::to_value(credential_status)
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?,
    );
    envelope.data = data;
    if envelope.last_error.is_some() {
        envelope.last_error = Some("cluster configuration is unavailable".to_string());
    }
    Ok(envelope)
}

fn cluster_section_envelope(app_state: &AppState) -> Result<SectionEnvelope<Value>, AppError> {
    let facade = app_state.config_facade.as_ref().ok_or_else(|| {
        AppError::BadRequest(
            "cluster settings require the modular configuration facade".to_string(),
        )
    })?;
    let snapshot = facade.registry().cluster_fabric.snapshot();
    let fabric = snapshot.data.0.clone();
    let (statuses, store_healthy) = match app_state.credential_store.statuses_with_health() {
        Ok((statuses, health)) => (statuses, health.status != SectionStatus::Degraded),
        Err(_) => (Vec::new(), false),
    };
    let envelope = facade
        .registry()
        .envelope_value(SectionId::ClusterFabric)
        .map_err(|_| {
            AppError::InternalError(anyhow::anyhow!("cluster section envelope is unavailable"))
        })?;
    project_cluster_section(fabric, envelope, statuses, store_healthy)
}

pub(super) fn committed_cluster_section(
    snapshot: FabricCommitSnapshot,
) -> Result<SectionEnvelope<Value>, AppError> {
    let store_healthy = snapshot.credential_health.status != SectionStatus::Degraded;
    project_cluster_section(
        snapshot.config.cluster_fabric.clone(),
        snapshot.section,
        snapshot.credential_statuses,
        store_healthy,
    )
}

pub(super) async fn get_cluster_section(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    Ok(HttpResponse::Ok().json(cluster_section_envelope(&app_state)?))
}

fn replace_node_membership(
    fabric: &mut ClusterFabricConfig,
    node_id: &str,
    membership: Option<&NodeMembershipRequest>,
) -> Result<(), AppError> {
    let Some(membership) = membership else {
        return Ok(());
    };
    let mut names = BTreeSet::new();
    for name in &membership.cluster_names {
        let name = name.trim();
        if name.is_empty() {
            return Err(AppError::BadRequest(
                "Cluster membership names must be nonempty".to_string(),
            ));
        }
        if !names.insert(name.to_string()) {
            return Err(AppError::BadRequest(
                "Cluster membership names must be unique".to_string(),
            ));
        }
    }
    for name in &names {
        if fabric.cluster(name).is_none() {
            fabric.clusters.push(Cluster {
                name: name.clone(),
                description: None,
                node_ids: Vec::new(),
            });
        }
    }
    for cluster in &mut fabric.clusters {
        cluster.node_ids.retain(|member| member != node_id);
        if names.contains(&cluster.name) {
            cluster.node_ids.push(node_id.to_string());
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct ClusterMutationResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(flatten)]
    section: SectionEnvelope<Value>,
}

fn cluster_mutation_response(
    status: StatusCode,
    node_id: Option<String>,
    snapshot: FabricCommitSnapshot,
) -> Result<HttpResponse, AppError> {
    let section = committed_cluster_section(snapshot)?;
    Ok(HttpResponse::build(status).json(ClusterMutationResponse { node_id, section }))
}

// ─── Node handlers ─────────────────────────────────────────────────────

/// `GET /v1/bamboo/settings/nodes` — list nodes (redacted) + clusters.
pub async fn list_nodes(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    let nodes = config
        .cluster_fabric
        .nodes
        .iter()
        .map(secret_free_node_value)
        .collect();
    Ok(HttpResponse::Ok().json(FabricListResponse {
        nodes,
        clusters: config.cluster_fabric.clusters.clone(),
    }))
}

/// `GET /v1/bamboo/settings/nodes/{id}` — one node (redacted).
pub async fn get_node(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let config = app_state.config.read().await;
    let node = config
        .cluster_fabric
        .node(&id)
        .ok_or_else(|| AppError::NotFound(format!("Node '{id}'")))?;
    Ok(HttpResponse::Ok().json(secret_free_node_value(node)))
}

/// `POST /v1/bamboo/settings/nodes` — create a node.
pub async fn create_node(
    app_state: web::Data<AppState>,
    payload: web::Json<NodeUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    validate_node(&req)?;
    let NodeUpsertRequest {
        expected_revision,
        label,
        placement,
        trust_level,
        deploy,
        enabled,
        credential_changes,
        membership,
    } = req;

    let node = Node {
        id: Uuid::new_v4().to_string(),
        label,
        placement: placement.into_domain(),
        trust_level,
        deploy,
        state: None,
        enabled,
    };
    let node_id = node.id.clone();
    let node_id_for_update = node_id.clone();
    let node_intents = BTreeMap::from([(node_id.clone(), credential_changes.into_domain())]);

    let snapshot = app_state
        .update_cluster_fabric_credentials(expected_revision, node_intents, move |cfg| {
            cfg.cluster_fabric.nodes.push(node.clone());
            replace_node_membership(
                &mut cfg.cluster_fabric,
                &node_id_for_update,
                membership.as_ref(),
            )?;
            Ok(())
        })
        .await?;

    cluster_mutation_response(StatusCode::CREATED, Some(node_id), snapshot)
}

/// `PUT /v1/bamboo/settings/nodes/{id}` — update a node (secret-preserving).
pub async fn update_node(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<NodeUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let req = payload.into_inner();
    validate_node(&req)?;
    let NodeUpsertRequest {
        expected_revision,
        label,
        placement,
        trust_level,
        deploy,
        enabled,
        credential_changes,
        membership,
    } = req;
    let id_for_response = id.clone();
    let placement = placement.into_domain();
    let node_intents = BTreeMap::from([(id.clone(), credential_changes.into_domain())]);

    let snapshot = app_state
        .update_cluster_fabric_credentials(expected_revision, node_intents, move |cfg| {
            let existing = cfg
                .cluster_fabric
                .node(&id)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("Node '{id}'")))?;
            let node = Node {
                id: existing.id.clone(),
                label: label.clone(),
                placement: placement.clone(),
                trust_level,
                deploy: deploy.clone(),
                state: existing.state.clone(), // engine-owned: preserve
                enabled,
            };

            let slot = cfg
                .cluster_fabric
                .node_mut(&id)
                .expect("node existed above");
            *slot = node;
            replace_node_membership(&mut cfg.cluster_fabric, &id, membership.as_ref())?;
            Ok(())
        })
        .await?;

    cluster_mutation_response(StatusCode::OK, Some(id_for_response), snapshot)
}

/// `DELETE /v1/bamboo/settings/nodes/{id}` — remove a node.
pub async fn delete_node(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<NodeDeleteQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let expected_revision = query.expected_revision;
    let id_for_update = id.clone();

    let snapshot = app_state
        .update_cluster_fabric_credentials(
            expected_revision,
            BTreeMap::from([(id.clone(), ClusterNodeCredentialIntents::clear_all())]),
            move |cfg| {
                let before = cfg.cluster_fabric.nodes.len();
                cfg.cluster_fabric.nodes.retain(|n| n.id != id_for_update);
                if cfg.cluster_fabric.nodes.len() == before {
                    return Err(AppError::NotFound(format!("Node '{id_for_update}'")));
                }
                // Drop the node from any cluster membership too.
                for cluster in &mut cfg.cluster_fabric.clusters {
                    cluster.node_ids.retain(|nid| nid != &id_for_update);
                }
                cfg.cluster_fabric.credential_refs.remove(&id_for_update);
                Ok(())
            },
        )
        .await?;

    cluster_mutation_response(StatusCode::OK, Some(id), snapshot)
}

// ─── Cluster handlers ──────────────────────────────────────────────────

/// `POST /v1/bamboo/settings/clusters` — create a cluster.
pub async fn create_cluster(
    app_state: web::Data<AppState>,
    payload: web::Json<ClusterUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    if req.name.trim().is_empty() {
        return Err(AppError::BadRequest("Cluster name is required".into()));
    }
    let expected_revision = req.expected_revision;

    let snapshot = app_state
        .update_cluster_fabric_credentials(expected_revision, BTreeMap::new(), move |cfg| {
            if cfg.cluster_fabric.cluster(&req.name).is_some() {
                return Err(AppError::BadRequest(format!(
                    "Cluster '{}' already exists",
                    req.name
                )));
            }
            cfg.cluster_fabric.clusters.push(Cluster {
                name: req.name.clone(),
                description: req.description.clone(),
                node_ids: req.node_ids.clone(),
            });
            Ok(())
        })
        .await?;

    cluster_mutation_response(StatusCode::CREATED, None, snapshot)
}

/// `PUT /v1/bamboo/settings/clusters/{name}` — update a cluster.
pub async fn update_cluster(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ClusterUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();
    let req = payload.into_inner();
    if req.name.trim().is_empty() {
        return Err(AppError::BadRequest("Cluster name is required".into()));
    }
    let expected_revision = req.expected_revision;

    let snapshot = app_state
        .update_cluster_fabric_credentials(expected_revision, BTreeMap::new(), move |cfg| {
            if req.name != name && cfg.cluster_fabric.cluster(&req.name).is_some() {
                return Err(AppError::BadRequest(format!(
                    "Cluster '{}' already exists",
                    req.name
                )));
            }
            let cluster = cfg
                .cluster_fabric
                .clusters
                .iter_mut()
                .find(|c| c.name == name)
                .ok_or_else(|| AppError::NotFound(format!("Cluster '{name}'")))?;
            cluster.description = req.description.clone();
            cluster.node_ids = req.node_ids.clone();
            cluster.name = req.name.clone();
            Ok(())
        })
        .await?;

    cluster_mutation_response(StatusCode::OK, None, snapshot)
}

/// `DELETE /v1/bamboo/settings/clusters/{name}` — remove a cluster (nodes kept).
pub async fn delete_cluster(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<NodeDeleteQuery>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();
    let expected_revision = query.expected_revision;

    let snapshot = app_state
        .update_cluster_fabric_credentials(expected_revision, BTreeMap::new(), move |cfg| {
            let before = cfg.cluster_fabric.clusters.len();
            cfg.cluster_fabric.clusters.retain(|c| c.name != name);
            if cfg.cluster_fabric.clusters.len() == before {
                return Err(AppError::NotFound(format!("Cluster '{name}'")));
            }
            Ok(())
        })
        .await?;

    cluster_mutation_response(StatusCode::OK, None, snapshot)
}

// ─── Lifecycle ─────────────────────────────────────────────────────────

/// `GET /v1/bamboo/settings/nodes/{id}/status` — persisted state (no live probe yet).
pub async fn node_status(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let config = app_state.config.read().await;
    let node = config
        .cluster_fabric
        .node(&id)
        .ok_or_else(|| AppError::NotFound(format!("Node '{id}'")))?;
    Ok(HttpResponse::Ok().json(json!({
        "id": node.id,
        "enabled": node.enabled,
        "state": node.state,
    })))
}

/// Query params for `node_deploy`.
#[derive(Debug, Clone, Deserialize)]
pub struct DeployQuery {
    pub expected_revision: u64,
    /// Deploy the no-LLM echo executor (connectivity smoke test).
    #[serde(default)]
    pub echo: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleQuery {
    pub expected_revision: u64,
}

#[derive(Serialize)]
struct NodeStateMutationResponse {
    id: String,
    state: bamboo_config::cluster_fabric::NodeState,
    #[serde(flatten)]
    section: SectionEnvelope<Value>,
}

#[derive(Serialize)]
struct NodeTestResponse {
    id: String,
    ok: bool,
    preflight: String,
    #[serde(flatten)]
    section: SectionEnvelope<Value>,
}

/// `POST /v1/bamboo/settings/nodes/{id}/deploy` — deploy a worker for the node.
pub async fn node_deploy(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<DeployQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let result = deploy::deploy_node(&app_state, &id, query.echo, query.expected_revision).await?;
    let section = committed_cluster_section(result.snapshot)?;
    Ok(HttpResponse::Ok().json(NodeStateMutationResponse {
        id,
        state: result.value,
        section,
    }))
}

/// `POST /v1/bamboo/settings/nodes/{id}/stop` — stop the node's worker.
pub async fn node_stop(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LifecycleQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let result = deploy::stop_node(&app_state, &id, query.expected_revision).await?;
    let section = committed_cluster_section(result.snapshot)?;
    Ok(HttpResponse::Ok().json(NodeStateMutationResponse {
        id,
        state: result.value,
        section,
    }))
}

/// `POST /v1/bamboo/settings/nodes/{id}/test` — connectivity preflight (no deploy).
pub async fn node_test(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LifecycleQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let result = deploy::test_node(&app_state, &id, query.expected_revision).await?;
    let section = committed_cluster_section(result.snapshot)?;
    Ok(HttpResponse::Ok().json(NodeTestResponse {
        id,
        ok: true,
        preflight: result.value,
        section,
    }))
}

/// Query params for `node_logs`.
#[derive(Debug, Clone, Deserialize)]
pub struct LogsQuery {
    /// Number of trailing lines to return (default 200).
    #[serde(default = "default_log_lines")]
    pub lines: usize,
}

fn default_log_lines() -> usize {
    200
}

/// `GET /v1/bamboo/settings/nodes/{id}/logs` — tail the node worker's log.
pub async fn node_logs(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LogsQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let lines = query.lines.clamp(1, 5000);
    let logs = deploy::read_logs(&app_state, &id, lines).await?;
    Ok(HttpResponse::Ok().json(json!({ "id": id, "logs": logs })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_config::{credential_ref, CredentialRef};

    fn pw_node(password: &str, encrypted: Option<&str>) -> Node {
        Node {
            id: "n1".into(),
            label: "n1".into(),
            placement: NodePlacement::Ssh(SshTarget {
                host: "h".into(),
                port: 22,
                username: "u".into(),
                auth: SshAuth::Password {
                    password: password.into(),
                    password_encrypted: encrypted.map(|s| s.into()),
                },
                host_key_fingerprint: None,
            }),
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        }
    }

    #[::core::prelude::v1::test]
    fn public_node_projection_omits_plaintext_ciphertext_and_masks() {
        let node = pw_node("hunter2", Some("ciphertext"));
        let v = secret_free_node_value(&node);
        let auth = &v["placement"]["auth"];
        assert!(auth.get("password").is_none());
        assert!(auth.get("password_encrypted").is_none());
        let encoded = serde_json::to_string(&v).unwrap();
        assert!(!encoded.contains("hunter2"));
        assert!(!encoded.contains("ciphertext"));
        assert!(!encoded.contains("****"));
    }

    #[::core::prelude::v1::test]
    fn request_rejects_credentials_inside_placement() {
        let request = serde_json::json!({
            "expected_revision": 1,
            "label": "node",
            "placement": {
                "type": "ssh",
                "host": "example.test",
                "username": "deploy",
                "auth": {"method": "password", "password": "must-not-enter-placement"}
            },
            "credential_changes": {
                "password": {"action": "replace", "value": "request-only-secret"},
                "private_key": {"action": "clear"},
                "passphrase": {"action": "clear"}
            }
        });
        let error = match serde_json::from_value::<NodeUpsertRequest>(request) {
            Ok(_) => panic!("placement credential must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unknown field `password`"));
        assert!(!error.to_string().contains("must-not-enter-placement"));
    }

    #[::core::prelude::v1::test]
    fn password_request_accepts_explicit_keep_or_replace_only() {
        let placement = NodePlacementRequest::Ssh {
            host: "example.test".to_string(),
            port: 22,
            username: "deploy".to_string(),
            auth: SshAuthRequest::Password {},
            host_key_fingerprint: None,
        };
        let valid = NodeCredentialChangesRequest {
            password: CredentialActionRequest::Keep,
            private_key: CredentialActionRequest::Clear,
            passphrase: CredentialActionRequest::Clear,
        };
        validate_credential_actions(&placement, &valid).unwrap();

        let cleared = NodeCredentialChangesRequest {
            password: CredentialActionRequest::Clear,
            private_key: CredentialActionRequest::Clear,
            passphrase: CredentialActionRequest::Clear,
        };
        assert!(validate_credential_actions(&placement, &cleared).is_err());
    }

    #[::core::prelude::v1::test]
    fn credential_replacement_rejects_mask_sentinel() {
        let placement = NodePlacementRequest::Ssh {
            host: "example.test".to_string(),
            port: 22,
            username: "deploy".to_string(),
            auth: SshAuthRequest::Password {},
            host_key_fingerprint: None,
        };
        let invalid = NodeCredentialChangesRequest {
            password: CredentialActionRequest::Replace {
                value: "****...****".to_string(),
            },
            private_key: CredentialActionRequest::Clear,
            passphrase: CredentialActionRequest::Clear,
        };
        assert!(validate_credential_actions(&placement, &invalid)
            .unwrap_err()
            .to_string()
            .contains("mask sentinel"));
    }

    #[::core::prelude::v1::test]
    fn membership_replacement_creates_missing_cluster_in_candidate() {
        let node = Node {
            id: "n1".to_string(),
            label: "n1".to_string(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        };
        let mut fabric = ClusterFabricConfig {
            nodes: vec![node],
            clusters: vec![Cluster {
                name: "old".to_string(),
                description: None,
                node_ids: vec!["n1".to_string()],
            }],
            ..ClusterFabricConfig::default()
        };
        replace_node_membership(
            &mut fabric,
            "n1",
            Some(&NodeMembershipRequest {
                cluster_names: vec!["new".to_string()],
            }),
        )
        .unwrap();
        assert!(fabric.cluster("old").unwrap().node_ids.is_empty());
        assert_eq!(fabric.cluster("new").unwrap().node_ids, ["n1"]);
    }

    #[::core::prelude::v1::test]
    fn credential_status_distinguishes_configured_missing_and_error_without_refs() {
        let reference: CredentialRef = credential_ref("cluster", "n1", "password").unwrap();
        let metadata = ClusterNodeCredentialRefs {
            password_credential_ref: Some(reference.clone()),
            password_configured: true,
            ..ClusterNodeCredentialRefs::default()
        };
        let status = CredentialStatus {
            credential_ref: reference,
            configured: true,
            source: CredentialSource::User,
            updated_at: None,
        };
        let statuses = BTreeMap::from([(status.credential_ref.as_str().to_string(), status)]);
        let healthy = node_credential_status(Some(&metadata), &statuses, true);
        let healthy_json = serde_json::to_value(healthy).unwrap();
        assert_eq!(healthy_json["password"]["state"], "configured");
        assert_eq!(healthy_json["private_key"]["state"], "missing");
        assert!(!healthy_json.to_string().contains("cluster.n1.password"));

        let error = node_credential_status(Some(&metadata), &statuses, false);
        assert_eq!(
            serde_json::to_value(error).unwrap()["password"]["state"],
            "error"
        );
    }

    #[actix_web::test]
    async fn delayed_mutation_response_stays_bound_to_its_own_commit_snapshot() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x73; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let first = state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "race-node".to_string(),
                    ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(Node {
                        id: "race-node".to_string(),
                        label: "first-commit".to_string(),
                        placement: NodePlacement::Local,
                        trust_level: TrustLevel::Trusted,
                        deploy: DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        state
            .update_cluster_fabric_credentials(
                1,
                BTreeMap::from([(
                    "race-node".to_string(),
                    ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.node_mut("race-node").unwrap().label =
                        "second-commit".to_string();
                    Ok(())
                },
            )
            .await
            .unwrap();

        let response =
            cluster_mutation_response(StatusCode::OK, Some("race-node".to_string()), first)
                .unwrap();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["revision"], 1);
        assert_eq!(body["data"]["nodes"][0]["label"], "first-commit");
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("race-node")
                .unwrap()
                .label,
            "second-commit"
        );
    }

    #[actix_web::test]
    async fn node_api_returns_one_redacted_section_revision_and_canonical_conflicts() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x72; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/nodes", web::post().to(create_node))
                .route("/nodes/{id}", web::put().to(update_node))
                .route("/nodes/{id}/stop", web::post().to(node_stop))
                .route("/clusters", web::post().to(create_cluster))
                .route("/clusters/{name}", web::put().to(update_cluster))
                .route("/clusters/{name}", web::delete().to(delete_cluster)),
        )
        .await;
        let create_secret = "create-request-only-secret";
        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/nodes")
                .set_json(json!({
                    "expected_revision": 0,
                    "label": "node",
                    "placement": {
                        "type": "ssh",
                        "host": "example.test",
                        "username": "deploy",
                        "auth": {"method": "password"}
                    },
                    "credential_changes": {
                        "password": {"action": "replace", "value": create_secret},
                        "private_key": {"action": "clear"},
                        "passphrase": {"action": "clear"}
                    },
                    "membership": {"cluster_names": ["created-with-node"]}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_body = String::from_utf8(test::read_body(created).await.to_vec()).unwrap();
        assert!(!created_body.contains(create_secret));
        assert!(!created_body.contains("****"));
        assert!(!created_body.contains("credential_ref"));
        let created: Value = serde_json::from_str(&created_body).unwrap();
        assert_eq!(created["revision"], 1);
        let node_id = created["node_id"].as_str().unwrap();
        assert_eq!(
            created["data"]["credential_status"][node_id]["password"]["state"],
            "configured"
        );
        assert_eq!(created["data"]["clusters"][0]["node_ids"], json!([node_id]));
        let auth = &created["data"]["nodes"][0]["placement"]["auth"];
        assert_eq!(auth["method"], "password");
        assert!(auth.get("password").is_none());

        let unrelated = CredentialRef::parse("custom.unrelated.cluster_test").unwrap();
        let credential_revision = state.credential_store.revision().unwrap();
        state
            .credential_store
            .replace(
                unrelated.clone(),
                "unrelated-secret",
                CredentialSource::User,
                credential_revision,
            )
            .unwrap();
        let updated = test::call_service(
            &app,
            test::TestRequest::put()
                .uri(&format!("/nodes/{node_id}"))
                .set_json(json!({
                    "expected_revision": 1,
                    "label": "updated",
                    "placement": {
                        "type": "ssh",
                        "host": "example.test",
                        "username": "deploy",
                        "auth": {"method": "password"}
                    },
                    "credential_changes": {
                        "password": {"action": "keep"},
                        "private_key": {"action": "clear"},
                        "passphrase": {"action": "clear"}
                    },
                    "membership": {"cluster_names": ["created-with-node"]}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            updated.status(),
            StatusCode::OK,
            "unrelated credential revision must not create a false 409"
        );
        let updated: Value = test::read_body_json(updated).await;
        assert_eq!(updated["revision"], 2);
        assert_eq!(updated["data"]["nodes"][0]["label"], "updated");
        assert_eq!(
            state
                .credential_store
                .resolve(&unrelated)
                .unwrap()
                .unwrap()
                .expose(),
            "unrelated-secret"
        );

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri(&format!("/nodes/{node_id}"))
                .set_json(json!({
                    "expected_revision": 1,
                    "label": "stale",
                    "placement": {
                        "type": "ssh",
                        "host": "example.test",
                        "username": "deploy",
                        "auth": {"method": "password"}
                    },
                    "credential_changes": {
                        "password": {"action": "keep"},
                        "private_key": {"action": "clear"},
                        "passphrase": {"action": "clear"}
                    }
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        let stale: Value = test::read_body_json(stale).await;
        assert_eq!(stale["error"]["code"], "config_revision_conflict");
        assert!(stale["error"]["message"]
            .as_str()
            .unwrap()
            .contains("expected 1, actual 2"));
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node(node_id)
                .unwrap()
                .label,
            "updated"
        );

        let stale_create = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/clusters")
                .set_json(json!({
                    "expected_revision": 1,
                    "name": "crud-cluster",
                    "node_ids": [node_id]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale_create.status(), StatusCode::CONFLICT);
        let stale_create: Value = test::read_body_json(stale_create).await;
        assert_eq!(
            stale_create["error"]["message"],
            "Configuration revision conflict: expected 1, actual 2"
        );

        let created_cluster = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/clusters")
                .set_json(json!({
                    "expected_revision": 2,
                    "name": "crud-cluster",
                    "node_ids": [node_id]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created_cluster.status(), StatusCode::CREATED);
        let created_cluster: Value = test::read_body_json(created_cluster).await;
        assert_eq!(created_cluster["revision"], 3);
        assert!(created_cluster["data"]["clusters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|cluster| cluster["name"] == "crud-cluster"));

        let stale_update = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/clusters/crud-cluster")
                .set_json(json!({
                    "expected_revision": 2,
                    "name": "crud-cluster",
                    "description": "stale",
                    "node_ids": [node_id]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale_update.status(), StatusCode::CONFLICT);
        let stale_update: Value = test::read_body_json(stale_update).await;
        assert_eq!(
            stale_update["error"]["message"],
            "Configuration revision conflict: expected 2, actual 3"
        );

        let updated_cluster = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/clusters/crud-cluster")
                .set_json(json!({
                    "expected_revision": 3,
                    "name": "crud-cluster",
                    "description": "updated",
                    "node_ids": [node_id]
                }))
                .to_request(),
        )
        .await;
        assert_eq!(updated_cluster.status(), StatusCode::OK);
        let updated_cluster: Value = test::read_body_json(updated_cluster).await;
        assert_eq!(updated_cluster["revision"], 4);
        assert_eq!(
            updated_cluster["data"]["clusters"]
                .as_array()
                .unwrap()
                .iter()
                .find(|cluster| cluster["name"] == "crud-cluster")
                .unwrap()["description"],
            "updated"
        );

        let stale_delete = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri("/clusters/crud-cluster?expected_revision=3")
                .to_request(),
        )
        .await;
        assert_eq!(stale_delete.status(), StatusCode::CONFLICT);
        let stale_delete: Value = test::read_body_json(stale_delete).await;
        assert_eq!(
            stale_delete["error"]["message"],
            "Configuration revision conflict: expected 3, actual 4"
        );

        let deleted_cluster = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri("/clusters/crud-cluster?expected_revision=4")
                .to_request(),
        )
        .await;
        assert_eq!(deleted_cluster.status(), StatusCode::OK);
        let deleted_cluster: Value = test::read_body_json(deleted_cluster).await;
        assert_eq!(deleted_cluster["revision"], 5);
        assert!(!deleted_cluster["data"]["clusters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|cluster| cluster["name"] == "crud-cluster"));

        let stale_stop = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/nodes/{node_id}/stop?expected_revision=4"))
                .to_request(),
        )
        .await;
        assert_eq!(stale_stop.status(), StatusCode::CONFLICT);

        let stopped = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/nodes/{node_id}/stop?expected_revision=5"))
                .to_request(),
        )
        .await;
        let stopped_status = stopped.status();
        let stopped_body = String::from_utf8(test::read_body(stopped).await.to_vec()).unwrap();
        assert_eq!(stopped_status, StatusCode::OK, "{stopped_body}");
        assert!(!stopped_body.contains(create_secret));
        assert!(!stopped_body.contains("credential_ref"));
        assert!(!stopped_body.contains("****"));
        let stopped: Value = serde_json::from_str(&stopped_body).unwrap();
        assert_eq!(stopped["revision"], 6);
        assert_eq!(stopped["state"]["status"], "stopped");
        assert_eq!(stopped["data"]["nodes"][0]["state"]["status"], "stopped");
    }
}
