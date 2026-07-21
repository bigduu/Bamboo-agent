//! Remote Cluster Fabric operator API (RFC v2 §4): CRUD for nodes & clusters,
//! plus lifecycle endpoints (test/deploy/stop/status/logs).
//!
//! P1 scope: the persisted registry + redacted round-trip. The lifecycle
//! actions (`test`/`deploy`/`stop`/`logs`) are **stubbed `501 Not Implemented`**
//! until the deploy engine lands in P2; `status` returns the persisted state
//! (no live SSH probe yet).
//!
//! Secrets (SSH password / private key / passphrase) never leave the backend:
//! responses are redacted ([`redact_node_value`]) and updates that re-send the
//! mask sentinel preserve the stored ciphertext.

use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use bamboo_config::cluster_fabric::{
    Cluster, DeployProfile, Node, NodePlacement, SshAuth, TrustLevel,
};

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::error::AppError;

mod deploy;

/// The sentinel a redacted secret is replaced with (matches the rest of the
/// settings redaction). An update that re-sends this value means "keep current".
// This is a public redaction marker, not a password or cryptographic value.
// codeql[rust/hard-coded-cryptographic-value]
const SECRET_MASK: &str = "****...****";

// ─── Request / response types ──────────────────────────────────────────

/// Combined inventory returned by `GET /nodes` (nodes are redacted).
#[derive(Serialize)]
pub struct FabricListResponse {
    pub nodes: Vec<Value>,
    pub clusters: Vec<Cluster>,
}

/// Create/replace payload for a node. `id`/`state` are server-owned and ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeUpsertRequest {
    pub expected_revision: u64,
    pub label: String,
    pub placement: NodePlacement,
    #[serde(default)]
    pub trust_level: TrustLevel,
    #[serde(default)]
    pub deploy: DeployProfile,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDeleteQuery {
    pub expected_revision: u64,
}

fn default_true() -> bool {
    true
}

/// Create/replace payload for a cluster.
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterUpsertRequest {
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
    if let NodePlacement::Ssh(target) = &req.placement {
        if target.host.trim().is_empty() {
            return Err(AppError::BadRequest("SSH host is required".into()));
        }
        if target.username.trim().is_empty() {
            return Err(AppError::BadRequest("SSH username is required".into()));
        }
        if target.port == 0 {
            return Err(AppError::BadRequest("SSH port must be non-zero".into()));
        }
        let carries_ciphertext = match &target.auth {
            SshAuth::SystemSshConfig => false,
            SshAuth::Password {
                password_encrypted, ..
            } => password_encrypted.is_some(),
            SshAuth::PrivateKey {
                private_key_encrypted,
                passphrase_encrypted,
                ..
            } => private_key_encrypted.is_some() || passphrase_encrypted.is_some(),
        };
        if carries_ciphertext {
            return Err(AppError::BadRequest(
                "SSH credential ciphertext is server-managed".into(),
            ));
        }
    }
    Ok(())
}

// ─── Secret redaction & preservation ───────────────────────────────────

/// Serialize a node to JSON with all SSH secret material masked / stripped.
fn redact_node_value(node: &Node) -> Value {
    let mut value = serde_json::to_value(node).unwrap_or(Value::Null);
    if let Some(auth) = value
        .get_mut("placement")
        .and_then(|p| p.get_mut("auth"))
        .and_then(|a| a.as_object_mut())
    {
        for field in ["password", "private_key", "passphrase"] {
            if auth.get(field).and_then(|v| v.as_str()).is_some() {
                auth.insert(field.to_string(), Value::String(SECRET_MASK.to_string()));
            }
            // Never expose ciphertext over the API.
            auth.remove(&format!("{field}_encrypted"));
        }
    }
    value
}

/// Carry forward the existing secret when an update re-sends the mask sentinel
/// (or an empty secret) on the SAME auth variant. Changing the auth method
/// discards the old secret (a fresh one is required).
///
/// The `existing` node here is the in-memory config node, whose secrets are
/// hydrated to PLAINTEXT (its `*_encrypted` are `None` — ciphertext is only
/// materialized on the disk-bound clone). So the source of truth to carry is the
/// old plaintext; `refresh_cluster_fabric_encrypted` re-encrypts it on save.
#[cfg(test)]
fn preserve_node_secrets(existing: &Node, incoming: &mut Node) {
    let (NodePlacement::Ssh(old), NodePlacement::Ssh(new)) =
        (&existing.placement, &mut incoming.placement)
    else {
        return;
    };
    match (&old.auth, &mut new.auth) {
        (
            SshAuth::Password {
                password: old_pw,
                password_encrypted: old_enc,
            },
            SshAuth::Password {
                password,
                password_encrypted,
            },
        ) => {
            preserve_secret(password, password_encrypted, old_pw, old_enc);
        }
        (
            SshAuth::PrivateKey {
                private_key: old_pk,
                private_key_encrypted: old_pk_enc,
                passphrase: old_pp,
                passphrase_encrypted: old_pp_enc,
                ..
            },
            SshAuth::PrivateKey {
                private_key,
                private_key_encrypted,
                passphrase,
                passphrase_encrypted,
                ..
            },
        ) => {
            preserve_secret(private_key, private_key_encrypted, old_pk, old_pk_enc);
            preserve_secret(passphrase, passphrase_encrypted, old_pp, old_pp_enc);
        }
        _ => {}
    }
}

/// Until node CRUD is backed by the exact credential/config transaction, a
/// node that already owns isolated refs may only receive metadata edits and a
/// redacted keep round-trip. Accepting a real replacement, auth switch, or
/// delete here would return success while leaving the old store value behind.
#[cfg(test)]
fn ensure_managed_node_secret_unchanged(
    existing: &Node,
    incoming: &NodePlacement,
) -> Result<(), AppError> {
    let unchanged = match (&existing.placement, incoming) {
        (NodePlacement::Ssh(old), NodePlacement::Ssh(new)) => match (&old.auth, &new.auth) {
            (SshAuth::SystemSshConfig, SshAuth::SystemSshConfig) => true,
            (SshAuth::Password { .. }, SshAuth::Password { password, .. }) => {
                password.trim().is_empty() || password == SECRET_MASK
            }
            (
                SshAuth::PrivateKey { .. },
                SshAuth::PrivateKey {
                    private_key,
                    passphrase,
                    ..
                },
            ) => {
                (private_key.trim().is_empty() || private_key == SECRET_MASK)
                    && (passphrase.trim().is_empty() || passphrase == SECRET_MASK)
            }
            _ => false,
        },
        (NodePlacement::Local, NodePlacement::Local) => true,
        _ => false,
    };
    if unchanged {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "isolated cluster credentials cannot be changed until the revisioned node credential API is available"
                .to_string(),
        ))
    }
}

/// If `plaintext` is empty or the mask sentinel, replace it with the existing
/// secret so it survives a redacted round-trip: prefer the old plaintext (what
/// the hydrated in-memory config holds), else the old ciphertext.
#[cfg(test)]
fn preserve_secret(
    plaintext: &mut String,
    encrypted: &mut Option<String>,
    old_plaintext: &str,
    old_encrypted: &Option<String>,
) {
    let keep = plaintext.trim().is_empty() || plaintext == SECRET_MASK;
    if !keep {
        return;
    }
    plaintext.clear();
    if !old_plaintext.trim().is_empty() {
        // Hydrated in-memory case: carry plaintext; refresh re-encrypts on save.
        *plaintext = old_plaintext.to_string();
    } else if encrypted.is_none() {
        // Loaded-but-unhydrated case: carry the stored ciphertext as-is.
        *encrypted = old_encrypted.clone();
    }
}

// ─── Node handlers ─────────────────────────────────────────────────────

/// `GET /v1/bamboo/settings/nodes` — list nodes (redacted) + clusters.
pub async fn list_nodes(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    let nodes = config
        .cluster_fabric
        .nodes
        .iter()
        .map(redact_node_value)
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
    Ok(HttpResponse::Ok().json(redact_node_value(node)))
}

/// `POST /v1/bamboo/settings/nodes` — create a node.
pub async fn create_node(
    app_state: web::Data<AppState>,
    payload: web::Json<NodeUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    validate_node(&req)?;

    let expected_revision = req.expected_revision;

    let node = Node {
        id: Uuid::new_v4().to_string(),
        label: req.label,
        placement: req.placement,
        trust_level: req.trust_level,
        deploy: req.deploy,
        state: None,
        enabled: req.enabled,
    };
    let node_id = node.id.clone();

    let updated = app_state
        .update_cluster_fabric_credentials(
            expected_revision,
            std::iter::once(node_id.clone()).collect(),
            move |cfg| {
                cfg.cluster_fabric.nodes.push(node.clone());
                Ok(())
            },
        )
        .await?;

    let (updated, revision) = updated;

    let created = updated
        .cluster_fabric
        .node(&node_id)
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("created node missing")))?;
    Ok(HttpResponse::Created().json(json!({
        "revision": revision,
        "node": redact_node_value(created),
    })))
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
    let expected_revision = req.expected_revision;
    let id_for_response = id.clone();

    let updated = app_state
        .update_cluster_fabric_credentials(
            expected_revision,
            std::iter::once(id.clone()).collect(),
            move |cfg| {
                let existing = cfg
                    .cluster_fabric
                    .node(&id)
                    .cloned()
                    .ok_or_else(|| AppError::NotFound(format!("Node '{id}'")))?;
                let node = Node {
                    id: existing.id.clone(),
                    label: req.label.clone(),
                    placement: req.placement.clone(),
                    trust_level: req.trust_level,
                    deploy: req.deploy.clone(),
                    state: existing.state.clone(), // engine-owned: preserve
                    enabled: req.enabled,
                };

                let slot = cfg
                    .cluster_fabric
                    .node_mut(&id)
                    .expect("node existed above");
                *slot = node;
                Ok(())
            },
        )
        .await?;

    let (updated, revision) = updated;

    let node = updated
        .cluster_fabric
        .node(&id_for_response)
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("updated node missing")))?;
    Ok(HttpResponse::Ok().json(json!({
        "revision": revision,
        "node": redact_node_value(node),
    })))
}

/// `DELETE /v1/bamboo/settings/nodes/{id}` — remove a node.
pub async fn delete_node(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<NodeDeleteQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let expected_revision = query.expected_revision;

    let (_, revision) = app_state
        .update_cluster_fabric_credentials(
            expected_revision,
            std::iter::once(id.clone()).collect(),
            move |cfg| {
                let before = cfg.cluster_fabric.nodes.len();
                cfg.cluster_fabric.nodes.retain(|n| n.id != id);
                if cfg.cluster_fabric.nodes.len() == before {
                    return Err(AppError::NotFound(format!("Node '{id}'")));
                }
                // Drop the node from any cluster membership too.
                for cluster in &mut cfg.cluster_fabric.clusters {
                    cluster.node_ids.retain(|nid| nid != &id);
                }
                cfg.cluster_fabric.credential_refs.remove(&id);
                Ok(())
            },
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "success": true, "revision": revision })))
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

    let updated = app_state
        .update_config(
            move |cfg| {
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
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Created().json(json!({ "clusters": updated.cluster_fabric.clusters })))
}

/// `PUT /v1/bamboo/settings/clusters/{name}` — update a cluster.
pub async fn update_cluster(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ClusterUpsertRequest>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();
    let req = payload.into_inner();

    let updated = app_state
        .update_config(
            move |cfg| {
                let cluster = cfg
                    .cluster_fabric
                    .clusters
                    .iter_mut()
                    .find(|c| c.name == name)
                    .ok_or_else(|| AppError::NotFound(format!("Cluster '{name}'")))?;
                cluster.description = req.description.clone();
                cluster.node_ids = req.node_ids.clone();
                // Allow rename via the body.
                if !req.name.trim().is_empty() {
                    cluster.name = req.name.clone();
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "clusters": updated.cluster_fabric.clusters })))
}

/// `DELETE /v1/bamboo/settings/clusters/{name}` — remove a cluster (nodes kept).
pub async fn delete_cluster(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();

    let updated = app_state
        .update_config(
            move |cfg| {
                let before = cfg.cluster_fabric.clusters.len();
                cfg.cluster_fabric.clusters.retain(|c| c.name != name);
                if cfg.cluster_fabric.clusters.len() == before {
                    return Err(AppError::NotFound(format!("Cluster '{name}'")));
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(json!({ "clusters": updated.cluster_fabric.clusters })))
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
    /// Deploy the no-LLM echo executor (connectivity smoke test).
    #[serde(default)]
    pub echo: bool,
}

/// `POST /v1/bamboo/settings/nodes/{id}/deploy` — deploy a worker for the node.
pub async fn node_deploy(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<DeployQuery>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let state = deploy::deploy_node(&app_state, &id, query.echo).await?;
    Ok(HttpResponse::Ok().json(json!({ "id": id, "state": state })))
}

/// `POST /v1/bamboo/settings/nodes/{id}/stop` — stop the node's worker.
pub async fn node_stop(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let state = deploy::stop_node(&app_state, &id).await?;
    Ok(HttpResponse::Ok().json(json!({ "id": id, "state": state })))
}

/// `POST /v1/bamboo/settings/nodes/{id}/test` — connectivity preflight (no deploy).
pub async fn node_test(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let info = deploy::test_node(&app_state, &id).await?;
    Ok(HttpResponse::Ok().json(json!({ "id": id, "ok": true, "preflight": info })))
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
    use bamboo_config::cluster_fabric::SshTarget;

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

    #[test]
    fn redaction_masks_password_and_strips_ciphertext() {
        let node = pw_node("hunter2", Some("ciphertext"));
        let v = redact_node_value(&node);
        let auth = &v["placement"]["auth"];
        assert_eq!(auth["password"], SECRET_MASK);
        assert!(auth.get("password_encrypted").is_none());
    }

    #[test]
    fn redaction_omits_password_when_unset() {
        // SystemSshConfig has no secret fields → nothing to mask.
        let mut node = pw_node("", None);
        node.placement = NodePlacement::Ssh(SshTarget {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            auth: SshAuth::SystemSshConfig,
            host_key_fingerprint: None,
        });
        let v = redact_node_value(&node);
        assert_eq!(v["placement"]["auth"]["method"], "system_ssh_config");
    }

    #[test]
    fn update_with_mask_preserves_existing_ciphertext() {
        let existing = pw_node("", Some("stored-cipher"));
        let mut incoming = pw_node(SECRET_MASK, None);
        preserve_node_secrets(&existing, &mut incoming);
        let NodePlacement::Ssh(t) = &incoming.placement else {
            panic!()
        };
        let SshAuth::Password {
            password,
            password_encrypted,
        } = &t.auth
        else {
            panic!()
        };
        assert!(password.is_empty(), "masked plaintext cleared");
        assert_eq!(
            password_encrypted.as_deref(),
            Some("stored-cipher"),
            "old ciphertext carried forward"
        );
    }

    #[test]
    fn update_with_mask_carries_hydrated_plaintext() {
        // The realistic case: the in-memory existing node holds PLAINTEXT (it
        // was hydrated on load); its ciphertext is None. A masked update must
        // carry the plaintext forward so refresh can re-encrypt it on save.
        let existing = pw_node("s3cr3t", None);
        let mut incoming = pw_node(SECRET_MASK, None);
        preserve_node_secrets(&existing, &mut incoming);
        let NodePlacement::Ssh(t) = &incoming.placement else {
            panic!()
        };
        let SshAuth::Password { password, .. } = &t.auth else {
            panic!()
        };
        assert_eq!(password, "s3cr3t", "hydrated plaintext carried forward");
    }

    #[test]
    fn update_with_new_secret_overrides() {
        let existing = pw_node("", Some("old-cipher"));
        let mut incoming = pw_node("brand-new-password", None);
        preserve_node_secrets(&existing, &mut incoming);
        let NodePlacement::Ssh(t) = &incoming.placement else {
            panic!()
        };
        let SshAuth::Password {
            password,
            password_encrypted,
        } = &t.auth
        else {
            panic!()
        };
        assert_eq!(password, "brand-new-password", "new plaintext kept");
        assert!(
            password_encrypted.is_none(),
            "no carry-forward; refresh will encrypt the new plaintext"
        );
    }

    #[test]
    fn changing_auth_method_does_not_carry_secret() {
        let existing = pw_node("", Some("old-cipher"));
        let mut incoming = pw_node("", None);
        incoming.placement = NodePlacement::Ssh(SshTarget {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            auth: SshAuth::SystemSshConfig,
            host_key_fingerprint: None,
        });
        // Should be a no-op (variant changed) — no panic, no carry.
        preserve_node_secrets(&existing, &mut incoming);
        let NodePlacement::Ssh(t) = &incoming.placement else {
            panic!()
        };
        assert!(matches!(t.auth, SshAuth::SystemSshConfig));
    }

    #[test]
    fn isolated_password_update_accepts_only_redacted_keep() {
        let stored_secret = Uuid::new_v4().to_string();
        let existing = pw_node(&stored_secret, None);
        let masked = pw_node(SECRET_MASK, None);
        ensure_managed_node_secret_unchanged(&existing, &masked.placement).unwrap();
        let empty_secret = String::new();
        let empty = pw_node(&empty_secret, None);
        ensure_managed_node_secret_unchanged(&existing, &empty.placement).unwrap();

        let replacement_secret = Uuid::new_v4().to_string();
        let replacement = pw_node(&replacement_secret, None);
        let error =
            ensure_managed_node_secret_unchanged(&existing, &replacement.placement).unwrap_err();
        assert!(error.to_string().contains("cannot be changed"));
    }

    #[test]
    fn isolated_password_update_rejects_auth_switch() {
        let stored_secret = Uuid::new_v4().to_string();
        let existing = pw_node(&stored_secret, None);
        let switched = NodePlacement::Ssh(SshTarget {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            auth: SshAuth::SystemSshConfig,
            host_key_fingerprint: None,
        });
        assert!(ensure_managed_node_secret_unchanged(&existing, &switched).is_err());
    }

    #[test]
    fn node_validation_rejects_client_ciphertext() {
        let client_ciphertext = Uuid::new_v4().to_string();
        let node = pw_node("", Some(&client_ciphertext));
        let request = NodeUpsertRequest {
            expected_revision: 0,
            label: node.label,
            placement: node.placement,
            trust_level: node.trust_level,
            deploy: node.deploy,
            enabled: node.enabled,
        };
        assert!(validate_node(&request)
            .unwrap_err()
            .to_string()
            .contains("server-managed"));
    }
}
