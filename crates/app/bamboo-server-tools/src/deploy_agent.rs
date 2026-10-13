//! `deploy_agent` — the AI-callable "spin up a worker myself" tool.
//!
//! Standalone callers can deploy a broker worker on demand — as a local
//! subprocess, in Docker, or over SSH — and command it with `ask_agent`.
//! Host-bound local deployment creates a durable, cold Child ActorSession.
//! Its first task enters the canonical SessionInbox through `ask_agent`.
//! Docker/SSH still require an executor that can bind the canonical Session.
//!
//! Only registered on the Root surface when a broker is configured.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Mutex, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_broker::{
    AgentDeployment, DeployedAgent, Deployer, DockerDeployer, LocalProcessDeployer, SshDeployer,
};
use bamboo_config::Config;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFence, ActorActivationStatus, ActorDirectoryPort,
    ActorLogicalState, ActorPlacementClass, ActorPlacementRef, ActorSession, ProjectId, Session,
    SessionKind,
};
use bamboo_engine::session_app::child_session::{self, ChildSessionPort, CreateChildInput};
use bamboo_storage::SessionStoreV2;

/// Keeps deployed workers alive (the handles are kill-on-drop) and lets `stop`
/// tear them down. Shared for the server's lifetime.
pub type DeployedRegistry = Arc<Mutex<HashMap<String, Deployed>>>;

/// One live deployment: how it was deployed + the kill-on-drop handle.
pub struct Deployed {
    /// Host logical identity; None retains existing cluster/legacy compatibility.
    pub actor: Option<ActorSession>,
    /// Exact Host activation owning this physical worker; absent for legacy cluster peers.
    pub activation: Option<ActorActivationFence>,
    pub env: String,
    pub handle: DeployedAgent,
}

pub struct DeployAgentTool {
    broker_endpoint: String,
    broker_token: String,
    /// Path to the `bamboo` binary used for local subprocess deploys.
    bamboo_bin: PathBuf,
    registry: DeployedRegistry,
    /// Live config, read (never written) to resolve a scoped `ProvisionSpec`
    /// for `env=docker` deploys — the assigned model's credential only, never
    /// the whole config or the master encryption key (#46).
    config: Arc<RwLock<Config>>,
    actor_store: Option<Arc<SessionStoreV2>>,
    child_port: Option<Arc<dyn ChildSessionPort>>,
}

impl DeployAgentTool {
    pub fn new(
        broker_endpoint: impl Into<String>,
        broker_token: impl Into<String>,
        bamboo_bin: impl Into<PathBuf>,
        registry: DeployedRegistry,
        config: Arc<RwLock<Config>>,
    ) -> Self {
        Self {
            broker_endpoint: broker_endpoint.into(),
            broker_token: broker_token.into(),
            bamboo_bin: bamboo_bin.into(),
            registry,
            config,
            actor_store: None,
            child_port: None,
        }
    }
    /// Production uses the actual Host store, never a worker-local Session.
    pub fn with_actor_store(mut self, store: Arc<SessionStoreV2>) -> Self {
        self.actor_store = Some(store);
        self
    }

    /// Use the same Host child-creation boundary as SubAgent. A deployed
    /// logical Child is cold until its first canonical SessionInbox message.
    pub fn with_child_port(mut self, port: Arc<dyn ChildSessionPort>) -> Self {
        self.child_port = Some(port);
        self
    }
}

pub(crate) struct ResolvedDeployment {
    pub key: String,
    pub worker_id: String,
    pub actor: Option<ActorSession>,
    pub activation: Option<ActorActivationFence>,
}

/// Resolve a live physical handle only after checking its saved Host identity
/// and exact running activation. A configured Host store makes an unbound
/// broker peer, physical worker id, or stale alias invalid. This does not grant
/// transcript authority.
pub(crate) async fn resolve_deployed_target(
    registry: &DeployedRegistry,
    store: Option<&Arc<SessionStoreV2>>,
    caller: Option<&str>,
    target: &str,
) -> Result<Option<ResolvedDeployment>, ToolError> {
    resolve_target(registry, store, caller, target, false).await
}

/// Preserve the direct cluster/fabric compatibility route for a currently
/// registered node worker. A configured node is capacity, not an ActorSession;
/// this must not be used as a logical actor lookup or a general broker fallback.
pub(crate) async fn resolve_ask_target(
    registry: &DeployedRegistry,
    store: Option<&Arc<SessionStoreV2>>,
    caller: Option<&str>,
    target: &str,
) -> Result<Option<ResolvedDeployment>, ToolError> {
    resolve_target(registry, store, caller, target, true).await
}

/// A Host-created compatibility deployment is a normal direct Child, not a
/// broker mailbox. Reload both Sessions and Directory entries on every call;
/// no process-local registry entry is needed to address it after restart.
async fn host_logical_deployment(
    store: &SessionStoreV2,
    caller: &str,
    target: &str,
) -> Result<Option<(Session, ActorSession, ActorLogicalState)>, ToolError> {
    let parent = store
        .load_session(caller)
        .await
        .map_err(|_| ToolError::Execution("canonical deployment parent is unavailable".into()))?
        .ok_or_else(|| ToolError::Execution("canonical deployment parent is unavailable".into()))?;
    if parent.kind != SessionKind::Root || parent.id != caller {
        return Err(ToolError::Execution(
            "deployment caller is not a saved Root".into(),
        ));
    }
    let parent_actor = store
        .inspect_actor(caller)
        .await
        .map_err(|_| ToolError::Execution("canonical deployment parent is unavailable".into()))?
        .actor;
    if !parent_actor.matches_session(&parent) || parent_actor.state == ActorLogicalState::Retired {
        return Err(ToolError::Execution(
            "deployment parent Actor identity changed".into(),
        ));
    }
    let Some(child) = store
        .load_session(target)
        .await
        .map_err(|_| ToolError::Execution("canonical deployment target is unavailable".into()))?
    else {
        return Ok(None);
    };
    if child.kind != SessionKind::Child
        || child.parent_session_id.as_deref() != Some(caller)
        || child.root_session_id != caller
        || child.project_id_meta() != parent.project_id_meta()
        || child.metadata.get("deployment_kind").map(String::as_str) != Some("legacy_logical")
    {
        return Err(ToolError::Execution(
            "deployment target is not owned by the caller".into(),
        ));
    }
    let entry = store
        .inspect_actor(target)
        .await
        .map_err(|_| ToolError::Execution("canonical deployment Actor is unavailable".into()))?;
    let ancestor = entry.actor.ancestor_observations.first();
    if !entry.actor.matches_session(&child)
        || entry.actor.parent_actor_id.as_deref() != Some(caller)
        || entry.actor.root_actor_id != parent_actor.actor_id
        || ancestor.is_none_or(|ancestor| {
            ancestor.actor_id != caller || ancestor.session_created_at != parent.created_at
        })
    {
        return Err(ToolError::Execution(
            "deployment Actor identity or caller changed".into(),
        ));
    }
    let state = entry.actor.state;
    Ok(Some((child, entry.actor, state)))
}

async fn resolve_target(
    registry: &DeployedRegistry,
    store: Option<&Arc<SessionStoreV2>>,
    caller: Option<&str>,
    target: &str,
    allow_fabric_worker: bool,
) -> Result<Option<ResolvedDeployment>, ToolError> {
    let resolved = {
        let reg = registry.lock().await;
        let alias = crate::registry_keys::agent_key(target);
        let owned = reg
            .get_key_value(&alias)
            .filter(|(_, deployment)| store.is_none() || deployment.actor.is_some())
            .or_else(|| {
                reg.iter().find(|(_, deployment)| {
                    deployment.actor.as_ref().is_some_and(|actor| {
                        actor.actor_id == target
                            || (!allow_fabric_worker && deployment.handle.id == target)
                    })
                })
            });
        let fabric = (allow_fabric_worker && store.is_some()).then(|| {
            reg.iter().find(|(key, deployment)| {
                crate::registry_keys::split(key).0 == "node"
                    && deployment.actor.is_none()
                    && deployment.activation.is_none()
                    && deployment.handle.id == target
            })
        });
        owned
            .or_else(|| fabric.flatten())
            .map(|(key, d)| ResolvedDeployment {
                key: key.clone(),
                worker_id: d.handle.id.clone(),
                actor: d.actor.clone(),
                activation: d.activation.clone(),
            })
    };
    let Some(resolved) = resolved else {
        if store.is_some() || target.starts_with("actor-") {
            return Err(ToolError::Execution(
                "target is not a live deployment owned by the caller".into(),
            ));
        }
        return Ok(None);
    };
    if let Some(saved) = &resolved.actor {
        let invalid = || {
            ToolError::Execution("deployment Actor identity or caller is no longer valid".into())
        };
        if caller != saved.parent_actor_id.as_deref() {
            return Err(invalid());
        }
        let store = store.ok_or_else(invalid)?;
        let parent = store
            .load_session(caller.ok_or_else(invalid)?)
            .await
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        let current = store
            .inspect_actor(&saved.actor_id)
            .await
            .map_err(|_| invalid())?;
        let parent_birth = current
            .actor
            .ancestor_observations
            .first()
            .filter(|ancestor| ancestor.actor_id == parent.id)
            .map(|ancestor| ancestor.session_created_at);
        if parent_birth != Some(parent.created_at)
            || parent.project_id_meta() != saved.project_id
            || parent.id != saved.parent_actor_id.as_deref().unwrap_or_default()
        {
            return Err(invalid());
        }
        if current.actor.state != ActorLogicalState::Active
            || current.actor.session_created_at != saved.session_created_at
            || current.actor.parent_actor_id != saved.parent_actor_id
            || current.actor.root_actor_id != saved.root_actor_id
            || current.actor.project_id != saved.project_id
            || current.actor.spawn_depth != saved.spawn_depth
            || !current.activation.as_ref().is_some_and(|activation| {
                activation.status == ActorActivationStatus::Running
                    && resolved.activation.as_ref() == Some(&activation.fence())
                    && activation
                        .placement_ref
                        .as_ref()
                        .is_some_and(|placement| placement.lease_id == resolved.worker_id)
            })
        {
            return Err(invalid());
        }
        store
            .validate_fence(
                resolved.activation.as_ref().ok_or_else(invalid)?,
                chrono::Utc::now(),
            )
            .await
            .map_err(|_| invalid())?;
    } else if store.is_some() {
        // Cluster nodes remain a bounded physical compatibility route. Require
        // a real saved Root caller, never an arbitrary broker-provided identity.
        let invalid = || ToolError::Execution("cluster caller is not a saved Root".into());
        let caller = caller.ok_or_else(invalid)?;
        let root = store
            .ok_or_else(invalid)?
            .load_session(caller)
            .await
            .map_err(|_| invalid())?
            .filter(|session| session.id == caller && session.kind == SessionKind::Root)
            .ok_or_else(invalid)?;
        store
            .ok_or_else(invalid)?
            .inspect_actor(&root.id)
            .await
            .map_err(|_| invalid())?;
    }
    Ok(Some(resolved))
}

const RESIDENT_LEASE: chrono::Duration = chrono::Duration::minutes(5);
const RESIDENT_RENEW_INTERVAL: Duration = Duration::from_secs(60);

/// A lost Host authority stops the physical worker. The weak registry reference
/// lets server shutdown drop the kill-on-drop handle even while this task sleeps.
fn renew_resident_activation(
    registry: &DeployedRegistry,
    key: String,
    worker_id: String,
    store: Arc<SessionStoreV2>,
    fence: ActorActivationFence,
) {
    let registry = Arc::downgrade(registry);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RESIDENT_RENEW_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let Some(registry) = registry.upgrade() else {
                break;
            };
            let process_exited = registry.lock().await.get_mut(&key).and_then(|deployment| {
                (deployment.handle.id == worker_id
                    && deployment.activation.as_ref() == Some(&fence))
                .then(|| {
                    deployment
                        .handle
                        .process_exited()
                        .unwrap_or(Some(true))
                        .unwrap_or(false)
                })
            });
            let Some(process_exited) = process_exited else {
                break;
            };
            let now = chrono::Utc::now();
            if !process_exited
                && store
                    .renew_activation(&fence, now, now + RESIDENT_LEASE)
                    .await
                    .is_ok()
            {
                continue;
            }
            let removed = {
                let mut live = registry.lock().await;
                if live.get(&key).is_some_and(|deployment| {
                    deployment.handle.id == worker_id
                        && deployment.activation.as_ref() == Some(&fence)
                }) {
                    live.remove(&key)
                } else {
                    None
                }
            };
            if let Some(deployment) = removed {
                tracing::warn!(actor_id = %fence.actor_id, process_exited, "resident worker lost Host activation; stopping worker");
                deployment.handle.shutdown().await;
                let _ = store
                    .retire_actor_if_activation(&fence, chrono::Utc::now())
                    .await;
            }
            break;
        }
    });
}

/// Parameters for `action=deploy`, grouped so the deploy call stays tidy.
#[derive(Debug, Deserialize)]
struct DeployParams {
    /// Optional live deployment alias, never the Host ActorId or physical mailbox.
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    role: Option<String>,
    /// `provider:model` for the worker's agent (ignored when `echo`).
    #[serde(default)]
    model: Option<String>,
    /// Where to run it: `local` (default), `docker`, or `ssh`.
    #[serde(default)]
    env: Option<String>,
    /// Docker image (required when `env=docker`).
    #[serde(default)]
    image: Option<String>,
    /// Remote host (required when `env=ssh`).
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
    /// Run the dependency-free echo executor (no LLM) — smoke/testing.
    #[serde(default)]
    echo: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum DeployArgs {
    /// Deploy a new worker and return its id.
    Deploy(DeployParams),
    /// Stop a previously-deployed worker and remove it.
    Stop { id: String },
    /// List currently-deployed workers.
    List,
}

impl DeployAgentTool {
    async fn deploy_host_local(
        &self,
        params: DeployParams,
        caller: Option<&str>,
    ) -> Result<ToolResult, ToolError> {
        let store = self
            .actor_store
            .as_ref()
            .ok_or_else(|| ToolError::Execution("canonical Actor store is unavailable".into()))?;
        let port = self.child_port.as_ref().ok_or_else(|| {
            ToolError::Execution("canonical Child creation is unavailable".into())
        })?;
        if params.env.as_deref().is_some_and(|env| env != "local")
            || params.image.is_some()
            || params.host.is_some()
        {
            return Err(ToolError::InvalidArguments(
                "Host-bound deploy currently supports local ActorSessions only".into(),
            ));
        }
        if params.echo {
            return Err(ToolError::InvalidArguments(
                "echo workers have no canonical Child Session executor".into(),
            ));
        }
        if params.id.is_some() {
            return Err(ToolError::InvalidArguments(
                "physical deployment aliases are unavailable for canonical Child creation; use the returned ActorId"
                    .into(),
            ));
        }
        let caller = caller
            .ok_or_else(|| ToolError::Execution("deploy requires a saved calling Root".into()))?;
        let parent = port
            .load_root_session(caller)
            .await
            .map_err(|_| ToolError::Execution("deployment parent could not be loaded".into()))?;
        if parent.id != caller || parent.kind != SessionKind::Root {
            return Err(ToolError::Execution(
                "deploy requires a saved calling Root".into(),
            ));
        }
        let parent_actor = store.ensure_actor(caller).await.map_err(|_| {
            ToolError::Execution("deployment parent Actor authority is unavailable".into())
        })?;
        if !parent_actor.actor.matches_session(&parent)
            || parent_actor.actor.state == ActorLogicalState::Retired
        {
            return Err(ToolError::Execution(
                "deployment parent Actor identity changed".into(),
            ));
        }
        let role = params.role.unwrap_or_else(|| "worker".into());
        if role.is_empty()
            || role.len() > 128
            || !role.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            return Err(ToolError::InvalidArguments(
                "role must be a bounded ASCII name using letters, digits, '_' or '-'".into(),
            ));
        }
        let model_ref_override = match params.model.as_deref() {
            None => None,
            Some(model) => {
                let model = model.trim();
                if model.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "model must be non-empty".into(),
                    ));
                }
                let (provider, model) = if let Some((provider, model)) = model.split_once(':') {
                    if provider.trim().is_empty() || model.trim().is_empty() {
                        return Err(ToolError::InvalidArguments(
                            "model must be provider:model with both parts non-empty".into(),
                        ));
                    }
                    (provider.trim().to_owned(), model.trim().to_owned())
                } else {
                    let provider = parent.provider_name().ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "bare model needs a parent provider; use provider:model".into(),
                        )
                    })?;
                    (provider, model.to_owned())
                };
                Some(bamboo_domain::ProviderModelRef::new(provider, model))
            }
        };
        let (workspace, workspace_source) = port
            .resolve_child_workspace(&parent, params.workspace.as_deref())
            .await
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        let child_id = format!("actor-{}", uuid::Uuid::new_v4());
        let result = child_session::create_child_action(
            port.as_ref(),
            CreateChildInput {
                parent_session: parent,
                child_id,
                title: format!("Deployed {role}"),
                responsibility: "Handle tasks sent by the direct parent through the logical SessionInbox."
                    .into(),
                assignment_prompt: "Wait for the direct parent's first concrete task. Do not infer additional scope from this deployment."
                    .into(),
                subagent_type: role,
                workspace,
                workspace_source,
                model_override: model_ref_override.as_ref().map(|model| model.model.clone()),
                model_ref_override,
                runtime_metadata: std::collections::HashMap::from([(
                    "deployment_kind".into(),
                    "legacy_logical".into(),
                )]),
                read_only: false,
                auto_run: false,
                reasoning_effort: None,
                lifecycle: Some("resident".into()),
                resident_name: None,
                resident_context: Some("accumulate".into()),
                disabled_tools: None,
                context_fork: None,
            },
        )
        .await
        .map_err(|error| ToolError::Execution(error.to_string()))?;
        let child = store
            .load_session(&result.child_session_id)
            .await
            .map_err(|_| ToolError::Execution("created Child persistence is unavailable".into()))?
            .ok_or_else(|| {
                ToolError::Execution("created Child persistence is unavailable".into())
            })?;
        let actor = store
            .ensure_actor(&child.id)
            .await
            .map_err(|_| {
                ToolError::Execution("created Child Actor authority is unavailable".into())
            })?
            .actor;
        if !actor.matches_session(&child)
            || actor.parent_actor_id.as_deref() != Some(caller)
            || child.metadata.get("deployment_kind").map(String::as_str) != Some("legacy_logical")
        {
            return Err(ToolError::Execution(
                "created Child Actor authority is inconsistent".into(),
            ));
        }
        Ok(tool_json(json!({
            "id": actor.actor_id,
            "env": "local",
            "status": "cold",
            "note": "ActorSession is durable and awaits its first ask_agent(target=<ActorId>, mode=steer) task",
        })))
    }

    async fn deploy(
        &self,
        params: DeployParams,
        caller: Option<&str>,
    ) -> Result<ToolResult, ToolError> {
        if self.actor_store.is_some() {
            return self.deploy_host_local(params, caller).await;
        }
        let DeployParams {
            id,
            role,
            model,
            env,
            image,
            host,
            workspace,
            echo,
        } = params;
        let id = id.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| {
            format!("agent-{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
        });
        let alias = id;
        if self.actor_store.is_some() && alias.starts_with("actor-") {
            return Err(ToolError::InvalidArguments(
                "deployment aliases cannot use the logical Actor namespace".into(),
            ));
        }
        let key = crate::registry_keys::agent_key(&alias);
        if self.registry.lock().await.contains_key(&key) {
            return Err(ToolError::InvalidArguments(
                "deployment alias is already live".into(),
            ));
        }
        let id = if self.actor_store.is_some() {
            format!("worker-{}", uuid::Uuid::new_v4())
        } else {
            alias.clone()
        };
        let env = env.unwrap_or_else(|| "local".to_string());

        // A container cannot reach the host's loopback; for docker, address the
        // broker via host.docker.internal (the deployer maps it to the host
        // gateway). local/ssh keep the configured endpoint as-is. Computed
        // before the docker spec build below, which also ships this endpoint
        // (as the worker's `bus` target) inside the ProvisionSpec.
        let broker_endpoint = if env == "docker" {
            self.broker_endpoint
                .replace("127.0.0.1", "host.docker.internal")
                .replace("localhost", "host.docker.internal")
        } else {
            self.broker_endpoint.clone()
        };

        // docker=only: a parent-resolved ProvisionSpec (assigned model's
        // credential, no encryption key, no full config) shipped over a
        // one-shot stdin pipe — NOT the orchestrator's whole `~/.bamboo` home
        // (#46). `None` falls back to a homeless, credential-less container
        // (fine for `echo=true` smoke tests; a real worker then has nothing to
        // authenticate with, which surfaces at the presence-verify/first-task
        // step rather than silently handing over every provider key).
        let mut spec_json: Option<String> = None;

        let deployer: Box<dyn Deployer> = match env.as_str() {
            "local" => Box::new(LocalProcessDeployer::new(self.bamboo_bin.clone())),
            "docker" => {
                let image = image.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                    ToolError::InvalidArguments("env=docker requires `image`".to_string())
                })?;
                {
                    let cfg = self.config.read().await;
                    spec_json = crate::fabric_deploy::build_ondemand_provision_spec(
                        &id,
                        role.as_deref(),
                        model.as_deref(),
                        workspace.as_deref(),
                        std::env::temp_dir().join("bamboo-docker-agents").join(&id),
                        &broker_endpoint,
                        &self.broker_token,
                        &cfg,
                        echo,
                    );
                }
                // No `--network host`: the worker stays on an isolated bridge
                // network and reaches the host broker via host.docker.internal
                // (DockerDeployer adds the host-gateway alias above). No home
                // mount either — the spec built above is the worker's only
                // source of model/creds/MCP-proxy.
                Box::new(DockerDeployer::new(image))
            }
            "ssh" => {
                let host = host.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                    ToolError::InvalidArguments("env=ssh requires `host`".to_string())
                })?;
                Box::new(SshDeployer::new(host))
            }
            other => {
                return Err(ToolError::InvalidArguments(format!(
                    "unknown env '{other}' (use local|docker|ssh)"
                )))
            }
        };

        let deployment = AgentDeployment {
            id: id.clone(),
            role,
            broker_endpoint,
            token: self.broker_token.clone(),
            model,
            workspace,
            echo,
            // Deployed workers proxy MCP to the orchestrator (single MCP host).
            mcp_proxy: Some(bamboo_broker::ORCHESTRATOR_ID.to_string()),
            log_path: None,
            spec_json,
            // No self-signed CA to trust: on-demand-deploy targets (local/docker/
            // ssh via this tool) don't yet expose a per-deploy `--tls-ca-cert`
            // param; `wss://` still works here against a CA-signed broker cert
            // (or a self-signed one whose CA is already in the OS trust store).
            tls_ca_cert: None,
        };
        let (actor, activation) = if let Some(store) = &self.actor_store {
            let caller = caller.ok_or_else(|| {
                ToolError::Execution("deploy requires a saved calling Root".into())
            })?;
            let parent = store
                .load_session(caller)
                .await
                .map_err(|_| ToolError::Execution("deployment parent could not be loaded".into()))?
                .filter(|parent| parent.id == caller && parent.kind == SessionKind::Root)
                .ok_or_else(|| {
                    ToolError::Execution("deploy requires a saved calling Root".into())
                })?;
            let project = parent
                .project_id_meta()
                .map(ProjectId::parse)
                .transpose()
                .map_err(|_| ToolError::Execution("deployment parent Project is invalid".into()))?;
            let mut child = Session::new_child_of(
                format!("actor-{}", uuid::Uuid::new_v4()),
                &parent,
                deployment
                    .model
                    .clone()
                    .unwrap_or_else(|| parent.model.clone()),
                "Deployed resident",
            );
            child.metadata.insert("lifecycle".into(), "resident".into());
            child
                .metadata
                .insert("deployment_kind".into(), "legacy_broker".into());
            if let Some(project) = project {
                child.set_project_id_meta(project.as_str());
            }
            store.save_session(&child).await.map_err(|_| {
                ToolError::Execution(
                    "deployment Actor Session could not be persisted; no worker launched".into(),
                )
            })?;
            let actor = store
                .ensure_actor(&child.id)
                .await
                .map_err(|_| {
                    ToolError::Execution(
                        "deployment Actor identity could not be persisted; no worker launched"
                            .into(),
                    )
                })?
                .actor;
            let now = chrono::Utc::now();
            let placement_class = match env.as_str() {
                "local" => ActorPlacementClass::Local,
                "docker" => ActorPlacementClass::Docker,
                "ssh" => ActorPlacementClass::Ssh,
                _ => unreachable!("deployment environment was validated above"),
            };
            let activation = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: actor.actor_id.clone(),
                    run_id: format!("resident-{}", uuid::Uuid::new_v4()),
                    lease_owner: format!("host-resident-{}", uuid::Uuid::new_v4()),
                    lease_expires_at: now + RESIDENT_LEASE,
                    inbox_generation: 0,
                    placement_ref: Some(ActorPlacementRef {
                        class: placement_class,
                        lease_id: id.clone(),
                        slot_epoch: None,
                    }),
                    now,
                })
                .await;
            let activation = match activation {
                Ok(activation) => activation,
                Err(_) => {
                    let _ = store
                        .retire_actor(&actor.actor_id, chrono::Utc::now())
                        .await;
                    return Err(ToolError::Execution(
                        "deployment Actor activation could not be reserved; no worker launched"
                            .into(),
                    ));
                }
            };
            (Some(actor), Some(activation.fence()))
        } else {
            (None, None)
        };
        let handle = match deployer.deploy(&deployment).await {
            Ok(handle) => handle,
            Err(error) => {
                if let (Some(store), Some(fence)) = (&self.actor_store, &activation) {
                    let _ = store
                        .retire_actor_if_activation(fence, chrono::Utc::now())
                        .await;
                    tracing::warn!(%error, "deployment launcher failed after logical identity persistence");
                    return Err(ToolError::Execution(
                        "deployment launcher failed; logical identity was not reported as running"
                            .into(),
                    ));
                }
                return Err(ToolError::Execution(format!(
                    "deploy '{id}' ({env}) failed: {error}"
                )));
            }
        };
        let public_id = actor
            .as_ref()
            .map(|actor| actor.actor_id.clone())
            .unwrap_or_else(|| id.clone());

        if let (Some(store), Some(fence)) = (&self.actor_store, &activation) {
            if store
                .start_activation(fence, chrono::Utc::now())
                .await
                .is_err()
            {
                handle.shutdown().await;
                let _ = store
                    .retire_actor_if_activation(fence, chrono::Utc::now())
                    .await;
                return Err(ToolError::Execution(
                    "deployment Actor activation could not start; worker was stopped".into(),
                ));
            }
        }

        // Namespace the registry key so an agent-chosen id can never collide
        // with a cluster-fabric node id in the SHARED registry (cross-eviction).
        let pending = Deployed {
            actor,
            activation: activation.clone(),
            env: env.clone(),
            handle,
        };
        let duplicate = {
            let mut registry = self.registry.lock().await;
            match registry.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(pending);
                    None
                }
                std::collections::hash_map::Entry::Occupied(_) => Some(pending),
            }
        };
        // Concurrent launches must never replace an already published handle.
        if let Some(duplicate) = duplicate {
            duplicate.handle.shutdown().await;
            if let (Some(store), Some(fence)) = (&self.actor_store, duplicate.activation) {
                let _ = store
                    .retire_actor_if_activation(&fence, chrono::Utc::now())
                    .await;
            }
            return Err(ToolError::Execution(
                "deployment alias became live during launch; the new worker was stopped".into(),
            ));
        }

        if let (Some(store), Some(fence)) = (&self.actor_store, activation) {
            renew_resident_activation(
                &self.registry,
                crate::registry_keys::agent_key(&alias),
                id.clone(),
                store.clone(),
                fence,
            );
        }

        Ok(tool_json(json!({
            "id": public_id,
            "env": env,
            "status": "deployed",
            "note": format!("deployment is connecting; use ask_agent(target=\"{public_id}\", ...)"),
        })))
    }

    async fn stop(&self, id: String, caller: Option<&str>) -> Result<ToolResult, ToolError> {
        if let Some(store) = &self.actor_store {
            let caller = caller
                .ok_or_else(|| ToolError::Execution("stop requires a saved calling Root".into()))?;
            let Some((child, actor, _state)) = host_logical_deployment(store, caller, &id).await?
            else {
                return Ok(tool_json(json!({ "id": id, "status": "not_found" })));
            };
            let port = self.child_port.as_ref().ok_or_else(|| {
                ToolError::Execution("canonical Child control is unavailable".into())
            })?;
            // Fence any new inbox activation before waiting for a running
            // worker. A retry still attempts cancellation if a prior wait failed.
            store
                .retire_actor(&child.id, chrono::Utc::now())
                .await
                .map_err(|_| ToolError::Execution("Actor retirement is unconfirmed".into()))?;
            port.cancel_child_run_and_wait(&child.id)
                .await
                .map_err(|_| ToolError::Execution("Child cancellation is unconfirmed".into()))?;
            return Ok(tool_json(
                json!({ "id": actor.actor_id, "status": "stopped" }),
            ));
        }
        // Take the entry out FIRST, then shut down without holding the registry
        // lock: shutdown is now graceful (SIGTERM + drain grace window, #49), so
        // it can take seconds — other deploy/stop/list calls must not serialize
        // behind it.
        let target =
            resolve_deployed_target(&self.registry, self.actor_store.as_ref(), caller, &id).await?;
        let Some(target) = target else {
            return Ok(tool_json(json!({ "id": id, "status": "not_found" })));
        };
        let removed = {
            let mut reg = self.registry.lock().await;
            if reg
                .get(&target.key)
                .is_some_and(|d| d.handle.id == target.worker_id)
            {
                reg.remove(&target.key)
            } else {
                None
            }
        };
        match removed {
            Some(d) => {
                d.handle.shutdown().await;
                let public_id = if let Some(actor) = d.actor {
                    let fence = d.activation.as_ref().ok_or_else(|| {
                        ToolError::Execution(
                            "worker stopped; Actor activation is unavailable".into(),
                        )
                    })?;
                    self.actor_store
                        .as_ref()
                        .ok_or_else(|| {
                            ToolError::Execution("deployment Host store is unavailable".into())
                        })?
                        .retire_actor_if_activation(fence, chrono::Utc::now())
                        .await
                        .map_err(|_| {
                            ToolError::Execution(
                                "worker stopped; Actor retirement is unconfirmed".into(),
                            )
                        })?;
                    actor.actor_id
                } else {
                    id
                };
                Ok(tool_json(json!({ "id": public_id, "status": "stopped" })))
            }
            None => Ok(tool_json(json!({ "id": id, "status": "not_found" }))),
        }
    }

    async fn list(&self, caller: Option<&str>) -> Result<ToolResult, ToolError> {
        if let Some(store) = &self.actor_store {
            let caller = caller
                .ok_or_else(|| ToolError::Execution("list requires a saved calling Root".into()))?;
            let mut agents = Vec::new();
            for entry in store.list_index_entries().await {
                if entry.kind != SessionKind::Child
                    || entry.parent_session_id.as_deref() != Some(caller)
                {
                    continue;
                }
                let child = store
                    .load_session(&entry.id)
                    .await
                    .map_err(|_| {
                        ToolError::Execution("canonical deployment list is unavailable".into())
                    })?
                    .ok_or_else(|| {
                        ToolError::Execution("canonical deployment list is inconsistent".into())
                    })?;
                if child.metadata.get("deployment_kind").map(String::as_str)
                    != Some("legacy_logical")
                {
                    continue;
                }
                if let Some((_, actor, state)) =
                    host_logical_deployment(store, caller, &entry.id).await?
                {
                    if state != ActorLogicalState::Retired {
                        agents.push(json!({
                            "id": actor.actor_id,
                            "source": "logical",
                            "env": "local",
                            "status": state,
                        }));
                    }
                }
                if agents.len() > 256 {
                    return Err(ToolError::Execution(
                        "logical deployment list exceeds its bound".into(),
                    ));
                }
            }
            return Ok(tool_json(json!({ "agents": agents })));
        }
        let entries: Vec<_> = {
            let registry = self.registry.lock().await;
            registry
                .iter()
                .map(|(key, deployment)| {
                    (
                        key.clone(),
                        deployment
                            .actor
                            .as_ref()
                            .map(|actor| actor.actor_id.clone()),
                        deployment.env.clone(),
                    )
                })
                .collect()
        };
        let mut agents = Vec::new();
        for (key, actor_id, env) in entries {
            // Never await canonical reads while holding the registry mutex.
            if let Some(actor_id) = &actor_id {
                if resolve_deployed_target(
                    &self.registry,
                    self.actor_store.as_ref(),
                    caller,
                    actor_id,
                )
                .await
                .ok()
                .flatten()
                .is_none()
                {
                    continue;
                }
            }
            let (source, alias) = crate::registry_keys::split(&key);
            let id = actor_id.as_deref().unwrap_or(alias);
            agents.push(json!({ "id": id, "source": source, "env": env }));
        }
        Ok(tool_json(json!({ "agents": agents })))
    }
}

fn tool_json(value: serde_json::Value) -> ToolResult {
    ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: None,
        images: Vec::new(),
    }
}

#[async_trait]
impl Tool for DeployAgentTool {
    fn name(&self) -> &str {
        "deploy_agent"
    }

    fn description(&self) -> &str {
        if self.actor_store.is_some() {
            return "Create and manage a durable local Child ActorSession. \
                    action=deploy returns a stable ActorId in cold state; give it a concrete \
                    task with ask_agent(target=<ActorId>, mode=steer). action=list shows direct \
                    deployments, and action=stop cancels and retires one. Host-bound Docker, SSH, \
                    echo, and custom deployment aliases are unavailable. Registered cluster node \
                    workers remain available through ask_agent.";
        }
        "Spin up a NEW worker agent on demand, anywhere, and manage its lifecycle. This is how you \
         scale yourself out: you deploy a fresh broker-agent, then drive it with ask_agent. The \
         worker connects back to the same message broker you are on, and inherits your MCP servers \
         + skills (via the orchestrator MCP proxy), so it can do real work — not just echo.\n\
         The returned ActorId names a durable Host resident; it does not claim an Actor activation.\n\
         \n\
         PREFER LOCAL. Default to a local `SubAgent` (an in-context child) for delegation. Reach for \
         a REMOTE worker (env=ssh, or a cluster node) ONLY when the task genuinely needs THAT \
         machine — its data, GPU, network location/proximity, or a clean sandbox. Remote adds a \
         binary upload, deploy cost, network latency, and can hit host firewalls; do not pick it by \
         default. Local-subprocess (env=local) is fine for extra parallel hands here.\n\
         \n\
         THREE PLACEMENTS (action=deploy, pick with `env`):\n\
         - env=local (default) — a subprocess on THIS machine. Fastest; use for extra parallel \
         hands here.\n\
         - env=docker — a container (requires `image`, e.g. \"bamboo:latest\"). Isolated; it gets \
         only the assigned model's credential (via a one-shot handoff, not your whole config) plus \
         your MCP servers proxied through you. Use for sandboxed or clean-env work.\n\
         - env=ssh — a process on a REMOTE host (requires `host`, e.g. \"user@box\"). Use to run \
         work near other machines/data or to borrow remote compute.\n\
         \n\
         OTHER ACTIONS: action=stop (id=…) tears a worker down and frees it; action=list shows the \
         workers you currently have running. Workers are kept alive until you stop them or the \
         server exits.\n\
         \n\
         WORKED EXAMPLE (scale out, use, tear down):\n\
         1. deploy_agent(action=deploy, env=local, role=\"tester\", model=\"anthropic:claude-opus-4-8\") \
         → returns logical id \"actor-…\".\n\
         2. ask_agent(target=\"actor-…\", question=\"Run the full test suite and report \
         failures.\", mode=steer).\n\
         3. deploy_agent(action=list) → confirm it (and any siblings) are running.\n\
         4. deploy_agent(action=stop, id=\"actor-…\") → once its work is collected.\n\
         \n\
         Tip: use echo=true to deploy a dependency-free no-LLM worker for a connectivity smoke test \
         before committing to a real model. Returned id is what you pass as ask_agent's `target`."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        let actions = json!(["deploy", "stop", "list"]);
        json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": actions },
                "id": { "type": "string", "description": "deploy: optional live alias (auto if omitted). stop: returned logical ActorId or its live alias." },
                "role": { "type": "string", "description": "deploy: role/profile label." },
                "model": { "type": "string", "description": "deploy: provider:model for the worker." },
                "env": { "type": "string", "enum": ["local", "docker", "ssh"], "description": "deploy: where to run (default local)." },
                "image": { "type": "string", "description": "deploy: docker image (env=docker)." },
                "host": { "type": "string", "description": "deploy: remote host (env=ssh)." },
                "workspace": { "type": "string", "description": "deploy: worker working directory." },
                "echo": { "type": "boolean", "description": "deploy: run the no-LLM echo executor (smoke)." }
            },
            "required": ["action"]
        })
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL.promotable()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: DeployArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid deploy_agent args: {e}")))?;
        if ctx.plan_read_only && !matches!(&parsed, DeployArgs::List) {
            return Err(ToolError::Execution(
                "deploy_agent mutations are unavailable in Plan mode".into(),
            ));
        }
        match parsed {
            DeployArgs::Deploy(params) => self.deploy(params, ctx.session_id()).await,
            DeployArgs::Stop { id } => self.stop(id, ctx.session_id()).await,
            DeployArgs::List => self.list(ctx.session_id()).await,
        }
        .map(ToolOutcome::Completed)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn empty_registry() -> DeployedRegistry {
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()))
    }

    fn tool_with(registry: DeployedRegistry) -> DeployAgentTool {
        // bamboo_bin is never spawned in these tests (we don't drive deploy()).
        DeployAgentTool::new(
            "ws://localhost:0",
            "test-token",
            "/bin/true",
            registry,
            std::sync::Arc::new(tokio::sync::RwLock::new(bamboo_config::Config::default())),
        )
    }

    /// A trivial long-running child so the kill/wait path is genuinely exercised.
    fn spawn_sleeper(id: &str, cleanup: Option<Vec<String>>) -> DeployedAgent {
        let child = tokio::process::Command::new("sleep")
            .arg("60")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn sleep");
        DeployedAgent::from_parts(id, child, cleanup)
    }

    /// True while `pid` is a live process (POSIX `kill -0`).
    fn pid_alive(pid: u32) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            // `kill -0` on a reaped pid prints "No such process" to stderr; that
            // stderr is the expected signal, not test noise — silence it.
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn parse(result: ToolResult) -> serde_json::Value {
        serde_json::from_str(&result.result).expect("tool result is JSON")
    }

    #[tokio::test]
    async fn deploy_list_stop_lifecycle_kills_process() {
        let registry = empty_registry();
        let tool = tool_with(registry.clone());

        // (1) register a worker (the registry effect of a successful deploy); list shows it.
        // Use the namespaced key so the tool's stop()/list() find it.
        let agent = spawn_sleeper("w1", None);
        let pid = agent.pid().expect("child has a pid");
        registry.lock().await.insert(
            crate::registry_keys::agent_key("w1"),
            Deployed {
                actor: None,
                activation: None,
                env: "local".into(),
                handle: agent,
            },
        );
        assert!(
            pid_alive(pid),
            "registered worker process should be running"
        );

        let listed = parse(tool.list(None).await.unwrap());
        let agents = listed["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0]["id"], "w1");
        assert_eq!(agents[0]["env"], "local");

        // (2) stop: removes the entry AND kills the process (shutdown awaits the child).
        let stopped = parse(tool.stop("w1".to_string(), None).await.unwrap());
        assert_eq!(stopped["id"], "w1");
        assert_eq!(stopped["status"], "stopped");
        assert!(!pid_alive(pid), "stopped worker process must be killed");

        // (3) list after stop is empty.
        let listed = parse(tool.list(None).await.unwrap());
        assert!(listed["agents"].as_array().unwrap().is_empty());

        // (4) double-stop (already removed) is a no-op, not a crash.
        let again = parse(tool.stop("w1".to_string(), None).await.unwrap());
        assert_eq!(again["status"], "not_found");
    }

    #[tokio::test]
    async fn host_bound_deploy_without_canonical_child_port_fails_before_persistence() {
        use bamboo_agent_core::storage::Storage;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(dir.path().into()).await.unwrap());
        let root = Session::new("saved-root", "echo-model");
        store.save_session(&root).await.unwrap();
        let registry = empty_registry();
        let tool = DeployAgentTool::new(
            "ws://127.0.0.1:1",
            "test-token",
            dir.path().join("absent-bamboo"),
            registry.clone(),
            Arc::new(RwLock::new(Config::default())),
        )
        .with_actor_store(store.clone());
        assert_eq!(
            tool.parameters_schema()["properties"]["action"]["enum"],
            json!(["deploy", "stop", "list"])
        );
        for env in ["local", "docker", "ssh"] {
            let params = serde_json::from_value(json!({"env":env,"echo":true})).unwrap();
            let error = tool.deploy(params, Some(&root.id)).await.unwrap_err();
            assert!(error
                .to_string()
                .contains("canonical Child creation is unavailable"));
        }
        assert!(registry.lock().await.is_empty());
        let children = store.sessions_root_dir().join(&root.id).join("children");
        assert!(!children.exists() || std::fs::read_dir(children).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn stop_unknown_id_is_not_found_not_a_crash() {
        let tool = tool_with(empty_registry());
        let r = parse(tool.stop("never-deployed".to_string(), None).await.unwrap());
        assert_eq!(r["status"], "not_found");
    }

    #[tokio::test]
    async fn deployed_agent_shutdown_kills_and_runs_cleanup() {
        // A unique marker the cleanup command will `touch` — proves cleanup ran.
        let marker = std::env::temp_dir().join(format!(
            "bamboo_deploy_cleanup_{}_{:?}.marker",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&marker);

        let agent = spawn_sleeper(
            "cleanup-worker",
            Some(vec![
                "sh".into(),
                "-c".into(),
                format!("touch {}", marker.display()),
            ]),
        );
        let pid = agent.pid().expect("child has a pid");

        agent.shutdown().await;

        assert!(!pid_alive(pid), "shutdown must kill the process");
        assert!(
            marker.exists(),
            "shutdown must run the cleanup command (docker rm -f path)"
        );
        let _ = std::fs::remove_file(&marker);
    }
}
