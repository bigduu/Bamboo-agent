//! Remote Cluster Fabric deploy engine.
//!
//! [`FabricDeployer`] is the SINGLE orchestration path — `deploy` / `stop` /
//! `test` / `read_logs` — shared by the operator HTTP handlers and the agent
//! [`crate::cluster_tool::ClusterTool`]. Both hold the same `Arc<FabricDeployer>`
//! so they share ONE worker registry (stop from either side sees the same
//! workers) and one persistence path. Living here (the one crate that sees both
//! `bamboo-config`'s `Node` and `bamboo-broker`'s deployers) keeps placement/
//! auth handling in one place.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

use bamboo_broker::{
    ask_agent, AgentDeployment, BrokerClient, Deployer, LocalProcessDeployer, RusshAuth,
    RusshDeployer, SshDeployer, UploadSpec, ORCHESTRATOR_ID,
};
use bamboo_config::cluster_fabric::{
    ClusterFabricConfig, Node, NodePlacement, NodeState, NodeStatus, SshAuth, SshTarget,
};
use bamboo_config::{
    BrokerClientConfig, Config, ConfigFacade, ConfigSectionEvent, ConfigStoreError,
    CredentialStatus, CredentialStore, CredentialStoreHealth, SectionEnvelope, SectionId,
    SectionSourceKind, SectionStatus,
};
use bamboo_subagent::{AgentRef, AskMode};

use crate::deploy_agent::{Deployed, DeployedRegistry};

/// Typed error so callers (HTTP / agent tool) can map to the right status/kind.
#[derive(Debug)]
pub enum FabricError {
    NotFound(String),
    BadRequest(String),
    Conflict {
        expected: u64,
        actual: u64,
    },
    /// The durable transaction committed, but its process-local publication
    /// invariant failed. Lifecycle callers must not compensate the already
    /// authoritative action as if the CAS had failed before commit.
    Committed(String),
    Internal(String),
}

impl std::fmt::Display for FabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FabricError::NotFound(m)
            | FabricError::BadRequest(m)
            | FabricError::Committed(m)
            | FabricError::Internal(m) => write!(f, "{m}"),
            FabricError::Conflict { expected, actual } => {
                write!(
                    f,
                    "cluster configuration conflict: expected revision {expected}, current revision {actual}"
                )
            }
        }
    }
}

type FabricResult<T> = Result<T, FabricError>;

async fn join_fabric_task<T>(
    context: &'static str,
    task: tokio::task::JoinHandle<FabricResult<T>>,
) -> FabricResult<T> {
    task.await
        .map_err(|error| FabricError::Internal(format!("{context} task failed: {error}")))?
}

fn map_config_store_error(error: ConfigStoreError) -> FabricError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            FabricError::Conflict { expected, actual }
        }
        ConfigStoreError::Validation(message) => FabricError::BadRequest(message),
        ConfigStoreError::Io(error) => {
            FabricError::Internal(format!("cluster configuration storage failed: {error}"))
        }
        ConfigStoreError::Json(_) => {
            FabricError::Internal("cluster configuration document is invalid".to_string())
        }
        ConfigStoreError::Watch(error) => {
            FabricError::Internal(format!("cluster configuration watch failed: {error}"))
        }
    }
}

/// Exact, adopted cluster-fabric snapshot bound to one lifecycle read or
/// durable mutation. This type deliberately has no `Debug`/`Serialize`
/// implementation: the runtime `Config` may contain hydrated SSH secrets and
/// must only be projected through the server's redacted response builder.
pub struct FabricCommitSnapshot {
    pub config: Config,
    pub section: SectionEnvelope<serde_json::Value>,
    pub credential_statuses: Vec<CredentialStatus>,
    pub credential_health: CredentialStoreHealth,
}

/// Lifecycle result paired with the exact cluster snapshot it observed or
/// committed. HTTP callers use the snapshot for their redacted envelope;
/// agent-tool callers can consume only `value`.
pub struct FabricActionResult<T> {
    pub value: T,
    pub snapshot: FabricCommitSnapshot,
}

type FabricEventPublisher = Arc<dyn Fn(&ConfigSectionEvent) + Send + Sync>;

#[cfg(test)]
type DeployBeforeFinalPersistTestHook = Box<dyn FnOnce(&Path) + Send + 'static>;

#[cfg(test)]
fn deploy_before_final_persist_test_hooks(
) -> &'static std::sync::Mutex<HashMap<PathBuf, DeployBeforeFinalPersistTestHook>> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<PathBuf, DeployBeforeFinalPersistTestHook>>,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_deploy_before_final_persist_test_hook(
    data_dir: &Path,
    hook: impl FnOnce(&Path) + Send + 'static,
) {
    deploy_before_final_persist_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_deploy_before_final_persist_test_hook(data_dir: &Path) {
    let hook = deploy_before_final_persist_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    if let Some(hook) = hook {
        hook(data_dir);
    }
}

#[cfg(test)]
struct AfterCommitBeforeAdoptionTestHook {
    expected_revision: u64,
    hook: Box<dyn FnOnce(&Path) + Send + 'static>,
}

#[cfg(test)]
fn after_commit_before_adoption_test_hooks(
) -> &'static std::sync::Mutex<HashMap<PathBuf, AfterCommitBeforeAdoptionTestHook>> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<PathBuf, AfterCommitBeforeAdoptionTestHook>>,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_after_commit_before_adoption_test_hook(
    data_dir: &Path,
    expected_revision: u64,
    hook: impl FnOnce(&Path) + Send + 'static,
) {
    after_commit_before_adoption_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            data_dir.to_path_buf(),
            AfterCommitBeforeAdoptionTestHook {
                expected_revision,
                hook: Box::new(hook),
            },
        );
}

#[cfg(test)]
fn run_after_commit_before_adoption_test_hook(data_dir: &Path, expected_revision: u64) {
    let hook = {
        let mut hooks = after_commit_before_adoption_test_hooks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if hooks
            .get(data_dir)
            .is_some_and(|hook| hook.expected_revision == expected_revision)
        {
            hooks.remove(data_dir)
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        (hook.hook)(data_dir);
    }
}

#[cfg(test)]
struct HealthAfterProbeTestHook {
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
fn health_after_probe_test_hooks(
) -> &'static std::sync::Mutex<HashMap<PathBuf, HealthAfterProbeTestHook>> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<PathBuf, HealthAfterProbeTestHook>>,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_health_after_probe_test_hook(
    data_dir: &Path,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    health_after_probe_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            data_dir.to_path_buf(),
            HealthAfterProbeTestHook { reached, release },
        );
}

#[cfg(test)]
async fn run_health_after_probe_test_hook(data_dir: &Path) {
    let hook = health_after_probe_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    if let Some(hook) = hook {
        let _ = hook.reached.send(());
        let _ = hook.release.await;
    }
}

/// The shared fabric deploy engine: turns persisted nodes into running
/// `broker-agent` workers, holding their handles + persisting `NodeState`.
#[derive(Clone)]
pub struct FabricDeployer {
    config: Arc<RwLock<Config>>,
    /// Serializes the mutate+persist of a fabric config write (same guarantee as
    /// `AppState::update_config`'s io-lock — #126).
    config_io_lock: Arc<Mutex<()>>,
    data_dir: PathBuf,
    /// Process-owned modular section authority. Production installs this
    /// together with the credential store and event publisher; legacy unit
    /// fixtures retain the compatibility fallback.
    config_facade: Option<Arc<ConfigFacade>>,
    credential_store: Option<Arc<CredentialStore>>,
    publish_event: Option<FabricEventPublisher>,
    /// Worker handles, keyed by node id — SHARED with `deploy_agent` so both
    /// surfaces see/manage the same workers.
    registry: DeployedRegistry,
    bamboo_bin: PathBuf,
    /// Per-node auto-recovery bookkeeping (debounce + backoff + attempt cap).
    /// Ephemeral: cleared on recovery and re-derived after a restart.
    recovery: Arc<Mutex<HashMap<String, RecoveryState>>>,
}

/// Auto-recovery state for one node (see [`FabricDeployer::recovery_decision`]).
#[derive(Default)]
struct RecoveryState {
    /// Consecutive Unreachable observations (the debounce counter).
    consecutive_unreachable: u32,
    /// Redeploy attempts made this outage.
    attempts: u32,
    /// Earliest time the next attempt may fire (exponential backoff gate).
    next_eligible: Option<tokio::time::Instant>,
    /// Set once the attempt cap is hit + the node marked Failed (don't repeat).
    gave_up: bool,
    /// A redeploy is currently running — don't launch an overlapping one (a slow
    /// SSH deploy can outlast the sweep interval).
    in_flight: bool,
}

/// Consecutive Unreachable probes before the first redeploy (ride out a blip).
const RECOVERY_DEBOUNCE: u32 = 2;
/// Redeploy attempts before giving up and marking the node Failed.
const RECOVERY_MAX_ATTEMPTS: u32 = 3;

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
            config_facade: None,
            credential_store: None,
            publish_event: None,
            registry,
            bamboo_bin: bamboo_bin.into(),
            recovery: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach the process-owned modular authorities used by production. Every
    /// durable lifecycle write then participates in the cluster-fabric section
    /// CAS and publishes the same authoritative section event as operator CRUD.
    pub fn with_modular_persistence(
        mut self,
        config_facade: Arc<ConfigFacade>,
        credential_store: Arc<CredentialStore>,
        publish_event: FabricEventPublisher,
    ) -> Self {
        self.config_facade = Some(config_facade);
        self.credential_store = Some(credential_store);
        self.publish_event = Some(publish_event);
        self
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

    fn credential_snapshot(&self) -> (Vec<CredentialStatus>, CredentialStoreHealth) {
        let store = self
            .credential_store
            .clone()
            .unwrap_or_else(|| Arc::new(CredentialStore::open(&self.data_dir)));
        match store.statuses_with_health() {
            Ok(snapshot) => snapshot,
            Err(_) => (
                Vec::new(),
                CredentialStoreHealth {
                    revision: 0,
                    status: SectionStatus::Degraded,
                    source: SectionSourceKind::Default,
                    last_error: Some("credential status is unavailable".to_string()),
                },
            ),
        }
    }

    async fn hydrate_cluster_fabric(
        &self,
        fabric: ClusterFabricConfig,
    ) -> FabricResult<ClusterFabricConfig> {
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut config = Config::default();
            config.cluster_fabric = fabric;
            config.hydrate_cluster_credentials_from_store(&data_dir)?;
            Ok::<_, ConfigStoreError>(config.cluster_fabric.clone())
        })
        .await
        .map_err(|error| {
            FabricError::Internal(format!("credential hydration task failed: {error}"))
        })?
        .map_err(map_config_store_error)
    }

    async fn snapshot_locked(&self, expected_revision: u64) -> FabricResult<FabricCommitSnapshot> {
        let mut config = self.config.read().await.clone();
        let section = if let Some(facade) = &self.config_facade {
            let actual = self.cluster_revision_locked();
            if expected_revision != actual {
                return Err(FabricError::Conflict {
                    expected: expected_revision,
                    actual,
                });
            }
            config.cluster_fabric = self
                .hydrate_cluster_fabric(facade.effective_config().cluster_fabric.clone())
                .await?;
            facade
                .registry()
                .envelope_value(SectionId::ClusterFabric)
                .map_err(map_config_store_error)?
        } else {
            if expected_revision != 0 {
                return Err(FabricError::Conflict {
                    expected: expected_revision,
                    actual: 0,
                });
            }
            SectionEnvelope {
                data: serde_json::Value::Null,
                revision: 0,
                loaded_at: chrono::Utc::now(),
                source_path: self.data_dir.join("config.json"),
                source_kind: SectionSourceKind::Default,
                status: SectionStatus::Healthy,
                last_error: None,
            }
        };
        let (credential_statuses, credential_health) = self.credential_snapshot();
        Ok(FabricCommitSnapshot {
            config,
            section,
            credential_statuses,
            credential_health,
        })
    }

    fn cluster_revision_locked(&self) -> u64 {
        self.config_facade
            .as_ref()
            .map(|facade| facade.registry().cluster_fabric.snapshot().revision)
            .unwrap_or(0)
    }

    pub async fn current_cluster_revision(&self) -> FabricResult<u64> {
        let _io = self.config_io_lock.lock().await;
        Ok(self.cluster_revision_locked())
    }

    /// Reconcile stale process-bound worker states during server startup using
    /// the process-owned section authority. The durable section commit happens
    /// before runtime adoption and event publication, and the same config lock
    /// prevents the health monitor or an operator mutation from racing boot.
    /// The detached task owns the whole transaction so cancellation of the
    /// server-construction future cannot strand disk ahead of the process
    /// facade/runtime publication.
    pub async fn reconcile_stale_nodes_on_boot(&self) -> FabricResult<usize> {
        let deployer = self.clone();
        join_fabric_task(
            "cluster-fabric boot reconcile",
            tokio::spawn(async move { deployer.reconcile_stale_nodes_on_boot_inner().await }),
        )
        .await
    }

    async fn reconcile_stale_nodes_on_boot_inner(&self) -> FabricResult<usize> {
        let _io = self.config_io_lock.lock().await;
        let stale = {
            let config = self.config.read().await;
            config
                .cluster_fabric
                .nodes
                .iter()
                .filter(|node| {
                    node.state.as_ref().is_some_and(|state| {
                        matches!(state.status, NodeStatus::Running | NodeStatus::Deploying)
                    })
                })
                .count()
        };
        if stale == 0 {
            return Ok(0);
        }

        let expected_revision = self.cluster_revision_locked();
        self.persist_node_update_locked(expected_revision, |config| {
            for node in &mut config.cluster_fabric.nodes {
                if let Some(state) = node.state.as_mut() {
                    if matches!(state.status, NodeStatus::Running | NodeStatus::Deploying) {
                        state.status = NodeStatus::Unreachable;
                        state.last_error =
                            Some("orchestrator restarted; worker no longer tracked".to_string());
                    }
                }
            }
            Ok(())
        })
        .await?;
        tracing::info!(
            reconciled = stale,
            revision = expected_revision + 1,
            "cluster-fabric: marked stale Running nodes Unreachable on boot"
        );
        Ok(stale)
    }

    /// Commit one engine-owned node mutation through the cluster-fabric
    /// section authority. Runtime publication occurs only after the durable
    /// CAS succeeds; the matching section event is emitted only after runtime
    /// adopts the committed snapshot.
    async fn persist_node_update_at_revision<F>(
        &self,
        expected_revision: u64,
        update: F,
    ) -> FabricResult<FabricCommitSnapshot>
    where
        F: FnOnce(&mut Config) -> FabricResult<()> + Send + 'static,
    {
        let deployer = self.clone();
        join_fabric_task(
            "cluster-fabric state transaction",
            tokio::spawn(async move {
                let _io = deployer.config_io_lock.lock().await;
                deployer
                    .persist_node_update_locked(expected_revision, update)
                    .await
            }),
        )
        .await
    }

    /// Locked half of [`Self::persist_node_update_at_revision`]. Lifecycle
    /// operations hold the same guard across their external worker action, so
    /// a validated revision cannot become stale after a worker is stopped or
    /// deployed but before its resulting state is committed.
    async fn persist_node_update_locked<F>(
        &self,
        expected_revision: u64,
        update: F,
    ) -> FabricResult<FabricCommitSnapshot>
    where
        F: FnOnce(&mut Config) -> FabricResult<()> + Send,
    {
        if let Some(facade) = &self.config_facade {
            let actual = self.cluster_revision_locked();
            if expected_revision != actual {
                return Err(FabricError::Conflict {
                    expected: expected_revision,
                    actual,
                });
            }

            let mut candidate = self.config.read().await.clone();
            update(&mut candidate)?;
            let data_dir = self.data_dir.clone();
            let commit_facade = facade.clone();
            let (mut candidate, commit) = tokio::task::spawn_blocking(move || {
                let commit =
                    bamboo_config::persist_cluster_fabric_credential_transaction_with_adoption(
                        &data_dir,
                        &mut candidate,
                        &BTreeMap::new(),
                        expected_revision,
                        commit_facade.as_ref(),
                        |_, _| {
                            #[cfg(test)]
                            run_after_commit_before_adoption_test_hook(
                                &data_dir,
                                expected_revision,
                            );
                        },
                    )?;
                Ok::<_, ConfigStoreError>((candidate, commit))
            })
            .await
            .map_err(|error| FabricError::Internal(format!("persist task failed: {error}")))?
            .map_err(map_config_store_error)?;

            // The transaction returns the exact hydrated candidate it
            // installed. Adopt that runtime before exposing the already
            // captured section event; do not reread a later disk winner.
            let bamboo_config::ClusterFabricTransactionCommit {
                revision,
                adoption,
                credential_adoption,
                runtime,
            } = commit;
            let runtime = match runtime {
                Ok(bamboo_config::ClusterFabricRuntimeSnapshot {
                    cluster_fabric,
                    credential_statuses,
                    credential_health,
                }) => {
                    candidate.cluster_fabric = cluster_fabric;
                    Ok((credential_statuses, credential_health))
                }
                Err(error) if revision == expected_revision => {
                    return Err(FabricError::Internal(format!(
                        "cluster configuration at revision {revision} could not materialize its exact runtime credentials: {error}"
                    )));
                }
                Err(error) => {
                    candidate.clear_cluster_runtime_credentials();
                    Err(error)
                }
            };
            *self.config.write().await = candidate.clone();
            let event = match adoption {
                Some(Ok(event)) => Some(event),
                Some(Err(error)) => {
                    return Err(FabricError::Committed(format!(
                        "cluster configuration committed at revision {} but process adoption failed: {error}",
                        revision
                    )));
                }
                None if revision == expected_revision => None,
                None => {
                    return Err(FabricError::Committed(format!(
                        "cluster configuration committed at revision {} without a process adoption result",
                        revision
                    )));
                }
            };
            let section = facade
                .registry()
                .envelope_value(SectionId::ClusterFabric)
                .map_err(|error| {
                    FabricError::Committed(format!(
                        "cluster configuration committed at revision {} but its exact envelope is unavailable: {error}",
                        revision
                    ))
                })?;
            if section.revision != revision {
                return Err(FabricError::Committed(format!(
                    "cluster configuration committed at revision {} but facade retained revision {}",
                    revision, section.revision
                )));
            }

            if let (Some(event), Some(publish)) = (event.as_ref(), self.publish_event.as_ref()) {
                publish(event);
            }
            if let Some(Err(error)) = credential_adoption {
                return Err(FabricError::Committed(format!(
                    "cluster configuration committed at revision {revision} but credential facade adoption failed: {error}"
                )));
            }
            let (credential_statuses, credential_health) = runtime.map_err(|error| {
                FabricError::Committed(format!(
                    "cluster configuration committed at revision {revision} but could not materialize its exact runtime credentials: {error}"
                ))
            })?;
            return Ok(FabricCommitSnapshot {
                config: candidate,
                section,
                credential_statuses,
                credential_health,
            });
        }

        // Compatibility-only test fixtures have no modular facade. Preserve
        // the same durable-before-runtime ordering without pretending to offer
        // a revision other than zero.
        if expected_revision != 0 {
            return Err(FabricError::Conflict {
                expected: expected_revision,
                actual: 0,
            });
        }
        let mut candidate = self.config.read().await.clone();
        update(&mut candidate)?;
        let durable_candidate = candidate.clone();
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || durable_candidate.save_to_dir(data_dir))
            .await
            .map_err(|error| FabricError::Internal(format!("persist task failed: {error}")))?
            .map_err(|error| FabricError::Internal(format!("save config failed: {error}")))?;
        *self.config.write().await = candidate;
        self.snapshot_locked(0).await
    }

    /// Deploy a worker onto a node and persist its running state.
    ///
    /// `echo=true` runs the dependency-free echo executor (no LLM) — a
    /// connectivity smoke test.
    pub async fn deploy(&self, node_id: &str, echo: bool) -> FabricResult<NodeState> {
        let expected_revision = self.current_cluster_revision().await?;
        Ok(self
            .deploy_at_revision(node_id, echo, expected_revision)
            .await?
            .value)
    }

    /// Deploy against an operator-captured cluster-fabric revision and return
    /// the exact adopted snapshot from the final state commit.
    pub async fn deploy_at_revision(
        &self,
        node_id: &str,
        echo: bool,
        expected_revision: u64,
    ) -> FabricResult<FabricActionResult<NodeState>> {
        let deployer = self.clone();
        let node_id = node_id.to_string();
        join_fabric_task(
            "cluster-fabric deploy",
            tokio::spawn(async move {
                deployer
                    .deploy_at_revision_inner(&node_id, echo, expected_revision)
                    .await
            }),
        )
        .await
    }

    async fn deploy_at_revision_inner(
        &self,
        node_id: &str,
        echo: bool,
        expected_revision: u64,
    ) -> FabricResult<FabricActionResult<NodeState>> {
        let _io = self.config_io_lock.lock().await;
        let action_snapshot = self.snapshot_locked(expected_revision).await?;
        let (mut node, broker) = {
            let cfg = &action_snapshot.config;
            (
                self.node_snapshot(cfg, node_id)?,
                cfg.subagents().broker.clone(),
            )
        };
        if !node.enabled {
            return Err(FabricError::BadRequest(format!(
                "Node '{node_id}' is disabled"
            )));
        }
        let broker = broker
            .filter(|b| !b.endpoint.trim().is_empty())
            .ok_or_else(|| {
                FabricError::BadRequest(
                    "No broker configured (subagents.broker) — a worker has nowhere to dial home"
                        .to_string(),
                )
            })?;

        // Zero-config default: if the operator didn't pin an artifact, ship our OWN
        // `bamboo` binary so a fresh remote node needs no manual install — but only
        // when the remote arch matches (a cross-arch binary can't run there). We
        // preflight `uname` for the arch; a mismatch is a clear error, not a wasted
        // 100MB+ upload that silently fails to exec. (Local placement runs the
        // binary directly, so it never needs an upload.)
        if node.deploy.artifact_path.is_none() && matches!(node.placement, NodePlacement::Ssh(_)) {
            let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
            let uname = build.deployer.preflight().await.map_err(|e| {
                FabricError::Internal(format!("preflight for '{node_id}' failed: {e}"))
            })?;
            if remote_matches_orchestrator(&uname) {
                node.deploy.artifact_path = Some(self.bamboo_bin.to_string_lossy().into_owned());
                tracing::info!(
                    node = node_id,
                    %uname,
                    "no artifact_path set — auto-uploading orchestrator binary (arch match)"
                );
            } else {
                return Err(FabricError::BadRequest(format!(
                    "node '{node_id}': remote is '{uname}' but the orchestrator binary is \
                     {}/{} — set deploy.artifact_path to a bamboo binary built for the remote arch",
                    std::env::consts::OS,
                    std::env::consts::ARCH,
                )));
            }
        }

        let worker_id = worker_id_for(&node);
        let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
        let log_path = log_path_for(&node);

        // Resolve the worker's full ProvisionSpec PARENT-side (model + creds + MCP
        // + bus) so a remote node needs no bamboo config of its own. Deployers that
        // deliver it (local stdin / russh file-upload) ship it; others fall back to
        // the legacy argv+env self-resolve (spec_json is then ignored, harmless).
        let spec_json = {
            build_resident_spec(
                &node,
                &broker.endpoint,
                &broker.token,
                &action_snapshot.config,
                echo,
                &worker_id,
            )
        };

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
            spec_json,
            // Fabric config doesn't yet expose a per-node CA-cert path (#48
            // wires the capability into `AgentDeployment`/the CLI; a fast-follow
            // can add `node.deploy.tls_ca_cert` if fabric nodes need self-signed
            // `wss://` brokers without an OS-trust-store install).
            tls_ca_cert: None,
        };

        // Publish a durable deployment intent before replacing any live worker.
        // If the external action or its final state commit fails, the last
        // durable state is therefore Deploying (or the subsequently committed
        // Failed state), never the old Running claim with an empty registry.
        let mut deploying = node.state.clone().unwrap_or_default();
        deploying.status = NodeStatus::Deploying;
        deploying.last_error = None;
        let deployment_revision = self
            .persist_node_update_locked(expected_revision, |config| {
                let node = config
                    .cluster_fabric
                    .node_mut(node_id)
                    .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
                node.state = Some(deploying);
                Ok(())
            })
            .await?
            .section
            .revision;

        // Release any prior worker FIRST so its reverse tunnel frees the broker
        // port before the new deploy requests the same forward. Remove under the
        // lock, shut down outside it: shutdown is graceful now (SIGTERM + drain
        // grace, #49) and must not hold the shared registry for its duration.
        let prev = self
            .registry
            .lock()
            .await
            .remove(&crate::registry_keys::node_key(node_id));
        if let Some(prev) = prev {
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
                self.persist_node_update_locked(deployment_revision, |config| {
                    let node = config
                        .cluster_fabric
                        .node_mut(node_id)
                        .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
                    node.state = Some(failed);
                    Ok(())
                })
                .await?;
                tracing::warn!(
                    audit = "cluster_fabric.deploy",
                    node = node_id,
                    placement = placement_env(&node),
                    outcome = "failed",
                    error = %e,
                );
                return Err(FabricError::Internal(format!(
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

        self.registry.lock().await.insert(
            crate::registry_keys::node_key(node_id),
            Deployed {
                env: placement_env(&node).to_string(),
                handle,
            },
        );

        // TOFU: pin the observed host-key fingerprint if not already set.
        let observed_fingerprint = if let Some(cell) = build.observed_fp {
            cell.lock().await.clone()
        } else {
            None
        };

        // Verify-on-deploy: exec'ing the worker used to report "running" even when
        // the worker never dialed home (phantom success — e.g. a missing/incompatible
        // binary silently failed to exec). Surface a broken deploy→tunnel→broker→worker
        // chain HERE, not as a hung ask on the first real task.
        //   • echo executor → round-trip a `ping` (proves the executor loop runs).
        //   • real executor → presence probe on the bus. A live LLM worker must NOT
        //     be handed a bogus task, so we only confirm it registered its mailbox —
        //     enough to prove the chain is live. The role matches what the resident
        //     registers under (the spec's `default_role`, else `general-purpose`).
        let verify = if echo {
            verify_echo_worker(&broker, &worker_id).await
        } else {
            let role = node
                .deploy
                .default_role
                .clone()
                .unwrap_or_else(|| "general-purpose".to_string());
            verify_worker_connected(&broker, &worker_id, &role, Duration::from_secs(30)).await
        };
        if let Err(e) = verify {
            // Tear the half-dead worker down and report the real failure.
            // (Remove under the lock, shut down outside it — see deploy above.)
            let dead = self
                .registry
                .lock()
                .await
                .remove(&crate::registry_keys::node_key(node_id));
            if let Some(d) = dead {
                d.handle.shutdown().await;
            }
            let msg = format!(
                "worker deployed but never came up on the bus (verify failed): {e} — \
                 check that `bamboo` runs on the remote (arch/deps) and see the node log"
            );
            let failed = NodeState {
                status: NodeStatus::Failed,
                worker_id: Some(worker_id.clone()),
                log_path: Some(log_path.clone()),
                last_error: Some(msg.clone()),
                ..Default::default()
            };
            self.persist_node_update_locked(deployment_revision, |config| {
                let node = config
                    .cluster_fabric
                    .node_mut(node_id)
                    .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
                node.state = Some(failed);
                Ok(())
            })
            .await?;
            tracing::warn!(
                audit = "cluster_fabric.deploy",
                node = node_id,
                worker_id = %worker_id,
                echo,
                outcome = "verify_failed",
                error = %e,
            );
            return Err(FabricError::Internal(format!(
                "deploy node '{node_id}': {msg}"
            )));
        }
        tracing::info!(
            node = node_id,
            worker_id = %worker_id,
            echo,
            "deploy verify ok — worker is reachable on the bus"
        );

        let state = NodeState {
            status: NodeStatus::Running,
            worker_id: Some(worker_id),
            remote_pid: pid,
            log_path: Some(log_path),
            deployed_at: Some(chrono::Utc::now().to_rfc3339()),
            ..Default::default()
        };
        let response_state = state.clone();
        let placement = node.placement.clone();
        #[cfg(test)]
        run_deploy_before_final_persist_test_hook(&self.data_dir);
        let snapshot = match self
            .persist_node_update_locked(deployment_revision, move |config| {
                let target = config
                    .cluster_fabric
                    .node_mut(node_id)
                    .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
                if target.placement != placement {
                    return Err(FabricError::Internal(
                        "runtime cluster placement diverged from the adopted action snapshot"
                            .to_string(),
                    ));
                }
                if let (Some(fingerprint), NodePlacement::Ssh(ssh)) =
                    (observed_fingerprint, &mut target.placement)
                {
                    if ssh.host_key_fingerprint.is_none() {
                        ssh.host_key_fingerprint = Some(fingerprint);
                    }
                }
                target.state = Some(state.clone());
                Ok(())
            })
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error @ FabricError::Committed(_)) => {
                // The Running state is already durable/runtime authoritative.
                // Keep the matching verified worker instead of compensating a
                // transaction that did not fail before its commit point.
                return Err(error);
            }
            Err(error) => {
                let deployed = self
                    .registry
                    .lock()
                    .await
                    .remove(&crate::registry_keys::node_key(node_id));
                if let Some(deployed) = deployed {
                    deployed.handle.shutdown().await;
                }
                return Err(error);
            }
        };
        Ok(FabricActionResult {
            value: response_state,
            snapshot,
        })
    }

    /// Stop a node's worker (if running) and persist the stopped state.
    pub async fn stop(&self, node_id: &str) -> FabricResult<NodeState> {
        let expected_revision = self.current_cluster_revision().await?;
        Ok(self
            .stop_at_revision(node_id, expected_revision)
            .await?
            .value)
    }

    /// Stop against an operator-captured cluster-fabric revision.
    pub async fn stop_at_revision(
        &self,
        node_id: &str,
        expected_revision: u64,
    ) -> FabricResult<FabricActionResult<NodeState>> {
        let deployer = self.clone();
        let node_id = node_id.to_string();
        join_fabric_task(
            "cluster-fabric stop",
            tokio::spawn(async move {
                deployer
                    .stop_at_revision_inner(&node_id, expected_revision)
                    .await
            }),
        )
        .await
    }

    async fn stop_at_revision_inner(
        &self,
        node_id: &str,
        expected_revision: u64,
    ) -> FabricResult<FabricActionResult<NodeState>> {
        let _io = self.config_io_lock.lock().await;
        let snapshot = self.snapshot_locked(expected_revision).await?;
        self.node_snapshot(&snapshot.config, node_id)?;
        let state = NodeState {
            status: NodeStatus::Stopped,
            ..Default::default()
        };
        let persisted = match self
            .persist_node_update_locked(expected_revision, |config| {
                let node = config
                    .cluster_fabric
                    .node_mut(node_id)
                    .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
                node.state = Some(state.clone());
                Ok(())
            })
            .await
        {
            outcome @ (Ok(_) | Err(FabricError::Committed(_))) => outcome,
            Err(error) => return Err(error),
        };
        // The durable CAS succeeds before the external worker is affected.
        // Remove under the registry lock and shut down outside that lock; the
        // configuration guard remains held so no later lifecycle mutation can
        // overtake the stop before the worker has actually exited.
        let removed = self
            .registry
            .lock()
            .await
            .remove(&crate::registry_keys::node_key(node_id));
        if let Some(d) = removed {
            d.handle.shutdown().await;
        }
        let persisted = match persisted {
            Ok(snapshot) => snapshot,
            Err(error @ FabricError::Committed(_)) => return Err(error),
            Err(_) => unreachable!("pre-commit stop errors return before worker shutdown"),
        };
        tracing::info!(
            audit = "cluster_fabric.stop",
            node = node_id,
            outcome = "stopped"
        );
        Ok(FabricActionResult {
            value: state,
            snapshot: persisted,
        })
    }

    /// Connectivity preflight: connect + auth + `uname`, WITHOUT deploying.
    pub async fn test(&self, node_id: &str) -> FabricResult<String> {
        let expected_revision = self.current_cluster_revision().await?;
        Ok(self
            .test_at_revision(node_id, expected_revision)
            .await?
            .value)
    }

    /// Preflight against an operator-captured cluster-fabric revision. The
    /// operation does not mutate config, so the returned snapshot is the exact
    /// validated base.
    pub async fn test_at_revision(
        &self,
        node_id: &str,
        expected_revision: u64,
    ) -> FabricResult<FabricActionResult<String>> {
        let _io = self.config_io_lock.lock().await;
        let snapshot = self.snapshot_locked(expected_revision).await?;
        let node = self.node_snapshot(&snapshot.config, node_id)?;
        let build = build_deployer(&node, &self.bamboo_bin).map_err(FabricError::BadRequest)?;
        let result = build.deployer.preflight().await;
        tracing::info!(
            audit = "cluster_fabric.test",
            node = node_id,
            placement = placement_env(&node),
            outcome = if result.is_ok() { "ok" } else { "failed" },
        );
        let value = result.map_err(|e| FabricError::Internal(format!("preflight failed: {e}")))?;
        Ok(FabricActionResult { value, snapshot })
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

    /// Single-probe timeout. Fast when the worker is present (returns on first
    /// sighting); this only bounds how long a genuinely-gone worker is chased.
    const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Health-check `node_id` with the production probe timeout.
    pub async fn health_check(&self, node_id: &str) -> FabricResult<NodeState> {
        self.health_check_within(node_id, Self::HEALTH_PROBE_TIMEOUT)
            .await
    }

    /// Probe a Running/Unreachable node's worker on the bus and reconcile its live
    /// state: a worker present on the bus → `Running` + fresh `last_health`; a
    /// vanished one → `Unreachable`. Nodes not meant to be up
    /// (NotDeployed/Deploying/Stopped/Failed) are left untouched. A status FLIP is
    /// persisted to disk (durable + audited); a steady-state heartbeat only
    /// refreshes `last_health` in memory, so a healthy cluster doesn't rewrite
    /// config.json every tick. Presence is checked (never a task ping), so a live
    /// LLM worker is never disturbed — same rationale as the deploy verify.
    async fn health_check_within(
        &self,
        node_id: &str,
        probe_timeout: Duration,
    ) -> FabricResult<NodeState> {
        // The potentially long broker probe belongs to the requesting owner:
        // cancelling or replacing that owner cancels the probe too. Only the
        // short final state transition below is detached once it owns the
        // config guard and has revalidated the observation.
        self.health_check_within_inner(node_id, probe_timeout).await
    }

    async fn health_check_within_inner(
        &self,
        node_id: &str,
        probe_timeout: Duration,
    ) -> FabricResult<NodeState> {
        let (node, broker, observed_revision) = {
            let _io = self.config_io_lock.lock().await;
            let cfg = self.config.read().await;
            (
                self.node_snapshot(&cfg, node_id)?,
                cfg.subagents().broker.clone(),
                self.cluster_revision_locked(),
            )
        };
        let current = node.state.clone().unwrap_or_default();
        if !matches!(
            current.status,
            NodeStatus::Running | NodeStatus::Unreachable
        ) {
            return Ok(current); // only nodes that should be up are monitored
        }
        let worker_id = current
            .worker_id
            .clone()
            .unwrap_or_else(|| worker_id_for(&node));
        let role = node
            .deploy
            .default_role
            .clone()
            .unwrap_or_else(|| "general-purpose".to_string());
        let Some(broker) = broker.filter(|b| !b.endpoint.trim().is_empty()) else {
            return Ok(current); // no broker configured → nothing to probe against
        };

        // Fast when present (returns on first sighting); costs the timeout only
        // when the worker is genuinely gone. The short window absorbs a blip — a
        // false Unreachable self-corrects on the next tick.
        let alive = verify_worker_connected(&broker, &worker_id, &role, probe_timeout)
            .await
            .is_ok();
        #[cfg(test)]
        run_health_after_probe_test_hook(&self.data_dir).await;

        let new_status = if alive {
            NodeStatus::Running
        } else {
            NodeStatus::Unreachable
        };
        let new_error = if alive {
            None
        } else {
            Some(format!(
                "worker '{worker_id}' not present on the bus under role '{role}'"
            ))
        };

        // Bind the unlocked probe to the section revision and worker identity
        // captured before it started. A redeploy or node edit that wins while
        // the probe is in flight makes this observation stale; discard it
        // rather than applying it against the winner's newer revision.
        let io = self.config_io_lock.clone().lock_owned().await;
        let actual_revision = self.cluster_revision_locked();
        let (live, identity_matches) = {
            let cfg = self.config.read().await;
            let Some(node) = cfg.cluster_fabric.node(node_id) else {
                return Ok(current);
            };
            let live = node.state.clone().unwrap_or_default();
            let live_worker_id = live
                .worker_id
                .clone()
                .unwrap_or_else(|| worker_id_for(node));
            let live_role = node
                .deploy
                .default_role
                .clone()
                .unwrap_or_else(|| "general-purpose".to_string());
            let live_broker = cfg
                .subagents()
                .broker
                .clone()
                .filter(|candidate| !candidate.endpoint.trim().is_empty());
            let identity_matches = actual_revision == observed_revision
                && live.status == current.status
                && live_worker_id == worker_id
                && live_role == role
                && live_broker.as_ref() == Some(&broker);
            (live, identity_matches)
        };
        if !identity_matches
            || !matches!(live.status, NodeStatus::Running | NodeStatus::Unreachable)
        {
            return Ok(live);
        }
        let from = live.status;
        let next = NodeState {
            status: new_status,
            last_health: Some(chrono::Utc::now().to_rfc3339()),
            last_error: new_error,
            ..live.clone()
        };
        if from != new_status {
            let committed_next = next.clone();
            let deployer = self.clone();
            let node_id = node_id.to_string();
            let worker_id = worker_id.clone();
            return join_fabric_task(
                "cluster-fabric health state transaction",
                tokio::spawn(async move {
                    // Move the already-acquired guard into the finalizer. From
                    // this point request cancellation cannot strand a durable
                    // state flip before runtime/facade/event publication.
                    let _io = io;
                    let snapshot = deployer
                        .persist_node_update_locked(observed_revision, |config| {
                            let node =
                                config.cluster_fabric.node_mut(&node_id).ok_or_else(|| {
                                    FabricError::NotFound(format!("Node '{node_id}'"))
                                })?;
                            let current = node.state.clone().unwrap_or_default();
                            node.state = Some(NodeState {
                                status: committed_next.status,
                                last_health: committed_next.last_health.clone(),
                                last_error: committed_next.last_error.clone(),
                                ..current
                            });
                            Ok(())
                        })
                        .await?;
                    let adopted = snapshot
                        .config
                        .cluster_fabric
                        .node(&node_id)
                        .and_then(|node| node.state.clone())
                        .unwrap_or(live);
                    tracing::info!(
                        audit = "cluster_fabric.health",
                        node = node_id,
                        worker_id = %worker_id,
                        from = ?from,
                        to = ?adopted.status,
                        "node health changed",
                    );
                    Ok(adopted)
                }),
            )
            .await;
        }

        let mut cfg = self.config.write().await;
        let Some(node) = cfg.cluster_fabric.node_mut(node_id) else {
            return Ok(current);
        };
        let latest = node.state.clone().unwrap_or_default();
        let latest_worker_id = latest
            .worker_id
            .clone()
            .unwrap_or_else(|| worker_id_for(node));
        let latest_role = node
            .deploy
            .default_role
            .clone()
            .unwrap_or_else(|| "general-purpose".to_string());
        if latest.status != live.status || latest_worker_id != worker_id || latest_role != role {
            return Ok(latest);
        }
        node.state = Some(next.clone());
        Ok(next)
    }

    /// Node ids whose persisted status is Running or Unreachable — the set the
    /// health monitor sweeps.
    async fn monitored_node_ids(&self) -> Vec<String> {
        let cfg = self.config.read().await;
        cfg.cluster_fabric
            .nodes
            .iter()
            .filter(|n| {
                n.state.as_ref().is_some_and(|s| {
                    matches!(s.status, NodeStatus::Running | NodeStatus::Unreachable)
                })
            })
            .map(|n| n.id.clone())
            .collect()
    }

    /// Decide whether to auto-recover `node_id` given its just-probed `state`,
    /// advancing the debounce/backoff bookkeeping. `Some(attempt)` ⇒ the caller
    /// should redeploy now; `None` ⇒ hold (not opted in, still debouncing, inside
    /// backoff, or exhausted). On exhausting [`RECOVERY_MAX_ATTEMPTS`] it marks the
    /// node `Failed` once and stops. A node that is no longer Unreachable clears its
    /// recovery progress (a recovered node starts fresh next outage). A user-Stopped
    /// node never reaches here — `health_check` only probes Running/Unreachable.
    async fn recovery_decision(&self, node_id: &str, state: &NodeState) -> Option<u32> {
        if state.status != NodeStatus::Unreachable {
            self.recovery.lock().await.remove(node_id);
            return None;
        }
        let auto = {
            let cfg = self.config.read().await;
            cfg.cluster_fabric
                .node(node_id)
                .map(|n| n.deploy.auto_recover)
                .unwrap_or(false)
        };
        if !auto {
            return None;
        }

        let mut map = self.recovery.lock().await;
        let rs = map.entry(node_id.to_string()).or_default();
        rs.consecutive_unreachable = rs.consecutive_unreachable.saturating_add(1);
        if rs.consecutive_unreachable < RECOVERY_DEBOUNCE {
            return None; // ride out a blip before touching a live node
        }
        if rs.in_flight {
            return None; // a redeploy is still running — don't overlap it
        }
        if rs.attempts >= RECOVERY_MAX_ATTEMPTS {
            let first_give_up = !rs.gave_up;
            rs.gave_up = true;
            drop(map);
            if first_give_up {
                self.mark_failed(
                    node_id,
                    &format!("auto-recover gave up after {RECOVERY_MAX_ATTEMPTS} attempts"),
                )
                .await;
            }
            return None;
        }
        if let Some(t) = rs.next_eligible {
            if tokio::time::Instant::now() < t {
                return None; // inside backoff
            }
        }
        rs.attempts += 1;
        rs.in_flight = true;
        let attempt = rs.attempts;
        rs.next_eligible = Some(tokio::time::Instant::now() + Self::recovery_backoff(attempt));
        Some(attempt)
    }

    /// Clear the in-flight guard after a recovery redeploy settles, so a later tick
    /// can retry (on failure); on success the next `health_check` resets the entry.
    async fn clear_recovery_in_flight(&self, node_id: &str) {
        if let Some(rs) = self.recovery.lock().await.get_mut(node_id) {
            rs.in_flight = false;
        }
    }

    /// Exponential backoff between recovery attempts: ~10s, 20s, 40s… capped 300s.
    fn recovery_backoff(attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(5);
        Duration::from_secs((10u64 << shift).min(300))
    }

    /// Persist a node as `Failed` with `reason`, preserving its other engine fields.
    async fn mark_failed(&self, node_id: &str, reason: &str) {
        let current = {
            let cfg = self.config.read().await;
            cfg.cluster_fabric
                .node(node_id)
                .and_then(|n| n.state.clone())
                .unwrap_or_default()
        };
        let failed = NodeState {
            status: NodeStatus::Failed,
            last_error: Some(reason.to_string()),
            ..current
        };
        if let Err(e) = self.persist_state(node_id, Some(failed)).await {
            tracing::warn!(node = node_id, error = %e, "failed to persist Failed state");
        }
        tracing::warn!(
            audit = "cluster_fabric.recover",
            node = node_id,
            reason,
            "auto-recover exhausted → Failed",
        );
    }

    /// Spawn the background health monitor: every `cluster_fabric.health_interval`
    /// it [`health_check`](Self::health_check)s each Running/Unreachable node.
    /// `None` (no task) when disabled (`health_interval_secs = 0`); abort the
    /// handle to stop it. The cadence is read once at spawn (change → restart).
    pub async fn spawn_health_monitor(self: Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let interval = {
            let cfg = self.config.read().await;
            cfg.cluster_fabric.health_interval()?
        };
        tracing::info!(
            interval_secs = interval.as_secs(),
            "cluster health monitor started"
        );
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                for id in self.monitored_node_ids().await {
                    match self.health_check(&id).await {
                        Ok(state) => {
                            if let Some(attempt) = self.recovery_decision(&id, &state).await {
                                // Redeploy OFF the sweep's critical path so a slow
                                // deploy doesn't stall other nodes' health checks;
                                // the backoff gate was already advanced, so the next
                                // tick won't re-trigger until it elapses.
                                let this = self.clone();
                                let node = id.clone();
                                tokio::spawn(async move {
                                    tracing::warn!(
                                        audit = "cluster_fabric.recover",
                                        node = %node,
                                        attempt,
                                        "auto-recovering unreachable node",
                                    );
                                    match this.deploy(&node, false).await {
                                        Ok(_) => tracing::info!(
                                            node = %node,
                                            attempt,
                                            "auto-recover redeploy succeeded"
                                        ),
                                        Err(e) => tracing::warn!(
                                            node = %node,
                                            attempt,
                                            error = %e,
                                            "auto-recover redeploy failed"
                                        ),
                                    }
                                    this.clear_recovery_in_flight(&node).await;
                                });
                            }
                        }
                        Err(e) => tracing::warn!(node = %id, error = %e, "health check failed"),
                    }
                }
            }
        }))
    }

    /// Persist an engine-owned node state against a specific section revision.
    async fn persist_state_at_revision(
        &self,
        node_id: &str,
        state: Option<NodeState>,
        expected_revision: u64,
    ) -> FabricResult<FabricCommitSnapshot> {
        let node_id = node_id.to_string();
        self.persist_node_update_at_revision(expected_revision, move |config| {
            let node = config
                .cluster_fabric
                .node_mut(&node_id)
                .ok_or_else(|| FabricError::NotFound(format!("Node '{node_id}'")))?;
            node.state = state;
            Ok(())
        })
        .await
    }

    /// Internal monitor/recovery writes adopt the current cluster revision at
    /// the moment the mutation begins and still use the same durable CAS path.
    async fn persist_state(
        &self,
        node_id: &str,
        state: Option<NodeState>,
    ) -> FabricResult<FabricCommitSnapshot> {
        let expected_revision = self.current_cluster_revision().await?;
        self.persist_state_at_revision(node_id, state, expected_revision)
            .await
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

/// Resolve a deployed worker's FULL `ProvisionSpec` parent-side (model + creds +
/// MCP-proxy + bus + identity) — the orchestrator counterpart to the self-resolve
/// `broker-agent` does from local config. Returned as JSON to ship to the worker
/// (stdin for local, file-upload for russh). `None` when there are no credentials
/// to ship and it is not an echo deploy — the worker then self-resolves (legacy
/// fallback), so we never deploy a real worker with no model/creds.
fn build_resident_spec(
    node: &Node,
    broker_endpoint: &str,
    broker_token: &str,
    config: &Config,
    echo: bool,
    worker_id: &str,
) -> Option<String> {
    build_ondemand_provision_spec(
        worker_id,
        node.deploy.default_role.as_deref(),
        node.deploy.model.as_deref(),
        node.deploy.workspace.as_deref(),
        std::env::temp_dir()
            .join("bamboo-fabric-agents")
            .join(worker_id),
        broker_endpoint,
        broker_token,
        config,
        echo,
    )
}

/// Build a parent-resolved `ProvisionSpec` (model + creds + MCP-proxy + bus +
/// identity), serialized as JSON, for an on-demand worker deploy. Shared by
/// [`build_resident_spec`] (cluster-fabric nodes) and `deploy_agent`'s
/// `env=docker` path (#46: Docker used to bind-mount the orchestrator's ENTIRE
/// `~/.bamboo` — including `config.json` and the master
/// `.bamboo_encryption_key` — into the worker container; this spec ships only
/// the credentials the assigned model actually needs, over a one-shot stdin
/// pipe, with no encryption key and no home mount at all). `None` when there
/// are no credentials to ship and this is not an echo deploy — the caller then
/// falls back to legacy self-resolve rather than deploying a real worker with
/// no model/creds.
///
/// Credential scoping mirrors `ActorChildRunner::build_spec`
/// (`external_agents/actor_adapter.rs`): `extract_provider_credentials`
/// returns every configured provider's key, so it is filtered down to the
/// single credential matching `spec.model.provider` *after* the model is
/// resolved — never the raw unfiltered list. This applies to both callers
/// (cluster-fabric node deploys and the AI-triggered docker path), for the
/// same least-privilege reason `ActorChildRunner` already scopes its own
/// workers: a review bot flagged a prior version of this helper for shipping
/// every configured provider's key regardless of which model was pinned.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_ondemand_provision_spec(
    worker_id: &str,
    role: Option<&str>,
    pinned_model: Option<&str>,
    workspace: Option<&str>,
    storage_dir: PathBuf,
    broker_endpoint: &str,
    broker_token: &str,
    config: &Config,
    echo: bool,
) -> Option<String> {
    use bamboo_subagent::provision::{
        BusEndpoint, ChildIdentity, ExecutorSpec, McpProxyConfig, ModelRefSpec, ProvisionSpec,
    };

    let all_credentials =
        bamboo_engine::external_agents::runtime::extract_provider_credentials(config);
    if all_credentials.is_empty() && !echo {
        return None;
    }

    let role = role
        .map(str::to_string)
        .unwrap_or_else(|| "general-purpose".to_string());
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: worker_id.to_string(),
            parent_id: None,
            project_key: None,
            role,
            depth: 0,
        },
        if echo {
            ExecutorSpec::Echo
        } else {
            ExecutorSpec::BambooRuntime
        },
        storage_dir.to_string_lossy().into_owned(),
    );
    spec.bus = Some(BusEndpoint {
        endpoint: broker_endpoint.to_string(),
        token: broker_token.to_string(),
    });
    // Model: the caller's pinned `provider:model`, else the configured
    // sub-agent / chat default (resolved HERE, on the orchestrator, never on
    // the worker).
    spec.model = pinned_model.and_then(parse_provider_model).or_else(|| {
        config.defaults.as_ref().and_then(|d| {
            // sub_agent default, else chat. Guard emptiness so we never ship an
            // invalid `{provider:"", model:""}` spec — a modelless non-echo worker
            // then fails the presence verify at deploy instead of at first task.
            let r = d.sub_agent.as_ref().unwrap_or(&d.chat);
            (!r.provider.trim().is_empty() && !r.model.trim().is_empty()).then(|| ModelRefSpec {
                provider: r.provider.clone(),
                model: r.model.clone(),
            })
        })
    });
    spec.workspace = workspace.map(str::to_string);
    // Least-privilege secrets: only the credential for the resolved model's
    // provider ships — never the full `all_credentials` set (#46 follow-up).
    match spec
        .model
        .as_ref()
        .map(|m| m.provider.as_str())
        .filter(|p| !p.trim().is_empty())
    {
        Some(provider) => match all_credentials.into_iter().find(|c| c.provider == provider) {
            Some(cred) => spec.secrets.provider_credentials = vec![cred],
            None => {
                tracing::warn!(
                    "ondemand spec for worker {}: no credential found for provider '{}'; \
                         shipping none",
                    worker_id,
                    provider
                );
            }
        },
        None => {
            if !echo {
                tracing::warn!(
                    "ondemand spec for worker {}: no model provider resolved to scope \
                     credentials to; shipping none",
                    worker_id
                );
            }
        }
    }
    // Deployed workers proxy ALL MCP to the orchestrator (single MCP host).
    spec.capabilities.mcp_proxy = Some(McpProxyConfig {
        orchestrator: ORCHESTRATOR_ID.to_string(),
        endpoint: broker_endpoint.to_string(),
        token: broker_token.to_string(),
    });
    // `to_json` enforces the mcp XOR mcp_proxy guard before it goes on the wire.
    spec.to_json().ok()
}

/// Parse a `provider:model` reference; `None` for empty or provider-less input
/// (the config-default fallback handles those).
fn parse_provider_model(s: &str) -> Option<bamboo_subagent::provision::ModelRefSpec> {
    let s = s.trim();
    s.split_once(':').and_then(|(p, m)| {
        (!p.is_empty() && !m.is_empty()).then(|| bamboo_subagent::provision::ModelRefSpec {
            provider: p.to_string(),
            model: m.to_string(),
        })
    })
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

/// True if a remote `uname -s -m` string (e.g. `"Darwin arm64"`) matches the
/// orchestrator's own OS + arch, so this process's `bamboo` binary can run there.
fn remote_matches_orchestrator(uname: &str) -> bool {
    let os = match std::env::consts::OS {
        "macos" => "Darwin",
        "linux" => "Linux",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x86_64",
        other => other,
    };
    uname.contains(os) && uname.contains(arch)
}

/// Round-trip a ping to a freshly-deployed **echo** worker over the bus, proving
/// the deploy → reverse-tunnel → broker → worker chain is actually live. Returns
/// `Ok` once the worker echoes back, or a timeout/transport error otherwise.
async fn verify_echo_worker(broker: &BrokerClientConfig, worker_id: &str) -> Result<(), String> {
    let me = AgentRef {
        session_id: format!("{ORCHESTRATOR_ID}-deploy-verify"),
        role: Some("orchestrator".to_string()),
    };
    ask_agent(
        &broker.endpoint,
        me,
        &broker.token,
        worker_id,
        "ping",
        AskMode::Query,
        Duration::from_secs(30),
    )
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// Presence probe for a freshly-deployed **non-echo** worker. Unlike the echo
/// verify it sends NO task (a live LLM worker must not be handed a bogus ping):
/// it polls the bus's live-actor registry until the worker's mailbox appears
/// under `role`, proving the deploy → tunnel → broker → worker chain came up.
/// A worker that never dialed home (wrong arch, missing deps, failed exec) fails
/// the deploy HERE instead of silently reporting "Running" and hanging the first
/// task. `role` must match what the resident registers under (the spec's
/// `default_role`, else the `general-purpose` default).
async fn verify_worker_connected(
    broker: &BrokerClientConfig,
    worker_id: &str,
    role: &str,
    timeout: Duration,
) -> Result<(), String> {
    let me = AgentRef {
        session_id: format!("{ORCHESTRATOR_ID}-deploy-presence"),
        role: Some("orchestrator".to_string()),
    };
    let mut client = BrokerClient::connect(&broker.endpoint, me, &broker.token)
        .await
        .map_err(|e| e.to_string())?;
    let deadline = Instant::now() + timeout;
    loop {
        match client.list_connected(role).await {
            Ok(ids) if ids.iter().any(|id| id == worker_id) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "worker '{worker_id}' never registered on the bus under role \
                 '{role}' within {}s",
                timeout.as_secs()
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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

    Ok(RusshDeployer::new(
        target.host.clone(),
        target.port,
        target.username.clone(),
        auth,
    )
    .with_fingerprint(target.host_key_fingerprint.clone())
    .with_upload(upload))
}

#[cfg(test)]
mod lifecycle_persistence_tests {
    use super::*;
    use bamboo_config::cluster_fabric::{DeployProfile, TrustLevel};
    use std::sync::Mutex as StdMutex;

    struct ModularFixture {
        _data_dir: tempfile::TempDir,
        facade: Arc<ConfigFacade>,
        config: Arc<RwLock<Config>>,
        config_io_lock: Arc<Mutex<()>>,
        registry: DeployedRegistry,
        deployer: Arc<FabricDeployer>,
        events: Arc<StdMutex<Vec<ConfigSectionEvent>>>,
    }

    fn modular_running_fixture(bamboo_bin: &str) -> ModularFixture {
        let data_dir = tempfile::tempdir().unwrap();
        let facade = Arc::new(ConfigFacade::open_or_migrate(data_dir.path()).unwrap());
        let mut candidate = facade.effective_config();
        candidate.cluster_fabric.nodes.push(Node {
            id: "n1".to_string(),
            label: "node".to_string(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: Some(NodeState {
                status: NodeStatus::Running,
                worker_id: Some("cluster-node-n1".to_string()),
                ..Default::default()
            }),
            enabled: true,
        });
        let revision = bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
            data_dir.path(),
            &mut candidate,
            &BTreeMap::new(),
            0,
        )
        .unwrap();
        assert_eq!(revision, 1);
        assert!(facade
            .registry()
            .reload_if_changed(SectionId::ClusterFabric)
            .is_some());

        let mut runtime = facade.effective_config();
        runtime.subagents_mut().broker = Some(BrokerClientConfig {
            endpoint: "ws://127.0.0.1:9".to_string(),
            token: "test-token".to_string(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        });
        let config = Arc::new(RwLock::new(runtime));
        let config_io_lock = Arc::new(Mutex::new(()));
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let events = Arc::new(StdMutex::new(Vec::new()));
        let event_log = events.clone();
        let deployer = Arc::new(
            FabricDeployer::new(
                config.clone(),
                config_io_lock.clone(),
                data_dir.path().to_path_buf(),
                registry.clone(),
                bamboo_bin,
            )
            .with_modular_persistence(
                facade.clone(),
                Arc::new(CredentialStore::open(data_dir.path())),
                Arc::new(move |event| event_log.lock().unwrap().push(event.clone())),
            ),
        );
        ModularFixture {
            _data_dir: data_dir,
            facade,
            config,
            config_io_lock,
            registry,
            deployer,
            events,
        }
    }

    #[tokio::test]
    async fn changed_lifecycle_commit_publishes_secret_free_runtime_before_committed_error() {
        let data_dir = tempfile::tempdir().unwrap();
        let initial_facade = ConfigFacade::open_or_migrate(data_dir.path()).unwrap();
        let password_ref = bamboo_config::cluster_password_credential_ref("secret-node").unwrap();
        let mut candidate = initial_facade.effective_config();
        candidate.cluster_fabric.nodes.push(Node {
            id: "secret-node".to_string(),
            label: "before-corruption".to_string(),
            placement: NodePlacement::Ssh(SshTarget {
                host: "corrupt.example.test".to_string(),
                port: 22,
                username: "operator".to_string(),
                auth: SshAuth::Password {
                    password: String::new(),
                    password_encrypted: None,
                },
                host_key_fingerprint: None,
            }),
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let revision = bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
            data_dir.path(),
            &mut candidate,
            &BTreeMap::from([(
                "secret-node".to_string(),
                bamboo_config::ClusterNodeCredentialIntents {
                    password: bamboo_config::ClusterCredentialAction::Replace(
                        "initial-password".to_string(),
                    ),
                    private_key: bamboo_config::ClusterCredentialAction::Clear,
                    passphrase: bamboo_config::ClusterCredentialAction::Clear,
                },
            )]),
            0,
        )
        .unwrap();
        assert_eq!(revision, 1);
        candidate
            .hydrate_cluster_credentials_from_store(data_dir.path())
            .unwrap();

        let facade = Arc::new(ConfigFacade::open(data_dir.path()).unwrap());
        let config = Arc::new(RwLock::new(candidate));
        let events = Arc::new(StdMutex::new(Vec::new()));
        let event_log = events.clone();
        let deployer = FabricDeployer::new(
            config.clone(),
            Arc::new(Mutex::new(())),
            data_dir.path().to_path_buf(),
            Arc::new(Mutex::new(HashMap::new())),
            "/usr/bin/true",
        )
        .with_modular_persistence(
            facade.clone(),
            Arc::new(CredentialStore::open(data_dir.path())),
            Arc::new(move |event| event_log.lock().unwrap().push(event.clone())),
        );

        let credentials_path = data_dir.path().join("credentials.json");
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&credentials_path).unwrap()).unwrap();
        document["data"]["entries"][password_ref.as_str()]["ciphertext"] =
            serde_json::Value::String("corrupt-ciphertext".to_string());
        std::fs::write(
            credentials_path,
            serde_json::to_vec_pretty(&document).unwrap(),
        )
        .unwrap();

        let noop = deployer
            .persist_node_update_at_revision(1, |_| Ok(()))
            .await;
        match noop {
            Err(FabricError::Internal(_)) => {}
            Err(error) => panic!("no-op materialization error was misclassified: {error}"),
            Ok(_) => panic!("corrupt credential unexpectedly materialized"),
        }
        let runtime = config.read().await;
        let node = runtime.cluster_fabric.node("secret-node").unwrap();
        let NodePlacement::Ssh(target) = &node.placement else {
            panic!("expected SSH placement")
        };
        let SshAuth::Password { password, .. } = &target.auth else {
            panic!("expected password authentication")
        };
        assert_eq!(
            password, "initial-password",
            "a true no-op materialization failure must preserve the old runtime"
        );
        drop(runtime);
        assert_eq!(facade.registry().cluster_fabric.snapshot().revision, 1);
        assert!(events.lock().unwrap().is_empty());

        let result = deployer
            .persist_node_update_at_revision(1, |config| {
                config.cluster_fabric.node_mut("secret-node").unwrap().label =
                    "committed-metadata".to_string();
                Ok(())
            })
            .await;
        match result {
            Err(FabricError::Committed(_)) => {}
            Err(error) => panic!("post-commit materialization error was misclassified: {error}"),
            Ok(_) => panic!("corrupt credential unexpectedly materialized"),
        }

        let runtime = config.read().await;
        let node = runtime.cluster_fabric.node("secret-node").unwrap();
        assert_eq!(node.label, "committed-metadata");
        let NodePlacement::Ssh(target) = &node.placement else {
            panic!("expected SSH placement")
        };
        let SshAuth::Password {
            password,
            password_encrypted,
        } = &target.auth
        else {
            panic!("expected password authentication")
        };
        assert!(password.is_empty());
        assert!(password_encrypted.is_none());
        drop(runtime);

        let section = facade.registry().cluster_fabric.snapshot();
        assert_eq!(section.revision, 2);
        assert_eq!(
            section.data.0.node("secret-node").unwrap().label,
            "committed-metadata"
        );
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );
    }

    async fn start_broker() -> (String, tempfile::TempDir) {
        let data_dir = tempfile::tempdir().unwrap();
        let core = Arc::new(bamboo_broker::BrokerCore::new(data_dir.path()));
        let server = Arc::new(bamboo_broker::BrokerServer::new(core, "test-token"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{address}"), data_dir)
    }

    async fn join_expected_worker(
        fixture: &ModularFixture,
        endpoint: &str,
    ) -> bamboo_broker::BrokerClient {
        fixture.config.write().await.subagents_mut().broker = Some(BrokerClientConfig {
            endpoint: endpoint.to_string(),
            token: "test-token".to_string(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        });
        let node = fixture
            .config
            .read()
            .await
            .cluster_fabric
            .node("n1")
            .cloned()
            .unwrap();
        let worker_id = worker_id_for(&node);
        let role = node
            .deploy
            .default_role
            .clone()
            .unwrap_or_else(|| "general-purpose".to_string());
        let mut worker = bamboo_broker::BrokerClient::connect(
            endpoint,
            AgentRef {
                session_id: worker_id,
                role: Some(role),
            },
            "test-token",
        )
        .await
        .unwrap();
        worker.subscribe().await.unwrap();
        worker
    }

    async fn insert_prior_worker(fixture: &ModularFixture) {
        let child = tokio::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        fixture.registry.lock().await.insert(
            crate::registry_keys::node_key("n1"),
            Deployed {
                env: "local".to_string(),
                handle: bamboo_broker::DeployedAgent::from_parts("old-worker", child, None),
            },
        );
    }

    #[tokio::test]
    async fn failed_durable_state_write_does_not_advance_runtime() {
        let root = tempfile::tempdir().unwrap();
        let invalid_data_dir = root.path().join("not-a-directory");
        std::fs::write(&invalid_data_dir, b"file").unwrap();

        let mut config = Config::default();
        config.cluster_fabric.nodes.push(Node {
            id: "n1".to_string(),
            label: "node".to_string(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let config = Arc::new(RwLock::new(config));
        let deployer = FabricDeployer::new(
            config.clone(),
            Arc::new(Mutex::new(())),
            invalid_data_dir,
            Arc::new(Mutex::new(HashMap::new())),
            "/usr/bin/true",
        );

        let result = deployer
            .persist_state_at_revision(
                "n1",
                Some(NodeState {
                    status: NodeStatus::Stopped,
                    ..Default::default()
                }),
                0,
            )
            .await;
        assert!(result.is_err(), "invalid data directory must fail");
        assert!(
            config
                .read()
                .await
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .is_none(),
            "runtime must retain the pre-commit state when persistence fails"
        );
    }

    #[tokio::test]
    async fn cancelled_state_caller_still_finishes_disk_runtime_facade_and_event_publication() {
        let fixture = modular_running_fixture("/usr/bin/true");
        let guard = fixture.config_io_lock.lock().await;
        let caller = {
            let deployer = fixture.deployer.clone();
            tokio::spawn(async move {
                deployer
                    .persist_state_at_revision(
                        "n1",
                        Some(NodeState {
                            status: NodeStatus::Stopped,
                            ..Default::default()
                        }),
                        1,
                    )
                    .await
            })
        };

        // Let the public future spawn its transaction owner, then cancel only
        // the caller while that owner is blocked behind the shared config lock.
        tokio::time::sleep(Duration::from_millis(50)).await;
        caller.abort();
        let join_error = match caller.await {
            Err(error) => error,
            Ok(_) => panic!("the caller must be cancelled while its transaction stays alive"),
        };
        assert!(join_error.is_cancelled());
        drop(guard);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let revision = fixture.facade.registry().cluster_fabric.snapshot().revision;
                let status = fixture
                    .config
                    .read()
                    .await
                    .cluster_fabric
                    .node("n1")
                    .and_then(|node| node.state.as_ref())
                    .map(|state| state.status);
                if revision == 2
                    && status == Some(NodeStatus::Stopped)
                    && fixture.events.lock().unwrap().len() == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached lifecycle state transaction must finish after caller cancellation");

        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 2);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Stopped
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );
    }

    #[tokio::test]
    async fn cancelled_stop_caller_still_commits_and_shuts_down_the_registered_worker() {
        let fixture = modular_running_fixture("/usr/bin/true");
        insert_prior_worker(&fixture).await;
        let guard = fixture.config_io_lock.lock().await;
        let caller = {
            let deployer = fixture.deployer.clone();
            tokio::spawn(async move { deployer.stop_at_revision("n1", 1).await })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        caller.abort();
        let join_error = match caller.await {
            Err(error) => error,
            Ok(_) => panic!("the request-facing stop caller must be cancelled"),
        };
        assert!(join_error.is_cancelled());
        drop(guard);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let revision = fixture.facade.registry().cluster_fabric.snapshot().revision;
                let status = fixture
                    .config
                    .read()
                    .await
                    .cluster_fabric
                    .node("n1")
                    .and_then(|node| node.state.as_ref())
                    .map(|state| state.status);
                if revision == 2
                    && status == Some(NodeStatus::Stopped)
                    && fixture.registry.lock().await.is_empty()
                    && fixture.events.lock().unwrap().len() == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached stop must finish commit and worker shutdown");

        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 2);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Stopped
        );
    }

    #[tokio::test]
    async fn failed_redeploy_never_leaves_the_old_running_claim_after_worker_replacement() {
        let fixture = modular_running_fixture("/definitely/missing/bamboo");
        insert_prior_worker(&fixture).await;

        let error = match fixture.deployer.deploy_at_revision("n1", true, 1).await {
            Err(error) => error,
            Ok(_) => panic!("the replacement binary does not exist"),
        };
        assert!(matches!(error, FabricError::Internal(_)));
        assert!(fixture.registry.lock().await.is_empty());
        let process_snapshot = fixture.facade.registry().cluster_fabric.snapshot();
        assert_eq!(
            process_snapshot.revision, 3,
            "Deploying intent and Failed result are distinct durable commits"
        );
        assert_eq!(
            process_snapshot
                .data
                .0
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Failed
        );
        assert_eq!(
            fixture
                .config
                .read()
                .await
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Failed
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[
                ConfigSectionEvent::Changed {
                    section: "cluster-fabric".to_string(),
                    revision: 2,
                },
                ConfigSectionEvent::Changed {
                    section: "cluster-fabric".to_string(),
                    revision: 3,
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_deploy_caller_cannot_strand_a_verified_worker_before_final_publication() {
        let (endpoint, _broker_dir) = start_broker().await;
        let fixture = modular_running_fixture("/usr/bin/true");
        let _worker = join_expected_worker(&fixture, &endpoint).await;
        insert_prior_worker(&fixture).await;

        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_deploy_before_final_persist_test_hook(fixture._data_dir.path(), move |_| {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let caller = {
            let deployer = fixture.deployer.clone();
            tokio::spawn(async move { deployer.deploy_at_revision("n1", false, 1).await })
        };
        tokio::task::spawn_blocking(move || {
            reached_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("deploy must reach the final persistence boundary")
        })
        .await
        .unwrap();

        caller.abort();
        let join_error = match caller.await {
            Err(error) => error,
            Ok(_) => panic!("the request-facing deploy caller must be cancelled"),
        };
        assert!(join_error.is_cancelled());
        assert!(
            fixture
                .registry
                .lock()
                .await
                .contains_key(&crate::registry_keys::node_key("n1")),
            "the detached owner still controls the verified replacement"
        );
        release_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let revision = fixture.facade.registry().cluster_fabric.snapshot().revision;
                let status = fixture
                    .config
                    .read()
                    .await
                    .cluster_fabric
                    .node("n1")
                    .and_then(|node| node.state.as_ref())
                    .map(|state| state.status);
                if revision == 3
                    && status == Some(NodeStatus::Running)
                    && fixture.events.lock().unwrap().len() == 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached deploy must finish final durable/runtime/event publication");
        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 3);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Running
        );

        let replacement = fixture
            .registry
            .lock()
            .await
            .remove(&crate::registry_keys::node_key("n1"));
        if let Some(replacement) = replacement {
            replacement.handle.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_commit_after_running_durable_commit_waits_for_exact_adoption() {
        let (endpoint, _broker_dir) = start_broker().await;
        let fixture = modular_running_fixture("/usr/bin/true");
        let _worker = join_expected_worker(&fixture, &endpoint).await;
        insert_prior_worker(&fixture).await;

        let (external_done_tx, external_done_rx) = std::sync::mpsc::sync_channel(1);
        set_after_commit_before_adoption_test_hook(fixture._data_dir.path(), 2, move |data_dir| {
            let data_dir = data_dir.to_path_buf();
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let external = ConfigFacade::open(&data_dir).unwrap();
                let mut winner = external.effective_config();
                winner.cluster_fabric.node_mut("n1").unwrap().label =
                    "external-after-running".to_string();
                let result =
                    bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                        &data_dir,
                        &mut winner,
                        &BTreeMap::new(),
                        3,
                    );
                external_done_tx.send(result).unwrap();
            });
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("external writer must be launched at the post-commit boundary");
        });

        let result = fixture
            .deployer
            .deploy_at_revision("n1", false, 1)
            .await
            .unwrap();
        assert_eq!(result.snapshot.section.revision, 3);
        assert_eq!(
            result
                .snapshot
                .config
                .cluster_fabric
                .node("n1")
                .unwrap()
                .label,
            "node"
        );
        assert_eq!(
            fixture.facade.registry().cluster_fabric.snapshot().revision,
            3,
            "the process facade must adopt the exact Running commit first"
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[
                ConfigSectionEvent::Changed {
                    section: "cluster-fabric".to_string(),
                    revision: 2,
                },
                ConfigSectionEvent::Changed {
                    section: "cluster-fabric".to_string(),
                    revision: 3,
                },
            ]
        );
        assert!(
            fixture
                .registry
                .lock()
                .await
                .contains_key(&crate::registry_keys::node_key("n1")),
            "a committed Running worker must not be compensated"
        );

        assert_eq!(
            tokio::task::spawn_blocking(move || {
                external_done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("external writer must finish after adoption")
                    .unwrap()
            })
            .await
            .unwrap(),
            4
        );
        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 4);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("n1")
                .unwrap()
                .label,
            "external-after-running"
        );

        // Model the production watcher after it obtains the same local config
        // guard: the later winner is applied and emitted strictly after r3.
        {
            let _io = fixture.config_io_lock.lock().await;
            let event = fixture
                .facade
                .registry()
                .reload_if_changed(SectionId::ClusterFabric)
                .expect("later external revision must be observable");
            let mut runtime = fixture.config.read().await.clone();
            runtime.cluster_fabric = fixture.facade.effective_config().cluster_fabric.clone();
            *fixture.config.write().await = runtime;
            fixture.events.lock().unwrap().push(event);
        }
        assert_eq!(
            fixture.facade.registry().cluster_fabric.snapshot().revision,
            4
        );
        assert_eq!(
            fixture
                .config
                .read()
                .await
                .cluster_fabric
                .node("n1")
                .unwrap()
                .label,
            "external-after-running"
        );
        assert_eq!(
            fixture.events.lock().unwrap().last(),
            Some(&ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 4,
            })
        );

        let replacement = fixture
            .registry
            .lock()
            .await
            .remove(&crate::registry_keys::node_key("n1"));
        if let Some(replacement) = replacement {
            replacement.handle.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_commit_after_stopped_durable_commit_cannot_skip_worker_shutdown() {
        let fixture = modular_running_fixture("/usr/bin/true");
        insert_prior_worker(&fixture).await;

        let (external_done_tx, external_done_rx) = std::sync::mpsc::sync_channel(1);
        set_after_commit_before_adoption_test_hook(fixture._data_dir.path(), 1, move |data_dir| {
            let data_dir = data_dir.to_path_buf();
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let external = ConfigFacade::open(&data_dir).unwrap();
                let mut winner = external.effective_config();
                winner.cluster_fabric.node_mut("n1").unwrap().label =
                    "external-after-stop".to_string();
                let result =
                    bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                        &data_dir,
                        &mut winner,
                        &BTreeMap::new(),
                        2,
                    );
                external_done_tx.send(result).unwrap();
            });
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("external writer must be launched at the post-commit boundary");
        });

        let result = fixture.deployer.stop_at_revision("n1", 1).await.unwrap();
        assert_eq!(result.snapshot.section.revision, 2);
        assert_eq!(result.value.status, NodeStatus::Stopped);
        assert!(
            fixture.registry.lock().await.is_empty(),
            "the committed stop must always shut down its registered worker"
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );

        assert_eq!(
            tokio::task::spawn_blocking(move || {
                external_done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("external writer must finish after adoption")
                    .unwrap()
            })
            .await
            .unwrap(),
            3
        );
        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        let durable = reopened.effective_config();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 3);
        assert_eq!(
            durable.cluster_fabric.node("n1").unwrap().label,
            "external-after-stop"
        );
        assert_eq!(
            durable
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Stopped
        );
    }

    #[tokio::test]
    async fn final_redeploy_cas_loss_leaves_durable_deploying_intent_not_stale_running() {
        let (endpoint, _broker_dir) = start_broker().await;
        let fixture = modular_running_fixture("/usr/bin/true");
        let _worker = join_expected_worker(&fixture, &endpoint).await;
        insert_prior_worker(&fixture).await;

        set_deploy_before_final_persist_test_hook(fixture._data_dir.path(), move |data_dir| {
            let external = ConfigFacade::open(data_dir).unwrap();
            let mut winner = external.effective_config();
            let node = winner.cluster_fabric.node_mut("n1").unwrap();
            assert_eq!(
                node.state.as_ref().unwrap().status,
                NodeStatus::Deploying,
                "the destructive replacement must follow a durable intent"
            );
            node.label = "external-winner".to_string();
            let revision =
                bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                    data_dir,
                    &mut winner,
                    &BTreeMap::new(),
                    2,
                )
                .unwrap();
            assert_eq!(revision, 3);
        });

        let error = match fixture.deployer.deploy_at_revision("n1", false, 1).await {
            Err(error) => error,
            Ok(_) => panic!("the cross-process revision winner must reject the final deploy CAS"),
        };
        assert!(matches!(
            error,
            FabricError::Conflict {
                expected: 2,
                actual: 3
            }
        ));
        assert!(
            fixture.registry.lock().await.is_empty(),
            "the rejected replacement worker must be compensated"
        );
        assert_eq!(
            fixture
                .config
                .read()
                .await
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Deploying,
            "runtime must never retain the destroyed old Running claim"
        );
        assert_eq!(
            fixture
                .facade
                .registry()
                .cluster_fabric
                .snapshot()
                .data
                .0
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Deploying
        );
        let reopened = ConfigFacade::open(fixture._data_dir.path()).unwrap();
        let durable = reopened.effective_config();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 3);
        assert_eq!(
            durable
                .cluster_fabric
                .node("n1")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Deploying
        );
        assert_eq!(
            durable.cluster_fabric.node("n1").unwrap().label,
            "external-winner"
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );
    }
}

#[cfg(test)]
mod resident_spec_tests {
    use super::{build_ondemand_provision_spec, parse_provider_model};
    use bamboo_config::Config;

    #[test]
    fn parse_provider_model_splits_and_guards() {
        let r = parse_provider_model("anthropic:claude-opus-4-8").unwrap();
        assert_eq!(r.provider, "anthropic");
        assert_eq!(r.model, "claude-opus-4-8");
        // Bare model (no provider) and empty parts fall back to config defaults.
        assert!(parse_provider_model("just-a-model").is_none());
        assert!(parse_provider_model(":m").is_none());
        assert!(parse_provider_model("p:").is_none());
        assert!(parse_provider_model("  ").is_none());
    }

    /// A single `anthropic` provider instance carrying `key` — the
    /// actually-live credential path `extract_provider_credentials` reads
    /// (unlike the legacy `providers.anthropic` slot, whose `api_key` is
    /// `#[serde(skip_serializing)]` and so never round-trips through the
    /// generic serde projection that function uses for legacy slots).
    fn config_with_anthropic_key(key: &str) -> Config {
        let mut cfg = Config::default();
        let instance: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type": "anthropic",
                "api_key": key,
            }))
            .expect("minimal ProviderInstanceConfig JSON");
        cfg.provider_instances
            .insert("anthropic".to_string(), instance);
        cfg
    }

    /// Two provider instances configured (`anthropic` + `openai`) — the
    /// multi-provider setup the review on #494 flagged: with more than one
    /// provider configured, the ondemand spec must still carry only the ONE
    /// credential backing the pinned model, not every configured provider.
    fn config_with_two_providers(anthropic_key: &str, openai_key: &str) -> Config {
        let mut cfg = config_with_anthropic_key(anthropic_key);
        let openai: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": openai_key,
            }))
            .expect("minimal ProviderInstanceConfig JSON");
        cfg.provider_instances.insert("openai".to_string(), openai);
        cfg
    }

    /// #46 — a real (non-echo) on-demand deploy with no configured credentials
    /// must fall back to `None` (caller then declines to hand a worker nothing
    /// to authenticate with) rather than shipping an empty/invalid spec.
    #[test]
    fn ondemand_spec_is_none_without_credentials_and_not_echo() {
        let cfg = Config::default();
        let spec = build_ondemand_provision_spec(
            "w1",
            Some("researcher"),
            Some("anthropic:claude-opus-4-8"),
            None,
            std::env::temp_dir().join("bamboo-test-agents").join("w1"),
            "ws://broker:9600",
            "tok",
            &cfg,
            false,
        );
        assert!(spec.is_none());
    }

    /// echo deploys never need credentials — always produce a spec so the
    /// connectivity smoke test can proceed.
    #[test]
    fn ondemand_spec_is_some_for_echo_even_without_credentials() {
        let cfg = Config::default();
        let spec = build_ondemand_provision_spec(
            "w1",
            None,
            None,
            None,
            std::env::temp_dir().join("bamboo-test-agents").join("w1"),
            "ws://broker:9600",
            "tok",
            &cfg,
            true,
        );
        assert!(spec.is_some());
    }

    /// The core #46 regression guard: the serialized spec carries ONLY the
    /// configured provider credential (here `anthropic`) — never the
    /// `.bamboo_encryption_key` string, a raw `config.json` blob, or any
    /// unrelated provider ("openai" is unset here and must not appear).
    #[test]
    fn ondemand_spec_carries_only_configured_provider_credential() {
        let cfg = config_with_anthropic_key("sk-ant-super-secret");
        let spec_json = build_ondemand_provision_spec(
            "w1",
            Some("researcher"),
            Some("anthropic:claude-opus-4-8"),
            Some("/workspace"),
            std::env::temp_dir().join("bamboo-test-agents").join("w1"),
            "ws://broker:9600",
            "tok",
            &cfg,
            false,
        )
        .expect("credentials configured — spec must be built");

        assert!(spec_json.contains("sk-ant-super-secret"));
        assert!(spec_json.contains("anthropic"));
        // No encryption key, no on-disk config dump, no other provider.
        assert!(!spec_json.contains("bamboo_encryption_key"));
        assert!(!spec_json.contains("openai"));
        assert!(!spec_json.contains("gemini"));

        let parsed: bamboo_subagent::provision::ProvisionSpec =
            serde_json::from_str(&spec_json).expect("valid ProvisionSpec JSON");
        assert_eq!(parsed.secrets.provider_credentials.len(), 1);
        assert_eq!(parsed.secrets.provider_credentials[0].provider, "anthropic");
        assert_eq!(
            parsed.secrets.provider_credentials[0].api_key,
            "sk-ant-super-secret"
        );
        assert_eq!(parsed.workspace.as_deref(), Some("/workspace"));
    }

    /// #494 review finding: with TWO providers configured, a deploy pinned to
    /// `anthropic:...` must ship ONLY the anthropic credential — the prior
    /// version unconditionally assigned the entire `extract_provider_credentials`
    /// output (every configured provider) regardless of which model was
    /// pinned, so this would previously have leaked the openai key too.
    #[test]
    fn ondemand_spec_scopes_credentials_to_pinned_model_provider_only() {
        let cfg = config_with_two_providers("sk-ant-secret", "sk-oai-secret");
        let spec_json = build_ondemand_provision_spec(
            "w1",
            Some("researcher"),
            Some("anthropic:claude-opus-4-8"),
            None,
            std::env::temp_dir().join("bamboo-test-agents").join("w1"),
            "ws://broker:9600",
            "tok",
            &cfg,
            false,
        )
        .expect("credentials configured — spec must be built");

        let parsed: bamboo_subagent::provision::ProvisionSpec =
            serde_json::from_str(&spec_json).expect("valid ProvisionSpec JSON");
        assert_eq!(parsed.secrets.provider_credentials.len(), 1);
        assert_eq!(parsed.secrets.provider_credentials[0].provider, "anthropic");
        assert_eq!(
            parsed.secrets.provider_credentials[0].api_key,
            "sk-ant-secret"
        );
        // The unrelated, but CONFIGURED, openai key must not ship.
        assert!(!spec_json.contains("sk-oai-secret"));

        // Pin the other provider instead — only ITS credential should ship.
        let spec_json_openai = build_ondemand_provision_spec(
            "w2",
            Some("researcher"),
            Some("openai:gpt-5"),
            None,
            std::env::temp_dir().join("bamboo-test-agents").join("w2"),
            "ws://broker:9600",
            "tok",
            &cfg,
            false,
        )
        .expect("credentials configured — spec must be built");
        let parsed_openai: bamboo_subagent::provision::ProvisionSpec =
            serde_json::from_str(&spec_json_openai).expect("valid ProvisionSpec JSON");
        assert_eq!(parsed_openai.secrets.provider_credentials.len(), 1);
        assert_eq!(
            parsed_openai.secrets.provider_credentials[0].provider,
            "openai"
        );
        assert!(!spec_json_openai.contains("sk-ant-secret"));
    }

    /// A pinned model whose provider has no matching configured credential
    /// (e.g. typo'd provider id, or a provider that was since removed) must
    /// still build a spec — with an EMPTY credential list, not a fallback to
    /// every other configured provider's key. Mirrors the idiom already used
    /// by `ActorChildRunner::build_spec` (warn + ship nothing) rather than
    /// failing the whole deploy.
    #[test]
    fn ondemand_spec_has_no_credentials_when_pinned_provider_has_no_match() {
        let cfg = config_with_two_providers("sk-ant-secret", "sk-oai-secret");
        let spec_json = build_ondemand_provision_spec(
            "w1",
            Some("researcher"),
            Some("gemini:gemini-3-pro"),
            None,
            std::env::temp_dir().join("bamboo-test-agents").join("w1"),
            "ws://broker:9600",
            "tok",
            &cfg,
            false,
        )
        .expect("at least one provider configured overall — spec must be built");

        let parsed: bamboo_subagent::provision::ProvisionSpec =
            serde_json::from_str(&spec_json).expect("valid ProvisionSpec JSON");
        assert!(parsed.secrets.provider_credentials.is_empty());
        assert!(!spec_json.contains("sk-ant-secret"));
        assert!(!spec_json.contains("sk-oai-secret"));
    }
}

#[cfg(test)]
mod presence_verify_tests {
    //! The non-echo deploy verify is a task-free presence probe: it must confirm a
    //! worker registered on the bus under its role, fail fast when it never came
    //! up, and stay role-scoped (so a worker under a different role doesn't count).
    use super::verify_worker_connected;
    use bamboo_config::BrokerClientConfig;
    use std::sync::Arc;
    use std::time::Duration;

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

    /// Connect + subscribe a worker so the broker's live-actor registry lists it
    /// under `role` (mailbox id == worker id, matching a resident deploy).
    async fn join(endpoint: &str, id: &str, role: &str) -> bamboo_broker::BrokerClient {
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

    fn cfg(endpoint: &str) -> BrokerClientConfig {
        BrokerClientConfig {
            endpoint: endpoint.to_string(),
            token: "t".into(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        }
    }

    #[tokio::test]
    async fn ok_when_worker_registered_under_role() {
        let (endpoint, _dir) = start_broker().await;
        let _worker = join(&endpoint, "w-mon", "monitor").await;
        let out =
            verify_worker_connected(&cfg(&endpoint), "w-mon", "monitor", Duration::from_secs(3))
                .await;
        assert!(out.is_ok(), "present worker should verify: {out:?}");
    }

    #[tokio::test]
    async fn times_out_when_worker_absent() {
        let (endpoint, _dir) = start_broker().await;
        // Nobody joined "monitor" — the probe must fail fast, never hang.
        let out = verify_worker_connected(
            &cfg(&endpoint),
            "w-mon",
            "monitor",
            Duration::from_millis(400),
        )
        .await;
        assert!(out.is_err(), "absent worker should fail verify");
        assert!(out.unwrap_err().contains("never registered"));
    }

    #[tokio::test]
    async fn role_scoped_ignores_worker_under_other_role() {
        let (endpoint, _dir) = start_broker().await;
        // Right id, wrong role bucket → must not satisfy a "monitor" probe.
        let _other = join(&endpoint, "w-mon", "builder").await;
        let out = verify_worker_connected(
            &cfg(&endpoint),
            "w-mon",
            "monitor",
            Duration::from_millis(400),
        )
        .await;
        assert!(
            out.is_err(),
            "a worker under a different role must not count"
        );
    }
}

#[cfg(test)]
mod health_check_tests {
    //! The health probe drives node status live: worker present → Running +
    //! last_health; worker gone → Unreachable; a non-deployed node is untouched.
    use super::*;
    use bamboo_config::cluster_fabric::{
        DeployProfile, Node, NodePlacement, NodeState, NodeStatus, TrustLevel,
    };
    use bamboo_config::{BrokerClientConfig, Config};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::{Mutex, RwLock};

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

    async fn join(endpoint: &str, id: &str, role: &str) -> bamboo_broker::BrokerClient {
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

    fn running_node(id: &str, worker_id: &str, role: &str) -> Node {
        Node {
            id: id.into(),
            label: id.into(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile {
                default_role: Some(role.into()),
                ..Default::default()
            },
            state: Some(NodeState {
                status: NodeStatus::Running,
                worker_id: Some(worker_id.into()),
                ..Default::default()
            }),
            enabled: true,
        }
    }

    fn deployer_with(nodes: Vec<Node>, endpoint: &str) -> Arc<FabricDeployer> {
        let mut cfg = Config::default();
        cfg.cluster_fabric.nodes = nodes;
        cfg.subagents_mut().broker = Some(BrokerClientConfig {
            endpoint: endpoint.into(),
            token: "t".into(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        });
        Arc::new(FabricDeployer::new(
            Arc::new(RwLock::new(cfg)),
            Arc::new(Mutex::new(())),
            std::env::temp_dir(),
            Arc::new(Mutex::new(HashMap::new())),
            "/usr/bin/true",
        ))
    }

    struct ModularDeployerFixture {
        deployer: Arc<FabricDeployer>,
        facade: Arc<ConfigFacade>,
        data_dir: tempfile::TempDir,
        events: Arc<StdMutex<Vec<ConfigSectionEvent>>>,
    }

    fn modular_deployer_with(nodes: Vec<Node>, endpoint: &str) -> ModularDeployerFixture {
        let dir = tempfile::tempdir().unwrap();
        let facade = Arc::new(ConfigFacade::open_or_migrate(dir.path()).unwrap());
        let mut candidate = facade.effective_config();
        candidate.cluster_fabric.nodes = nodes;
        let revision = bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeMap::new(),
            0,
        )
        .unwrap();
        assert_eq!(revision, 1);
        assert!(facade
            .registry()
            .reload_if_changed(SectionId::ClusterFabric)
            .is_some());

        let mut runtime = facade.effective_config();
        runtime.subagents_mut().broker = Some(BrokerClientConfig {
            endpoint: endpoint.into(),
            token: "t".into(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        });
        let config = Arc::new(RwLock::new(runtime));
        let events = Arc::new(StdMutex::new(Vec::new()));
        let event_log = events.clone();
        let deployer = Arc::new(
            FabricDeployer::new(
                config,
                Arc::new(Mutex::new(())),
                dir.path().to_path_buf(),
                Arc::new(Mutex::new(HashMap::new())),
                "/usr/bin/true",
            )
            .with_modular_persistence(
                facade.clone(),
                Arc::new(CredentialStore::open(dir.path())),
                Arc::new(move |event| event_log.lock().unwrap().push(event.clone())),
            ),
        );
        ModularDeployerFixture {
            deployer,
            facade,
            data_dir: dir,
            events,
        }
    }

    #[tokio::test]
    async fn boot_reconcile_advances_the_process_facade_before_runtime_and_event() {
        let node = running_node("a", "node-a", "mon");
        let fixture = modular_deployer_with(vec![node], "");

        assert_eq!(
            fixture
                .deployer
                .reconcile_stale_nodes_on_boot()
                .await
                .unwrap(),
            1
        );
        let process_snapshot = fixture.facade.registry().cluster_fabric.snapshot();
        assert_eq!(process_snapshot.revision, 2);
        assert_eq!(
            process_snapshot
                .data
                .0
                .node("a")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Unreachable
        );
        assert_eq!(
            fixture
                .deployer
                .config
                .read()
                .await
                .cluster_fabric
                .node("a")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Unreachable
        );
        let reopened = ConfigFacade::open(fixture.data_dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 2);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("a")
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .status,
            NodeStatus::Unreachable
        );
        assert_eq!(
            fixture.events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );
    }

    #[tokio::test]
    async fn keeps_running_when_worker_present() {
        let (endpoint, _dir) = start_broker().await;
        let _w = join(&endpoint, "node-a", "mon").await;
        let d = deployer_with(vec![running_node("a", "node-a", "mon")], &endpoint);
        let st = d
            .health_check_within("a", Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(st.status, NodeStatus::Running);
        assert!(st.last_health.is_some(), "heartbeat stamped");
        assert!(st.last_error.is_none());
    }

    #[tokio::test]
    async fn flips_to_unreachable_when_worker_gone() {
        let (endpoint, _dir) = start_broker().await;
        // Nobody joined the bus → the node's worker is absent.
        let d = deployer_with(vec![running_node("a", "node-a", "mon")], &endpoint);
        let st = d
            .health_check_within("a", Duration::from_millis(400))
            .await
            .unwrap();
        assert_eq!(st.status, NodeStatus::Unreachable);
        assert!(st.last_error.is_some());
    }

    #[tokio::test]
    async fn leaves_non_deployed_node_untouched() {
        let (endpoint, _dir) = start_broker().await;
        let mut node = running_node("a", "node-a", "mon");
        node.state = Some(NodeState {
            status: NodeStatus::Stopped,
            ..Default::default()
        });
        let d = deployer_with(vec![node], &endpoint);
        let st = d
            .health_check_within("a", Duration::from_millis(400))
            .await
            .unwrap();
        assert_eq!(
            st.status,
            NodeStatus::Stopped,
            "a stopped node is not probed"
        );
        assert!(st.last_health.is_none(), "no probe → no heartbeat");
    }

    #[tokio::test]
    async fn does_not_clobber_a_concurrent_status_change() {
        // The probe runs UNLOCKED; a stop() landing during it must win (otherwise a
        // user-Stopped node gets resurrected as Unreachable → wrongly auto-recovered).
        let (endpoint, _dir) = start_broker().await;
        // No worker joined → the probe runs its full timeout before deciding.
        let d = deployer_with(vec![running_node("a", "node-a", "mon")], &endpoint);
        let probe = {
            let d = d.clone();
            tokio::spawn(async move {
                d.health_check_within("a", Duration::from_millis(1500))
                    .await
            })
        };
        // Mid-probe, flip the node to Stopped as stop() would.
        tokio::time::sleep(Duration::from_millis(200)).await;
        {
            let mut cfg = d.config.write().await;
            cfg.cluster_fabric.node_mut("a").unwrap().state = Some(NodeState {
                status: NodeStatus::Stopped,
                ..Default::default()
            });
        }
        let observed = probe.await.unwrap().unwrap();
        assert_eq!(
            observed.status,
            NodeStatus::Stopped,
            "health_check yields to the stop"
        );
        let cfg = d.config.read().await;
        let st = cfg
            .cluster_fabric
            .node("a")
            .unwrap()
            .state
            .as_ref()
            .unwrap();
        assert_eq!(
            st.status,
            NodeStatus::Stopped,
            "Stopped not overwritten to Unreachable"
        );
    }

    #[tokio::test]
    async fn stale_probe_does_not_overwrite_a_newer_redeploy_revision() {
        let (endpoint, _dir) = start_broker().await;
        let fixture =
            modular_deployer_with(vec![running_node("a", "old-worker", "old-role")], &endpoint);
        let deployer = fixture.deployer.clone();
        let probe = {
            let deployer = deployer.clone();
            tokio::spawn(async move {
                deployer
                    .health_check_within("a", Duration::from_millis(1500))
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(200)).await;
        let winning_revision = deployer.current_cluster_revision().await.unwrap();
        let winner = deployer
            .persist_node_update_at_revision(winning_revision, |config| {
                let node = config.cluster_fabric.node_mut("a").unwrap();
                node.deploy.default_role = Some("new-role".to_string());
                node.state = Some(NodeState {
                    status: NodeStatus::Running,
                    worker_id: Some("new-worker".to_string()),
                    ..Default::default()
                });
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(winner.section.revision, 2);

        let observed = probe.await.unwrap().unwrap();
        assert_eq!(observed.status, NodeStatus::Running);
        assert_eq!(observed.worker_id.as_deref(), Some("new-worker"));
        let process_snapshot = fixture.facade.registry().cluster_fabric.snapshot();
        assert_eq!(
            process_snapshot.revision, 2,
            "the stale probe must not create revision 3"
        );
        let node = deployer
            .config
            .read()
            .await
            .cluster_fabric
            .node("a")
            .cloned()
            .unwrap();
        assert_eq!(node.deploy.default_role.as_deref(), Some("new-role"));
        assert_eq!(node.state.unwrap().worker_id.as_deref(), Some("new-worker"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_probe_cannot_outlive_owner_and_overwrite_replacement_owner() {
        let (endpoint, _dir) = start_broker().await;
        let fixture =
            modular_deployer_with(vec![running_node("a", "old-worker", "old-role")], &endpoint);
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        set_health_after_probe_test_hook(fixture.data_dir.path(), reached_tx, release_rx);

        let probe = {
            let deployer = fixture.deployer.clone();
            tokio::spawn(async move {
                deployer
                    .health_check_within("a", Duration::from_millis(150))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .expect("probe must reach its owner-cancellable boundary")
            .expect("probe boundary signal must remain live");
        probe.abort();
        assert!(probe.await.unwrap_err().is_cancelled());
        assert!(
            release_tx.send(()).is_err(),
            "cancelling the owner must drop the suspended probe"
        );

        let replacement_facade = Arc::new(ConfigFacade::open(fixture.data_dir.path()).unwrap());
        let replacement_config =
            Arc::new(RwLock::new(fixture.deployer.config.read().await.clone()));
        let replacement_events = Arc::new(StdMutex::new(Vec::new()));
        let replacement_event_log = replacement_events.clone();
        let replacement = Arc::new(
            FabricDeployer::new(
                replacement_config.clone(),
                Arc::new(Mutex::new(())),
                fixture.data_dir.path().to_path_buf(),
                Arc::new(Mutex::new(HashMap::new())),
                "/usr/bin/true",
            )
            .with_modular_persistence(
                replacement_facade.clone(),
                Arc::new(CredentialStore::open(fixture.data_dir.path())),
                Arc::new(move |event| replacement_event_log.lock().unwrap().push(event.clone())),
            ),
        );
        let winner = replacement
            .persist_node_update_at_revision(1, |config| {
                let node = config.cluster_fabric.node_mut("a").unwrap();
                node.label = "replacement-owner".to_string();
                node.deploy.default_role = Some("new-role".to_string());
                node.state = Some(NodeState {
                    status: NodeStatus::Running,
                    worker_id: Some("new-worker".to_string()),
                    ..Default::default()
                });
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(winner.section.revision, 2);

        tokio::time::sleep(Duration::from_millis(300)).await;
        let reopened = ConfigFacade::open(fixture.data_dir.path()).unwrap();
        assert_eq!(
            reopened.registry().cluster_fabric.snapshot().revision,
            2,
            "the cancelled stale probe must never create revision 3"
        );
        let replacement_node = replacement_config
            .read()
            .await
            .cluster_fabric
            .node("a")
            .cloned()
            .unwrap();
        assert_eq!(replacement_node.label, "replacement-owner");
        assert_eq!(
            replacement_node.state.unwrap().worker_id.as_deref(),
            Some("new-worker")
        );
        assert_eq!(
            replacement_events.lock().unwrap().as_slice(),
            &[ConfigSectionEvent::Changed {
                section: "cluster-fabric".to_string(),
                revision: 2,
            }]
        );
        assert!(
            fixture.events.lock().unwrap().is_empty(),
            "the cancelled owner must publish no stale health event"
        );
    }
}

#[cfg(test)]
mod recovery_tests {
    //! Auto-recovery POLICY (no bus/deploy needed): opt-in gate, 2-miss debounce,
    //! exponential backoff between attempts, and a cap that marks the node Failed.
    use super::*;
    use bamboo_config::cluster_fabric::{
        DeployProfile, Node, NodePlacement, NodeState, NodeStatus, TrustLevel,
    };
    use bamboo_config::Config;
    use tokio::sync::{Mutex, RwLock};

    fn recoverable_node(id: &str) -> Node {
        Node {
            id: id.into(),
            label: id.into(),
            placement: NodePlacement::Local,
            trust_level: TrustLevel::Trusted,
            deploy: DeployProfile {
                default_role: Some("mon".into()),
                auto_recover: true,
                ..Default::default()
            },
            state: Some(NodeState {
                status: NodeStatus::Unreachable,
                ..Default::default()
            }),
            enabled: true,
        }
    }

    fn deployer(nodes: Vec<Node>) -> (Arc<FabricDeployer>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.cluster_fabric.nodes = nodes;
        let d = Arc::new(FabricDeployer::new(
            Arc::new(RwLock::new(cfg)),
            Arc::new(Mutex::new(())),
            dir.path().to_path_buf(),
            Arc::new(Mutex::new(HashMap::new())),
            "/usr/bin/true",
        ));
        (d, dir)
    }

    fn unreachable() -> NodeState {
        NodeState {
            status: NodeStatus::Unreachable,
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn debounces_backs_off_then_caps_to_failed() {
        let (d, _dir) = deployer(vec![recoverable_node("a")]);
        let un = unreachable();

        // 1st miss → debounce (no action); 2nd → first redeploy (attempt in flight).
        assert_eq!(d.recovery_decision("a", &un).await, None);
        assert_eq!(d.recovery_decision("a", &un).await, Some(1));
        // In-flight guard: no overlapping attempt, even once backoff elapses.
        assert_eq!(d.recovery_decision("a", &un).await, None);
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(d.recovery_decision("a", &un).await, None, "still in-flight");
        // Redeploy settled (failed) → guard clears; past backoff(1) → attempt 2.
        d.clear_recovery_in_flight("a").await;
        assert_eq!(d.recovery_decision("a", &un).await, Some(2));
        d.clear_recovery_in_flight("a").await;
        tokio::time::advance(Duration::from_secs(21)).await;
        assert_eq!(d.recovery_decision("a", &un).await, Some(3));
        // Past backoff(3) → cap reached → None, and the node is marked Failed.
        d.clear_recovery_in_flight("a").await;
        tokio::time::advance(Duration::from_secs(41)).await;
        assert_eq!(d.recovery_decision("a", &un).await, None);
        let cfg = d.config.read().await;
        let st = cfg
            .cluster_fabric
            .node("a")
            .unwrap()
            .state
            .as_ref()
            .unwrap();
        assert_eq!(st.status, NodeStatus::Failed);
        assert!(st
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("gave up"));
    }

    #[tokio::test]
    async fn no_recovery_when_flag_off() {
        let mut node = recoverable_node("a");
        node.deploy.auto_recover = false;
        let (d, _dir) = deployer(vec![node]);
        let un = unreachable();
        assert_eq!(d.recovery_decision("a", &un).await, None);
        assert_eq!(
            d.recovery_decision("a", &un).await,
            None,
            "opt-out never redeploys"
        );
    }

    #[tokio::test]
    async fn recovered_node_clears_progress() {
        let (d, _dir) = deployer(vec![recoverable_node("a")]);
        let un = unreachable();
        let ok = NodeState {
            status: NodeStatus::Running,
            ..Default::default()
        };
        assert_eq!(d.recovery_decision("a", &un).await, None); // miss 1
        assert_eq!(d.recovery_decision("a", &ok).await, None); // recovered → reset
                                                               // Debounce restarts from zero, so it takes another 2 misses.
        assert_eq!(d.recovery_decision("a", &un).await, None);
        assert_eq!(d.recovery_decision("a", &un).await, Some(1));
    }
}
