use super::*;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bamboo_config::{
    ensure_provider_mcp_migration_ready, AtomicJsonStore, ConfigDirectoryWatcher,
    ConfigSectionEvent, ConfigStoreError, McpSection, ProviderConfigs, SectionId,
    SectionSourceKind, SectionStatus,
};
use bamboo_mcp::{McpConfig, McpServerManager, TransportConfig};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

#[cfg(test)]
struct InitialMcpApplyTestHook {
    before: Box<dyn FnOnce() + Send + 'static>,
    after: Box<dyn FnOnce() + Send + 'static>,
}

#[cfg(test)]
fn initial_mcp_apply_test_hooks(
) -> &'static std::sync::Mutex<std::collections::HashMap<PathBuf, InitialMcpApplyTestHook>> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<PathBuf, InitialMcpApplyTestHook>>,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_initial_mcp_apply_test_hook(
    data_dir: &Path,
    before: impl FnOnce() + Send + 'static,
    after: impl FnOnce() + Send + 'static,
) {
    initial_mcp_apply_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            data_dir.to_path_buf(),
            InitialMcpApplyTestHook {
                before: Box::new(before),
                after: Box::new(after),
            },
        );
}

#[cfg(test)]
struct InitialMcpApplyTestCompletion(Option<Box<dyn FnOnce() + Send + 'static>>);

#[cfg(test)]
impl Drop for InitialMcpApplyTestCompletion {
    fn drop(&mut self) {
        if let Some(after) = self.0.take() {
            after();
        }
    }
}

#[cfg(test)]
fn begin_initial_mcp_apply_test_hook(data_dir: &Path) -> InitialMcpApplyTestCompletion {
    let hook = initial_mcp_apply_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    let Some(hook) = hook else {
        return InitialMcpApplyTestCompletion(None);
    };
    (hook.before)();
    InitialMcpApplyTestCompletion(Some(hook.after))
}

#[cfg(test)]
struct ClusterAfterCommitBeforeAdoptionTestHook {
    expected_revision: u64,
    hook: Box<dyn FnOnce(&Path) + Send + 'static>,
}

#[cfg(test)]
fn cluster_after_commit_before_adoption_test_hooks() -> &'static std::sync::Mutex<
    std::collections::HashMap<PathBuf, ClusterAfterCommitBeforeAdoptionTestHook>,
> {
    static HOOKS: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<PathBuf, ClusterAfterCommitBeforeAdoptionTestHook>,
        >,
    > = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_cluster_after_commit_before_adoption_test_hook(
    data_dir: &Path,
    expected_revision: u64,
    hook: impl FnOnce(&Path) + Send + 'static,
) {
    cluster_after_commit_before_adoption_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            data_dir.to_path_buf(),
            ClusterAfterCommitBeforeAdoptionTestHook {
                expected_revision,
                hook: Box::new(hook),
            },
        );
}

#[cfg(test)]
fn run_cluster_after_commit_before_adoption_test_hook(data_dir: &Path, expected_revision: u64) {
    let hook = {
        let mut hooks = cluster_after_commit_before_adoption_test_hooks()
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
type CredentialCommitTestHook = Box<dyn FnOnce() + Send + 'static>;
#[cfg(test)]
type CredentialCommitTestHooks =
    std::sync::Mutex<std::collections::HashMap<(PathBuf, SectionId), CredentialCommitTestHook>>;

#[cfg(test)]
fn credential_after_commit_before_live_test_hooks() -> &'static CredentialCommitTestHooks {
    static HOOKS: std::sync::OnceLock<CredentialCommitTestHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_credential_after_commit_before_live_test_hook(
    data_dir: &Path,
    section: SectionId,
    hook: impl FnOnce() + Send + 'static,
) {
    credential_after_commit_before_live_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert((data_dir.to_path_buf(), section), Box::new(hook));
}

#[cfg(test)]
fn run_credential_after_commit_before_live_test_hook(data_dir: &Path, section: SectionId) {
    let hook = credential_after_commit_before_live_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&(data_dir.to_path_buf(), section));
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
type GenericBeforeEventTestHook = Box<dyn FnOnce() + Send + 'static>;
#[cfg(test)]
type GenericBeforeEventTestHooks =
    std::sync::Mutex<std::collections::HashMap<PathBuf, GenericBeforeEventTestHook>>;

#[cfg(test)]
fn generic_before_event_test_hooks() -> &'static GenericBeforeEventTestHooks {
    static HOOKS: std::sync::OnceLock<GenericBeforeEventTestHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_generic_before_event_test_hook(data_dir: &Path, hook: impl FnOnce() + Send + 'static) {
    generic_before_event_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_generic_before_event_test_hook(data_dir: &Path) {
    let hook = generic_before_event_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
type GenericBeforeProviderPublishTestHook = Box<dyn FnOnce() + Send + 'static>;
#[cfg(test)]
type GenericBeforeProviderPublishTestHooks =
    std::sync::Mutex<std::collections::HashMap<PathBuf, GenericBeforeProviderPublishTestHook>>;

#[cfg(test)]
fn generic_before_provider_publish_test_hooks() -> &'static GenericBeforeProviderPublishTestHooks {
    static HOOKS: std::sync::OnceLock<GenericBeforeProviderPublishTestHooks> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_generic_before_provider_publish_test_hook(
    data_dir: &Path,
    hook: impl FnOnce() + Send + 'static,
) {
    generic_before_provider_publish_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_generic_before_provider_publish_test_hook(data_dir: &Path) {
    let hook = generic_before_provider_publish_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
type ResetAfterDeleteTestHook = Box<dyn FnOnce() + Send + 'static>;
#[cfg(test)]
type ResetAfterDeleteTestHooks =
    std::sync::Mutex<std::collections::HashMap<PathBuf, ResetAfterDeleteTestHook>>;

#[cfg(test)]
fn reset_after_delete_test_hooks() -> &'static ResetAfterDeleteTestHooks {
    static HOOKS: std::sync::OnceLock<ResetAfterDeleteTestHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn set_reset_after_delete_test_hook(data_dir: &Path, hook: impl FnOnce() + Send + 'static) {
    reset_after_delete_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_reset_after_delete_test_hook(data_dir: &Path) {
    let hook = reset_after_delete_test_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir);
    if let Some(hook) = hook {
        hook();
    }
}

struct FacadeRuntimeMaterialization {
    config: Config,
    failures: BTreeSet<SectionId>,
}

fn materialize_facade_effective_config(
    facade: &bamboo_config::ConfigFacade,
    data_dir: &Path,
) -> FacadeRuntimeMaterialization {
    let mut config = facade.effective_config();
    let mut failures = BTreeSet::new();
    if let Err(error) = config.hydrate_proxy_auth_from_store(data_dir) {
        tracing::warn!(error = %error, "proxy auth credential hydration unavailable");
        config.proxy_auth = None;
        failures.insert(SectionId::Core);
    }
    if let Err(error) = config.hydrate_provider_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "provider credential hydration unavailable");
        failures.insert(SectionId::Providers);
    }
    if let Err(error) = config.hydrate_mcp_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "MCP credential hydration unavailable");
        failures.insert(SectionId::Mcp);
    }
    if let Err(error) = config.hydrate_env_var_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "env credential hydration unavailable");
        for entry in &mut config.env_vars {
            if entry.secret {
                entry.value.clear();
            }
        }
        failures.insert(SectionId::Env);
    }
    if let Err(error) = config.hydrate_cluster_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "cluster credential hydration unavailable");
        failures.insert(SectionId::ClusterFabric);
    }
    if let Err(error) = config.hydrate_notification_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "notification credential hydration unavailable");
        config.notifications.ntfy.token = None;
        config.notifications.bark.device_key = None;
        failures.insert(SectionId::Notifications);
    }
    if let Err(error) = config.hydrate_connect_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "connect credential hydration unavailable");
        for platform in &mut config.connect.platforms {
            platform.token = None;
            platform.app_secret = None;
        }
        failures.insert(SectionId::Connect);
    }
    if let Err(error) = config.hydrate_access_control_credentials_from_store(data_dir) {
        tracing::warn!(error = %error, "access-control credential hydration unavailable");
        config.clear_access_control_runtime_verifiers();
        failures.insert(SectionId::AccessControl);
    }
    if config
        .access_control
        .as_ref()
        .is_some_and(|access| access.repair_required)
    {
        failures.insert(SectionId::AccessControl);
    }
    if let Some(broker) = config.subagents_mut().broker.as_mut() {
        if let Err(error) = broker.hydrate_credential_from_store(data_dir) {
            tracing::warn!(error = %error, "external broker credential hydration unavailable");
            broker.token.clear();
            failures.insert(SectionId::Subagents);
        }
    }
    config.apply_runtime_env_overrides();
    FacadeRuntimeMaterialization { config, failures }
}

pub(super) fn load_facade_effective_config(
    facade: &bamboo_config::ConfigFacade,
    data_dir: &Path,
) -> Config {
    let materialized = materialize_facade_effective_config(facade, data_dir);
    for section in &materialized.failures {
        facade.registry().mark_runtime_degraded(
            *section,
            "configuration runtime credential repair is required",
        );
    }
    materialized.config
}

fn load_committed_effective_config(data_dir: &Path) -> Result<Config, ConfigStoreError> {
    if bamboo_config::modular_authority_boundary_present(data_dir)? {
        let facade = bamboo_config::ConfigFacade::open_or_migrate(data_dir)?;
        let config = load_facade_effective_config(&facade, data_dir);
        Ok(config)
    } else {
        Ok(Config::from_data_dir_without_publish(Some(
            data_dir.to_path_buf(),
        )))
    }
}

/// Health metadata for the server-owned effective/provider configuration view.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigLiveHealth {
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

/// Owns the blocking filesystem watcher and async runtime-apply task.
pub struct ConfigWatcherRuntime {
    stop: Arc<AtomicBool>,
    watcher_task: Option<std::thread::JoinHandle<()>>,
    apply_task: Option<tokio::task::JoinHandle<()>>,
}

struct ConfigPathChanges {
    paths: Vec<PathBuf>,
    initial_mcp_revision: Option<u64>,
    startup_legacy_root: Option<bamboo_config::LegacyRootReconciliationOutcome>,
    startup_recoveries: BTreeMap<SectionId, u64>,
    legacy_root_retry_attempt: u8,
}

/// Owned handles needed to publish provider and MCP effects for one committed
/// configuration generation. Keeping them together makes it explicit that a
/// detached transaction carries one immutable publication context end to end.
struct ConfigRuntimeEffectContext {
    app_data_dir: PathBuf,
    config_facade: Option<Arc<bamboo_config::ConfigFacade>>,
    provider_registry: Arc<bamboo_llm::ProviderRegistry>,
    provider: Arc<RwLock<Arc<dyn LLMProvider>>>,
    mcp_manager: Arc<McpServerManager>,
    account_sink: Arc<bamboo_engine::events::AccountEventSink>,
    config_live_health: Arc<std::sync::RwLock<ConfigLiveHealth>>,
    mcp_config_live_health: Arc<std::sync::RwLock<ConfigLiveHealth>>,
}

impl ConfigWatcherRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        data_dir: PathBuf,
        config: Arc<RwLock<Config>>,
        config_facade: Option<Arc<bamboo_config::ConfigFacade>>,
        config_io_lock: Arc<tokio::sync::Mutex<()>>,
        provider_registry: Arc<bamboo_llm::ProviderRegistry>,
        provider: Arc<RwLock<Arc<dyn LLMProvider>>>,
        mcp_manager: Arc<McpServerManager>,
        account_sink: Arc<bamboo_engine::events::AccountEventSink>,
    ) -> (
        Self,
        Arc<std::sync::RwLock<ConfigLiveHealth>>,
        Arc<std::sync::RwLock<ConfigLiveHealth>>,
    ) {
        let provider_store = AtomicJsonStore::new(data_dir.join("providers.json"), 1);
        let provider_health = Arc::new(std::sync::RwLock::new(initial_provider_health(
            &provider_store,
        )));
        let mcp_store = AtomicJsonStore::new(data_dir.join("mcp.json"), 1);
        let mcp_health = Arc::new(std::sync::RwLock::new(initial_mcp_health(&mcp_store)));
        let stop = Arc::new(AtomicBool::new(false));
        let watcher = match ConfigDirectoryWatcher::watch(&data_dir, Duration::from_millis(120)) {
            Ok(watcher) => watcher,
            Err(error) => {
                tracing::warn!(error = %error, "live configuration watcher could not start");
                {
                    let mut value = provider_health
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    value.status = SectionStatus::Degraded;
                    value.last_error = Some("configuration watcher unavailable".to_string());
                }
                {
                    let mut value = mcp_health
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    value.status = SectionStatus::Degraded;
                    value.last_error = Some("configuration watcher unavailable".to_string());
                }
                return (
                    Self {
                        stop,
                        watcher_task: None,
                        apply_task: None,
                    },
                    provider_health,
                    mcp_health,
                );
            }
        };

        // The filesystem side must never block in send: Drop joins this OS
        // thread after aborting the async consumer, so a bounded blocking_send
        // could deadlock shutdown if its queue were full.
        let self_write_marker = watcher.self_write_marker();
        let startup_legacy_root = config_facade
            .as_ref()
            .and_then(|facade| facade.take_startup_legacy_root_reconciliation());
        let (changes_tx, mut changes_rx) =
            tokio::sync::mpsc::unbounded_channel::<ConfigPathChanges>();
        let initial_changes = changes_tx.clone();
        let worker_stop = stop.clone();
        let watcher_task = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                match watcher.recv_timeout(Duration::from_millis(250)) {
                    Ok(paths) => {
                        if changes_tx
                            .send(ConfigPathChanges {
                                paths,
                                initial_mcp_revision: None,
                                startup_legacy_root: None,
                                startup_recoveries: BTreeMap::new(),
                                legacy_root_retry_attempt: 0,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        // A sidecar may already exist before the server starts. Queue it through
        // the exact same candidate/runtime transaction as later filesystem
        // events; parse-only initial health must never be mistaken for a
        // published runtime snapshot.
        let initial_mcp_path = data_dir.join("mcp.json");
        let mut initial_paths = Vec::new();
        let initial_mcp_revision = if initial_mcp_path.exists() {
            initial_paths.push(initial_mcp_path);
            Some(
                config_facade
                    .as_ref()
                    .map(|facade| facade.registry().mcp.snapshot().revision)
                    .unwrap_or_else(|| {
                        mcp_health
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .revision
                    }),
            )
        } else {
            None
        };
        let rejected = bamboo_config::legacy_root_rejected_sections(&data_dir);
        let startup_recoveries = match (config_facade.as_ref(), rejected.as_ref()) {
            (Some(facade), Ok(rejected)) => {
                durable_invalid_recoveries(&account_sink, facade, rejected)
            }
            _ => BTreeMap::new(),
        };
        if let (Some(facade), Ok(rejected)) = (config_facade.as_ref(), rejected.as_ref()) {
            if let Ok(health) = facade.registry().health() {
                initial_paths.extend(
                    health
                        .into_iter()
                        .filter(|health| {
                            health.status != SectionStatus::Healthy
                                && !rejected.contains(&health.section)
                        })
                        .map(|health| data_dir.join(health.section.descriptor().file_name)),
                );
            }
        }
        initial_paths.extend(
            startup_recoveries
                .keys()
                .map(|id| data_dir.join(id.descriptor().file_name)),
        );
        // Every modular watcher start performs one catch-up pass. This is the
        // recovery seam for crashes after a canonical outbox/rejection update
        // but before its runtime or account-feed publication. The pass is
        // content-deduped and does not make config.json authoritative again.
        let legacy_root_needs_reconciliation = config_facade.is_some();
        if legacy_root_needs_reconciliation {
            initial_paths.push(data_dir.join("config.json"));
        }
        if !initial_paths.is_empty() {
            let _ = initial_changes.send(ConfigPathChanges {
                paths: initial_paths,
                initial_mcp_revision,
                startup_legacy_root,
                startup_recoveries,
                legacy_root_retry_attempt: 0,
            });
        }

        let apply_provider_health = provider_health.clone();
        let apply_mcp_health = mcp_health.clone();
        let catchup_changes = initial_changes.clone();
        let apply_task = tokio::spawn(async move {
            let mut reported_root_runtime_failures = BTreeSet::<(SectionId, u64)>::new();
            while let Some(mut changes) = changes_rx.recv().await {
                let mut watched_sections = config_facade
                    .as_ref()
                    .map(|_| {
                        changes
                            .paths
                            .iter()
                            .filter_map(|path| {
                                path.file_name()
                                    .and_then(|name| name.to_str())
                                    .and_then(SectionId::from_file_name)
                            })
                            .collect::<BTreeSet<_>>()
                    })
                    .unwrap_or_default();
                let direct_watched_sections = watched_sections.clone();
                let legacy_root_watched = config_facade.is_some()
                    && changes.paths.iter().any(|path| {
                        matches!(
                            path.file_name().and_then(|name| name.to_str()),
                            Some(
                                "config.json"
                                    | "config.json.bak"
                                    | "config.json.bak.1"
                                    | "config.json.bak.2"
                            )
                        )
                    });
                let mut provider_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("providers.json")
                });
                let mut mcp_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("mcp.json")
                });
                if !provider_watched
                    && !mcp_watched
                    && watched_sections.is_empty()
                    && !legacy_root_watched
                {
                    continue;
                }

                #[cfg(test)]
                let _initial_mcp_apply_completion = changes
                    .initial_mcp_revision
                    .map(|_| begin_initial_mcp_apply_test_hook(&data_dir));

                // Serialize candidate construction and publication with config
                // writers. Otherwise a slow provider build could later publish
                // a clone taken before an unrelated API update and clobber it.
                let _io = config_io_lock.lock().await;
                let mut synthetic_root_events = BTreeMap::<SectionId, ConfigSectionEvent>::new();
                let mut pending_root_publications =
                    BTreeMap::<SectionId, ConfigSectionEvent>::new();
                let startup_root_batch = changes.startup_legacy_root.is_some();
                let mut requeue_legacy_root = false;
                let mut retry_legacy_root_publication = false;
                let mut canonical_root_rejections = BTreeSet::new();
                if legacy_root_watched {
                    let reconciliation = match changes.startup_legacy_root.take() {
                        Some(outcome) => Ok(Some(outcome)),
                        None => {
                            let reconcile_facade = config_facade
                                .as_ref()
                                .expect("legacy root watching requires a facade")
                                .clone();
                            tokio::task::spawn_blocking(move || {
                                reconcile_facade.reconcile_reappeared_legacy_root()
                            })
                            .await
                            .map_err(|_| {
                                ConfigStoreError::Validation(
                                    "legacy root reconciliation task failed".to_string(),
                                )
                            })
                            .and_then(|result| result)
                        }
                    };
                    match reconciliation {
                        Ok(Some(outcome)) if !outcome.duplicate => {
                            requeue_legacy_root = outcome.partial || startup_root_batch;
                            for event in &outcome.committed {
                                let section = match event {
                                    ConfigSectionEvent::Changed { section, .. }
                                    | ConfigSectionEvent::Invalid { section, .. }
                                    | ConfigSectionEvent::Recovered { section, .. } => section,
                                };
                                if let Some(id) = SectionId::from_name(section) {
                                    watched_sections.insert(id);
                                    synthetic_root_events.insert(id, event.clone());
                                    if matches!(
                                        event,
                                        ConfigSectionEvent::Changed { .. }
                                            | ConfigSectionEvent::Recovered { .. }
                                    ) {
                                        pending_root_publications.insert(id, event.clone());
                                    }
                                }
                            }
                            if let Some(facade) = config_facade.as_ref() {
                                for id in &outcome.recovered {
                                    queue_legacy_root_recovery(
                                        facade,
                                        *id,
                                        None,
                                        &mut watched_sections,
                                        &mut synthetic_root_events,
                                    );
                                }
                                for rejection in outcome.rejected {
                                    canonical_root_rejections.insert(rejection.section);
                                    if rejection.reason
                                        == bamboo_config::LegacyRootRejectionReason::RevisionConflict
                                    {
                                        watched_sections.insert(rejection.section);
                                    }
                                    if let Some(event) = legacy_root_rejection_event(
                                        facade,
                                        rejection.section,
                                        rejection.reason.diagnostic(),
                                        startup_root_batch,
                                    ) {
                                        publish_registry_event(&account_sink, &event).await;
                                    }
                                }
                            }
                            if outcome.partial {
                                tracing::warn!(
                                    "legacy config root changed during reconciliation; awaiting the newer generation"
                                );
                            }
                        }
                        Ok(Some(outcome)) => {
                            requeue_legacy_root = outcome.partial || startup_root_batch;
                            // Another facade/process may have committed this
                            // exact root while this process registry still
                            // lags. Keep its outbox pending and synthesize only
                            // when the process already owns the exact healthy
                            // revision; otherwise the normal reload path must
                            // install it before the durable event is acked.
                            if let Some(facade) = config_facade.as_ref() {
                                for id in &outcome.recovered {
                                    queue_legacy_root_recovery(
                                        facade,
                                        *id,
                                        None,
                                        &mut watched_sections,
                                        &mut synthetic_root_events,
                                    );
                                }
                                for event in &outcome.committed {
                                    let (section, revision) = match event {
                                        ConfigSectionEvent::Changed { section, revision }
                                        | ConfigSectionEvent::Recovered { section, revision } => {
                                            (section, *revision)
                                        }
                                        ConfigSectionEvent::Invalid { .. } => continue,
                                    };
                                    let Some(id) = SectionId::from_name(section) else {
                                        continue;
                                    };
                                    watched_sections.insert(id);
                                    pending_root_publications.insert(id, event.clone());
                                    if facade.registry().envelope_value(id).is_ok_and(|envelope| {
                                        envelope.revision == revision
                                            && envelope.status == SectionStatus::Healthy
                                    }) {
                                        synthetic_root_events.insert(id, event.clone());
                                    }
                                }
                                for rejection in outcome.rejected {
                                    canonical_root_rejections.insert(rejection.section);
                                    if rejection.reason
                                        == bamboo_config::LegacyRootRejectionReason::RevisionConflict
                                    {
                                        watched_sections.insert(rejection.section);
                                    }
                                    if let Some(event) = legacy_root_rejection_event(
                                        facade,
                                        rejection.section,
                                        rejection.reason.diagnostic(),
                                        startup_root_batch,
                                    ) {
                                        publish_registry_event(&account_sink, &event).await;
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            if bamboo_config::modular_authority_boundary_present(&data_dir)
                                .unwrap_or(false)
                            {
                                if let Some(facade) = config_facade.as_ref() {
                                    if let Some(event) = facade.registry().mark_runtime_degraded(
                                        SectionId::Core,
                                        "completed modular configuration reconciliation is unavailable",
                                    ) {
                                        publish_registry_event(&account_sink, &event).await;
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            if let Some(facade) = config_facade.as_ref() {
                                if let Some(event) = facade.registry().mark_runtime_degraded(
                                    SectionId::Core,
                                    "legacy config root reconciliation is unavailable",
                                ) {
                                    publish_registry_event(&account_sink, &event).await;
                                }
                            }
                        }
                    }
                    if let Some(facade) = config_facade.as_ref() {
                        for (id, event) in pending_root_publications.clone() {
                            let revision = config_section_event_revision(&event);
                            if !pending_root_publication_matches_fresh_durable(
                                &data_dir, facade, id, revision,
                            ) {
                                // A typed writer advanced after root
                                // reconciliation released the migration lock.
                                // Never materialize or journal the superseded
                                // root revision. Catch the typed generation up
                                // now, then requeue the root so its durable
                                // outbox is rejected as a revision conflict.
                                pending_root_publications.remove(&id);
                                synthetic_root_events.remove(&id);
                                watched_sections.insert(id);
                                requeue_legacy_root = true;
                            }
                        }
                    }
                    provider_watched |= watched_sections.contains(&SectionId::Providers);
                    mcp_watched |= watched_sections.contains(&SectionId::Mcp);
                }
                if let Some(facade) = config_facade.as_ref() {
                    for (id, revision) in std::mem::take(&mut changes.startup_recoveries) {
                        if !canonical_root_rejections.contains(&id) {
                            queue_legacy_root_recovery(
                                facade,
                                id,
                                Some(revision),
                                &mut watched_sections,
                                &mut synthetic_root_events,
                            );
                        }
                    }
                    provider_watched |= watched_sections.contains(&SectionId::Providers);
                    mcp_watched |= watched_sections.contains(&SectionId::Mcp);
                }
                // If notify coalesced the compatibility root and a later
                // direct typed generation, process the root's exact adopted
                // snapshot first, then queue one bounded catch-up pass. The
                // next pass has no synthetic root event and therefore cannot
                // recursively requeue itself.
                let catchup_paths = synthetic_root_events
                    .keys()
                    .filter(|id| startup_root_batch || direct_watched_sections.contains(id))
                    .map(|id| data_dir.join(id.descriptor().file_name))
                    .collect::<Vec<_>>();
                let ordinary_watched = watched_sections
                    .iter()
                    .copied()
                    .filter(|id| !matches!(id, SectionId::Providers | SectionId::Mcp))
                    .collect::<Vec<_>>();
                if let Some(facade) = config_facade.as_ref() {
                    retry_legacy_root_publication |= reload_and_apply_ordinary_sections(
                        &data_dir,
                        &config,
                        facade,
                        &account_sink,
                        ordinary_watched,
                        OrdinarySectionReloadState {
                            synthetic_events: &mut synthetic_root_events,
                            pending_root_publications: &mut pending_root_publications,
                            reported_root_runtime_failures: &mut reported_root_runtime_failures,
                        },
                    )
                    .await;
                }
                if provider_watched {
                    if let Some(facade) = config_facade.as_ref() {
                        wait_for_section_file_settle(&data_dir, SectionId::Providers).await;
                        if let Some(observed) = synthetic_root_events
                            .remove(&SectionId::Providers)
                            .or_else(|| facade.registry().reload_if_changed(SectionId::Providers))
                        {
                            if let Some(event) = pending_root_publication_event(
                                &data_dir,
                                facade,
                                &account_sink,
                                &pending_root_publications,
                                SectionId::Providers,
                                observed,
                            ) {
                                if matches!(event, ConfigSectionEvent::Invalid { .. }) {
                                    publish_section_failure(
                                        &apply_provider_health,
                                        &account_sink,
                                        "providers",
                                        facade.registry().providers.snapshot().status,
                                        "provider section is invalid; retaining last-known-good runtime"
                                            .to_string(),
                                    )
                                    .await;
                                } else {
                                    self_write_marker.mark_self_write(provider_store.path());
                                    let materialized =
                                        materialize_facade_effective_config(facade, &data_dir);
                                    if materialized.failures.contains(&SectionId::Providers) {
                                        retry_legacy_root_publication |=
                                        publish_staged_facade_section_failure(
                                            &data_dir,
                                            facade,
                                            SectionId::Providers,
                                            "provider credential hydration failed; retaining last-known-good runtime",
                                            StagedFacadeSectionFailureContext {
                                                health: &apply_provider_health,
                                                account_sink: &account_sink,
                                                section: "providers",
                                                pending_root_publications: &pending_root_publications,
                                            },
                                        )
                                        .await;
                                    } else {
                                        let mut candidate = config.read().await.clone();
                                        apply_runtime_section(
                                            SectionId::Providers,
                                            &materialized.config,
                                            &mut candidate,
                                        );
                                        match prepare_provider_candidate(candidate, &data_dir).await
                                        {
                                            Ok((candidate, registry, next_provider)) => {
                                                let mut live_config = config.write().await;
                                                let mut live_provider = provider.write().await;
                                                let recovered =
                                                    section_is_unhealthy(&apply_provider_health);
                                                candidate.publish_env_vars();
                                                *live_config = candidate;
                                                provider_registry.replace_with(registry);
                                                *live_provider = next_provider;
                                                drop(live_provider);
                                                drop(live_config);
                                                let revision =
                                                    facade.registry().providers.snapshot().revision;
                                                if pending_root_publications
                                                    .get(&SectionId::Providers)
                                                    .is_some_and(|event| {
                                                        config_section_event_revision(event)
                                                            == revision
                                                    })
                                                {
                                                    set_live_health_revision(
                                                        &apply_provider_health,
                                                        revision,
                                                        Some((
                                                            data_dir.join("providers.json"),
                                                            SectionSourceKind::File,
                                                        )),
                                                    );
                                                    retry_legacy_root_publication |=
                                                        publish_registry_event_with_root_ack(
                                                            &data_dir,
                                                            &account_sink,
                                                            &mut pending_root_publications,
                                                            SectionId::Providers,
                                                            &event,
                                                        )
                                                        .await;
                                                } else if matches!(
                                                    event,
                                                    ConfigSectionEvent::Recovered { .. }
                                                ) {
                                                    set_live_health_revision(
                                                        &apply_provider_health,
                                                        revision,
                                                        Some((
                                                            data_dir.join("providers.json"),
                                                            SectionSourceKind::File,
                                                        )),
                                                    );
                                                    publish_registry_event(&account_sink, &event)
                                                        .await;
                                                } else {
                                                    publish_section_success(
                                                        &apply_provider_health,
                                                        &account_sink,
                                                        "providers",
                                                        data_dir.join("providers.json"),
                                                        recovered,
                                                        Some(revision),
                                                    )
                                                    .await;
                                                }
                                            }
                                            Err(_) => {
                                                retry_legacy_root_publication |=
                                                publish_staged_facade_section_failure(
                                                    &data_dir,
                                                    facade,
                                                    SectionId::Providers,
                                                    "provider runtime initialization failed; retaining last-known-good runtime",
                                                    StagedFacadeSectionFailureContext {
                                                        health: &apply_provider_health,
                                                        account_sink: &account_sink,
                                                        section: "providers",
                                                        pending_root_publications: &pending_root_publications,
                                                    },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            } else {
                                retry_legacy_root_publication = true;
                            }
                        }
                    } else {
                        let current_config = config.read().await.clone();
                        let current_revision = apply_provider_health
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .revision;
                        let result = load_and_prepare_provider_candidate(
                            &provider_store,
                            current_revision,
                            current_config,
                        )
                        .await;
                        match result {
                            Ok(candidate) if candidate.unchanged => {}
                            Ok(candidate) => {
                                if candidate.normalized_external_revision {
                                    self_write_marker.mark_self_write(provider_store.path());
                                }
                                let mut live_config = config.write().await;
                                let mut live_provider = provider.write().await;
                                let recovered = section_is_unhealthy(&apply_provider_health);
                                candidate.config.publish_env_vars();
                                *live_config = candidate.config;
                                provider_registry.replace_with(candidate.registry);
                                *live_provider = candidate.provider;
                                drop(live_provider);
                                drop(live_config);

                                publish_section_success(
                                    &apply_provider_health,
                                    &account_sink,
                                    "providers",
                                    data_dir.join("providers.json"),
                                    recovered,
                                    Some(candidate.revision),
                                )
                                .await;
                            }
                            Err(error) => {
                                publish_section_failure(
                                    &apply_provider_health,
                                    &account_sink,
                                    "providers",
                                    candidate_error_status(&error.kind),
                                    error.message,
                                )
                                .await
                            }
                        }
                    }
                }

                if mcp_watched {
                    if let Some(facade) = config_facade.as_ref() {
                        wait_for_section_file_settle(&data_dir, SectionId::Mcp).await;
                        let startup_root_mcp = synthetic_root_events.contains_key(&SectionId::Mcp);
                        let synthetic = synthetic_root_events.remove(&SectionId::Mcp);
                        let reloaded = synthetic
                            .is_none()
                            .then(|| facade.registry().reload_if_changed(SectionId::Mcp))
                            .flatten();
                        let forced_initial_mcp = synthetic.is_none()
                            && reloaded.is_none()
                            && changes.initial_mcp_revision.is_some_and(|revision| {
                                facade.registry().mcp.snapshot().revision == revision
                            });
                        let event = synthetic.or(reloaded).or_else(|| {
                            forced_initial_mcp.then(|| ConfigSectionEvent::Changed {
                                section: "mcp".to_string(),
                                revision: facade.registry().mcp.snapshot().revision,
                            })
                        });
                        if let Some(observed) = event {
                            if let Some(event) = pending_root_publication_event(
                                &data_dir,
                                facade,
                                &account_sink,
                                &pending_root_publications,
                                SectionId::Mcp,
                                observed,
                            ) {
                                if matches!(event, ConfigSectionEvent::Invalid { .. }) {
                                    publish_section_failure(
                                        &apply_mcp_health,
                                        &account_sink,
                                        "mcp",
                                        facade.registry().mcp.snapshot().status,
                                        "MCP section is invalid; retaining last-known-good runtime"
                                            .to_string(),
                                    )
                                    .await;
                                } else {
                                    self_write_marker.mark_self_write(mcp_store.path());
                                    let materialized =
                                        materialize_facade_effective_config(facade, &data_dir);
                                    if materialized.failures.contains(&SectionId::Mcp) {
                                        retry_legacy_root_publication |=
                                        publish_staged_facade_section_failure(
                                            &data_dir,
                                            facade,
                                            SectionId::Mcp,
                                            "MCP credential hydration failed; retaining last-known-good runtime",
                                            StagedFacadeSectionFailureContext {
                                                health: &apply_mcp_health,
                                                account_sink: &account_sink,
                                                section: "mcp",
                                                pending_root_publications: &pending_root_publications,
                                            },
                                        )
                                        .await;
                                    } else {
                                        let next_mcp = materialized.config.mcp.clone();
                                        let publish_config = config.clone();
                                        match mcp_manager
                                            .reconcile_from_config_transactional_after(
                                                &materialized.config.mcp,
                                                || async move {
                                                    publish_config.write().await.mcp = next_mcp;
                                                    Ok(())
                                                },
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                let recovered =
                                                    section_is_unhealthy(&apply_mcp_health);
                                                let snapshot = facade.registry().mcp.snapshot();
                                                let revision = snapshot.revision;
                                                if forced_initial_mcp
                                                    && !startup_root_mcp
                                                    && !pending_root_publications
                                                        .contains_key(&SectionId::Mcp)
                                                    && snapshot.status == SectionStatus::Healthy
                                                {
                                                    // Startup reconciliation makes the
                                                    // already-materialized section live in
                                                    // the MCP manager; it is not a config
                                                    // mutation and must not consume an
                                                    // account-feed sequence number.
                                                    set_live_health_revision(
                                                        &apply_mcp_health,
                                                        revision,
                                                        Some((
                                                            data_dir.join("mcp.json"),
                                                            SectionSourceKind::File,
                                                        )),
                                                    );
                                                } else {
                                                    if pending_root_publications
                                                        .get(&SectionId::Mcp)
                                                        .is_some_and(|event| {
                                                            config_section_event_revision(event)
                                                                == revision
                                                        })
                                                    {
                                                        set_live_health_revision(
                                                            &apply_mcp_health,
                                                            revision,
                                                            Some((
                                                                data_dir.join("mcp.json"),
                                                                SectionSourceKind::File,
                                                            )),
                                                        );
                                                        retry_legacy_root_publication |=
                                                            publish_registry_event_with_root_ack(
                                                                &data_dir,
                                                                &account_sink,
                                                                &mut pending_root_publications,
                                                                SectionId::Mcp,
                                                                &event,
                                                            )
                                                            .await;
                                                    } else if matches!(
                                                        event,
                                                        ConfigSectionEvent::Recovered { .. }
                                                    ) {
                                                        set_live_health_revision(
                                                            &apply_mcp_health,
                                                            revision,
                                                            Some((
                                                                data_dir.join("mcp.json"),
                                                                SectionSourceKind::File,
                                                            )),
                                                        );
                                                        publish_registry_event(
                                                            &account_sink,
                                                            &event,
                                                        )
                                                        .await;
                                                    } else {
                                                        publish_section_success(
                                                            &apply_mcp_health,
                                                            &account_sink,
                                                            "mcp",
                                                            data_dir.join("mcp.json"),
                                                            recovered,
                                                            Some(revision),
                                                        )
                                                        .await;
                                                    }
                                                }
                                            }
                                            Err(_) => {
                                                retry_legacy_root_publication |=
                                                publish_staged_facade_section_failure(
                                                    &data_dir,
                                                    facade,
                                                    SectionId::Mcp,
                                                    "MCP runtime initialization failed; retaining last-known-good runtime",
                                                    StagedFacadeSectionFailureContext {
                                                        health: &apply_mcp_health,
                                                        account_sink: &account_sink,
                                                        section: "mcp",
                                                        pending_root_publications: &pending_root_publications,
                                                    },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            } else {
                                retry_legacy_root_publication = true;
                            }
                        }
                    } else {
                        let current_config = config.read().await.clone();
                        let current_revision = apply_mcp_health
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .revision;
                        let force_initial_mcp =
                            changes.initial_mcp_revision == Some(current_revision);
                        let result = load_and_validate_mcp_candidate(
                            &mcp_store,
                            current_revision,
                            current_config,
                            force_initial_mcp,
                        )
                        .await;
                        match result {
                            Ok(candidate) if candidate.unchanged => {}
                            Ok(candidate) => {
                                if candidate.normalized_external_revision
                                    || candidate.source_kind == SectionSourceKind::Backup
                                {
                                    // Normalization writes the primary and backup
                                    // recovery quarantines it. Suppress only that
                                    // exact watcher echo; a later external write has
                                    // a different fingerprint and remains visible.
                                    self_write_marker.mark_self_write(mcp_store.path());
                                }
                                let next_mcp = candidate.config.mcp.clone();
                                let publish_config = config.clone();
                                match mcp_manager
                                    .reconcile_from_config_transactional_after(
                                        &candidate.config.mcp,
                                        || async move {
                                            publish_config.write().await.mcp = next_mcp;
                                            Ok(())
                                        },
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        let recovered = section_is_unhealthy(&apply_mcp_health);
                                        if candidate.source_kind == SectionSourceKind::Backup {
                                            publish_mcp_backup_lkg(
                                                &apply_mcp_health,
                                                &account_sink,
                                                candidate.source_path,
                                                candidate.revision,
                                            )
                                            .await;
                                        } else {
                                            publish_section_success(
                                                &apply_mcp_health,
                                                &account_sink,
                                                "mcp",
                                                data_dir.join("mcp.json"),
                                                recovered,
                                                Some(candidate.revision),
                                            )
                                            .await;
                                        }
                                    }
                                    Err(_) => publish_section_failure(
                                        &apply_mcp_health,
                                        &account_sink,
                                        "mcp",
                                        SectionStatus::Degraded,
                                        "MCP runtime initialization failed; retaining last-known-good runtime"
                                            .to_string(),
                                    )
                                    .await,
                                }
                            }
                            Err(error) => {
                                publish_section_failure(
                                    &apply_mcp_health,
                                    &account_sink,
                                    "mcp",
                                    candidate_error_status(&error.kind),
                                    error.message,
                                )
                                .await
                            }
                        }
                    }
                }
                if !catchup_paths.is_empty() {
                    let _ = catchup_changes.send(ConfigPathChanges {
                        paths: catchup_paths,
                        initial_mcp_revision: None,
                        startup_legacy_root: None,
                        startup_recoveries: BTreeMap::new(),
                        legacy_root_retry_attempt: 0,
                    });
                }
                if requeue_legacy_root && pending_root_publications.is_empty() {
                    let _ = catchup_changes.send(ConfigPathChanges {
                        paths: vec![data_dir.join("config.json")],
                        initial_mcp_revision: None,
                        startup_legacy_root: None,
                        startup_recoveries: BTreeMap::new(),
                        legacy_root_retry_attempt: 0,
                    });
                }
                if retry_legacy_root_publication {
                    let retry_changes = catchup_changes.clone();
                    let retry_path = data_dir.join("config.json");
                    let retry_attempt = changes.legacy_root_retry_attempt.saturating_add(1);
                    let delay = Duration::from_millis(50_u64 << retry_attempt.min(5));
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = retry_changes.send(ConfigPathChanges {
                            paths: vec![retry_path],
                            initial_mcp_revision: None,
                            startup_legacy_root: None,
                            startup_recoveries: BTreeMap::new(),
                            legacy_root_retry_attempt: retry_attempt,
                        });
                    });
                }
            }
        });

        (
            Self {
                stop,
                watcher_task: Some(watcher_task),
                apply_task: Some(apply_task),
            },
            provider_health,
            mcp_health,
        )
    }
}

fn durable_invalid_recoveries(
    account_sink: &bamboo_engine::events::AccountEventSink,
    facade: &bamboo_config::ConfigFacade,
    rejected: &BTreeSet<SectionId>,
) -> BTreeMap<SectionId, u64> {
    let Ok(durable_facade) = bamboo_config::ConfigFacade::open(facade.data_dir()) else {
        return BTreeMap::new();
    };
    let mut latest = BTreeMap::<SectionId, (bool, u64)>::new();
    if let Ok(events) = bamboo_engine::events::journal::read_since(account_sink.events_dir(), 0) {
        for change in events {
            let state = match change.event {
                AgentEvent::ConfigInvalid { section, revision } => Some((section, true, revision)),
                AgentEvent::ConfigChanged { section, revision }
                | AgentEvent::ConfigRecovered { section, revision } => {
                    Some((section, false, revision))
                }
                _ => None,
            };
            if let Some((section, invalid, revision)) = state {
                if let Some(id) = SectionId::from_name(&section) {
                    latest.insert(id, (invalid, revision));
                }
            }
        }
    }
    latest
        .into_iter()
        .filter_map(|(id, (invalid, invalid_revision))| {
            if !invalid || rejected.contains(&id) {
                return None;
            }
            let envelope = durable_facade.registry().envelope_value(id).ok()?;
            (envelope.revision >= invalid_revision
                && envelope.status == SectionStatus::Healthy
                && envelope.source_kind == SectionSourceKind::File)
                .then_some((id, envelope.revision))
        })
        .collect()
}

fn queue_legacy_root_recovery(
    facade: &bamboo_config::ConfigFacade,
    id: SectionId,
    minimum_revision: Option<u64>,
    watched_sections: &mut BTreeSet<SectionId>,
    synthetic_events: &mut BTreeMap<SectionId, ConfigSectionEvent>,
) {
    watched_sections.insert(id);
    // A same-facade watcher restart may retain process-local Degraded health;
    // reload the healthy typed authority before its runtime is installed.
    let _ = facade.registry().reload(id);
    let Ok(envelope) = facade.registry().envelope_value(id) else {
        return;
    };
    if envelope.status != SectionStatus::Healthy
        || envelope.source_kind != SectionSourceKind::File
        || minimum_revision.is_some_and(|minimum| envelope.revision < minimum)
    {
        return;
    }
    synthetic_events
        .entry(id)
        .or_insert_with(|| ConfigSectionEvent::Recovered {
            section: id.descriptor().name.to_string(),
            revision: envelope.revision,
        });
}

fn pending_root_publication_matches_fresh_durable(
    data_dir: &Path,
    process_facade: &bamboo_config::ConfigFacade,
    id: SectionId,
    revision: u64,
) -> bool {
    let Ok(process) = process_facade.registry().envelope_value(id) else {
        return false;
    };
    if process.revision != revision {
        return false;
    }
    let event = ConfigSectionEvent::Changed {
        section: id.descriptor().name.to_string(),
        revision,
    };
    bamboo_config::legacy_root_publication_matches_snapshot(data_dir, &event, &process.data)
        .unwrap_or(false)
}

async fn wait_for_section_file_settle(data_dir: &Path, id: SectionId) {
    let path = data_dir.join(id.descriptor().file_name);
    for _ in 0..3 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Reload ordinary section authorities, install every successfully hydrated
/// runtime generation, and only then publish its registry event.
///
/// The caller owns `config_io_lock`; keeping this sequence shared by the live
/// watcher and focused regressions makes the event/runtime ordering explicit.
struct OrdinarySectionReloadState<'a> {
    synthetic_events: &'a mut BTreeMap<SectionId, ConfigSectionEvent>,
    pending_root_publications: &'a mut BTreeMap<SectionId, ConfigSectionEvent>,
    reported_root_runtime_failures: &'a mut BTreeSet<(SectionId, u64)>,
}

async fn reload_and_apply_ordinary_sections(
    data_dir: &Path,
    config: &Arc<RwLock<Config>>,
    facade: &bamboo_config::ConfigFacade,
    account_sink: &bamboo_engine::events::AccountEventSink,
    sections: impl IntoIterator<Item = SectionId>,
    state: OrdinarySectionReloadState<'_>,
) -> bool {
    let OrdinarySectionReloadState {
        synthetic_events,
        pending_root_publications,
        reported_root_runtime_failures,
    } = state;
    let mut retry_legacy_root_publication = false;
    let mut publishable = Vec::new();
    for id in sections {
        wait_for_section_file_settle(data_dir, id).await;
        let Some(event) = synthetic_events
            .remove(&id)
            .or_else(|| facade.registry().reload_if_changed(id))
        else {
            continue;
        };
        let Some(event) = pending_root_publication_event(
            data_dir,
            facade,
            account_sink,
            pending_root_publications,
            id,
            event,
        ) else {
            retry_legacy_root_publication = true;
            continue;
        };
        if matches!(event, ConfigSectionEvent::Invalid { .. }) {
            publish_registry_event(account_sink, &event).await;
        } else {
            publishable.push((id, event));
        }
    }
    if publishable.is_empty() {
        return retry_legacy_root_publication;
    }

    let materialized = materialize_facade_effective_config(facade, data_dir);
    let mut current = config.read().await.clone();
    let mut applied = Vec::new();
    for (id, event) in publishable {
        if materialized.failures.contains(&id) {
            let revision = facade
                .registry()
                .envelope_value(id)
                .map(|envelope| envelope.revision)
                .unwrap_or_default();
            let invalid = facade.registry().mark_runtime_degraded(
                id,
                "configuration runtime hydration failed; retaining last-known-good runtime",
            );
            if let Some(invalid) = invalid {
                if pending_root_publications
                    .get(&id)
                    .is_some_and(|event| config_section_event_revision(event) == revision)
                {
                    let _ =
                        confirm_legacy_root_runtime_failure(data_dir, account_sink, &invalid).await;
                } else if reported_root_runtime_failures.insert((id, revision)) {
                    publish_registry_event(account_sink, &invalid).await;
                }
            }
            retry_legacy_root_publication |= pending_root_publications.contains_key(&id);
            continue;
        }
        apply_runtime_section(id, &materialized.config, &mut current);
        applied.push((id, event));
    }
    if applied.is_empty() {
        return retry_legacy_root_publication;
    }

    let publishes_env = applied.iter().any(|(id, _)| *id == SectionId::Env);
    let enforcement_newly_off = !config.read().await.plugin_trust.enforcement_is_off()
        && current.plugin_trust.enforcement_is_off();
    *config.write().await = current.clone();
    if publishes_env {
        current.publish_env_vars();
    }
    if enforcement_newly_off {
        warn_plugin_trust_enforcement_off();
    }
    for (id, event) in applied {
        retry_legacy_root_publication |= publish_registry_event_with_root_ack(
            data_dir,
            account_sink,
            pending_root_publications,
            id,
            &event,
        )
        .await;
        reported_root_runtime_failures.retain(|(failed_id, _)| *failed_id != id);
    }
    retry_legacy_root_publication
}

fn pending_root_publication_event(
    data_dir: &Path,
    facade: &bamboo_config::ConfigFacade,
    account_sink: &bamboo_engine::events::AccountEventSink,
    pending: &BTreeMap<SectionId, ConfigSectionEvent>,
    id: SectionId,
    observed: ConfigSectionEvent,
) -> Option<ConfigSectionEvent> {
    let Some(pending_event) = pending.get(&id) else {
        return Some(observed);
    };
    let revision = config_section_event_revision(pending_event);
    let Ok(envelope) = facade.registry().envelope_value(id) else {
        return None;
    };
    if envelope.revision != revision || envelope.status != SectionStatus::Healthy {
        return None;
    }
    let pending = ConfigSectionEvent::Changed {
        section: id.descriptor().name.to_string(),
        revision,
    };
    if account_sink.latest_config_transition_is_invalid(id.descriptor().name, revision) {
        let invalid = ConfigSectionEvent::Invalid {
            section: id.descriptor().name.to_string(),
            revision,
        };
        match bamboo_config::mark_legacy_root_publication_runtime_degraded(data_dir, &invalid) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                tracing::warn!(
                    %error,
                    section = id.descriptor().name,
                    revision,
                    "failed to repair canonical root runtime-degraded proof from the durable journal"
                );
                return None;
            }
        }
    }
    bamboo_config::legacy_root_publication_success_event(data_dir, &pending, &envelope.data)
        .ok()
        .flatten()
}

async fn publish_registry_event_with_root_ack(
    data_dir: &Path,
    account_sink: &bamboo_engine::events::AccountEventSink,
    pending: &mut BTreeMap<SectionId, ConfigSectionEvent>,
    id: SectionId,
    event: &ConfigSectionEvent,
) -> bool {
    if matches!(event, ConfigSectionEvent::Invalid { .. }) {
        publish_registry_event(account_sink, event).await;
        return false;
    }
    if pending.get(&id).is_none_or(|pending_event| {
        config_section_event_revision(pending_event) != config_section_event_revision(event)
    }) {
        publish_registry_event(account_sink, event).await;
        return false;
    }
    let durable = account_sink
        .record_confirmed(None, &registry_agent_event(event))
        .await;
    if durable
        && bamboo_config::acknowledge_legacy_root_publication(data_dir, event).unwrap_or(false)
    {
        pending.remove(&id);
        false
    } else {
        true
    }
}

pub(super) async fn publish_registry_event(
    account_sink: &bamboo_engine::events::AccountEventSink,
    event: &ConfigSectionEvent,
) {
    let event = registry_agent_event(event);
    if !account_sink.record_confirmed(None, &event).await {
        tracing::warn!("configuration event could not be confirmed in the account journal");
    }
}

async fn confirm_legacy_root_runtime_failure(
    data_dir: &Path,
    account_sink: &bamboo_engine::events::AccountEventSink,
    event: &ConfigSectionEvent,
) -> bool {
    if !account_sink
        .record_confirmed(None, &registry_agent_event(event))
        .await
    {
        return false;
    }
    match bamboo_config::mark_legacy_root_publication_runtime_degraded(data_dir, event) {
        Ok(marked) => marked,
        Err(error) => {
            tracing::warn!(
                %error,
                "failed to mark canonical root publication runtime-degraded"
            );
            false
        }
    }
}

fn legacy_root_rejection_event(
    facade: &bamboo_config::ConfigFacade,
    id: SectionId,
    diagnostic: &str,
    force_publication: bool,
) -> Option<ConfigSectionEvent> {
    let already_reported = facade.registry().envelope_value(id).is_ok_and(|envelope| {
        envelope.status == SectionStatus::Degraded
            && envelope.last_error.as_deref() == Some(diagnostic)
    });
    if already_reported && !force_publication {
        return None;
    }
    facade
        .registry()
        .mark_runtime_degraded(id, diagnostic)
        .or_else(|| {
            // Defensive fallback for section implementations that cannot
            // expose process-local degraded health.
            force_publication
                .then(|| {
                    facade.registry().envelope_value(id).ok().map(|envelope| {
                        ConfigSectionEvent::Invalid {
                            section: id.descriptor().name.to_string(),
                            revision: envelope.revision,
                        }
                    })
                })
                .flatten()
        })
}

fn config_section_event_revision(event: &ConfigSectionEvent) -> u64 {
    match event {
        ConfigSectionEvent::Changed { revision, .. }
        | ConfigSectionEvent::Invalid { revision, .. }
        | ConfigSectionEvent::Recovered { revision, .. } => *revision,
    }
}

fn registry_agent_event(event: &ConfigSectionEvent) -> AgentEvent {
    match event {
        ConfigSectionEvent::Changed { section, revision } => AgentEvent::ConfigChanged {
            section: section.clone(),
            revision: *revision,
        },
        ConfigSectionEvent::Invalid { section, revision } => AgentEvent::ConfigInvalid {
            section: section.clone(),
            revision: *revision,
        },
        ConfigSectionEvent::Recovered { section, revision } => AgentEvent::ConfigRecovered {
            section: section.clone(),
            revision: *revision,
        },
    }
}

async fn publish_exact_facade_events(
    account_sink: &bamboo_engine::events::AccountEventSink,
    events: &[ConfigSectionEvent],
) -> Result<(), AppError> {
    for event in events {
        let durable = account_sink
            .record_confirmed(None, &registry_agent_event(event))
            .await;
        if !durable {
            return Err(AppError::InternalError(anyhow::anyhow!(
                "committed configuration event could not be confirmed in the account journal"
            )));
        }
        if matches!(event, ConfigSectionEvent::Invalid { .. }) {
            return Err(AppError::InternalError(anyhow::anyhow!(
                "committed configuration section became invalid before publication"
            )));
        }
    }
    Ok(())
}

struct InstalledCredentialSectionCommit {
    events: Vec<ConfigSectionEvent>,
    metadata: bamboo_config::CredentialSectionRuntimeMetadata,
    section: Option<bamboo_config::SectionEnvelope<Value>>,
}

pub(crate) struct ExactCredentialSectionSnapshot {
    pub config: Config,
    pub section: bamboo_config::SectionEnvelope<Value>,
    pub metadata: bamboo_config::CredentialSectionRuntimeMetadata,
}

fn map_exact_credential_store_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        ConfigStoreError::Validation(message) => AppError::BadRequest(message),
        ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(anyhow::anyhow!(
            "configuration commit outcome is indeterminate: {message}"
        )),
        ConfigStoreError::Io(error) => AppError::StorageError(error),
        ConfigStoreError::Json(_) => {
            AppError::BadRequest("configuration document is invalid".to_string())
        }
        ConfigStoreError::Watch(error) => {
            AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
        }
    }
}

async fn install_exact_credential_section_mutation_base(
    data_dir: PathBuf,
    section: SectionId,
    expected_revision: u64,
    target: &mut Config,
) -> Result<bamboo_config::CredentialSectionRuntimeMetadata, AppError> {
    let exact = tokio::task::spawn_blocking(move || {
        bamboo_config::read_exact_credential_section_snapshot(
            data_dir,
            section,
            Some(expected_revision),
        )
    })
    .await
    .map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "{} exact mutation snapshot task failed: {error}",
            section.descriptor().name
        ))
    })?
    .map_err(map_exact_credential_store_error)?;
    Ok(exact.install_into(target))
}

fn read_credential_runtime_metadata(
    data_dir: &std::path::Path,
) -> Result<bamboo_config::CredentialSectionRuntimeMetadata, ConfigStoreError> {
    let (credential_statuses, credential_health) =
        bamboo_config::CredentialStore::open(data_dir).statuses_with_health()?;
    Ok(bamboo_config::CredentialSectionRuntimeMetadata {
        credential_statuses,
        credential_health,
    })
}

fn install_credential_section_commit(
    commit: bamboo_config::CredentialSectionTransactionCommit,
    target: &mut Config,
) -> Result<InstalledCredentialSectionCommit, ConfigStoreError> {
    let bamboo_config::CredentialSectionTransactionCommit {
        revision: _,
        section_adoption,
        credential_adoption,
        section,
        runtime,
    } = commit;
    let section = section?;
    let metadata = runtime?.install_into(target);
    let mut events = Vec::new();
    if let Some(adoption) = credential_adoption {
        if let Some(event) = adoption? {
            events.push(event);
        }
    }
    if let Some(adoption) = section_adoption {
        events.push(adoption?);
    }
    Ok(InstalledCredentialSectionCommit {
        events,
        metadata,
        section: Some(section),
    })
}

fn install_facade_config_commit(
    commit: bamboo_config::FacadeConfigCommit,
    target: &mut Config,
) -> Result<Vec<ConfigSectionEvent>, ConfigStoreError> {
    let bamboo_config::FacadeConfigCommit {
        section_adoption,
        runtime,
    } = commit;
    if let Some(runtime) = runtime? {
        runtime.install_into(target);
    }
    section_adoption
        .map(|adoption| adoption.map(|event| vec![event]))
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn apply_runtime_section(id: SectionId, source: &Config, target: &mut Config) {
    match id {
        SectionId::Core => {
            target.http_proxy = source.http_proxy.clone();
            target.https_proxy = source.https_proxy.clone();
            target.proxy_auth = source.proxy_auth.clone();
            target.proxy_auth_encrypted = None;
            target.proxy_auth_credential_ref = source.proxy_auth_credential_ref.clone();
            target.headless_auth = source.headless_auth;
            target.server = source.server.clone();
            target.default_work_area = source.default_work_area.clone();
            target.run_budget = source.run_budget;
            target.stream_timeout = source.stream_timeout;
            target.extra = source.extra.clone();
        }
        SectionId::Providers => {
            target.provider = source.provider.clone();
            target.defaults = source.defaults.clone();
            target.provider_instances = source.provider_instances.clone();
            target.default_provider_instance = source.default_provider_instance.clone();
            target.features = source.features.clone();
            *target.providers_mut() = source.providers().clone();
        }
        SectionId::Mcp => target.mcp = source.mcp.clone(),
        SectionId::ToolsSkills => {
            target.tools = source.tools.clone();
            target.skills = source.skills.clone();
            target.plugin_trust = source.plugin_trust.clone();
        }
        SectionId::Memory => *target.memory_mut() = source.memory().clone(),
        SectionId::Subagents => {
            let runtime_broker = target.subagents().broker.clone();
            *target.subagents_mut() = source.subagents().clone();
            if target.subagents().broker.is_none() {
                target.subagents_mut().broker = runtime_broker;
            }
        }
        SectionId::Notifications => target.notifications = source.notifications.clone(),
        SectionId::Connect => target.connect = source.connect.clone(),
        SectionId::ClusterFabric => target.cluster_fabric = source.cluster_fabric.clone(),
        SectionId::Env => target.env_vars = source.env_vars.clone(),
        SectionId::AccessControl => target.access_control = source.access_control.clone(),
        SectionId::Hooks => target.hooks = source.hooks.clone(),
        SectionId::ModelPolicy => {
            target.keyword_masking = source.keyword_masking.clone();
            target.anthropic_model_mapping = source.anthropic_model_mapping.clone();
            target.gemini_model_mapping = source.gemini_model_mapping.clone();
        }
        SectionId::ModelLimits | SectionId::Credentials => {}
    }
}

/// Compatibility config writers never own the cluster-fabric section. Rebase
/// their detached candidate on the process facade's last-known-good cluster
/// projection so runtime-only heartbeat updates cannot become an unrevisioned
/// durable cluster mutation.
fn restore_authoritative_cluster_fabric(
    facade: Option<&std::sync::Arc<bamboo_config::ConfigFacade>>,
    candidate: &mut Config,
) {
    if let Some(facade) = facade {
        candidate.cluster_fabric = facade.registry().cluster_fabric.snapshot().data.0.clone();
    }
}

/// Preserve the process-only broker injected at boot when a config projection
/// cannot carry it. An incoming runtime broker is authoritative, so external
/// broker changes still replace the previous endpoint.
fn preserve_runtime_broker(new_config: &mut Config, previous: &Config) {
    if new_config.subagents().broker.is_none() {
        new_config.subagents_mut().broker = previous.subagents().broker.clone();
    }
}

fn section_is_unhealthy(health: &std::sync::RwLock<ConfigLiveHealth>) -> bool {
    health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .status
        != SectionStatus::Healthy
}

async fn publish_section_success(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    account_sink: &bamboo_engine::events::AccountEventSink,
    section: &str,
    source_path: PathBuf,
    recovered: bool,
    revision: Option<u64>,
) {
    let revision = match revision {
        Some(revision) => set_live_health_revision(
            health,
            revision,
            Some((source_path, SectionSourceKind::File)),
        ),
        None => update_live_health(
            health,
            SectionStatus::Healthy,
            None,
            true,
            Some((source_path, SectionSourceKind::File)),
        ),
    };
    let event = if recovered {
        AgentEvent::ConfigRecovered {
            section: section.to_string(),
            revision,
        }
    } else {
        AgentEvent::ConfigChanged {
            section: section.to_string(),
            revision,
        }
    };
    if !account_sink.record_confirmed(None, &event).await {
        tracing::warn!(
            section,
            revision,
            "configuration success event was not durable"
        );
    }
}

async fn publish_section_failure(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    account_sink: &bamboo_engine::events::AccountEventSink,
    section: &str,
    status: SectionStatus,
    message: String,
) {
    let duplicate = {
        let health = health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        health.status == status && health.last_error.as_deref() == Some(message.as_str())
    };
    let revision = update_live_health(health, status, Some(message), false, None);
    if duplicate {
        return;
    }
    let event = AgentEvent::ConfigInvalid {
        section: section.to_string(),
        revision,
    };
    if !account_sink.record_confirmed(None, &event).await {
        tracing::warn!(
            section,
            revision,
            "configuration failure event was not durable"
        );
    }
}

struct StagedFacadeSectionFailureContext<'a> {
    health: &'a std::sync::RwLock<ConfigLiveHealth>,
    account_sink: &'a bamboo_engine::events::AccountEventSink,
    section: &'a str,
    pending_root_publications: &'a BTreeMap<SectionId, ConfigSectionEvent>,
}

async fn publish_staged_facade_section_failure(
    data_dir: &Path,
    facade: &bamboo_config::ConfigFacade,
    id: SectionId,
    message: &str,
    context: StagedFacadeSectionFailureContext<'_>,
) -> bool {
    let StagedFacadeSectionFailureContext {
        health,
        account_sink,
        section,
        pending_root_publications,
    } = context;
    let event = facade
        .registry()
        .mark_runtime_degraded(id, message)
        .expect("every facade section exposes runtime health");
    let exact_pending = matches!(
        &event,
        ConfigSectionEvent::Invalid { revision, .. }
            if pending_root_publications
                .get(&id)
                .is_some_and(|event| config_section_event_revision(event) == *revision)
    );
    if exact_pending {
        // Keep ConfigLiveHealth's revision as the last installed runtime, but
        // publish the exact typed candidate revision that failed its runtime
        // hook. AccountSink's transition state dedupes delayed retries.
        update_live_health(
            health,
            SectionStatus::Degraded,
            Some(message.to_string()),
            false,
            None,
        );
        if !confirm_legacy_root_runtime_failure(data_dir, account_sink, &event).await {
            tracing::warn!(
                section,
                "root runtime failure was not confirmed against its canonical publication"
            );
        }
    } else {
        publish_section_failure(
            health,
            account_sink,
            section,
            SectionStatus::Degraded,
            message.to_string(),
        )
        .await;
    }
    exact_pending
}

async fn publish_mcp_backup_lkg(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    account_sink: &bamboo_engine::events::AccountEventSink,
    source_path: PathBuf,
    revision: u64,
) {
    {
        let mut health = health
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        health.revision = revision;
        health.loaded_at = Utc::now();
        health.source_path = source_path;
        health.source_kind = SectionSourceKind::Backup;
        health.status = SectionStatus::Degraded;
        health.last_error =
            Some("primary MCP section invalid; running last-known-good backup runtime".to_string());
    }
    let event = AgentEvent::ConfigInvalid {
        section: "mcp".to_string(),
        revision,
    };
    if !account_sink.record_confirmed(None, &event).await {
        tracing::warn!(revision, "MCP backup health event was not durable");
    }
}

fn initial_provider_health(store: &AtomicJsonStore<ProviderConfigs>) -> ConfigLiveHealth {
    if ensure_provider_mcp_migration_ready(store.path().parent().unwrap_or_else(|| Path::new(".")))
        .is_err()
    {
        return ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Degraded,
            last_error: Some("provider/MCP credential migration is pending".to_string()),
        };
    }
    match store.load_validated_allowing_unversioned(|_| Ok(())) {
        Ok(Some(stored)) => ConfigLiveHealth {
            revision: stored.revision,
            loaded_at: Utc::now(),
            source_path: stored.source_path,
            source_kind: if stored.recovered_from_backup {
                SectionSourceKind::Backup
            } else {
                SectionSourceKind::File
            },
            status: if stored.recovered_from_backup {
                SectionStatus::Degraded
            } else {
                SectionStatus::Healthy
            },
            last_error: stored.recovered_from_backup.then(|| {
                "primary provider section invalid; using last-known-good backup".to_string()
            }),
        },
        Ok(None) => ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::Default,
            status: SectionStatus::Missing,
            last_error: None,
        },
        Err(_) => ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Invalid,
            last_error: Some("provider section could not be parsed or read".to_string()),
        },
    }
}

fn initial_mcp_health(store: &AtomicJsonStore<McpConfig>) -> ConfigLiveHealth {
    if ensure_provider_mcp_migration_ready(store.path().parent().unwrap_or_else(|| Path::new(".")))
        .is_err()
    {
        return ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Degraded,
            last_error: Some("provider/MCP credential migration is pending".to_string()),
        };
    }
    match store.load_validated(validate_mcp_config) {
        Ok(Some(stored)) => ConfigLiveHealth {
            // The document is only a parsed candidate until runtime staging
            // succeeds; the published LKG revision remains zero meanwhile.
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: if stored.recovered_from_backup {
                SectionSourceKind::Backup
            } else {
                SectionSourceKind::File
            },
            status: SectionStatus::Degraded,
            last_error: Some(if stored.recovered_from_backup {
                "primary MCP section invalid; runtime initialization pending from backup"
                    .to_string()
            } else {
                "MCP runtime initialization pending".to_string()
            }),
        },
        Ok(None) => ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::Default,
            status: SectionStatus::Missing,
            last_error: None,
        },
        Err(_) => ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Invalid,
            last_error: Some("MCP section could not be parsed or validated".to_string()),
        },
    }
}

impl Drop for ConfigWatcherRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.apply_task.take() {
            task.abort();
        }
        if let Some(task) = self.watcher_task.take() {
            let _ = task.join();
        }
    }
}

async fn load_and_prepare_provider_candidate(
    store: &AtomicJsonStore<ProviderConfigs>,
    current_revision: u64,
    candidate_config: Config,
) -> Result<ProviderCandidate, ProviderCandidateError> {
    ensure_provider_mcp_migration_ready(store.path().parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|_| {
            ProviderCandidateError::invalid(
                "provider/MCP credential migration is pending; retaining last-known-good runtime",
            )
        })?;
    // Editors commonly implement save as delete/rename/create. Retry a missing
    // watched file briefly instead of treating the transient gap as a reset.
    for _ in 0..3 {
        if store.path().exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !store.path().exists() {
        return Err(ProviderCandidateError::missing());
    }
    let stored = store
        .load_validated_for_reload_allowing_unversioned(
            current_revision,
            candidate_config.providers(),
            validate_provider_config,
        )
        .map_err(|_| {
            if store.path().exists() {
                ProviderCandidateError::invalid("provider section is invalid")
            } else {
                ProviderCandidateError::missing()
            }
        })?
        .ok_or_else(ProviderCandidateError::missing)?;
    if stored.recovered_from_backup {
        return Err(ProviderCandidateError::invalid(
            "primary provider section is invalid; retaining last-known-good runtime",
        ));
    }
    let unchanged = stored.revision == current_revision
        && serde_json::to_value(&stored.data).ok()
            == serde_json::to_value(candidate_config.providers()).ok();
    let mut candidate_config = candidate_config;
    *candidate_config.providers_mut() = stored.data;
    let (candidate_config, registry, provider) = prepare_provider_candidate(
        candidate_config,
        store
            .path()
            .parent()
            .unwrap_or_else(|| std::path::Path::new(".")),
    )
    .await?;
    Ok(ProviderCandidate {
        config: candidate_config,
        registry,
        provider,
        revision: stored.revision,
        normalized_external_revision: stored.normalized_external_revision,
        unchanged,
    })
}

async fn prepare_provider_candidate(
    mut candidate_config: Config,
    data_dir: &std::path::Path,
) -> Result<(Config, bamboo_llm::ProviderRegistry, Arc<dyn LLMProvider>), ProviderCandidateError> {
    candidate_config.hydrate_provider_api_keys_from_encrypted();
    candidate_config
        .hydrate_provider_credentials_from_store(data_dir)
        .map_err(|_| ProviderCandidateError::invalid("provider credential is unavailable"))?;
    let candidate_registry =
        bamboo_llm::ProviderRegistry::from_config(&candidate_config, data_dir.to_path_buf())
            .await
            .map_err(|_| ProviderCandidateError::runtime())?;
    let candidate_provider = candidate_registry
        .get_default()
        .ok_or_else(ProviderCandidateError::runtime)?;
    Ok((candidate_config, candidate_registry, candidate_provider))
}

struct ProviderCandidate {
    config: Config,
    registry: bamboo_llm::ProviderRegistry,
    provider: Arc<dyn LLMProvider>,
    revision: u64,
    normalized_external_revision: bool,
    unchanged: bool,
}

async fn load_and_validate_mcp_candidate(
    store: &AtomicJsonStore<McpConfig>,
    current_revision: u64,
    mut candidate_config: Config,
    allow_startup_backup: bool,
) -> Result<McpCandidate, ProviderCandidateError> {
    ensure_provider_mcp_migration_ready(store.path().parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|_| {
            ProviderCandidateError::invalid(
                "provider/MCP credential migration is pending; retaining last-known-good runtime",
            )
        })?;
    for _ in 0..3 {
        if store.path().exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !store.path().exists() {
        return Err(ProviderCandidateError::missing_section(
            "MCP section is missing",
        ));
    }
    let current_document = mcp_durable_comparison_document(&candidate_config.mcp);
    let stored = store
        .load_validated_for_reload(current_revision, &current_document, validate_mcp_config)
        .map_err(|_| ProviderCandidateError::invalid("MCP section is invalid"))?
        .ok_or_else(|| ProviderCandidateError::missing_section("MCP section is missing"))?;
    if stored.recovered_from_backup && !allow_startup_backup {
        return Err(ProviderCandidateError::invalid(
            "primary MCP section is invalid; retaining last-known-good runtime",
        ));
    }
    let unchanged = !allow_startup_backup
        && stored.revision == current_revision
        && serde_json::to_value(&stored.data).ok() == serde_json::to_value(&current_document).ok();
    candidate_config.mcp = stored.data;
    candidate_config.hydrate_mcp_secrets_from_encrypted();
    candidate_config
        .hydrate_mcp_credentials_from_store(
            store
                .path()
                .parent()
                .unwrap_or_else(|| std::path::Path::new(".")),
        )
        .map_err(|_| ProviderCandidateError::invalid("MCP credential is unavailable"))?;
    Ok(McpCandidate {
        config: candidate_config,
        revision: stored.revision,
        source_kind: if stored.recovered_from_backup {
            SectionSourceKind::Backup
        } else {
            SectionSourceKind::File
        },
        source_path: stored.source_path,
        normalized_external_revision: stored.normalized_external_revision,
        unchanged,
    })
}

struct McpCandidate {
    config: Config,
    revision: u64,
    source_kind: SectionSourceKind,
    source_path: PathBuf,
    normalized_external_revision: bool,
    unchanged: bool,
}

/// Project a hydrated runtime section back to its durable comparison shape.
/// Public compatibility serialization intentionally retains plaintext beside
/// ciphertext, while the sidecar stores only ciphertext for paired secrets.
fn mcp_durable_comparison_document(config: &McpConfig) -> McpConfig {
    let mut document = config.clone();
    for server in &mut document.servers {
        match &mut server.transport {
            TransportConfig::Stdio(config) => {
                config.env.retain(|name, _| {
                    !config.env_encrypted.contains_key(name)
                        && !config.env_credential_refs.contains_key(name)
                });
            }
            TransportConfig::Sse(config) => clear_paired_header_plaintext(&mut config.headers),
            TransportConfig::StreamableHttp(config) => {
                clear_paired_header_plaintext(&mut config.headers)
            }
        }
    }
    document
}

fn clear_paired_header_plaintext(headers: &mut [bamboo_mcp::HeaderConfig]) {
    for header in headers {
        if header.value_encrypted.is_some() || header.credential_ref.is_some() {
            header.value.clear();
        }
    }
}

fn validate_mcp_config(config: &McpConfig) -> Result<(), String> {
    let mut ids = std::collections::HashSet::new();
    for server in &config.servers {
        if server.id.trim().is_empty() {
            return Err("MCP server id cannot be empty".to_string());
        }
        if !ids.insert(server.id.as_str()) {
            return Err(format!("duplicate MCP server id '{}'", server.id));
        }
        if server.request_timeout_ms == 0 || server.healthcheck_interval_ms == 0 {
            return Err(format!(
                "MCP server '{}' timeouts must be non-zero",
                server.id
            ));
        }
        match &server.transport {
            TransportConfig::Stdio(stdio) if stdio.command.trim().is_empty() => {
                return Err(format!(
                    "MCP stdio server '{}' command cannot be empty",
                    server.id
                ));
            }
            TransportConfig::Sse(sse) if sse.url.trim().is_empty() => {
                return Err(format!(
                    "MCP SSE server '{}' URL cannot be empty",
                    server.id
                ));
            }
            TransportConfig::StreamableHttp(http) if http.url.trim().is_empty() => {
                return Err(format!(
                    "MCP HTTP server '{}' URL cannot be empty",
                    server.id
                ));
            }
            _ => {}
        }
        match &server.transport {
            TransportConfig::Stdio(stdio) => {
                if !stdio.env_encrypted.is_empty()
                    || stdio.env.iter().any(|(name, value)| {
                        !value.is_empty() && !stdio.env_credential_refs.contains_key(name)
                    })
                {
                    return Err(format!(
                        "MCP server '{}' contains a secret outside the credential store",
                        server.id
                    ));
                }
                for raw in stdio.env_credential_refs.values() {
                    bamboo_config::CredentialRef::parse(raw.clone())
                        .map_err(|_| "MCP credential reference is invalid".to_string())?;
                }
            }
            TransportConfig::Sse(config) => validate_header_refs(&server.id, &config.headers)?,
            TransportConfig::StreamableHttp(config) => {
                validate_header_refs(&server.id, &config.headers)?
            }
        }
    }
    Ok(())
}

fn validate_provider_config(providers: &ProviderConfigs) -> Result<(), String> {
    macro_rules! validate {
        ($field:ident) => {
            if let Some(provider) = &providers.$field {
                if provider.api_key_encrypted.is_some()
                    || (!provider.api_key.trim().is_empty()
                        && !provider.api_key_from_env
                        && provider.credential_ref.is_none())
                {
                    return Err("provider secret is outside the credential store".to_string());
                }
            }
        };
    }
    validate!(openai);
    validate!(anthropic);
    validate!(gemini);
    if let Some(provider) = &providers.bodhi {
        if provider.api_key_encrypted.is_some()
            || (!provider.api_key.trim().is_empty() && provider.credential_ref.is_none())
        {
            return Err("provider secret is outside the credential store".to_string());
        }
    }
    Ok(())
}

fn validate_header_refs(
    server_id: &str,
    headers: &[bamboo_mcp::HeaderConfig],
) -> Result<(), String> {
    for header in headers {
        if header.value_encrypted.is_some()
            || (!header.value.is_empty() && header.credential_ref.is_none())
        {
            return Err(format!(
                "MCP server '{server_id}' contains a secret outside the credential store"
            ));
        }
        if let Some(raw) = &header.credential_ref {
            bamboo_config::CredentialRef::parse(raw.clone())
                .map_err(|_| "MCP credential reference is invalid".to_string())?;
        }
    }
    Ok(())
}

enum ProviderCandidateErrorKind {
    Missing,
    InvalidDocument,
    Runtime,
}

struct ProviderCandidateError {
    kind: ProviderCandidateErrorKind,
    message: String,
}

impl ProviderCandidateError {
    fn missing() -> Self {
        Self {
            kind: ProviderCandidateErrorKind::Missing,
            message: "provider section is missing".to_string(),
        }
    }

    fn invalid(message: &str) -> Self {
        Self {
            kind: ProviderCandidateErrorKind::InvalidDocument,
            message: message.to_string(),
        }
    }

    fn missing_section(message: &str) -> Self {
        Self {
            kind: ProviderCandidateErrorKind::Missing,
            message: message.to_string(),
        }
    }

    fn runtime() -> Self {
        Self {
            kind: ProviderCandidateErrorKind::Runtime,
            message: "provider runtime initialization failed".to_string(),
        }
    }
}

fn candidate_error_status(kind: &ProviderCandidateErrorKind) -> SectionStatus {
    match kind {
        ProviderCandidateErrorKind::Missing => SectionStatus::Missing,
        ProviderCandidateErrorKind::InvalidDocument => SectionStatus::Invalid,
        ProviderCandidateErrorKind::Runtime => SectionStatus::Degraded,
    }
}

fn update_live_health(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    status: SectionStatus,
    last_error: Option<String>,
    advance_revision: bool,
    source: Option<(PathBuf, SectionSourceKind)>,
) -> u64 {
    let mut health = health
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if advance_revision {
        health.revision = health.revision.saturating_add(1);
    }
    health.loaded_at = Utc::now();
    health.status = status;
    health.last_error = last_error;
    if let Some((source_path, source_kind)) = source {
        health.source_path = source_path;
        health.source_kind = source_kind;
    }
    health.revision
}

fn set_live_health_revision(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    revision: u64,
    source: Option<(PathBuf, SectionSourceKind)>,
) -> u64 {
    let mut health = health
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    health.revision = revision;
    health.loaded_at = Utc::now();
    health.status = SectionStatus::Healthy;
    health.last_error = None;
    if let Some((source_path, source_kind)) = source {
        health.source_path = source_path;
        health.source_kind = source_kind;
    }
    revision
}

fn set_live_health_from_snapshot<T>(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    snapshot: &bamboo_config::SectionSnapshot<T>,
) {
    let mut health = health
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    health.revision = snapshot.revision;
    health.loaded_at = snapshot.loaded_at;
    health.source_path = snapshot.source_path.clone();
    health.source_kind = snapshot.source_kind;
    health.status = snapshot.status;
    health.last_error = snapshot.last_error.clone();
}

#[derive(Debug)]
pub(crate) enum ConfigSectionMutationError {
    Store(ConfigStoreError),
    Invalid(String),
    Runtime(String),
}

pub(crate) enum CredentialBackedResetCommit {
    Section(bamboo_config::SectionEnvelope<Value>),
    Cluster(Box<bamboo_server_tools::FabricCommitSnapshot>),
}

impl AppState {
    #[cfg(test)]
    pub(crate) fn stop_config_watcher_for_test(&mut self) {
        self.config_watcher.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.config_watcher.apply_task.take() {
            task.abort();
        }
        if let Some(task) = self.config_watcher.watcher_task.take() {
            let _ = task.join();
        }
    }

    #[cfg(test)]
    pub(crate) async fn reload_ordinary_section_for_test(&self, id: SectionId) {
        let _io = self.config_io_lock.lock().await;
        let facade = self
            .config_facade
            .as_ref()
            .expect("test ordinary reload requires the modular facade");
        let mut synthetic_events = BTreeMap::new();
        let mut pending_root_publications = BTreeMap::new();
        let mut reported_root_runtime_failures = BTreeSet::new();
        reload_and_apply_ordinary_sections(
            &self.app_data_dir,
            &self.config,
            facade,
            &self.account_sink,
            std::iter::once(id),
            OrdinarySectionReloadState {
                synthetic_events: &mut synthetic_events,
                pending_root_publications: &mut pending_root_publications,
                reported_root_runtime_failures: &mut reported_root_runtime_failures,
            },
        )
        .await;
    }

    /// Read one exact durable credential-backed section and its secret-free
    /// credential status generation. The process facade may lag another
    /// process, so GET handlers must not assemble these authorities
    /// separately.
    pub(crate) async fn read_exact_credential_section(
        &self,
        section: SectionId,
    ) -> Result<ExactCredentialSectionSnapshot, AppError> {
        let _io = self.config_io_lock.lock().await;
        let data_dir = self.app_data_dir.clone();
        let exact = tokio::task::spawn_blocking(move || {
            bamboo_config::read_exact_credential_section_snapshot(data_dir, section, None)
        })
        .await
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "{} exact read snapshot task failed: {error}",
                section.descriptor().name
            ))
        })?
        .map_err(map_exact_credential_store_error)?;
        let envelope = exact.section.clone();
        let mut config = Config::default();
        let metadata = exact.install_into(&mut config);
        Ok(ExactCredentialSectionSnapshot {
            config,
            section: envelope,
            metadata,
        })
    }

    /// Commit a single ordinary typed section with CAS, then publish exactly
    /// that section into the process-owned effective snapshot. Credential
    /// bindings are server-owned and cannot be forged or detached here.
    pub(crate) async fn put_ordinary_section(
        &self,
        id: SectionId,
        expected_revision: u64,
        candidate: Value,
    ) -> Result<bamboo_config::SectionEnvelope<Value>, ConfigSectionMutationError> {
        if matches!(
            id,
            SectionId::Providers
                | SectionId::Mcp
                | SectionId::Credentials
                | SectionId::ClusterFabric
        ) {
            return Err(ConfigSectionMutationError::Invalid(
                "this section requires its dedicated endpoint".to_string(),
            ));
        }
        let _io = self.config_io_lock.lock().await;
        ensure_provider_mcp_migration_ready(&self.app_data_dir)
            .map_err(ConfigSectionMutationError::Store)?;
        let facade = self.config_facade.as_ref().ok_or_else(|| {
            ConfigSectionMutationError::Invalid(
                "typed section writes require the modular configuration facade".to_string(),
            )
        })?;
        let current = if id == SectionId::Core {
            let data_dir = self.app_data_dir.clone();
            let exact = tokio::task::spawn_blocking(move || {
                bamboo_config::read_exact_credential_section_snapshot(
                    data_dir,
                    SectionId::Core,
                    Some(expected_revision),
                )
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "Core exact inventory snapshot task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;
            if let Some(reference) = exact
                .section
                .data
                .get("proxy_auth_credential_ref")
                .and_then(Value::as_str)
            {
                let configured = exact
                    .credential_statuses
                    .iter()
                    .any(|status| status.credential_ref.as_str() == reference && status.configured);
                if !configured {
                    return Err(ConfigSectionMutationError::Invalid(
                        "the active Core proxy credential is invalid; explicitly replace or clear it through the proxy-auth API"
                            .to_string(),
                    ));
                }
            }
            exact.section
        } else {
            facade
                .registry()
                .envelope_value(id)
                .map_err(ConfigSectionMutationError::Store)?
        };
        if credential_reference_inventory(&current.data)
            != credential_reference_inventory(&candidate)
        {
            return Err(ConfigSectionMutationError::Invalid(
                "credential references are server-managed; use the credential or domain API"
                    .to_string(),
            ));
        }

        let (event, committed) = if id == SectionId::Core {
            // Exact Core GETs may be ahead of this process's watcher. Keep the
            // shared credential transaction lock from durable-base validation
            // through the content-aware Core CAS and jump directly to the
            // committed revision; never consume or publish the intermediate
            // generation before its runtime has been installed.
            bamboo_config::commit_core_metadata_from_durable_base(
                &self.app_data_dir,
                facade,
                expected_revision,
                candidate,
            )
        } else {
            facade
                .registry()
                .commit_value_with_envelope(id, expected_revision, candidate)
        }
        .map_err(ConfigSectionMutationError::Store)?;
        let materialized = materialize_facade_effective_config(facade, &self.app_data_dir);
        if materialized.failures.contains(&id) {
            let message =
                "configuration runtime hydration failed; retaining last-known-good runtime"
                    .to_string();
            if let Some(invalid) = facade.registry().mark_runtime_degraded(id, message.clone()) {
                publish_registry_event(&self.account_sink, &invalid).await;
            }
            return Err(ConfigSectionMutationError::Runtime(message));
        }

        let mut live = self.config.read().await.clone();
        let enforcement_newly_off = id == SectionId::ToolsSkills
            && !live.plugin_trust.enforcement_is_off()
            && materialized.config.plugin_trust.enforcement_is_off();
        apply_runtime_section(id, &materialized.config, &mut live);
        if id == SectionId::Env {
            live.publish_env_vars();
        }
        *self.config.write().await = live;
        if enforcement_newly_off {
            warn_plugin_trust_enforcement_off();
        }
        publish_registry_event(&self.account_sink, &event).await;
        Ok(committed)
    }

    /// Validate and stage a provider runtime before the first durable CAS
    /// write, then publish config/runtime/health/event under `config_io_lock`.
    pub(crate) async fn put_provider_section(
        &self,
        expected_revision: u64,
        mut providers: ProviderConfigs,
    ) -> Result<u64, ConfigSectionMutationError> {
        let _io = self.config_io_lock.lock().await;
        ensure_provider_mcp_migration_ready(&self.app_data_dir)
            .map_err(ConfigSectionMutationError::Store)?;
        let current = self.config.read().await.clone();
        retain_provider_credentials(current.providers(), &mut providers);
        let mut candidate = current;
        *candidate.providers_mut() = providers.clone();
        let (candidate, registry, provider) =
            match prepare_provider_candidate(candidate, &self.app_data_dir).await {
                Ok(prepared) => prepared,
                Err(_) => {
                    let message =
                        "provider runtime initialization failed; retaining last-known-good runtime"
                            .to_string();
                    publish_section_failure(
                        &self.config_live_health,
                        &self.account_sink,
                        "providers",
                        SectionStatus::Degraded,
                        message.clone(),
                    )
                    .await;
                    return Err(ConfigSectionMutationError::Runtime(message));
                }
            };

        let durable_providers = provider_durable_document(&providers)?;
        // Acquire every async publication guard before crossing the durable
        // boundary. Once commit succeeds, cancellation cannot strand the file
        // ahead of the live config/provider snapshots.
        let mut live_config = self.config.write().await;
        let mut live_provider = self.provider.write().await;
        let (revision, source_path) = if let Some(facade) = self.config_facade.as_ref() {
            let mut section = facade.registry().providers.snapshot().data.as_ref().clone();
            section.providers = durable_providers;
            let event = facade
                .registry()
                .providers
                .commit(expected_revision, section)
                .map_err(ConfigSectionMutationError::Store)?;
            let ConfigSectionEvent::Changed { revision, .. } = event else {
                unreachable!("a successful section commit is changed")
            };
            (
                revision,
                facade.registry().providers.snapshot().source_path.clone(),
            )
        } else {
            let store = AtomicJsonStore::new(self.app_data_dir.join("providers.json"), 1);
            let revision = store
                .commit_allowing_unversioned(
                    expected_revision,
                    durable_providers,
                    validate_provider_config,
                )
                .map_err(ConfigSectionMutationError::Store)?;
            (revision, store.path().to_path_buf())
        };

        candidate.publish_env_vars();
        *live_config = candidate;
        self.provider_registry.replace_with(registry);
        *live_provider = provider;
        publish_section_success(
            &self.config_live_health,
            &self.account_sink,
            "providers",
            source_path,
            section_is_unhealthy(&self.config_live_health),
            Some(revision),
        )
        .await;
        Ok(revision)
    }

    /// Replace the complete provider-owned settings domain under the typed
    /// provider section revision. Provider metadata and explicitly touched
    /// built-in/instance credentials share one recoverable exact transaction;
    /// runtime construction is staged before the durable CAS boundary.
    pub(crate) async fn put_provider_settings<F>(
        &self,
        expected_revision: u64,
        update: F,
    ) -> Result<u64, ConfigSectionMutationError>
    where
        F: FnOnce(
                &Config,
                &mut Config,
            )
                -> Result<(BTreeSet<String>, BTreeSet<String>), ConfigSectionMutationError>
            + Send
            + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let config_live_health = self.config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            ensure_provider_mcp_migration_ready(&app_data_dir)
                .map_err(ConfigSectionMutationError::Store)?;
            let facade = config_facade.as_ref().ok_or_else(|| {
                ConfigSectionMutationError::Invalid(
                    "provider settings require the modular configuration facade".to_string(),
                )
            })?;
            let current = config.read().await.clone();
            let mut candidate = current.clone();
            let (provider_intents, provider_instance_intents) = update(&current, &mut candidate)?;

            let (candidate, registry, candidate_provider) =
                match prepare_provider_candidate(candidate, &app_data_dir).await {
                    Ok(prepared) => prepared,
                    Err(_) => {
                        let message =
                        "provider runtime initialization failed; retaining last-known-good runtime"
                            .to_string();
                        publish_section_failure(
                            &config_live_health,
                            &account_sink,
                            "providers",
                            SectionStatus::Degraded,
                            message.clone(),
                        )
                        .await;
                        return Err(ConfigSectionMutationError::Runtime(message));
                    }
                };

            // Acquire publication guards before the durable boundary. The
            // detached task owns them until config and provider runtime are
            // published, so request cancellation cannot strand disk ahead of
            // process state.
            let mut live_config = config.write().await;
            let mut live_provider = provider.write().await;
            let transaction_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let (mut committed, commit) = tokio::task::spawn_blocking(move || {
                let mut durable_candidate = candidate;
                let commit =
                    bamboo_config::persist_provider_credential_transaction_at_revision_with_adoption(
                    &transaction_dir,
                    &mut durable_candidate,
                    &provider_intents,
                    &provider_instance_intents,
                    expected_revision,
                    commit_facade.as_ref(),
                )?;
                Ok::<_, ConfigStoreError>((durable_candidate, commit))
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "provider settings transaction task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;

            let installed = install_credential_section_commit(commit, &mut committed)
                .map_err(ConfigSectionMutationError::Store)?;
            committed.publish_env_vars();
            *live_config = committed;
            provider_registry.replace_with(registry);
            *live_provider = candidate_provider;
            publish_exact_facade_events(&account_sink, &installed.events)
                .await
                .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;
            let provider_snapshot = facade.registry().providers.snapshot();
            set_live_health_revision(
                &config_live_health,
                provider_snapshot.revision,
                Some((
                    provider_snapshot.source_path.clone(),
                    SectionSourceKind::File,
                )),
            );
            Ok(provider_snapshot.revision)
        });
        transaction.await.map_err(|error| {
            ConfigSectionMutationError::Runtime(format!(
                "provider settings transaction task failed: {error}"
            ))
        })?
    }

    /// Reset the complete provider section and all provider-owned credentials
    /// in one recoverable exact transaction guarded by the typed section
    /// revision. Runtime publication remains last-known-good when the default
    /// provider cannot be initialized without credentials.
    pub(crate) async fn reset_provider_section(
        &self,
        expected_revision: u64,
    ) -> Result<u64, ConfigSectionMutationError> {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let config_live_health = self.config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            ensure_provider_mcp_migration_ready(&app_data_dir)
                .map_err(ConfigSectionMutationError::Store)?;
            let facade = config_facade.as_ref().ok_or_else(|| {
                ConfigSectionMutationError::Invalid(
                    "provider reset requires the modular configuration facade".to_string(),
                )
            })?;
            let current = config.read().await.clone();
            let provider_intents = BTreeSet::from([
                "openai".to_string(),
                "anthropic".to_string(),
                "gemini".to_string(),
                "bodhi".to_string(),
            ]);
            let provider_instance_intents = current.provider_instances.keys().cloned().collect();
            let mut candidate = current;
            apply_runtime_section(SectionId::Providers, &Config::default(), &mut candidate);

            let transaction_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let (mut candidate, commit) = tokio::task::spawn_blocking(move || {
                let commit =
                    bamboo_config::persist_provider_reset_credential_transaction_at_revision_with_adoption(
                    &transaction_dir,
                    &mut candidate,
                    &provider_intents,
                    &provider_instance_intents,
                    expected_revision,
                    commit_facade.as_ref(),
                )?;
                Ok::<_, ConfigStoreError>((candidate, commit))
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "provider reset transaction task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;

            let installed = install_credential_section_commit(commit, &mut candidate)
                .map_err(ConfigSectionMutationError::Store)?;
            *config.write().await = candidate.clone();
            let provider_snapshot = facade.registry().providers.snapshot();
            set_live_health_revision(
                &config_live_health,
                provider_snapshot.revision,
                Some((
                    provider_snapshot.source_path.clone(),
                    SectionSourceKind::File,
                )),
            );
            publish_exact_facade_events(&account_sink, &installed.events)
                .await
                .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;

            let runtime_failure = match bamboo_llm::ProviderRegistry::from_config(
                &candidate,
                app_data_dir,
            )
            .await
            {
                Ok(registry) => {
                    if let Some(candidate_provider) = registry.get_default() {
                        provider_registry.replace_with(registry);
                        *provider.write().await = candidate_provider;
                        None
                    } else {
                        Some(
                            "provider reset committed; default provider is not initialized"
                                .to_string(),
                        )
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "provider reset committed but runtime initialization failed");
                    Some("provider reset committed; retaining last-known-good runtime".to_string())
                }
            };
            if let Some(message) = runtime_failure {
                publish_section_failure(
                    &config_live_health,
                    &account_sink,
                    "providers",
                    SectionStatus::Degraded,
                    message,
                )
                .await;
            }
            Ok(provider_snapshot.revision)
        });
        transaction.await.map_err(|error| {
            ConfigSectionMutationError::Runtime(format!(
                "provider reset transaction task failed: {error}"
            ))
        })?
    }

    /// Stage MCP connection/initialization/tool discovery, perform the CAS at
    /// the manager's pre-publication boundary, then publish the config snapshot
    /// before the prepared runtimes and finally emit one section event.
    pub(crate) async fn put_mcp_section(
        &self,
        expected_revision: u64,
        mut candidate: McpConfig,
    ) -> Result<u64, ConfigSectionMutationError> {
        let _io = self.config_io_lock.lock().await;
        ensure_provider_mcp_migration_ready(&self.app_data_dir)
            .map_err(ConfigSectionMutationError::Store)?;
        retain_mcp_credentials(
            &self.config.read().await.mcp,
            &mut candidate,
            &BTreeSet::new(),
        );
        validate_mcp_config(&candidate).map_err(ConfigSectionMutationError::Invalid)?;
        let mut hydration_config = Config::default();
        hydration_config.mcp = candidate;
        hydration_config
            .hydrate_mcp_credentials_from_store(&self.app_data_dir)
            .map_err(|_| {
                ConfigSectionMutationError::Invalid(
                    "referenced MCP credential is unavailable".to_string(),
                )
            })?;
        let candidate = hydration_config.mcp.clone();
        let mut revision = None;
        let mut store_error = None;
        let durable_candidate = credential_ref_mcp_document(&candidate)?;
        let mut next_config = candidate.clone();
        retain_mcp_credential_refs(&durable_candidate, &mut next_config);
        let result = self
            .mcp_manager
            .reconcile_from_config_transactional_after(&candidate, || async {
                // Stage may await freely, but acquire the snapshot guard before
                // the durable boundary. Commit + snapshot publication below is
                // then one cancellation-free synchronous critical section.
                let mut live_config = self.config.write().await;
                let commit = if let Some(facade) = self.config_facade.as_ref() {
                    facade
                        .registry()
                        .mcp
                        .commit(expected_revision, McpSection(durable_candidate))
                        .map(|event| match event {
                            ConfigSectionEvent::Changed { revision, .. } => revision,
                            _ => unreachable!("a successful section commit is changed"),
                        })
                } else {
                    AtomicJsonStore::new(self.app_data_dir.join("mcp.json"), 1).commit(
                        expected_revision,
                        durable_candidate,
                        validate_mcp_config,
                    )
                };
                match commit {
                    Ok(committed) => {
                        live_config.mcp = next_config;
                        revision = Some(committed);
                        Ok(())
                    }
                    Err(error) => {
                        store_error = Some(error);
                        Err(bamboo_mcp::McpError::InvalidConfig(
                            "MCP section durable commit failed".to_string(),
                        ))
                    }
                }
            })
            .await;
        if let Some(error) = store_error {
            return Err(ConfigSectionMutationError::Store(error));
        }
        if result.is_err() {
            let message =
                "MCP runtime initialization failed; retaining last-known-good runtime".to_string();
            publish_section_failure(
                &self.mcp_config_live_health,
                &self.account_sink,
                "mcp",
                SectionStatus::Degraded,
                message.clone(),
            )
            .await;
            return Err(ConfigSectionMutationError::Runtime(message));
        }
        let revision = revision.expect("successful MCP reconcile commits a revision");
        publish_section_success(
            &self.mcp_config_live_health,
            &self.account_sink,
            "mcp",
            self.app_data_dir.join("mcp.json"),
            section_is_unhealthy(&self.mcp_config_live_health),
            Some(revision),
        )
        .await;
        Ok(revision)
    }

    /// Replace editable MCP metadata and explicitly touched credentials under
    /// one section CAS. Runtime construction is staged first; credential and
    /// MCP documents cross the durable boundary together.
    pub(crate) async fn put_mcp_settings(
        &self,
        expected_revision: u64,
        candidate: McpConfig,
        credential_intents: BTreeSet<bamboo_config::CredentialRef>,
    ) -> Result<u64, ConfigSectionMutationError> {
        if credential_intents.is_empty() {
            return self.put_mcp_section(expected_revision, candidate).await;
        }

        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let mcp_manager = self.mcp_manager.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            ensure_provider_mcp_migration_ready(&app_data_dir)
                .map_err(ConfigSectionMutationError::Store)?;
            let facade = config_facade.as_ref().ok_or_else(|| {
                ConfigSectionMutationError::Invalid(
                    "MCP settings require the modular configuration facade".to_string(),
                )
            })?;
            let current = config.read().await.clone();
            let mut runtime_candidate = candidate;
            materialize_mcp_touched_replacements(&mut runtime_candidate, &credential_intents)
                .map_err(ConfigSectionMutationError::Invalid)?;
            retain_mcp_credentials(&current.mcp, &mut runtime_candidate, &credential_intents);
            validate_mcp_config(&runtime_candidate).map_err(ConfigSectionMutationError::Invalid)?;

            let mut transaction_error = None;
            let mut commit_events = Vec::new();
            let transaction_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let mut durable_candidate = current;
            durable_candidate.mcp = runtime_candidate.clone();
            let result = mcp_manager
                .reconcile_from_config_transactional_after(&runtime_candidate, || async {
                    let mut live_config = config.write().await;
                    let commit = tokio::task::spawn_blocking(move || {
                        let commit =
                            bamboo_config::persist_mcp_credential_transaction_at_revision_with_adoption(
                            &transaction_dir,
                            &mut durable_candidate,
                            &credential_intents,
                            expected_revision,
                            commit_facade.as_ref(),
                        )?;
                        Ok::<_, ConfigStoreError>((durable_candidate, commit))
                    })
                    .await;
                    match commit {
                        Ok(Ok((mut committed, commit))) => {
                            let installed =
                                match install_credential_section_commit(commit, &mut committed) {
                                    Ok(installed) => installed,
                                    Err(error) => {
                                        transaction_error =
                                            Some(ConfigSectionMutationError::Store(error));
                                        return Err(bamboo_mcp::McpError::InvalidConfig(
                                            "MCP settings process adoption failed".to_string(),
                                        ));
                                    }
                                };
                            *live_config = committed;
                            commit_events = installed.events;
                            Ok(())
                        }
                        Ok(Err(error)) => {
                            transaction_error = Some(ConfigSectionMutationError::Store(error));
                            Err(bamboo_mcp::McpError::InvalidConfig(
                                "MCP settings durable transaction failed".to_string(),
                            ))
                        }
                        Err(error) => {
                            transaction_error = Some(ConfigSectionMutationError::Runtime(format!(
                                "MCP settings transaction task failed: {error}"
                            )));
                            Err(bamboo_mcp::McpError::InvalidConfig(
                                "MCP settings durable transaction failed".to_string(),
                            ))
                        }
                    }
                })
                .await;
            if let Some(error) = transaction_error {
                return Err(error);
            }
            if result.is_err() {
                let message =
                    "MCP runtime initialization failed; retaining last-known-good runtime"
                        .to_string();
                publish_section_failure(
                    &mcp_config_live_health,
                    &account_sink,
                    "mcp",
                    SectionStatus::Degraded,
                    message.clone(),
                )
                .await;
                return Err(ConfigSectionMutationError::Runtime(message));
            }

            publish_exact_facade_events(&account_sink, &commit_events)
                .await
                .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;
            let snapshot = facade.registry().mcp.snapshot();
            set_live_health_revision(
                &mcp_config_live_health,
                snapshot.revision,
                Some((snapshot.source_path.clone(), SectionSourceKind::File)),
            );
            Ok(snapshot.revision)
        });
        transaction.await.map_err(|error| {
            ConfigSectionMutationError::Runtime(format!(
                "MCP settings transaction task failed: {error}"
            ))
        })?
    }

    /// Apply one legacy MCP mutation through the typed MCP section's exact
    /// credential/runtime transaction.
    ///
    /// Lock order is always `config_io_lock` -> MCP `reconcile_lock` -> live
    /// `config` write. Provider locks are never acquired on this path. Runtime
    /// connection, initialization, and tool discovery finish before the
    /// durable MCP/credential boundary. The detached owner then publishes the
    /// exact live section, runtime set, tool index, health, and events before a
    /// later config generation may acquire `config_io_lock`.
    pub(crate) async fn update_legacy_mcp_config<F>(
        &self,
        force_restart: BTreeSet<String>,
        update: F,
    ) -> Result<Config, AppError>
    where
        F: FnOnce(&mut McpConfig) -> Result<(), AppError>,
    {
        // Apply the caller's mutation against the exact lock-time live
        // generation. Cancellation while either guard is pending is pre-commit.
        let io = self.config_io_lock.clone().lock_owned().await;
        let (mut candidate_config, expected_revision, credential_intents) = {
            ensure_provider_mcp_migration_ready(&self.app_data_dir)
                .map_err(map_exact_credential_store_error)?;
            let facade = self.config_facade.as_ref().ok_or_else(|| {
                AppError::BadRequest(
                    "legacy MCP mutations require the modular configuration facade".to_string(),
                )
            })?;
            let current = self.config.read().await.clone();
            reject_if_recovery_pending(&current)?;
            let mut candidate = current.mcp.clone();
            update(&mut candidate)?;
            let credential_intents =
                normalize_legacy_mcp_credentials(&current.mcp, &mut candidate)?;
            validate_mcp_config(&candidate).map_err(AppError::BadRequest)?;
            let expected_revision = facade.registry().mcp.snapshot().revision;
            let mut candidate_config = current;
            candidate_config.mcp = candidate;
            (candidate_config, expected_revision, credential_intents)
        };

        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let mcp_manager = self.mcp_manager.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = io;
            let facade = config_facade.expect("validated modular configuration facade");
            let runtime_candidate = candidate_config.mcp.clone();
            let force_replacements = force_restart.into_iter().collect();
            let durable_document =
                credential_ref_mcp_document(&runtime_candidate).map_err(map_mcp_section_error)?;
            bamboo_config::validate_mcp_section(&durable_document).map_err(AppError::BadRequest)?;
            let current_document = facade.registry().mcp.snapshot();
            let metadata_changed = serde_json::to_value(&current_document.data.0)
                .map_err(AppError::SerializationError)?
                != serde_json::to_value(&durable_document).map_err(AppError::SerializationError)?;
            let credential_transaction = !credential_intents.is_empty();
            let mut transaction_error = None;
            let mut commit_events = Vec::new();
            let mut published_config = None;
            let commit_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let result = mcp_manager
                .reconcile_from_config_transactional_after_forcing(
                    &runtime_candidate,
                    &force_replacements,
                    || async {
                        // Acquiring the live guard before the durable call
                        // closes the only cancellation window between disk and
                        // process state.
                        let mut live_config = config.write().await;
                        if credential_transaction {
                            let commit = tokio::task::spawn_blocking(move || {
                                let commit = bamboo_config::persist_mcp_credential_transaction_at_revision_with_adoption(
                                    &commit_dir,
                                    &mut candidate_config,
                                    &credential_intents,
                                    expected_revision,
                                    commit_facade.as_ref(),
                                )?;
                                Ok::<_, ConfigStoreError>((candidate_config, commit))
                            })
                            .await;
                            match commit {
                                Ok(Ok((mut committed, commit))) => {
                                    #[cfg(test)]
                                    run_credential_after_commit_before_live_test_hook(
                                        &app_data_dir,
                                        SectionId::Mcp,
                                    );
                                    match install_credential_section_commit(commit, &mut committed) {
                                        Ok(installed) => {
                                            *live_config = committed.clone();
                                            commit_events = installed.events;
                                            published_config = Some(committed);
                                        }
                                        Err(error) => {
                                            transaction_error =
                                                Some(map_exact_credential_store_error(error));
                                            return Err(bamboo_mcp::McpError::InvalidConfig(
                                                "MCP process adoption failed".to_string(),
                                            ));
                                        }
                                    }
                                }
                                Ok(Err(error)) => {
                                    transaction_error =
                                        Some(map_exact_credential_store_error(error));
                                    return Err(bamboo_mcp::McpError::InvalidConfig(
                                        "MCP durable transaction failed".to_string(),
                                    ));
                                }
                                Err(error) => {
                                    transaction_error = Some(AppError::InternalError(
                                        anyhow::anyhow!(
                                            "MCP credential transaction task failed: {error}"
                                        ),
                                    ));
                                    return Err(bamboo_mcp::McpError::InvalidConfig(
                                        "MCP durable transaction failed".to_string(),
                                    ));
                                }
                            }
                        } else {
                            if metadata_changed {
                                match facade
                                    .registry()
                                    .mcp
                                    .commit(expected_revision, McpSection(durable_document))
                                {
                                    Ok(event) => commit_events.push(event),
                                    Err(error) => {
                                        transaction_error =
                                            Some(map_exact_credential_store_error(error));
                                        return Err(bamboo_mcp::McpError::InvalidConfig(
                                            "MCP durable commit failed".to_string(),
                                        ));
                                    }
                                }
                            }
                            candidate_config.mcp = runtime_candidate.clone();
                            *live_config = candidate_config.clone();
                            published_config = Some(candidate_config);
                        }
                        Ok(())
                    },
                )
                .await;

            if let Some(error) = transaction_error {
                return Err(error);
            }
            if result.is_err() {
                tracing::warn!("legacy MCP runtime staging failed before durable commit");
                let message =
                    "MCP runtime initialization failed before commit; retaining last-known-good generation"
                        .to_string();
                return Err(AppError::InternalError(anyhow::anyhow!(message)));
            }

            // Runtime/tool-index publication is complete when transactional
            // reconcile returns. Event/health publication therefore cannot
            // prevent an already-committed generation from becoming runtime.
            publish_exact_facade_events(&account_sink, &commit_events).await?;
            let snapshot = facade.registry().mcp.snapshot();
            set_live_health_revision(
                &mcp_config_live_health,
                snapshot.revision,
                Some((snapshot.source_path.clone(), SectionSourceKind::File)),
            );
            Ok::<_, AppError>(
                published_config.expect("successful MCP transaction publishes config"),
            )
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "legacy MCP config transaction task failed: {error}"
            ))
        })?
    }

    /// Reset MCP metadata and its owned credentials at the runtime manager's
    /// pre-publication boundary, preserving the same durable-before-live
    /// ordering as a normal typed MCP write.
    pub(crate) async fn reset_mcp_section(
        &self,
        expected_revision: u64,
    ) -> Result<u64, ConfigSectionMutationError> {
        let _io = self.config_io_lock.lock().await;
        ensure_provider_mcp_migration_ready(&self.app_data_dir)
            .map_err(ConfigSectionMutationError::Store)?;
        let facade = self.config_facade.as_ref().ok_or_else(|| {
            ConfigSectionMutationError::Invalid(
                "MCP reset requires the modular configuration facade".to_string(),
            )
        })?;
        let candidate_mcp = McpConfig::default();
        let mut candidate_config = self.config.read().await.clone();
        candidate_config.mcp = candidate_mcp.clone();
        let mut committed = false;
        let mut commit_events = Vec::new();
        let mut store_error = None;
        let data_dir = self.app_data_dir.clone();
        let result = self
            .mcp_manager
            .reconcile_from_config_transactional_after(&candidate_mcp, || async {
                let mut live_config = self.config.write().await;
                match bamboo_config::persist_mcp_reset_credential_transaction_at_revision_with_adoption(
                    &data_dir,
                    &mut candidate_config,
                    expected_revision,
                    facade.as_ref(),
                ) {
                    Ok(commit) => {
                        match install_credential_section_commit(commit, &mut candidate_config) {
                            Ok(installed) => {
                                *live_config = candidate_config.clone();
                                commit_events = installed.events;
                                committed = true;
                            }
                            Err(error) => {
                                store_error = Some(error);
                                return Err(bamboo_mcp::McpError::InvalidConfig(
                                    "MCP reset process adoption failed".to_string(),
                                ));
                            }
                        }
                        Ok(())
                    }
                    Err(error) => {
                        store_error = Some(error);
                        Err(bamboo_mcp::McpError::InvalidConfig(
                            "MCP reset durable commit failed".to_string(),
                        ))
                    }
                }
            })
            .await;
        if let Some(error) = store_error {
            return Err(ConfigSectionMutationError::Store(error));
        }
        if result.is_err() || !committed {
            let message =
                "MCP reset runtime initialization failed; retaining last-known-good runtime"
                    .to_string();
            publish_section_failure(
                &self.mcp_config_live_health,
                &self.account_sink,
                "mcp",
                SectionStatus::Degraded,
                message.clone(),
            )
            .await;
            return Err(ConfigSectionMutationError::Runtime(message));
        }
        publish_exact_facade_events(&self.account_sink, &commit_events)
            .await
            .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;
        let revision = facade.registry().mcp.snapshot().revision;
        publish_section_success(
            &self.mcp_config_live_health,
            &self.account_sink,
            "mcp",
            self.app_data_dir.join("mcp.json"),
            section_is_unhealthy(&self.mcp_config_live_health),
            Some(revision),
        )
        .await;
        Ok(revision)
    }

    /// Reset a credential-backed non-provider section using the typed section
    /// revision as the client CAS authority. The exact transaction rebases on
    /// the latest unrelated credential document while clearing only references
    /// owned by this section.
    pub(crate) async fn reset_credential_backed_section(
        &self,
        id: SectionId,
        expected_revision: u64,
    ) -> Result<CredentialBackedResetCommit, ConfigSectionMutationError> {
        if !matches!(
            id,
            SectionId::Core
                | SectionId::Notifications
                | SectionId::Connect
                | SectionId::Env
                | SectionId::ClusterFabric
                | SectionId::AccessControl
        ) {
            return Err(ConfigSectionMutationError::Invalid(
                "section is not a credential-backed reset domain".to_string(),
            ));
        }
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let deployed_registry = self.fabric_deployer.registry();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            if id == SectionId::ClusterFabric {
                let deployed = deployed_registry.lock().await;
                if let Some(node_id) = deployed.keys().find_map(|key| {
                    let (source, node_id) = bamboo_server_tools::registry_keys::split(key);
                    (source == "node").then(|| node_id.to_string())
                }) {
                    return Err(ConfigSectionMutationError::Invalid(format!(
                        "node '{node_id}' is deployed; stop it before resetting cluster-fabric"
                    )));
                }
            }
            ensure_provider_mcp_migration_ready(&app_data_dir)
                .map_err(ConfigSectionMutationError::Store)?;
            let facade = config_facade.as_ref().ok_or_else(|| {
                ConfigSectionMutationError::Invalid(
                    "section reset requires the modular configuration facade".to_string(),
                )
            })?;
            let mut candidate = config.read().await.clone();
            apply_runtime_section(id, &Config::default(), &mut candidate);
            let transaction_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let (mut candidate, revision, cluster_commit, section_commit) =
                tokio::task::spawn_blocking(move || {
                    if id == SectionId::ClusterFabric {
                        let commit =
                            bamboo_config::persist_cluster_fabric_reset_at_revision_with_adoption(
                                &transaction_dir,
                                &mut candidate,
                                expected_revision,
                                commit_facade.as_ref(),
                                |_, _| {},
                            )?;
                        let revision = commit.revision;
                        Ok::<_, ConfigStoreError>((candidate, revision, Some(commit), None))
                    } else {
                        let commit =
                            bamboo_config::persist_credential_backed_section_reset_at_revision_with_adoption(
                                &transaction_dir,
                                &mut candidate,
                                id,
                                expected_revision,
                                commit_facade.as_ref(),
                            )?;
                        let revision = commit.revision;
                        Ok((candidate, revision, None, Some(commit)))
                    }
                })
                .await
                .map_err(|error| {
                    ConfigSectionMutationError::Runtime(format!(
                        "section reset transaction task failed: {error}"
                    ))
                })?
                .map_err(ConfigSectionMutationError::Store)?;

            let cluster_runtime = match cluster_commit {
                Some(commit) => {
                    let bamboo_config::ClusterFabricTransactionCommit {
                        revision: _,
                        adoption,
                        credential_adoption,
                        committed_recovery,
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
                            // A semantic no-op did not cross a durable boundary,
                            // so preserve the ordinary store error contract.
                            return Err(ConfigSectionMutationError::Store(error));
                        }
                        Err(error) => {
                            // The reset metadata and facade are already
                            // committed. Publish the exact secret-free reset
                            // candidate instead of retaining the pre-reset
                            // runtime while reporting the post-commit failure.
                            candidate.clear_cluster_runtime_credentials();
                            Err(error)
                        }
                    };
                    Some((adoption, credential_adoption, committed_recovery, runtime))
                }
                None => None,
            };
            let (section_events, exact_section) = match section_commit {
                Some(commit) => {
                    let installed = install_credential_section_commit(commit, &mut candidate)
                        .map_err(ConfigSectionMutationError::Store)?;
                    (installed.events, installed.section)
                }
                None => (Vec::new(), None),
            };
            if id == SectionId::Env {
                candidate.publish_env_vars();
            }
            *config.write().await = candidate.clone();
            if id != SectionId::ClusterFabric {
                publish_exact_facade_events(&account_sink, &section_events)
                    .await
                    .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;
            }
            let commit = if id == SectionId::ClusterFabric {
                let (cluster_adoption, credential_adoption, committed_recovery, cluster_runtime) =
                    cluster_runtime.expect("cluster reset captures an exact runtime");
                let event = match cluster_adoption {
                    Some(Ok(event)) => Some(event),
                    Some(Err(error)) => {
                        return Err(ConfigSectionMutationError::Runtime(format!(
                            "cluster reset committed at revision {revision} but process adoption failed: {error}"
                        )));
                    }
                    None if revision == expected_revision => None,
                    None => {
                        return Err(ConfigSectionMutationError::Runtime(format!(
                            "cluster reset committed at revision {revision} without a process adoption result"
                        )));
                    }
                };
                let section = facade
                    .registry()
                    .envelope_value(SectionId::ClusterFabric)
                    .map_err(|error| {
                        if revision == expected_revision {
                            ConfigSectionMutationError::Store(error)
                        } else {
                            ConfigSectionMutationError::Runtime(format!(
                                "cluster reset committed at revision {revision} but its exact envelope is unavailable: {error}"
                            ))
                        }
                    })?;
                if section.revision != revision {
                    return Err(ConfigSectionMutationError::Runtime(format!(
                        "cluster reset committed at revision {revision} but facade retained revision {}",
                        section.revision
                    )));
                }
                if let Some(event) = event.as_ref() {
                    publish_registry_event(&account_sink, event).await;
                }
                if let Err(error) = committed_recovery {
                    return Err(ConfigSectionMutationError::Runtime(format!(
                        "cluster reset committed at revision {revision} but transaction recovery failed: {error}"
                    )));
                }
                if let Some(Err(error)) = credential_adoption {
                    return Err(ConfigSectionMutationError::Runtime(format!(
                        "cluster reset committed at revision {revision} but credential facade adoption failed: {error}"
                    )));
                }
                let (credential_statuses, credential_health) =
                    cluster_runtime.map_err(|error| {
                        ConfigSectionMutationError::Runtime(format!(
                            "cluster reset committed at revision {revision} but could not materialize its exact runtime credentials: {error}"
                        ))
                    })?;
                CredentialBackedResetCommit::Cluster(Box::new(
                    bamboo_server_tools::FabricCommitSnapshot {
                        config: candidate.clone(),
                        section,
                        credential_statuses,
                        credential_health,
                    },
                ))
            } else {
                let section = exact_section.ok_or_else(|| {
                    ConfigSectionMutationError::Runtime(format!(
                        "{} reset committed at revision {revision} without its exact envelope",
                        id.descriptor().name
                    ))
                })?;
                if section.revision != revision {
                    return Err(ConfigSectionMutationError::Runtime(format!(
                        "{} reset committed at revision {revision} but captured revision {}",
                        id.descriptor().name,
                        section.revision
                    )));
                }
                CredentialBackedResetCommit::Section(section)
            };

            if id == SectionId::Core {
                match bamboo_llm::ProviderRegistry::from_config(&candidate, app_data_dir.clone())
                    .await
                {
                    Ok(registry) => {
                        if let Some(candidate_provider) = registry.get_default() {
                            provider_registry.replace_with(registry);
                            *provider.write().await = candidate_provider;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "core reset committed but provider reload failed");
                    }
                }
                mcp_manager.reconcile_from_config(&candidate.mcp).await;
            }

            Ok(commit)
        });
        transaction.await.map_err(|error| {
            ConfigSectionMutationError::Runtime(format!(
                "section reset transaction task failed: {error}"
            ))
        })?
    }
}

fn credential_reference_inventory(value: &Value) -> std::collections::BTreeMap<String, Value> {
    fn collect(value: &Value, path: &str, output: &mut std::collections::BTreeMap<String, Value>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    let child_path =
                        format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
                    let normalized = key
                        .chars()
                        .filter(|ch| ch.is_ascii_alphanumeric())
                        .flat_map(char::to_lowercase)
                        .collect::<String>();
                    if normalized == "credentialref"
                        || normalized.ends_with("credentialref")
                        || normalized.ends_with("credentialrefs")
                    {
                        output.insert(child_path, value.clone());
                    } else {
                        collect(value, &child_path, output);
                    }
                }
            }
            Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    collect(value, &format!("{path}/{index}"), output);
                }
            }
            _ => {}
        }
    }

    let mut output = std::collections::BTreeMap::new();
    collect(value, "", &mut output);
    output
}

fn provider_durable_document(
    providers: &ProviderConfigs,
) -> Result<ProviderConfigs, ConfigSectionMutationError> {
    let mut document = providers.clone();
    macro_rules! sanitize {
        ($field:ident) => {
            if let Some(provider) = document.$field.as_mut() {
                provider.api_key.clear();
                provider.api_key_encrypted = None;
            }
        };
    }
    sanitize!(openai);
    sanitize!(anthropic);
    sanitize!(gemini);
    if let Some(provider) = document.bodhi.as_mut() {
        provider.api_key.clear();
        provider.api_key_encrypted = None;
    }
    validate_provider_config(&document).map_err(ConfigSectionMutationError::Invalid)?;
    Ok(document)
}

fn retain_provider_credentials(current: &ProviderConfigs, candidate: &mut ProviderConfigs) {
    candidate.extra = current.extra.clone();
    macro_rules! retain {
        ($field:ident) => {
            if let (Some(current), Some(candidate)) = (&current.$field, &mut candidate.$field) {
                candidate.api_key = current.api_key.clone();
                candidate.api_key_encrypted = current.api_key_encrypted.clone();
                if candidate.credential_ref.is_none() {
                    candidate.credential_ref = current.credential_ref.clone();
                }
                if candidate.credential_ref != current.credential_ref {
                    candidate.api_key.clear();
                    candidate.api_key_encrypted = None;
                }
                candidate.api_key_from_env = current.api_key_from_env;
                candidate.request_overrides = current.request_overrides.clone();
                candidate.extra = current.extra.clone();
            }
        };
    }
    retain!(openai);
    retain!(anthropic);
    retain!(gemini);
    if let (Some(current), Some(candidate)) = (&current.bodhi, &mut candidate.bodhi) {
        candidate.api_key = current.api_key.clone();
        candidate.api_key_encrypted = current.api_key_encrypted.clone();
        if candidate.credential_ref.is_none() {
            candidate.credential_ref = current.credential_ref.clone();
        }
        if candidate.credential_ref != current.credential_ref {
            candidate.api_key.clear();
            candidate.api_key_encrypted = None;
        }
        candidate.extra = current.extra.clone();
    }
    if let (Some(current), Some(candidate)) = (&current.copilot, &mut candidate.copilot) {
        candidate.request_overrides = current.request_overrides.clone();
        candidate.extra = current.extra.clone();
    }
}

fn retain_mcp_credentials(
    current: &McpConfig,
    candidate: &mut McpConfig,
    touched: &BTreeSet<bamboo_config::CredentialRef>,
) {
    for candidate_server in &mut candidate.servers {
        let Some(current_server) = current
            .servers
            .iter()
            .find(|server| server.id == candidate_server.id)
        else {
            continue;
        };
        if let (TransportConfig::Stdio(current), TransportConfig::Stdio(candidate)) =
            (&current_server.transport, &mut candidate_server.transport)
        {
            if candidate.env.is_empty()
                && candidate.env_encrypted.is_empty()
                && candidate.env_credential_refs.is_empty()
            {
                for (name, reference) in &current.env_credential_refs {
                    if mcp_credential_ref_is_touched(Some(reference), touched) {
                        continue;
                    }
                    candidate
                        .env_credential_refs
                        .insert(name.clone(), reference.clone());
                    if let Some(value) = current.env.get(name) {
                        candidate.env.insert(name.clone(), value.clone());
                    }
                    if let Some(value) = current.env_encrypted.get(name) {
                        candidate.env_encrypted.insert(name.clone(), value.clone());
                    }
                }
            } else {
                for (name, reference) in &current.env_credential_refs {
                    if candidate.env_credential_refs.get(name) != Some(reference) {
                        continue;
                    }
                    if candidate.env.get(name).is_none_or(|value| value.is_empty()) {
                        if let Some(value) = current.env.get(name) {
                            candidate.env.insert(name.clone(), value.clone());
                        }
                    }
                }
            }
        }
        match (&current_server.transport, &mut candidate_server.transport) {
            (TransportConfig::Sse(current), TransportConfig::Sse(candidate)) => {
                retain_mcp_header_credentials(&current.headers, &mut candidate.headers)
            }
            (
                TransportConfig::StreamableHttp(current),
                TransportConfig::StreamableHttp(candidate),
            ) => retain_mcp_header_credentials(&current.headers, &mut candidate.headers),
            _ => {}
        }
    }
}

fn materialize_mcp_touched_replacements(
    candidate: &mut McpConfig,
    touched: &BTreeSet<bamboo_config::CredentialRef>,
) -> Result<(), String> {
    let mut replacements = BTreeMap::<bamboo_config::CredentialRef, String>::new();
    for server in &candidate.servers {
        match &server.transport {
            TransportConfig::Stdio(stdio) => {
                for (name, raw_reference) in &stdio.env_credential_refs {
                    let reference = bamboo_config::CredentialRef::parse(raw_reference.clone())
                        .map_err(|_| "MCP credential reference is invalid".to_string())?;
                    if let Some(value) = stdio
                        .env
                        .get(name)
                        .filter(|value| touched.contains(&reference) && !value.is_empty())
                    {
                        insert_mcp_replacement(&mut replacements, reference, value)?;
                    }
                }
            }
            TransportConfig::Sse(http) => {
                collect_mcp_header_replacements(&http.headers, touched, &mut replacements)?
            }
            TransportConfig::StreamableHttp(http) => {
                collect_mcp_header_replacements(&http.headers, touched, &mut replacements)?
            }
        }
    }
    if replacements.is_empty() {
        return Ok(());
    }
    for server in &mut candidate.servers {
        match &mut server.transport {
            TransportConfig::Stdio(stdio) => {
                for (name, raw_reference) in &stdio.env_credential_refs {
                    let reference = bamboo_config::CredentialRef::parse(raw_reference.clone())
                        .map_err(|_| "MCP credential reference is invalid".to_string())?;
                    if let Some(value) = replacements.get(&reference) {
                        stdio.env.insert(name.clone(), value.clone());
                    }
                }
            }
            TransportConfig::Sse(http) => {
                apply_mcp_header_replacements(&mut http.headers, &replacements)?
            }
            TransportConfig::StreamableHttp(http) => {
                apply_mcp_header_replacements(&mut http.headers, &replacements)?
            }
        }
    }
    Ok(())
}

fn collect_mcp_header_replacements(
    headers: &[bamboo_mcp::HeaderConfig],
    touched: &BTreeSet<bamboo_config::CredentialRef>,
    replacements: &mut BTreeMap<bamboo_config::CredentialRef, String>,
) -> Result<(), String> {
    for header in headers {
        let Some(raw_reference) = header.credential_ref.as_ref() else {
            continue;
        };
        let reference = bamboo_config::CredentialRef::parse(raw_reference.clone())
            .map_err(|_| "MCP credential reference is invalid".to_string())?;
        if touched.contains(&reference) && !header.value.is_empty() {
            insert_mcp_replacement(replacements, reference, &header.value)?;
        }
    }
    Ok(())
}

fn insert_mcp_replacement(
    replacements: &mut BTreeMap<bamboo_config::CredentialRef, String>,
    reference: bamboo_config::CredentialRef,
    value: &str,
) -> Result<(), String> {
    match replacements.get(&reference) {
        Some(existing) if existing != value => {
            Err("MCP updates assign conflicting values to one credential reference".to_string())
        }
        Some(_) => Ok(()),
        None => {
            replacements.insert(reference, value.to_string());
            Ok(())
        }
    }
}

fn apply_mcp_header_replacements(
    headers: &mut [bamboo_mcp::HeaderConfig],
    replacements: &BTreeMap<bamboo_config::CredentialRef, String>,
) -> Result<(), String> {
    for header in headers {
        let Some(raw_reference) = header.credential_ref.as_ref() else {
            continue;
        };
        let reference = bamboo_config::CredentialRef::parse(raw_reference.clone())
            .map_err(|_| "MCP credential reference is invalid".to_string())?;
        if let Some(value) = replacements.get(&reference) {
            header.value = value.clone();
        }
    }
    Ok(())
}

fn mcp_credential_ref_is_touched(
    raw_reference: Option<&String>,
    touched: &BTreeSet<bamboo_config::CredentialRef>,
) -> bool {
    raw_reference
        .and_then(|raw| bamboo_config::CredentialRef::parse(raw.clone()).ok())
        .is_some_and(|reference| touched.contains(&reference))
}

fn retain_mcp_header_credentials(
    current: &[bamboo_mcp::HeaderConfig],
    candidate: &mut [bamboo_mcp::HeaderConfig],
) {
    for candidate_header in candidate {
        let Some(current_header) = current
            .iter()
            .find(|header| header.name == candidate_header.name)
        else {
            continue;
        };
        if candidate_header.credential_ref == current_header.credential_ref
            && candidate_header.value.is_empty()
        {
            candidate_header.value = current_header.value.clone();
            candidate_header.value_encrypted = current_header.value_encrypted.clone();
        }
    }
}

fn credential_ref_mcp_document(
    runtime: &McpConfig,
) -> Result<McpConfig, ConfigSectionMutationError> {
    let mut document = runtime.clone();
    for server in &mut document.servers {
        match &mut server.transport {
            TransportConfig::Stdio(config) => {
                config.env_encrypted.clear();
                config.env.retain(|name, value| {
                    !(value.is_empty() || config.env_credential_refs.contains_key(name))
                });
                if !config.env.is_empty() {
                    return Err(ConfigSectionMutationError::Invalid(
                        "MCP secret requires a credential reference".to_string(),
                    ));
                }
            }
            TransportConfig::Sse(config) => reference_headers(&mut config.headers)?,
            TransportConfig::StreamableHttp(config) => reference_headers(&mut config.headers)?,
        }
    }
    Ok(document)
}

fn retain_mcp_credential_refs(document: &McpConfig, runtime: &mut McpConfig) {
    for runtime_server in &mut runtime.servers {
        let Some(document_server) = document
            .servers
            .iter()
            .find(|server| server.id == runtime_server.id)
        else {
            continue;
        };
        match (&document_server.transport, &mut runtime_server.transport) {
            (TransportConfig::Stdio(document), TransportConfig::Stdio(runtime)) => {
                runtime.env_encrypted.clear();
                runtime.env_credential_refs = document.env_credential_refs.clone();
            }
            (TransportConfig::Sse(document), TransportConfig::Sse(runtime)) => {
                copy_header_ciphertext(&document.headers, &mut runtime.headers);
            }
            (
                TransportConfig::StreamableHttp(document),
                TransportConfig::StreamableHttp(runtime),
            ) => copy_header_ciphertext(&document.headers, &mut runtime.headers),
            _ => {}
        }
    }
}

fn copy_header_ciphertext(
    document: &[bamboo_mcp::HeaderConfig],
    runtime: &mut [bamboo_mcp::HeaderConfig],
) {
    for runtime_header in runtime {
        if let Some(document_header) = document
            .iter()
            .find(|header| header.name == runtime_header.name)
        {
            runtime_header.value_encrypted = None;
            runtime_header.credential_ref = document_header.credential_ref.clone();
        }
    }
}

fn reference_headers(
    headers: &mut [bamboo_mcp::HeaderConfig],
) -> Result<(), ConfigSectionMutationError> {
    for header in headers {
        if !header.value.is_empty() && header.credential_ref.is_none() {
            return Err(ConfigSectionMutationError::Invalid(
                "MCP secret requires a credential reference".to_string(),
            ));
        }
        header.value.clear();
        header.value_encrypted = None;
    }
    Ok(())
}

/// Convert the legacy MCP request shape (which carries credential plaintext
/// inline) into the same server-owned references and explicit credential
/// intents used by the typed MCP settings transaction.
///
/// The caller must hold `config_io_lock`. References supplied by a legacy
/// client are never authoritative: an existing binding is retained by field
/// identity, while a new binding receives its canonical server-owned ref.
fn mcp_credential_refs(
    config: &McpConfig,
) -> Result<BTreeSet<bamboo_config::CredentialRef>, ConfigSectionMutationError> {
    let mut references = BTreeSet::new();
    for server in &config.servers {
        match &server.transport {
            TransportConfig::Stdio(stdio) => {
                for raw in stdio.env_credential_refs.values() {
                    references.insert(bamboo_config::CredentialRef::parse(raw.clone()).map_err(
                        |_| {
                            ConfigSectionMutationError::Invalid(
                                "MCP credential reference is invalid".to_string(),
                            )
                        },
                    )?);
                }
            }
            TransportConfig::Sse(http) => collect_mcp_header_refs(&http.headers, &mut references)?,
            TransportConfig::StreamableHttp(http) => {
                collect_mcp_header_refs(&http.headers, &mut references)?
            }
        }
    }
    Ok(references)
}

fn collect_mcp_header_refs(
    headers: &[bamboo_mcp::HeaderConfig],
    output: &mut BTreeSet<bamboo_config::CredentialRef>,
) -> Result<(), ConfigSectionMutationError> {
    for raw in headers
        .iter()
        .filter_map(|header| header.credential_ref.as_ref())
    {
        output.insert(
            bamboo_config::CredentialRef::parse(raw.clone()).map_err(|_| {
                ConfigSectionMutationError::Invalid(
                    "MCP credential reference is invalid".to_string(),
                )
            })?,
        );
    }
    Ok(())
}

fn normalize_legacy_mcp_credentials(
    current: &McpConfig,
    candidate: &mut McpConfig,
) -> Result<BTreeSet<bamboo_config::CredentialRef>, AppError> {
    let current_refs = mcp_credential_refs(current).map_err(map_mcp_section_error)?;
    let mut intents = BTreeSet::new();

    for candidate_server in &mut candidate.servers {
        let current_server = current
            .servers
            .iter()
            .find(|server| server.id == candidate_server.id);
        match &mut candidate_server.transport {
            TransportConfig::Stdio(candidate_stdio) => {
                if !candidate_stdio.env_encrypted.is_empty() {
                    return Err(AppError::BadRequest(
                        "MCP ciphertext is server-managed and cannot be supplied".to_string(),
                    ));
                }
                let current_stdio = current_server.and_then(|server| match &server.transport {
                    TransportConfig::Stdio(stdio) => Some(stdio),
                    _ => None,
                });
                for (name, incoming_reference) in &candidate_stdio.env_credential_refs {
                    let current_reference = current_stdio
                        .and_then(|stdio| stdio.env_credential_refs.get(name))
                        .map(String::as_str);
                    if current_reference != Some(incoming_reference.as_str()) {
                        return Err(AppError::BadRequest(
                            "MCP credential references are server-managed and cannot be supplied"
                                .to_string(),
                        ));
                    }
                }
                let incoming = std::mem::take(&mut candidate_stdio.env);
                candidate_stdio.env_credential_refs.clear();
                for (name, value) in incoming {
                    let current_reference = current_stdio
                        .and_then(|stdio| stdio.env_credential_refs.get(&name))
                        .map(|raw| {
                            bamboo_config::CredentialRef::parse(raw.clone()).map_err(|_| {
                                AppError::BadRequest(
                                    "MCP credential reference is invalid".to_string(),
                                )
                            })
                        })
                        .transpose()?;
                    let current_value = current_stdio.and_then(|stdio| stdio.env.get(&name));
                    let (value, reference, touched) =
                        if bamboo_config::patch::is_masked_api_key(&value) {
                            let reference = current_reference.ok_or_else(|| {
                                AppError::BadRequest(
                                    "masked MCP credential has no existing value".to_string(),
                                )
                            })?;
                            let value = current_value.cloned().ok_or_else(|| {
                                AppError::BadRequest(
                                    "referenced MCP credential is unavailable".to_string(),
                                )
                            })?;
                            (value, reference, false)
                        } else if value.is_empty() {
                            if let Some(reference) = current_reference {
                                intents.insert(reference);
                            }
                            continue;
                        } else {
                            let reference = current_reference.map_or_else(
                                || {
                                    bamboo_config::credential_ref(
                                        "mcp",
                                        &candidate_server.id,
                                        &format!("env_{name}"),
                                    )
                                    .map_err(map_exact_credential_store_error)
                                },
                                Ok,
                            )?;
                            let touched = current_value != Some(&value);
                            (value, reference, touched)
                        };
                    if touched {
                        intents.insert(reference.clone());
                    }
                    candidate_stdio.env.insert(name.clone(), value);
                    candidate_stdio
                        .env_credential_refs
                        .insert(name, reference.as_str().to_string());
                }
            }
            TransportConfig::Sse(candidate_http) => normalize_legacy_mcp_headers(
                &candidate_server.id,
                current_server.and_then(|server| match &server.transport {
                    TransportConfig::Sse(http) => Some(http.headers.as_slice()),
                    _ => None,
                }),
                &mut candidate_http.headers,
                &mut intents,
            )?,
            TransportConfig::StreamableHttp(candidate_http) => normalize_legacy_mcp_headers(
                &candidate_server.id,
                current_server.and_then(|server| match &server.transport {
                    TransportConfig::StreamableHttp(http) => Some(http.headers.as_slice()),
                    _ => None,
                }),
                &mut candidate_http.headers,
                &mut intents,
            )?,
        }
    }

    let candidate_refs = mcp_credential_refs(candidate).map_err(map_mcp_section_error)?;
    intents.extend(current_refs.symmetric_difference(&candidate_refs).cloned());
    Ok(intents)
}

fn normalize_legacy_mcp_headers(
    server_id: &str,
    current: Option<&[bamboo_mcp::HeaderConfig]>,
    candidate: &mut [bamboo_mcp::HeaderConfig],
    intents: &mut BTreeSet<bamboo_config::CredentialRef>,
) -> Result<(), AppError> {
    for header in candidate {
        if header.value_encrypted.is_some() {
            return Err(AppError::BadRequest(
                "MCP ciphertext is server-managed and cannot be supplied".to_string(),
            ));
        }
        let current_header =
            current.and_then(|headers| headers.iter().find(|current| current.name == header.name));
        if header.credential_ref.as_deref().is_some_and(|incoming| {
            current_header.and_then(|current| current.credential_ref.as_deref()) != Some(incoming)
        }) {
            return Err(AppError::BadRequest(
                "MCP credential references are server-managed and cannot be supplied".to_string(),
            ));
        }
        let current_reference = current_header
            .and_then(|current| current.credential_ref.as_ref())
            .map(|raw| {
                bamboo_config::CredentialRef::parse(raw.clone()).map_err(|_| {
                    AppError::BadRequest("MCP credential reference is invalid".to_string())
                })
            })
            .transpose()?;
        let current_value = current_header.map(|current| &current.value);
        if bamboo_config::patch::is_masked_api_key(&header.value) {
            let reference = current_reference.ok_or_else(|| {
                AppError::BadRequest("masked MCP credential has no existing value".to_string())
            })?;
            header.value = current_value.cloned().ok_or_else(|| {
                AppError::BadRequest("referenced MCP credential is unavailable".to_string())
            })?;
            header.credential_ref = Some(reference.as_str().to_string());
        } else if header.value.is_empty() {
            if let Some(reference) = current_reference {
                intents.insert(reference);
            }
            header.credential_ref = None;
        } else {
            let reference = current_reference.map_or_else(
                || {
                    bamboo_config::credential_ref(
                        "mcp",
                        server_id,
                        &format!("header_{}", header.name),
                    )
                    .map_err(map_exact_credential_store_error)
                },
                Ok,
            )?;
            if current_value != Some(&header.value) {
                intents.insert(reference.clone());
            }
            header.credential_ref = Some(reference.as_str().to_string());
        }
        header.value_encrypted = None;
    }
    Ok(())
}

fn map_mcp_section_error(error: ConfigSectionMutationError) -> AppError {
    match error {
        ConfigSectionMutationError::Store(error) => map_exact_credential_store_error(error),
        ConfigSectionMutationError::Invalid(message)
        | ConfigSectionMutationError::Runtime(message) => AppError::BadRequest(message),
    }
}

impl AppState {
    /// Reload the provider based on current configuration
    ///
    /// Re-reads the configuration and creates a new LLM provider
    /// instance, allowing runtime switching of providers or models.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the provider was successfully reloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration cannot be read
    /// - Provider initialization fails (e.g., invalid API key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///
    ///     // User updated config file...
    ///     state.reload_provider().await.expect("Provider reload failed");
    /// }
    /// ```
    pub async fn reload_provider(&self) -> Result<(), bamboo_llm::LLMError> {
        // Every direct provider reload participates in the same generation
        // order as config writers. A caller may await construction, but no
        // later durable/live config generation can commit and then be
        // overwritten by this publication.
        let _io = self.config_io_lock.lock().await;
        let config = self.config.read().await.clone();
        let candidate_registry =
            bamboo_llm::ProviderRegistry::from_config(&config, self.app_data_dir.clone()).await?;
        let default_provider_name = candidate_registry.default_provider_name();
        tracing::info!(
            default_provider = %default_provider_name,
            legacy_provider = %config.provider,
            has_provider_instances = config.has_provider_instances(),
            "Reloading provider runtime from current config"
        );

        let new_provider = candidate_registry.get_default().ok_or_else(|| {
            let message = if config.has_provider_instances() {
                format!(
                    "Default provider instance '{}' is not available or failed to initialize",
                    default_provider_name
                )
            } else {
                format!(
                    "Provider '{}' is not available or failed to initialize",
                    config.provider
                )
            };
            bamboo_llm::LLMError::Auth(message)
        })?;

        #[cfg(test)]
        run_generic_before_provider_publish_test_hook(&self.app_data_dir);
        let mut provider = self.provider.write().await;
        self.provider_registry.replace_with(candidate_registry);
        *provider = new_provider;

        tracing::info!(
            default_provider = %default_provider_name,
            "Provider reloaded successfully"
        );
        Ok(())
    }

    /// Reload the configuration from file
    ///
    /// Reads the configuration file again and updates the in-memory
    /// config. Note: This does NOT automatically reload the provider;
    /// call `reload_provider()` afterwards if needed.
    ///
    /// # Returns
    ///
    /// The newly loaded configuration.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bamboo_server::app_state::AppState;
    /// use std::path::PathBuf;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let state = AppState::new(PathBuf::from("/path/to/.bamboo"))
    ///         .await
    ///         .expect("failed to initialize app state");
    ///
    ///     // Reload config from disk
    ///     let new_config = state.reload_config().await;
    ///
    ///     // Optionally reload provider with new config
    ///     state.reload_provider().await.ok();
    /// }
    /// ```
    pub async fn reload_config(&self) -> Config {
        // The process facade is the only production read authority. Its
        // watcher owns disk reloads, so this compatibility method republishes
        // the current immutable facade view instead of constructing a second
        // disk-reading Config authority.
        let _io = self.config_io_lock.lock().await;
        let mut config = self.config.write().await;
        let mut new_config = self
            .config_facade
            .as_ref()
            .map(|facade| load_facade_effective_config(facade, &self.app_data_dir))
            .unwrap_or_else(|| {
                Config::from_data_dir_without_publish(Some(self.app_data_dir.clone()))
            });
        preserve_runtime_broker(&mut new_config, &config);
        new_config.publish_env_vars();
        *config = new_config.clone();
        new_config
    }

    /// Reload one exact root snapshot and publish its config, provider and MCP
    /// runtimes under a single generation boundary.
    ///
    /// The detached owner retains `config_io_lock` through all publication, so
    /// request cancellation cannot strand a newly installed live config ahead
    /// of provider/MCP convergence and a later writer cannot be overwritten by
    /// effects captured from this reload.
    pub async fn reload_config_and_runtime(&self) -> Result<Config, AppError> {
        let io = self.config_io_lock.clone().lock_owned().await;
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let config = self.config.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let account_sink = self.account_sink.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = io;
            let mut new_config = config_facade
                .as_ref()
                .map(|facade| load_facade_effective_config(facade, &app_data_dir))
                .unwrap_or_else(|| {
                    Config::from_data_dir_without_publish(Some(app_data_dir.clone()))
                });
            {
                let previous = config.read().await;
                preserve_runtime_broker(&mut new_config, &previous);
            }
            if let Err(error) = bamboo_llm::validate_provider_config(&new_config) {
                tracing::warn!("reloaded provider config is invalid");
                let message =
                    "provider configuration is invalid; retaining last-known-good generation"
                        .to_string();
                if let Some(facade) = config_facade.as_ref() {
                    if let Some(event) = facade
                        .registry()
                        .mark_runtime_degraded(SectionId::Providers, message.clone())
                    {
                        publish_registry_event(&account_sink, &event).await;
                    }
                }
                publish_section_failure(
                    &config_live_health,
                    &account_sink,
                    "providers",
                    SectionStatus::Invalid,
                    message,
                )
                .await;
                return Err(AppError::BadRequest(format!(
                    "Invalid configuration: {error}"
                )));
            }
            new_config.publish_env_vars();
            *config.write().await = new_config.clone();
            Self::apply_config_effects_owned(
                new_config.clone(),
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::Strict,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::Strict,
                },
                ConfigRuntimeEffectContext {
                    app_data_dir,
                    config_facade,
                    provider_registry,
                    provider,
                    mcp_manager,
                    account_sink,
                    config_live_health,
                    mcp_config_live_health,
                },
            )
            .await?;
            Ok::<_, AppError>(new_config)
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "config/runtime reload transaction task failed: {error}"
            ))
        })?
    }

    /// Delete the legacy primary config, model-limits, and connect authorities,
    /// then install the resulting default config/provider/MCP generation while
    /// one detached owner retains `config_io_lock`.
    ///
    /// `config.json.bak` remains as the intentionally recoverable low-sensitivity
    /// configuration snapshot. `connect.json.bak` is removed because it can
    /// contain an immediately usable encrypted remote-control credential.
    ///
    /// Once dispatched, request cancellation cannot stop the reset between
    /// file deletion and runtime convergence. Runtime failures are committed
    /// as degraded last-known-good state because the destructive reset itself
    /// has already crossed its durable boundary.
    pub async fn reset_legacy_config_and_runtime(&self) -> Result<Config, AppError> {
        if self.config_facade.is_some() {
            return Err(AppError::BadRequest(
                "full config reset spans multiple revisioned sections and is disabled without a recoverable manifest; reset sections individually through the typed section API"
                    .to_string(),
            ));
        }

        let io = self.config_io_lock.clone().lock_owned().await;
        let app_data_dir = self.app_data_dir.clone();
        let config = self.config.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let account_sink = self.account_sink.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let _io = io;
            let mut deletion_error = None;
            for path in [
                app_data_dir.join("config.json"),
                app_data_dir.join("model_limits.json"),
                app_data_dir.join("connect.json"),
                app_data_dir.join("connect.json.bak"),
            ] {
                let result = match tokio::fs::try_exists(&path).await {
                    Ok(true) => tokio::fs::remove_file(&path).await,
                    Ok(false) => Ok(()),
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    tracing::warn!(
                        file = %path.display(),
                        "failed to delete one legacy config artifact during reset"
                    );
                    deletion_error.get_or_insert(error);
                }
            }
            #[cfg(test)]
            run_reset_after_delete_test_hook(&app_data_dir);

            let mut new_config = Config::from_data_dir_without_publish(Some(app_data_dir.clone()));
            {
                let previous = config.read().await;
                preserve_runtime_broker(&mut new_config, &previous);
            }
            new_config.publish_env_vars();
            *config.write().await = new_config.clone();
            Self::apply_config_effects_owned(
                new_config.clone(),
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::BestEffort,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::BestEffort,
                },
                ConfigRuntimeEffectContext {
                    app_data_dir,
                    config_facade: None,
                    provider_registry,
                    provider,
                    mcp_manager,
                    account_sink,
                    config_live_health,
                    mcp_config_live_health,
                },
            )
            .await?;
            match deletion_error {
                Some(error) => Err(AppError::StorageError(error)),
                None => Ok(new_config),
            }
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "legacy config reset transaction task failed: {error}"
            ))
        })?
    }

    async fn persist_config_snapshot(
        data_dir: PathBuf,
        config_facade: Option<Arc<bamboo_config::ConfigFacade>>,
        config: Config,
    ) -> Result<Option<bamboo_config::FacadeConfigCommit>, AppError> {
        if let Some(facade) = config_facade {
            tokio::task::spawn_blocking(move || {
                let result = bamboo_config::persist_facade_effective_config_with_adoption(
                    &data_dir,
                    &config,
                    facade.as_ref(),
                );
                #[cfg(test)]
                if result.is_ok() {
                    run_generic_before_event_test_hook(&data_dir);
                }
                result
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("Config save task failed: {error}"))
            })?
            .map(Some)
            .map_err(map_exact_credential_store_error)
        } else {
            tokio::task::spawn_blocking(move || {
                let result = config.save_to_dir(data_dir.clone());
                #[cfg(test)]
                if result.is_ok() {
                    run_generic_before_event_test_hook(&data_dir);
                }
                result
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("Config save task failed: {error}"))
            })?
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("Failed to save config: {error}"))
            })?;
            Ok(None)
        }
    }

    /// Unified config update entrypoint.
    ///
    /// Invariants:
    /// - Build and validate a detached candidate
    /// - Persist to disk before publishing it in memory
    /// - Apply runtime side-effects last (provider reload, MCP reconcile)
    pub async fn update_config<F>(
        &self,
        update: F,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError>,
    {
        self.update_config_with_forced_mcp_replacements(update, effects, HashSet::new())
            .await
    }

    pub(crate) async fn update_config_with_forced_mcp_replacements<F>(
        &self,
        update: F,
        effects: ConfigUpdateEffects,
        forced_mcp_replacements: HashSet<String>,
    ) -> Result<Config, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError>,
    {
        // Cancellation while waiting for either guard is pre-commit and leaves
        // every authority untouched. After candidate construction there is no
        // await before the owned guard and candidate enter the detached task.
        let io = self.config_io_lock.clone().lock_owned().await;
        let (mut snapshot, live_base, enforcement_newly_off) = {
            let cfg = self.config.read().await;
            // Refuse the whole operation (no in-memory mutation, no disk
            // write) while a config-corruption recovery is pending
            // confirmation (#153) — `save_to_dir` would reject the persist
            // anyway, but checking here BEFORE `update()` runs keeps the
            // in-memory config frozen exactly at the recovered state.
            reject_if_recovery_pending(&cfg)?;
            let was_off = cfg.plugin_trust.enforcement_is_off();
            let live_base = cfg.clone();
            let mut candidate = cfg.clone();
            restore_authoritative_cluster_fabric(self.config_facade.as_ref(), &mut candidate);
            update(&mut candidate)?;
            // No caller of this compatibility entrypoint owns cluster CAS.
            restore_authoritative_cluster_fabric(self.config_facade.as_ref(), &mut candidate);
            if self.config_facade.is_none() {
                candidate.assign_connect_platform_ids();
                candidate.refresh_encrypted_secrets().map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {e}"
                    ))
                })?;
            }
            let newly_off = !was_off && candidate.plugin_trust.enforcement_is_off();
            (candidate, live_base, newly_off)
        };
        if enforcement_newly_off {
            warn_plugin_trust_enforcement_off();
        }
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        // The detached task owns every step after dispatch. Dropping the
        // request's JoinHandle cannot strand a completed durable/facade commit
        // ahead of live publication, its exact events, or requested effects.
        let transaction = tokio::spawn(async move {
            // Hold the config-IO lock across BOTH the in-memory mutation AND the
            // disk persist, so a concurrent reload_config can't read the disk in
            // the gap before we persist and then clobber this mutation with the
            // stale copy (#126). Effects stay inside the same owned lifetime:
            // otherwise a later writer can publish its runtime and then be
            // overwritten by this task's older async provider/MCP effects.
            let snapshot = {
                let _io = io;
                let commit = Self::persist_config_snapshot(
                    app_data_dir.clone(),
                    config_facade.clone(),
                    snapshot.clone(),
                )
                .await?;
                let events = match commit {
                    Some(commit) => {
                        let mut published = live_base;
                        let events =
                            install_facade_config_commit(commit, &mut published).map_err(|e| {
                                AppError::InternalError(anyhow::anyhow!(
                                    "failed to install committed configuration section: {e}"
                                ))
                            })?;
                        snapshot = published;
                        events
                    }
                    None => Vec::new(),
                };
                {
                    let mut cfg = config.write().await;
                    preserve_runtime_broker(&mut snapshot, &cfg);
                    snapshot.publish_env_vars();
                    *cfg = snapshot.clone();
                }
                // Keep synchronous event enqueue inside the same serialization
                // boundary as durable commit and live installation. Otherwise a
                // later local writer can enqueue r2 before this task enqueues r1.
                publish_exact_facade_events(&account_sink, &events).await?;
                Self::apply_config_effects_owned_after_forcing(
                    snapshot.clone(),
                    effects,
                    ConfigRuntimeEffectContext {
                        app_data_dir,
                        config_facade,
                        provider_registry,
                        provider,
                        mcp_manager,
                        account_sink,
                        config_live_health,
                        mcp_config_live_health,
                    },
                    forced_mcp_replacements,
                )
                .await?;
                snapshot
            };
            Ok::<_, AppError>(snapshot)
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "config update transaction task failed: {error}"
            ))
        })?
    }

    /// Compatibility provider update whose credential and metadata documents
    /// share the recoverable config transaction manifest.
    pub async fn update_config_with_provider_credentials<F>(
        &self,
        update: F,
        provider_intents: std::collections::BTreeSet<String>,
        provider_instance_intents: std::collections::BTreeSet<String>,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError>,
    {
        if provider_intents.is_empty() && provider_instance_intents.is_empty() {
            return self.update_config(update, effects).await;
        }
        let io = self.config_io_lock.clone().lock_owned().await;
        let config_facade = self.config_facade.clone();
        let (mut candidate, live_base, enforcement_newly_off) = {
            let cfg = self.config.read().await;
            reject_if_recovery_pending(&cfg)?;
            let was_off = cfg.plugin_trust.enforcement_is_off();
            let live_base = cfg.clone();
            let mut candidate = cfg.clone();
            restore_authoritative_cluster_fabric(config_facade.as_ref(), &mut candidate);
            update(&mut candidate)?;
            // Provider compatibility updates never own cluster CAS.
            restore_authoritative_cluster_fabric(config_facade.as_ref(), &mut candidate);
            // Provider plaintext may be present until the exact credential
            // transaction assigns its durable reference. Compare every other
            // section against a candidate whose provider projection is
            // replaced with the current durable domain.
            let mut non_provider_candidate = candidate.clone();
            apply_runtime_section(SectionId::Providers, &cfg, &mut non_provider_candidate);
            let mut comparison_base = cfg.clone();
            restore_authoritative_cluster_fabric(config_facade.as_ref(), &mut comparison_base);
            let mut changed =
                bamboo_config::changed_facade_sections(&comparison_base, &non_provider_candidate)
                    .map_err(|_| {
                    AppError::InternalError(anyhow::anyhow!(
                        "failed to compare modular configuration sections"
                    ))
                })?;
            if serde_json::to_value(cfg.subagents()).ok()
                == serde_json::to_value(candidate.subagents()).ok()
            {
                changed.retain(|section| *section != SectionId::Subagents);
            }
            if let Some(other) = changed
                .into_iter()
                .find(|section| *section != SectionId::Providers)
            {
                return Err(AppError::BadRequest(format!(
                    "provider credential updates cannot be combined with {} changes; split the request",
                    other.descriptor().name
                )));
            }
            if config_facade.is_none() {
                candidate.assign_connect_platform_ids();
                candidate.refresh_encrypted_secrets().map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {error}"
                    ))
                })?;
            }
            let newly_off = !was_off && candidate.plugin_trust.enforcement_is_off();
            (candidate, live_base, newly_off)
        };
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            let snapshot = {
                let _io = io;
                let data_dir = app_data_dir.clone();
                let commit_facade = config_facade.clone();
                let (candidate, commit) = tokio::task::spawn_blocking(move || {
                    let result = if let Some(facade) = commit_facade {
                        let commit =
                            bamboo_config::persist_provider_instance_credential_transaction_with_adoption(
                                &data_dir,
                                &mut candidate,
                                &provider_intents,
                                &provider_instance_intents,
                                facade.as_ref(),
                            )?;
                        Ok::<_, ConfigStoreError>((candidate, Some(commit)))
                    } else {
                        bamboo_config::persist_provider_instance_credential_transaction(
                            &data_dir,
                            &mut candidate,
                            &provider_intents,
                            &provider_instance_intents,
                        )?;
                        Ok((load_committed_effective_config(&data_dir)?, None))
                    };
                    #[cfg(test)]
                    run_generic_before_event_test_hook(&data_dir);
                    result
                })
                .await
                .map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "provider credential transaction task failed: {error}"
                    ))
                })?
                .map_err(|error| match error {
                    ConfigStoreError::Conflict { expected, actual } => {
                        AppError::ConfigConflict { expected, actual }
                    }
                    ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                    ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                        anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                    ),
                    ConfigStoreError::Io(error) => AppError::StorageError(error),
                    ConfigStoreError::Json(_) => {
                        AppError::BadRequest("configuration document is invalid".to_string())
                    }
                    ConfigStoreError::Watch(error) => AppError::InternalError(anyhow::anyhow!(
                        "configuration watch failed: {error}"
                    )),
                })?;
                let (mut snapshot, events) = match commit {
                    Some(commit) => {
                        let mut published = live_base;
                        let installed = install_credential_section_commit(commit, &mut published)
                            .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "provider process adoption failed: {error}"
                            ))
                        })?;
                        (published, installed.events)
                    }
                    None => (candidate, Vec::new()),
                };
                {
                    let mut cfg = config.write().await;
                    preserve_runtime_broker(&mut snapshot, &cfg);
                    snapshot.publish_env_vars();
                    *cfg = snapshot.clone();
                }
                publish_exact_facade_events(&account_sink, &events).await?;
                if enforcement_newly_off {
                    warn_plugin_trust_enforcement_off();
                }
                Self::apply_config_effects_owned(
                    snapshot.clone(),
                    effects,
                    ConfigRuntimeEffectContext {
                        app_data_dir,
                        config_facade,
                        provider_registry,
                        provider,
                        mcp_manager,
                        account_sink,
                        config_live_health,
                        mcp_config_live_health,
                    },
                )
                .await?;
                snapshot
            };
            Ok::<_, AppError>(snapshot)
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "provider config transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate user env vars through the recoverable Env-section + credential
    /// exact transaction. The detached task owns the mutation so request
    /// cancellation cannot strand durable metadata ahead of runtime.
    pub async fn update_env_var_credentials<F>(
        &self,
        expected_revision: u64,
        mut env_intents: std::collections::BTreeSet<String>,
        full_replace: bool,
        update: F,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialSectionRuntimeMetadata,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    >
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let config_facade = self.config_facade.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let live_base = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                current.clone()
            };
            let mut candidate = live_base.clone();
            if config_facade.is_some() {
                install_exact_credential_section_mutation_base(
                    app_data_dir.clone(),
                    SectionId::Env,
                    expected_revision,
                    &mut candidate,
                )
                .await?;
            }
            if full_replace {
                env_intents.extend(candidate.env_vars.iter().map(|entry| entry.name.clone()));
            }
            update(&mut candidate)?;
            if config_facade.is_none() {
                candidate.assign_connect_platform_ids();
            }
            let transaction_dir = app_data_dir.clone();
            let commit_facade = config_facade.clone();
            let (candidate, revision, commit) = tokio::task::spawn_blocking(move || {
                if let Some(facade) = commit_facade {
                    let commit =
                        bamboo_config::persist_env_var_credential_transaction_at_revision_with_adoption(
                            &transaction_dir,
                            &mut candidate,
                            &env_intents,
                            expected_revision,
                            facade.as_ref(),
                        )?;
                    #[cfg(test)]
                    run_credential_after_commit_before_live_test_hook(
                        &transaction_dir,
                        SectionId::Env,
                    );
                    let revision = commit.revision;
                    Ok::<_, ConfigStoreError>((candidate, revision, Some(commit)))
                } else {
                    let revision =
                        bamboo_config::persist_env_var_credential_transaction_at_revision(
                            &transaction_dir,
                            &mut candidate,
                            &env_intents,
                            expected_revision,
                        )?;
                    Ok((
                        load_committed_effective_config(&transaction_dir)?,
                        revision,
                        None,
                    ))
                }
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "env credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let (published, installed) = match commit {
                Some(commit) => {
                    let mut published = live_base;
                    let installed = install_credential_section_commit(commit, &mut published)
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "env process adoption failed: {error}"
                            ))
                        })?;
                    (published, installed)
                }
                None => (
                    candidate,
                    InstalledCredentialSectionCommit {
                        events: Vec::new(),
                        metadata: read_credential_runtime_metadata(&app_data_dir).map_err(
                            |error| {
                                AppError::InternalError(anyhow::anyhow!(
                                    "env credential status unavailable after commit: {error}"
                                ))
                            },
                        )?,
                        section: None,
                    },
                ),
            };
            published.publish_env_vars();
            *config.write().await = published.clone();
            publish_exact_facade_events(&account_sink, &installed.events).await?;
            let section = installed.section;
            Ok::<_, AppError>((published, revision, installed.metadata, section))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "env credential transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate notification metadata and ntfy/Bark credentials through the
    /// recoverable Notifications-section + credential exact transaction. The
    /// detached task completes durable commit and live publication even if the
    /// initiating HTTP request is cancelled.
    pub async fn update_notification_credentials<F>(
        &self,
        expected_revision: u64,
        secret_intents: std::collections::BTreeSet<String>,
        reset_domain: bool,
        update: F,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialSectionRuntimeMetadata,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    >
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let config_facade = self.config_facade.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let live_base = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                current.clone()
            };
            let mut candidate = live_base.clone();
            if config_facade.is_some() {
                install_exact_credential_section_mutation_base(
                    app_data_dir.clone(),
                    SectionId::Notifications,
                    expected_revision,
                    &mut candidate,
                )
                .await?;
            }
            update(&mut candidate)?;
            let transaction_dir = app_data_dir.clone();
            let commit_facade = config_facade.clone();
            let (candidate, revision, commit) = tokio::task::spawn_blocking(move || {
                if let Some(facade) = commit_facade {
                    let commit =
                        bamboo_config::persist_notification_credential_transaction_at_revision_with_reset_and_adoption(
                            &transaction_dir,
                            &mut candidate,
                            &secret_intents,
                            reset_domain,
                            expected_revision,
                            facade.as_ref(),
                        )?;
                    let revision = commit.revision;
                    Ok::<_, ConfigStoreError>((candidate, revision, Some(commit)))
                } else {
                    let revision =
                        bamboo_config::persist_notification_credential_transaction_at_revision_with_reset(
                            &transaction_dir,
                            &mut candidate,
                            &secret_intents,
                            reset_domain,
                            expected_revision,
                        )?;
                    Ok((
                        load_committed_effective_config(&transaction_dir)?,
                        revision,
                        None,
                    ))
                }
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "notification credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => {
                    AppError::InternalError(anyhow::anyhow!(
                        "configuration commit outcome is indeterminate: {message}"
                    ))
                }
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let (published, installed) = match commit {
                Some(commit) => {
                    let mut published = live_base;
                    let installed = install_credential_section_commit(commit, &mut published)
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "notification process adoption failed: {error}"
                            ))
                        })?;
                    (published, installed)
                }
                None => (
                    candidate,
                    InstalledCredentialSectionCommit {
                        events: Vec::new(),
                        metadata: read_credential_runtime_metadata(&app_data_dir).map_err(
                            |error| {
                                AppError::InternalError(anyhow::anyhow!(
                                    "notification credential status unavailable after commit: {error}"
                                ))
                            },
                        )?,
                        section: None,
                    },
                ),
            };
            *config.write().await = published.clone();
            publish_exact_facade_events(&account_sink, &installed.events).await?;
            let section = installed.section;
            Ok::<_, AppError>((published, revision, installed.metadata, section))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "notification credential transaction task failed: {error}"
            ))
        })?
    }

    /// Replace connect metadata and explicitly touched platform credentials
    /// through the active connect-section + credential exact transaction.
    /// The detached task owns durable commit and runtime publication so a
    /// cancelled HTTP request cannot leave memory behind the committed pair.
    pub async fn update_connect_credentials<F>(
        &self,
        expected_revision: u64,
        secret_intents: bamboo_config::patch::ConnectSecretIntents,
        update: F,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialSectionRuntimeMetadata,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    >
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let config_facade = self.config_facade.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let live_base = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                current.clone()
            };
            let mut candidate = live_base.clone();
            if config_facade.is_some() {
                install_exact_credential_section_mutation_base(
                    app_data_dir.clone(),
                    SectionId::Connect,
                    expected_revision,
                    &mut candidate,
                )
                .await?;
            }
            update(&mut candidate)?;
            candidate.assign_connect_platform_ids();
            let transaction_dir = app_data_dir.clone();
            let commit_facade = config_facade.clone();
            let (candidate, revision, commit) = tokio::task::spawn_blocking(move || {
                if let Some(facade) = commit_facade {
                    let commit =
                        bamboo_config::persist_connect_credential_transaction_at_revision_with_adoption(
                            &transaction_dir,
                            &mut candidate,
                            &secret_intents,
                            expected_revision,
                            facade.as_ref(),
                        )?;
                    let revision = commit.revision;
                    Ok::<_, ConfigStoreError>((candidate, revision, Some(commit)))
                } else {
                    let revision =
                        bamboo_config::persist_connect_credential_transaction_at_revision(
                            &transaction_dir,
                            &mut candidate,
                            &secret_intents,
                            expected_revision,
                        )?;
                    Ok((
                        load_committed_effective_config(&transaction_dir)?,
                        revision,
                        None,
                    ))
                }
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "connect credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let (published, installed) = match commit {
                Some(commit) => {
                    let mut published = live_base;
                    let installed = install_credential_section_commit(commit, &mut published)
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "connect process adoption failed: {error}"
                            ))
                        })?;
                    (published, installed)
                }
                None => (
                    candidate,
                    InstalledCredentialSectionCommit {
                        events: Vec::new(),
                        metadata: read_credential_runtime_metadata(&app_data_dir).map_err(
                            |error| {
                                AppError::InternalError(anyhow::anyhow!(
                                    "connect credential status unavailable after commit: {error}"
                                ))
                            },
                        )?,
                        section: None,
                    },
                ),
            };
            *config.write().await = published.clone();
            publish_exact_facade_events(&account_sink, &installed.events).await?;
            let section = installed.section;
            Ok::<_, AppError>((published, revision, installed.metadata, section))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "connect credential transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate access-control metadata and verifier records through the active
    /// access-control section + credential exact transaction.
    pub async fn update_access_control_credentials<F>(
        &self,
        expected_revision: u64,
        password_intent: bool,
        device_intents: std::collections::BTreeSet<String>,
        update: F,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialSectionRuntimeMetadata,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    >
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let config_facade = self.config_facade.clone();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let live_base = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                current.clone()
            };
            let mut candidate = live_base.clone();
            if config_facade.is_some() {
                install_exact_credential_section_mutation_base(
                    app_data_dir.clone(),
                    SectionId::AccessControl,
                    expected_revision,
                    &mut candidate,
                )
                .await?;
            }
            update(&mut candidate)?;
            let transaction_dir = app_data_dir.clone();
            let commit_facade = config_facade.clone();
            let (candidate, revision, commit) = tokio::task::spawn_blocking(move || {
                if let Some(facade) = commit_facade {
                    let commit =
                        bamboo_config::persist_access_control_credential_transaction_at_revision_with_adoption(
                        &transaction_dir,
                        &mut candidate,
                        password_intent,
                        &device_intents,
                        expected_revision,
                        facade.as_ref(),
                    )?;
                    let revision = commit.revision;
                    Ok::<_, ConfigStoreError>((candidate, revision, Some(commit)))
                } else {
                    let revision =
                        bamboo_config::persist_access_control_credential_transaction_at_revision(
                            &transaction_dir,
                            &mut candidate,
                            password_intent,
                            &device_intents,
                            expected_revision,
                        )?;
                    Ok((
                        load_committed_effective_config(&transaction_dir)?,
                        revision,
                        None,
                    ))
                }
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "access-control credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let (published, installed) = match commit {
                Some(commit) => {
                    let mut published = live_base;
                    let installed = install_credential_section_commit(commit, &mut published)
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "access-control process adoption failed: {error}"
                            ))
                        })?;
                    (published, installed)
                }
                None => (
                    candidate,
                    InstalledCredentialSectionCommit {
                        events: Vec::new(),
                        metadata: read_credential_runtime_metadata(&app_data_dir).map_err(
                            |error| {
                                AppError::InternalError(anyhow::anyhow!(
                                    "access-control credential status unavailable after commit: {error}"
                                ))
                            },
                        )?,
                        section: None,
                    },
                ),
            };
            *config.write().await = published.clone();
            publish_exact_facade_events(&account_sink, &installed.events).await?;
            let section = installed.section;
            Ok::<_, AppError>((published, revision, installed.metadata, section))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "access-control credential transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate cluster-fabric node metadata and SSH credentials through the
    /// recoverable Cluster-section + credential exact transaction. The
    /// detached task owns durable commit, committed-root reload, and live
    /// publication so request cancellation cannot leave runtime behind disk.
    pub async fn update_cluster_fabric_credentials<F>(
        &self,
        expected_revision: u64,
        node_intents: std::collections::BTreeMap<
            String,
            bamboo_config::ClusterNodeCredentialIntents,
        >,
        update: F,
    ) -> Result<bamboo_server_tools::FabricCommitSnapshot, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        self.update_cluster_fabric_credentials_guarded(
            expected_revision,
            node_intents,
            None,
            update,
        )
        .await
    }

    /// Delete one cluster node only while its lifecycle registry entry is
    /// absent. The guard is evaluated after acquiring `config_io_lock`, the
    /// same lock used by deploy/stop, so the check and durable mutation cannot
    /// race a worker lifecycle transition.
    pub(crate) async fn delete_cluster_node_credentials<F>(
        &self,
        expected_revision: u64,
        node_id: String,
        node_intents: std::collections::BTreeMap<
            String,
            bamboo_config::ClusterNodeCredentialIntents,
        >,
        update: F,
    ) -> Result<bamboo_server_tools::FabricCommitSnapshot, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        self.update_cluster_fabric_credentials_guarded(
            expected_revision,
            node_intents,
            Some(node_id),
            update,
        )
        .await
    }

    async fn update_cluster_fabric_credentials_guarded<F>(
        &self,
        expected_revision: u64,
        node_intents: std::collections::BTreeMap<
            String,
            bamboo_config::ClusterNodeCredentialIntents,
        >,
        required_stopped_node: Option<String>,
        update: F,
    ) -> Result<bamboo_server_tools::FabricCommitSnapshot, AppError>
    where
        F: FnOnce(&mut Config) -> Result<(), AppError> + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let account_sink = self.account_sink.clone();
        let config_facade = self.config_facade.clone();
        let deployed_registry = self.fabric_deployer.registry();
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            if let Some(node_id) = required_stopped_node.as_deref() {
                let deployed = deployed_registry.lock().await;
                if deployed.contains_key(&bamboo_server_tools::registry_keys::node_key(node_id)) {
                    return Err(AppError::BadRequest(format!(
                        "node '{node_id}' is deployed; stop it before deleting it"
                    )));
                }
            }
            let facade = config_facade.as_ref().ok_or_else(|| {
                AppError::BadRequest(
                    "cluster mutations require the modular configuration facade".to_string(),
                )
            })?;
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                current.clone()
            };
            // A process-local runtime can lag a commit from another process.
            // Build the request from the exact durable generation named by the
            // client. `None` deliberately selects the secret-free read: using
            // `Some(expected_revision)` would hydrate first and prevent an
            // explicit Clear/Replace from repairing corrupt ciphertext. The
            // returned status metadata is still crypto-validated. An untouched
            // corrupt active ref retains the established commit-first contract:
            // a metadata change can durably commit and then report its exact
            // runtime materialization error (covered by
            // `changed_cluster_commit_publishes_secret_free_runtime_before_materialization_error`).
            // The compound writer rechecks the same revision before committing,
            // so an intervening winner becomes a conflict.
            let snapshot_dir = app_data_dir.clone();
            let exact = tokio::task::spawn_blocking(move || {
                bamboo_config::read_exact_cluster_fabric_snapshot(&snapshot_dir, None)
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("cluster snapshot task failed: {error}"))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            if exact.section.revision != expected_revision {
                return Err(AppError::ConfigConflict {
                    expected: expected_revision,
                    actual: exact.section.revision,
                });
            }
            if exact.section.status != SectionStatus::Healthy
                || exact.section.source_kind != SectionSourceKind::File
                || exact.credential_health.status == SectionStatus::Degraded
            {
                return Err(AppError::BadRequest(
                    "revision-bound cluster mutations require healthy primary authorities"
                        .to_string(),
                ));
            }
            candidate.cluster_fabric = exact.cluster_fabric;
            update(&mut candidate)?;
            let transaction_dir = app_data_dir.clone();
            let commit_facade = facade.clone();
            let (mut candidate, commit) = tokio::task::spawn_blocking(move || {
                let commit =
                    bamboo_config::persist_cluster_fabric_credential_transaction_with_adoption(
                        &transaction_dir,
                        &mut candidate,
                        &node_intents,
                        expected_revision,
                        commit_facade.as_ref(),
                        |_, _| {
                            #[cfg(test)]
                            run_cluster_after_commit_before_adoption_test_hook(
                                &transaction_dir,
                                expected_revision,
                            );
                        },
                    )?;
                Ok::<_, ConfigStoreError>((candidate, commit))
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "cluster credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let bamboo_config::ClusterFabricTransactionCommit {
                revision,
                adoption,
                credential_adoption,
                committed_recovery,
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
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "cluster configuration at revision {revision} could not materialize its exact runtime credentials: {error}"
                    )));
                }
                Err(error) => {
                    candidate.clear_cluster_runtime_credentials();
                    Err(error)
                }
            };
            *config.write().await = candidate.clone();
            let event = match adoption {
                Some(Ok(event)) => Some(event),
                Some(Err(error)) => {
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "cluster configuration committed at revision {} but process adoption failed: {error}",
                        revision
                    )));
                }
                None if revision == expected_revision => None,
                None => {
                    return Err(AppError::InternalError(anyhow::anyhow!(
                        "cluster configuration committed at revision {} without a process adoption result",
                        revision
                    )));
                }
            };
            let section = facade
                .registry()
                .envelope_value(SectionId::ClusterFabric)
                .map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "committed cluster section envelope is unavailable: {error}"
                    ))
                })?;
            if section.revision != revision {
                return Err(AppError::InternalError(anyhow::anyhow!(
                    "cluster configuration committed at revision {} but facade retained revision {}",
                    revision,
                    section.revision
                )));
            }
            if let Some(event) = event.as_ref() {
                publish_registry_event(&account_sink, event).await;
            }
            if let Err(error) = committed_recovery {
                return Err(AppError::InternalError(anyhow::anyhow!(
                    "cluster configuration committed at revision {revision} but transaction recovery failed: {error}"
                )));
            }
            if let Some(Err(error)) = credential_adoption {
                return Err(AppError::InternalError(anyhow::anyhow!(
                    "cluster configuration committed at revision {revision} but credential facade adoption failed: {error}"
                )));
            }
            let (credential_statuses, credential_health) = runtime.map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "cluster configuration committed at revision {revision} but could not materialize its exact runtime credentials: {error}"
                ))
            })?;
            Ok::<_, AppError>(bamboo_server_tools::FabricCommitSnapshot {
                config: candidate,
                section,
                credential_statuses,
                credential_health,
            })
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "cluster credential transaction task failed: {error}"
            ))
        })?
    }

    /// Persist proxy authentication through the isolated credential store and
    /// publish the detached runtime candidate only after the exact transaction
    /// has durably committed.
    pub async fn update_proxy_auth_credential(
        &self,
        auth: Option<bamboo_config::ProxyAuth>,
        expected_revision: u64,
        effects: ConfigUpdateEffects,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialStatus,
            bamboo_config::CredentialStoreHealth,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    > {
        self.update_core_with_proxy_credential(expected_revision, effects, move |candidate| {
            candidate.proxy_auth = auth;
        })
        .await
    }

    async fn update_core_with_proxy_credential<F>(
        &self,
        expected_revision: u64,
        effects: ConfigUpdateEffects,
        update: F,
    ) -> Result<
        (
            Config,
            u64,
            bamboo_config::CredentialStatus,
            bamboo_config::CredentialStoreHealth,
            Option<bamboo_config::SectionEnvelope<Value>>,
        ),
        AppError,
    >
    where
        F: FnOnce(&mut Config) + Send + 'static,
    {
        let config_io_lock = self.config_io_lock.clone();
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let credential_store = self.credential_store.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();

        // This task owns the mutation after dispatch. Dropping the request's
        // JoinHandle does not cancel it, so the blocking durable transaction,
        // live publication, and runtime convergence complete as one serialized
        // operation even when the caller disconnects.
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let live_base = {
                let cfg = config.read().await;
                reject_if_recovery_pending(&cfg)?;
                cfg.clone()
            };
            let mut candidate = live_base.clone();
            if config_facade.is_some() {
                install_exact_credential_section_mutation_base(
                    app_data_dir.clone(),
                    SectionId::Core,
                    expected_revision,
                    &mut candidate,
                )
                .await?;
            }
            update(&mut candidate);
            if config_facade.is_none() {
                candidate.assign_connect_platform_ids();
                candidate.refresh_encrypted_secrets().map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {error}"
                    ))
                })?;
            }
            let transaction_dir = app_data_dir.clone();
            let status_reference =
                candidate
                    .proxy_auth_credential_ref
                    .clone()
                    .unwrap_or_else(|| {
                        bamboo_config::CredentialRef::parse("proxy.default.auth")
                            .expect("canonical proxy credential reference is valid")
                    });
            let commit_facade = config_facade.clone();
            let (candidate, revision, reference, commit) =
                tokio::task::spawn_blocking(move || {
                if let Some(facade) = commit_facade {
                    let commit =
                        bamboo_config::persist_proxy_auth_credential_transaction_at_revision_with_adoption(
                            &transaction_dir,
                            &mut candidate,
                            expected_revision,
                            facade.as_ref(),
                        )?;
                    let revision = commit.revision;
                    Ok::<_, ConfigStoreError>((
                        candidate,
                        revision,
                        status_reference,
                        Some(commit),
                    ))
                } else {
                    let revision =
                        bamboo_config::persist_proxy_auth_credential_transaction_at_revision(
                        &transaction_dir,
                        &mut candidate,
                        expected_revision,
                    )?;
                    Ok((
                        load_committed_effective_config(&transaction_dir)?,
                        revision,
                        status_reference,
                        None,
                    ))
                }
            })
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "proxy credential transaction task failed: {error}"
                ))
            })?
            .map_err(|error| match error {
                ConfigStoreError::Conflict { expected, actual } => {
                    AppError::ConfigConflict { expected, actual }
                }
                ConfigStoreError::Validation(message) => AppError::BadRequest(message),
                ConfigStoreError::CommitIndeterminate(message) => AppError::InternalError(
                    anyhow::anyhow!("configuration commit outcome is indeterminate: {message}"),
                ),
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            let (published, installed) = match commit {
                Some(commit) => {
                    let mut published = live_base;
                    let installed = install_credential_section_commit(commit, &mut published)
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "proxy process adoption failed: {error}"
                            ))
                        })?;
                    (published, Some(installed))
                }
                None => (candidate, None),
            };
            let section = installed
                .as_ref()
                .and_then(|installed| installed.section.clone());

            // No fallible metadata read occurs before publication. Once the
            // transaction commits, a response error can no longer leave live
            // config behind its durable credential/config pair.
            published.publish_env_vars();
            *config.write().await = published.clone();

            if let Some(installed) = installed.as_ref() {
                publish_exact_facade_events(&account_sink, &installed.events).await?;
            }

            Self::apply_config_effects_owned(
                published.clone(),
                effects,
                ConfigRuntimeEffectContext {
                    app_data_dir,
                    config_facade,
                    provider_registry,
                    provider,
                    mcp_manager,
                    account_sink,
                    config_live_health,
                    mcp_config_live_health,
                },
            )
            .await?;

            let (status, health) = if let Some(installed) = installed {
                (
                    installed.metadata.status(&reference),
                    installed.metadata.credential_health,
                )
            } else {
                credential_store
                    .status_with_health(&reference)
                    .map_err(|error| match error {
                        ConfigStoreError::Conflict { expected, actual } => {
                            AppError::ConfigConflict { expected, actual }
                        }
                        ConfigStoreError::Validation(_)
                        | ConfigStoreError::CommitIndeterminate(_)
                        | ConfigStoreError::Json(_) => AppError::InternalError(anyhow::anyhow!(
                            "credential store validation failed"
                        )),
                        ConfigStoreError::Io(error) => AppError::StorageError(error),
                        ConfigStoreError::Watch(error) => AppError::InternalError(anyhow::anyhow!(
                            "configuration watch failed: {error}"
                        )),
                    })?
            };
            Ok::<_, AppError>((published, revision, status, health, section))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "proxy credential mutation task failed: {error}"
            ))
        })?
    }

    /// Replace the full config (used for JSON merge endpoints).
    pub async fn replace_config(
        &self,
        mut new_config: Config,
        effects: ConfigUpdateEffects,
    ) -> Result<Config, AppError> {
        // Backfill any missing connect.platforms id (#496) up front, before
        // any of the clones below are taken, so the in-memory config, the
        // disk-persisted snapshot, and the value this call returns to the
        // caller (the settings-merge HTTP response) all agree on the same
        // ids — mirrors the `update_config` treatment above.
        if self.config_facade.is_none() {
            new_config.assign_connect_platform_ids();
            // Keep ciphertext in sync with plaintext on legacy layouts. In a
            // modular layout the one owned section transaction is responsible
            // for its credentials and unrelated runtime fields must not move.
            new_config.refresh_encrypted_secrets().map_err(|e| {
                AppError::InternalError(anyhow::anyhow!("Failed to refresh encrypted secrets: {e}"))
            })?;
        }

        let io = self.config_io_lock.clone().lock_owned().await;
        restore_authoritative_cluster_fabric(self.config_facade.as_ref(), &mut new_config);
        let (was_off, live_base) = {
            let cfg = self.config.read().await;
            // Same guard as `update_config` (#153): a full-config replace must
            // not silently blow away an unconfirmed recovery either.
            reject_if_recovery_pending(&cfg)?;
            (cfg.plugin_trust.enforcement_is_off(), cfg.clone())
        };
        let config = self.config.clone();
        let app_data_dir = self.app_data_dir.clone();
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let provider_registry = self.provider_registry.clone();
        let provider = self.provider.clone();
        let mcp_manager = self.mcp_manager.clone();
        let config_live_health = self.config_live_health.clone();
        let mcp_config_live_health = self.mcp_config_live_health.clone();
        let transaction = tokio::spawn(async move {
            // Same #126 serialization as update_config: mutate + persist under
            // the config-IO lock so a reload can't interleave. Provider/MCP
            // effects also remain serialized so an older replacement cannot
            // overwrite a later writer's runtime after an async suspension.
            let new_config = {
                let _io = io;
                let commit = Self::persist_config_snapshot(
                    app_data_dir.clone(),
                    config_facade.clone(),
                    new_config.clone(),
                )
                .await?;
                let mut published = if commit.is_some() {
                    live_base
                } else {
                    new_config
                };
                let events = match commit {
                    Some(commit) => {
                        install_facade_config_commit(commit, &mut published).map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "failed to install committed configuration section: {error}"
                            ))
                        })?
                    }
                    None => Vec::new(),
                };
                let enforcement_newly_off = !was_off && published.plugin_trust.enforcement_is_off();
                {
                    let mut current = config.write().await;
                    preserve_runtime_broker(&mut published, &current);
                    published.publish_env_vars();
                    *current = published.clone();
                }
                // Same live signal as `update_config` — a full-config replace
                // that transitions plugin_trust.enforcement into `Off` warns.
                if enforcement_newly_off {
                    warn_plugin_trust_enforcement_off();
                }
                publish_exact_facade_events(&account_sink, &events).await?;
                Self::apply_config_effects_owned(
                    published.clone(),
                    effects,
                    ConfigRuntimeEffectContext {
                        app_data_dir,
                        config_facade,
                        provider_registry,
                        provider,
                        mcp_manager,
                        account_sink,
                        config_live_health,
                        mcp_config_live_health,
                    },
                )
                .await?;
                published
            };
            Ok::<_, AppError>(new_config)
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "config replacement transaction task failed: {error}"
            ))
        })?
    }

    async fn apply_config_effects_owned(
        new_config: Config,
        effects: ConfigUpdateEffects,
        context: ConfigRuntimeEffectContext,
    ) -> Result<(), AppError> {
        Self::apply_config_effects_owned_after_forcing(new_config, effects, context, HashSet::new())
            .await
    }

    async fn apply_config_effects_owned_after_forcing(
        new_config: Config,
        effects: ConfigUpdateEffects,
        context: ConfigRuntimeEffectContext,
        forced_mcp_replacements: HashSet<String>,
    ) -> Result<(), AppError> {
        let ConfigRuntimeEffectContext {
            app_data_dir,
            config_facade,
            provider_registry,
            provider,
            mcp_manager,
            account_sink,
            config_live_health,
            mcp_config_live_health,
        } = context;
        // The caller owns config_io_lock for this whole method. Build every
        // runtime from `new_config` itself: consulting the mutable live view
        // here would let one durable generation publish another generation's
        // provider or MCP state.
        let mut provider_failure = None;
        if !matches!(
            effects.reload_provider,
            bamboo_config::patch::ReloadMode::None
        ) {
            let candidate = async {
                let candidate_registry =
                    bamboo_llm::ProviderRegistry::from_config(&new_config, app_data_dir.clone())
                        .await?;
                let default_provider_name = candidate_registry.default_provider_name();
                let candidate_provider = candidate_registry.get_default().ok_or_else(|| {
                    let message = if new_config.has_provider_instances() {
                        format!(
                            "Default provider instance '{}' is not available or failed to initialize",
                            default_provider_name
                        )
                    } else {
                        format!(
                            "Provider '{}' is not available or failed to initialize",
                            new_config.provider
                        )
                    };
                    bamboo_llm::LLMError::Auth(message)
                })?;
                Ok::<_, bamboo_llm::LLMError>((
                    candidate_registry,
                    candidate_provider,
                    default_provider_name,
                ))
            }
            .await;

            match candidate {
                Ok((candidate_registry, candidate_provider, default_provider_name)) => {
                    #[cfg(test)]
                    run_generic_before_provider_publish_test_hook(&app_data_dir);
                    {
                        // Never hold the provider guard while acquiring the MCP
                        // reconcile guard below.
                        let mut live_provider = provider.write().await;
                        provider_registry.replace_with(candidate_registry);
                        *live_provider = candidate_provider;
                    }
                    if let Some(facade) = config_facade.as_ref() {
                        let snapshot = facade.registry().providers.snapshot();
                        set_live_health_revision(
                            &config_live_health,
                            snapshot.revision,
                            Some((snapshot.source_path.clone(), snapshot.source_kind)),
                        );
                    } else {
                        update_live_health(
                            &config_live_health,
                            SectionStatus::Healthy,
                            None,
                            true,
                            Some((app_data_dir.join("config.json"), SectionSourceKind::File)),
                        );
                    }
                    tracing::info!(
                        default_provider = %default_provider_name,
                        "Provider reloaded successfully"
                    );
                }
                Err(_) => {
                    tracing::warn!("committed provider generation could not start");
                    let message =
                        "provider runtime initialization failed; retaining last-known-good runtime"
                            .to_string();
                    if let Some(facade) = config_facade.as_ref() {
                        if let Some(event) = facade
                            .registry()
                            .mark_runtime_degraded(SectionId::Providers, message.clone())
                        {
                            let snapshot = facade.registry().providers.snapshot();
                            set_live_health_from_snapshot(&config_live_health, &snapshot);
                            publish_registry_event(&account_sink, &event).await;
                        }
                    } else {
                        publish_section_failure(
                            &config_live_health,
                            &account_sink,
                            "providers",
                            SectionStatus::Degraded,
                            message.clone(),
                        )
                        .await;
                    }
                    if matches!(
                        effects.reload_provider,
                        bamboo_config::patch::ReloadMode::Strict
                    ) {
                        provider_failure = Some(AppError::InternalError(anyhow::anyhow!(message)));
                    }
                }
            }
        }

        let mut mcp_failure = None;
        if !matches!(
            effects.reconcile_mcp,
            bamboo_config::patch::ReloadMode::None
        ) {
            match mcp_manager
                .reconcile_from_config_transactional_after_forcing(
                    &new_config.mcp,
                    &forced_mcp_replacements,
                    || async { Ok(()) },
                )
                .await
            {
                Ok(()) => {
                    if let Some(facade) = config_facade.as_ref() {
                        let snapshot = facade.registry().mcp.snapshot();
                        set_live_health_revision(
                            &mcp_config_live_health,
                            snapshot.revision,
                            Some((snapshot.source_path.clone(), snapshot.source_kind)),
                        );
                    } else {
                        update_live_health(
                            &mcp_config_live_health,
                            SectionStatus::Healthy,
                            None,
                            true,
                            Some((app_data_dir.join("config.json"), SectionSourceKind::File)),
                        );
                    }
                }
                Err(_) => {
                    tracing::warn!("committed MCP generation could not start");
                    let message =
                        "MCP runtime initialization failed; retaining last-known-good runtime"
                            .to_string();
                    if let Some(facade) = config_facade.as_ref() {
                        if let Some(event) = facade
                            .registry()
                            .mark_runtime_degraded(SectionId::Mcp, message.clone())
                        {
                            let snapshot = facade.registry().mcp.snapshot();
                            set_live_health_from_snapshot(&mcp_config_live_health, &snapshot);
                            publish_registry_event(&account_sink, &event).await;
                        }
                    } else {
                        publish_section_failure(
                            &mcp_config_live_health,
                            &account_sink,
                            "mcp",
                            SectionStatus::Degraded,
                            message.clone(),
                        )
                        .await;
                    }
                    if matches!(
                        effects.reconcile_mcp,
                        bamboo_config::patch::ReloadMode::Strict
                    ) {
                        mcp_failure = Some(AppError::InternalError(anyhow::anyhow!(message)));
                    }
                }
            }
        }

        provider_failure.or(mcp_failure).map_or(Ok(()), Err)
    }

    /// Resolve a pending config-corruption recovery (#153; see
    /// [`bamboo_config::ConfigRecoveryStatus`]).
    ///
    /// - `accept = true`: confirms the recovery and persists it to
    ///   `config.json` in the same step ([`Config::confirm_recovery_and_save_to_dir`]),
    ///   then clears the pending flag — the config is no longer "pending
    ///   confirmation" once this returns `Ok`.
    /// - `accept = false`: a no-op that leaves everything untouched — disk,
    ///   in-memory config, and the pending flag are all left exactly as they
    ///   were. `config.json` stays refused-to-write (see `save_to_dir`) until
    ///   either a later `accept = true` call or the user hand-fixes
    ///   `config.json` and the process reloads/restarts.
    ///
    /// Errors with [`AppError::BadRequest`] if there's no pending recovery to
    /// resolve.
    pub async fn confirm_config_recovery(&self, accept: bool) -> Result<Config, AppError> {
        let _io = self.config_io_lock.lock().await;

        if !accept {
            let cfg = self.config.read().await;
            return match cfg.recovery_status() {
                Some(_) => Ok(cfg.clone()),
                None => Err(AppError::BadRequest(
                    "No pending config-corruption recovery to resolve".to_string(),
                )),
            };
        }

        let mut candidate = {
            let cfg = self.config.read().await;
            match cfg.recovery_status() {
                Some(_) => cfg.clone(),
                None => {
                    return Err(AppError::BadRequest(
                        "No pending config-corruption recovery to resolve".to_string(),
                    ))
                }
            }
        };

        let data_dir = self.app_data_dir.clone();
        candidate = tokio::task::spawn_blocking(move || {
            candidate
                .confirm_recovery_and_save_to_dir(data_dir)
                .map(|_| candidate)
        })
        .await
        .map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Config recovery-confirm task failed: {e}"))
        })?
        .map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to save recovered config: {e}"))
        })?;

        {
            let mut cfg = self.config.write().await;
            *cfg = candidate.clone();
            cfg.publish_env_vars();
        }

        Ok(candidate)
    }
}

/// Short-circuit config-mutating entrypoints while a config-corruption
/// recovery is pending confirmation (#153): `save_to_dir` would refuse the
/// disk write anyway, but rejecting here — before any in-memory mutation
/// runs — keeps the in-memory config frozen at exactly the recovered state
/// instead of drifting further out of sync with what's actually on disk.
/// Resolve the pending recovery via `AppState::confirm_config_recovery`
/// first.
fn reject_if_recovery_pending(cfg: &Config) -> Result<(), AppError> {
    if let Some(status) = cfg.recovery_status() {
        if !status.confirmed {
            return Err(AppError::ConfigRecoveryPending(format!(
                "config.json was recovered from corruption ({:?}) and is awaiting \
                 confirmation; confirm or reject the recovery (see /bamboo/config/recovery-status \
                 and /bamboo/config/recovery/confirm) before changing settings",
                status.source
            )));
        }
    }
    Ok(())
}

/// The prominent warning emitted whenever `plugin_trust.enforcement` is (or
/// becomes) `Off` — that setting silently downgrades EVERY subsequent `url`
/// plugin install/update to skip the host allowlist, signature, and
/// checksum-required-by-default layers, with no per-install flag needed (see
/// `crate::plugin_source`'s module docs). Factored into one function so the
/// boot-time signal (`AppState::new`) and the live-apply signal
/// (`update_config`/`replace_config`, covering the HTTP `config set` / PATCH
/// paths) emit the EXACT same message — no drift, and no trigger can flip
/// enforcement off silently.
pub(crate) fn warn_plugin_trust_enforcement_off() {
    tracing::warn!(
        "plugin_trust.enforcement is OFF — plugin installs from ANY URL are accepted \
         without host/signature/checksum verification (config.json plugin_trust.enforcement)"
    );
}

#[cfg(test)]
mod live_reload_tests {
    use super::*;
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{LLMError, LLMStream};
    use bamboo_mcp::{McpServerConfig, ReconnectConfig, StdioConfig};

    struct WorkingProvider;

    fn stop_config_watcher(state: &mut AppState) {
        state.config_watcher.stop.store(true, Ordering::Relaxed);
        if let Some(task) = state.config_watcher.apply_task.take() {
            task.abort();
        }
        if let Some(task) = state.config_watcher.watcher_task.take() {
            task.join().unwrap();
        }
    }

    fn restart_config_watcher(state: &mut AppState) {
        let (runtime, provider_health, mcp_health) = ConfigWatcherRuntime::start(
            state.app_data_dir.clone(),
            state.config.clone(),
            state.config_facade.clone(),
            state.config_io_lock.clone(),
            state.provider_registry.clone(),
            state.provider.clone(),
            state.mcp_manager.clone(),
            state.account_sink.clone(),
        );
        state.config_watcher = runtime;
        state.config_live_health = provider_health;
        state.mcp_config_live_health = mcp_health;
    }

    async fn insert_registry_worker(state: &AppState, key: String, worker_id: &str) {
        #[cfg(unix)]
        let child = tokio::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let child = tokio::process::Command::new("cmd")
            .args(["/C", "timeout", "/T", "30", "/NOBREAK"])
            .spawn()
            .unwrap();
        state.fabric_deployer.registry().lock().await.insert(
            key,
            bamboo_server_tools::Deployed {
                env: "test".to_string(),
                handle: bamboo_broker::DeployedAgent::from_parts(worker_id, child, None),
            },
        );
    }

    fn disabled_mcp_config(id: &str) -> McpConfig {
        McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: id.to_string(),
                name: None,
                enabled: false,
                transport: TransportConfig::Stdio(StdioConfig {
                    command: "unused-disabled-command".to_string(),
                    args: vec![],
                    cwd: None,
                    env: std::collections::HashMap::new(),
                    env_encrypted: std::collections::HashMap::new(),
                    env_credential_refs: std::collections::HashMap::new(),
                    startup_timeout_ms: 100,
                }),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        }
    }

    fn working_stdio_mcp_config(dir: &Path, id: &str, secret: Option<&str>) -> McpConfig {
        let script = dir.join(format!("{id}-mcp-fixture.py"));
        std::fs::write(
            &script,
            r#"import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    if request_id is None:
        continue
    if request.get("method") == "server/discover":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "Method not found"},
        }), flush=True)
        continue
    if request.get("method") == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "config-generation-fixture", "version": "1.0.0"},
        }
    elif request.get("method") == "tools/list":
        result = {"tools": []}
    else:
        result = {}
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
"#,
        )
        .unwrap();
        let python = ["python3", "python"]
            .into_iter()
            .find(|command| {
                std::process::Command::new(command)
                    .arg("--version")
                    .output()
                    .is_ok_and(|output| output.status.success())
            })
            .expect("a Python interpreter is required for the MCP ordering fixture");
        let mut env = std::collections::HashMap::new();
        if let Some(secret) = secret {
            env.insert("TOKEN".to_string(), secret.to_string());
        }
        McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: id.to_string(),
                name: None,
                enabled: true,
                transport: TransportConfig::Stdio(StdioConfig {
                    command: python.to_string(),
                    args: vec![script.to_string_lossy().into_owned()],
                    cwd: None,
                    env,
                    env_encrypted: std::collections::HashMap::new(),
                    env_credential_refs: std::collections::HashMap::new(),
                    startup_timeout_ms: 2_000,
                }),
                request_timeout_ms: 2_000,
                healthcheck_interval_ms: 10_000,
                reconnect: ReconnectConfig {
                    enabled: false,
                    ..Default::default()
                },
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        }
    }

    #[tokio::test]
    async fn config_update_preserves_runtime_broker() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let expected = state
            .config
            .read()
            .await
            .subagents()
            .broker
            .clone()
            .expect("AppState embeds a runtime broker");

        let updated = state
            .update_config(
                |config| {
                    config.subagents_mut().max_concurrent = Some(3);
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();

        assert_eq!(updated.subagents().broker.as_ref(), Some(&expected));
        assert_eq!(
            state.config.read().await.subagents().broker.as_ref(),
            Some(&expected)
        );
    }

    #[test]
    fn preserve_runtime_broker_keeps_explicit_broker() {
        let previous_broker = bamboo_config::BrokerClientConfig {
            endpoint: "ws://127.0.0.1:41001".to_string(),
            token: "previous".to_string(),
            token_encrypted: None,
            credential_ref: None,
            configured: false,
        };
        let explicit_broker = bamboo_config::BrokerClientConfig {
            endpoint: "wss://broker.example.test".to_string(),
            token: "explicit".to_string(),
            token_encrypted: None,
            credential_ref: None,
            configured: true,
        };
        let mut previous = Config::default();
        previous.subagents_mut().broker = Some(previous_broker);
        let mut incoming = Config::default();
        incoming.subagents_mut().broker = Some(explicit_broker.clone());

        preserve_runtime_broker(&mut incoming, &previous);

        assert_eq!(incoming.subagents().broker.as_ref(), Some(&explicit_broker));
    }

    fn mcp_document_bytes(revision: u64, config: &McpConfig) -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "revision": revision,
            "data": config,
        }))
        .unwrap()
    }

    #[test]
    fn legacy_mcp_rejects_client_owned_stdio_and_header_credential_refs() {
        let mut stdio_current = disabled_mcp_config("stdio-server");
        let stdio_reference =
            bamboo_config::credential_ref("mcp", "stdio-server", "env_TOKEN").unwrap();
        let TransportConfig::Stdio(stdio) = &mut stdio_current.servers[0].transport else {
            unreachable!()
        };
        stdio
            .env
            .insert("TOKEN".to_string(), "existing-secret".to_string());
        stdio
            .env_credential_refs
            .insert("TOKEN".to_string(), stdio_reference.as_str().to_string());
        let mut stdio_candidate = stdio_current.clone();
        let TransportConfig::Stdio(stdio) = &mut stdio_candidate.servers[0].transport else {
            unreachable!()
        };
        stdio
            .env_credential_refs
            .insert("TOKEN".to_string(), "mcp.foreign.env_token".to_string());
        let error = normalize_legacy_mcp_credentials(&stdio_current, &mut stdio_candidate)
            .expect_err("an arbitrary stdio credential ref must be rejected");
        assert!(matches!(
            error,
            AppError::BadRequest(message)
                if message == "MCP credential references are server-managed and cannot be supplied"
        ));

        let header_reference =
            bamboo_config::credential_ref("mcp", "http-server", "header_Authorization").unwrap();
        let http_current = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "http-server".to_string(),
                name: None,
                enabled: false,
                transport: TransportConfig::Sse(bamboo_mcp::SseConfig {
                    url: "https://example.test/sse".to_string(),
                    headers: vec![bamboo_mcp::HeaderConfig {
                        name: "Authorization".to_string(),
                        value: "existing-secret".to_string(),
                        value_encrypted: None,
                        credential_ref: Some(header_reference.as_str().to_string()),
                    }],
                    connect_timeout_ms: 100,
                }),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };
        let mut http_candidate = http_current.clone();
        let TransportConfig::Sse(http) = &mut http_candidate.servers[0].transport else {
            unreachable!()
        };
        http.headers[0].credential_ref = Some("mcp.foreign.header_authorization".to_string());
        let error = normalize_legacy_mcp_credentials(&http_current, &mut http_candidate)
            .expect_err("an arbitrary header credential ref must be rejected");
        assert!(matches!(
            error,
            AppError::BadRequest(message)
                if message == "MCP credential references are server-managed and cannot be supplied"
        ));
    }

    #[test]
    fn touched_shared_mcp_refs_stage_replacements_and_preserve_surviving_clears() {
        let shared =
            bamboo_config::CredentialRef::parse("mcp.shared.env_token".to_string()).unwrap();
        let mut current = disabled_mcp_config("first");
        let mut second = current.servers[0].clone();
        second.id = "second".to_string();
        current.servers.push(second);
        for server in &mut current.servers {
            let TransportConfig::Stdio(stdio) = &mut server.transport else {
                unreachable!()
            };
            stdio
                .env
                .insert("TOKEN".to_string(), "old-shared-secret".to_string());
            stdio
                .env_credential_refs
                .insert("TOKEN".to_string(), shared.as_str().to_string());
        }
        let touched = BTreeSet::from([shared]);

        let mut replace = current.clone();
        for server in &mut replace.servers {
            let TransportConfig::Stdio(stdio) = &mut server.transport else {
                unreachable!()
            };
            stdio.env.get_mut("TOKEN").unwrap().clear();
        }
        let TransportConfig::Stdio(first) = &mut replace.servers[0].transport else {
            unreachable!()
        };
        first
            .env
            .insert("TOKEN".to_string(), "new-shared-secret".to_string());
        materialize_mcp_touched_replacements(&mut replace, &touched).unwrap();
        retain_mcp_credentials(&current, &mut replace, &touched);
        for server in &replace.servers {
            let TransportConfig::Stdio(stdio) = &server.transport else {
                unreachable!()
            };
            assert_eq!(stdio.env["TOKEN"], "new-shared-secret");
        }

        let mut clear_one = current.clone();
        for server in &mut clear_one.servers {
            let TransportConfig::Stdio(stdio) = &mut server.transport else {
                unreachable!()
            };
            stdio.env.get_mut("TOKEN").unwrap().clear();
        }
        let TransportConfig::Stdio(first) = &mut clear_one.servers[0].transport else {
            unreachable!()
        };
        first.env.remove("TOKEN");
        first.env_credential_refs.remove("TOKEN");
        materialize_mcp_touched_replacements(&mut clear_one, &touched).unwrap();
        retain_mcp_credentials(&current, &mut clear_one, &touched);
        let TransportConfig::Stdio(first) = &clear_one.servers[0].transport else {
            unreachable!()
        };
        assert!(!first.env.contains_key("TOKEN"));
        assert!(!first.env_credential_refs.contains_key("TOKEN"));
        let TransportConfig::Stdio(second) = &clear_one.servers[1].transport else {
            unreachable!()
        };
        assert_eq!(second.env["TOKEN"], "old-shared-secret");
        assert_eq!(second.env_credential_refs["TOKEN"], "mcp.shared.env_token");

        let header_ref =
            bamboo_config::CredentialRef::parse("mcp.shared.header_token".to_string()).unwrap();
        let current_http = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "http".to_string(),
                name: None,
                enabled: false,
                transport: TransportConfig::Sse(bamboo_mcp::SseConfig {
                    url: "https://example.test/sse".to_string(),
                    headers: vec![bamboo_mcp::HeaderConfig {
                        name: "Authorization".to_string(),
                        value: "old-header-secret".to_string(),
                        value_encrypted: None,
                        credential_ref: Some(header_ref.as_str().to_string()),
                    }],
                    connect_timeout_ms: 100,
                }),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };
        let mut delete_all_headers = current_http.clone();
        let TransportConfig::Sse(candidate) = &mut delete_all_headers.servers[0].transport else {
            unreachable!()
        };
        candidate.headers.clear();
        let touched = BTreeSet::from([header_ref]);
        materialize_mcp_touched_replacements(&mut delete_all_headers, &touched).unwrap();
        retain_mcp_credentials(&current_http, &mut delete_all_headers, &touched);
        let TransportConfig::Sse(candidate) = &delete_all_headers.servers[0].transport else {
            unreachable!()
        };
        assert!(candidate.headers.is_empty());
    }

    fn install_unrecoverable_pending_provider_migration(dir: &Path) {
        let transaction_id = uuid::Uuid::new_v4().to_string();
        std::fs::write(
            dir.join("config.json"),
            br#"{"providers":{"openai":{"model":"root-lkg"}}}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("providers.json"),
            br#"{"schema_version":1,"revision":2,"data":{"openai":{"model":"partial-must-not-load","credential_ref":"provider.openai.api_key"}}}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("config-credential-migration.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "transaction_id": transaction_id.clone(),
                "stage_dir": format!(".config-credential-stage-v1-{transaction_id}"),
                "state": "pending",
                "files": [
                    {
                        "name": "credentials.json",
                        "staged_name": "credentials.json",
                        "sha256": "0".repeat(64),
                        "sensitive": true
                    },
                    {
                        "name": "providers.json",
                        "staged_name": "providers.json",
                        "sha256": "1".repeat(64),
                        "original_sha256": "2".repeat(64),
                        "migration_generation": 2,
                        "sensitive": false
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
    }

    async fn wait_for_mcp_health(
        state: &AppState,
        status: SectionStatus,
        minimum_revision: u64,
    ) -> ConfigLiveHealth {
        match tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let health = state
                    .mcp_config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if health.status == status && health.revision >= minimum_revision {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        {
            Ok(health) => health,
            Err(_) => panic!(
                "MCP health transition timed out: {:?}",
                state
                    .mcp_config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            ),
        }
    }

    async fn next_config_event(
        feed: &mut tokio::sync::broadcast::Receiver<Arc<bamboo_engine::events::ChangeEvent>>,
        expected_section: &str,
    ) -> AgentEvent {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let envelope = feed.recv().await.expect("account feed remains open");
                match &envelope.event {
                    AgentEvent::ConfigChanged { section, .. }
                    | AgentEvent::ConfigInvalid { section, .. }
                    | AgentEvent::ConfigRecovered { section, .. }
                        if section == expected_section =>
                    {
                        break envelope.event.clone();
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("config event timed out")
    }

    async fn next_mcp_config_event(
        feed: &mut tokio::sync::broadcast::Receiver<Arc<bamboo_engine::events::ChangeEvent>>,
    ) -> AgentEvent {
        next_config_event(feed, "mcp").await
    }

    async fn wait_for_root_outbox_to_clear(data_dir: &Path) {
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if !bamboo_config::has_pending_legacy_root_publications(data_dir).unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("legacy root outbox did not clear");
    }

    #[tokio::test]
    async fn compatibility_update_cannot_reintroduce_an_unrevisioned_cluster_mutation() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x70; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                std::collections::BTreeMap::from([(
                    "owned-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "owned-node".to_string(),
                        label: "revisioned-label".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        let cluster_path = dir.path().join("cluster-fabric.json");
        let cluster_before = std::fs::read(&cluster_path).unwrap();

        let updated = state
            .update_config(
                |config| {
                    config.server.port = 21_000;
                    config.cluster_fabric.node_mut("owned-node").unwrap().label =
                        "unrevisioned-label".to_string();
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();

        assert_eq!(updated.server.port, 21_000);
        assert_eq!(
            updated.cluster_fabric.node("owned-node").unwrap().label,
            "revisioned-label"
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert_eq!(std::fs::read(cluster_path).unwrap(), cluster_before);
    }

    #[tokio::test]
    async fn stopped_watcher_compatibility_writers_install_only_their_owned_section() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x72; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "shared-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "shared-node".to_string(),
                        label: "generation-one".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .node_mut("shared-node")
            .unwrap()
            .label = "external-generation-two".to_string();
        assert_eq!(
            bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external_candidate,
                &BTreeMap::new(),
                1,
            )
            .unwrap(),
            2
        );
        let cluster_path = dir.path().join("cluster-fabric.json");
        let cluster_r2 = std::fs::read(&cluster_path).unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("shared-node")
                .unwrap()
                .label,
            "generation-one"
        );

        let baseline_seq = state.account_sink.latest_seq();
        let mut core_feed = state.account_sink.subscribe();
        let mut cluster_feed = state.account_sink.subscribe();
        let stale_runtime = state.config.read().await;
        let updating = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        |config| {
                            config.server.port = 23_332;
                            Ok(())
                        },
                        ConfigUpdateEffects::default(),
                    )
                    .await
            })
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                next_config_event(&mut core_feed, "core"),
            )
            .await
            .is_err(),
            "core event became observable while the old AppState snapshot was held"
        );
        assert_ne!(stale_runtime.server.port, 23_332);
        drop(stale_runtime);
        let published = updating.await.unwrap().unwrap();
        assert!(matches!(
            next_config_event(&mut core_feed, "core").await,
            AgentEvent::ConfigChanged { section, .. } if section == "core"
        ));
        assert_eq!(state.config.read().await.server.port, 23_332);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(300),
                next_config_event(&mut cluster_feed, "cluster-fabric"),
            )
            .await
            .is_err(),
            "an unrelated compatibility update published a cluster event"
        );
        assert_eq!(
            published.cluster_fabric.node("shared-node").unwrap().label,
            "generation-one"
        );
        assert_eq!(std::fs::read(&cluster_path).unwrap(), cluster_r2);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1,
            "an unrelated compatibility update must not catch up cluster"
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("shared-node")
                .unwrap()
                .label,
            "generation-one"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, .. } if section == "core"
                ))
                .count(),
            1
        );
        assert!(!events.iter().any(|event| matches!(
            &event.event,
            AgentEvent::ConfigChanged { section, .. } if section == "cluster-fabric"
        )));

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .node_mut("shared-node")
            .unwrap()
            .label = "external-generation-three".to_string();
        assert_eq!(
            bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external_candidate,
                &BTreeMap::new(),
                2,
            )
            .unwrap(),
            3
        );
        let cluster_r3 = std::fs::read(&cluster_path).unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1,
            "the stopped watcher must remain stale before replace_config"
        );

        let mut replacement = state.config.read().await.clone();
        replacement.server.port = 23_333;
        let baseline_seq = state.account_sink.latest_seq();
        let mut core_feed = state.account_sink.subscribe();
        let mut cluster_feed = state.account_sink.subscribe();
        let stale_runtime = state.config.read().await;
        let replacing = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .replace_config(replacement, ConfigUpdateEffects::default())
                    .await
            })
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                next_config_event(&mut core_feed, "core"),
            )
            .await
            .is_err(),
            "replacement event became observable while the old AppState snapshot was held"
        );
        assert_ne!(stale_runtime.server.port, 23_333);
        drop(stale_runtime);
        let published = replacing.await.unwrap().unwrap();
        assert!(matches!(
            next_config_event(&mut core_feed, "core").await,
            AgentEvent::ConfigChanged { section, .. } if section == "core"
        ));
        assert_eq!(state.config.read().await.server.port, 23_333);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(300),
                next_config_event(&mut cluster_feed, "cluster-fabric"),
            )
            .await
            .is_err(),
            "an unrelated compatibility replacement published a cluster event"
        );
        assert_eq!(published.server.port, 23_333);
        assert_eq!(
            published.cluster_fabric.node("shared-node").unwrap().label,
            "generation-one"
        );
        assert_eq!(std::fs::read(&cluster_path).unwrap(), cluster_r3);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("shared-node")
                .unwrap()
                .label,
            "generation-one"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, .. } if section == "core"
                ))
                .count(),
            1
        );
        assert!(!events.iter().any(|event| matches!(
            &event.event,
            AgentEvent::ConfigChanged { section, .. } if section == "cluster-fabric"
        )));
    }

    #[tokio::test]
    async fn exact_notification_publication_installs_only_its_owned_runtime_section() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        {
            let mut live = state.config.write().await;
            live.connect
                .platforms
                .push(bamboo_config::ConnectPlatformConfig {
                    id: None,
                    project_id: None,
                    platform_type: "runtime-sentinel".to_string(),
                    token: None,
                    token_encrypted: None,
                    token_credential_ref: None,
                    token_configured: false,
                    app_id: None,
                    app_secret: None,
                    app_secret_encrypted: None,
                    app_secret_credential_ref: None,
                    app_secret_configured: false,
                    domain: None,
                    allow_from: Vec::new(),
                    admin_from: Vec::new(),
                });
            live.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                api_key: "runtime-provider-sentinel".to_string(),
                ..Default::default()
            });
        }
        let connect_before = std::fs::read(dir.path().join("connect.json")).unwrap();
        let (published, revision, _, section) = state
            .update_notification_credentials(0, BTreeSet::new(), false, |candidate| {
                candidate.notifications.ntfy.enabled = true;
                candidate.notifications.ntfy.topic = "owned-notification".to_string();
                // Deliberately perturb unrelated runtime-only state. Modular
                // publication must discard both changes after committing only
                // the Notifications section.
                candidate.assign_connect_platform_ids();
                candidate
                    .providers_mut()
                    .openai
                    .as_mut()
                    .unwrap()
                    .api_key
                    .clear();
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(revision, 1);
        assert_eq!(section.unwrap().revision, 1);
        assert_eq!(published.notifications.ntfy.topic, "owned-notification");
        assert!(published.connect.platforms[0].id.is_none());
        assert_eq!(
            published.providers().openai.as_ref().unwrap().api_key,
            "runtime-provider-sentinel"
        );
        let live = state.config.read().await;
        assert!(live.connect.platforms[0].id.is_none());
        assert_eq!(
            live.providers().openai.as_ref().unwrap().api_key,
            "runtime-provider-sentinel"
        );
        drop(live);
        assert_eq!(
            std::fs::read(dir.path().join("connect.json")).unwrap(),
            connect_before
        );
        assert_eq!(
            bamboo_config::ConfigFacade::open(dir.path())
                .unwrap()
                .registry()
                .connect
                .snapshot()
                .revision,
            0
        );
    }

    #[tokio::test]
    async fn generic_update_cannot_forge_exact_core_credential_binding() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let core_before = std::fs::read(dir.path().join("core.json")).unwrap();
        let error = state
            .update_config(
                |candidate| {
                    candidate.proxy_auth_credential_ref =
                        Some(bamboo_config::CredentialRef::parse("proxy.default.auth").unwrap());
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
        assert!(error.to_string().contains("credential bindings"));
        assert_eq!(
            std::fs::read(dir.path().join("core.json")).unwrap(),
            core_before
        );
        assert!(state
            .config
            .read()
            .await
            .proxy_auth_credential_ref
            .is_none());
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .revision,
            0
        );
    }

    #[tokio::test]
    async fn env_credential_commit_installs_owned_runtime_before_exact_events() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x73; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "shared-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "shared-node".to_string(),
                        label: "generation-one".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .node_mut("shared-node")
            .unwrap()
            .label = "external-generation-two".to_string();
        bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut external_candidate,
            &BTreeMap::new(),
            1,
        )
        .unwrap();
        let cluster_path = dir.path().join("cluster-fabric.json");
        let cluster_r2 = std::fs::read(&cluster_path).unwrap();
        let expected_revision = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .env
            .snapshot()
            .revision;

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        set_credential_after_commit_before_live_test_hook(dir.path(), SectionId::Env, move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let baseline_seq = state.account_sink.latest_seq();
        let mut credential_feed = state.account_sink.subscribe();
        let mut env_feed = state.account_sink.subscribe();
        let mut cluster_feed = state.account_sink.subscribe();
        let updating = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_env_var_credentials(
                        expected_revision,
                        BTreeSet::from(["TOKEN".to_string()]),
                        false,
                        |config| {
                            config.env_vars.push(bamboo_config::EnvVarEntry {
                                name: "TOKEN".to_string(),
                                value: "exact-secret".to_string(),
                                secret: true,
                                value_encrypted: None,
                                credential_ref: None,
                                configured: true,
                                description: None,
                            });
                            Ok(())
                        },
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();
        let stale_runtime = state.config.read().await;
        release_tx.send(()).unwrap();
        for (feed, section) in [
            (&mut credential_feed, "credentials"),
            (&mut env_feed, "env"),
            (&mut cluster_feed, "cluster-fabric"),
        ] {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), next_config_event(feed, section),)
                    .await
                    .is_err(),
                "{section} event became observable before the owned runtime install"
            );
        }
        assert!(
            stale_runtime
                .env_vars
                .iter()
                .all(|entry| entry.name != "TOKEN"),
            "the held runtime must still be the pre-commit env generation"
        );
        assert_eq!(
            stale_runtime
                .cluster_fabric
                .node("shared-node")
                .unwrap()
                .label,
            "generation-one"
        );
        drop(stale_runtime);

        let (published, revision, _, _) = updating.await.unwrap().unwrap();
        assert!(revision > expected_revision);
        assert!(published
            .env_vars
            .iter()
            .any(|entry| entry.name == "TOKEN" && entry.value == "exact-secret"));
        assert_eq!(
            published.cluster_fabric.node("shared-node").unwrap().label,
            "generation-one"
        );
        assert!(matches!(
            next_config_event(&mut credential_feed, "credentials").await,
            AgentEvent::ConfigChanged { section, .. } if section == "credentials"
        ));
        assert!(matches!(
            next_config_event(&mut env_feed, "env").await,
            AgentEvent::ConfigChanged { section, .. } if section == "env"
        ));
        assert!(tokio::time::timeout(
            Duration::from_millis(300),
            next_config_event(&mut cluster_feed, "cluster-fabric"),
        )
        .await
        .is_err());
        assert_eq!(std::fs::read(cluster_path).unwrap(), cluster_r2);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, .. } if section == "credentials"
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, .. } if section == "env"
                ))
                .count(),
            1
        );
        assert!(!events.iter().any(|event| matches!(
            &event.event,
            AgentEvent::ConfigChanged { section, .. } if section == "cluster-fabric"
        )));
    }

    #[tokio::test]
    async fn env_mutation_returns_its_captured_envelope_after_a_later_section_commit() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x74; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        set_credential_after_commit_before_live_test_hook(dir.path(), SectionId::Env, move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let updating = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_env_var_credentials(
                        0,
                        BTreeSet::from(["TOKEN".to_string()]),
                        false,
                        |config| {
                            config.env_vars.push(bamboo_config::EnvVarEntry {
                                name: "TOKEN".to_string(),
                                value: "first-secret".to_string(),
                                secret: true,
                                value_encrypted: None,
                                credential_ref: None,
                                configured: true,
                                description: Some("first generation".to_string()),
                            });
                            Ok(())
                        },
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();

        let external_dir = dir.path().to_path_buf();
        let process_facade = state.config_facade.clone().unwrap();
        let later = tokio::task::spawn_blocking(move || {
            let external = bamboo_config::ConfigFacade::open(&external_dir).unwrap();
            let mut candidate = external.effective_config();
            candidate.env_vars[0].description = Some("later generation".to_string());
            bamboo_config::persist_env_var_credential_transaction_at_revision_with_adoption(
                &external_dir,
                &mut candidate,
                &BTreeSet::from(["TOKEN".to_string()]),
                1,
                process_facade.as_ref(),
            )
            .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(later.revision, 2);
        assert_eq!(later.section.unwrap().revision, 2);
        release_tx.send(()).unwrap();

        let (_, revision, _, section) = updating.await.unwrap().unwrap();
        let section = section.expect("modular mutation returns its exact section");
        assert_eq!(revision, 1);
        assert_eq!(section.revision, 1);
        assert_eq!(section.data[0]["description"], "first generation");
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .env
                .snapshot()
                .revision,
            2,
            "the process facade advanced, but the response retained its own commit"
        );
    }

    #[tokio::test]
    async fn cluster_commit_installs_runtime_before_one_authoritative_event() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x71; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let revision = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .cluster_fabric
            .snapshot()
            .revision;
        let baseline_seq = state.account_sink.latest_seq();
        let mut feed = state.account_sink.subscribe();
        let runtime = state.config.clone();
        let observer = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(3), async move {
                loop {
                    let event = feed.recv().await.unwrap();
                    match &event.event {
                        AgentEvent::ConfigChanged { section, .. } if section == "credentials" => {
                            panic!("cluster mutation published an internal credential event")
                        }
                        AgentEvent::ConfigChanged { section, revision }
                            if section == "cluster-fabric" =>
                        {
                            assert!(
                                runtime
                                    .read()
                                    .await
                                    .cluster_fabric
                                    .node("event-node")
                                    .is_some(),
                                "event observer saw the old runtime snapshot"
                            );
                            return *revision;
                        }
                        _ => {}
                    }
                }
            })
            .await
            .expect("cluster event timed out")
        });

        let node = bamboo_config::Node {
            id: "event-node".to_string(),
            label: "event-node".to_string(),
            placement: bamboo_config::NodePlacement::Local,
            trust_level: bamboo_config::TrustLevel::Trusted,
            deploy: bamboo_config::DeployProfile::default(),
            state: None,
            enabled: true,
        };
        let committed = state
            .update_cluster_fabric_credentials(
                revision,
                BTreeMap::from([(
                    "event-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                move |config| {
                    config.cluster_fabric.nodes.push(node);
                    Ok(())
                },
            )
            .await
            .unwrap();
        let committed = committed.section.revision;
        assert_eq!(committed, revision + 1);
        assert_eq!(observer.await.unwrap(), committed);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let cluster_events = events
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, revision: event_revision }
                        if section == "cluster-fabric" && *event_revision == committed
                )
            })
            .count();
        let credential_events = events
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, .. } if section == "credentials"
                )
            })
            .count();
        assert_eq!(cluster_events, 1);
        assert_eq!(credential_events, 0);
    }

    #[tokio::test]
    async fn stale_process_cluster_candidate_rebases_on_exact_durable_client_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "shared-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "shared-node".to_string(),
                        label: "generation-one".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        stop_config_watcher(&mut state);

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .clusters
            .push(bamboo_config::Cluster {
                name: "external-cluster".to_string(),
                description: Some("durable-r2-field".to_string()),
                node_ids: vec!["shared-node".to_string()],
            });
        assert_eq!(
            bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external_candidate,
                &BTreeMap::new(),
                1,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .cluster("external-cluster")
                .is_none(),
            "the process runtime is intentionally stale at r1"
        );

        let committed = state
            .update_cluster_fabric_credentials(2, BTreeMap::new(), |config| {
                config.cluster_fabric.node_mut("shared-node").unwrap().label =
                    "client-r3-edit".to_string();
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(committed.section.revision, 3);
        assert_eq!(
            committed
                .config
                .cluster_fabric
                .node("shared-node")
                .unwrap()
                .label,
            "client-r3-edit"
        );
        assert_eq!(
            committed
                .config
                .cluster_fabric
                .cluster("external-cluster")
                .unwrap()
                .description
                .as_deref(),
            Some("durable-r2-field")
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            3,
            "compound adoption must safely catch the stale r1 facade up to r3"
        );

        let runtime_before_conflict = state.config.read().await.cluster_fabric.clone();
        let conflict = state
            .update_cluster_fabric_credentials(2, BTreeMap::new(), |config| {
                config.cluster_fabric.nodes.clear();
                config.cluster_fabric.clusters.clear();
                Ok(())
            })
            .await;
        assert!(matches!(
            conflict,
            Err(AppError::ConfigConflict {
                expected: 2,
                actual: 3
            })
        ));
        assert_eq!(
            state.config.read().await.cluster_fabric,
            runtime_before_conflict,
            "a durable CAS conflict must not overwrite the process runtime"
        );
    }

    #[tokio::test]
    async fn stale_process_cluster_noop_catches_up_exact_durable_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .nodes
            .push(bamboo_config::Node {
                id: "external-node".to_string(),
                label: "external-r1".to_string(),
                placement: bamboo_config::NodePlacement::Local,
                trust_level: bamboo_config::TrustLevel::Trusted,
                deploy: bamboo_config::DeployProfile::default(),
                state: None,
                enabled: true,
            });
        assert_eq!(
            bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external_candidate,
                &BTreeMap::from([(
                    "external-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                0,
            )
            .unwrap(),
            1
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            0
        );
        let baseline_seq = state.account_sink.latest_seq();

        let committed = state
            .update_cluster_fabric_credentials(1, BTreeMap::new(), |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(committed.section.revision, 1);
        assert_eq!(
            committed
                .config
                .cluster_fabric
                .node("external-node")
                .unwrap()
                .label,
            "external-r1"
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("external-node")
                .unwrap()
                .label,
            "external-r1"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let revisions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "cluster-fabric" => {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(revisions, vec![1]);
    }

    #[tokio::test]
    async fn stale_process_cluster_reset_noop_catches_up_exact_durable_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);

        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut external_candidate = external.effective_config();
        external_candidate
            .cluster_fabric
            .nodes
            .push(bamboo_config::Node {
                id: "reset-node".to_string(),
                label: "reset-node".to_string(),
                placement: bamboo_config::NodePlacement::Local,
                trust_level: bamboo_config::TrustLevel::Trusted,
                deploy: bamboo_config::DeployProfile::default(),
                state: None,
                enabled: true,
            });
        assert_eq!(
            bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external_candidate,
                &BTreeMap::from([(
                    "reset-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                0,
            )
            .unwrap(),
            1
        );
        let reset_facade = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let mut reset_candidate = reset_facade.effective_config();
        reset_candidate.cluster_fabric = bamboo_config::ClusterFabricConfig::default();
        let external_reset = bamboo_config::persist_cluster_fabric_reset_at_revision_with_adoption(
            dir.path(),
            &mut reset_candidate,
            1,
            &reset_facade,
            |_, _| {},
        )
        .unwrap();
        assert_eq!(external_reset.revision, 2);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            0
        );
        let baseline_seq = state.account_sink.latest_seq();

        let committed = state
            .reset_credential_backed_section(SectionId::ClusterFabric, 2)
            .await
            .unwrap();
        let CredentialBackedResetCommit::Cluster(committed) = committed else {
            panic!("cluster reset must return its exact snapshot")
        };
        assert_eq!(committed.section.revision, 2);
        assert!(committed.config.cluster_fabric.nodes.is_empty());
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            2
        );
        assert!(state.config.read().await.cluster_fabric.nodes.is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let revisions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "cluster-fabric" => {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(revisions, vec![2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generic_events_follow_serialized_local_commit_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let baseline_seq = state.account_sink.latest_seq();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_event_test_hook(dir.path(), move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        let first = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        |config| {
                            config.server.port = 22_231;
                            Ok(())
                        },
                        ConfigUpdateEffects::default(),
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();
        let second = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        |config| {
                            config.server.port = 22_232;
                            Ok(())
                        },
                        ConfigUpdateEffects::default(),
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !second.is_finished(),
            "the later writer must remain behind the first writer's event"
        );
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert!(
            events.iter().all(|event| !matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. } if section == "core"
            )),
            "neither local commit can publish while the first owns config_io_lock"
        );

        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().server.port, 22_231);
        assert_eq!(second.await.unwrap().unwrap().server.port, 22_232);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let events = bamboo_engine::events::journal::read_since(
                    state.account_sink.events_dir(),
                    baseline_seq,
                )
                .unwrap();
                let revisions = events
                    .iter()
                    .filter_map(|event| match &event.event {
                        AgentEvent::ConfigChanged { section, revision } if section == "core" => {
                            Some(*revision)
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if revisions.len() == 2 {
                    break revisions;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map(|revisions| assert_eq!(revisions, vec![1, 2]))
        .expect("both serialized core events must reach the journal");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generic_runtime_effects_finish_before_later_config_writer() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("mcp-fixture.py");
        std::fs::write(
            &script,
            r#"import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    if request_id is None:
        continue
    if request.get("method") == "server/discover":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "Method not found"},
        }), flush=True)
        continue
    if request.get("method") == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "config-order-fixture", "version": "1.0.0"},
        }
    elif request.get("method") == "tools/list":
        result = {"tools": []}
    else:
        result = {}
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
"#,
        )
        .unwrap();
        let python = ["python3", "python"]
            .into_iter()
            .find(|command| {
                std::process::Command::new(command)
                    .arg("--version")
                    .output()
                    .is_ok_and(|output| output.status.success())
            })
            .expect("a Python interpreter is required for the MCP ordering fixture");

        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let held_provider = state.provider.write().await;
        let (provider_ready_tx, provider_ready_rx) = std::sync::mpsc::channel();
        set_generic_before_provider_publish_test_hook(dir.path(), move || {
            provider_ready_tx.send(()).unwrap();
        });

        let first = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        |config| {
                            config.provider = "copilot".to_string();
                            Ok(())
                        },
                        ConfigUpdateEffects {
                            reload_provider: bamboo_config::patch::ReloadMode::Strict,
                            reconcile_mcp: bamboo_config::patch::ReloadMode::Strict,
                        },
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || provider_ready_rx.recv().unwrap())
            .await
            .unwrap();
        assert!(
            state.config_io_lock.try_lock().is_err(),
            "the first writer must retain config_io_lock until its runtime effects finish"
        );
        assert!(
            !first.is_finished(),
            "the first writer must still be waiting to publish its provider"
        );

        let later_mcp = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "later-winner".to_string(),
                name: None,
                enabled: true,
                transport: TransportConfig::Stdio(StdioConfig {
                    command: python.to_string(),
                    args: vec![script.to_string_lossy().into_owned()],
                    cwd: None,
                    env: std::collections::HashMap::new(),
                    env_encrypted: std::collections::HashMap::new(),
                    env_credential_refs: std::collections::HashMap::new(),
                    startup_timeout_ms: 2_000,
                }),
                request_timeout_ms: 2_000,
                healthcheck_interval_ms: 10_000,
                reconnect: ReconnectConfig {
                    enabled: false,
                    ..Default::default()
                },
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };
        let second = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        move |config| {
                            config.mcp = later_mcp;
                            Ok(())
                        },
                        ConfigUpdateEffects {
                            reload_provider: bamboo_config::patch::ReloadMode::None,
                            reconcile_mcp: bamboo_config::patch::ReloadMode::Strict,
                        },
                    )
                    .await
            })
        };

        drop(held_provider);
        first.await.unwrap().unwrap();
        let published = second.await.unwrap().unwrap();
        assert_eq!(published.mcp.servers[0].id, "later-winner");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "later-winner");
        assert_eq!(
            state.mcp_manager.list_servers(),
            vec!["later-winner".to_string()],
            "the later durable config generation must remain the final runtime generation"
        );
        state.mcp_manager.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn direct_provider_reload_cannot_publish_after_later_config_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut initial = Config::default();
        initial.provider = "openai".to_string();
        initial.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            api_key: "first-generation-key".to_string(),
            base_url: Some("http://127.0.0.1:1/v1".to_string()),
            ..Default::default()
        });
        let mut state = AppState::new_with_provider(
            dir.path().to_path_buf(),
            initial,
            Arc::new(WorkingProvider),
        )
        .await
        .unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let quiesced = tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
            .await
            .expect("startup config work must quiesce");
        drop(quiesced);
        let (reload_ready_tx, reload_ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_provider_publish_test_hook(dir.path(), move || {
            let _ = reload_ready_tx.send(());
            release_rx.recv().unwrap();
        });

        let reload = {
            let state = state.clone();
            tokio::spawn(async move { state.reload_provider().await })
        };
        tokio::time::timeout(Duration::from_secs(5), reload_ready_rx)
            .await
            .expect("direct reload reaches provider publication hook")
            .unwrap();
        assert!(state.config_io_lock.try_lock().is_err());

        let mut later_instance: bamboo_config::ProviderInstanceConfig =
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "base_url": "http://127.0.0.1:1/v1",
                "enabled": true
            }))
            .unwrap();
        later_instance.api_key = "later-generation-key".to_string();
        let (later_started_tx, later_started_rx) = tokio::sync::oneshot::channel();
        let later = {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = later_started_tx.send(());
                state
                    .update_config_with_provider_credentials(
                        move |config| {
                            config
                                .provider_instances
                                .insert("later-winner".to_string(), later_instance);
                            config.default_provider_instance = Some("later-winner".to_string());
                            Ok(())
                        },
                        BTreeSet::new(),
                        BTreeSet::from(["later-winner".to_string()]),
                        ConfigUpdateEffects {
                            reload_provider: bamboo_config::patch::ReloadMode::Strict,
                            reconcile_mcp: bamboo_config::patch::ReloadMode::None,
                        },
                    )
                    .await
            })
        };
        later_started_rx.await.unwrap();
        assert!(!later.is_finished());

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            reload.await.unwrap().unwrap();
            later.await.unwrap().unwrap();
        })
        .await
        .expect("serialized provider generations finish");
        assert_eq!(
            state
                .config
                .read()
                .await
                .default_provider_instance
                .as_deref(),
            Some("later-winner")
        );
        assert_eq!(
            state.provider_registry.default_provider_name(),
            "later-winner"
        );
        let registry_default = state.provider_registry.get_default().unwrap();
        let live_provider = state.provider.read().await.clone();
        assert!(
            Arc::ptr_eq(&registry_default, &live_provider),
            "registry default and reloadable provider handle must publish one generation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn combined_reload_cannot_publish_captured_mcp_after_later_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut initial = Config::default();
        initial.provider = "openai".to_string();
        initial.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            api_key: "combined-reload-key".to_string(),
            base_url: Some("http://127.0.0.1:1/v1".to_string()),
            ..Default::default()
        });
        let mut state = AppState::new_with_provider(
            dir.path().to_path_buf(),
            initial,
            Arc::new(WorkingProvider),
        )
        .await
        .unwrap();
        stop_config_watcher(&mut state);
        state
            .update_config_with_provider_credentials(
                |_| Ok(()),
                BTreeSet::from(["openai".to_string()]),
                BTreeSet::new(),
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();
        let first_mcp = working_stdio_mcp_config(dir.path(), "captured-first", None);
        state
            .update_config(
                move |config| {
                    config.mcp = first_mcp;
                    Ok(())
                },
                ConfigUpdateEffects::default(),
            )
            .await
            .unwrap();
        let state = Arc::new(state);
        let (reload_ready_tx, reload_ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_provider_publish_test_hook(dir.path(), move || {
            let _ = reload_ready_tx.send(());
            release_rx.recv().unwrap();
        });

        let reload = {
            let state = state.clone();
            tokio::spawn(async move { state.reload_config_and_runtime().await })
        };
        tokio::time::timeout(Duration::from_secs(5), reload_ready_rx)
            .await
            .expect("combined reload reaches provider publication hook")
            .unwrap();
        assert!(state.config_io_lock.try_lock().is_err());

        let later_mcp = working_stdio_mcp_config(dir.path(), "later-winner", None);
        let (later_started_tx, later_started_rx) = tokio::sync::oneshot::channel();
        let later = {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = later_started_tx.send(());
                state
                    .update_config(
                        move |config| {
                            config.mcp = later_mcp;
                            Ok(())
                        },
                        ConfigUpdateEffects {
                            reload_provider: bamboo_config::patch::ReloadMode::None,
                            reconcile_mcp: bamboo_config::patch::ReloadMode::Strict,
                        },
                    )
                    .await
            })
        };
        later_started_rx.await.unwrap();
        assert!(!later.is_finished());

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            reload.await.unwrap().unwrap();
            later.await.unwrap().unwrap();
        })
        .await
        .expect("serialized config/runtime generations finish");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "later-winner");
        assert_eq!(
            state.mcp_manager.list_servers(),
            vec!["later-winner".to_string()]
        );
        state.mcp_manager.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_mcp_credentials_round_trip_without_plaintext_in_durable_or_events() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let baseline_seq = state.account_sink.latest_seq();
        let secret = "legacy-mcp-roundtrip-secret";
        let mut candidate = disabled_mcp_config("credential-server");
        let TransportConfig::Stdio(stdio) = &mut candidate.servers[0].transport else {
            unreachable!()
        };
        stdio.env.insert("TOKEN".to_string(), secret.to_string());

        state
            .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                *mcp = candidate;
                Ok(())
            })
            .await
            .unwrap();
        let live = state.config.read().await.clone();
        let TransportConfig::Stdio(stdio) = &live.mcp.servers[0].transport else {
            unreachable!()
        };
        assert_eq!(stdio.env["TOKEN"], secret);
        let reference =
            bamboo_config::CredentialRef::parse(stdio.env_credential_refs["TOKEN"].clone())
                .unwrap();
        assert_eq!(
            state
                .credential_store
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            secret
        );
        for path in [
            dir.path().join("mcp.json"),
            dir.path().join("credentials.json"),
        ] {
            let bytes = std::fs::read(path).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains(secret));
        }
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        assert!(!format!("{events:?}").contains(secret));

        state
            .update_legacy_mcp_config(BTreeSet::new(), |mcp| {
                let TransportConfig::Stdio(stdio) = &mut mcp.servers[0].transport else {
                    unreachable!()
                };
                stdio
                    .env
                    .insert("TOKEN".to_string(), "****...****".to_string());
                Ok(())
            })
            .await
            .unwrap();
        let live = state.config.read().await.clone();
        let TransportConfig::Stdio(stdio) = &live.mcp.servers[0].transport else {
            unreachable!()
        };
        assert_eq!(stdio.env["TOKEN"], secret);
        assert_eq!(stdio.env_credential_refs["TOKEN"], reference.as_str());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_mcp_cancelled_start_finishes_before_later_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let secret = "legacy-mcp-cancel-secret";
        let candidate = working_stdio_mcp_config(dir.path(), "cancelled-start", Some(secret));
        let reference =
            bamboo_config::credential_ref("mcp", "cancelled-start", "env_TOKEN").unwrap();
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_credential_after_commit_before_live_test_hook(dir.path(), SectionId::Mcp, move || {
            let _ = commit_tx.send(());
            release_rx.recv().unwrap();
        });

        let operation = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                        *mcp = candidate;
                        Ok(())
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), commit_rx)
            .await
            .expect("legacy MCP write reaches durable-before-live hook")
            .unwrap();
        assert!(state.config_io_lock.try_lock().is_err());
        assert!(state.mcp_manager.list_servers().is_empty());
        assert!(
            !String::from_utf8_lossy(&std::fs::read(dir.path().join("mcp.json")).unwrap())
                .contains(secret)
        );
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());

        let (delete_started_tx, delete_started_rx) = tokio::sync::oneshot::channel();
        let delete = {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = delete_started_tx.send(());
                state
                    .update_legacy_mcp_config(BTreeSet::new(), |mcp| {
                        mcp.servers.clear();
                        Ok(())
                    })
                    .await
            })
        };
        delete_started_rx.await.unwrap();
        assert!(!delete.is_finished());
        release_tx.send(()).unwrap();
        delete.await.unwrap().unwrap();

        assert!(state.config.read().await.mcp.servers.is_empty());
        assert!(state.mcp_manager.list_servers().is_empty());
        assert!(state.mcp_manager.tool_index().all_aliases().is_empty());
        assert!(state
            .credential_store
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert!(
            !String::from_utf8_lossy(&std::fs::read(dir.path().join("mcp.json")).unwrap())
                .contains(secret)
        );
    }

    #[tokio::test]
    async fn legacy_mcp_rejects_secret_bearing_url_before_runtime_or_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let before = std::fs::read(dir.path().join("mcp.json")).unwrap();
        let secret = "must-never-connect-or-log";
        let candidate = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "unsafe-url".to_string(),
                name: None,
                enabled: true,
                transport: TransportConfig::Sse(bamboo_mcp::SseConfig {
                    url: format!("https://example.test/sse?token={secret}"),
                    headers: vec![],
                    connect_timeout_ms: 100,
                }),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };
        let error = state
            .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                *mcp = candidate;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
        assert!(!error.to_string().contains(secret));
        assert_eq!(std::fs::read(dir.path().join("mcp.json")).unwrap(), before);
        assert!(state.config.read().await.mcp.servers.is_empty());
        assert!(state.mcp_manager.list_servers().is_empty());
    }

    #[tokio::test]
    async fn legacy_mcp_start_failure_keeps_every_authority_on_the_previous_generation() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let disk_before = std::fs::read(dir.path().join("mcp.json")).unwrap();
        let config_before = state.config.read().await.clone();
        let facade_before = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .mcp
            .snapshot();
        let health_before = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let baseline_seq = state.account_sink.latest_seq();
        let mut failing = disabled_mcp_config("never-committed");
        failing.servers[0].enabled = true;
        let TransportConfig::Stdio(stdio) = &mut failing.servers[0].transport else {
            unreachable!()
        };
        stdio.command = "definitely-not-a-real-mcp-command-before-commit-736".to_string();

        let error = state
            .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                *mcp = failing;
                Ok(())
            })
            .await
            .expect_err("runtime staging must fail before the MCP durable boundary");
        assert!(matches!(error, AppError::InternalError(_)));
        assert_eq!(
            error.to_string(),
            "Internal server error: MCP runtime initialization failed before commit; retaining last-known-good generation"
        );
        assert_eq!(
            std::fs::read(dir.path().join("mcp.json")).unwrap(),
            disk_before
        );
        assert_eq!(
            serde_json::to_value(state.config.read().await.clone()).unwrap(),
            serde_json::to_value(config_before).unwrap()
        );
        assert!(state.mcp_manager.list_servers().is_empty());
        assert!(state.mcp_manager.tool_index().all_aliases().is_empty());

        let facade_after = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .mcp
            .snapshot();
        assert_eq!(facade_after.revision, facade_before.revision);
        assert_eq!(facade_after.loaded_at, facade_before.loaded_at);
        assert_eq!(facade_after.status, facade_before.status);
        let health_after = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(health_after.revision, health_before.revision);
        assert_eq!(health_after.loaded_at, health_before.loaded_at);
        assert_eq!(health_after.status, health_before.status);
        assert_eq!(health_after.last_error, health_before.last_error);
        assert!(bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap()
        .into_iter()
        .all(|event| !matches!(
            event.event,
            AgentEvent::ConfigChanged { ref section, .. }
                | AgentEvent::ConfigInvalid { ref section, .. }
                | AgentEvent::ConfigRecovered { ref section, .. }
                if section == "mcp"
        )));
    }

    #[tokio::test]
    async fn invalid_explicit_reload_retains_live_provider_generation_and_marks_health() {
        let dir = tempfile::tempdir().unwrap();
        let mut initial = Config::default();
        initial.server.port = 24_301;
        initial.save_to_dir(dir.path().to_path_buf()).unwrap();
        let injected: Arc<dyn LLMProvider> = Arc::new(WorkingProvider);
        let mut state = AppState::new_with_provider(
            dir.path().to_path_buf(),
            initial.clone(),
            injected.clone(),
        )
        .await
        .unwrap();
        stop_config_watcher(&mut state);
        let expected_provider = state.config.read().await.provider.clone();

        let mut invalid = initial;
        invalid.provider = "unknown-invalid-provider".to_string();
        invalid.save_to_dir(dir.path().to_path_buf()).unwrap();
        let error = state.reload_config_and_runtime().await.unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
        assert_eq!(state.config.read().await.server.port, 24_301);
        assert_eq!(state.config.read().await.provider, expected_provider);
        let live_provider = state.provider.read().await.clone();
        assert!(Arc::ptr_eq(&live_provider, &injected));
        let health = state
            .config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(health.status, SectionStatus::Invalid);
        assert_eq!(
            health.last_error.as_deref(),
            Some("provider configuration is invalid; retaining last-known-good generation")
        );
    }

    #[tokio::test]
    async fn committed_provider_start_failure_publishes_one_exact_revision() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let baseline_seq = state.account_sink.latest_seq();
        let previous_provider = state.provider.read().await.clone();
        let previous_default = state.provider_registry.default_provider_name();

        let published = state
            .update_config(
                |config| {
                    config.provider = "unknown-runtime-provider".to_string();
                    config.default_provider_instance = None;
                    config.provider_instances.clear();
                    Ok(())
                },
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::BestEffort,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::None,
                },
            )
            .await
            .expect("the durable provider generation commits with degraded runtime health");
        assert_eq!(published.provider, "unknown-runtime-provider");
        assert_eq!(
            state.config.read().await.provider,
            "unknown-runtime-provider"
        );
        assert_eq!(
            state.provider_registry.default_provider_name(),
            previous_default
        );
        assert!(Arc::ptr_eq(
            &state.provider.read().await.clone(),
            &previous_provider
        ));

        let facade_snapshot = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .providers
            .snapshot();
        let health = state
            .config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(facade_snapshot.revision, 1);
        assert_eq!(facade_snapshot.status, SectionStatus::Degraded);
        assert_eq!(health.revision, facade_snapshot.revision);
        assert_eq!(health.loaded_at, facade_snapshot.loaded_at);
        assert_eq!(health.source_path, facade_snapshot.source_path);
        assert_eq!(health.source_kind, facade_snapshot.source_kind);
        assert_eq!(health.status, facade_snapshot.status);
        assert_eq!(health.last_error, facade_snapshot.last_error);

        let invalid_revisions = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap()
        .into_iter()
        .filter_map(|event| match event.event {
            AgentEvent::ConfigInvalid { section, revision } if section == "providers" => {
                Some(revision)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
        assert_eq!(invalid_revisions, vec![facade_snapshot.revision]);
    }

    #[tokio::test]
    async fn committed_mcp_start_failure_publishes_one_exact_revision() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let baseline_seq = state.account_sink.latest_seq();
        let mut failing = disabled_mcp_config("committed-but-unstartable");
        failing.servers[0].enabled = true;
        let TransportConfig::Stdio(stdio) = &mut failing.servers[0].transport else {
            unreachable!()
        };
        stdio.command = "definitely-not-a-real-mcp-command-736".to_string();

        let published = state
            .update_config(
                move |config| {
                    config.mcp = failing;
                    Ok(())
                },
                ConfigUpdateEffects {
                    reload_provider: bamboo_config::patch::ReloadMode::None,
                    reconcile_mcp: bamboo_config::patch::ReloadMode::BestEffort,
                },
            )
            .await
            .expect("the durable MCP generation commits with degraded runtime health");
        assert_eq!(published.mcp.servers[0].id, "committed-but-unstartable");
        assert_eq!(
            state.config.read().await.mcp.servers[0].id,
            "committed-but-unstartable"
        );
        assert!(state.mcp_manager.list_servers().is_empty());

        let facade_snapshot = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .mcp
            .snapshot();
        let health = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(facade_snapshot.revision, 1);
        assert_eq!(facade_snapshot.status, SectionStatus::Degraded);
        assert_eq!(health.revision, facade_snapshot.revision);
        assert_eq!(health.loaded_at, facade_snapshot.loaded_at);
        assert_eq!(health.source_path, facade_snapshot.source_path);
        assert_eq!(health.source_kind, facade_snapshot.source_kind);
        assert_eq!(health.status, facade_snapshot.status);
        assert_eq!(health.last_error, facade_snapshot.last_error);

        let invalid_revisions = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap()
        .into_iter()
        .filter_map(|event| match event.event {
            AgentEvent::ConfigInvalid { section, revision } if section == "mcp" => Some(revision),
            _ => None,
        })
        .collect::<Vec<_>>();
        assert_eq!(invalid_revisions, vec![facade_snapshot.revision]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_legacy_reset_finishes_deletion_and_runtime_publication() {
        let dir = tempfile::tempdir().unwrap();
        let mut initial = Config::default();
        initial.server.port = 24_302;
        initial.save_to_dir(dir.path().to_path_buf()).unwrap();
        std::fs::write(dir.path().join("config.json.bak"), b"recovery-marker").unwrap();
        std::fs::write(dir.path().join("model_limits.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("connect.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("connect.json.bak"), b"credential-backup").unwrap();
        let injected: Arc<dyn LLMProvider> = Arc::new(WorkingProvider);
        let mut state =
            AppState::new_with_provider(dir.path().to_path_buf(), initial, injected.clone())
                .await
                .unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let (deleted_tx, deleted_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_reset_after_delete_test_hook(dir.path(), move || {
            let _ = deleted_tx.send(());
            release_rx.recv().unwrap();
        });

        let operation = {
            let state = state.clone();
            tokio::spawn(async move { state.reset_legacy_config_and_runtime().await })
        };
        tokio::time::timeout(Duration::from_secs(5), deleted_rx)
            .await
            .expect("reset reaches durable delete boundary")
            .unwrap();
        for path in [
            dir.path().join("config.json"),
            dir.path().join("model_limits.json"),
            dir.path().join("connect.json"),
            dir.path().join("connect.json.bak"),
        ] {
            assert!(!path.exists());
        }
        assert_eq!(
            std::fs::read(dir.path().join("config.json.bak")).unwrap(),
            b"recovery-marker"
        );
        assert_eq!(state.config.read().await.server.port, 24_302);
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());

        release_tx.send(()).unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
            .await
            .expect("detached reset must finish live/runtime publication");
        drop(completed);
        assert_eq!(state.config.read().await.server.port, 9562);
        assert!(state.mcp_manager.list_servers().is_empty());
        let health = state
            .config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(
            health.status,
            SectionStatus::Degraded,
            "the committed default config has no usable Anthropic credential, so reset must report truthful runtime degradation"
        );
        assert_eq!(
            health.last_error.as_deref(),
            Some("provider runtime initialization failed; retaining last-known-good runtime")
        );
        assert!(Arc::ptr_eq(&state.provider.read().await.clone(), &injected));
    }

    #[tokio::test]
    async fn legacy_reset_converges_runtime_after_partial_delete_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut initial = Config::default();
        initial.server.port = 24_303;
        initial.save_to_dir(dir.path().to_path_buf()).unwrap();
        std::fs::write(dir.path().join("model_limits.json"), b"{}").unwrap();
        let injected: Arc<dyn LLMProvider> = Arc::new(WorkingProvider);
        let mut state =
            AppState::new_with_provider(dir.path().to_path_buf(), initial, injected.clone())
                .await
                .unwrap();
        stop_config_watcher(&mut state);

        // A directory at a file path makes `remove_file` fail deterministically.
        // The reset must still delete later sensitive artifacts and publish the
        // generation represented by whatever remains on disk.
        std::fs::create_dir(dir.path().join("connect.json")).unwrap();
        std::fs::write(dir.path().join("connect.json.bak"), b"credential-backup").unwrap();

        let error = state.reset_legacy_config_and_runtime().await.unwrap_err();
        assert!(matches!(error, AppError::StorageError(_)));
        assert!(!dir.path().join("config.json").exists());
        assert!(!dir.path().join("model_limits.json").exists());
        assert!(dir.path().join("connect.json").is_dir());
        assert!(!dir.path().join("connect.json.bak").exists());
        assert_eq!(state.config.read().await.server.port, 9562);
        assert!(state.mcp_manager.list_servers().is_empty());
        assert!(Arc::ptr_eq(&state.provider.read().await.clone(), &injected));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generic_update_cancellation_after_commit_finishes_publication() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let mut feed = state.account_sink.subscribe();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_event_test_hook(dir.path(), move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        let operation = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config(
                        |config| {
                            config.server.port = 22_240;
                            Ok(())
                        },
                        ConfigUpdateEffects::default(),
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .revision,
            1,
            "the abort boundary must follow durable commit and facade adoption"
        );
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        let converged = tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
            .await
            .expect("detached generic update must finish live publication");
        drop(converged);

        assert_eq!(state.config.read().await.server.port, 22_240);
        assert_eq!(
            bamboo_config::ConfigFacade::open(dir.path())
                .unwrap()
                .effective_config()
                .server
                .port,
            22_240
        );
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged {
                section,
                revision: 1
            } if section == "core"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn provider_update_cancellation_after_commit_finishes_publication() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x7d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let mut feed = state.account_sink.subscribe();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_event_test_hook(dir.path(), move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        let operation = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .update_config_with_provider_credentials(
                        |config| {
                            config.provider = "openai".to_string();
                            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                                api_key: "cancellation-secret".to_string(),
                                model: Some("cancellation-model".to_string()),
                                ..Default::default()
                            });
                            Ok(())
                        },
                        BTreeSet::from(["openai".to_string()]),
                        BTreeSet::new(),
                        ConfigUpdateEffects::default(),
                    )
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .providers
                .snapshot()
                .revision,
            1,
            "the abort boundary must follow provider durable/facade adoption"
        );
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        let converged = tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
            .await
            .expect("detached provider update must finish live publication");
        drop(converged);

        assert_eq!(state.config.read().await.provider, "openai");
        let durable = bamboo_config::ConfigFacade::open(dir.path())
            .unwrap()
            .effective_config();
        assert_eq!(durable.provider, "openai");
        assert_eq!(
            durable
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("cancellation-model")
        );
        assert!(matches!(
            next_config_event(&mut feed, "providers").await,
            AgentEvent::ConfigChanged {
                section,
                revision: 1
            } if section == "providers"
        ));
        for file in ["providers.json", "credentials.json"] {
            assert!(
                !std::fs::read_to_string(dir.path().join(file))
                    .unwrap()
                    .contains("cancellation-secret"),
                "{file} must remain secret-free"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replace_config_cancellation_after_commit_finishes_publication() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let state = Arc::new(state);
        let mut replacement = state.config.read().await.clone();
        replacement.server.port = 22_241;
        let mut feed = state.account_sink.subscribe();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        set_generic_before_event_test_hook(dir.path(), move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        let operation = {
            let state = state.clone();
            tokio::spawn(async move {
                state
                    .replace_config(replacement, ConfigUpdateEffects::default())
                    .await
            })
        };
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .revision,
            1,
            "the abort boundary must follow replacement durable/facade adoption"
        );
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        let converged = tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
            .await
            .expect("detached replacement must finish live publication");
        drop(converged);

        assert_eq!(state.config.read().await.server.port, 22_241);
        assert_eq!(
            bamboo_config::ConfigFacade::open(dir.path())
                .unwrap()
                .effective_config()
                .server
                .port,
            22_241
        );
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged {
                section,
                revision: 1
            } if section == "core"
        ));
    }

    #[tokio::test]
    async fn deployed_node_delete_and_cluster_reset_reject_before_commit_and_remain_stoppable() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "live-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "live-node".to_string(),
                        label: "live-node".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: Some(bamboo_config::NodeState {
                            status: bamboo_config::NodeStatus::Running,
                            worker_id: Some("live-worker".to_string()),
                            ..Default::default()
                        }),
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        insert_registry_worker(
            &state,
            bamboo_server_tools::registry_keys::node_key("live-node"),
            "live-worker",
        )
        .await;
        let transaction_marker = dir.path().join("config-credential-migration.json");
        let marker_before_guard = std::fs::read(&transaction_marker).ok();
        bamboo_config::set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            bamboo_config::ClusterExactCommitTestFault::AfterManifestRecoveryFailure,
        );

        let delete = state
            .delete_cluster_node_credentials(
                1,
                "live-node".to_string(),
                BTreeMap::from([(
                    "live-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config
                        .cluster_fabric
                        .nodes
                        .retain(|node| node.id != "live-node");
                    Ok(())
                },
            )
            .await;
        assert!(matches!(delete, Err(AppError::BadRequest(_))));
        let reset = state
            .reset_credential_backed_section(SectionId::ClusterFabric, 1)
            .await;
        assert!(matches!(reset, Err(ConfigSectionMutationError::Invalid(_))));
        assert_eq!(
            std::fs::read(&transaction_marker).ok(),
            marker_before_guard,
            "registry guards must reject before opening a new durable transaction"
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        assert!(state
            .config
            .read()
            .await
            .cluster_fabric
            .node("live-node")
            .is_some());
        assert!(state
            .fabric_deployer
            .registry()
            .lock()
            .await
            .contains_key(&bamboo_server_tools::registry_keys::node_key("live-node")));

        bamboo_config::clear_cluster_exact_commit_test_fault(dir.path());
        let stopped = state
            .fabric_deployer
            .stop_at_revision("live-node", 1)
            .await
            .unwrap();
        assert_eq!(stopped.snapshot.section.revision, 2);
        assert!(!state
            .fabric_deployer
            .registry()
            .lock()
            .await
            .contains_key(&bamboo_server_tools::registry_keys::node_key("live-node")));
        let deleted = state
            .delete_cluster_node_credentials(
                2,
                "live-node".to_string(),
                BTreeMap::from([(
                    "live-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config
                        .cluster_fabric
                        .nodes
                        .retain(|node| node.id != "live-node");
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(deleted.section.revision, 3);
        assert!(deleted.config.cluster_fabric.node("live-node").is_none());
    }

    #[tokio::test]
    async fn unrelated_agent_registry_entry_does_not_block_cluster_reset() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "reset-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "reset-node".to_string(),
                        label: "reset-node".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        let agent_key = bamboo_server_tools::registry_keys::agent_key("unrelated-agent");
        insert_registry_worker(&state, agent_key.clone(), "unrelated-agent").await;

        state
            .reset_credential_backed_section(SectionId::ClusterFabric, 1)
            .await
            .unwrap();
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            2
        );
        assert!(state.config.read().await.cluster_fabric.nodes.is_empty());
        let unrelated = state
            .fabric_deployer
            .registry()
            .lock()
            .await
            .remove(&agent_key)
            .expect("agent registry entry must survive cluster reset");
        unrelated.handle.shutdown().await;
    }

    #[tokio::test]
    async fn operator_cluster_crud_recovers_before_finish_and_converges_once() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x72; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let baseline_seq = state.account_sink.latest_seq();
        bamboo_config::set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            bamboo_config::ClusterExactCommitTestFault::BeforeFinish,
        );

        let committed = state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "recovered-crud-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "recovered-crud-node".to_string(),
                        label: "recovered-crud-node".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .expect("operator CRUD must recover the committed transaction");
        assert_eq!(committed.section.revision, 1);
        assert_eq!(
            committed
                .config
                .cluster_fabric
                .node("recovered-crud-node")
                .unwrap()
                .label,
            "recovered-crud-node"
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .cluster_fabric
                .node("recovered-crud-node")
                .unwrap()
                .label,
            "recovered-crud-node"
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        let reopened = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 1);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("recovered-crud-node")
                .unwrap()
                .label,
            "recovered-crud-node"
        );
        bamboo_config::ensure_provider_mcp_migration_ready(dir.path()).unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let cluster_events = events
            .iter()
            .filter(|event| {
                matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, revision }
                        if section == "cluster-fabric" && *revision == 1
                )
            })
            .count();
        assert_eq!(cluster_events, 1);
        assert!(!events.iter().any(|event| {
            matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. } if section == "credentials"
            )
        }));
    }

    #[tokio::test]
    async fn cluster_replace_and_keep_noop_retain_the_exact_hydrated_runtime() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x73; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let baseline_seq = state.account_sink.latest_seq();
        let password_ref = bamboo_config::cluster_password_credential_ref("secret-node").unwrap();
        let password_from = |config: &Config| match &config
            .cluster_fabric
            .node("secret-node")
            .expect("secret node exists")
            .placement
        {
            bamboo_config::NodePlacement::Ssh(target) => match &target.auth {
                bamboo_config::SshAuth::Password { password, .. } => password.clone(),
                _ => panic!("expected password authentication"),
            },
            _ => panic!("expected SSH placement"),
        };

        let replaced = state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "secret-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents {
                        password: bamboo_config::ClusterCredentialAction::Replace(
                            "exact-password".to_string(),
                        ),
                        private_key: bamboo_config::ClusterCredentialAction::Clear,
                        passphrase: bamboo_config::ClusterCredentialAction::Clear,
                    },
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "secret-node".to_string(),
                        label: "secret-node".to_string(),
                        placement: bamboo_config::NodePlacement::Ssh(bamboo_config::SshTarget {
                            host: "secret.example.test".to_string(),
                            port: 22,
                            username: "operator".to_string(),
                            auth: bamboo_config::SshAuth::Password {
                                password: String::new(),
                                password_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(replaced.section.revision, 1);
        assert_eq!(password_from(&replaced.config), "exact-password");
        assert_eq!(
            password_from(&*state.config.read().await),
            "exact-password",
            "live runtime must install the under-lock hydrated candidate"
        );
        assert_eq!(replaced.credential_health.revision, 1);
        assert_eq!(replaced.credential_statuses.len(), 1);
        assert_eq!(replaced.credential_statuses[0].credential_ref, password_ref);
        assert!(replaced.credential_statuses[0].configured);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let replace_events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let cluster_revisions = replace_events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "cluster-fabric" => {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(cluster_revisions, vec![1]);
        assert!(!replace_events.iter().any(|event| {
            matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. }
                    | AgentEvent::ConfigInvalid { section, .. }
                    | AgentEvent::ConfigRecovered { section, .. }
                    if section == "credentials"
            )
        }));

        let noop_baseline_seq = state.account_sink.latest_seq();
        let kept = state
            .update_cluster_fabric_credentials(
                1,
                BTreeMap::from([(
                    "secret-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents {
                        password: bamboo_config::ClusterCredentialAction::Keep,
                        private_key: bamboo_config::ClusterCredentialAction::Clear,
                        passphrase: bamboo_config::ClusterCredentialAction::Clear,
                    },
                )]),
                |config| {
                    let node = config
                        .cluster_fabric
                        .node_mut("secret-node")
                        .expect("secret node exists");
                    let bamboo_config::NodePlacement::Ssh(target) = &mut node.placement else {
                        panic!("expected SSH placement")
                    };
                    let bamboo_config::SshAuth::Password {
                        password,
                        password_encrypted,
                    } = &mut target.auth
                    else {
                        panic!("expected password authentication")
                    };
                    password.clear();
                    *password_encrypted = None;
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(kept.section.revision, 1);
        assert_eq!(kept.credential_health.revision, 1);
        assert_eq!(password_from(&kept.config), "exact-password");
        assert_eq!(
            password_from(&*state.config.read().await),
            "exact-password",
            "semantic no-op must retain the exact credential snapshot"
        );

        tokio::time::sleep(Duration::from_millis(500)).await;
        let noop_events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            noop_baseline_seq,
        )
        .unwrap();
        assert!(!noop_events.iter().any(|event| {
            matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. }
                    | AgentEvent::ConfigInvalid { section, .. }
                    | AgentEvent::ConfigRecovered { section, .. }
                    if section == "cluster-fabric" || section == "credentials"
            )
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn later_external_credential_winner_remains_observable_after_exact_cluster_commit() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x74; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let baseline_seq = state.account_sink.latest_seq();
        let password_ref =
            bamboo_config::cluster_password_credential_ref("credential-race-node").unwrap();
        let external_ref = password_ref.clone();
        let (external_done_tx, external_done_rx) = std::sync::mpsc::sync_channel(1);
        set_cluster_after_commit_before_adoption_test_hook(dir.path(), 0, move |data_dir| {
            let data_dir = data_dir.to_path_buf();
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let result = bamboo_config::CredentialStore::open(&data_dir).replace(
                    external_ref,
                    "later-external-password",
                    bamboo_config::CredentialSource::User,
                    1,
                );
                external_done_tx.send(result).unwrap();
            });
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("external credential writer must launch under the commit lock");
        });

        let committed = state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "credential-race-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents {
                        password: bamboo_config::ClusterCredentialAction::Replace(
                            "exact-commit-password".to_string(),
                        ),
                        private_key: bamboo_config::ClusterCredentialAction::Clear,
                        passphrase: bamboo_config::ClusterCredentialAction::Clear,
                    },
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "credential-race-node".to_string(),
                        label: "credential-race-node".to_string(),
                        placement: bamboo_config::NodePlacement::Ssh(bamboo_config::SshTarget {
                            host: "race.example.test".to_string(),
                            port: 22,
                            username: "operator".to_string(),
                            auth: bamboo_config::SshAuth::Password {
                                password: String::new(),
                                password_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        let committed_password = match &committed
            .config
            .cluster_fabric
            .node("credential-race-node")
            .unwrap()
            .placement
        {
            bamboo_config::NodePlacement::Ssh(target) => match &target.auth {
                bamboo_config::SshAuth::Password { password, .. } => password,
                _ => panic!("expected password authentication"),
            },
            _ => panic!("expected SSH placement"),
        };
        assert_eq!(committed.section.revision, 1);
        assert_eq!(committed.credential_health.revision, 1);
        assert_eq!(committed_password, "exact-commit-password");

        let external_revision = tokio::task::spawn_blocking(move || {
            external_done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("external credential writer must complete")
                .unwrap()
                .0
        })
        .await
        .unwrap();
        assert_eq!(external_revision, 2);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let facade_revision = state
                    .config_facade
                    .as_ref()
                    .unwrap()
                    .registry()
                    .credentials
                    .snapshot()
                    .revision;
                let events = bamboo_engine::events::journal::read_since(
                    state.account_sink.events_dir(),
                    baseline_seq,
                )
                .unwrap();
                let saw_external_event = events.iter().any(|event| {
                    matches!(
                        &event.event,
                        AgentEvent::ConfigChanged { section, revision }
                            if section == "credentials" && *revision == 2
                    )
                });
                if facade_revision == 2 && saw_external_event {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("watcher must expose the later credential revision");
        tokio::time::sleep(Duration::from_millis(250)).await;

        let runtime_password = match &state
            .config
            .read()
            .await
            .cluster_fabric
            .node("credential-race-node")
            .unwrap()
            .placement
        {
            bamboo_config::NodePlacement::Ssh(target) => match &target.auth {
                bamboo_config::SshAuth::Password { password, .. } => password.clone(),
                _ => panic!("expected password authentication"),
            },
            _ => panic!("expected SSH placement"),
        };
        assert_eq!(
            runtime_password, "exact-commit-password",
            "a status-only credential event must not rewrite the exact cluster runtime"
        );
        let credential_dir = dir.path().to_path_buf();
        let durable_password = tokio::task::spawn_blocking(move || {
            bamboo_config::CredentialStore::open(credential_dir)
                .resolve(&password_ref)
                .unwrap()
                .unwrap()
                .expose()
                .to_string()
        })
        .await
        .unwrap();
        assert_eq!(durable_password, "later-external-password");

        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let relevant = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision }
                    if section == "cluster-fabric" || section == "credentials" =>
                {
                    Some((section.as_str(), *revision))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            relevant,
            vec![("cluster-fabric", 1), ("credentials", 2)],
            "the exact cluster event must precede the genuine later credential winner"
        );
    }

    #[tokio::test]
    async fn changed_cluster_commit_publishes_secret_free_runtime_before_materialization_error() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x75; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let password_ref =
            bamboo_config::cluster_password_credential_ref("corrupt-secret-node").unwrap();
        state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "corrupt-secret-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents {
                        password: bamboo_config::ClusterCredentialAction::Replace(
                            "initial-password".to_string(),
                        ),
                        private_key: bamboo_config::ClusterCredentialAction::Clear,
                        passphrase: bamboo_config::ClusterCredentialAction::Clear,
                    },
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "corrupt-secret-node".to_string(),
                        label: "before-corruption".to_string(),
                        placement: bamboo_config::NodePlacement::Ssh(bamboo_config::SshTarget {
                            host: "corrupt.example.test".to_string(),
                            port: 22,
                            username: "operator".to_string(),
                            auth: bamboo_config::SshAuth::Password {
                                password: String::new(),
                                password_encrypted: None,
                            },
                            host_key_fingerprint: None,
                        }),
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();

        let credentials_path = dir.path().join("credentials.json");
        let mut document: Value =
            serde_json::from_slice(&std::fs::read(&credentials_path).unwrap()).unwrap();
        document["data"]["entries"][password_ref.as_str()]["ciphertext"] =
            Value::String("corrupt-ciphertext".to_string());
        std::fs::write(
            &credentials_path,
            serde_json::to_vec_pretty(&document).unwrap(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let noop_baseline_seq = state.account_sink.latest_seq();
        let noop = state
            .update_cluster_fabric_credentials(1, BTreeMap::new(), |_| Ok(()))
            .await;
        match noop {
            Err(AppError::InternalError(_)) => {}
            Err(error) => panic!("no-op materialization error was misclassified: {error}"),
            Ok(_) => panic!("corrupt credential unexpectedly materialized"),
        }
        let runtime = state.config.read().await;
        let node = runtime.cluster_fabric.node("corrupt-secret-node").unwrap();
        let bamboo_config::NodePlacement::Ssh(target) = &node.placement else {
            panic!("expected SSH placement")
        };
        let bamboo_config::SshAuth::Password { password, .. } = &target.auth else {
            panic!("expected password authentication")
        };
        assert_eq!(
            password, "initial-password",
            "a true no-op materialization failure must preserve the old runtime"
        );
        drop(runtime);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .cluster_fabric
                .snapshot()
                .revision,
            1
        );
        let noop_events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            noop_baseline_seq,
        )
        .unwrap();
        assert!(!noop_events.iter().any(|event| {
            matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. } if section == "cluster-fabric"
            )
        }));

        let baseline_seq = state.account_sink.latest_seq();
        let result = state
            .update_cluster_fabric_credentials(1, BTreeMap::new(), |config| {
                config
                    .cluster_fabric
                    .node_mut("corrupt-secret-node")
                    .unwrap()
                    .label = "committed-metadata".to_string();
                Ok(())
            })
            .await;
        match result {
            Err(AppError::InternalError(_)) => {}
            Err(error) => panic!("post-commit materialization error was misclassified: {error}"),
            Ok(_) => panic!("corrupt credential unexpectedly materialized"),
        }

        let runtime = state.config.read().await;
        let node = runtime.cluster_fabric.node("corrupt-secret-node").unwrap();
        assert_eq!(node.label, "committed-metadata");
        let bamboo_config::NodePlacement::Ssh(target) = &node.placement else {
            panic!("expected SSH placement")
        };
        let bamboo_config::SshAuth::Password {
            password,
            password_encrypted,
        } = &target.auth
        else {
            panic!("expected password authentication")
        };
        assert!(password.is_empty());
        assert!(password_encrypted.is_none());
        drop(runtime);

        let section = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .cluster_fabric
            .snapshot();
        assert_eq!(section.revision, 2);
        assert_eq!(
            section.data.0.node("corrupt-secret-node").unwrap().label,
            "committed-metadata"
        );
        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let cluster_revisions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "cluster-fabric" => {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(cluster_revisions, vec![2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_commit_adopts_exact_revision_before_later_external_winner() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x72; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let baseline_seq = state.account_sink.latest_seq();
        let (external_done_tx, external_done_rx) = std::sync::mpsc::sync_channel(1);
        set_cluster_after_commit_before_adoption_test_hook(dir.path(), 0, move |data_dir| {
            let data_dir = data_dir.to_path_buf();
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let external = bamboo_config::ConfigFacade::open(&data_dir).unwrap();
                let mut winner = external.effective_config();
                winner.cluster_fabric.node_mut("race-node").unwrap().label =
                    "external-winner".to_string();
                let result =
                    bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                        &data_dir,
                        &mut winner,
                        &BTreeMap::new(),
                        1,
                    );
                external_done_tx.send(result).unwrap();
            });
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("external writer must launch after the durable commit");
        });

        let committed = state
            .update_cluster_fabric_credentials(
                0,
                BTreeMap::from([(
                    "race-node".to_string(),
                    bamboo_config::ClusterNodeCredentialIntents::clear_all(),
                )]),
                |config| {
                    config.cluster_fabric.nodes.push(bamboo_config::Node {
                        id: "race-node".to_string(),
                        label: "api-commit".to_string(),
                        placement: bamboo_config::NodePlacement::Local,
                        trust_level: bamboo_config::TrustLevel::Trusted,
                        deploy: bamboo_config::DeployProfile::default(),
                        state: None,
                        enabled: true,
                    });
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(committed.section.revision, 1);
        assert_eq!(
            committed
                .config
                .cluster_fabric
                .node("race-node")
                .unwrap()
                .label,
            "api-commit",
            "the response must remain bound to its exact committed candidate"
        );
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                external_done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("later external winner must complete")
                    .unwrap()
            })
            .await
            .unwrap(),
            2
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let facade_revision = state
                    .config_facade
                    .as_ref()
                    .unwrap()
                    .registry()
                    .cluster_fabric
                    .snapshot()
                    .revision;
                let runtime_label = state
                    .config
                    .read()
                    .await
                    .cluster_fabric
                    .node("race-node")
                    .map(|node| node.label.clone());
                if facade_revision == 2 && runtime_label.as_deref() == Some("external-winner") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("watcher must apply the later external revision");

        let events = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            baseline_seq,
        )
        .unwrap();
        let revisions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "cluster-fabric" => {
                    Some(*revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            revisions,
            vec![1, 2],
            "the exact API event must precede the later watcher winner exactly once"
        );
        assert!(!events.iter().any(|event| {
            matches!(
                &event.event,
                AgentEvent::ConfigChanged { section, .. } if section == "credentials"
            )
        }));
    }

    async fn wait_for_facade_health(
        state: &AppState,
        id: SectionId,
        status: SectionStatus,
        revision: u64,
    ) -> bamboo_config::SectionHealth {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let health = state
                    .config_facade
                    .as_ref()
                    .expect("production state owns a facade")
                    .registry()
                    .health()
                    .unwrap()
                    .into_iter()
                    .find(|health| health.section == id)
                    .unwrap();
                if health.status == status && health.revision == revision {
                    break health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("facade health transition timed out")
    }

    #[test]
    fn initial_provider_health_validates_primary_and_backup() {
        let dir = tempfile::tempdir().unwrap();
        let store = AtomicJsonStore::new(dir.path().join("providers.json"), 1);
        let missing = initial_provider_health(&store);
        assert_eq!(missing.status, SectionStatus::Missing);
        assert_eq!(missing.source_kind, SectionSourceKind::Default);

        std::fs::write(dir.path().join("providers.json"), b"{broken").unwrap();
        let invalid = initial_provider_health(&store);
        assert_eq!(invalid.status, SectionStatus::Invalid);
        assert_eq!(invalid.source_kind, SectionSourceKind::File);

        std::fs::write(dir.path().join("providers.json.bak"), b"{}").unwrap();
        let recovered = initial_provider_health(&store);
        assert_eq!(recovered.status, SectionStatus::Degraded);
        assert_eq!(recovered.source_kind, SectionSourceKind::Backup);
        assert!(recovered
            .last_error
            .as_deref()
            .unwrap()
            .contains("last-known-good backup"));

        std::fs::write(dir.path().join("providers.json"), b"{}").unwrap();
        let healthy = initial_provider_health(&store);
        assert_eq!(healthy.status, SectionStatus::Healthy);
        assert_eq!(healthy.source_kind, SectionSourceKind::File);
    }

    #[tokio::test]
    async fn unrecoverable_pending_manifest_never_publishes_partial_provider_state() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x6c; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_unrecoverable_pending_provider_migration(dir.path());

        let loaded = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.providers().openai.as_ref().unwrap().model.as_deref(),
            Some("root-lkg")
        );
        let store = AtomicJsonStore::new(dir.path().join("providers.json"), 1);
        let health = initial_provider_health(&store);
        assert_eq!(health.status, SectionStatus::Degraded);
        assert!(health
            .last_error
            .as_deref()
            .unwrap()
            .contains("migration is pending"));
        let error = match load_and_prepare_provider_candidate(&store, 0, loaded).await {
            Ok(_) => panic!("pending migration must reject provider candidate"),
            Err(error) => error,
        };
        assert!(error.message.contains("retaining last-known-good runtime"));
        assert!(!error.message.contains("partial-must-not-load"));

        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        assert_eq!(
            state
                .config
                .read()
                .await
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("root-lkg")
        );
        assert_eq!(
            state
                .config_live_health
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .status,
            SectionStatus::Degraded
        );
    }

    #[async_trait::async_trait]
    impl LLMProvider for WorkingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("working-provider-marker".to_string()))
        }
    }

    #[tokio::test]
    async fn cancelled_provider_put_cannot_commit_before_publication_guards() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x53; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let secret = "provider-cancel-secret";
        let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
        bamboo_config::CredentialStore::open(dir.path())
            .replace(
                reference.clone(),
                secret,
                bamboo_config::CredentialSource::User,
                0,
            )
            .unwrap();
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            *config.providers_mut() = ProviderConfigs {
                openai: Some(bamboo_config::OpenAIConfig {
                    api_key: secret.to_string(),
                    credential_ref: Some(reference),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }
        let provider_lock = state.provider.clone();
        let held_provider = provider_lock.write().await;
        let providers_before = std::fs::read(dir.path().join("providers.json")).unwrap();
        let mut operation = Box::pin(state.put_provider_section(
            0,
            ProviderConfigs {
                openai: Some(bamboo_config::OpenAIConfig {
                    model: Some("candidate-model".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut operation)
                .await
                .is_err()
        );
        drop(operation);
        assert_eq!(
            std::fs::read(dir.path().join("providers.json")).unwrap(),
            providers_before,
            "cancellation while waiting for publication guards must precede durable commit"
        );
        drop(held_provider);
    }

    #[test]
    fn cancelled_provider_settings_request_finishes_exact_commit_and_live_publication() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x71; 32]);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let state = Arc::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
            wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;

            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();

            let operation_state = state.clone();
            let operation = tokio::spawn(async move {
                operation_state
                    .put_provider_settings(0, |_current, candidate| {
                        candidate.provider = "openai".to_string();
                        candidate.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                            api_key: "provider-settings-cancel-secret".to_string(),
                            model: Some("provider-settings-cancel-model".to_string()),
                            ..Default::default()
                        });
                        Ok((BTreeSet::from(["openai".to_string()]), BTreeSet::new()))
                    })
                    .await
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if state.config_io_lock.try_lock().is_err() {
                        break;
                    }
                    assert!(!operation.is_finished());
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("provider settings mutation acquires the config IO lock");
            operation.abort();
            let _ = operation.await;
            release_tx.send(()).unwrap();
            blocker.await.unwrap();

            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let committed = std::fs::read(dir.path().join("providers.json"))
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                        .is_some_and(|value| {
                            value["revision"] == 1
                                && value["data"]["openai"]["model"]
                                    == "provider-settings-cancel-model"
                        });
                    if committed {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("owned provider settings transaction completes after cancellation");

            let converged =
                tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
                    .await
                    .expect("owned provider runtime publication completes after cancellation");
            drop(converged);
            let live = state.config.read().await;
            let openai = live.providers().openai.as_ref().unwrap();
            assert_eq!(
                openai.model.as_deref(),
                Some("provider-settings-cancel-model")
            );
            assert_eq!(openai.api_key, "provider-settings-cancel-secret");
            drop(live);
            let providers = std::fs::read_to_string(dir.path().join("providers.json")).unwrap();
            let credentials = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
            assert!(!providers.contains("provider-settings-cancel-secret"));
            assert!(!credentials.contains("provider-settings-cancel-secret"));
        });
    }

    #[test]
    fn cancelled_proxy_update_cannot_leave_durable_state_ahead_of_live_snapshot() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let state = Arc::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
            wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;

            // Occupy Tokio's only blocking worker. The pre-fix implementation
            // queued the durable transaction with `spawn_blocking`, so aborting
            // the request detached that queued commit from live publication.
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();

            let operation_state = state.clone();
            let operation = tokio::spawn(async move {
                operation_state
                    .update_proxy_auth_credential(
                        Some(bamboo_config::ProxyAuth {
                            username: "cancel-user".to_string(),
                            password: "cancel-secret".to_string(),
                        }),
                        0,
                        ConfigUpdateEffects {
                            reload_provider: bamboo_config::patch::ReloadMode::Strict,
                            reconcile_mcp: bamboo_config::patch::ReloadMode::Strict,
                        },
                    )
                    .await
            });

            // Wait until the owned mutation has acquired config_io_lock and
            // queued its blocking transaction. Aborting the caller from this
            // exact point reproduced the old detached-commit/live-publication
            // split deterministically.
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if state.config_io_lock.try_lock().is_err() {
                        break;
                    }
                    assert!(!operation.is_finished());
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("proxy mutation acquires the config IO lock");
            operation.abort();
            let _ = operation.await;
            release_tx.send(()).unwrap();
            blocker.await.unwrap();

            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let credentials_ready = std::fs::read(dir.path().join("credentials.json"))
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                        .and_then(|value| value.get("revision").and_then(|value| value.as_u64()))
                        == Some(1);
                    let config_ready = std::fs::read(dir.path().join("core.json"))
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                        .and_then(|value| {
                            value
                                .get("data")
                                .and_then(|value| value.get("proxy_auth_credential_ref"))
                                .and_then(|value| value.as_str())
                                .map(str::to_string)
                        })
                        .as_deref()
                        == Some("proxy.default.auth");
                    if credentials_ready && config_ready {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("owned durable transaction completes after caller cancellation");

            // The owner retains config_io_lock through best-effort provider/MCP
            // convergence. Acquiring it proves cancellation did not strand the
            // post-commit runtime task either.
            let converged =
                tokio::time::timeout(Duration::from_secs(5), state.config_io_lock.lock())
                    .await
                    .expect("owned runtime convergence completes after cancellation");
            drop(converged);

            let live = state.config.read().await;
            assert_eq!(
                live.proxy_auth_credential_ref
                    .as_ref()
                    .map(|reference| reference.as_str()),
                Some("proxy.default.auth")
            );
            let auth = live
                .proxy_auth
                .as_ref()
                .expect("durable proxy auth must be published despite cancellation");
            assert_eq!(auth.username, "cancel-user");
            assert_eq!(auth.password, "cancel-secret");
            drop(live);

            let root = std::fs::read_to_string(dir.path().join("core.json")).unwrap();
            let credentials = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
            assert!(!root.contains("cancel-secret"));
            assert!(!credentials.contains("cancel-secret"));
        });
    }

    #[tokio::test]
    async fn missing_or_corrupt_referenced_credentials_reject_candidates_redacted() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x68; 32]);
        for corrupt_credentials in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
            let providers = ProviderConfigs {
                openai: Some(bamboo_config::OpenAIConfig {
                    credential_ref: Some(reference),
                    model: Some("candidate".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let provider_store = AtomicJsonStore::new(dir.path().join("providers.json"), 1);
            provider_store
                .commit(0, providers, validate_provider_config)
                .unwrap();
            if corrupt_credentials {
                std::fs::write(dir.path().join("credentials.json"), b"{corrupt-secret").unwrap();
            }
            let error =
                match load_and_prepare_provider_candidate(&provider_store, 0, Config::default())
                    .await
                {
                    Ok(_) => panic!("unavailable credential must reject provider candidate"),
                    Err(error) => error,
                };
            assert_eq!(error.message, "provider credential is unavailable");
            assert!(!error
                .message
                .contains(dir.path().to_string_lossy().as_ref()));

            let mut mcp = disabled_mcp_config("credential-lkg");
            let TransportConfig::Stdio(stdio) = &mut mcp.servers[0].transport else {
                unreachable!()
            };
            stdio.env_credential_refs.insert(
                "TOKEN".to_string(),
                bamboo_config::credential_ref("mcp", "credential-lkg", "env_TOKEN")
                    .unwrap()
                    .as_str()
                    .to_string(),
            );
            let mcp_store = AtomicJsonStore::new(dir.path().join("mcp.json"), 1);
            mcp_store.commit(0, mcp, validate_mcp_config).unwrap();
            let error = match load_and_validate_mcp_candidate(
                &mcp_store,
                0,
                Config::default(),
                false,
            )
            .await
            {
                Ok(_) => panic!("unavailable credential must reject MCP candidate"),
                Err(error) => error,
            };
            assert_eq!(error.message, "MCP credential is unavailable");
            assert!(!error.message.contains("TOKEN"));
            assert!(!error
                .message
                .contains(dir.path().to_string_lossy().as_ref()));
        }
    }

    #[tokio::test]
    async fn typed_provider_put_switches_refs_and_rejects_missing_ref_without_mutation() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x6d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let ref_a = bamboo_config::credential_ref("provider", "openai-a", "api_key").unwrap();
        let ref_b = bamboo_config::credential_ref("provider", "openai-b", "api_key").unwrap();
        let missing = bamboo_config::credential_ref("provider", "missing", "api_key").unwrap();
        state
            .credential_store
            .replace(
                ref_a.clone(),
                "provider-secret-a",
                bamboo_config::CredentialSource::User,
                0,
            )
            .unwrap();
        state
            .credential_store
            .replace(
                ref_b.clone(),
                "provider-secret-b",
                bamboo_config::CredentialSource::User,
                1,
            )
            .unwrap();
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            *config.providers_mut() = ProviderConfigs {
                openai: Some(bamboo_config::OpenAIConfig {
                    api_key: "provider-secret-a".to_string(),
                    credential_ref: Some(ref_a),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }

        let revision = state
            .put_provider_section(
                0,
                ProviderConfigs {
                    openai: Some(bamboo_config::OpenAIConfig {
                        credential_ref: Some(ref_b.clone()),
                        model: Some("switched".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(revision, 1);
        let runtime = state.config.read().await;
        let openai = runtime.providers().openai.as_ref().unwrap();
        assert_eq!(openai.credential_ref.as_ref(), Some(&ref_b));
        assert_eq!(openai.api_key, "provider-secret-b");
        drop(runtime);
        let disk_before = std::fs::read(dir.path().join("providers.json")).unwrap();

        assert!(state
            .put_provider_section(
                1,
                ProviderConfigs {
                    openai: Some(bamboo_config::OpenAIConfig {
                        credential_ref: Some(missing),
                        model: Some("must-not-publish".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .is_err());
        assert_eq!(
            std::fs::read(dir.path().join("providers.json")).unwrap(),
            disk_before
        );
        let runtime = state.config.read().await;
        let openai = runtime.providers().openai.as_ref().unwrap();
        assert_eq!(openai.credential_ref.as_ref(), Some(&ref_b));
        assert_eq!(openai.api_key, "provider-secret-b");
    }

    #[tokio::test]
    async fn typed_mcp_put_switches_stdio_and_header_refs_atomically() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x6e; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let refs = [
            bamboo_config::credential_ref("mcp", "stdio-a", "env_TOKEN").unwrap(),
            bamboo_config::credential_ref("mcp", "header-a", "header_Authorization").unwrap(),
            bamboo_config::credential_ref("mcp", "stdio-b", "env_TOKEN").unwrap(),
            bamboo_config::credential_ref("mcp", "header-b", "header_Authorization").unwrap(),
        ];
        for (revision, (reference, value)) in refs
            .iter()
            .zip(["env-a", "header-a", "env-b", "header-b"])
            .enumerate()
        {
            state
                .credential_store
                .replace(
                    reference.clone(),
                    value,
                    bamboo_config::CredentialSource::User,
                    revision as u64,
                )
                .unwrap();
        }
        let make_config = |env_ref: &bamboo_config::CredentialRef,
                           header_ref: &bamboo_config::CredentialRef| {
            McpConfig {
                version: 1,
                servers: vec![
                    McpServerConfig {
                        id: "switch-stdio".to_string(),
                        name: None,
                        enabled: false,
                        transport: TransportConfig::Stdio(StdioConfig {
                            command: "unused-disabled-command".to_string(),
                            args: vec![],
                            cwd: None,
                            env: std::collections::HashMap::new(),
                            env_encrypted: std::collections::HashMap::new(),
                            env_credential_refs: std::collections::HashMap::from([(
                                "TOKEN".to_string(),
                                env_ref.as_str().to_string(),
                            )]),
                            startup_timeout_ms: 100,
                        }),
                        request_timeout_ms: 100,
                        healthcheck_interval_ms: 100,
                        reconnect: ReconnectConfig::default(),
                        allowed_tools: vec![],
                        denied_tools: vec![],
                    },
                    McpServerConfig {
                        id: "switch-header".to_string(),
                        name: None,
                        enabled: false,
                        transport: TransportConfig::Sse(bamboo_mcp::SseConfig {
                            url: "https://example.test/sse".to_string(),
                            headers: vec![bamboo_mcp::HeaderConfig {
                                name: "Authorization".to_string(),
                                value: String::new(),
                                value_encrypted: None,
                                credential_ref: Some(header_ref.as_str().to_string()),
                            }],
                            connect_timeout_ms: 100,
                        }),
                        request_timeout_ms: 100,
                        healthcheck_interval_ms: 100,
                        reconnect: ReconnectConfig::default(),
                        allowed_tools: vec![],
                        denied_tools: vec![],
                    },
                ],
            }
        };
        let mut current = make_config(&refs[0], &refs[1]);
        if let TransportConfig::Stdio(stdio) = &mut current.servers[0].transport {
            stdio.env.insert("TOKEN".to_string(), "env-a".to_string());
        }
        if let TransportConfig::Sse(sse) = &mut current.servers[1].transport {
            sse.headers[0].value = "header-a".to_string();
        }
        state.config.write().await.mcp = current;

        assert_eq!(
            state
                .put_mcp_section(0, make_config(&refs[2], &refs[3]))
                .await
                .unwrap(),
            1
        );
        let runtime = state.config.read().await;
        let TransportConfig::Stdio(stdio) = &runtime
            .mcp
            .servers
            .iter()
            .find(|server| server.id == "switch-stdio")
            .expect("stdio server")
            .transport
        else {
            panic!("stdio transport")
        };
        assert_eq!(stdio.env["TOKEN"], "env-b");
        let TransportConfig::Sse(sse) = &runtime
            .mcp
            .servers
            .iter()
            .find(|server| server.id == "switch-header")
            .expect("SSE server")
            .transport
        else {
            panic!("sse transport")
        };
        assert_eq!(sse.headers[0].value, "header-b");
        drop(runtime);
        let disk_before = std::fs::read(dir.path().join("mcp.json")).unwrap();
        let missing_env = bamboo_config::credential_ref("mcp", "missing", "env_TOKEN").unwrap();
        let missing_header =
            bamboo_config::credential_ref("mcp", "missing", "header_Authorization").unwrap();
        assert!(state
            .put_mcp_section(1, make_config(&missing_env, &missing_header))
            .await
            .is_err());
        assert_eq!(
            std::fs::read(dir.path().join("mcp.json")).unwrap(),
            disk_before
        );
        let runtime = state.config.read().await;
        let TransportConfig::Stdio(stdio) = &runtime
            .mcp
            .servers
            .iter()
            .find(|server| server.id == "switch-stdio")
            .expect("stdio server")
            .transport
        else {
            panic!("stdio transport")
        };
        assert_eq!(stdio.env["TOKEN"], "env-b");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_initial_mcp_batch_does_not_reapply_after_typed_revision_advances() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let make_server = |id: &str, transport: TransportConfig| McpServerConfig {
            id: id.to_string(),
            name: None,
            enabled: false,
            transport,
            request_timeout_ms: 100,
            healthcheck_interval_ms: 100,
            reconnect: ReconnectConfig::default(),
            allowed_tools: vec![],
            denied_tools: vec![],
        };
        let initial = McpConfig {
            version: 1,
            servers: vec![make_server(
                "initial",
                TransportConfig::Stdio(StdioConfig {
                    command: "unused-initial-command".to_string(),
                    args: vec![],
                    cwd: None,
                    env: std::collections::HashMap::new(),
                    env_encrypted: std::collections::HashMap::new(),
                    env_credential_refs: std::collections::HashMap::new(),
                    startup_timeout_ms: 100,
                }),
            )],
        };
        assert_eq!(state.put_mcp_section(0, initial).await.unwrap(), 1);
        stop_config_watcher(&mut state);

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        set_initial_mcp_apply_test_hook(
            dir.path(),
            move || {
                reached_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            },
            move || {
                done_tx.send(()).unwrap();
            },
        );
        restart_config_watcher(&mut state);
        tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
            .await
            .unwrap();

        // The typed r2 commit wins while the queued startup batch still owns
        // an r1 generation token. The intentionally non-lexical Vec order is
        // only an internal canary: a redundant JSON-map reload would reorder
        // it even though server order is not a public persistence contract.
        let latest = McpConfig {
            version: 1,
            servers: vec![
                make_server(
                    "z-stdio",
                    TransportConfig::Stdio(StdioConfig {
                        command: "unused-latest-command".to_string(),
                        args: vec![],
                        cwd: None,
                        env: std::collections::HashMap::new(),
                        env_encrypted: std::collections::HashMap::new(),
                        env_credential_refs: std::collections::HashMap::new(),
                        startup_timeout_ms: 100,
                    }),
                ),
                make_server(
                    "a-sse",
                    TransportConfig::Sse(bamboo_mcp::SseConfig {
                        url: "https://example.test/sse".to_string(),
                        headers: vec![],
                        connect_timeout_ms: 100,
                    }),
                ),
            ],
        };
        let baseline = state.account_sink.latest_seq();
        assert_eq!(state.put_mcp_section(1, latest).await.unwrap(), 2);
        release_tx.send(()).unwrap();
        tokio::task::spawn_blocking(move || done_rx.recv().unwrap())
            .await
            .unwrap();

        let runtime = state.config.read().await;
        assert_eq!(
            runtime
                .mcp
                .servers
                .iter()
                .map(|server| server.id.as_str())
                .collect::<Vec<_>>(),
            vec!["z-stdio", "a-sse"],
            "the superseded startup generation must not reapply"
        );
        drop(runtime);
        let mcp_events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), baseline)
                .unwrap()
                .into_iter()
                .filter(|change| {
                    matches!(
                        &change.event,
                        AgentEvent::ConfigChanged { section, revision }
                            | AgentEvent::ConfigRecovered { section, revision }
                            if section == "mcp" && *revision == 2
                    )
                })
                .count();
        assert_eq!(
            mcp_events, 1,
            "the startup batch must not emit a pseudo event"
        );
    }

    #[tokio::test]
    async fn failed_candidate_keeps_existing_provider_registry_and_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let working: Arc<dyn LLMProvider> = Arc::new(WorkingProvider);
        state
            .provider_registry
            .insert("working".to_string(), working.clone());
        state.provider_registry.set_default("working".to_string());
        *state.provider.write().await = working.clone();
        state.config.write().await.provider = "openai".to_string();

        assert!(state.reload_provider().await.is_err());
        assert_eq!(state.provider_registry.default_provider_name(), "working");
        assert!(Arc::ptr_eq(
            &state.provider_registry.get_default().unwrap(),
            &working
        ));
        let live = state.provider.read().await;
        assert!(Arc::ptr_eq(&*live, &working));
    }

    #[tokio::test]
    async fn provider_watcher_retains_lkg_on_invalid_and_recovers_after_repair() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x43; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let working: Arc<dyn LLMProvider> = Arc::new(WorkingProvider);
        state
            .provider_registry
            .insert("working".to_string(), working.clone());
        state.provider_registry.set_default("working".to_string());
        *state.provider.write().await = working.clone();
        state.config.write().await.provider = "openai".to_string();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let mut feed = state.account_sink.subscribe();
        let providers_path = dir.path().join("providers.json");

        std::fs::write(&providers_path, b"{broken").unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state
                    .config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .status
                    == SectionStatus::Invalid
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            state
                .config_live_health
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .revision,
            0,
            "invalid edits must not advance the LKG revision"
        );
        {
            let health = state
                .config_live_health
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(health.status, SectionStatus::Invalid);
            assert_eq!(health.source_kind, SectionSourceKind::File);
            assert_eq!(health.source_path, providers_path);
        }
        assert!(Arc::ptr_eq(
            &state.provider_registry.get_default().unwrap(),
            &working
        ));
        let invalid = next_config_event(&mut feed, "providers").await;
        assert!(matches!(
            invalid,
            AgentEvent::ConfigInvalid { revision: 0, .. }
        ));

        let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
        let credential_store = bamboo_config::CredentialStore::open(dir.path());
        let credential_revision = credential_store.revision().unwrap();
        credential_store
            .replace(
                reference.clone(),
                "watcher-test-key",
                bamboo_config::CredentialSource::User,
                credential_revision,
            )
            .unwrap();
        let providers = ProviderConfigs {
            openai: Some(bamboo_config::OpenAIConfig {
                credential_ref: Some(reference),
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(
            &providers_path,
            serde_json::to_vec_pretty(&providers).unwrap(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let health = state
                    .config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if health.status == SectionStatus::Healthy && health.revision == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let recovered = next_config_event(&mut feed, "providers").await;
        assert!(matches!(
            recovered,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        assert_eq!(state.provider_registry.default_provider_name(), "openai");
    }

    #[tokio::test]
    async fn ordinary_section_watcher_updates_runtime_retains_lkg_and_recovers() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x44; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let mut feed = state.account_sink.subscribe();
        let path = dir.path().join("core.json");
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        document["revision"] = serde_json::json!(2);
        document["data"]["server"]["port"] = serde_json::json!(9876);
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();

        wait_for_facade_health(&state, SectionId::Core, SectionStatus::Healthy, 2).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state.config.read().await.server.port == 9876 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 2, .. }
        ));

        std::fs::write(&path, b"{broken").unwrap();
        wait_for_facade_health(&state, SectionId::Core, SectionStatus::Invalid, 2).await;
        assert_eq!(state.config.read().await.server.port, 9876);
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigInvalid { revision: 2, .. }
        ));

        document["revision"] = serde_json::json!(3);
        document["data"]["server"]["port"] = serde_json::json!(9877);
        std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        wait_for_facade_health(&state, SectionId::Core, SectionStatus::Healthy, 3).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state.config.read().await.server.port == 9877 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigRecovered { revision: 3, .. }
        ));

        std::fs::remove_file(&path).unwrap();
        wait_for_facade_health(&state, SectionId::Core, SectionStatus::Missing, 3).await;
        assert_eq!(state.config.read().await.server.port, 9877);
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigInvalid { revision: 3, .. }
        ));

        document["revision"] = serde_json::json!(4);
        document["data"]["server"]["port"] = serde_json::json!(9878);
        let swap = dir.path().join("core.json.swap");
        std::fs::write(&swap, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        std::fs::rename(&swap, &path).unwrap();
        wait_for_facade_health(&state, SectionId::Core, SectionStatus::Healthy, 4).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state.config.read().await.server.port == 9878 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigRecovered { revision: 4, .. }
        ));
    }

    #[tokio::test]
    async fn mcp_watcher_updates_lkg_rejects_invalid_and_recovers_after_atomic_replace() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x45; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let mut feed = state.account_sink.subscribe();
        let path = dir.path().join("mcp.json");

        std::fs::write(&path, mcp_document_bytes(2, &disabled_mcp_config("first"))).unwrap();
        let first = wait_for_mcp_health(&state, SectionStatus::Healthy, 2).await;
        assert_eq!(first.revision, 2);
        assert_eq!(state.config.read().await.mcp.servers[0].id, "first");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigChanged { revision: 2, .. }
        ));

        std::fs::write(&path, b"{broken").unwrap();
        let invalid = wait_for_mcp_health(&state, SectionStatus::Invalid, 2).await;
        assert_eq!(invalid.revision, 2, "invalid candidates cannot advance LKG");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "first");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 2, .. }
        ));

        // Model an editor's temp-write + atomic rename, with an immediate
        // follow-up write in the same debounce burst. The watcher must settle
        // on the final complete document rather than treating the rename gap as
        // a reset.
        let swap = dir.path().join("mcp.json.swap");
        std::fs::write(
            &swap,
            mcp_document_bytes(3, &disabled_mcp_config("intermediate")),
        )
        .unwrap();
        std::fs::rename(&swap, &path).unwrap();
        std::fs::write(
            &path,
            mcp_document_bytes(3, &disabled_mcp_config("recovered")),
        )
        .unwrap();
        let recovered = wait_for_mcp_health(&state, SectionStatus::Healthy, 3).await;
        assert_eq!(recovered.revision, 3, "rename burst should coalesce once");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "recovered");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 3, .. }
        ));

        // Reusing the live revision with different content forces the shared
        // store to normalize it to revision 4. The normalization write itself
        // must be suppressed exactly once rather than triggering a duplicate
        // reconcile/event.
        std::fs::write(
            &path,
            mcp_document_bytes(3, &disabled_mcp_config("normalized")),
        )
        .unwrap();
        let normalized = wait_for_mcp_health(&state, SectionStatus::Healthy, 4).await;
        assert_eq!(normalized.revision, 4);
        assert_eq!(state.config.read().await.mcp.servers[0].id, "normalized");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigChanged { revision: 4, .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), feed.recv())
                .await
                .is_err()
        );
        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted["revision"], 4);
    }

    #[tokio::test]
    async fn mcp_sidecar_present_at_startup_is_applied_through_runtime_transaction() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x47; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("mcp.json"),
            mcp_document_bytes(1, &disabled_mcp_config("startup-sidecar")),
        )
        .unwrap();

        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let health = wait_for_mcp_health(&state, SectionStatus::Healthy, 1).await;
        assert_eq!(health.revision, 1);
        assert_eq!(health.source_kind, SectionSourceKind::File);
        assert_eq!(
            state.config.read().await.mcp.servers[0].id,
            "startup-sidecar"
        );
    }

    #[tokio::test]
    async fn mcp_startup_uses_valid_backup_and_reports_degraded_invalid_health() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x48; 32]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.json");
        let store = AtomicJsonStore::new(&path, 1);
        store
            .commit(0, disabled_mcp_config("backup-lkg"), validate_mcp_config)
            .unwrap();
        store
            .commit(1, disabled_mcp_config("new-primary"), validate_mcp_config)
            .unwrap();
        std::fs::write(&path, b"{corrupt-primary").unwrap();

        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let health = wait_for_mcp_health(&state, SectionStatus::Degraded, 1).await;
        assert_eq!(health.revision, 1);
        assert_eq!(health.source_kind, SectionSourceKind::Backup);
        assert_eq!(health.source_path, path.with_extension("json.bak"));
        assert!(health
            .last_error
            .as_deref()
            .unwrap()
            .contains("last-known-good backup runtime"));
        assert_eq!(state.config.read().await.mcp.servers[0].id, "backup-lkg");

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state.account_sink.latest_seq() > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigInvalid { section, revision }
                        if section == "mcp" && *revision == 1
                ))
                .count(),
            1
        );
        let stable_health = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(stable_health.status, SectionStatus::Degraded);
        assert_eq!(stable_health.source_kind, SectionSourceKind::Backup);
        assert_eq!(stable_health.source_path, path.with_extension("json.bak"));
    }

    #[tokio::test]
    async fn mcp_runtime_init_failure_marks_degraded_and_retains_lkg_config() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x46; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let mut feed = state.account_sink.subscribe();
        let path = dir.path().join("mcp.json");

        std::fs::write(
            &path,
            mcp_document_bytes(1, &disabled_mcp_config("last-known-good")),
        )
        .unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 1).await;
        let _ = next_mcp_config_event(&mut feed).await;

        let mut failing = disabled_mcp_config("candidate");
        failing.servers[0].enabled = true;
        if let TransportConfig::Stdio(stdio) = &mut failing.servers[0].transport {
            stdio.command = "definitely-not-a-real-mcp-command-597".to_string();
        }
        std::fs::write(&path, mcp_document_bytes(2, &failing)).unwrap();

        let degraded = wait_for_mcp_health(&state, SectionStatus::Degraded, 1).await;
        assert_eq!(degraded.revision, 1);
        assert!(degraded
            .last_error
            .as_deref()
            .unwrap()
            .contains("last-known-good runtime"));
        assert_eq!(
            state.config.read().await.mcp.servers[0].id,
            "last-known-good"
        );
        assert!(state.mcp_manager.list_servers().is_empty());
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stopped_root_commit_is_installed_before_one_confirmed_event_on_same_facade_restart() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x52; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25201}}"#,
        )
        .unwrap();
        let committed = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(committed.committed.len(), 1);
        assert_ne!(state.config.read().await.server.port, 25_201);

        let mut feed = state.account_sink.subscribe();
        let config = state.config.clone();
        let read_guard = config.read().await;
        restart_config_watcher(&mut state);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(350),
                next_config_event(&mut feed, "core")
            )
            .await
            .is_err(),
            "the account event must wait for the runtime write"
        );
        drop(read_guard);

        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert_eq!(state.config.read().await.server.port, 25_201);
        wait_for_root_outbox_to_clear(dir.path()).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, revision }
                        if section == "core" && *revision == 1
                ))
                .count(),
            1
        );

        stop_config_watcher(&mut state);
        restart_config_watcher(&mut state);
        assert!(tokio::time::timeout(
            Duration::from_millis(500),
            next_config_event(&mut feed, "core")
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn pending_root_resolver_unavailable_keeps_lkg_silent_and_requests_retry() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x59; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        let old_port = state.config.read().await.server.port;
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25901}}"#,
        )
        .unwrap();
        let committed = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        let event = committed.committed[0].clone();
        let mut synthetic_events = BTreeMap::from([(SectionId::Core, event.clone())]);
        let mut pending_root_publications = BTreeMap::from([(SectionId::Core, event)]);
        let mut reported_root_runtime_failures = BTreeSet::new();
        let mut feed = state.account_sink.subscribe();
        std::fs::remove_file(dir.path().join("config-section-layout-completion.json")).unwrap();

        let retry = reload_and_apply_ordinary_sections(
            dir.path(),
            &state.config,
            state.config_facade.as_ref().unwrap(),
            &state.account_sink,
            std::iter::once(SectionId::Core),
            OrdinarySectionReloadState {
                synthetic_events: &mut synthetic_events,
                pending_root_publications: &mut pending_root_publications,
                reported_root_runtime_failures: &mut reported_root_runtime_failures,
            },
        )
        .await;

        assert!(retry);
        assert_eq!(state.config.read().await.server.port, old_port);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), feed.recv())
                .await
                .is_err()
        );
        assert!(pending_root_publications.contains_key(&SectionId::Core));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_root_then_new_root_survives_coalesced_mcp_noop_and_requeues() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x53; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let mut feed = state.account_sink.subscribe();
        let io = state.config_io_lock.clone().lock_owned().await;
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25301}}"#,
        )
        .unwrap();
        let first = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(first.committed.len(), 1);
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25302}}"#,
        )
        .unwrap();
        let mcp_bytes = std::fs::read(dir.path().join("mcp.json")).unwrap();
        std::fs::write(dir.path().join("mcp.json"), mcp_bytes).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(io);

        let first_event = next_config_event(&mut feed, "core").await;
        let second_event = next_config_event(&mut feed, "core").await;
        assert!(matches!(
            first_event,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert!(matches!(
            second_event,
            AgentEvent::ConfigChanged { revision: 2, .. }
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.config.read().await.server.port == 25_302 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        wait_for_root_outbox_to_clear(dir.path()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn startup_handoff_always_catches_root_generation_written_before_watcher_registration() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x56; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25601}}"#,
        )
        .unwrap();
        let startup_facade =
            Arc::new(bamboo_config::ConfigFacade::open_or_migrate(dir.path()).unwrap());
        assert!(bamboo_config::has_pending_legacy_root_publications(dir.path()).unwrap());
        state.config_facade = Some(startup_facade);
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25602}}"#,
        )
        .unwrap();
        let mut feed = state.account_sink.subscribe();

        restart_config_watcher(&mut state);

        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 2, .. }
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.config.read().await.server.port == 25_602 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        wait_for_root_outbox_to_clear(dir.path()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_facade_restart_replays_one_rejection_and_one_lost_recovery() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x54; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        stop_config_watcher(&mut state);
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"vendor_api_key":"never-persist-this"}}"#,
        )
        .unwrap();
        let rejected = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(rejected.rejected.len(), 1);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .status,
            SectionStatus::Healthy
        );
        let mut feed = state.account_sink.subscribe();
        restart_config_watcher(&mut state);
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigInvalid { revision: 0, .. }
        ));
        stop_config_watcher(&mut state);

        std::fs::write(dir.path().join("config.json"), b"{}").unwrap();
        let recovered = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(recovered.recovered, vec![SectionId::Core]);
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .status,
            SectionStatus::Degraded
        );
        restart_config_watcher(&mut state);
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigRecovered { revision: 0, .. }
        ));
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .core
                .snapshot()
                .status,
            SectionStatus::Healthy
        );

        stop_config_watcher(&mut state);
        restart_config_watcher(&mut state);
        assert!(tokio::time::timeout(
            Duration::from_millis(500),
            next_config_event(&mut feed, "core")
        )
        .await
        .is_err());
        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigInvalid { section, .. } if section == "core"
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigRecovered { section, .. } if section == "core"
                ))
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn degraded_root_mcp_is_carried_while_new_root_core_commits_then_recovers() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x57; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let failing = working_stdio_mcp_config(dir.path(), "root-carry", None);
        let script = match &failing.servers[0].transport {
            TransportConfig::Stdio(stdio) => PathBuf::from(&stdio.args[0]),
            _ => unreachable!(),
        };
        std::fs::remove_file(&script).unwrap();
        let mut feed = state.account_sink.subscribe();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": failing})).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));

        std::fs::write(
            dir.path().join("config.json"),
            br#"{"server":{"port":25701}}"#,
        )
        .unwrap();
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.config.read().await.server.port == 25_701 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(bamboo_config::has_pending_legacy_root_publications(dir.path()).unwrap());

        let repaired = working_stdio_mcp_config(dir.path(), "root-carry", None);
        assert_eq!(repaired.servers[0].id, "root-carry");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        wait_for_root_outbox_to_clear(dir.path()).await;
        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigInvalid { section, revision }
                        if section == "mcp" && *revision == 1
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigRecovered { section, revision }
                        if section == "mcp" && *revision == 1
                ))
                .count(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rejected_new_mcp_keeps_old_degraded_publication_dormant_until_clean_root() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5b; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let failing = working_stdio_mcp_config(dir.path(), "root-dormant", None);
        let script = match &failing.servers[0].transport {
            TransportConfig::Stdio(stdio) => PathBuf::from(&stdio.args[0]),
            _ => unreachable!(),
        };
        std::fs::remove_file(&script).unwrap();
        let mut feed = state.account_sink.subscribe();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": failing})).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));

        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({
                "server": {"port": 25801},
                "mcpServers": {
                    "rejected-next": {
                        "command": "unused-rejected-command",
                        "disabled": true,
                        "access_token_value": "must-not-cross"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            next_config_event(&mut feed, "core").await,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert_eq!(state.config.read().await.server.port, 25_801);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let dormant_events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert!(!dormant_events.iter().any(|event| matches!(
            &event.event,
            AgentEvent::ConfigRecovered { section, revision }
                if section == "mcp" && *revision == 1
        )));
        assert!(bamboo_config::has_pending_legacy_root_publications(dir.path()).unwrap());

        std::fs::write(dir.path().join("config.json"), b"{}").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !bamboo_config::legacy_root_rejected_sections(dir.path())
                    .unwrap()
                    .contains(&SectionId::Mcp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let repaired = working_stdio_mcp_config(dir.path(), "root-dormant", None);
        assert_eq!(repaired.servers[0].id, "root-dormant");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        wait_for_root_outbox_to_clear(dir.path()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn changed_before_ack_then_runtime_failure_recovers_with_exact_kind() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x58; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        stop_config_watcher(&mut state);
        let failing = working_stdio_mcp_config(dir.path(), "root-crash-window", None);
        let script = match &failing.servers[0].transport {
            TransportConfig::Stdio(stdio) => PathBuf::from(&stdio.args[0]),
            _ => unreachable!(),
        };
        std::fs::remove_file(&script).unwrap();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": failing})).unwrap(),
        )
        .unwrap();
        let committed = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(
            committed.committed,
            vec![ConfigSectionEvent::Changed {
                section: "mcp".to_string(),
                revision: 1,
            }]
        );
        assert!(
            state
                .account_sink
                .record_confirmed(None, &registry_agent_event(&committed.committed[0]))
                .await
        );
        assert!(bamboo_config::has_pending_legacy_root_publications(dir.path()).unwrap());

        let events_dir = state.account_sink.events_dir().to_path_buf();
        state.account_sink = bamboo_engine::events::AccountEventSink::new(events_dir).unwrap();
        tokio::task::yield_now().await;
        let mut feed = state.account_sink.subscribe();
        restart_config_watcher(&mut state);
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));
        let repaired = working_stdio_mcp_config(dir.path(), "root-crash-window", None);
        assert_eq!(repaired.servers[0].id, "root-crash-window");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        wait_for_root_outbox_to_clear(dir.path()).await;

        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        let transitions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "mcp" => {
                    Some(("changed", *revision))
                }
                AgentEvent::ConfigInvalid { section, revision } if section == "mcp" => {
                    Some(("invalid", *revision))
                }
                AgentEvent::ConfigRecovered { section, revision } if section == "mcp" => {
                    Some(("recovered", *revision))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            transitions,
            vec![("changed", 1), ("invalid", 1), ("recovered", 1)]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalid_journaled_before_canonical_mark_restarts_as_recovered_only() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5b; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        stop_config_watcher(&mut state);
        let candidate = disabled_mcp_config("root-invalid-mark-crash");
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": candidate})).unwrap(),
        )
        .unwrap();
        let committed = state
            .config_facade
            .as_ref()
            .unwrap()
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(
            committed.committed,
            vec![ConfigSectionEvent::Changed {
                section: "mcp".to_string(),
                revision: 1,
            }]
        );
        let invalid = ConfigSectionEvent::Invalid {
            section: "mcp".to_string(),
            revision: 1,
        };
        assert!(
            state
                .account_sink
                .record_confirmed(None, &registry_agent_event(&invalid))
                .await
        );
        let envelope = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .envelope_value(SectionId::Mcp)
            .unwrap();
        assert!(matches!(
            bamboo_config::legacy_root_publication_success_event(
                dir.path(),
                &committed.committed[0],
                &envelope.data,
            )
            .unwrap(),
            Some(ConfigSectionEvent::Changed { revision: 1, .. })
        ));

        let events_dir = state.account_sink.events_dir().to_path_buf();
        state.account_sink = bamboo_engine::events::AccountEventSink::new(events_dir).unwrap();
        assert!(state
            .account_sink
            .latest_config_transition_is_invalid("mcp", 1));
        let mut feed = state.account_sink.subscribe();
        restart_config_watcher(&mut state);
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        wait_for_root_outbox_to_clear(dir.path()).await;

        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        let transitions = events
            .iter()
            .filter_map(|event| match &event.event {
                AgentEvent::ConfigChanged { section, revision } if section == "mcp" => {
                    Some(("changed", *revision))
                }
                AgentEvent::ConfigInvalid { section, revision } if section == "mcp" => {
                    Some(("invalid", *revision))
                }
                AgentEvent::ConfigRecovered { section, revision } if section == "mcp" => {
                    Some(("recovered", *revision))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(transitions, vec![("invalid", 1), ("recovered", 1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn startup_lagging_mcp_facade_installs_and_acknowledges_root_publication() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5a; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        stop_config_watcher(&mut state);
        let external = bamboo_config::ConfigFacade::open(dir.path()).unwrap();
        let candidate = disabled_mcp_config("startup-lag-root");
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": candidate})).unwrap(),
        )
        .unwrap();
        let committed = external
            .reconcile_reappeared_legacy_root()
            .unwrap()
            .unwrap();
        assert_eq!(
            committed.committed,
            vec![ConfigSectionEvent::Changed {
                section: "mcp".to_string(),
                revision: 1,
            }]
        );
        assert_eq!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .mcp
                .snapshot()
                .revision,
            0
        );

        let mut feed = state.account_sink.subscribe();
        restart_config_watcher(&mut state);
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        wait_for_root_outbox_to_clear(dir.path()).await;
        assert!(state
            .config
            .read()
            .await
            .mcp
            .servers
            .iter()
            .any(|server| server.id == "startup-lag-root"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn persistent_root_mcp_runtime_failure_retries_without_invalid_event_storm() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x55; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        wait_for_mcp_health(&state, SectionStatus::Healthy, 0).await;
        let failing = working_stdio_mcp_config(dir.path(), "root-retry", None);
        let script = match &failing.servers[0].transport {
            TransportConfig::Stdio(stdio) => PathBuf::from(&stdio.args[0]),
            _ => unreachable!(),
        };
        std::fs::remove_file(&script).unwrap();
        let mut feed = state.account_sink.subscribe();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": failing})).unwrap(),
        )
        .unwrap();

        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));
        tokio::time::sleep(Duration::from_secs(4)).await;
        let failed_events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            failed_events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigInvalid { section, .. } if section == "mcp"
                ))
                .count(),
            1
        );
        assert!(bamboo_config::has_pending_legacy_root_publications(dir.path()).unwrap());

        let mut repaired = working_stdio_mcp_config(dir.path(), "root-retry-fixed", None);
        repaired.servers[0].id = "root-retry".to_string();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&serde_json::json!({"mcp": repaired})).unwrap(),
        )
        .unwrap();
        let repaired_root_event = next_mcp_config_event(&mut feed).await;
        let repaired_health = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let repaired_snapshot = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .mcp
            .snapshot();
        assert!(
            matches!(
                repaired_root_event,
                AgentEvent::ConfigChanged { revision: 2, .. }
            ),
            "unexpected repaired root event: {repaired_root_event:?}; health: {repaired_health:?}; typed: {:?}",
            repaired_snapshot.data
        );
        wait_for_root_outbox_to_clear(dir.path()).await;
        assert!(state.config.read().await.mcp.servers.iter().any(|server| {
            server.id == "root-retry"
                && matches!(
                    &server.transport,
                    TransportConfig::Stdio(stdio)
                        if stdio.args.iter().any(|arg| arg.contains("root-retry-fixed"))
                )
        }));
        let events =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigInvalid { section, .. } if section == "mcp"
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, revision }
                        if section == "mcp" && *revision == 2
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    AgentEvent::ConfigChanged { section, revision }
                        if section == "mcp" && *revision == 1
                ))
                .count(),
            0
        );
    }
}
