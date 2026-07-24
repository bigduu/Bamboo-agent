use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
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
    materialize_facade_effective_config(facade, data_dir).config
}

fn load_committed_effective_config(data_dir: &Path) -> Result<Config, ConfigStoreError> {
    if bamboo_config::section_layout_is_active(data_dir)? {
        let facade = bamboo_config::ConfigFacade::open(data_dir)?;
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
    initial_mcp: bool,
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
                                initial_mcp: false,
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
        if initial_mcp_path.exists() {
            let _ = initial_changes.send(ConfigPathChanges {
                paths: vec![initial_mcp_path],
                initial_mcp: true,
            });
        }

        let apply_provider_health = provider_health.clone();
        let apply_mcp_health = mcp_health.clone();
        let apply_task = tokio::spawn(async move {
            while let Some(changes) = changes_rx.recv().await {
                let watched_sections = config_facade
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
                let provider_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("providers.json")
                });
                let mcp_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("mcp.json")
                });
                let ordinary_watched = watched_sections
                    .iter()
                    .copied()
                    .filter(|id| !matches!(id, SectionId::Providers | SectionId::Mcp))
                    .collect::<Vec<_>>();
                if !provider_watched && !mcp_watched && ordinary_watched.is_empty() {
                    continue;
                }

                // Serialize candidate construction and publication with config
                // writers. Otherwise a slow provider build could later publish
                // a clone taken before an unrelated API update and clobber it.
                let _io = config_io_lock.lock().await;
                if let Some(facade) = config_facade.as_ref() {
                    let mut publishable = Vec::new();
                    for id in ordinary_watched {
                        wait_for_section_file_settle(&data_dir, id).await;
                        let Some(event) = facade.registry().reload_if_changed(id) else {
                            continue;
                        };
                        if matches!(event, ConfigSectionEvent::Invalid { .. }) {
                            publish_registry_event(&account_sink, &event);
                        } else {
                            publishable.push((id, event));
                        }
                    }
                    if !publishable.is_empty() {
                        let materialized = materialize_facade_effective_config(facade, &data_dir);
                        let mut current = config.read().await.clone();
                        let mut applied = Vec::new();
                        for (id, event) in publishable {
                            if materialized.failures.contains(&id) {
                                if let Some(invalid) = facade.registry().mark_runtime_degraded(
                                    id,
                                    "configuration runtime hydration failed; retaining last-known-good runtime",
                                ) {
                                    publish_registry_event(&account_sink, &invalid);
                                }
                                continue;
                            }
                            apply_runtime_section(id, &materialized.config, &mut current);
                            applied.push((id, event));
                        }
                        if !applied.is_empty() {
                            let publishes_env = applied.iter().any(|(id, _)| *id == SectionId::Env);
                            let enforcement_newly_off =
                                !config.read().await.plugin_trust.enforcement_is_off()
                                    && current.plugin_trust.enforcement_is_off();
                            *config.write().await = current.clone();
                            if publishes_env {
                                current.publish_env_vars();
                            }
                            if enforcement_newly_off {
                                warn_plugin_trust_enforcement_off();
                            }
                            for (_, event) in applied {
                                publish_registry_event(&account_sink, &event);
                            }
                        }
                    }
                }
                if provider_watched {
                    if let Some(facade) = config_facade.as_ref() {
                        wait_for_section_file_settle(&data_dir, SectionId::Providers).await;
                        if let Some(event) =
                            facade.registry().reload_if_changed(SectionId::Providers)
                        {
                            if matches!(event, ConfigSectionEvent::Invalid { .. }) {
                                publish_section_failure(
                                &apply_provider_health,
                                &account_sink,
                                "providers",
                                facade.registry().providers.snapshot().status,
                                "provider section is invalid; retaining last-known-good runtime"
                                    .to_string(),
                            );
                            } else {
                                self_write_marker.mark_self_write(provider_store.path());
                                let materialized =
                                    materialize_facade_effective_config(facade, &data_dir);
                                if materialized.failures.contains(&SectionId::Providers) {
                                    facade.registry().mark_runtime_degraded(
                                    SectionId::Providers,
                                    "provider credential hydration failed; retaining last-known-good runtime",
                                );
                                    publish_section_failure(
                                    &apply_provider_health,
                                    &account_sink,
                                    "providers",
                                    SectionStatus::Degraded,
                                    "provider credential hydration failed; retaining last-known-good runtime"
                                        .to_string(),
                                );
                                } else {
                                    let mut candidate = config.read().await.clone();
                                    apply_runtime_section(
                                        SectionId::Providers,
                                        &materialized.config,
                                        &mut candidate,
                                    );
                                    match prepare_provider_candidate(candidate, &data_dir).await {
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
                                            publish_section_success(
                                                &apply_provider_health,
                                                &account_sink,
                                                "providers",
                                                data_dir.join("providers.json"),
                                                recovered,
                                                Some(revision),
                                            );
                                        }
                                        Err(_) => {
                                            facade.registry().mark_runtime_degraded(
                                            SectionId::Providers,
                                            "provider runtime initialization failed; retaining last-known-good runtime",
                                        );
                                            publish_section_failure(
                                            &apply_provider_health,
                                            &account_sink,
                                            "providers",
                                            SectionStatus::Degraded,
                                            "provider runtime initialization failed; retaining last-known-good runtime"
                                                .to_string(),
                                        );
                                        }
                                    }
                                }
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
                                );
                            }
                            Err(error) => publish_section_failure(
                                &apply_provider_health,
                                &account_sink,
                                "providers",
                                candidate_error_status(&error.kind),
                                error.message,
                            ),
                        }
                    }
                }

                if mcp_watched {
                    if let Some(facade) = config_facade.as_ref() {
                        wait_for_section_file_settle(&data_dir, SectionId::Mcp).await;
                        let event =
                            facade
                                .registry()
                                .reload_if_changed(SectionId::Mcp)
                                .or_else(|| {
                                    changes.initial_mcp.then(|| ConfigSectionEvent::Changed {
                                        section: "mcp".to_string(),
                                        revision: facade.registry().mcp.snapshot().revision,
                                    })
                                });
                        let Some(event) = event else {
                            continue;
                        };
                        if matches!(event, ConfigSectionEvent::Invalid { .. }) {
                            publish_section_failure(
                                &apply_mcp_health,
                                &account_sink,
                                "mcp",
                                facade.registry().mcp.snapshot().status,
                                "MCP section is invalid; retaining last-known-good runtime"
                                    .to_string(),
                            );
                        } else {
                            self_write_marker.mark_self_write(mcp_store.path());
                            let materialized =
                                materialize_facade_effective_config(facade, &data_dir);
                            if materialized.failures.contains(&SectionId::Mcp) {
                                facade.registry().mark_runtime_degraded(
                                    SectionId::Mcp,
                                    "MCP credential hydration failed; retaining last-known-good runtime",
                                );
                                publish_section_failure(
                                    &apply_mcp_health,
                                    &account_sink,
                                    "mcp",
                                    SectionStatus::Degraded,
                                    "MCP credential hydration failed; retaining last-known-good runtime"
                                        .to_string(),
                                );
                                continue;
                            }
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
                                    let recovered = section_is_unhealthy(&apply_mcp_health);
                                    let snapshot = facade.registry().mcp.snapshot();
                                    let revision = snapshot.revision;
                                    if changes.initial_mcp
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
                                        publish_section_success(
                                            &apply_mcp_health,
                                            &account_sink,
                                            "mcp",
                                            data_dir.join("mcp.json"),
                                            recovered,
                                            Some(revision),
                                        );
                                    }
                                }
                                Err(_) => {
                                    facade.registry().mark_runtime_degraded(
                                        SectionId::Mcp,
                                        "MCP runtime initialization failed; retaining last-known-good runtime",
                                    );
                                    publish_section_failure(
                                        &apply_mcp_health,
                                        &account_sink,
                                        "mcp",
                                        SectionStatus::Degraded,
                                        "MCP runtime initialization failed; retaining last-known-good runtime"
                                            .to_string(),
                                    )
                                }
                            }
                        }
                    } else {
                        let current_config = config.read().await.clone();
                        let current_revision = apply_mcp_health
                            .read()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .revision;
                        let result = load_and_validate_mcp_candidate(
                            &mcp_store,
                            current_revision,
                            current_config,
                            changes.initial_mcp,
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
                                            );
                                        } else {
                                            publish_section_success(
                                                &apply_mcp_health,
                                                &account_sink,
                                                "mcp",
                                                data_dir.join("mcp.json"),
                                                recovered,
                                                Some(candidate.revision),
                                            );
                                        }
                                    }
                                    Err(_) => publish_section_failure(
                                        &apply_mcp_health,
                                        &account_sink,
                                        "mcp",
                                        SectionStatus::Degraded,
                                        "MCP runtime initialization failed; retaining last-known-good runtime"
                                            .to_string(),
                                    ),
                                }
                            }
                            Err(error) => publish_section_failure(
                                &apply_mcp_health,
                                &account_sink,
                                "mcp",
                                candidate_error_status(&error.kind),
                                error.message,
                            ),
                        }
                    }
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

async fn wait_for_section_file_settle(data_dir: &Path, id: SectionId) {
    let path = data_dir.join(id.descriptor().file_name);
    for _ in 0..3 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn publish_registry_event(
    account_sink: &bamboo_engine::events::AccountEventSink,
    event: &ConfigSectionEvent,
) {
    let event = match event {
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
    };
    account_sink.record(None, &event);
}

fn publish_exact_facade_commit(
    facade: Option<&std::sync::Arc<bamboo_config::ConfigFacade>>,
    account_sink: &bamboo_engine::events::AccountEventSink,
    sections: &[SectionId],
) -> Result<(), AppError> {
    let Some(facade) = facade else {
        return Ok(());
    };
    for section in sections {
        let Some(event) = facade.registry().reload_if_changed(*section) else {
            continue;
        };
        publish_registry_event(account_sink, &event);
        if matches!(event, ConfigSectionEvent::Invalid { .. }) {
            return Err(AppError::InternalError(anyhow::anyhow!(
                "committed configuration section became invalid before publication"
            )));
        }
    }
    Ok(())
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
        SectionId::Subagents => *target.subagents_mut() = source.subagents().clone(),
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

fn section_is_unhealthy(health: &std::sync::RwLock<ConfigLiveHealth>) -> bool {
    health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .status
        != SectionStatus::Healthy
}

fn publish_section_success(
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
    account_sink.record(None, &event);
}

fn publish_section_failure(
    health: &std::sync::RwLock<ConfigLiveHealth>,
    account_sink: &bamboo_engine::events::AccountEventSink,
    section: &str,
    status: SectionStatus,
    message: String,
) {
    let revision = update_live_health(health, status, Some(message), false, None);
    account_sink.record(
        None,
        &AgentEvent::ConfigInvalid {
            section: section.to_string(),
            revision,
        },
    );
}

fn publish_mcp_backup_lkg(
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
    account_sink.record(
        None,
        &AgentEvent::ConfigInvalid {
            section: "mcp".to_string(),
            revision,
        },
    );
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

#[derive(Debug)]
pub(crate) enum ConfigSectionMutationError {
    Store(ConfigStoreError),
    Invalid(String),
    Runtime(String),
}

impl AppState {
    /// Commit a single ordinary typed section with CAS, then publish exactly
    /// that section into the process-owned effective snapshot. Credential
    /// bindings are server-owned and cannot be forged or detached here.
    pub(crate) async fn put_ordinary_section(
        &self,
        id: SectionId,
        expected_revision: u64,
        candidate: Value,
    ) -> Result<u64, ConfigSectionMutationError> {
        if matches!(
            id,
            SectionId::Providers | SectionId::Mcp | SectionId::Credentials
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
        let current = facade
            .registry()
            .envelope_value(id)
            .map_err(ConfigSectionMutationError::Store)?;
        if credential_reference_inventory(&current.data)
            != credential_reference_inventory(&candidate)
        {
            return Err(ConfigSectionMutationError::Invalid(
                "credential references are server-managed; use the credential or domain API"
                    .to_string(),
            ));
        }

        let event = facade
            .registry()
            .commit_value(id, expected_revision, candidate)
            .map_err(ConfigSectionMutationError::Store)?;
        let revision = match &event {
            ConfigSectionEvent::Changed { revision, .. }
            | ConfigSectionEvent::Recovered { revision, .. }
            | ConfigSectionEvent::Invalid { revision, .. } => *revision,
        };
        let materialized = materialize_facade_effective_config(facade, &self.app_data_dir);
        if materialized.failures.contains(&id) {
            let message =
                "configuration runtime hydration failed; retaining last-known-good runtime"
                    .to_string();
            if let Some(invalid) = facade.registry().mark_runtime_degraded(id, message.clone()) {
                publish_registry_event(&self.account_sink, &invalid);
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
        publish_registry_event(&self.account_sink, &event);
        Ok(revision)
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
                    );
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
        );
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
                        );
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
            let committed = tokio::task::spawn_blocking(move || {
                let mut durable_candidate = candidate;
                bamboo_config::persist_provider_credential_transaction_at_revision(
                    &transaction_dir,
                    &mut durable_candidate,
                    &provider_intents,
                    &provider_instance_intents,
                    expected_revision,
                )?;
                load_committed_effective_config(&transaction_dir)
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "provider settings transaction task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;

            committed.publish_env_vars();
            *live_config = committed;
            provider_registry.replace_with(registry);
            *live_provider = candidate_provider;
            publish_exact_facade_commit(
                Some(facade),
                &account_sink,
                &[SectionId::Credentials, SectionId::Providers],
            )
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
            let candidate = tokio::task::spawn_blocking(move || {
                bamboo_config::persist_provider_reset_credential_transaction_at_revision(
                    &transaction_dir,
                    &mut candidate,
                    &provider_intents,
                    &provider_instance_intents,
                    expected_revision,
                )?;
                load_committed_effective_config(&transaction_dir)
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "provider reset transaction task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;

            *config.write().await = candidate.clone();
            publish_exact_facade_commit(
                Some(facade),
                &account_sink,
                &[SectionId::Credentials, SectionId::Providers],
            )
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

            match bamboo_llm::ProviderRegistry::from_config(&candidate, app_data_dir).await {
                Ok(registry) => {
                    if let Some(candidate_provider) = registry.get_default() {
                        provider_registry.replace_with(registry);
                        *provider.write().await = candidate_provider;
                    } else {
                        publish_section_failure(
                            &config_live_health,
                            &account_sink,
                            "providers",
                            SectionStatus::Degraded,
                            "provider reset committed; default provider is not initialized"
                                .to_string(),
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "provider reset committed but runtime initialization failed");
                    publish_section_failure(
                        &config_live_health,
                        &account_sink,
                        "providers",
                        SectionStatus::Degraded,
                        "provider reset committed; retaining last-known-good runtime".to_string(),
                    );
                }
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
            );
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
        );
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
            let transaction_dir = app_data_dir.clone();
            let mut durable_candidate = current;
            durable_candidate.mcp = runtime_candidate.clone();
            let result = mcp_manager
                .reconcile_from_config_transactional_after(&runtime_candidate, || async {
                    let mut live_config = config.write().await;
                    let commit = tokio::task::spawn_blocking(move || {
                        bamboo_config::persist_mcp_credential_transaction_at_revision(
                            &transaction_dir,
                            &mut durable_candidate,
                            &credential_intents,
                            expected_revision,
                        )?;
                        load_committed_effective_config(&transaction_dir)
                    })
                    .await;
                    match commit {
                        Ok(Ok(committed)) => {
                            *live_config = committed;
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
                );
                return Err(ConfigSectionMutationError::Runtime(message));
            }

            publish_exact_facade_commit(
                Some(facade),
                &account_sink,
                &[SectionId::Credentials, SectionId::Mcp],
            )
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
        let mut committed_config = None;
        let mut store_error = None;
        let data_dir = self.app_data_dir.clone();
        let result = self
            .mcp_manager
            .reconcile_from_config_transactional_after(&candidate_mcp, || async {
                let mut live_config = self.config.write().await;
                match bamboo_config::persist_mcp_reset_credential_transaction_at_revision(
                    &data_dir,
                    &mut candidate_config,
                    expected_revision,
                )
                .and_then(|_| load_committed_effective_config(&data_dir))
                {
                    Ok(committed) => {
                        *live_config = committed.clone();
                        committed_config = Some(committed);
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
        if result.is_err() || committed_config.is_none() {
            let message =
                "MCP reset runtime initialization failed; retaining last-known-good runtime"
                    .to_string();
            publish_section_failure(
                &self.mcp_config_live_health,
                &self.account_sink,
                "mcp",
                SectionStatus::Degraded,
                message.clone(),
            );
            return Err(ConfigSectionMutationError::Runtime(message));
        }
        publish_exact_facade_commit(
            Some(facade),
            &self.account_sink,
            &[SectionId::Credentials, SectionId::Mcp],
        )
        .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;
        let revision = facade.registry().mcp.snapshot().revision;
        publish_section_success(
            &self.mcp_config_live_health,
            &self.account_sink,
            "mcp",
            self.app_data_dir.join("mcp.json"),
            section_is_unhealthy(&self.mcp_config_live_health),
            Some(revision),
        );
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
    ) -> Result<u64, ConfigSectionMutationError> {
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
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
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
            let candidate = tokio::task::spawn_blocking(move || {
                bamboo_config::persist_credential_backed_section_reset_at_revision(
                    &transaction_dir,
                    &mut candidate,
                    id,
                    expected_revision,
                )?;
                load_committed_effective_config(&transaction_dir)
            })
            .await
            .map_err(|error| {
                ConfigSectionMutationError::Runtime(format!(
                    "section reset transaction task failed: {error}"
                ))
            })?
            .map_err(ConfigSectionMutationError::Store)?;

            if id == SectionId::Env {
                candidate.publish_env_vars();
            }
            *config.write().await = candidate.clone();
            publish_exact_facade_commit(Some(facade), &account_sink, &[SectionId::Credentials, id])
                .map_err(|error| ConfigSectionMutationError::Runtime(error.to_string()))?;

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

            facade
                .registry()
                .envelope_value(id)
                .map(|envelope| envelope.revision)
                .map_err(ConfigSectionMutationError::Store)
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
        let new_config = self
            .config_facade
            .as_ref()
            .map(|facade| load_facade_effective_config(facade, &self.app_data_dir))
            .unwrap_or_else(|| {
                Config::from_data_dir_without_publish(Some(self.app_data_dir.clone()))
            });
        new_config.publish_env_vars();
        *config = new_config.clone();
        new_config
    }

    async fn persist_config_snapshot(&self, config: Config) -> anyhow::Result<()> {
        let data_dir = self.app_data_dir.clone();
        if self.config_facade.is_some() {
            tokio::task::spawn_blocking(move || {
                bamboo_config::persist_facade_effective_config(data_dir, &config)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
            let sections = bamboo_config::SECTION_DESCRIPTORS
                .iter()
                .map(|descriptor| descriptor.id)
                .collect::<Vec<_>>();
            publish_exact_facade_commit(self.config_facade.as_ref(), &self.account_sink, &sections)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        } else {
            tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
                .await
                .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
        }
        Ok(())
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
        // Hold the config-IO lock across BOTH the in-memory mutation AND the disk
        // persist, so a concurrent reload_config can't read the disk in the gap
        // before we persist and then clobber this mutation with the stale copy
        // (#126). The lock is dropped before apply_config_effects — slow side
        // effects (provider reload) don't need to block reloads/other updates.
        let snapshot = {
            let _io = self.config_io_lock.lock().await;
            let (snapshot, enforcement_newly_off) = {
                let cfg = self.config.read().await;
                // Refuse the whole operation (no in-memory mutation, no disk
                // write) while a config-corruption recovery is pending
                // confirmation (#153) — `save_to_dir` would reject the persist
                // anyway, but checking here BEFORE `update()` runs keeps the
                // in-memory config frozen exactly at the recovered state
                // instead of silently drifting further from what's on disk.
                reject_if_recovery_pending(&cfg)?;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                let mut candidate = cfg.clone();
                update(&mut candidate)?;
                // Backfill any missing connect.platforms id (#496) on the live
                // in-memory config itself — not just inside `save_to_dir`'s
                // internal save-copy — so the response this update returns
                // (and any GET immediately after) already reflects the id a
                // client can round-trip on its next PATCH.
                candidate.assign_connect_platform_ids();
                // Same live-vs-save-copy treatment for ciphertext (#516):
                // `save_to_dir` refreshes `*_encrypted` only on its save-time
                // clone, so a secret set through this entrypoint (e.g. a
                // provider instance created over HTTP) would otherwise stay
                // plaintext-only in memory — and the next settings-PATCH merge
                // (`build_merged_config`'s serde round-trip drops plaintext)
                // would lose the key entirely.
                candidate.refresh_encrypted_secrets().map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {e}"
                    ))
                })?;
                let newly_off = !was_off && candidate.plugin_trust.enforcement_is_off();
                (candidate, newly_off)
            };
            // Loud signal at the MOMENT plugin_trust.enforcement is flipped off
            // live (e.g. via `bamboo config set plugin_trust.enforcement off`
            // over HTTP), mirroring the boot-time warn in `AppState::new` — so
            // this security-relevant relaxation is never applied silently. Only
            // on the transition into `Off` (not every unrelated config write
            // while already off).
            if enforcement_newly_off {
                warn_plugin_trust_enforcement_off();
            }
            self.persist_config_snapshot(snapshot.clone())
                .await
                .map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}"))
                })?;
            let snapshot = self
                .config_facade
                .as_ref()
                .map(|facade| load_facade_effective_config(facade, &self.app_data_dir))
                .unwrap_or(snapshot);
            {
                let mut cfg = self.config.write().await;
                snapshot.publish_env_vars();
                *cfg = snapshot.clone();
            }
            snapshot
        };

        self.apply_config_effects(snapshot.clone(), effects).await?;
        Ok(snapshot)
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
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();
        let (snapshot, enforcement_newly_off) = {
            let _io = self.config_io_lock.lock().await;
            let (mut candidate, enforcement_newly_off) = {
                let cfg = self.config.read().await;
                reject_if_recovery_pending(&cfg)?;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                let mut candidate = cfg.clone();
                update(&mut candidate)?;
                // Provider plaintext may be present until the exact credential
                // transaction assigns its durable reference. Compare every
                // other section against a candidate whose provider projection
                // is replaced with the current durable domain; the exact
                // transaction below remains the sole provider validator.
                let mut non_provider_candidate = candidate.clone();
                apply_runtime_section(SectionId::Providers, &cfg, &mut non_provider_candidate);
                let mut changed =
                    bamboo_config::changed_facade_sections(&cfg, &non_provider_candidate).map_err(
                        |_| {
                            AppError::InternalError(anyhow::anyhow!(
                                "failed to compare modular configuration sections"
                            ))
                        },
                    )?;
                // The compatibility wire shape can normalize duplicated
                // external-broker metadata while leaving the actual subagents
                // module byte-for-byte equivalent. That is an unchanged echo,
                // not a second section mutation.
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
                candidate.assign_connect_platform_ids();
                candidate.refresh_encrypted_secrets().map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {error}"
                    ))
                })?;
                let newly_off = !was_off && candidate.plugin_trust.enforcement_is_off();
                (candidate, newly_off)
            };
            let data_dir = self.app_data_dir.clone();
            let candidate = tokio::task::spawn_blocking(move || {
                bamboo_config::persist_provider_instance_credential_transaction(
                    &data_dir,
                    &mut candidate,
                    &provider_intents,
                    &provider_instance_intents,
                )?;
                load_committed_effective_config(&data_dir)
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::Providers],
            )?;
            {
                let mut cfg = self.config.write().await;
                candidate.publish_env_vars();
                *cfg = candidate.clone();
            }
            (candidate, enforcement_newly_off)
        };
        if enforcement_newly_off {
            warn_plugin_trust_enforcement_off();
        }
        self.apply_config_effects(snapshot.clone(), effects).await?;
        Ok(snapshot)
    }

    /// Mutate user env vars through the recoverable credentials.json +
    /// config.json exact transaction. The detached task owns the mutation so
    /// request cancellation cannot strand durable metadata ahead of runtime.
    pub async fn update_env_var_credentials<F>(
        &self,
        expected_revision: u64,
        env_intents: std::collections::BTreeSet<String>,
        update: F,
    ) -> Result<(Config, u64), AppError>
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
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                let mut candidate = current.clone();
                update(&mut candidate)?;
                candidate.assign_connect_platform_ids();
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let (candidate, revision) = tokio::task::spawn_blocking(move || {
                let revision = bamboo_config::persist_env_var_credential_transaction_at_revision(
                    &transaction_dir,
                    &mut candidate,
                    &env_intents,
                    expected_revision,
                )?;
                // Reload from the durable authority selected by the layout.
                // Active modular layouts never publish stale config.json.
                let committed = load_committed_effective_config(&transaction_dir)?;
                Ok::<_, ConfigStoreError>((committed, revision))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::Env],
            )?;
            candidate.publish_env_vars();
            *config.write().await = candidate.clone();
            Ok::<_, AppError>((candidate, revision))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "env credential transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate notification metadata and ntfy/Bark credentials through the
    /// recoverable credentials.json + config.json exact transaction. The
    /// detached task completes durable commit and live publication even if the
    /// initiating HTTP request is cancelled.
    pub async fn update_notification_credentials<F>(
        &self,
        expected_revision: u64,
        secret_intents: std::collections::BTreeSet<String>,
        reset_domain: bool,
        update: F,
    ) -> Result<(Config, u64), AppError>
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
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                let mut candidate = current.clone();
                update(&mut candidate)?;
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let (candidate, revision) = tokio::task::spawn_blocking(move || {
                let revision =
                    bamboo_config::persist_notification_credential_transaction_at_revision_with_reset(
                            &transaction_dir,
                            &mut candidate,
                            &secret_intents,
                            reset_domain,
                            expected_revision,
                        )?;
                let committed = load_committed_effective_config(&transaction_dir)?;
                Ok::<_, ConfigStoreError>((committed, revision))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::Notifications],
            )?;
            *config.write().await = candidate.clone();
            Ok::<_, AppError>((candidate, revision))
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
    ) -> Result<(Config, u64), AppError>
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
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                let mut candidate = current.clone();
                update(&mut candidate)?;
                candidate.assign_connect_platform_ids();
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let (candidate, revision) = tokio::task::spawn_blocking(move || {
                let revision = bamboo_config::persist_connect_credential_transaction_at_revision(
                    &transaction_dir,
                    &mut candidate,
                    &secret_intents,
                    expected_revision,
                )?;
                let committed = load_committed_effective_config(&transaction_dir)?;
                Ok::<_, ConfigStoreError>((committed, revision))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::Connect],
            )?;
            *config.write().await = candidate.clone();
            Ok::<_, AppError>((candidate, revision))
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
    ) -> Result<(Config, u64), AppError>
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
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                let mut candidate = current.clone();
                update(&mut candidate)?;
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let (candidate, revision) = tokio::task::spawn_blocking(move || {
                let revision =
                    bamboo_config::persist_access_control_credential_transaction_at_revision(
                        &transaction_dir,
                        &mut candidate,
                        password_intent,
                        &device_intents,
                        expected_revision,
                    )?;
                let committed = load_committed_effective_config(&transaction_dir)?;
                Ok::<_, ConfigStoreError>((committed, revision))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::AccessControl],
            )?;
            *config.write().await = candidate.clone();
            Ok::<_, AppError>((candidate, revision))
        });
        transaction.await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "access-control credential transaction task failed: {error}"
            ))
        })?
    }

    /// Mutate cluster-fabric node metadata and SSH credentials through the
    /// recoverable credentials.json + config.json exact transaction. The
    /// detached task owns durable commit, committed-root reload, and live
    /// publication so request cancellation cannot leave runtime behind disk.
    pub async fn update_cluster_fabric_credentials<F>(
        &self,
        expected_revision: u64,
        node_intents: std::collections::BTreeSet<String>,
        update: F,
    ) -> Result<(Config, u64), AppError>
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
            let mut candidate = {
                let current = config.read().await;
                reject_if_recovery_pending(&current)?;
                let mut candidate = current.clone();
                update(&mut candidate)?;
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let (candidate, revision) = tokio::task::spawn_blocking(move || {
                let revision =
                    bamboo_config::persist_cluster_fabric_credential_transaction_at_revision(
                        &transaction_dir,
                        &mut candidate,
                        &node_intents,
                        expected_revision,
                    )?;
                let committed = load_committed_effective_config(&transaction_dir)?;
                Ok::<_, ConfigStoreError>((committed, revision))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;
            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::ClusterFabric],
            )?;
            *config.write().await = candidate.clone();
            Ok::<_, AppError>((candidate, revision))
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
            bamboo_config::CredentialStatus,
            bamboo_config::CredentialStoreHealth,
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
            bamboo_config::CredentialStatus,
            bamboo_config::CredentialStoreHealth,
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
        let config_facade = self.config_facade.clone();
        let account_sink = self.account_sink.clone();

        // This task owns the mutation after dispatch. Dropping the request's
        // JoinHandle does not cancel it, so the blocking durable transaction,
        // live publication, and runtime convergence complete as one serialized
        // operation even when the caller disconnects.
        let transaction = tokio::spawn(async move {
            let _io = config_io_lock.lock().await;
            let mut candidate = {
                let cfg = config.read().await;
                reject_if_recovery_pending(&cfg)?;
                let mut candidate = cfg.clone();
                update(&mut candidate);
                candidate.assign_connect_platform_ids();
                candidate.refresh_encrypted_secrets().map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {error}"
                    ))
                })?;
                candidate
            };
            let transaction_dir = app_data_dir.clone();
            let status_reference =
                candidate
                    .proxy_auth_credential_ref
                    .clone()
                    .unwrap_or_else(|| {
                        bamboo_config::CredentialRef::parse("proxy.default.auth")
                            .expect("canonical proxy credential reference is valid")
                    });
            let (candidate, reference) = tokio::task::spawn_blocking(move || {
                bamboo_config::persist_proxy_auth_credential_transaction_at_revision(
                    &transaction_dir,
                    &mut candidate,
                    expected_revision,
                )?;
                Ok::<_, ConfigStoreError>((
                    load_committed_effective_config(&transaction_dir)?,
                    status_reference,
                ))
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
                ConfigStoreError::Io(error) => AppError::StorageError(error),
                ConfigStoreError::Json(_) => {
                    AppError::BadRequest("configuration document is invalid".to_string())
                }
                ConfigStoreError::Watch(error) => {
                    AppError::InternalError(anyhow::anyhow!("configuration watch failed: {error}"))
                }
            })?;

            publish_exact_facade_commit(
                config_facade.as_ref(),
                &account_sink,
                &[SectionId::Credentials, SectionId::Core],
            )?;

            // No fallible metadata read occurs before publication. Once the
            // transaction commits, a response error can no longer leave live
            // config behind its durable credential/config pair.
            candidate.publish_env_vars();
            *config.write().await = candidate.clone();

            if effects.reload_provider {
                match bamboo_llm::ProviderRegistry::from_config(&candidate, app_data_dir.clone())
                    .await
                {
                    Ok(candidate_registry) => {
                        if let Some(candidate_provider) = candidate_registry.get_default() {
                            let mut live_provider = provider.write().await;
                            provider_registry.replace_with(candidate_registry);
                            *live_provider = candidate_provider;
                        } else {
                            tracing::warn!(
                                "proxy auth committed but provider reload had no default provider"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "proxy auth committed but provider reload failed"
                        );
                    }
                }
            }

            if effects.reconcile_mcp {
                mcp_manager.reconcile_from_config(&candidate.mcp).await;
            }

            let (status, health) =
                credential_store
                    .status_with_health(&reference)
                    .map_err(|error| match error {
                        ConfigStoreError::Conflict { expected, actual } => {
                            AppError::ConfigConflict { expected, actual }
                        }
                        ConfigStoreError::Validation(_) | ConfigStoreError::Json(_) => {
                            AppError::InternalError(anyhow::anyhow!(
                                "credential store validation failed"
                            ))
                        }
                        ConfigStoreError::Io(error) => AppError::StorageError(error),
                        ConfigStoreError::Watch(error) => AppError::InternalError(anyhow::anyhow!(
                            "configuration watch failed: {error}"
                        )),
                    })?;
            Ok::<_, AppError>((candidate, status, health))
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
        new_config.assign_connect_platform_ids();
        // Keep ciphertext in sync with plaintext on the config that becomes
        // the live in-memory state — same #516 rationale as `update_config`.
        new_config.refresh_encrypted_secrets().map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to refresh encrypted secrets: {e}"))
        })?;

        // Same #126 serialization as update_config: mutate + persist under the
        // config-IO lock so a reload can't interleave; effects run unlocked.
        let new_config = {
            let _io = self.config_io_lock.lock().await;
            let was_off = {
                let cfg = self.config.read().await;
                // Same guard as `update_config` (#153): a full-config replace
                // must not silently blow away an unconfirmed recovery either.
                reject_if_recovery_pending(&cfg)?;
                cfg.plugin_trust.enforcement_is_off()
            };
            self.persist_config_snapshot(new_config.clone())
                .await
                .map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}"))
                })?;
            let published = self
                .config_facade
                .as_ref()
                .map(|facade| load_facade_effective_config(facade, &self.app_data_dir))
                .unwrap_or(new_config);
            let enforcement_newly_off = !was_off && published.plugin_trust.enforcement_is_off();
            published.publish_env_vars();
            *self.config.write().await = published.clone();
            // Same live signal as `update_config` — a full-config replace (JSON
            // merge / PATCH endpoints) that transitions plugin_trust.enforcement
            // into `Off` must warn just as loudly as a targeted set.
            if enforcement_newly_off {
                warn_plugin_trust_enforcement_off();
            }
            published
        };

        self.apply_config_effects(new_config.clone(), effects)
            .await?;
        Ok(new_config)
    }

    async fn apply_config_effects(
        &self,
        new_config: Config,
        effects: ConfigUpdateEffects,
    ) -> Result<(), AppError> {
        if effects.reload_provider {
            self.reload_provider().await.map_err(|e| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reload provider after updating config: {e}"
                ))
            })?;
        }

        if effects.reconcile_mcp {
            self.mcp_manager
                .reconcile_from_config(&new_config.mcp)
                .await;
        }

        Ok(())
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

    fn mcp_document_bytes(revision: u64, config: &McpConfig) -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "revision": revision,
            "data": config,
        }))
        .unwrap()
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
                            reload_provider: true,
                            reconcile_mcp: true,
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
        let TransportConfig::Stdio(stdio) = &runtime.mcp.servers[0].transport else {
            panic!("stdio transport")
        };
        assert_eq!(stdio.env["TOKEN"], "env-b");
        let TransportConfig::Sse(sse) = &runtime.mcp.servers[1].transport else {
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
        let TransportConfig::Stdio(stdio) = &runtime.mcp.servers[0].transport else {
            panic!("stdio transport")
        };
        assert_eq!(stdio.env["TOKEN"], "env-b");
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
}
