use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bamboo_config::{
    AtomicJsonStore, ConfigDirectoryWatcher, ProviderConfigs, SectionSourceKind, SectionStatus,
};
use bamboo_mcp::{McpConfig, McpServerManager, TransportConfig};
use chrono::{DateTime, Utc};
use serde::Serialize;

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
        let provider_health = Arc::new(std::sync::RwLock::new(initial_provider_health(&data_dir)));
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
                let provider_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("providers.json")
                });
                let mcp_watched = changes.paths.iter().any(|path| {
                    path.file_name().and_then(|name| name.to_str()) == Some("mcp.json")
                });
                if !provider_watched && !mcp_watched {
                    continue;
                }

                // Serialize candidate construction and publication with config
                // writers. Otherwise a slow provider build could later publish
                // a clone taken before an unrelated API update and clobber it.
                let _io = config_io_lock.lock().await;
                if provider_watched {
                    let current_config = config.read().await.clone();
                    let result =
                        load_and_prepare_provider_candidate(&data_dir, current_config).await;
                    match result {
                        Ok((candidate_config, candidate_registry, candidate_provider)) => {
                            let mut live_config = config.write().await;
                            let mut live_provider = provider.write().await;
                            let recovered = section_is_unhealthy(&apply_provider_health);
                            candidate_config.publish_env_vars();
                            *live_config = candidate_config;
                            provider_registry.replace_with(candidate_registry);
                            *live_provider = candidate_provider;
                            drop(live_provider);
                            drop(live_config);

                            publish_section_success(
                                &apply_provider_health,
                                &account_sink,
                                "providers",
                                data_dir.join("providers.json"),
                                recovered,
                                None,
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

                if mcp_watched {
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
                            match mcp_manager
                                .reconcile_from_config_transactional(&candidate.config.mcp)
                                .await
                            {
                                Ok(()) => {
                                    let recovered = section_is_unhealthy(&apply_mcp_health);
                                    config.write().await.mcp = candidate.config.mcp.clone();
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

fn initial_provider_health(data_dir: &std::path::Path) -> ConfigLiveHealth {
    let primary = data_dir.join("providers.json");
    if !primary.exists() {
        return ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: primary,
            source_kind: SectionSourceKind::Default,
            status: SectionStatus::Missing,
            last_error: None,
        };
    }
    if std::fs::read(&primary)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ProviderConfigs>(&bytes).ok())
        .is_some()
    {
        return ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: primary,
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Healthy,
            last_error: None,
        };
    }

    let backup = data_dir.join("providers.json.bak");
    if std::fs::read(&backup)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ProviderConfigs>(&bytes).ok())
        .is_some()
    {
        ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: backup,
            source_kind: SectionSourceKind::Backup,
            status: SectionStatus::Degraded,
            last_error: Some(
                "primary provider section invalid; using last-known-good backup".to_string(),
            ),
        }
    } else {
        ConfigLiveHealth {
            revision: 0,
            loaded_at: Utc::now(),
            source_path: primary,
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Invalid,
            last_error: Some("provider section could not be parsed or read".to_string()),
        }
    }
}

fn initial_mcp_health(store: &AtomicJsonStore<McpConfig>) -> ConfigLiveHealth {
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
    data_dir: &std::path::Path,
    mut candidate_config: Config,
) -> Result<(Config, bamboo_llm::ProviderRegistry, Arc<dyn LLMProvider>), ProviderCandidateError> {
    // Editors commonly implement save as delete/rename/create. Retry a missing
    // watched file briefly instead of treating the transient gap as a reset.
    for _ in 0..3 {
        if data_dir.join("providers.json").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let providers_path = data_dir.join("providers.json");
    let bytes = std::fs::read(&providers_path).map_err(|_| {
        if providers_path.exists() {
            ProviderCandidateError::invalid("provider section is unreadable")
        } else {
            ProviderCandidateError::missing()
        }
    })?;
    let providers = serde_json::from_slice::<ProviderConfigs>(&bytes)
        .map_err(|_| ProviderCandidateError::invalid("provider section is invalid"))?;
    *candidate_config.providers_mut() = providers;
    candidate_config.hydrate_provider_api_keys_from_encrypted();
    let candidate_registry =
        bamboo_llm::ProviderRegistry::from_config(&candidate_config, data_dir.to_path_buf())
            .await
            .map_err(|_| ProviderCandidateError::runtime())?;
    let candidate_provider = candidate_registry
        .get_default()
        .ok_or_else(ProviderCandidateError::runtime)?;
    Ok((candidate_config, candidate_registry, candidate_provider))
}

async fn load_and_validate_mcp_candidate(
    store: &AtomicJsonStore<McpConfig>,
    current_revision: u64,
    mut candidate_config: Config,
    allow_startup_backup: bool,
) -> Result<McpCandidate, ProviderCandidateError> {
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
    let stored = store
        .load_validated_for_reload(current_revision, &candidate_config.mcp, validate_mcp_config)
        .map_err(|_| ProviderCandidateError::invalid("MCP section is invalid"))?
        .ok_or_else(|| ProviderCandidateError::missing_section("MCP section is missing"))?;
    if stored.recovered_from_backup && !allow_startup_backup {
        return Err(ProviderCandidateError::invalid(
            "primary MCP section is invalid; retaining last-known-good runtime",
        ));
    }
    candidate_config.mcp = stored.data;
    candidate_config.hydrate_mcp_secrets_from_encrypted();
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
    })
}

struct McpCandidate {
    config: Config,
    revision: u64,
    source_kind: SectionSourceKind,
    source_path: PathBuf,
    normalized_external_revision: bool,
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
        // Read from disk INSIDE the write lock. If the disk read happened before
        // acquiring the lock, a concurrent update_config() could persist new
        // state in that gap and then be clobbered here by the stale disk copy
        // (in-memory-only mutations silently lost). Holding the lock across the
        // read+swap serializes reload with update_config's in-memory mutation.
        // Config::from_data_dir is a sync read (no await), so this doesn't hold
        // the lock across an await point. #41.
        // Hold the config-IO lock across the read+swap so it can't interleave
        // with a config write's mutate+persist (which would let us read the disk
        // BEFORE that write persisted, then clobber its in-memory mutation). #126.
        let _io = self.config_io_lock.lock().await;
        let mut config = self.config.write().await;
        let new_config = Config::from_data_dir(Some(self.app_data_dir.clone()));
        *config = new_config.clone();
        new_config
    }

    async fn persist_config_snapshot(&self, config: Config) -> anyhow::Result<()> {
        let data_dir = self.app_data_dir.clone();
        tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("Config save task failed: {e}"))??;
        Ok(())
    }

    /// Unified config update entrypoint.
    ///
    /// Invariants:
    /// - Update in-memory first
    /// - Persist to disk
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
                let mut cfg = self.config.write().await;
                // Refuse the whole operation (no in-memory mutation, no disk
                // write) while a config-corruption recovery is pending
                // confirmation (#153) — `save_to_dir` would reject the persist
                // anyway, but checking here BEFORE `update()` runs keeps the
                // in-memory config frozen exactly at the recovered state
                // instead of silently drifting further from what's on disk.
                reject_if_recovery_pending(&cfg)?;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                update(&mut cfg)?;
                // Backfill any missing connect.platforms id (#496) on the live
                // in-memory config itself — not just inside `save_to_dir`'s
                // internal save-copy — so the response this update returns
                // (and any GET immediately after) already reflects the id a
                // client can round-trip on its next PATCH.
                cfg.assign_connect_platform_ids();
                // Same live-vs-save-copy treatment for ciphertext (#516):
                // `save_to_dir` refreshes `*_encrypted` only on its save-time
                // clone, so a secret set through this entrypoint (e.g. a
                // provider instance created over HTTP) would otherwise stay
                // plaintext-only in memory — and the next settings-PATCH merge
                // (`build_merged_config`'s serde round-trip drops plaintext)
                // would lose the key entirely.
                cfg.refresh_encrypted_secrets().map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to refresh encrypted secrets: {e}"
                    ))
                })?;
                cfg.publish_env_vars();
                let newly_off = !was_off && cfg.plugin_trust.enforcement_is_off();
                (cfg.clone(), newly_off)
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
            snapshot
        };

        self.apply_config_effects(snapshot.clone(), effects).await?;
        Ok(snapshot)
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
        {
            let _io = self.config_io_lock.lock().await;
            let enforcement_newly_off = {
                let mut cfg = self.config.write().await;
                // Same guard as `update_config` (#153): a full-config replace
                // must not silently blow away an unconfirmed recovery either.
                reject_if_recovery_pending(&cfg)?;
                let was_off = cfg.plugin_trust.enforcement_is_off();
                *cfg = new_config.clone();
                cfg.publish_env_vars();
                !was_off && cfg.plugin_trust.enforcement_is_off()
            };
            // Same live signal as `update_config` — a full-config replace (JSON
            // merge / PATCH endpoints) that transitions plugin_trust.enforcement
            // into `Off` must warn just as loudly as a targeted set.
            if enforcement_newly_off {
                warn_plugin_trust_enforcement_off();
            }
            self.persist_config_snapshot(new_config.clone())
                .await
                .map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!("Failed to save config: {e}"))
                })?;
        }

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

    async fn wait_for_mcp_health(
        state: &AppState,
        status: SectionStatus,
        minimum_revision: u64,
    ) -> ConfigLiveHealth {
        tokio::time::timeout(Duration::from_secs(4), async {
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
        .expect("MCP health transition timed out")
    }

    async fn next_mcp_config_event(
        feed: &mut tokio::sync::broadcast::Receiver<Arc<bamboo_engine::events::ChangeEvent>>,
    ) -> AgentEvent {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let envelope = feed.recv().await.expect("account feed remains open");
                match &envelope.event {
                    AgentEvent::ConfigChanged { section, .. }
                    | AgentEvent::ConfigInvalid { section, .. }
                    | AgentEvent::ConfigRecovered { section, .. }
                        if section == "mcp" =>
                    {
                        break envelope.event.clone();
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("MCP config event timed out")
    }

    #[test]
    fn initial_provider_health_validates_primary_and_backup() {
        let dir = tempfile::tempdir().unwrap();
        let missing = initial_provider_health(dir.path());
        assert_eq!(missing.status, SectionStatus::Missing);
        assert_eq!(missing.source_kind, SectionSourceKind::Default);

        std::fs::write(dir.path().join("providers.json"), b"{broken").unwrap();
        let invalid = initial_provider_health(dir.path());
        assert_eq!(invalid.status, SectionStatus::Invalid);
        assert_eq!(invalid.source_kind, SectionSourceKind::File);

        std::fs::write(dir.path().join("providers.json.bak"), b"{}").unwrap();
        let recovered = initial_provider_health(dir.path());
        assert_eq!(recovered.status, SectionStatus::Degraded);
        assert_eq!(recovered.source_kind, SectionSourceKind::Backup);
        assert!(recovered
            .last_error
            .as_deref()
            .unwrap()
            .contains("last-known-good backup"));

        std::fs::write(dir.path().join("providers.json"), b"{}").unwrap();
        let healthy = initial_provider_health(dir.path());
        assert_eq!(healthy.status, SectionStatus::Healthy);
        assert_eq!(healthy.source_kind, SectionSourceKind::File);
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
            assert_eq!(health.source_kind, SectionSourceKind::Default);
            assert_eq!(health.source_path, providers_path);
        }
        assert!(Arc::ptr_eq(
            &state.provider_registry.get_default().unwrap(),
            &working
        ));
        let invalid = tokio::time::timeout(Duration::from_secs(2), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            invalid.event,
            AgentEvent::ConfigInvalid { revision: 0, .. }
        ));

        let providers = ProviderConfigs {
            openai: Some(bamboo_config::OpenAIConfig {
                api_key_encrypted: Some(
                    bamboo_config::encryption::encrypt("watcher-test-key").unwrap(),
                ),
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
        let recovered = tokio::time::timeout(Duration::from_secs(2), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            recovered.event,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));
        assert_eq!(state.provider_registry.default_provider_name(), "openai");
    }

    #[tokio::test]
    async fn mcp_watcher_updates_lkg_rejects_invalid_and_recovers_after_atomic_replace() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x45; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
        let mut feed = state.account_sink.subscribe();
        let path = dir.path().join("mcp.json");

        std::fs::write(&path, mcp_document_bytes(1, &disabled_mcp_config("first"))).unwrap();
        let first = wait_for_mcp_health(&state, SectionStatus::Healthy, 1).await;
        assert_eq!(first.revision, 1);
        assert_eq!(state.config.read().await.mcp.servers[0].id, "first");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 1, .. }
        ));

        std::fs::write(&path, b"{broken").unwrap();
        let invalid = wait_for_mcp_health(&state, SectionStatus::Invalid, 1).await;
        assert_eq!(invalid.revision, 1, "invalid candidates cannot advance LKG");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "first");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));

        // Model an editor's temp-write + atomic rename, with an immediate
        // follow-up write in the same debounce burst. The watcher must settle
        // on the final complete document rather than treating the rename gap as
        // a reset.
        let swap = dir.path().join("mcp.json.swap");
        std::fs::write(
            &swap,
            mcp_document_bytes(2, &disabled_mcp_config("intermediate")),
        )
        .unwrap();
        std::fs::rename(&swap, &path).unwrap();
        std::fs::write(
            &path,
            mcp_document_bytes(2, &disabled_mcp_config("recovered")),
        )
        .unwrap();
        let recovered = wait_for_mcp_health(&state, SectionStatus::Healthy, 2).await;
        assert_eq!(recovered.revision, 2, "rename burst should coalesce once");
        assert_eq!(state.config.read().await.mcp.servers[0].id, "recovered");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigRecovered { revision: 2, .. }
        ));

        // Reusing the live revision with different content forces the shared
        // store to normalize it to revision 3. The normalization write itself
        // must be suppressed exactly once rather than triggering a duplicate
        // reconcile/event.
        std::fs::write(
            &path,
            mcp_document_bytes(2, &disabled_mcp_config("normalized")),
        )
        .unwrap();
        let normalized = wait_for_mcp_health(&state, SectionStatus::Healthy, 3).await;
        assert_eq!(normalized.revision, 3);
        assert_eq!(state.config.read().await.mcp.servers[0].id, "normalized");
        assert!(matches!(
            next_mcp_config_event(&mut feed).await,
            AgentEvent::ConfigChanged { revision: 3, .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), feed.recv())
                .await
                .is_err()
        );
        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted["revision"], 3);
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
