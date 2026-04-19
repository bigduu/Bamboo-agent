//! Infrastructure initialization helpers.
//!
//! These functions create domain-level services (storage, skill manager, provider handles,
//! MCP servers, metrics, permissions, schedules) that are shared between `AppState`
//! and `AgentRuntime`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_engine::Agent;
use bamboo_engine::McpServerManager;
use bamboo_engine::{SkillManager, SkillStoreConfig};
use bamboo_infrastructure::Config;
use bamboo_infrastructure::LLMProvider;
use bamboo_infrastructure::SessionStoreV2;

use crate::error::AppError;
use crate::metrics_service::MetricsService;
use crate::schedules::manager::{build_schedule_context, ScheduleContext};
use crate::schedules::ScheduleManager;
use crate::schedules::ScheduleStore;
use crate::spawn_scheduler::{SpawnContext, SpawnScheduler};

use super::{AgentRunner, AgentStatus};

/// Type alias for the permission checker trait object.
pub(super) type PermissionChecker = dyn bamboo_tools::permission::PermissionChecker;

/// Type alias for the provider lock + handle pair returned by [`build_provider_handles`].
type ProviderHandles = (Arc<RwLock<Arc<dyn LLMProvider>>>, Arc<dyn LLMProvider>);

/// Initialize storage components (session store + storage trait object).
///
/// Spawns a background task to rebuild the search index on startup.
pub async fn init_storage(
    data_dir: &Path,
) -> Result<(Arc<SessionStoreV2>, Arc<dyn Storage>), AppError> {
    tracing::info!("Initializing session store V2 at: {:?}", data_dir);
    let session_store = Arc::new(SessionStoreV2::new(data_dir.to_path_buf()).await.map_err(
        |error| {
            tracing::error!(
                "Failed to initialize SessionStoreV2 at {:?}: {}",
                data_dir,
                error
            );
            AppError::StorageError(error)
        },
    )?);
    let session_store_for_rebuild = session_store.clone();
    tokio::spawn(async move {
        let purged_rows = match session_store_for_rebuild
            .search_index()
            .prune_stale_sessions()
            .await
        {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!("Background session search index prune failed: {}", error);
                0
            }
        };

        if let Err(error) = session_store_for_rebuild.rebuild_search_index().await {
            tracing::warn!("Background session search index rebuild failed: {}", error);
        } else {
            tracing::info!("Background session search index rebuild completed");
        }

        match session_store_for_rebuild
            .search_index()
            .maybe_vacuum_if_needed(purged_rows)
            .await
        {
            Ok(true) => tracing::info!("Background session search index vacuum completed"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!("Background session search index vacuum failed: {}", error)
            }
        }
    });

    let storage: Arc<dyn Storage> = session_store.clone();
    tracing::info!(
        "Session store V2 initialized (index: {:?}, sessions: {:?})",
        session_store.index_path(),
        session_store.sessions_root_dir()
    );
    Ok((session_store, storage))
}

/// Initialize the skill manager with workspace directory and active mode from environment.
pub async fn init_skill_manager(data_dir: &Path) -> Arc<SkillManager> {
    let project_dir = std::env::var_os("BAMBOO_WORKSPACE_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok());
    let active_mode = std::env::var("BAMBOO_SKILL_MODE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: data_dir.join("skills"),
        project_dir,
        active_mode,
    }));
    if let Err(error) = skill_manager.initialize().await {
        tracing::warn!("Failed to initialize skill manager: {}", error);
    }
    skill_manager
}

/// Build reloadable provider handles.
///
/// Returns `(provider_lock, provider_handle)` where `provider_lock` is the
/// hot-reloadable `RwLock` and `provider_handle` is a stable `ReloadableProvider`
/// that always delegates to the latest value in the lock.
pub fn build_provider_handles(provider: Arc<dyn LLMProvider>) -> ProviderHandles {
    let provider_lock: Arc<RwLock<Arc<dyn LLMProvider>>> = Arc::new(RwLock::new(provider));
    let provider_handle: Arc<dyn LLMProvider> = Arc::new(
        crate::reloadable_provider::ReloadableProvider::new(provider_lock.clone()),
    );
    (provider_lock, provider_handle)
}

/// Load permission configuration from disk.
///
/// Falls back to disabled permissions if no config exists or loading fails.
pub async fn load_permission_checker(bamboo_home_dir: &Path) -> Arc<PermissionChecker> {
    let storage = bamboo_tools::permission::storage::PermissionStorage::new(bamboo_home_dir);
    let permission_config = match storage.load().await {
        Ok(Some(config)) => config,
        Ok(None) => {
            let cfg = bamboo_tools::permission::PermissionConfig::new();
            cfg.set_enabled(false);
            cfg
        }
        Err(error) => {
            tracing::warn!("Failed to load permission config; defaulting to disabled: {error}");
            let cfg = bamboo_tools::permission::PermissionConfig::new();
            cfg.set_enabled(false);
            cfg
        }
    };
    permission_config.cleanup_expired_grants();
    Arc::new(bamboo_tools::permission::ConfigPermissionChecker::new(
        Arc::new(permission_config),
    ))
}

/// Initialize MCP server manager with background server initialization.
pub fn init_mcp_manager(config: Arc<RwLock<Config>>) -> Arc<McpServerManager> {
    let mcp_manager = Arc::new(McpServerManager::new_with_config(config.clone()));

    // Initialize MCP servers in the background so the HTTP API is responsive quickly.
    {
        let mcp_manager = mcp_manager.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mcp_config = config.read().await.mcp.clone();
            mcp_manager.initialize_from_config(&mcp_config).await;
        });
    }

    mcp_manager
}

/// Initialize metrics service with SQLite backend.
pub async fn init_metrics_service(data_dir: &Path) -> Result<Arc<MetricsService>, AppError> {
    let service = MetricsService::new(data_dir.join("metrics.db"))
        .await
        .map_err(|error| {
            tracing::error!("Failed to initialize metrics storage: {}", error);
            AppError::InternalError(anyhow::anyhow!(
                "Failed to initialize metrics storage: {error}"
            ))
        })?;
    Ok(Arc::new(service))
}

/// Spawn a background task that cleans up completed agent runners after 5 minutes.
pub fn spawn_runner_cleanup_task(
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    log_prefix: Option<&'static str>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let mut runners_guard = runners.write().await;
            let now = Utc::now();

            runners_guard.retain(|session_id, runner| {
                let should_keep = match &runner.status {
                    AgentStatus::Running => true,
                    _ => {
                        let age = now.signed_duration_since(
                            runner.completed_at.unwrap_or(runner.started_at),
                        );
                        age.num_seconds() < 300 // 5 minute TTL
                    }
                };

                if !should_keep {
                    if let Some(prefix) = log_prefix {
                        tracing::debug!("[{}:{}] Cleaning up completed runner", prefix, session_id);
                    } else {
                        tracing::debug!("[{}] Cleaning up completed runner", session_id);
                    }
                }

                should_keep
            });
        }
    });
}

/// Initialize schedule store for timed tasks.
pub async fn init_schedule_store(data_dir: &PathBuf) -> Result<Arc<ScheduleStore>, AppError> {
    let store = ScheduleStore::new(data_dir.clone())
        .await
        .map_err(|error| {
            tracing::error!(
                "Failed to initialize ScheduleStore at {:?}: {}",
                data_dir,
                error
            );
            AppError::StorageError(error)
        })?;
    Ok(Arc::new(store))
}

/// Build sub-session spawn scheduler.
pub fn build_spawn_scheduler(
    agent: Arc<Agent>,
    child_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
) -> Arc<SpawnScheduler> {
    Arc::new(SpawnScheduler::new(SpawnContext {
        agent,
        tools: child_tools,
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
    }))
}

/// Build schedule manager with minimal tool surface for background automation.
pub fn build_schedule_manager(
    schedule_store: Arc<ScheduleStore>,
    agent: Arc<Agent>,
    tools_for_schedules: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    config: Arc<RwLock<Config>>,
) -> Arc<ScheduleManager> {
    let base_ctx = ScheduleContext {
        schedule_store,
        agent,
        tools: tools_for_schedules,
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
        trigger_engine: crate::schedules::default_trigger_engine(),
        resolve_run_config: Arc::new(|_| unimplemented!("replaced by build_schedule_context")),
    };
    Arc::new(ScheduleManager::new(build_schedule_context(
        base_ctx, config,
    )))
}
