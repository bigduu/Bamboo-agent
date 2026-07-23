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
use bamboo_agent_core::AgentEvent;
use bamboo_engine::Agent;
use bamboo_llm::Config;
use bamboo_llm::LLMProvider;
use bamboo_mcp::manager::McpServerManager;
use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_storage::LockedSessionStore;
use bamboo_storage::SessionStoreV2;

use crate::error::AppError;
use crate::schedule_app::manager::{build_schedule_context, ScheduleContext};
use crate::schedule_app::ScheduleManager;
use crate::schedule_app::ScheduleStore;
use bamboo_engine::execution::spawn::{SpawnContext, SpawnScheduler};
use bamboo_metrics::metrics_service::MetricsService;

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

    // One-shot, idempotent migration: split each legacy session's runtime
    // control-plane into a `runtime.json` sidecar so the O(1) runtime-save path
    // (used heavily during sub-agent spawn) is active immediately. Run inline
    // before the server starts serving so it cannot race concurrent saves; it is
    // a no-op on already-migrated stores (marker file).
    match session_store.migrate_runtime_sidecars().await {
        Ok(0) => {}
        Ok(count) => tracing::info!("Runtime sidecar migration created {count} sidecar(s)"),
        Err(error) => {
            // Non-fatal: loading tolerates a missing sidecar. Log and continue.
            tracing::warn!("Runtime sidecar migration failed (continuing): {error}");
        }
    }

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
    // A server process may serve several unrelated workspaces. Never treat its cwd as a
    // project root; session-scoped catalog requests provide the workspace explicitly.
    let project_dir = std::env::var_os("BAMBOO_WORKSPACE_DIR").map(PathBuf::from);
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
pub async fn load_permission_checker(
    bamboo_home_dir: &Path,
) -> Result<
    (
        Arc<PermissionChecker>,
        Arc<bamboo_tools::permission::PermissionSection>,
    ),
    AppError,
> {
    use bamboo_tools::permission::{PermissionConfig, PermissionSection};

    let data_dir = bamboo_home_dir.to_path_buf();
    let section = Arc::new(
        tokio::task::spawn_blocking(move || PermissionSection::open(data_dir))
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "permission section initialization task failed: {error}"
                ))
            })?
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "permission section initialization failed: {error}"
                ))
            })?,
    );
    let snapshot = section.snapshot();
    let permission_config = PermissionConfig::from_serializable(snapshot.data.as_ref().clone());
    permission_config.set_policy_revision(snapshot.revision);
    permission_config.cleanup_expired_grants();

    // Wrap the config checker in a mode-aware checker so the active PermissionMode
    // (Default / Plan / AcceptEdits / DontAsk / BypassPermissions) takes effect at
    // the checker level. Both share the same Arc<PermissionConfig>, so session
    // grants recorded on approval (see the respond handler) are visible here.
    let config = Arc::new(permission_config);
    let inner: Arc<dyn bamboo_tools::permission::PermissionChecker> = Arc::new(
        bamboo_tools::permission::ConfigPermissionChecker::new(config.clone()),
    );
    let checker = Arc::new(bamboo_tools::permission::ModeAwarePermissionChecker::new(
        inner, config,
    ));
    Ok((checker, section))
}

/// Initialize MCP server manager. A pre-existing revisioned `mcp.json` is the
/// sole startup authority and is staged by `ConfigWatcherRuntime`; legacy
/// inline config is bootstrapped only when no sidecar exists.
pub fn init_mcp_manager(
    config: Arc<RwLock<Config>>,
    data_dir: &Path,
) -> (Arc<McpServerManager>, Option<tokio::task::JoinHandle<()>>) {
    let mcp_manager = Arc::new(McpServerManager::new_with_config(config.clone()));

    let legacy_task = if data_dir.join("mcp.json").exists() {
        None
    } else {
        let mcp_manager = mcp_manager.clone();
        let config = config.clone();
        Some(tokio::spawn(async move {
            let mcp_config = config.read().await.mcp.clone();
            mcp_manager.initialize_from_config(&mcp_config).await;
        }))
    };

    (mcp_manager, legacy_task)
}

#[cfg(test)]
mod mcp_startup_tests {
    use super::*;
    use bamboo_mcp::{McpConfig, McpServerConfig, ReconnectConfig, StdioConfig, TransportConfig};

    fn server(id: &str) -> McpServerConfig {
        McpServerConfig {
            id: id.to_string(),
            name: None,
            enabled: true,
            transport: TransportConfig::Stdio(StdioConfig {
                command: "legacy-must-not-start".to_string(),
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
        }
    }

    #[tokio::test]
    async fn preexisting_sidecar_is_authoritative_over_conflicting_legacy_config() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.mcp = McpConfig {
            version: 1,
            servers: vec![server("legacy")],
        };
        std::fs::write(
            dir.path().join("mcp.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 1,
                "data": McpConfig {
                    version: 1,
                    servers: vec![server("sidecar")],
                },
            }))
            .unwrap(),
        )
        .unwrap();

        let (manager, legacy_task) = init_mcp_manager(Arc::new(RwLock::new(config)), dir.path());
        assert!(
            legacy_task.is_none(),
            "legacy bootstrap must not be spawned"
        );
        tokio::task::yield_now().await;
        assert!(manager.list_servers().is_empty());
    }

    #[tokio::test]
    async fn legacy_bootstrap_remains_enabled_without_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let (_manager, legacy_task) =
            init_mcp_manager(Arc::new(RwLock::new(Config::default())), dir.path());
        legacy_task
            .expect("legacy bootstrap should be spawned")
            .await
            .unwrap();
    }
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

/// Idle-eviction TTL for terminal runners / session senders, in seconds.
///
/// A completed runner is retained for at least this long after `completed_at`
/// so a client that subscribes shortly after the run finishes still gets the
/// cached `last_critical_events` / `last_budget_event` replay.
pub(crate) const SESSION_MAP_IDLE_TTL_SECS: i64 = 300;

/// Single idle-eviction pass over the per-session runner + event-sender maps.
///
/// Drops a `(agent_runners, session_event_senders)` pair together when the
/// runner is terminal, older than `ttl_secs`, and its broadcast channel has **no
/// live receivers**. Returns the number of runners evicted.
///
/// # Why `receiver_count() == 0` is required
///
/// A runner's `event_sender` is a clone of the session's long-lived sender (see
/// `reserve_runner` / `try_reserve_runner`), so `receiver_count()` reflects
/// every SSE/WS subscriber on the session stream. Evicting while a receiver is
/// live would (a) yank the replay cache out from under a client that just
/// subscribed after `Complete`, and (b) silently drop a terminal parent whose
/// still-running children forward events into this stream (that parent keeps a
/// live subscription). So we only reclaim genuinely-idle terminal sessions —
/// exactly the ephemeral sub-agent / guardian / scheduled runs that never had a
/// UI subscription and whose count therefore reaches zero.
///
/// Removing the map's `Sender` (the last clone once the terminal runner's own
/// clone is dropped by the `retain`) closes the channel, which also lets any
/// per-session notification relay task exit on `RecvError::Closed`.
///
/// This runs synchronously under both write guards held by the caller (no
/// `.await` between the check and the removals), so a concurrent `reserve_runner`
/// / `get_or_create_event_sender` (which need those same locks) cannot interleave.
pub(crate) fn evict_idle_session_entries(
    runners: &mut HashMap<String, AgentRunner>,
    senders: &mut HashMap<String, broadcast::Sender<AgentEvent>>,
    ttl_secs: i64,
    now: chrono::DateTime<Utc>,
    log_prefix: Option<&'static str>,
) -> usize {
    let mut evicted: Vec<String> = Vec::new();

    runners.retain(|session_id, runner| {
        let keep = match &runner.status {
            AgentStatus::Running => true,
            _ => {
                let age =
                    now.signed_duration_since(runner.completed_at.unwrap_or(runner.started_at));
                let expired = age.num_seconds() >= ttl_secs;
                let has_receivers = runner.event_sender.receiver_count() > 0;
                // Keep unless it is BOTH past the TTL AND has no live receiver.
                !expired || has_receivers
            }
        };
        if !keep {
            evicted.push(session_id.clone());
            if let Some(prefix) = log_prefix {
                tracing::debug!("[{}:{}] Evicting idle terminal runner", prefix, session_id);
            } else {
                tracing::debug!("[{}] Evicting idle terminal runner", session_id);
            }
        }
        keep
    });

    // Drop the paired long-lived sender for each evicted runner, but only if it
    // too has no live receiver (re-checked here; it is the same channel as the
    // runner's `event_sender`, so this is normally a formality — a session sender
    // can, however, outlive its runner, and we must never close a channel a
    // client is actively reading).
    for session_id in &evicted {
        if senders
            .get(session_id)
            .is_some_and(|sender| sender.receiver_count() == 0)
        {
            senders.remove(session_id);
        }
    }

    evicted.len()
}

/// Spawn a background task that idle-evicts completed agent runners **and their
/// paired session event senders** after [`SESSION_MAP_IDLE_TTL_SECS`].
///
/// Fire-and-forget (runs for the process lifetime), matching the other
/// background maintenance tickers. See [`evict_idle_session_entries`] for the
/// eviction predicate and its race-freedom argument.
pub fn spawn_session_map_cleanup_task(
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    log_prefix: Option<&'static str>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            // Lock order: runners ⊃ senders. This matches `reserve_runner_core`
            // (which nests senders inside the runners write lock to re-assert the
            // sender) and is never inverted anywhere, so nesting is deadlock-free.
            //
            // Both write locks are intentionally held across the whole O(n) pass
            // rather than snapshot-then-remove: the atomicity is load-bearing.
            // `reserve_runner_core` inserts a `Running` runner AND re-asserts its
            // sender under the runners lock; holding both here means a concurrent
            // reservation cannot interleave between our runner-eviction and our
            // sender-removal, so we can never drop a sender that a just-reserved
            // run re-asserted. The pass is cheap (a receiver_count atomic load +
            // timestamp math per entry) at the 60s cadence, so the widened window
            // is acceptable.
            let mut runners_guard = runners.write().await;
            let mut senders_guard = senders.write().await;
            let now = Utc::now();
            let evicted = evict_idle_session_entries(
                &mut runners_guard,
                &mut senders_guard,
                SESSION_MAP_IDLE_TTL_SECS,
                now,
                log_prefix,
            );
            drop(senders_guard);
            drop(runners_guard);

            if evicted > 0 {
                tracing::debug!("Idle-evicted {evicted} completed session runner(s)/sender(s)");
            }
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
#[allow(clippy::too_many_arguments)]
pub fn build_spawn_scheduler(
    agent: Arc<Agent>,
    child_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    sessions: bamboo_engine::SessionCache,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    external_child_runner: Arc<dyn bamboo_engine::runtime::execution::ExternalChildRunner>,
    provider_router: Option<Arc<bamboo_llm::ProviderModelRouter>>,
    completion_handler: Option<Arc<dyn bamboo_engine::execution::ChildCompletionHandler>>,
    app_data_dir: Option<std::path::PathBuf>,
    account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
) -> Arc<SpawnScheduler> {
    Arc::new(SpawnScheduler::new(SpawnContext {
        agent,
        tools: child_tools,
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
        external_child_runner,
        provider_router,
        app_data_dir,
        completion_handler,
        account_feed_inbox,
    }))
}

/// Build schedule manager with minimal tool surface for background automation.
#[allow(clippy::too_many_arguments)]
pub fn build_schedule_manager(
    schedule_store: Arc<ScheduleStore>,
    agent: Arc<Agent>,
    tools_for_schedules: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    sessions: bamboo_engine::SessionCache,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    persistence: Arc<LockedSessionStore>,
    config: Arc<RwLock<Config>>,
    provider_registry: Arc<bamboo_llm::ProviderRegistry>,
    app_data_dir: Option<std::path::PathBuf>,
    account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
    notification_relay: crate::app_state::session_events::NotificationRelayDeps,
    project_store: Arc<bamboo_projects::ProjectStore>,
) -> Arc<ScheduleManager> {
    let base_ctx = ScheduleContext {
        schedule_store,
        agent,
        tools: tools_for_schedules,
        permission_config,
        sessions_cache: sessions,
        agent_runners,
        session_event_senders,
        account_feed_inbox,
        persistence,
        app_data_dir,
        trigger_engine: crate::schedule_app::default_trigger_engine(),
        project_store,
        notification_relay,
        resolve_run_config: Arc::new(|_| unimplemented!("replaced by build_schedule_context")),
    };
    Arc::new(ScheduleManager::new(build_schedule_context(
        base_ctx,
        config,
        provider_registry,
    )))
}

/// Build (and start) the bamboo-connect manager (#452 / epic #447).
///
/// Reads `config`'s CURRENT snapshot for `connect.platforms` at construction
/// time (connect platforms are not hot-reloadable in this MVP — a config
/// change takes effect on the next restart, same as `subagents.broker`).
/// Returns a manager with zero background tasks when `connect.platforms` is
/// empty, so callers can always construct + store it unconditionally (mirrors
/// [`build_schedule_manager`]'s always-on lifecycle).
#[allow(clippy::too_many_arguments)]
pub async fn build_connect_manager(
    agent: Arc<Agent>,
    tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    session_repo: bamboo_engine::SessionRepository,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
    app_data_dir: Option<PathBuf>,
    config: Arc<RwLock<Config>>,
    provider_registry: Arc<bamboo_llm::ProviderRegistry>,
    permission_checker: Arc<PermissionChecker>,
    project_store: Arc<bamboo_projects::ProjectStore>,
) -> Result<crate::connect::ConnectManager, String> {
    let config_snapshot = config.read().await.clone();
    let resolved_connect = bamboo_engine::resolved_defaults::resolve_default_run_config(
        &config_snapshot,
        &provider_registry,
    );
    let mut project_ids_by_platform = HashMap::new();
    let start_guard = crate::connect::multi_bot_guard(&config_snapshot.connect.platforms);
    for (platform, guard_allows) in config_snapshot.connect.platforms.iter().zip(start_guard) {
        if !crate::connect::platform_config_will_start(platform, guard_allows) {
            continue;
        }
        if let Some(project_id) = platform.project_id.clone() {
            match project_store.get(&project_id) {
                Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {
                    project_ids_by_platform
                        .insert(platform.platform_type.clone(), project_id.clone());
                }
                Ok(_) => {
                    return Err(format!(
                        "connect platform '{}' references archived Project '{}'",
                        platform.platform_type, project_id
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "connect platform '{}' references unavailable Project '{}': {error}",
                        platform.platform_type, project_id
                    ));
                }
            }
        }
        crate::project_context::validate_workspace_assignment(
            &project_store,
            platform.project_id.as_ref(),
            resolved_connect.workspace_path.as_deref(),
        )
        .map_err(|error| {
            format!(
                "connect platform '{}' has an invalid Project/workspace assignment: {error}",
                platform.platform_type
            )
        })?;
    }
    let ctx = crate::connect::ConnectContext {
        agent,
        tools,
        session_repo,
        agent_runners,
        session_event_senders,
        account_feed_inbox,
        app_data_dir: app_data_dir.clone(),
        config,
        provider_registry,
        project_store,
        project_ids_by_platform: Arc::new(project_ids_by_platform),
        permission_checker,
    };
    Ok(crate::connect::ConnectManager::start(ctx, &config_snapshot, app_data_dir).await)
}

#[cfg(test)]
mod connect_project_mapping_tests {
    use super::*;

    fn telegram(
        token: &str,
        project_id: bamboo_domain::ProjectId,
    ) -> bamboo_config::ConnectPlatformConfig {
        bamboo_config::ConnectPlatformConfig {
            id: None,
            project_id: Some(project_id),
            platform_type: "telegram".to_string(),
            token: Some(token.to_string()),
            token_encrypted: None,
            token_credential_ref: None,
            token_configured: false,
            app_id: None,
            app_secret: None,
            app_secret_encrypted: None,
            app_secret_credential_ref: None,
            app_secret_configured: false,
            domain: None,
            allow_from: vec!["allowed".to_string()],
            admin_from: Vec::new(),
        }
    }

    fn feishu(
        app_id: &str,
        secret: &str,
        project_id: bamboo_domain::ProjectId,
    ) -> bamboo_config::ConnectPlatformConfig {
        bamboo_config::ConnectPlatformConfig {
            app_id: Some(app_id.to_string()),
            app_secret: Some(secret.to_string()),
            platform_type: "feishu".to_string(),
            token: None,
            project_id: Some(project_id),
            ..telegram("", bamboo_domain::ProjectId::new())
        }
    }

    #[test]
    fn duplicate_platform_project_mapping_follows_the_first_started_entry() {
        let data = tempfile::tempdir().expect("data");
        let store = bamboo_projects::ProjectStore::open(data.path()).expect("Project store");
        let first_telegram = store.create("Telegram Active", None).unwrap();
        let skipped_telegram = store.create("Telegram Skipped", None).unwrap();
        let first_feishu = store.create("Feishu Active", None).unwrap();
        let skipped_feishu = store.create("Feishu Skipped", None).unwrap();
        let platforms = vec![
            telegram("token-a", first_telegram.id.clone()),
            telegram("token-b", skipped_telegram.id),
            feishu("app-a", "secret-a", first_feishu.id.clone()),
            feishu("app-b", "secret-b", skipped_feishu.id),
        ];
        let guard = crate::connect::multi_bot_guard(&platforms);
        let mapped = platforms
            .iter()
            .zip(guard)
            .filter(|(platform, allowed)| {
                crate::connect::platform_config_will_start(platform, *allowed)
            })
            .filter_map(|(platform, _)| {
                platform
                    .project_id
                    .clone()
                    .map(|project_id| (platform.platform_type.clone(), project_id))
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(mapped.get("telegram"), Some(&first_telegram.id));
        assert_eq!(mapped.get("feishu"), Some(&first_feishu.id));
    }
}

#[cfg(test)]
mod eviction_tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    const TTL: i64 = SESSION_MAP_IDLE_TTL_SECS;

    /// A terminal (Completed) runner whose `event_sender` shares `tx`'s channel,
    /// exactly as production sets it in `reserve_runner`.
    fn terminal_runner(
        tx: &broadcast::Sender<AgentEvent>,
        completed_secs_ago: i64,
        now: chrono::DateTime<Utc>,
    ) -> AgentRunner {
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Completed;
        runner.completed_at = Some(now - ChronoDuration::seconds(completed_secs_ago));
        runner.event_sender = tx.clone();
        runner
    }

    fn insert_pair(
        runners: &mut HashMap<String, AgentRunner>,
        senders: &mut HashMap<String, broadcast::Sender<AgentEvent>>,
        id: &str,
        runner: AgentRunner,
        tx: broadcast::Sender<AgentEvent>,
    ) {
        runners.insert(id.to_string(), runner);
        senders.insert(id.to_string(), tx);
    }

    #[test]
    fn evicts_idle_terminal_pair_past_ttl_with_no_receivers() {
        let now = Utc::now();
        let (tx, _) = broadcast::channel::<AgentEvent>(16); // receiver dropped -> count 0
        let mut runners = HashMap::new();
        let mut senders = HashMap::new();
        insert_pair(
            &mut runners,
            &mut senders,
            "s1",
            terminal_runner(&tx, TTL + 100, now),
            tx.clone(),
        );

        let evicted = evict_idle_session_entries(&mut runners, &mut senders, TTL, now, None);

        assert_eq!(evicted, 1);
        assert!(runners.is_empty(), "terminal idle runner must be dropped");
        assert!(
            senders.is_empty(),
            "paired session sender must be dropped together with the runner"
        );
    }

    #[test]
    fn retains_terminal_runner_with_live_receiver() {
        let now = Utc::now();
        let (tx, _) = broadcast::channel::<AgentEvent>(16);
        // A client subscribed just after completion — its receiver is live.
        let _live_rx = tx.subscribe();
        assert_eq!(tx.receiver_count(), 1);

        let mut runners = HashMap::new();
        let mut senders = HashMap::new();
        insert_pair(
            &mut runners,
            &mut senders,
            "s1",
            terminal_runner(&tx, TTL + 100, now),
            tx.clone(),
        );

        let evicted = evict_idle_session_entries(&mut runners, &mut senders, TTL, now, None);

        assert_eq!(evicted, 0, "must not evict while a receiver is live");
        assert!(runners.contains_key("s1"));
        assert!(senders.contains_key("s1"));
    }

    #[test]
    fn retains_young_terminal_runner() {
        let now = Utc::now();
        let (tx, _) = broadcast::channel::<AgentEvent>(16);
        let mut runners = HashMap::new();
        let mut senders = HashMap::new();
        // Completed only 60s ago — inside the replay window.
        insert_pair(
            &mut runners,
            &mut senders,
            "s1",
            terminal_runner(&tx, 60, now),
            tx.clone(),
        );

        let evicted = evict_idle_session_entries(&mut runners, &mut senders, TTL, now, None);

        assert_eq!(
            evicted, 0,
            "must preserve the post-completion replay window"
        );
        assert!(runners.contains_key("s1"));
        assert!(senders.contains_key("s1"));
    }

    #[test]
    fn retains_running_runner_regardless_of_age() {
        let now = Utc::now();
        let (tx, _) = broadcast::channel::<AgentEvent>(16);
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        runner.started_at = now - ChronoDuration::seconds(TTL + 100_000);
        runner.completed_at = None;
        runner.event_sender = tx.clone();

        let mut runners = HashMap::new();
        let mut senders = HashMap::new();
        insert_pair(&mut runners, &mut senders, "s1", runner, tx.clone());

        let evicted = evict_idle_session_entries(&mut runners, &mut senders, TTL, now, None);

        assert_eq!(evicted, 0, "a Running runner is never evicted");
        assert!(runners.contains_key("s1"));
        assert!(senders.contains_key("s1"));
    }

    #[test]
    fn keeps_sender_if_it_has_receivers_even_when_runner_evicted() {
        // Defensive edge: the runner qualifies for eviction, but the session
        // sender independently has a live receiver (e.g. a subscriber that
        // reused the channel). The sender must survive so we never close a
        // channel a client is reading.
        let now = Utc::now();
        let (runner_tx, _) = broadcast::channel::<AgentEvent>(16);
        // Distinct channel for the map entry, with a live receiver.
        let (sender_tx, _) = broadcast::channel::<AgentEvent>(16);
        let _live = sender_tx.subscribe();

        let mut runners = HashMap::new();
        let mut senders = HashMap::new();
        runners.insert(
            "s1".to_string(),
            terminal_runner(&runner_tx, TTL + 100, now),
        );
        senders.insert("s1".to_string(), sender_tx.clone());

        let evicted = evict_idle_session_entries(&mut runners, &mut senders, TTL, now, None);

        assert_eq!(evicted, 1, "runner (no receivers) is evicted");
        assert!(runners.is_empty());
        assert!(
            senders.contains_key("s1"),
            "sender with a live receiver must be retained even when the runner is dropped"
        );
    }
}
