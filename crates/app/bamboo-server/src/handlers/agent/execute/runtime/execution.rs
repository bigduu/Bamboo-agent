//! Agent execution spawner — thin adapter over the runtime crate.
//!
//! Converts server-side `SpawnAgentExecution` args into the crate-agnostic
//! `SessionExecutionArgs` and delegates to
//! `bamboo_engine::execution::spawn_session_execution`.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock as StdRwLock};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app_state::AppState;
use crate::tools::ToolSurface;
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{resolve_planning_model, resolve_search_model};
use bamboo_engine::session_app::provider_model::session_effective_model_ref;

use bamboo_engine::config::GoldConfig;
use bamboo_engine::execution::agent_spawn::SessionExecutionArgs;
use bamboo_engine::{AuxiliaryModelConfig, ImageFallbackConfig, ModelRoster};
use bamboo_llm::{Config, LLMProvider};

use super::session_state;

pub(crate) struct SpawnAgentExecution {
    pub(crate) state: actix_web::web::Data<AppState>,
    pub(crate) session_id: String,
    pub(crate) session: bamboo_agent_core::Session,
    pub(crate) is_child_session: bool,
    /// Provider routing key for the primary model. Mirrors
    /// `model_roster.provider_name` but kept as a required `String` because the
    /// auxiliary-model resolver is keyed on it; the value is also threaded into
    /// the roster.
    pub(crate) provider_name: String,
    pub(crate) provider_override: Option<Arc<dyn LLMProvider>>,
    /// Cohesive primary + auxiliary model/provider selection.
    pub(crate) model_roster: ModelRoster,
    pub(crate) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(crate) reasoning_effort_source: String,
    pub(crate) disabled_tools: BTreeSet<String>,
    pub(crate) disabled_skill_ids: BTreeSet<String>,
    pub(crate) cancel_token: CancellationToken,
    pub(crate) mpsc_tx: mpsc::Sender<bamboo_agent_core::AgentEvent>,
    pub(crate) image_fallback: Option<ImageFallbackConfig>,
    pub(crate) gold_config: Option<GoldConfig>,
    pub(crate) app_data_dir: Option<std::path::PathBuf>,
    /// Optional per-run resource guardrail override from the `POST /execute`
    /// request body (issue #221). `None` uses the config-level default.
    pub(crate) run_budget: Option<bamboo_config::RunBudgetConfig>,
}

pub(super) fn execution_tool_surface(is_child_session: bool) -> ToolSurface {
    if is_child_session {
        ToolSurface::Child
    } else {
        ToolSurface::Root
    }
}

pub(super) fn tools_for_execution(
    state: &AppState,
    is_child_session: bool,
) -> Arc<dyn bamboo_agent_core::tools::ToolExecutor> {
    state.tools_for(execution_tool_surface(is_child_session))
}

fn read_config_snapshot(
    config: &Arc<tokio::sync::RwLock<Config>>,
    cached_config: &StdRwLock<Config>,
) -> Config {
    if let Ok(config_guard) = config.try_read() {
        let snapshot = config_guard.clone();

        if let Ok(mut cached_guard) = cached_config.try_write() {
            *cached_guard = snapshot.clone();
        }

        snapshot
    } else {
        cached_config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

pub(crate) fn make_auxiliary_model_resolver(
    state: &actix_web::web::Data<AppState>,
    provider_name: &str,
) -> Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync> {
    let config = state.config.clone();
    let cached_config = Arc::new(StdRwLock::new(
        config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default(),
    ));
    let provider_registry = state.provider_registry.clone();
    let provider_name = provider_name.to_string();

    Arc::new(move || {
        let config_snapshot = read_config_snapshot(&config, cached_config.as_ref());
        // Auxiliary models are global (config-derived), never session-bound.
        let areas =
            resolve_global_area_models(&config_snapshot, &provider_name, &provider_registry);
        let resolved_planning =
            resolve_planning_model(&config_snapshot, &provider_name, &provider_registry);
        let resolved_search =
            resolve_search_model(&config_snapshot, &provider_name, &provider_registry);

        AuxiliaryModelConfig {
            fast_model_name: areas.fast.as_ref().map(|m| m.model_name.clone()),
            fast_model_provider: areas.fast.map(|m| m.provider),
            background_model_name: areas.background.as_ref().map(|m| m.model_name.clone()),
            planning_model_name: resolved_planning.as_ref().map(|m| m.model_name.clone()),
            search_model_name: resolved_search.as_ref().map(|m| m.model_name.clone()),
            summarization_model_name: areas.summarization.as_ref().map(|m| m.model_name.clone()),
            background_model_provider: areas.background.map(|m| m.provider),
            summarization_model_provider: areas.summarization.map(|m| m.provider),
        }
    })
}

/// Build the per-round resolver for the LIVE disabled tool/skill sets (#136).
/// Returns the CURRENT `(disabled_tools, disabled_skill_ids)` from the live global
/// config each call, so disabling/re-enabling a tool mid-run takes effect on the
/// next round. Reuses `read_config_snapshot` (non-blocking `try_read` + cached
/// fallback — the same hot-path-safe mechanism `make_auxiliary_model_resolver`
/// uses). NB: returns the live GLOBAL set (not unioned with the request-time
/// snapshot), because that snapshot IS just the old global set — unioning it would
/// freeze it as a floor and break re-enable.
pub(crate) fn make_disabled_filter_resolver(
    state: &actix_web::web::Data<AppState>,
) -> Arc<dyn Fn() -> (BTreeSet<String>, BTreeSet<String>) + Send + Sync> {
    let config = state.config.clone();
    let cached_config = Arc::new(StdRwLock::new(
        config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default(),
    ));
    Arc::new(move || {
        let config_snapshot = read_config_snapshot(&config, cached_config.as_ref());
        (
            config_snapshot.disabled_tool_names().into_iter().collect(),
            config_snapshot.disabled_skill_ids().into_iter().collect(),
        )
    })
}

pub(crate) fn spawn_agent_execution(args: SpawnAgentExecution) {
    let tools_override = Some(tools_for_execution(
        args.state.as_ref(),
        args.is_child_session,
    ));

    let selected_skill_ids = session_state::selected_skill_ids_for_session(&args.session);
    let selected_skill_mode = session_state::selected_skill_mode_for_session(&args.session);
    let provider_override = session_effective_model_ref(&args.session)
        .and_then(|model_ref| match args.state.provider_router.route(&model_ref) {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(
                    session_id = %args.session_id,
                    provider = %model_ref.provider,
                    model = %model_ref.model,
                    error = %error,
                    "failed to resolve provider override for session execution; falling back to runtime provider"
                );
                None
            }
        })
        .or(args.provider_override);

    let auxiliary_model_resolver = make_auxiliary_model_resolver(&args.state, &args.provider_name);

    // The resolved provider name is the authoritative routing key; thread it into
    // the roster so it matches the value the old `provider_name` field carried
    // (and the resolver above is keyed on).
    let mut model_roster = args.model_roster;
    model_roster.provider_name = Some(args.provider_name);

    bamboo_engine::execution::spawn_session_execution(SessionExecutionArgs {
        agent: args.state.agent.clone(),
        session_id: args.session_id,
        session: args.session,
        tools_override,
        provider_override,
        model_roster,
        reasoning_effort: args.reasoning_effort,
        reasoning_effort_source: args.reasoning_effort_source,
        auxiliary_model_resolver: Some(auxiliary_model_resolver),
        // Live per-round disabled-set resolver (#136): a tool disabled/re-enabled
        // mid-run takes effect on the next round of this (long-running) agent loop.
        disabled_filter_resolver: Some(make_disabled_filter_resolver(&args.state)),
        disabled_tools: Some(args.disabled_tools),
        disabled_skill_ids: Some(args.disabled_skill_ids),
        selected_skill_ids,
        selected_skill_mode,
        cancel_token: args.cancel_token,
        mpsc_tx: args.mpsc_tx,
        image_fallback: args.image_fallback,
        gold_config: args.gold_config,
        // The guardian reviewer spawner is always available; the terminal gate
        // stays inert until `guardian_config` is enabled. (TODO: surface a
        // guardian config on the request, mirroring `gold_config`.)
        guardian_config: None,
        guardian_spawner: Some(args.state.guardian_spawner.clone()),
        bash_resume_hook: Some(args.state.bash_resume_hook.clone()),
        // The completion coordinator also implements `BashCompletionSink`: a
        // finished background shell pushes its result into this loop (injected at
        // the next round boundary, issue #84 Phase 2b follow-up).
        bash_completion_sink: Some(args.state.child_completion_coordinator.clone()),
        app_data_dir: args.app_data_dir,
        run_budget: args.run_budget,
        runners: args.state.agent_runners.clone(),
        sessions_cache: args.state.sessions.clone(),
        on_complete: None,
    });
}

#[cfg(test)]
mod tests {
    use super::read_config_snapshot;
    use bamboo_llm::Config;
    use std::sync::{Arc, RwLock as StdRwLock};

    #[test]
    fn read_config_snapshot_refreshes_cached_snapshot_from_live_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let config = Arc::new(tokio::sync::RwLock::new(Config::default()));
            config.write().await.provider = "copilot".to_string();
            let cached_config = StdRwLock::new(Config::default());

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "copilot");
            assert_eq!(
                cached_config.read().expect("cached snapshot lock").provider,
                "copilot"
            );
        });
    }

    #[test]
    fn read_config_snapshot_uses_cached_snapshot_when_live_lock_is_busy() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let cached_snapshot = Config {
                provider: "cached-provider".to_string(),
                ..Default::default()
            };

            let config = Arc::new(tokio::sync::RwLock::new(Config::default()));
            let cached_config = StdRwLock::new(cached_snapshot);
            let _write_guard = config.write().await;

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "cached-provider");
        });
    }
}
