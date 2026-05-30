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
use crate::model_config_helper::{
    resolve_background_model, resolve_fast_model, resolve_planning_model, resolve_search_model,
    resolve_task_summary_model,
};
use crate::session_app::provider_model::session_effective_model_ref;
use crate::tools::ToolSurface;

use bamboo_engine::config::GoldConfig;
use bamboo_engine::execution::agent_spawn::SessionExecutionArgs;
use bamboo_engine::{AuxiliaryModelConfig, ImageFallbackConfig};
use bamboo_infrastructure::{Config, LLMProvider};

use super::session_state;

pub(crate) struct SpawnAgentExecution {
    pub(crate) state: actix_web::web::Data<AppState>,
    pub(crate) session_id: String,
    pub(crate) session: bamboo_agent_core::Session,
    pub(crate) is_child_session: bool,
    pub(crate) provider_name: String,
    pub(crate) provider_type: Option<String>,
    pub(crate) provider_override: Option<Arc<dyn LLMProvider>>,
    pub(crate) model: String,
    pub(crate) fast_model: Option<String>,
    pub(crate) fast_model_provider: Option<Arc<dyn LLMProvider>>,
    pub(crate) background_model: Option<String>,
    pub(crate) background_model_provider: Option<Arc<dyn LLMProvider>>,
    pub(crate) summarization_model: Option<String>,
    pub(crate) summarization_model_provider: Option<Arc<dyn LLMProvider>>,
    pub(crate) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(crate) reasoning_effort_source: String,
    pub(crate) disabled_tools: BTreeSet<String>,
    pub(crate) disabled_skill_ids: BTreeSet<String>,
    pub(crate) cancel_token: CancellationToken,
    pub(crate) mpsc_tx: mpsc::Sender<bamboo_agent_core::AgentEvent>,
    pub(crate) image_fallback: Option<ImageFallbackConfig>,
    pub(crate) gold_config: Option<GoldConfig>,
    pub(crate) app_data_dir: Option<std::path::PathBuf>,
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
        let resolved_fast =
            resolve_fast_model(&config_snapshot, &provider_name, &provider_registry);
        let resolved_background =
            resolve_background_model(&config_snapshot, &provider_name, &provider_registry);
        let resolved_summarization =
            resolve_task_summary_model(&config_snapshot, &provider_name, &provider_registry);
        let resolved_planning =
            resolve_planning_model(&config_snapshot, &provider_name, &provider_registry);
        let resolved_search =
            resolve_search_model(&config_snapshot, &provider_name, &provider_registry);

        AuxiliaryModelConfig {
            fast_model_name: resolved_fast.as_ref().map(|m| m.model_name.clone()),
            fast_model_provider: resolved_fast.map(|m| m.provider),
            background_model_name: resolved_background.as_ref().map(|m| m.model_name.clone()),
            planning_model_name: resolved_planning.as_ref().map(|m| m.model_name.clone()),
            search_model_name: resolved_search.as_ref().map(|m| m.model_name.clone()),
            summarization_model_name: resolved_summarization
                .as_ref()
                .map(|m| m.model_name.clone()),
            background_model_provider: resolved_background.map(|m| m.provider),
            summarization_model_provider: resolved_summarization.map(|m| m.provider),
        }
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

    bamboo_engine::execution::spawn_session_execution(SessionExecutionArgs {
        agent: args.state.agent.clone(),
        session_id: args.session_id,
        session: args.session,
        tools_override,
        provider_override,
        provider_name: Some(args.provider_name),
        provider_type: args.provider_type,
        model: args.model,
        fast_model: args.fast_model,
        fast_model_provider: args.fast_model_provider,
        background_model: args.background_model,
        background_model_provider: args.background_model_provider,
        summarization_model: args.summarization_model,
        summarization_model_provider: args.summarization_model_provider,
        reasoning_effort: args.reasoning_effort,
        reasoning_effort_source: args.reasoning_effort_source,
        auxiliary_model_resolver: Some(auxiliary_model_resolver),
        disabled_tools: Some(args.disabled_tools),
        disabled_skill_ids: Some(args.disabled_skill_ids),
        selected_skill_ids,
        selected_skill_mode,
        cancel_token: args.cancel_token,
        mpsc_tx: args.mpsc_tx,
        image_fallback: args.image_fallback,
        gold_config: args.gold_config,
        app_data_dir: args.app_data_dir,
        runners: args.state.agent_runners.clone(),
        sessions_cache: args.state.sessions.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::read_config_snapshot;
    use bamboo_infrastructure::Config;
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
