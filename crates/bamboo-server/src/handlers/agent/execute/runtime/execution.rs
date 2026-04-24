//! Agent execution spawner — thin adapter over the runtime crate.
//!
//! Converts server-side `SpawnAgentExecution` args into the crate-agnostic
//! `SessionExecutionArgs` and delegates to
//! `bamboo_engine::execution::spawn_session_execution`.

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app_state::AppState;
use crate::session_app::provider_model::session_effective_model_ref;
use crate::tools::ToolSurface;

use bamboo_engine::execution::agent_spawn::SessionExecutionArgs;
use bamboo_engine::ImageFallbackConfig;
use bamboo_infrastructure::LLMProvider;

use super::session_state;

pub(crate) struct SpawnAgentExecution {
    pub(crate) state: actix_web::web::Data<AppState>,
    pub(crate) session_id: String,
    pub(crate) session: bamboo_agent_core::Session,
    pub(crate) is_child_session: bool,
    pub(crate) provider_name: String,
    pub(crate) provider_override: Option<Arc<dyn LLMProvider>>,
    pub(crate) model: String,
    pub(crate) fast_model: Option<String>,
    pub(crate) background_model_provider: Option<Arc<dyn LLMProvider>>,
    pub(crate) reasoning_effort: Option<bamboo_domain::reasoning::ReasoningEffort>,
    pub(crate) reasoning_effort_source: String,
    pub(crate) disabled_tools: BTreeSet<String>,
    pub(crate) disabled_skill_ids: BTreeSet<String>,
    pub(crate) cancel_token: CancellationToken,
    pub(crate) mpsc_tx: mpsc::Sender<bamboo_agent_core::AgentEvent>,
    pub(crate) image_fallback: Option<ImageFallbackConfig>,
}

pub(crate) fn spawn_agent_execution(args: SpawnAgentExecution) {
    let tools_override = if args.is_child_session {
        Some(args.state.tools_for(ToolSurface::Child))
    } else {
        None
    };

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

    bamboo_engine::execution::spawn_session_execution(SessionExecutionArgs {
        agent: args.state.agent.clone(),
        session_id: args.session_id,
        session: args.session,
        tools_override,
        provider_override,
        provider_name: Some(args.provider_name),
        model: args.model,
        fast_model: args.fast_model,
        background_model_provider: args.background_model_provider,
        reasoning_effort: args.reasoning_effort,
        reasoning_effort_source: args.reasoning_effort_source,
        disabled_tools: Some(args.disabled_tools),
        disabled_skill_ids: Some(args.disabled_skill_ids),
        selected_skill_ids,
        selected_skill_mode,
        cancel_token: args.cancel_token,
        mpsc_tx: args.mpsc_tx,
        image_fallback: args.image_fallback,
        runners: args.state.agent_runners.clone(),
        sessions_cache: args.state.sessions.clone(),
    });
}
