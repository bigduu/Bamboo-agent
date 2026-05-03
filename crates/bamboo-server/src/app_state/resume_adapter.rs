//! Resume execution port adapter.
//!
//! Bridges the application-layer `ResumeExecutionPort` trait to server
//! infrastructure (storage, runner lifecycle, agent spawning).
//!
//! `AppStateResumeRef` is a newtype wrapper around `Data<AppState>` to satisfy
//! Rust's orphan rules (can't impl a foreign trait on a foreign type).

use crate::model_config_helper::get_memory_background_model_for_provider;
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::resume::{ResumeExecutionPort, ResumeSpawnRequest};
use async_trait::async_trait;
use bamboo_agent_core::AgentEvent;
use tokio::sync::broadcast;

use super::runner_lifecycle::{try_reserve_runner, RunnerReservation};
use super::session_events::get_or_create_event_sender;
use super::AppState;
use crate::handlers::agent::execute::runtime::SpawnAgentExecution;
use crate::handlers::agent::execute::{spawn_agent_execution, spawn_event_forwarder};

/// Newtype wrapper that implements `ResumeExecutionPort`.
///
/// Needed because Rust's orphan rules prevent implementing
/// `crate::session_app::resume::ResumeExecutionPort` directly on
/// `actix_web::web::Data<AppState>`.
pub struct AppStateResumeRef(pub actix_web::web::Data<AppState>);

#[async_trait]
impl ResumeExecutionPort for AppStateResumeRef {
    async fn load_session(&self, session_id: &str) -> Option<bamboo_agent_core::Session> {
        AppState::load_session(&self.0, session_id).await
    }

    async fn save_and_cache_session(&self, session: &bamboo_agent_core::Session) {
        AppState::save_and_cache_session(&self.0, session).await;
    }

    async fn try_reserve_runner(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<RunnerReservation> {
        try_reserve_runner(&self.0.agent_runners, session_id, event_sender).await
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        get_or_create_event_sender(&self.0.session_event_senders, session_id).await
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
        let ResumeSpawnRequest {
            session_id,
            session,
            cancel_token,
            run_id: _,
            event_sender,
            config,
        } = request;

        let model = session.model.clone();
        let resolved_provider_name = session_effective_model_ref(&session)
            .map(|model_ref| model_ref.provider)
            .unwrap_or(config.provider_name);
        let resolved_bg_provider = config.background_model_provider.clone();
        let resolved_fast_model = if let Some(fast_model) = config.fast_model {
            Some(fast_model)
        } else {
            let config_snapshot = self.0.config.read().await.clone();
            get_memory_background_model_for_provider(&config_snapshot, &resolved_provider_name)
        };
        let is_child_session = session.kind == bamboo_agent_core::SessionKind::Child;
        let reasoning_effort = session.reasoning_effort;
        let reasoning_effort_source = session
            .metadata
            .get("reasoning_effort_source")
            .cloned()
            .unwrap_or_default();

        let image_fallback = config.image_fallback.clone();

        let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel::<bamboo_agent_core::AgentEvent>(100);

        let state = self.0.clone();
        spawn_event_forwarder(state.clone(), session_id.clone(), mpsc_rx, event_sender);

        spawn_agent_execution(SpawnAgentExecution {
            state,
            session_id,
            session,
            is_child_session,
            provider_name: resolved_provider_name,
            provider_override: None,
            model,
            fast_model: resolved_fast_model,
            background_model_provider: resolved_bg_provider,
            reasoning_effort,
            reasoning_effort_source,
            disabled_tools: config.disabled_tools,
            disabled_skill_ids: config.disabled_skill_ids,
            cancel_token,
            mpsc_tx,
            image_fallback,
        });
    }
}
