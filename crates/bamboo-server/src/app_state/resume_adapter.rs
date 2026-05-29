//! Resume execution port adapter.
//!
//! Bridges the application-layer `ResumeExecutionPort` trait to server
//! infrastructure (storage, runner lifecycle, agent spawning).
//!
//! `AppStateResumeRef` is a newtype wrapper around `Data<AppState>` to satisfy
//! Rust's orphan rules (can't impl a foreign trait on a foreign type).

use crate::model_config_helper::{
    resolve_background_model, resolve_fast_model, resolve_gold_config, resolve_provider_type,
    resolve_task_summary_model, GOLD_CONFIG_METADATA_KEY,
};
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

    async fn save_and_cache_session(&self, session: &mut bamboo_agent_core::Session) {
        AppState::save_and_cache_session(&self.0, session).await;
    }

    async fn try_reserve_runner(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<RunnerReservation> {
        try_reserve_runner(&self.0.agent_runners, session_id, event_sender).await
    }

    async fn get_existing_runner_run_id(&self, session_id: &str) -> Option<String> {
        let runners = self.0.agent_runners.read().await;
        runners.get(session_id).map(|r| r.run_id.clone())
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
        let config_snapshot = self.0.config.read().await.clone();
        let resolved_provider_type = resolve_provider_type(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        let resolved_fast = resolve_fast_model(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        let resolved_background = resolve_background_model(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        let resolved_summarization = resolve_task_summary_model(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        let resolved_fast_model = config
            .fast_model
            .clone()
            .or_else(|| resolved_fast.as_ref().map(|m| m.model_name.clone()));
        let resolved_fast_provider = resolved_fast.map(|m| m.provider);
        let resolved_background_model = config
            .background_model
            .clone()
            .or_else(|| resolved_background.as_ref().map(|m| m.model_name.clone()));
        let resolved_bg_provider = config
            .background_model_provider
            .clone()
            .or_else(|| resolved_background.map(|m| m.provider));
        let resolved_summarization_model = config.summarization_model.clone().or_else(|| {
            resolved_summarization
                .as_ref()
                .map(|m| m.model_name.clone())
        });
        let resolved_summarization_provider = config
            .summarization_model_provider
            .clone()
            .or_else(|| resolved_summarization.map(|m| m.provider));
        let is_child_session = session.kind == bamboo_agent_core::SessionKind::Child;
        let reasoning_effort = session.reasoning_effort;
        let reasoning_effort_source = session
            .metadata
            .get("reasoning_effort_source")
            .cloned()
            .unwrap_or_default();

        let image_fallback = config.image_fallback.clone();
        let gold_config = resolve_gold_config(
            &config_snapshot,
            session
                .metadata
                .get(GOLD_CONFIG_METADATA_KEY)
                .map(String::as_str),
        )
        .or(config.gold_config.clone());

        let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel::<bamboo_agent_core::AgentEvent>(100);

        let state = self.0.clone();
        spawn_event_forwarder(
            state.clone(),
            session_id.clone(),
            mpsc_rx,
            event_sender,
            gold_config.clone(),
        );

        spawn_agent_execution(SpawnAgentExecution {
            state: state.clone(),
            session_id,
            session,
            is_child_session,
            provider_name: resolved_provider_name,
            provider_type: resolved_provider_type,
            provider_override: None,
            model,
            fast_model: resolved_fast_model,
            fast_model_provider: resolved_fast_provider,
            background_model: resolved_background_model,
            background_model_provider: resolved_bg_provider,
            summarization_model: resolved_summarization_model,
            summarization_model_provider: resolved_summarization_provider,
            reasoning_effort,
            reasoning_effort_source,
            disabled_tools: config.disabled_tools,
            disabled_skill_ids: config.disabled_skill_ids,
            cancel_token,
            mpsc_tx,
            image_fallback,
            gold_config,
            app_data_dir: Some(state.app_data_dir.clone()),
        });
    }
}
