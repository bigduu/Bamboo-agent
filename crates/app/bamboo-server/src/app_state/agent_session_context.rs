//! `AgentSessionContext` implementation for `AppState`.
//!
//! Bridges the engine's dependency-inversion trait (used by the
//! agent-session-orchestration cluster: events, session_app, title_gen,
//! gold_auto_answer) to the server's concrete `AppState`. Every method is a
//! thin delegation to an existing field or method.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::ProviderModelRef;
use bamboo_engine::app_context::AgentSessionContext;
use bamboo_engine::events::AccountEventSink;
use bamboo_engine::runtime::execution::runner_state::AgentRunner;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};
use bamboo_storage::LockedSessionStore;

use super::AppState;

#[async_trait]
impl AgentSessionContext for AppState {
    fn sessions(&self) -> &bamboo_engine::SessionCache {
        &self.sessions
    }

    fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    fn persistence(&self) -> &Arc<LockedSessionStore> {
        &self.persistence
    }

    fn agent_runners(&self) -> &Arc<RwLock<HashMap<String, AgentRunner>>> {
        &self.agent_runners
    }

    fn account_sink(&self) -> &Arc<AccountEventSink> {
        &self.account_sink
    }

    fn config(&self) -> &Arc<RwLock<Config>> {
        &self.config
    }

    fn provider_registry(&self) -> &Arc<ProviderRegistry> {
        &self.provider_registry
    }

    async fn get_session_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        AppState::get_session_event_sender(self, session_id).await
    }

    async fn load_session_merged(&self, session_id: &str) -> Option<Session> {
        AppState::load_session_merged(self, session_id).await
    }

    async fn save_and_cache_session(&self, session: &mut Session) {
        AppState::save_and_cache_session(self, session).await
    }

    async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        AppState::get_provider(self).await
    }

    fn get_provider_for_model_ref(
        &self,
        target: &ProviderModelRef,
    ) -> Option<Arc<dyn LLMProvider>> {
        AppState::get_provider_for_model_ref(self, target).ok()
    }

    async fn get_provider_for_endpoint(&self, provider_name: &str) -> Option<Arc<dyn LLMProvider>> {
        AppState::get_provider_for_endpoint(self, provider_name)
            .await
            .ok()
    }

    fn title_gen_acquire(&self, session_id: &str) -> bool {
        AppState::title_gen_acquire(self, session_id)
    }

    fn title_gen_release(&self, session_id: &str) {
        AppState::title_gen_release(self, session_id)
    }
}
