//! Dependency-inversion context for the agent-session-orchestration cluster.
//!
//! The session/event/title/gold modules that live in this crate need a handful
//! of shared application resources (sessions cache, storage, persistence,
//! providers, the per-session event senders, and the account change-feed sink).
//! Historically those were reached through the server's `AppState`, which
//! coupled the cluster to the actix-web server crate.
//!
//! [`AgentSessionContext`] inverts that dependency: the cluster depends only on
//! this trait, and the server implements it on `AppState`. This keeps the
//! orchestration code free of any HTTP / server types.
//!
//! Accessor return types are deliberately the *exact* field types `AppState`
//! already exposes, so the server-side impl is a thin delegation and no copying
//! is introduced. Provider lookups return `Option` (the server's fallible
//! lookups discard the error type, which is actix-coupled).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::ProviderModelRef;
use bamboo_infrastructure::{Config, LLMProvider, LockedSessionStore, ProviderRegistry};

use crate::events::AccountEventSink;
use crate::runtime::execution::runner_state::AgentRunner;

/// Shared application context the agent-session-orchestration cluster depends
/// on, in place of the server's concrete `AppState`.
///
/// Implemented by the server on `AppState`. Every accessor mirrors an existing
/// `AppState` field or method; the trait exists solely to invert the
/// cluster→server dependency.
#[async_trait]
pub trait AgentSessionContext: Send + Sync {
    /// In-memory session cache.
    fn sessions(&self) -> &Arc<RwLock<HashMap<String, Session>>>;

    /// Persistent session storage backend.
    fn storage(&self) -> &Arc<dyn Storage>;

    /// Per-session write-serialising persistence layer.
    fn persistence(&self) -> &Arc<LockedSessionStore>;

    /// Active agent runners, keyed by session id.
    fn agent_runners(&self) -> &Arc<RwLock<HashMap<String, AgentRunner>>>;

    /// Account-wide durable change-feed sink.
    fn account_sink(&self) -> &Arc<AccountEventSink>;

    /// Hot-reloadable application configuration.
    fn config(&self) -> &Arc<RwLock<Config>>;

    /// Multi-provider registry.
    fn provider_registry(&self) -> &Arc<ProviderRegistry>;

    /// Get (or create) the long-lived broadcast sender for a session's events.
    async fn get_session_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent>;

    /// Load a session, merging the in-memory and on-disk copies.
    async fn load_session_merged(&self, session_id: &str) -> Option<Session>;

    /// Persist a session and refresh the in-memory cache.
    async fn save_and_cache_session(&self, session: &mut Session);

    /// Get the always-current default provider handle.
    async fn get_provider(&self) -> Arc<dyn LLMProvider>;

    /// Route to a provider for a specific provider/model ref.
    ///
    /// Returns `None` on failure (the server's fallible lookup discards its
    /// actix-coupled error type).
    fn get_provider_for_model_ref(&self, target: &ProviderModelRef)
        -> Option<Arc<dyn LLMProvider>>;

    /// Resolve a provider for a named provider endpoint.
    ///
    /// Returns `None` on failure (see [`Self::get_provider_for_model_ref`]).
    async fn get_provider_for_endpoint(&self, provider_name: &str)
        -> Option<Arc<dyn LLMProvider>>;

    /// Try to claim the title-generation slot for `session_id`.
    /// Returns `true` on success, `false` if generation is already in flight.
    fn title_gen_acquire(&self, session_id: &str) -> bool;

    /// Release the title-generation slot for `session_id`. Idempotent.
    fn title_gen_release(&self, session_id: &str);
}
