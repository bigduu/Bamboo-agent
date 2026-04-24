//! Shared adapter implementing `ChildSessionPort` for server-side child session tools.
//!
//! Both `SpawnSessionTool` and `SubSessionManagerTool` delegate to this adapter
//! instead of duplicating `ChildSessionPort` implementations.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration, Instant};

use crate::app_state::session_events::get_or_create_event_sender;
use crate::app_state::{AgentRunner, AgentStatus};
use crate::session_app::child_session::{
    ChildSessionEntry, ChildSessionError, ChildSessionPort, DeleteChildResult,
};
use crate::spawn_scheduler::{SpawnJob, SpawnScheduler};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolError;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_infrastructure::{SessionIndexEntry, SessionStoreV2};

/// Server-side adapter that bridges domain `ChildSessionPort` to infrastructure.
///
/// Holds all shared state needed by both `SpawnSessionTool` and `SubSessionManagerTool`.
/// Implements the full `ChildSessionPort` trait with real methods (no stubs).
pub struct ChildSessionAdapter {
    pub(crate) session_store: Arc<SessionStoreV2>,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) scheduler: Arc<SpawnScheduler>,
    pub(crate) sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    pub(crate) agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub(crate) session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    /// Optional subagent model resolver: maps subagent_type → model name.
    pub(crate) subagent_model_resolver: Option<Arc<dyn Fn(&str) -> Option<String> + Send + Sync>>,
}

impl ChildSessionAdapter {
    /// Resolve the model for a given subagent_type using the configured resolver.
    pub fn resolve_subagent_model(&self, subagent_type: &str) -> Option<String> {
        self.subagent_model_resolver
            .as_ref()
            .and_then(|resolver| resolver(subagent_type))
    }
}

fn map_index_entry_to_child_entry(entry: &SessionIndexEntry) -> ChildSessionEntry {
    ChildSessionEntry {
        child_session_id: entry.id.clone(),
        title: entry.title.clone(),
        pinned: entry.pinned,
        message_count: entry.message_count,
        updated_at: entry.updated_at.to_rfc3339(),
        last_run_status: entry.last_run_status.clone(),
        last_run_error: entry.last_run_error.clone(),
    }
}

#[async_trait]
impl ChildSessionPort for ChildSessionAdapter {
    async fn load_root_session(&self, root_session_id: &str) -> Result<Session, ChildSessionError> {
        let Some(session) = self
            .storage
            .load_session(root_session_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!(
                    "failed to load session {root_session_id}: {error}"
                ))
            })?
        else {
            return Err(ChildSessionError::NotFound(root_session_id.to_string()));
        };

        if session.kind != SessionKind::Root {
            return Err(ChildSessionError::NotRootSession(
                root_session_id.to_string(),
            ));
        }

        Ok(session)
    }

    async fn load_child_for_parent(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) -> Result<Session, ChildSessionError> {
        let Some(child) = self
            .storage
            .load_session(child_session_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!(
                    "failed to load child session {child_session_id}: {error}"
                ))
            })?
        else {
            return Err(ChildSessionError::NotFound(child_session_id.to_string()));
        };

        if child.kind != SessionKind::Child {
            return Err(ChildSessionError::NotChildSession(
                child_session_id.to_string(),
            ));
        }

        if child.parent_session_id.as_deref() != Some(parent_session_id) {
            return Err(ChildSessionError::NotChildOfParent {
                child_id: child_session_id.to_string(),
                parent_id: parent_session_id.to_string(),
            });
        }

        Ok(child)
    }

    async fn save_child_session(&self, child: &Session) -> Result<(), ChildSessionError> {
        self.storage.save_session(child).await.map_err(|error| {
            ChildSessionError::Execution(format!("failed to save child session: {error}"))
        })?;

        let mut sessions = self.sessions_cache.write().await;
        sessions.insert(child.id.clone(), child.clone());

        Ok(())
    }

    async fn is_child_running(&self, child_session_id: &str) -> bool {
        let runners = self.agent_runners.read().await;
        runners
            .get(child_session_id)
            .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
    }

    async fn list_children(&self, parent_session_id: &str) -> Vec<ChildSessionEntry> {
        self.session_store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|entry| {
                entry.kind == SessionKind::Child
                    && entry.parent_session_id.as_deref() == Some(parent_session_id)
            })
            .map(|entry| map_index_entry_to_child_entry(&entry))
            .collect()
    }

    async fn enqueue_child_run(
        &self,
        parent: &Session,
        child: &Session,
    ) -> Result<(), ChildSessionError> {
        let model = if child.model.trim().is_empty() {
            parent.model.clone()
        } else {
            child.model.clone()
        };
        if model.trim().is_empty() {
            return Err(ChildSessionError::Execution(
                "child model is empty and parent model is unavailable".to_string(),
            ));
        }

        self.scheduler
            .enqueue(SpawnJob {
                parent_session_id: parent.id.clone(),
                child_session_id: child.id.clone(),
                model,
            })
            .await
            .map_err(ChildSessionError::Execution)?;

        let parent_tx = get_or_create_event_sender(&self.session_event_senders, &parent.id).await;
        let _ = parent_tx.send(AgentEvent::SubSessionStarted {
            parent_session_id: parent.id.clone(),
            child_session_id: child.id.clone(),
            title: Some(child.title.clone()),
        });

        Ok(())
    }

    async fn cancel_child_run_and_wait(
        &self,
        child_session_id: &str,
    ) -> Result<(), ChildSessionError> {
        let cancelled = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.get_mut(child_session_id) {
                if matches!(runner.status, AgentStatus::Running) {
                    runner.cancel_token.cancel();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if !cancelled {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let still_running = {
                let runners = self.agent_runners.read().await;
                runners
                    .get(child_session_id)
                    .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
            };
            if !still_running {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ChildSessionError::Execution(format!(
                    "timed out waiting for child session {child_session_id} to stop after cancellation"
                )));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn delete_child_session(
        &self,
        parent_session_id: &str,
        child_id: &str,
    ) -> Result<DeleteChildResult, ChildSessionError> {
        let cancelled_running_child = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.remove(child_id) {
                runner.cancel_token.cancel();
                true
            } else {
                false
            }
        };

        let deleted = self
            .storage
            .delete_session(child_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!("failed to delete child session: {error}"))
            })?;

        {
            let mut sessions = self.sessions_cache.write().await;
            sessions.remove(child_id);
        }
        {
            let mut senders = self.session_event_senders.write().await;
            senders.remove(child_id);
            if cancelled_running_child {
                if let Some(parent_tx) = senders.get(parent_session_id) {
                    let _ = parent_tx.send(AgentEvent::SubSessionCompleted {
                        parent_session_id: parent_session_id.to_string(),
                        child_session_id: child_id.to_string(),
                        status: "cancelled".to_string(),
                        error: Some("Child session deleted while running".to_string()),
                    });
                }
            }
        }

        Ok(DeleteChildResult {
            deleted,
            cancelled_running_child,
        })
    }
}

/// Map a `ChildSessionError` to a server `ToolError`.
pub fn tool_error_from_child_session(error: ChildSessionError) -> ToolError {
    match error {
        ChildSessionError::NotFound(id) => ToolError::Execution(format!("session not found: {id}")),
        ChildSessionError::NotRootSession(id) => {
            ToolError::Execution(format!("session is not a root session: {id}"))
        }
        ChildSessionError::InvalidArguments(msg) => ToolError::InvalidArguments(msg),
        ChildSessionError::Execution(msg) => ToolError::Execution(msg),
        other => ToolError::Execution(other.to_string()),
    }
}
