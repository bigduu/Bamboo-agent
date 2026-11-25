use std::sync::Arc;

use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_storage::session_merge::BACKGROUND_EXEC_STOP_RECEIPT_PREFIX;
use bamboo_storage::v2::SIDE_CHAT_CLEAR_EPOCH_KEY;
use bamboo_tools::tools::exec_command::{
    stop_owned_background_exec, BackgroundExecStopOutcome as ProcessStop,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    read_runtime_state, session_resume_lock, write_runtime_state, ChildCompletionCoordinator,
};

/// Outcome of stopping the exact background command selected by the Human.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundExecStopOutcome {
    Stopped,
    NotCurrent,
    Unconfirmed,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StopReceipt {
    conversation: BackgroundExecConversation,
    run_id: Option<String>,
    confirmed: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BackgroundExecConversation {
    session_created_at: DateTime<Utc>,
    conversation_reset_sha256: Option<String>,
    legacy_clear_epoch: Option<String>,
}

impl BackgroundExecConversation {
    pub(super) fn capture(session: &Session) -> std::io::Result<Self> {
        Ok(Self {
            session_created_at: session.created_at,
            conversation_reset_sha256: session
                .conversation_reset_sha256()
                .map_err(std::io::Error::other)?,
            legacy_clear_epoch: session.metadata.get(SIDE_CHAT_CLEAR_EPOCH_KEY).cloned(),
        })
    }

    pub(super) fn matches(&self, session: &Session) -> bool {
        session.created_at == self.session_created_at
            && session.metadata.get(SIDE_CHAT_CLEAR_EPOCH_KEY) == self.legacy_clear_epoch.as_ref()
            && session
                .conversation_reset_sha256()
                .is_ok_and(|reset| reset == self.conversation_reset_sha256)
    }
}

fn receipt_key(handle: &str) -> String {
    format!("{BACKGROUND_EXEC_STOP_RECEIPT_PREFIX}{handle}")
}

pub(super) fn has_background_exec_stop(session: &Session, handle: &str) -> bool {
    session.metadata.contains_key(&receipt_key(handle))
}

fn selected_wait(session: &Session, runtime: &AgentRuntimeState, handle: &str) -> bool {
    session
        .metadata
        .get("runtime.background_exec_id")
        .is_some_and(|id| id == handle)
        && runtime
            .waiting_for_bash
            .as_ref()
            .is_some_and(|wait| matches!(wait.bash_ids.as_slice(), [id] if id == handle))
}

impl ChildCompletionCoordinator {
    pub async fn stop_background_exec(
        &self,
        session_id: &str,
        exec_session_id: &str,
    ) -> std::io::Result<BackgroundExecStopOutcome> {
        // An HTTP disconnect cannot strand the command after admission.
        let coordinator = self.clone();
        let session_id = session_id.to_owned();
        let exec_session_id = exec_session_id.to_owned();
        tokio::spawn(async move {
            coordinator
                .stop_background_exec_inner(&session_id, &exec_session_id)
                .await
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn stop_background_exec_inner(
        &self,
        session_id: &str,
        handle: &str,
    ) -> std::io::Result<BackgroundExecStopOutcome> {
        use BackgroundExecStopOutcome::{NotCurrent, Stopped, Unconfirmed};

        // Serialize the durable stop decision with both automatic wake paths.
        let guard = session_resume_lock(session_id);
        let _held = guard.lock().await;
        let Some(observed) = self.storage.load_session(session_id).await? else {
            return Ok(NotCurrent);
        };
        let key = receipt_key(handle);
        let receipt = if let Some(value) = observed.metadata.get(&key) {
            let receipt = serde_json::from_str::<StopReceipt>(value)?;
            if !receipt.conversation.matches(&observed) {
                return Ok(NotCurrent);
            }
            if receipt.confirmed {
                return Ok(Stopped);
            }
            receipt
        } else {
            let receipt = StopReceipt {
                conversation: BackgroundExecConversation::capture(&observed)?,
                run_id: observed.host_run_id().map(str::to_owned),
                confirmed: false,
            };
            let intent = serde_json::to_string(&receipt)?;
            let claimed =
                self.persistence
                    .mutate_runtime_session_and_publish(
                        session_id,
                        || None,
                        |latest| {
                            let runtime = read_runtime_state(latest);
                            if !receipt.conversation.matches(latest)
                                || latest.kind != SessionKind::Root
                                || latest.host_run_id() != receipt.run_id.as_deref()
                                || latest.last_run_status().as_deref() != Some("suspended")
                                || runtime.status != AgentStatusState::Suspended
                                || runtime.suspension.as_ref().is_none_or(|suspension| {
                                    suspension.reason != "waiting_for_bash"
                                })
                                || runtime.waiting_for_children.is_some()
                                || latest.has_pending_question()
                                || !selected_wait(latest, &runtime, handle)
                            {
                                return Err(());
                            }
                            latest.metadata.insert(key, intent);
                            Ok(())
                        },
                        |saved| self.cache_background_exec_stop(saved),
                    )
                    .await?;
            if !matches!(claimed, Ok(Some(_))) {
                return Ok(NotCurrent);
            }
            receipt
        };
        if stop_owned_background_exec(session_id, handle).await != ProcessStop::Stopped {
            // Keep the exact wait available for retry and suppress its wake.
            return Ok(Unconfirmed);
        }

        self.confirm_background_exec_stop(session_id, handle, receipt)
            .await
    }

    /// The caller holds the resume lock and captured terminal proof from the
    /// old monitor's retained process owner, even if its HTTP waiter left.
    pub(super) async fn finish_background_exec_stop_from_monitor(
        &self,
        session: &Session,
        handle: &str,
    ) -> std::io::Result<()> {
        let Some(value) = session.metadata.get(&receipt_key(handle)) else {
            return Ok(());
        };
        let receipt = serde_json::from_str::<StopReceipt>(value)?;
        if !receipt.confirmed && receipt.conversation.matches(session) {
            self.confirm_background_exec_stop(&session.id, handle, receipt)
                .await?;
        }
        Ok(())
    }

    async fn confirm_background_exec_stop(
        &self,
        session_id: &str,
        handle: &str,
        receipt: StopReceipt,
    ) -> std::io::Result<BackgroundExecStopOutcome> {
        use BackgroundExecStopOutcome::{NotCurrent, Stopped};

        let key = receipt_key(handle);
        let confirmed = StopReceipt {
            confirmed: true,
            ..receipt.clone()
        };
        let confirmation = serde_json::to_string(&confirmed)?;
        let saved = self
            .persistence
            .mutate_runtime_session_and_publish(
                session_id,
                || None,
                |latest| {
                    let latest_receipt = latest
                        .metadata
                        .get(&key)
                        .and_then(|value| serde_json::from_str::<StopReceipt>(value).ok());
                    if !receipt.conversation.matches(latest) {
                        return Err(NotCurrent);
                    }
                    if latest_receipt.as_ref() == Some(&confirmed) {
                        return Err(Stopped);
                    }
                    if latest_receipt.as_ref() != Some(&receipt) {
                        return Err(NotCurrent);
                    }
                    latest.metadata.insert(key, confirmation);
                    let mut runtime = read_runtime_state(latest);
                    if selected_wait(latest, &runtime, handle) {
                        clear_stopped_wait(latest, &mut runtime, &receipt);
                    }
                    latest.updated_at = Utc::now();
                    Ok(())
                },
                |saved| self.cache_background_exec_stop(saved),
            )
            .await?;
        match saved {
            Ok(Some(_)) => {
                if let Some(sink) = &self.root_account_sink {
                    sink.record(
                        Some(session_id),
                        &AgentEvent::SessionHistoryCommitted {
                            session_id: session_id.to_owned(),
                        },
                    );
                }
                Ok(Stopped)
            }
            Ok(None) => Ok(NotCurrent),
            Err(outcome) => Ok(outcome),
        }
    }

    fn cache_background_exec_stop(&self, saved: &Session) {
        self.sessions.insert(
            saved.id.clone(),
            Arc::new(crate::SessionSnapshot::new(saved.clone())),
        );
    }
}

fn clear_stopped_wait(
    latest: &mut Session,
    runtime: &mut AgentRuntimeState,
    receipt: &StopReceipt,
) {
    runtime.waiting_for_bash = None;
    latest.metadata.remove("runtime.background_exec_id");
    if latest
        .metadata
        .get("runtime.suspend_reason")
        .is_some_and(|reason| reason == "waiting_for_bash")
    {
        latest.metadata.remove("runtime.suspend_reason");
    }
    if runtime
        .suspension
        .as_ref()
        .is_some_and(|suspension| suspension.reason == "waiting_for_bash")
    {
        runtime.suspension = None;
        if runtime.status == AgentStatusState::Suspended {
            runtime.status = AgentStatusState::Idle;
        }
    }
    if latest.host_run_id() == receipt.run_id.as_deref()
        && latest.last_run_status().as_deref() == Some("suspended")
    {
        runtime.status = AgentStatusState::Cancelled;
        latest.set_last_run_status("cancelled");
        latest.clear_last_run_error();
    }
    write_runtime_state(latest, runtime);
}
