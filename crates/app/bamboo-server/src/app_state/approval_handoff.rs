//! Native I/O for the Supervisor's bounded approval handoff.

use std::sync::{Arc, OnceLock, Weak};

use actix_web::web;
use bamboo_domain::{SessionActivationPolicy, SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID};
use chrono::{DateTime, Utc};

use crate::handlers::agent::respond::capture_approval_handoff;

use super::AppState;

pub(super) type ApprovalRuntime = Arc<OnceLock<Weak<AppState>>>;

impl AppState {
    pub(crate) fn into_http_data(self) -> web::Data<Self> {
        let state = Arc::new(self);
        let weak = Arc::downgrade(&state);
        let _ = state.approval_runtime.set(weak.clone());
        if state.spawn_scheduler.supports_native_ticket_budget() {
            let shutdown = state.mcp_proxy_shutdown.clone();
            tokio::spawn(async move {
                let mut ticks = tokio::time::interval(
                    bamboo_orchestration::managed_session::approval_handoff::SCAN_INTERVAL,
                );
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut after = None;
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = ticks.tick() => {}
                    }
                    let Some(state) = weak.upgrade() else { break };
                    if let Err(error) = sweep(&state, &mut after, Utc::now()).await {
                        tracing::debug!(%error, "Managed approval handoff will retry");
                    }
                }
            });
        }
        web::Data::from(state)
    }
}

pub(super) async fn sweep(
    state: &AppState,
    after: &mut Option<String>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    use bamboo_orchestration::managed_session::{
        approval::classify_candidate, approval_handoff::select_candidates,
    };

    if state
        .permission_section
        .snapshot()
        .data
        .supervisor_approval_handoff_since
        .is_none()
    {
        return Ok(());
    }
    let entries = state.session_store.list_index_entries().await;
    let candidates = select_candidates(
        entries.into_iter().filter_map(|entry| {
            let eligible = entry.id != DEFAULT_SUPERVISOR_SESSION_ID
                && classify_candidate(
                    entry.kind,
                    entry.has_pending_question,
                    entry.session_origin.as_deref(),
                    entry.pending_material_run,
                    entry.material_notification_hint.is_some(),
                )
                .is_some();
            eligible.then_some(entry.id)
        }),
        after,
    );
    if candidates.is_empty() {
        return Ok(());
    }
    let Some(parent) = state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await?
    else {
        return Ok(());
    };
    let Some(incarnation_id) = parent.supervisor_incarnation_id() else {
        return Ok(());
    };
    let supervisor = SupervisorReference {
        session_id: parent.id,
        incarnation_id,
    };
    let mut wake = false;
    for id in candidates {
        let result = async {
            let Some(target) = state.storage.load_session(&id).await? else {
                return Ok(false);
            };
            let Some((handoff, request)) = capture_approval_handoff(state, &supervisor, &target)
            else {
                return Ok(false);
            };
            let Some(notice) = handoff.notice(&supervisor, &request, now) else {
                return Ok(false);
            };
            let receipt = state
                .session_inbox
                .deliver_runtime_for_target(&notice, parent.created_at)
                .await?;
            state
                .session_inbox
                .mark_message_activation_eligible(
                    &supervisor.session_id,
                    &receipt,
                    SessionActivationPolicy::RespectSpecificWait,
                )
                .await?;
            Ok::<_, anyhow::Error>(true)
        }
        .await;
        match result {
            Ok(eligible) => wake |= eligible,
            Err(error) => {
                tracing::debug!(session_id = %id, %error, "Managed approval proof will retry")
            }
        }
    }
    if wake {
        state
            .session_activation_router
            .reconcile_wake(&supervisor.session_id, now)
            .await?;
    }
    Ok(())
}
