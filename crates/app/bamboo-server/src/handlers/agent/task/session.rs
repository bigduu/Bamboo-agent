//! Session loading for the task handler.
//!
//! The main `load_session_from_memory_or_storage` has been migrated to
//! `AppState::load_session` in `crate::app_state::session_loader`.
use bamboo_agent_core::{Session, SessionKind};
use bamboo_engine::{
    ticket_runtime::TICKET_DISPATCH_KEY, ticket_worker_plan::TICKET_LOCAL_PLAN_KEY,
};

use crate::app_state::AppState;

pub(super) async fn task_session(state: &AppState, session: Session) -> Session {
    // Work's trusted Host dispatch and Task adapter install private plan markers,
    // including before the first Task has projected any items.
    // These markers select display data only; execution authority is unchanged.
    if session.kind != SessionKind::Child
        || session.metadata.contains_key(TICKET_LOCAL_PLAN_KEY)
        || session.metadata.contains_key(TICKET_DISPATCH_KEY)
    {
        return session;
    }
    state
        .load_session(&session.root_session_id)
        .await
        .unwrap_or(session)
}
