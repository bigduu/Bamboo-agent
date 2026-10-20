//! Managed loops deliberately stay within one live owner. Refuse a wait
//! before it exists; ordinary interactive runs and crash fences are unchanged.
use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;

pub(crate) const RUNNING_REFUSAL: &str =
    "managed loop refused a detached Running outcome; cleanup requested, stop unconfirmed";

pub(crate) fn active(config: &AgentLoopConfig) -> bool {
    config
        .loop_control
        .as_ref()
        .is_some_and(|control| control.generation().is_some())
}

pub(crate) async fn start_refusal(
    session: &Session,
    config: &AgentLoopConfig,
) -> Option<&'static str> {
    if config
        .loop_control
        .as_ref()
        .is_some_and(|control| control.has_observed_running())
        || bamboo_agent_core::tools::capture_suspension_restriction()
            .is_some_and(|scope| scope.observation().running_seen)
    {
        return Some("existing_detached_work_incompatible");
    }
    if config.guardian_active() || config.hook_runner.has_suspending_loop_control_hooks() {
        return Some("suspending_lifecycle_incompatible");
    }
    if session.pending_question.is_some()
        || session.metadata.contains_key("runtime.suspend_reason")
        || session.agent_runtime_state.as_ref().is_some_and(|runtime| {
            matches!(runtime.status, bamboo_domain::AgentStatusState::Suspended)
                || runtime.suspension.is_some()
                || runtime.waiting_for_children.is_some()
                || runtime.waiting_for_bash.is_some()
        })
        || !bamboo_tools::tools::bash_runtime::running_shells_for_session(&session.id).is_empty()
    {
        return Some("existing_wait_incompatible");
    }
    if let Some(storage) = config.storage.as_ref() {
        let children = match storage.list_child_run_statuses(&session.id).await {
            Ok(children) => children,
            Err(_) => return Some("cannot_observe_existing_children"),
        };
        if children.iter().any(|(_, status)| {
            !matches!(
                status.as_deref(),
                Some("completed" | "error" | "timeout" | "cancelled" | "skipped")
            )
        }) {
            return Some("active_children_incompatible");
        }
    }
    None
}
