//! Host run identity and terminal read-model projection. These values are not
//! execution authority: Storage validates the current incarnation/activation.

use super::Session;
use crate::{AgentRuntimeState, AgentStatusState};

const HOST_RUN_ID: &str = "runtime.host_run_id";

impl Session {
    pub fn host_run_id(&self) -> Option<&str> {
        self.metadata.get(HOST_RUN_ID).map(String::as_str)
    }

    pub fn set_host_run_id(&mut self, run_id: &str) {
        self.metadata.insert(HOST_RUN_ID.into(), run_id.into());
        self.metadata.insert(
            "runtime.host_launch_generation".into(),
            self.child_launch_generation().to_string(),
        );
    }

    pub fn host_run_matches_launch(&self) -> bool {
        match self.metadata.get("runtime.host_launch_generation") {
            Some(value) => value.parse::<u64>().ok() == Some(self.child_launch_generation()),
            None => self.host_run_id().is_none(),
        }
    }

    /// A completed activation may have handed execution to a durable wait.
    /// Neither a successful Worker return nor an absent runner clears it.
    pub fn has_runtime_wait(&self) -> bool {
        let legacy = self
            .metadata
            .get("agent.runtime.state")
            .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok());
        self.pending_question.is_some()
            || self
                .metadata
                .get("runtime.suspend_reason")
                .is_some_and(|s| !s.trim().is_empty())
            || self
                .agent_runtime_state
                .as_ref()
                .or(legacy.as_ref())
                .is_some_and(|state| {
                    state.status == AgentStatusState::Suspended
                        || state.suspension.is_some()
                        || state.waiting_for_children.is_some()
                        || state.waiting_for_bash.is_some()
                        || state.plan_mode.as_ref().is_some_and(|plan| {
                            plan.status == crate::PlanModeStatus::AwaitingApproval
                        })
                })
    }

    /// Project an already trusted final outcome, preserving round/accounting
    /// data and the legacy mirror. Never infer terminal truth from typed state.
    pub fn synchronize_terminal_runtime_status(&mut self) {
        if self.has_runtime_wait() {
            return;
        }
        let status = match self.last_run_status().as_deref() {
            Some("completed" | "skipped") => AgentStatusState::Completed,
            Some("cancelled") => AgentStatusState::Cancelled,
            Some("error" | "failed" | "timeout") => AgentStatusState::Failed,
            _ => return,
        };
        if self.agent_runtime_state.is_none() {
            self.agent_runtime_state = self
                .metadata
                .get("agent.runtime.state")
                .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok());
        }
        let state = self
            .agent_runtime_state
            .get_or_insert_with(|| AgentRuntimeState::new(&self.id));
        state.status = status;
        state.tools.active_tool_calls = 0;
        if let Ok(raw) = serde_json::to_string(state) {
            self.metadata.insert("agent.runtime.state".into(), raw);
        }
    }
}
