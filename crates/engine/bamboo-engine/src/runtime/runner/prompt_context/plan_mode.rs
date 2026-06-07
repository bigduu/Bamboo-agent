use bamboo_agent_core::Session;
use bamboo_domain::session::runtime_state::{PlanModeState, PlanModeStatus};

const PLAN_MODE_START_MARKER: &str = "<!-- BAMBOO_PLAN_MODE_START -->";
const PLAN_MODE_END_MARKER: &str = "<!-- BAMBOO_PLAN_MODE_END -->";

/// Build the plan mode instructions text for injection into the system prompt.
pub(super) fn build_plan_mode_instructions(plan_mode: &PlanModeState) -> String {
    let status_hint = match plan_mode.status {
        PlanModeStatus::Exploring => {
            "Phase: EXPLORE — Use read-only tools to understand the codebase."
        }
        PlanModeStatus::Designing => {
            "Phase: DESIGN — Formulate a concrete implementation approach."
        }
        PlanModeStatus::Reviewing => {
            "Phase: REVIEW — Review the plan for completeness and correctness."
        }
        PlanModeStatus::Finalizing => "Phase: FINALIZE — Write the plan to the plan file.",
        PlanModeStatus::AwaitingApproval => {
            "Phase: AWAITING APPROVAL — Plan is ready for user review."
        }
    };

    format!(
        r#"=== PLAN MODE ACTIVE ===

{status_hint}

The user has indicated they want you to PLAN before implementing. You MUST NOT:
- Make any file edits (Write, Edit, NotebookEdit)
- Execute any shell commands (Bash, BashOutput)
- Make HTTP requests that modify state
- Spawn child sessions for implementation (only read-only exploration)

You MAY:
- Read files, search code (Read, Glob, Grep, GetFileInfo)
- Fetch web content (WebFetch, WebSearch)
- Use SubAgent with subagent_type="plan" to spawn read-only exploration agents
- Update the task list (Task tool) — tasks created in plan mode default to phase=planning
- Write notes to session memory (session_note tool)

Your goal: Explore the codebase thoroughly, understand the problem, and produce a detailed implementation plan.

Workflow:
1. EXPLORE: Use read-only tools to understand the codebase structure and relevant code
2. DESIGN: Formulate a concrete implementation approach with clear steps
3. DECOMPOSE: Use the Task tool to break the plan into concrete steps (tasks default to planning phase)
4. WRITE PLAN: Save your plan (the assistant runtime will handle plan file persistence)
5. EXIT: Call ExitPlanMode when the plan is ready for user review

The plan should include:
- Files that need to be changed
- Key functions/structs to modify
- Testing approach
- Any risks or considerations
"#
    )
}

/// Inject plan mode instructions into the system message if plan mode is active,
/// or strip them if plan mode is not active.
pub(crate) fn inject_plan_mode_instructions(session: &mut Session) {
    let plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.plan_mode.as_ref());

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
    {
        let base_prompt = strip_existing_plan_mode_instructions(&system_message.content);

        if let Some(plan_mode) = plan_mode {
            let instructions = build_plan_mode_instructions(plan_mode);
            system_message.content = format!(
                "{}\n\n{}\n{}\n{}",
                base_prompt.trim_end(),
                PLAN_MODE_START_MARKER,
                instructions.trim(),
                PLAN_MODE_END_MARKER,
            );
            tracing::debug!(
                "Injected plan mode instructions into system message ({} chars)",
                instructions.len()
            );
        } else {
            system_message.content = base_prompt;
        }
    }
}

/// Strip existing plan mode instructions from a prompt.
pub(super) fn strip_existing_plan_mode_instructions(prompt: &str) -> String {
    super::system_sections::strip_existing_prompt_block(
        prompt,
        PLAN_MODE_START_MARKER,
        PLAN_MODE_END_MARKER,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn build_plan_mode_instructions_contains_key_constraints() {
        let plan_mode = PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        };
        let instructions = build_plan_mode_instructions(&plan_mode);
        assert!(instructions.contains("PLAN MODE ACTIVE"));
        assert!(instructions.contains("MUST NOT"));
        assert!(instructions.contains("Write, Edit, NotebookEdit"));
        assert!(instructions.contains("Bash"));
        assert!(instructions.contains("EXPLORE"));
    }

    #[test]
    fn strip_plan_mode_instructions_removes_block() {
        let prompt = "Base prompt\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPlan mode content\n<!-- BAMBOO_PLAN_MODE_END -->\n\nMore content";
        let stripped = strip_existing_plan_mode_instructions(prompt);
        assert!(!stripped.contains("PLAN MODE ACTIVE"));
        assert!(!stripped.contains("BAMBOO_PLAN_MODE_START"));
        assert!(stripped.contains("Base prompt"));
        assert!(stripped.contains("More content"));
    }

    #[test]
    fn inject_plan_mode_adds_instructions_when_active() {
        let mut session = bamboo_agent_core::Session::new("test", "gpt-4");
        session.messages.insert(
            0,
            bamboo_agent_core::Message::system("Base prompt".to_string()),
        );
        session.agent_runtime_state =
            Some(bamboo_domain::session::runtime_state::AgentRuntimeState::new("run-1"));
        if let Some(ref mut state) = session.agent_runtime_state {
            state.plan_mode = Some(PlanModeState {
                entered_at: Utc::now(),
                pre_permission_mode: "default".to_string(),
                plan_file_path: None,
                status: PlanModeStatus::Exploring,
            });
        }

        inject_plan_mode_instructions(&mut session);

        let system_msg = session
            .messages
            .iter()
            .find(|m| matches!(m.role, bamboo_agent_core::Role::System))
            .unwrap();
        assert!(system_msg.content.contains("PLAN MODE ACTIVE"));
        assert!(system_msg.content.contains("Base prompt"));
    }

    #[test]
    fn inject_plan_mode_removes_instructions_when_inactive() {
        let mut session = bamboo_agent_core::Session::new("test", "gpt-4");
        session.messages.insert(0, bamboo_agent_core::Message::system(
            "Base prompt\n\n<!-- BAMBOO_PLAN_MODE_START -->\nPlan mode content\n<!-- BAMBOO_PLAN_MODE_END -->".to_string()
        ));

        inject_plan_mode_instructions(&mut session);

        let system_msg = session
            .messages
            .iter()
            .find(|m| matches!(m.role, bamboo_agent_core::Role::System))
            .unwrap();
        assert!(!system_msg.content.contains("PLAN MODE ACTIVE"));
        assert!(system_msg.content.contains("Base prompt"));
    }
}
