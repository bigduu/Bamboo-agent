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

/// Render the plan-mode section text from session state, or `None` when plan
/// mode is inactive.
///
/// The agent loop builds this into a dedicated volatile
/// [`ContextBlockType::PlanModeState`] block (see `build_plan_mode_context_block`)
/// rather than injecting it into the system message, so an inactive↔active plan
/// transition never rewrites the cached system prefix.
///
/// [`ContextBlockType::PlanModeState`]: bamboo_agent_core::ContextBlockType::PlanModeState
pub(super) fn render_plan_mode_section(session: &Session) -> Option<String> {
    let plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.plan_mode.as_ref())?;
    Some(build_plan_mode_instructions(plan_mode))
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
    fn render_plan_mode_section_returns_instructions_when_active() {
        let mut session = bamboo_agent_core::Session::new("test", "gpt-4");
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

        let section =
            render_plan_mode_section(&session).expect("active plan mode renders a section");
        assert!(section.contains("PLAN MODE ACTIVE"));
    }

    #[test]
    fn render_plan_mode_section_is_none_when_inactive() {
        let session = bamboo_agent_core::Session::new("test", "gpt-4");
        assert!(render_plan_mode_section(&session).is_none());
    }
}
