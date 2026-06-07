//! Prompt context builders (extracted from server layer).
//!
//! Stateless functions that construct workspace, environment, and instruction
//! context for the agent system prompt.

pub mod instruction;

use bamboo_config::paths;
use bamboo_llm::Config;

pub const DEFAULT_BASE_PROMPT: &str =
    "You are Bodhi, a highly capable AI assistant. You run on the Bamboo agent runtime (you may see it referenced as \"Bamboo\" in injected context and tool names).\n\nYou help users solve problems quickly and correctly. Be concise, practical, and proactive.\nIf requirements are unclear, ask focused clarifying questions before proceeding.\nUse Task for non-trivial multi-step task tracking.\nDo not proactively use SubAgent/sub-agent delegation unless the user explicitly asks for sub agents, delegation, or parallel agent work.\n\nIf Bamboo has already injected relevant workspace or environment context, treat it as available working context instead of re-asking the user for the same information. Prefer a minimal verifiable attempt first, then diagnose failures and only ask follow-up questions for information that is still genuinely missing.\n\nYou have a persistent cross-session memory via the `memory` tool. When you learn a durable, non-derivable fact (a user preference, a confirmed decision, a stable reference), save it as one atomic memory with a specific, descriptive title. Treat injected memory as context to verify against current files, not as authoritative truth. Conversely, when the user refers to their own preferences, past decisions, or personal context you don't already know — including first-person questions about themselves (\"what do I...\", \"did I...\", \"我...?\") — query memory before answering instead of saying you don't know.\n\nWhen making function calls using tools, always include a brief text explanation before or alongside the tool calls describing what you are about to do and why. Never silently call tools without any visible narration to the user.";

pub const WORKSPACE_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_START -->";
pub const WORKSPACE_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_END -->";
pub const WORKSPACE_CONTEXT_PREFIX: &str = "Workspace path: ";
pub const ENV_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_START -->";
pub const ENV_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_END -->";

/// Guidance for workspace-based interactions
pub fn workspace_prompt_guidance() -> String {
    let config_path = paths::path_to_display_string(&paths::config_json_path());
    format!(
        "If you need to inspect files, check the workspace first, then Bamboo data at {}. Bamboo configuration is stored in {} (equivalent to ${{BAMBOO_DATA_DIR}}/config.json).",
        paths::bamboo_dir_display(),
        config_path
    )
}

fn build_env_prompt_guidance() -> Option<String> {
    let env_vars = Config::current_prompt_safe_env_vars();
    if env_vars.is_empty() {
        return None;
    }

    let mut lines = vec![
        "These environment variables were explicitly configured by the user inside Bodhi."
            .to_string(),
        "- They are already available to Bash/tool processes launched by Bodhi and may be relevant to tools and skills."
            .to_string(),
        "- Treat them as user-approved runtime context instead of asking the user to repeat them immediately."
            .to_string(),
        "- Secret values are intentionally hidden from the model.".to_string(),
        "- If the listed variables appear sufficient, prefer a minimal verification or execution attempt before asking follow-up questions."
            .to_string(),
        "- Only ask the user for additional env details after identifying a concrete missing variable, malformed value shape, or execution failure that cannot be resolved from this injected context."
            .to_string(),
    ];

    for entry in env_vars {
        let visibility = if entry.secret { "secret" } else { "non-secret" };
        let mut line = format!("- {} ({})", entry.name, visibility);
        if let Some(description) = entry.description {
            line.push_str(" — ");
            line.push_str(&description);
        }
        lines.push(line);
    }

    Some(lines.join("\n"))
}

pub fn build_env_prompt_context() -> Option<String> {
    let body = build_env_prompt_guidance()?;
    Some(format!(
        "{ENV_CONTEXT_START_MARKER}\n{body}\n{ENV_CONTEXT_END_MARKER}"
    ))
}

pub fn build_workspace_prompt_context(workspace_path: &str) -> Option<String> {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return None;
    }

    let body = format!(
        "{WORKSPACE_CONTEXT_PREFIX}{workspace_path}\n{}",
        workspace_prompt_guidance()
    );

    Some(format!(
        "{WORKSPACE_CONTEXT_START_MARKER}\n{body}\n{WORKSPACE_CONTEXT_END_MARKER}"
    ))
}

/// Assemble a full system prompt from base prompt, optional enhancement, and context segments.
///
/// This is the shared prompt assembly logic used by both the HTTP handler and the schedule
/// manager. It layers:
/// 1. Base system prompt (required)
/// 2. Optional enhancement text
/// 3. Workspace context (if workspace_path is provided)
/// 4. Instruction layer context (AGENTS.md / CLAUDE.md from workspace)
/// 5. Environment variable context
pub fn assemble_system_prompt(
    base: &str,
    enhance: Option<&str>,
    workspace_path: Option<&str>,
) -> String {
    let mut prompt = base.trim().to_string();
    if let Some(extra) = enhance.map(str::trim).filter(|v| !v.is_empty()) {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(extra);
    }
    if let Some(path) = workspace_path.map(str::trim).filter(|v| !v.is_empty()) {
        if let Some(segment) = build_workspace_prompt_context(path) {
            if !prompt.is_empty() {
                prompt.push_str("\n\n");
            }
            prompt.push_str(&segment);
        }
        if let Some(instruction_segment) = instruction::build_instruction_prompt_context(path) {
            if !prompt.is_empty() {
                prompt.push_str("\n\n");
            }
            prompt.push_str(&instruction_segment);
        }
    }
    if let Some(segment) = build_env_prompt_context() {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(&segment);
    }
    prompt
}
