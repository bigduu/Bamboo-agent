use crate::agent::core::{Message, Session};

use super::super::prompt_context::{strip_existing_external_memory, strip_existing_todo_list};

const WORKSPACE_CONTEXT_MARKER: &str = "\n\nWorkspace path: ";
const SKILL_CONTEXT_MARKERS: [&str; 2] = ["\n\n## Skill System\n", "\n\n## Available Skills\n"];
const TOOL_GUIDE_MARKER: &str = "## Tool Usage Guidelines\n";
const EXTERNAL_MEMORY_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n";
const TODO_LIST_MARKER: &str = "\n\n## Current Task List:";

pub(super) fn apply_workspace_path_to_session(session: &mut Session, workspace_path: &str) {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return;
    }

    session
        .metadata
        .insert("workspace_path".to_string(), workspace_path.to_string());

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        // Drop dynamic round sections first. They will be re-injected by the loop.
        let base_prompt =
            strip_existing_todo_list(&strip_existing_external_memory(&system_message.content));
        system_message.content = upsert_workspace_context(&base_prompt, workspace_path);
    } else {
        session.messages.insert(
            0,
            Message::system(upsert_workspace_context("", workspace_path)),
        );
    }
}

pub(super) fn upsert_workspace_context(prompt: &str, workspace_path: &str) -> String {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return strip_existing_workspace_context(prompt);
    }

    let guidance = crate::server::app_state::workspace_prompt_guidance();
    let segment = format!(
        "{WORKSPACE_CONTEXT_MARKER}{workspace_path}\n{}",
        guidance.trim()
    );
    let stripped = strip_existing_workspace_context(prompt);

    if stripped.trim().is_empty() {
        segment.trim_start().to_string()
    } else {
        format!("{}{}", stripped.trim_end(), segment)
    }
}

fn strip_existing_workspace_context(prompt: &str) -> String {
    let Some(start_idx) = prompt.find(WORKSPACE_CONTEXT_MARKER) else {
        return prompt.to_string();
    };

    let guidance = crate::server::app_state::workspace_prompt_guidance();
    if let Some(guidance_rel_idx) = prompt[start_idx..].find(&guidance) {
        let guidance_end_idx = start_idx + guidance_rel_idx + guidance.len();
        let mut out = String::new();
        out.push_str(prompt[..start_idx].trim_end());
        out.push_str(&prompt[guidance_end_idx..]);
        return out.trim_end().to_string();
    }

    let after_marker_idx = start_idx + WORKSPACE_CONTEXT_MARKER.len();
    let remainder = &prompt[after_marker_idx..];
    let next_section_idx = [
        remainder.find(SKILL_CONTEXT_MARKERS[0]),
        remainder.find(SKILL_CONTEXT_MARKERS[1]),
        remainder.find(TOOL_GUIDE_MARKER),
        remainder.find(EXTERNAL_MEMORY_MARKER),
        remainder.find(TODO_LIST_MARKER),
    ]
    .into_iter()
    .flatten()
    .min()
    .map(|idx| after_marker_idx + idx)
    .unwrap_or(prompt.len());

    let mut out = String::new();
    out.push_str(prompt[..start_idx].trim_end());
    out.push_str(&prompt[next_section_idx..]);
    out.trim_end().to_string()
}
