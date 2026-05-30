use bamboo_agent_core::{Message, Session};

use super::super::prompt_context::{strip_existing_external_memory, strip_existing_task_list};

const WORKSPACE_CONTEXT_START_MARKER: &str =
    crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER;
const WORKSPACE_CONTEXT_END_MARKER: &str = crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER;
const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const LEGACY_TODO_LIST_START_MARKER: &str = "<!-- BAMBOO_TODO_LIST_START -->";
const LEGACY_WORKSPACE_CONTEXT_MARKER: &str = "\n\nWorkspace path: ";
const LEGACY_SKILL_CONTEXT_MARKERS: [&str; 2] =
    ["\n\n## Skill System\n", "\n\n## Available Skills\n"];
const LEGACY_TOOL_GUIDE_MARKER: &str = "## Tool Usage Guidelines\n";
const LEGACY_EXTERNAL_MEMORY_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n";
const LEGACY_TASK_LIST_MARKER: &str = "\n\n## Current Task List:";

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
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
    {
        // Drop dynamic round sections first. They will be re-injected by the loop.
        let base_prompt =
            strip_existing_task_list(&strip_existing_external_memory(&system_message.content));
        system_message.content = upsert_workspace_context(&base_prompt, workspace_path);
        crate::runtime::runner::refresh_prompt_snapshot(session);
    } else {
        session.messages.insert(
            0,
            Message::system(upsert_workspace_context("", workspace_path)),
        );
        crate::runtime::runner::refresh_prompt_snapshot(session);
    }
}

pub(super) fn upsert_workspace_context(prompt: &str, workspace_path: &str) -> String {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return strip_existing_workspace_context(prompt);
    }

    let Some(segment) = crate::runtime::context::build_workspace_prompt_context(workspace_path)
    else {
        return strip_existing_workspace_context(prompt);
    };
    let stripped = strip_existing_workspace_context(prompt);

    if stripped.trim().is_empty() {
        segment
    } else {
        format!("{}\n\n{}", stripped.trim_end(), segment)
    }
}

fn strip_existing_workspace_context(prompt: &str) -> String {
    let prompt = strip_wrapped_workspace_context(prompt);
    strip_legacy_workspace_context(&prompt)
}

fn strip_wrapped_workspace_context(prompt: &str) -> String {
    let mut current = prompt.to_string();
    while let Some(start_idx) = current.find(WORKSPACE_CONTEXT_START_MARKER) {
        let search_from = start_idx + WORKSPACE_CONTEXT_START_MARKER.len();
        let Some(end_rel_idx) = current[search_from..].find(WORKSPACE_CONTEXT_END_MARKER) else {
            break;
        };
        let end_idx = search_from + end_rel_idx + WORKSPACE_CONTEXT_END_MARKER.len();

        let before = current[..start_idx].trim_end();
        let after = current[end_idx..].trim_start();
        current = match (before.is_empty(), after.is_empty()) {
            (true, true) => String::new(),
            (true, false) => after.to_string(),
            (false, true) => before.to_string(),
            (false, false) => format!("{before}\n\n{after}"),
        };
    }
    current
}

fn strip_legacy_workspace_context(prompt: &str) -> String {
    let Some(start_idx) = prompt.find(LEGACY_WORKSPACE_CONTEXT_MARKER) else {
        return prompt.to_string();
    };

    let guidance = crate::runtime::context::workspace_prompt_guidance();
    if let Some(guidance_rel_idx) = prompt[start_idx..].find(&guidance) {
        let guidance_end_idx = start_idx + guidance_rel_idx + guidance.len();
        let mut out = String::new();
        out.push_str(prompt[..start_idx].trim_end());
        out.push_str(&prompt[guidance_end_idx..]);
        return out.trim_end().to_string();
    }

    let after_marker_idx = start_idx + LEGACY_WORKSPACE_CONTEXT_MARKER.len();
    let remainder = &prompt[after_marker_idx..];
    let mut next_section_idx = [
        remainder.find(WORKSPACE_CONTEXT_START_MARKER),
        remainder.find(SKILL_CONTEXT_START_MARKER),
        remainder.find(TOOL_GUIDE_START_MARKER),
        remainder.find(EXTERNAL_MEMORY_START_MARKER),
        remainder.find(TASK_LIST_START_MARKER),
        remainder.find(LEGACY_TODO_LIST_START_MARKER),
    ]
    .into_iter()
    .flatten()
    .map(|idx| after_marker_idx + idx)
    .min();

    if next_section_idx.is_none() {
        next_section_idx = [
            remainder.find(LEGACY_SKILL_CONTEXT_MARKERS[0]),
            remainder.find(LEGACY_SKILL_CONTEXT_MARKERS[1]),
            remainder.find(LEGACY_TOOL_GUIDE_MARKER),
            remainder.find(LEGACY_EXTERNAL_MEMORY_MARKER),
            remainder.find(LEGACY_TASK_LIST_MARKER),
        ]
        .into_iter()
        .flatten()
        .map(|idx| after_marker_idx + idx)
        .min();
    }
    let next_section_idx = next_section_idx.unwrap_or(prompt.len());

    let mut out = String::new();
    out.push_str(prompt[..start_idx].trim_end());
    out.push_str(&prompt[next_section_idx..]);
    out.trim_end().to_string()
}
