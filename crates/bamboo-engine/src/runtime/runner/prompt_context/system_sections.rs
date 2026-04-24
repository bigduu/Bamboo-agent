const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const SKILL_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_END -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const TOOL_GUIDE_END_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_END -->";
const ENV_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_START -->";
const ENV_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_END -->";

pub(super) fn merge_system_prompt_with_contexts(
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
) -> String {
    let mut merged = strip_existing_tool_guide_context(&strip_existing_skill_context(base_prompt));

    let sections: Vec<String> = [
        wrap_generated_section(
            skill_context,
            SKILL_CONTEXT_START_MARKER,
            SKILL_CONTEXT_END_MARKER,
        ),
        wrap_generated_section(
            tool_guide_context,
            TOOL_GUIDE_START_MARKER,
            TOOL_GUIDE_END_MARKER,
        ),
    ]
    .into_iter()
    .flatten()
    .collect();

    if sections.is_empty() {
        return merged;
    }

    if merged.trim().is_empty() {
        return sections.join("\n\n");
    }

    for section in sections {
        merged.push_str("\n\n");
        merged.push_str(&section);
    }

    merged
}

pub(super) fn strip_existing_skill_context(prompt: &str) -> String {
    strip_existing_prompt_block(prompt, SKILL_CONTEXT_START_MARKER, SKILL_CONTEXT_END_MARKER)
}

pub(super) fn strip_existing_tool_guide_context(prompt: &str) -> String {
    strip_existing_prompt_block(prompt, TOOL_GUIDE_START_MARKER, TOOL_GUIDE_END_MARKER)
}

pub(super) fn strip_existing_env_context(prompt: &str) -> String {
    strip_existing_prompt_block(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER)
}

fn wrap_generated_section(section: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let section = section.trim();
    if section.is_empty() {
        return None;
    }
    Some(format!("{start_marker}\n{section}\n{end_marker}"))
}

pub(super) fn strip_existing_prompt_block(
    prompt: &str,
    start_marker: &str,
    end_marker: &str,
) -> String {
    let mut current = prompt.to_string();

    loop {
        let Some(start_idx) = current.find(start_marker) else {
            break;
        };
        let search_from = start_idx + start_marker.len();
        let Some(end_rel_idx) = current[search_from..].find(end_marker) else {
            break;
        };
        let end_idx = search_from + end_rel_idx + end_marker.len();

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
