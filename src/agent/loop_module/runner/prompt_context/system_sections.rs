const SKILL_CONTEXT_MARKERS: [&str; 2] = ["\n\n## Skill System\n", "\n\n## Available Skills\n"];
const TOOL_GUIDE_MARKER: &str = "## Tool Usage Guidelines\n";

pub(super) fn merge_system_prompt_with_contexts(
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
) -> String {
    let mut merged = strip_existing_tool_guide_context(&strip_existing_skill_context(base_prompt));

    let sections: Vec<&str> = [skill_context, tool_guide_context]
        .into_iter()
        .map(str::trim)
        .filter(|section| !section.is_empty())
        .collect();

    if sections.is_empty() {
        return merged;
    }

    if merged.trim().is_empty() {
        return sections.join("\n\n");
    }

    for section in sections {
        merged.push_str("\n\n");
        merged.push_str(section);
    }

    merged
}

pub(super) fn strip_existing_skill_context(prompt: &str) -> String {
    SKILL_CONTEXT_MARKERS
        .iter()
        .fold(prompt.to_string(), |acc, marker| {
            strip_existing_prompt_section(&acc, marker)
        })
}

pub(super) fn strip_existing_tool_guide_context(prompt: &str) -> String {
    strip_existing_prompt_section(prompt, TOOL_GUIDE_MARKER)
}

pub(super) fn strip_existing_prompt_section(prompt: &str, marker: &str) -> String {
    if let Some(index) = prompt.find(marker) {
        prompt[..index].trim_end().to_string()
    } else {
        prompt.to_string()
    }
}
