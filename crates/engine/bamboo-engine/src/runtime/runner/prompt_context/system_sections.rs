const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const SKILL_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_END -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const TOOL_GUIDE_END_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_END -->";
const ENV_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_START -->";
const ENV_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_END -->";
const CORE_DIRECTIVES_START_MARKER: &str = "<!-- BAMBOO_CORE_DIRECTIVES_START -->";
const CORE_DIRECTIVES_END_MARKER: &str = "<!-- BAMBOO_CORE_DIRECTIVES_END -->";

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

pub(super) fn strip_existing_core_directives(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        CORE_DIRECTIVES_START_MARKER,
        CORE_DIRECTIVES_END_MARKER,
    )
}

/// Append framework-invariant agent directives to a (user-swappable) base prompt.
///
/// The directives are wrapped in markers and any pre-existing directive block is
/// stripped first, so this is idempotent across rounds and the directives survive
/// even when the user fully replaces the base prompt. Returns the (stripped) base
/// unchanged when `directives` is empty.
pub(super) fn append_core_agent_directives(base_prompt: &str, directives: &str) -> String {
    let stripped = strip_existing_core_directives(base_prompt);
    let directives = directives.trim();
    if directives.is_empty() {
        return stripped;
    }
    let wrapped =
        format!("{CORE_DIRECTIVES_START_MARKER}\n{directives}\n{CORE_DIRECTIVES_END_MARKER}");
    let stripped = stripped.trim();
    if stripped.is_empty() {
        wrapped
    } else {
        format!("{stripped}\n\n{wrapped}")
    }
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

    while let Some(start_idx) = current.find(start_marker) {
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

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_DIRECTIVES: &str = "Investigate before you conclude.";

    #[test]
    fn appends_directives_to_non_empty_base() {
        let out = append_core_agent_directives("Custom base.", SAMPLE_DIRECTIVES);
        assert!(out.starts_with("Custom base."));
        assert!(out.contains(CORE_DIRECTIVES_START_MARKER));
        assert!(out.contains(SAMPLE_DIRECTIVES));
        assert!(out.contains(CORE_DIRECTIVES_END_MARKER));
    }

    #[test]
    fn append_is_idempotent_across_rounds() {
        let once = append_core_agent_directives("Base.", SAMPLE_DIRECTIVES);
        let twice = append_core_agent_directives(&once, SAMPLE_DIRECTIVES);
        assert_eq!(once, twice);
        assert_eq!(twice.matches(CORE_DIRECTIVES_START_MARKER).count(), 1);
    }

    #[test]
    fn directives_survive_a_fully_overridden_base() {
        // A user-provided system-prompt.md that drops every builtin instruction
        // must still carry the framework directives.
        let user_override = "You are Grumpy. Answer in one word.";
        let out = append_core_agent_directives(user_override, SAMPLE_DIRECTIVES);
        assert!(out.contains(SAMPLE_DIRECTIVES));
    }

    #[test]
    fn empty_directives_leaves_base_unchanged() {
        assert_eq!(append_core_agent_directives("Base.", "   "), "Base.");
    }

    #[test]
    fn real_core_directives_cover_investigation_and_adversarial_verification() {
        let directives = crate::runtime::context::CORE_AGENT_DIRECTIVES;
        let lower = directives.to_ascii_lowercase();
        assert!(lower.contains("investigate"));
        assert!(lower.contains("verify"));
        assert!(lower.contains("adversarial"));
        // And they actually ride on top of an overridden base.
        let assembled = append_core_agent_directives("Custom user base.", directives);
        assert!(assembled.contains("Custom user base."));
        assert!(assembled.contains(CORE_DIRECTIVES_START_MARKER));
    }
}
