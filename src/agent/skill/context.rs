use crate::agent::skill::types::SkillDefinition;

/// Build system prompt context text from available skills.
/// Only includes metadata (id, name, description, allowed tools).
/// The detailed skill content (SKILL.md body) is NOT included to save context space.
/// When a user's request matches a skill's description, load detailed instructions on demand.
pub fn build_skill_context(skills: &[SkillDefinition]) -> String {
    if skills.is_empty() {
        log::debug!("No skills available, returning empty context");
        return String::new();
    }

    log::info!(
        "Building skill metadata context from {} skill(s): [{}]",
        skills.len(),
        skills
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut context = String::from("\n\n## Skill System\n");
    context.push_str("You have access to specialized skills that provide domain expertise, workflows, and tools. ");
    context.push_str("When a user's request matches a skill's description, load the skill instructions and follow them.\n\n");
    context.push_str("### How to Use Skills\n");
    context.push_str("1. Analyze the user's request\n");
    context
        .push_str("2. Match it against the available skills below based on their descriptions\n");
    context.push_str(
        "3. If there's a match, call `load_skill` with `skill_id` to fetch full instructions\n",
    );
    context.push_str("4. If supporting files are needed, call `read_skill_resource` with `skill_id` and `resource_path`\n");
    context.push_str("5. Follow the loaded instructions to help the user\n\n");
    context.push_str("### Available Skills\n");

    for skill in skills {
        log::debug!(
            "Adding skill metadata '{}' with {} tool(s)",
            skill.id,
            skill.tool_refs.len(),
        );

        // Only metadata - minimal token usage
        context.push_str(&format!("\n**{}** (`{}`)\n", skill.name, skill.id));
        context.push_str(&format!("- Description: {}\n", skill.description));

        if !skill.tool_refs.is_empty() {
            context.push_str(&format!(
                "- Provides tools: {}\n",
                skill.tool_refs.join(", ")
            ));
        }

        if skill.compatibility.is_some() {
            context.push_str("- Compatibility details are available in the loaded skill payload\n");
        }
    }

    log::info!("Skill metadata context built: {} chars", context.len());

    context
}

#[cfg(test)]
mod tests {
    use crate::agent::skill::types::SkillDefinition;

    use super::build_skill_context;

    #[test]
    fn build_skill_context_returns_empty_for_empty_input() {
        assert!(build_skill_context(&[]).is_empty());
    }

    #[test]
    fn build_skill_context_renders_metadata_only() {
        let mut skill = SkillDefinition::new(
            "demo-skill",
            "Demo Skill",
            "A demo skill for testing",
            "This detailed prompt should NOT appear in context.", // This should NOT be in output
        )
        .with_tool_ref("read_file");
        skill.compatibility = Some("Requires Read and Write tools".to_string());

        let context = build_skill_context(&[skill]);

        // Should contain instructions for AI
        assert!(context.contains("## Skill System"));
        assert!(context.contains("How to Use Skills"));
        assert!(context.contains("Match it against the available skills"));
        assert!(context.contains("load_skill"));
        assert!(context.contains("read_skill_resource"));

        // Should contain skill metadata
        assert!(context.contains("Demo Skill"));
        assert!(context.contains("demo-skill"));
        assert!(context.contains("A demo skill for testing"));
        assert!(context.contains("Provides tools: read_file"));
        assert!(context.contains("Compatibility details are available in the loaded skill payload"));

        // Should NOT contain the detailed prompt
        assert!(!context.contains("This detailed prompt should NOT appear"));
    }
}
