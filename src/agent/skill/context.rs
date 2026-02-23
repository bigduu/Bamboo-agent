use crate::types::SkillDefinition;

/// Build system prompt context text from available skills.
/// Only includes metadata (id, name, description, category, tags).
/// The detailed skill content (SKILL.md body) is NOT included to save context space.
/// When a user's request matches a skill's description, read the full skill file for detailed instructions.
pub fn build_skill_context(skills: &[SkillDefinition]) -> String {
    if skills.is_empty() {
        log::debug!("No skills available, returning empty context");
        return String::new();
    }

    log::info!(
        "Building skill metadata context from {} skill(s): [{}]",
        skills.len(),
        skills.iter().map(|s| s.id.as_str()).collect::<Vec<_>>().join(", ")
    );

    let mut context = String::from("\n\n## Skill System\n");
    context.push_str("You have access to specialized skills that provide domain expertise, workflows, and tools. ");
    context.push_str("When a user's request matches a skill's description, read the skill file to get detailed instructions and follow them.\n\n");
    context.push_str("### How to Use Skills\n");
    context.push_str("1. Analyze the user's request\n");
    context.push_str("2. Match it against the available skills below based on their descriptions\n");
    context.push_str("3. If there's a match, read the skill file: `read_file({\"path\": \"~/.bamboo/skills/<skill_id>/SKILL.md\"})`\n");
    context.push_str("4. Follow the instructions in the skill file to help the user\n\n");
    context.push_str("### Available Skills\n");

    for skill in skills {
        log::debug!(
            "Adding skill metadata '{}' with {} tool(s), {} workflow(s)",
            skill.id,
            skill.tool_refs.len(),
            skill.workflow_refs.len()
        );

        // Only metadata - minimal token usage
        context.push_str(&format!("\n**{}** (`{}`)\n", skill.name, skill.id));
        context.push_str(&format!("- Description: {}\n", skill.description));
        context.push_str(&format!("- Category: {}\n", skill.category));

        if !skill.tags.is_empty() {
            context.push_str(&format!("- Tags: {}\n", skill.tags.join(", ")));
        }

        if !skill.tool_refs.is_empty() {
            context.push_str(&format!("- Provides tools: {}\n", skill.tool_refs.join(", ")));
        }

        // Tell AI where to find the full skill content
        context.push_str(&format!("- Skill file: `~/.bamboo/skills/{}/SKILL.md`\n", skill.id));
    }

    log::info!("Skill metadata context built: {} chars", context.len());
    log::debug!("Skill context content:\n{}", context);

    context
}

#[cfg(test)]
mod tests {
    use crate::types::SkillDefinition;

    use super::build_skill_context;

    #[test]
    fn build_skill_context_returns_empty_for_empty_input() {
        assert!(build_skill_context(&[]).is_empty());
    }

    #[test]
    fn build_skill_context_renders_metadata_only() {
        let skill = SkillDefinition::new(
            "demo-skill",
            "Demo Skill",
            "A demo skill for testing",
            "demo",
            "This detailed prompt should NOT appear in context.", // This should NOT be in output
        )
        .with_tool_ref("read_file")
        .with_tag("test")
        .with_workflow_ref("demo-workflow");

        let context = build_skill_context(&[skill]);

        // Should contain instructions for AI
        assert!(context.contains("## Skill System"));
        assert!(context.contains("How to Use Skills"));
        assert!(context.contains("Match it against the available skills"));
        assert!(context.contains("read_file"));

        // Should contain skill metadata
        assert!(context.contains("Demo Skill"));
        assert!(context.contains("demo-skill"));
        assert!(context.contains("A demo skill for testing"));
        assert!(context.contains("Category: demo"));
        assert!(context.contains("Tags: test"));
        assert!(context.contains("Provides tools: read_file"));
        assert!(context.contains("Skill file: `~/.bamboo/skills/demo-skill/SKILL.md`"));

        // Should NOT contain the detailed prompt
        assert!(!context.contains("This detailed prompt should NOT appear"));
    }
}
