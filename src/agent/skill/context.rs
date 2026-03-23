use crate::agent::skill::types::SkillDefinition;

/// Build system prompt context text from available skills.
/// Only includes metadata (id, name, description, allowed tools).
/// The detailed skill content (SKILL.md body) is NOT included to save context space.
/// When a user's request matches a skill's description, load detailed instructions on demand.
pub fn build_skill_context(skills: &[SkillDefinition]) -> String {
    if skills.is_empty() {
        tracing::debug!("No skills available, returning empty context");
        return String::new();
    }

    tracing::info!(
        "Building skill metadata context from {} skill(s): [{}]",
        skills.len(),
        skills
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut context = String::from("\n\n## Skill System\n");
    context.push_str(
        "Before producing any user-facing response, you MUST perform a skill applicability check.\n\n",
    );

    context.push_str("### Mandatory Skill Check\n");
    context.push_str(
        "1. Evaluate the user's request against ALL available skill descriptions below.\n",
    );
    context.push_str("2. Decide whether at least one skill clearly and unambiguously applies.\n");
    context.push_str("3. Do NOT skip this check.\n\n");

    context.push_str("### If A Skill Applies\n");
    context.push_str("1. Select EXACTLY ONE skill (prefer the most specific match).\n");
    context.push_str(
        "2. Call `load_skill` with the selected `skill_id` BEFORE producing your response.\n",
    );
    context.push_str("3. Follow the loaded SKILL.md instructions precisely.\n");
    context.push_str("4. Do NOT respond outside the selected skill's defined workflow unless the instructions explicitly allow it.\n\n");

    context.push_str("### If No Skill Applies\n");
    context.push_str("1. Proceed normally without loading any skill.\n");
    context.push_str(
        "2. Do NOT call `load_skill` or `read_skill_resource` when no skill applies.\n\n",
    );

    context.push_str("### Resource Loading Rules\n");
    context.push_str("1. Do NOT preload all skills.\n");
    context.push_str("2. Call `load_skill` only after selecting one skill.\n");
    context.push_str("3. Use `read_skill_resource` only for auxiliary files after `load_skill`.\n");
    context.push_str(
        "4. When a resource response has `has_more=true`, continue with `next_offset` until you have enough context.\n\n",
    );

    context.push_str("### Available Skills\n");

    for skill in skills {
        tracing::debug!(
            "Adding skill metadata '{}' with {} tool(s)",
            skill.id,
            skill.tool_refs.len(),
        );

        // Only metadata - minimal token usage
        context.push_str(&format!("\n**{}** (`{}`)\n", skill.name, skill.id));
        context.push_str(&format!("- skill_id: `{}`\n", skill.id));
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

    context.push_str("\n### Internal Verification\n");
    context.push_str(
        "Internally confirm `skill_check_completed=true` before each user-facing response.\n",
    );

    tracing::info!("Skill metadata context built: {} chars", context.len());

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
        assert!(context.contains("Mandatory Skill Check"));
        assert!(context.contains("Select EXACTLY ONE skill"));
        assert!(context.contains("load_skill"));
        assert!(context.contains("read_skill_resource"));
        assert!(context.contains("skill_check_completed=true"));

        // Should contain skill metadata
        assert!(context.contains("Demo Skill"));
        assert!(context.contains("demo-skill"));
        assert!(context.contains("skill_id: `demo-skill`"));
        assert!(context.contains("A demo skill for testing"));
        assert!(context.contains("Provides tools: read_file"));
        assert!(context.contains("Compatibility details are available in the loaded skill payload"));

        // Should NOT contain the detailed prompt
        assert!(!context.contains("This detailed prompt should NOT appear"));
    }
}
