use crate::types::SkillDefinition;
use crate::{
    WorkflowCatalogDiagnostic, WorkflowCatalogEntry, WorkflowKind, WorkflowSource, WorkflowStatus,
};

/// Build system prompt context text from available skills.
/// Only includes bounded metadata (id, name, summary, source, revision, policy).
/// The detailed skill content (SKILL.md body) is NOT included to save context space.
/// When a user's request matches a skill's description, load detailed instructions on demand.
pub fn build_skill_context(skills: &[SkillDefinition]) -> String {
    let entries = skills
        .iter()
        .map(|skill| WorkflowCatalogEntry {
            id: skill.id.clone(),
            name: skill.name.clone(),
            description: skill.description.clone(),
            kind: WorkflowKind::Instruction,
            source: WorkflowSource::User,
            revision: 0,
            content_digest: String::new(),
            version: "1".to_string(),
            invocation_policy: serde_json::json!({"explicit": true, "automatic": true}),
            argument_schema: serde_json::json!({"type":"object"}),
            status: WorkflowStatus::Valid,
            legacy: false,
            migration_status: None,
            last_error: None,
            winner: true,
            shadowed_candidates: Vec::new(),
        })
        .collect::<Vec<_>>();
    let chars = crate::DEFAULT_WORKFLOW_CATALOG_MAX_CHARS;
    build_workflow_catalog_context(
        skills,
        &entries,
        &WorkflowCatalogDiagnostic {
            total_candidates: skills.len(),
            advertised_candidates: skills.len(),
            initial_chars: 0,
            final_chars: 0,
            char_budget: chars,
            token_budget: chars / 4,
            compressed_descriptions: false,
            shortlisted: false,
            omitted_ids: Vec::new(),
            automatic_outcome: None,
        },
    )
}

/// Render the metadata-only, policy-aware workflow catalog from one immutable
/// publication. Full instructions/resources are intentionally absent.
pub fn build_workflow_catalog_context(
    skills: &[SkillDefinition],
    entries: &[WorkflowCatalogEntry],
    diagnostic: &WorkflowCatalogDiagnostic,
) -> String {
    if skills.is_empty() {
        if diagnostic.automatic_outcome.is_some() {
            let guidance = "\n\n## Skill System\nDiscover a Workflow when one could help.\n";
            if guidance.chars().count() <= diagnostic.char_budget {
                return guidance.to_owned();
            }
        }
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

    let mut context = workflow_catalog_prefix();
    for skill in skills {
        let Some(entry) = entries.iter().find(|entry| entry.id == skill.id) else {
            continue;
        };
        context.push_str(&render_workflow_catalog_entry(skill, entry));
    }

    tracing::info!("Skill metadata context built: {} chars", context.len());

    context
}

pub(crate) fn workflow_catalog_prefix() -> String {
    "\n\n## Skill System\n\n\
Choose the most specific advertised Workflow when it clearly applies. Proceed directly otherwise.\n\
Read instruction Workflows with `skills_list` and `skills_read` when available, or with `load_skill` on the legacy catalog. For orchestration, use `workflow_run` with the advertised revision and follow its invocation policy.\n\
Follow the loaded instructions. Read auxiliary files only when needed. Continue with the returned `next_cursor` or `next_offset` for more.\n\n\
### Available Workflows\n"
        .to_string()
}

pub(crate) fn render_workflow_catalog_entry(
    skill: &SkillDefinition,
    entry: &WorkflowCatalogEntry,
) -> String {
    let mut context = String::new();
    tracing::debug!("Adding workflow catalog metadata '{}'", skill.id);

    // Only metadata - minimal token usage
    context.push_str(&format!("\n**{}** (`{}`)\n", skill.name, skill.id));
    context.push_str(&format!("- skill_id: `{}`\n", skill.id));
    context.push_str(&format!(
        "- Summary: {}\n",
        crate::capability_discovery::bounded_summary(&skill.description)
    ));
    context.push_str(&format!("- Kind: {:?}\n", entry.kind).to_ascii_lowercase());
    context.push_str(&format!("- Source: {:?}\n", entry.source).to_ascii_lowercase());
    context.push_str(&format!("- Revision: {}\n", entry.revision));
    let explicit = entry.invocation_policy["explicit"].as_bool() == Some(true);
    let automatic = entry.invocation_policy["automatic"].as_bool() == Some(true);
    context.push_str(&format!(
        "- Invocation policy: explicit={explicit}, automatic={automatic}\n"
    ));

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
        let mut skill = SkillDefinition::new(
            "demo-skill",
            "Demo Skill",
            "A demo skill for testing",
            "This detailed prompt should NOT appear in context.", // This should NOT be in output
        )
        .with_tool_ref("read_file");
        skill.compatibility = Some("Requires Read and Write tools".to_string());

        let context = build_skill_context(&[skill]);

        assert!(context.contains("## Skill System"));
        assert!(context.contains("when it clearly applies"));
        assert!(context.contains("with `skills_list` and `skills_read` when available"));
        assert!(context.contains("with `load_skill` on the legacy catalog"));
        assert!(context.contains("workflow_run"));
        assert!(!context.contains("skill_check_completed"));
        assert!(!context.contains("Catalog budget:"));

        // Should contain skill metadata
        assert!(context.contains("Demo Skill"));
        assert!(context.contains("demo-skill"));
        assert!(context.contains("skill_id: `demo-skill`"));
        assert!(context.contains("A demo skill for testing"));
        // The initial catalog has a strict metadata allowlist. Tool details,
        // compatibility, body, references and scripts are load-time only.
        let entry = context
            .split("**Demo Skill**")
            .nth(1)
            .expect("catalog entry rendered");
        let rendered_labels = entry
            .lines()
            .filter_map(|line| line.strip_prefix("- "))
            .filter_map(|line| line.split(':').next())
            .collect::<Vec<_>>();
        assert_eq!(
            rendered_labels,
            vec![
                "skill_id",
                "Summary",
                "kind",
                "source",
                "Revision",
                "Invocation policy"
            ]
        );
        assert!(!context.contains("read_file"));
        assert!(!context.contains("Requires Read and Write tools"));
        assert!(!context.contains("This detailed prompt should NOT appear"));
        assert!(!context.contains("references/"));
        assert!(!context.contains("scripts/"));
    }

    #[test]
    fn build_skill_context_bounds_description_without_mutating_definition() {
        let marker = "FULL-DESCRIPTION-TAIL";
        let skill = SkillDefinition::new(
            "long-skill",
            "Long Skill",
            format!("{} {marker}", "metadata ".repeat(40)),
            "instructions",
        );

        let context = build_skill_context(std::slice::from_ref(&skill));

        assert!(context.contains("- Summary:"));
        assert!(!context.contains(marker));
        assert!(skill.description.contains(marker));
    }
}
