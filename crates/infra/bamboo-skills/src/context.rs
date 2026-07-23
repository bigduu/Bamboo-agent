use crate::types::SkillDefinition;
use crate::{
    WorkflowCatalogDiagnostic, WorkflowCatalogEntry, WorkflowKind, WorkflowSource, WorkflowStatus,
};

/// Build system prompt context text from available skills.
/// Only includes metadata (id, name, description, allowed tools).
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
        if diagnostic.total_candidates == 0 {
            tracing::debug!("No skills available, returning empty context");
            return String::new();
        }
        // The structured diagnostic is persisted in session metadata. Do not
        // violate a tiny prompt budget merely to explain that nothing fitted.
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
    context.push_str(&workflow_catalog_suffix(diagnostic));

    tracing::info!("Skill metadata context built: {} chars", context.len());

    context
}

pub(crate) fn workflow_catalog_prefix() -> String {
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

    context.push_str("### If A Workflow Applies\n");
    context.push_str("1. Select EXACTLY ONE workflow (prefer the most specific match).\n");
    context.push_str(
        "2. For `kind: instruction`, call `load_skill` with `skill_id` before responding.\n",
    );
    context.push_str("3. For `kind: orchestration`, never call `load_skill`; use `workflow_run` with the advertised fixed revision. The server will deny automatic starts unless policy allows them and the session explicitly opted in.\n");
    context.push_str("4. Follow loaded instruction workflows precisely; inspect orchestration status through workflow_run get/list/events.\n\n");

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

    context.push_str("### Execution Behavior With Injected Context\n");
    context.push_str("1. Treat Bamboo-injected workspace and environment context as already available working context.\n");
    context.push_str("2. If injected env variables appear sufficient for a skill workflow, prefer a minimal execution or verification attempt before asking the user for more information.\n");
    context.push_str("3. When execution fails, diagnose the concrete failure first and only ask follow-up questions for information that remains genuinely missing after using the injected context and available tools.\n");
    context.push_str("4. Do NOT ask the user to re-send env var values that Bamboo has already injected by name unless the value is clearly missing, malformed, or the user must change it.\n\n");

    context.push_str("### Available Workflows\n");
    context
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
    context.push_str(&format!("- Description: {}\n", skill.description));
    context.push_str(&format!("- Kind: {:?}\n", entry.kind).to_ascii_lowercase());
    context.push_str(&format!("- Source: {:?}\n", entry.source).to_ascii_lowercase());
    context.push_str(&format!("- Revision: {}\n", entry.revision));
    context.push_str(&format!(
        "- Invocation policy: {}\n",
        entry.invocation_policy
    ));

    context
}

pub(crate) fn workflow_catalog_suffix(diagnostic: &WorkflowCatalogDiagnostic) -> String {
    let mut context = String::new();
    context.push_str("\n### Internal Verification\n");
    context.push_str(
        "Internally confirm `skill_check_completed=true` before each user-facing response.\n",
    );
    context.push_str(&format!(
        "\nCatalog budget: advertised {}/{} candidates; char_budget={}; compressed={}; shortlisted={}.\n",
        diagnostic.advertised_candidates,
        diagnostic.total_candidates,
        diagnostic.char_budget,
        diagnostic.compressed_descriptions,
        diagnostic.shortlisted,
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

        // Should contain instructions for AI
        assert!(context.contains("## Skill System"));
        assert!(context.contains("Mandatory Skill Check"));
        assert!(context.contains("Select EXACTLY ONE workflow"));
        assert!(context.contains("load_skill"));
        assert!(context.contains("read_skill_resource"));
        assert!(context.contains("skill_check_completed=true"));
        assert!(context.contains("Execution Behavior With Injected Context"));
        assert!(context.contains("prefer a minimal execution or verification attempt"));
        assert!(context.contains("Do NOT ask the user to re-send env var values"));

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
                "Description",
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
}
