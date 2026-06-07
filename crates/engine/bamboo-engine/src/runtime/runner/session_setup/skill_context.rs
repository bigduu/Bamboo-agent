use crate::runtime::config::AgentLoopConfig;

#[derive(Debug, Clone, Default)]
pub(super) struct SkillContextLoadResult {
    pub(super) context: String,
    pub(super) selected_skill_ids: Vec<String>,
    pub(super) selection_source: Option<String>,
    pub(super) selected_skill_mode: Option<String>,
    pub(super) request_hint_present: bool,
}

pub(super) async fn load_skill_context(
    config: &AgentLoopConfig,
    session_id: &str,
    request_hint: &str,
) -> SkillContextLoadResult {
    if let Some(skill_manager) = config.skill_manager.as_ref() {
        let selected_skills: Vec<bamboo_skills::SkillDefinition> = skill_manager
            .resolve_skills_for_request_with_mode(
                &config.disabled_skill_ids,
                config.selected_skill_ids.as_deref(),
                config.selected_skill_mode.as_deref(),
                Some(request_hint),
            )
            .await;
        let selected_ids = selected_skills
            .iter()
            .map(|skill| skill.id.clone())
            .collect::<Vec<_>>();
        tracing::info!(
            "[{}] Skill selection trace: source={}, selected_count={}, selected_ids={:?}, skill_mode={}, request_hint_present={}",
            session_id,
            if config.selected_skill_ids.is_some() { "explicit" } else { "auto" },
            selected_ids.len(),
            selected_ids,
            config.selected_skill_mode.as_deref().unwrap_or("default"),
            !request_hint.trim().is_empty(),
        );

        let selection_source = if config.selected_skill_ids.is_some() {
            Some("explicit".to_string())
        } else {
            Some("auto".to_string())
        };

        let context = bamboo_skills::context::build_skill_context(&selected_skills);
        if !context.is_empty() {
            tracing::info!(
                "[{}] Skill context loaded, length: {} chars",
                session_id,
                context.len()
            );
            tracing::debug!("[{}] Skill context content:\n{}", session_id, context);
        } else {
            tracing::info!("[{}] No skill context loaded (empty)", session_id);
        }
        SkillContextLoadResult {
            context,
            selected_skill_ids: selected_ids,
            selection_source,
            selected_skill_mode: config.selected_skill_mode.clone(),
            request_hint_present: !request_hint.trim().is_empty(),
        }
    } else {
        tracing::info!("[{}] No skill manager configured", session_id);
        SkillContextLoadResult::default()
    }
}
