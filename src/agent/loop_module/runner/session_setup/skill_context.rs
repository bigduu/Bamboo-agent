use crate::agent::loop_module::config::AgentLoopConfig;

pub(super) async fn load_skill_context(config: &AgentLoopConfig, session_id: &str) -> String {
    if let Some(skill_manager) = config.skill_manager.as_ref() {
        let context = skill_manager
            .build_skill_context_for_selection_with_mode(
                config.selected_skill_ids.as_deref(),
                config.selected_skill_mode.as_deref(),
            )
            .await;
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
        context
    } else {
        tracing::info!("[{}] No skill manager configured", session_id);
        String::new()
    }
}
