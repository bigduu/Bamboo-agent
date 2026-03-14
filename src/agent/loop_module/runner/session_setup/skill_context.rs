use crate::agent::loop_module::config::AgentLoopConfig;

pub(super) async fn load_skill_context(
    config: &AgentLoopConfig,
    session_id: &str,
    skill_session_id: &str,
) -> String {
    if let Some(skill_manager) = config.skill_manager.as_ref() {
        let context = skill_manager
            .build_skill_context(Some(skill_session_id))
            .await;
        if !context.is_empty() {
            log::info!(
                "[{}] Skill context loaded, length: {} chars",
                session_id,
                context.len()
            );
            log::debug!("[{}] Skill context content:\n{}", session_id, context);
        } else {
            log::info!("[{}] No skill context loaded (empty)", session_id);
        }
        context
    } else {
        log::info!("[{}] No skill manager configured", session_id);
        String::new()
    }
}
