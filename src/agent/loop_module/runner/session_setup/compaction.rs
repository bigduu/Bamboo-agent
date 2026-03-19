use crate::agent::core::Session;
use crate::agent::loop_module::config::AgentLoopConfig;

pub(super) async fn compact_oversized_tool_messages(
    session: &mut Session,
    config: &AgentLoopConfig,
    session_id: &str,
) {
    // Compact oversized historical tool outputs so they don't keep blowing up budget
    // calculations or context payloads in subsequent rounds.
    let compacted_messages = session.compact_oversized_tool_messages();
    if compacted_messages > 0 {
        tracing::warn!(
            "[{}] Compacted {} oversized tool message(s) to protect context budget",
            session_id,
            compacted_messages
        );
        if let Some(ref storage) = config.storage {
            if let Err(error) = storage.save_session(session).await {
                tracing::warn!(
                    "[{}] Failed to persist compacted tool messages: {}",
                    session_id,
                    error
                );
            }
        }
    }
}
