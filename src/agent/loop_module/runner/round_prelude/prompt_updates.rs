use crate::agent::core::Session;

use super::super::prompt_context::{
    inject_external_memory_into_system_message, inject_task_list_into_system_message,
};

pub(super) async fn refresh_round_prompt_context(session: &mut Session) {
    // Load/refresh persistent memory note for this round.
    inject_external_memory_into_system_message(session).await;

    // Inject task list into system message at the start of each round.
    inject_task_list_into_system_message(session);
}
