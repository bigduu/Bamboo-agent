use crate::agent::core::{Role, Session};

pub(super) fn upsert_system_prompt_message(session: &mut Session, system_prompt: String) {
    session
        .messages
        .retain(|message| !matches!(message.role, Role::System));
    session
        .messages
        .insert(0, crate::agent::core::Message::system(system_prompt));
}

pub(super) fn build_enhanced_system_prompt(
    base_prompt: &str,
    enhance_prompt: Option<&str>,
    workspace_path: Option<&str>,
) -> String {
    let mut merged_prompt = base_prompt.to_string();

    if let Some(enhancement) = enhance_prompt
        .map(str::trim)
        .filter(|enhancement| !enhancement.is_empty())
    {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(enhancement);
    }

    if let Some(workspace_path) = workspace_path
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty())
    {
        if let Some(workspace_context) =
            crate::server::app_state::build_workspace_prompt_context(workspace_path)
        {
            merged_prompt.push_str("\n\n");
            merged_prompt.push_str(&workspace_context);
        }
    }

    merged_prompt
}
