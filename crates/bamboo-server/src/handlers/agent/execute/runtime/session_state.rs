use bamboo_application_agent::Session;

pub(crate) fn selected_skill_ids_for_session(session: &Session) -> Option<Vec<String>> {
    session
        .metadata
        .get("selected_skill_ids")
        .and_then(|raw| bamboo_application_skills::selection::parse_selected_skill_ids_metadata(raw))
}

pub(crate) fn selected_skill_mode_for_session(session: &Session) -> Option<String> {
    let value = session
        .metadata
        .get("skill_mode")
        .or_else(|| session.metadata.get("mode"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
