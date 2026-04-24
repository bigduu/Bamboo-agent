use bamboo_domain::{ProviderModelRef, Session};

pub(crate) fn normalize_model_name(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty() && *value != "unknown")
        .map(ToString::to_string)
}

pub(crate) fn normalize_provider_name(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub(crate) fn normalize_model_ref(raw: Option<&ProviderModelRef>) -> Option<ProviderModelRef> {
    let raw = raw?;
    Some(ProviderModelRef::new(
        normalize_provider_name(Some(raw.provider.as_str()))?,
        normalize_model_name(Some(raw.model.as_str()))?,
    ))
}

pub(crate) fn derive_model_ref(
    raw_model_ref: Option<&ProviderModelRef>,
    raw_provider: Option<&str>,
    raw_model: Option<&str>,
) -> Option<ProviderModelRef> {
    normalize_model_ref(raw_model_ref).or_else(|| {
        Some(ProviderModelRef::new(
            normalize_provider_name(raw_provider)?,
            normalize_model_name(raw_model)?,
        ))
    })
}

pub(crate) fn session_effective_model_ref(session: &Session) -> Option<ProviderModelRef> {
    derive_model_ref(
        session.model_ref.as_ref(),
        session.metadata.get("provider_name").map(String::as_str),
        Some(session.model.as_str()),
    )
}

pub(crate) fn persist_model_ref(session: &mut Session, model_ref: &ProviderModelRef) {
    session.model = model_ref.model.clone();
    session.model_ref = Some(model_ref.clone());
    session
        .metadata
        .insert("provider_name".to_string(), model_ref.provider.clone());
}

pub(crate) fn persist_legacy_model_provider(
    session: &mut Session,
    raw_model: Option<&str>,
    raw_provider: Option<&str>,
) {
    let normalized_model = normalize_model_name(raw_model);
    let normalized_provider = normalize_provider_name(raw_provider);

    if normalized_model.is_some() || normalized_provider.is_some() {
        session.model_ref = None;
    }

    if let Some(model) = normalized_model {
        session.model = model;
    }
    if let Some(provider_name) = normalized_provider {
        session
            .metadata
            .insert("provider_name".to_string(), provider_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_model_ref_prefers_structured_ref() {
        let model_ref = ProviderModelRef::new("openai", "gpt-4o");
        let derived = derive_model_ref(
            Some(&model_ref),
            Some("anthropic"),
            Some("claude-3-7-sonnet"),
        )
        .expect("structured ref should win");
        assert_eq!(derived, model_ref);
    }

    #[test]
    fn derive_model_ref_falls_back_to_provider_and_model() {
        let derived = derive_model_ref(None, Some(" anthropic "), Some(" claude-3-7-sonnet "))
            .expect("provider/model pair should derive a ref");
        assert_eq!(derived.provider, "anthropic");
        assert_eq!(derived.model, "claude-3-7-sonnet");
    }

    #[test]
    fn session_effective_model_ref_uses_metadata_provider_name() {
        let mut session = Session::new("session-1", "gpt-4o");
        session
            .metadata
            .insert("provider_name".to_string(), "openai".to_string());

        let effective = session_effective_model_ref(&session).expect("metadata should derive ref");
        assert_eq!(effective.provider, "openai");
        assert_eq!(effective.model, "gpt-4o");
    }

    #[test]
    fn persist_model_ref_updates_model_and_metadata() {
        let mut session = Session::new("session-1", "unknown");
        let model_ref = ProviderModelRef::new("gemini", "gemini-2.5-pro");

        persist_model_ref(&mut session, &model_ref);

        assert_eq!(session.model, "gemini-2.5-pro");
        assert_eq!(session.model_ref, Some(model_ref));
        assert_eq!(
            session.metadata.get("provider_name").map(String::as_str),
            Some("gemini")
        );
    }
}
