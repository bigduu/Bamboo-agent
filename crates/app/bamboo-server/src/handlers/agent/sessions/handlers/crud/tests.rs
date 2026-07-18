use bamboo_config::ProviderConfigs;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::session_app::session_create::{
    build_new_session, resolve_model, resolve_reasoning_effort, CreateSessionConfig,
    CreateSessionInput,
};
use bamboo_llm::Config;

macro_rules! test_config {
    (@assign $config:ident, providers, $value:expr) => { *$config.providers_mut() = $value; };
    (@assign $config:ident, memory, $value:expr) => { *$config.memory_mut() = $value; };
    (@assign $config:ident, subagents, $value:expr) => { *$config.subagents_mut() = $value; };
    (@assign $config:ident, $field:ident, $value:expr) => { $config.$field = $value; };
    ($($field:ident: $value:expr),* $(,)?) => {{
        let mut config = Config::default();
        $(test_config!(@assign config, $field, $value);)*
        config
    }};
}

const BUILTIN_FALLBACK: &str = crate::app_state::DEFAULT_BASE_PROMPT;

fn config_from_server(config: &Config) -> CreateSessionConfig {
    CreateSessionConfig {
        default_model: config.get_model(),
        default_reasoning_effort: config.get_reasoning_effort(),
        global_default_prompt: "Global fallback".to_string(),
        builtin_fallback_prompt: BUILTIN_FALLBACK,
    }
}

#[test]
fn model_from_request_uses_provider_default_when_absent_or_blank() {
    let config = test_config! {
        provider: "copilot".to_string(),
    };
    let expected = config
        .get_model()
        .expect("provider default model should exist");

    assert_eq!(resolve_model(None, config.get_model().as_deref()), expected);
    assert_eq!(
        resolve_model(Some("   "), config.get_model().as_deref()),
        expected
    );
}

#[test]
fn model_from_request_trims_non_empty_model() {
    assert_eq!(resolve_model(Some("  gpt-5  "), None), "gpt-5");
}

#[test]
fn build_new_session_applies_title_and_system_prompt_metadata() {
    let config = Config::default();
    let input = CreateSessionInput {
        id: "session-1".to_string(),
        title: Some("  Sprint Session  ".to_string()),
        system_prompt: Some("  You are helpful  ".to_string()),
        model: Some("gpt-5".to_string()),
        model_ref: None,
        reasoning_effort: Some(ReasoningEffort::High),
        gold_config_json: None,
        workspace_path: None,
    };

    let session = build_new_session(&input, &config_from_server(&config));

    assert_eq!(session.title, "Sprint Session");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("You are helpful")
    );
    assert_eq!(session.reasoning_effort, Some(ReasoningEffort::High));
    assert!(matches!(
        session.messages.first().map(|message| &message.role),
        Some(bamboo_agent_core::Role::System)
    ));
    assert_eq!(
        session
            .messages
            .first()
            .map(|message| message.content.as_str()),
        Some("You are helpful")
    );
    let snapshot = bamboo_engine::runner::read_prompt_snapshot(&session)
        .expect("prompt snapshot should exist for explicit prompt session");
    assert_eq!(snapshot.base_system_prompt, "You are helpful");
    assert_eq!(snapshot.effective_system_prompt, "You are helpful");
}

#[test]
fn build_new_session_uses_global_default_template_when_request_prompt_is_missing() {
    let config = test_config! {
        provider: "copilot".to_string(),
        providers: ProviderConfigs::default(),
    };
    let input = CreateSessionInput {
        id: "session-1".to_string(),
        title: Some("New Session".to_string()),
        system_prompt: None,
        model: Some("gpt-5".to_string()),
        model_ref: None,
        reasoning_effort: None,
        gold_config_json: None,
        workspace_path: None,
    };

    let session = build_new_session(&input, &config_from_server(&config));
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("Global fallback")
    );
    assert!(session.messages.is_empty());
}

#[test]
fn reasoning_effort_from_request_falls_back_to_provider_default() {
    let config = test_config! {
        provider: "copilot".to_string(),
        providers: ProviderConfigs::default(),
    };

    assert_eq!(
        resolve_reasoning_effort(None, config.get_reasoning_effort()),
        None
    );
}
