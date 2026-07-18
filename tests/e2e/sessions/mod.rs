use bamboo_config::OpenAIConfig;
use bamboo_domain::reasoning::ReasoningEffort;

mod crud;
mod patch_message_safety;

fn openai_config_with(model: &str, reasoning_effort: Option<ReasoningEffort>) -> OpenAIConfig {
    OpenAIConfig {
        api_key: "sk-test".to_string(),
        api_key_from_env: false,
        api_key_encrypted: None,
        base_url: None,
        model: Some(model.to_string()),
        fast_model: Some("gpt-fast-global".to_string()),
        vision_model: Some("gpt-vision-global".to_string()),
        reasoning_effort,
        responses_only_models: vec![],
        request_overrides: None,
        extra: Default::default(),
    }
}

async fn sessions_test_app() -> actix_web::web::Data<bamboo_agent::server::AppState> {
    crate::e2e::common::create_test_app().await
}

async fn configure_openai_defaults(
    state: &actix_web::web::Data<bamboo_agent::server::AppState>,
    model: &str,
    reasoning_effort: Option<ReasoningEffort>,
) {
    let mut config = state.config.write().await;
    config.provider = "openai".to_string();
    config.providers_mut().openai = Some(openai_config_with(model, reasoning_effort));
}
