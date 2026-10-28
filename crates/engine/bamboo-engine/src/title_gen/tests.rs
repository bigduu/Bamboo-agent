//! Unit tests for title_gen helpers (no LLM/AppState dependencies).

use super::*;

#[tokio::test]
async fn routing_contract_title_uses_saved_global_fast_and_live_replacement_for_every_flag() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    for flag in [None, Some(false), Some(true)] {
        let state = RoutingContext::new(flag);
        let context: Arc<dyn AgentSessionContext> = state.clone();
        let session = RoutingContext::session();
        assert_eq!(
            try_llm_title(&context, &session, "Synthetic title request")
                .await
                .unwrap(),
            "Synthetic routing title"
        );
        assert_eq!(
            state.requests("fast-b"),
            vec![("cheap-b".into(), "title_generation".into())]
        );
        assert!(state.requests("chat-a").is_empty());
        state.config.write().await.defaults.as_mut().unwrap().fast = Some(
            bamboo_domain::ProviderModelRef::new("memory-c", "background-c"),
        );
        try_llm_title(&context, &session, "Synthetic replacement request")
            .await
            .unwrap();
        assert_eq!(
            state.requests("memory-c"),
            vec![("background-c".into(), "title_generation".into())]
        );
        assert!(state.requests("chat-a").is_empty());
    }
}

#[tokio::test]
async fn routing_contract_title_invalid_explicit_fast_never_dispatches_a_fallback() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    for flag in [None, Some(false), Some(true)] {
        let state = RoutingContext::new(flag);
        state
            .config
            .write()
            .await
            .defaults
            .as_mut()
            .unwrap()
            .fast
            .as_mut()
            .unwrap()
            .provider = "missing-account".into();
        let context: Arc<dyn AgentSessionContext> = state.clone();
        assert!(try_llm_title(
            &context,
            &RoutingContext::session(),
            "Synthetic invalid route"
        )
        .await
        .is_err());
        state.assert_no_requests();
    }
}

#[tokio::test]
async fn routing_contract_title_rejects_removed_fast_with_a_retained_registry() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    for flag in [None, Some(false), Some(true)] {
        let state = RoutingContext::new(flag);
        {
            let mut config = state.config.write().await;
            assert!(config.provider_instances.remove("fast-b").is_some());
            assert!(config.provider_instances.contains_key("chat-a"));
            assert_eq!(
                config
                    .defaults
                    .as_ref()
                    .unwrap()
                    .fast
                    .as_ref()
                    .unwrap()
                    .provider,
                "fast-b"
            );
        }
        assert!(state.registry.get("fast-b").is_some());
        assert_eq!(
            state.registry.runtime_models_for_provider("fast-b"),
            Some(vec!["cheap-b".to_string()])
        );

        let context: Arc<dyn AgentSessionContext> = state.clone();
        let result = try_llm_title(
            &context,
            &RoutingContext::session(),
            "Synthetic removed-account request",
        )
        .await;
        assert!(
            result.is_err(),
            "flag {flag:?}: removed Fast instance unexpectedly resolved: {:?}",
            state.requests("fast-b")
        );
        state.assert_no_requests();
    }
}

#[tokio::test]
async fn routing_contract_title_accepts_an_embedder_owned_registry() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    use std::collections::HashMap;

    for flag in [None, Some(false), Some(true)] {
        for explicit_admission in [false, true] {
            let state = RoutingContext::new(flag);
            {
                let mut config = state.config.write().await;
                config.provider_instances.clear();
                config.default_provider_instance = None;
            }
            let providers: HashMap<String, Arc<dyn bamboo_llm::LLMProvider>> = state
                .providers
                .iter()
                .map(|(id, provider)| {
                    (
                        id.clone(),
                        provider.clone() as Arc<dyn bamboo_llm::LLMProvider>,
                    )
                })
                .collect();
            let registry = bamboo_llm::ProviderRegistry::new(providers, "chat-a".into());
            if explicit_admission {
                registry.set_runtime_models(HashMap::from([(
                    "fast-b".to_string(),
                    vec!["cheap-b".to_string()],
                )]));
            }
            state.registry.replace_with(registry);
            assert_eq!(
                state
                    .registry
                    .runtime_models_for_provider("fast-b")
                    .is_some(),
                explicit_admission
            );

            let context: Arc<dyn AgentSessionContext> = state.clone();
            assert_eq!(
                try_llm_title(
                    &context,
                    &RoutingContext::session(),
                    "Synthetic embedded request",
                )
                .await
                .unwrap(),
                "Synthetic routing title"
            );
            assert_eq!(
                state.requests("fast-b"),
                vec![("cheap-b".into(), "title_generation".into())]
            );
            assert!(state.requests("chat-a").is_empty());
            assert!(state.requests("memory-c").is_empty());
        }
    }
}

#[tokio::test]
async fn routing_contract_title_accepts_an_enabled_synthesized_legacy_provider() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    use std::collections::HashMap;

    for flag in [None, Some(false), Some(true)] {
        let state = RoutingContext::new(flag);
        {
            let mut config = state.config.write().await;
            config.provider_instances.clear();
            config.default_provider_instance = None;
            config.provider = "openai".to_string();
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                model: Some("legacy-chat".into()),
                fast_model: Some("legacy-fast".into()),
                ..Default::default()
            });
            let defaults = config.defaults.as_mut().unwrap();
            defaults.chat = bamboo_domain::ProviderModelRef::new("openai", "legacy-chat");
            defaults.fast = Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "legacy-fast",
            ));
            assert!(bamboo_config::synthesize_legacy_instances(&config)
                .iter()
                .any(|(id, instance)| id == "openai" && instance.enabled));
        }

        state
            .registry
            .insert("openai".into(), state.providers["fast-b"].clone());
        state.registry.set_runtime_models(HashMap::from([(
            "openai".to_string(),
            vec!["legacy-fast".to_string()],
        )]));
        let context: Arc<dyn AgentSessionContext> = state.clone();
        assert_eq!(
            try_llm_title(
                &context,
                &RoutingContext::session(),
                "Synthetic legacy request",
            )
            .await
            .unwrap(),
            "Synthetic routing title"
        );
        assert_eq!(
            state.requests("fast-b"),
            vec![("legacy-fast".into(), "title_generation".into())]
        );
        assert!(state.requests("chat-a").is_empty());
        assert!(state.requests("memory-c").is_empty());
    }
}

// These tests use only local mock services, temporary roots, and fake keys.
async fn managed_title_mock() -> wiremock::MockServer {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"id\":\"mock\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Synthetic routing title\"},\"finish_reason\":null}]}\n\n",
                    "data: [DONE]\n\n"
                )),
        )
        .mount(&server)
        .await;
    server
}

async fn managed_title_request_count(server: &wiremock::MockServer) -> usize {
    server.received_requests().await.unwrap().len()
}

#[tokio::test]
async fn routing_contract_title_config_managed_removed_instance_never_dispatches() {
    use crate::app_context::model_routing_test_support::RoutingContext;

    let mut observations = Vec::new();
    for flag in [None, Some(false), Some(true)] {
        let chat = managed_title_mock().await;
        let fast = managed_title_mock().await;
        let memory = managed_title_mock().await;
        let state = RoutingContext::new(flag);
        let config_snapshot = {
            let mut config = state.config.write().await;
            // The client's loopback exclusion prevents use of inherited proxies.
            config.http_proxy = "https://127.0.0.1:9".into();
            config.https_proxy.clear();
            config.proxy_auth = None;
            for (id, server) in [("chat-a", &chat), ("fast-b", &fast), ("memory-c", &memory)] {
                let instance = config.provider_instances.get_mut(id).unwrap();
                instance.api_key = "fixture-only-not-a-credential".into();
                instance.base_url = Some(server.uri());
            }
            config.clone()
        };
        let root = tempfile::tempdir().unwrap();
        let managed =
            bamboo_llm::ProviderRegistry::from_config(&config_snapshot, root.path().to_path_buf())
                .await
                .unwrap();
        assert!(managed.get("fast-b").is_some());
        state.registry.replace_with(managed);
        let old_handle = state.registry.get("fast-b").unwrap();
        let context: Arc<dyn AgentSessionContext> = state.clone();
        let session = RoutingContext::session();

        // Positive control proves that the selected managed provider really
        // reaches the Fast endpoint and that its stream is consumable.
        assert_eq!(
            try_llm_title(&context, &session, "Synthetic managed title")
                .await
                .unwrap(),
            "Synthetic routing title"
        );
        let requests = fast.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["model"], "cheap-b");
        assert_eq!(managed_title_request_count(&chat).await, 0);
        assert_eq!(managed_title_request_count(&memory).await, 0);

        {
            let mut config = state.config.write().await;
            assert!(config.provider_instances.remove("fast-b").is_some());
            assert!(config.provider_instances.contains_key("chat-a"));
            assert_eq!(
                config
                    .defaults
                    .as_ref()
                    .unwrap()
                    .fast
                    .as_ref()
                    .unwrap()
                    .provider,
                "fast-b"
            );
        }
        assert!(Arc::ptr_eq(
            &old_handle,
            &state.registry.get("fast-b").unwrap()
        ));
        let rejected = try_llm_title(&context, &session, "Synthetic stale managed title")
            .await
            .is_err();
        observations.push((
            flag,
            rejected,
            managed_title_request_count(&fast).await,
            managed_title_request_count(&chat).await,
            managed_title_request_count(&memory).await,
        ));
    }
    assert_eq!(observations.len(), 3);
    assert!(
        observations
            .iter()
            .all(|(_, rejected, fast, chat, memory)| *rejected
                && *fast == 1
                && *chat == 0
                && *memory == 0),
        "removed managed Fast must not dispatch, for any flag: {observations:?}"
    );
}

#[tokio::test]
async fn routing_contract_title_config_managed_same_id_legacy_requires_new_registry() {
    use crate::app_context::model_routing_test_support::RoutingContext;

    let mut observations = Vec::new();
    for flag in [None, Some(false), Some(true)] {
        let instance_endpoint = managed_title_mock().await;
        let legacy_endpoint = managed_title_mock().await;
        let other_endpoint = managed_title_mock().await;
        let state = RoutingContext::new(flag);
        let config_snapshot = {
            let mut config = state.config.write().await;
            config.http_proxy = "https://127.0.0.1:9".into();
            config.https_proxy.clear();
            config.proxy_auth = None;
            config.provider = "openai".into();
            config.default_provider_instance = Some("openai".into());
            config.provider_instances.clear();
            config.provider_instances.insert(
                "openai".into(),
                serde_json::from_value(serde_json::json!({
                    "provider_type": "openai", "enabled": true,
                    "api_key": "fixture-instance-not-a-credential",
                    "base_url": instance_endpoint.uri(), "model": "same-model",
                    "runtime_models": ["same-model"]
                }))
                .unwrap(),
            );
            // Keep one native instance after removing "openai" so the
            // legacy alias is rebuilt in hybrid mode, without constructing
            // the unrelated default Copilot provider.
            config.provider_instances.insert(
                "other-account".into(),
                serde_json::from_value(serde_json::json!({
                    "provider_type": "openai", "enabled": true,
                    "api_key": "fixture-other-not-a-credential",
                    "base_url": other_endpoint.uri(), "model": "other-model",
                    "runtime_models": ["other-model"]
                }))
                .unwrap(),
            );
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                api_key: "fixture-legacy-not-a-credential".into(),
                base_url: Some(legacy_endpoint.uri()),
                model: Some("same-model".into()),
                fast_model: Some("same-model".into()),
                ..Default::default()
            });
            config.defaults = Some(
                serde_json::from_value(serde_json::json!({
                    "chat": {"provider":"openai", "model":"same-model"},
                    "fast": {"provider":"openai", "model":"same-model"}
                }))
                .unwrap(),
            );
            assert!(!bamboo_config::synthesize_legacy_instances(&config)
                .iter()
                .any(|(id, _)| id == "openai"));
            config.clone()
        };
        let root = tempfile::tempdir().unwrap();
        let managed =
            bamboo_llm::ProviderRegistry::from_config(&config_snapshot, root.path().to_path_buf())
                .await
                .unwrap();
        assert!(managed.get("openai").is_some());
        state.registry.replace_with(managed);
        let old_handle = state.registry.get("openai").unwrap();
        let context: Arc<dyn AgentSessionContext> = state.clone();
        let session = RoutingContext::session();

        assert_eq!(
            try_llm_title(&context, &session, "Synthetic instance title")
                .await
                .unwrap(),
            "Synthetic routing title"
        );
        assert_eq!(managed_title_request_count(&instance_endpoint).await, 1);
        assert_eq!(managed_title_request_count(&legacy_endpoint).await, 0);
        assert_eq!(managed_title_request_count(&other_endpoint).await, 0);

        let replacement_config = {
            let mut config = state.config.write().await;
            assert!(config.provider_instances.remove("openai").is_some());
            assert!(config.provider_instances.contains_key("other-account"));
            let legacy = bamboo_config::synthesize_legacy_instances(&config);
            let (_, instance) = legacy.iter().find(|(id, _)| id == "openai").unwrap();
            assert_eq!(instance.model.as_deref(), Some("same-model"));
            let legacy_url = legacy_endpoint.uri();
            assert_eq!(instance.base_url.as_deref(), Some(legacy_url.as_str()));
            config.clone()
        };
        assert!(Arc::ptr_eq(
            &old_handle,
            &state.registry.get("openai").unwrap()
        ));
        let rejected = try_llm_title(&context, &session, "Synthetic stale instance title")
            .await
            .is_err();
        let old_after_stale = managed_title_request_count(&instance_endpoint).await;
        let new_after_stale = managed_title_request_count(&legacy_endpoint).await;
        let other_after_stale = managed_title_request_count(&other_endpoint).await;

        // Once the registry is built from the new legacy source, the identical
        // ID and model must select the new endpoint, proving compatibility.
        let refreshed = bamboo_llm::ProviderRegistry::from_config(
            &replacement_config,
            root.path().to_path_buf(),
        )
        .await
        .unwrap();
        assert!(refreshed.get("openai").is_some());
        state.registry.replace_with(refreshed);
        assert_eq!(
            try_llm_title(&context, &session, "Synthetic legacy title")
                .await
                .unwrap(),
            "Synthetic routing title"
        );
        assert_eq!(
            managed_title_request_count(&instance_endpoint).await,
            old_after_stale
        );
        assert_eq!(
            managed_title_request_count(&legacy_endpoint).await,
            new_after_stale + 1
        );
        assert_eq!(
            managed_title_request_count(&other_endpoint).await,
            other_after_stale
        );
        observations.push((
            flag,
            rejected,
            old_after_stale,
            new_after_stale,
            other_after_stale,
        ));
    }
    assert_eq!(observations.len(), 3);
    assert!(
        observations
            .iter()
            .all(|(_, rejected, old, legacy, other)| *rejected
                && *old == 1
                && *legacy == 0
                && *other == 0),
        "same ID must not route through the old source, for any flag: {observations:?}"
    );
}

#[test]
fn test_heuristic_title_strips_code_fences_and_caps_length() {
    let input = "```rust\nfn main() {}\n```\nHelp me debug this Rust program please";
    let t = heuristic_title(input);
    assert!(!t.is_empty());
    assert!(!t.contains("```"));
    assert!(!t.contains("fn main"));
    assert!(t.starts_with("Help me debug this Rust program"));

    let long = "a ".repeat(200);
    let t2 = heuristic_title(&long);
    assert!(t2.chars().count() <= 60);
}

#[test]
fn test_heuristic_title_first_sentence_only() {
    let input = "Refactor the auth module. Then add tests. Finally ship it!";
    assert_eq!(heuristic_title(input), "Refactor the auth module");
}

#[test]
fn test_heuristic_title_trims_punctuation_tail() {
    let input = "Plan a trip to Tokyo,";
    let t = heuristic_title(input);
    assert_eq!(t, "Plan a trip to Tokyo");
}

#[test]
fn test_sanitize_strips_quotes_and_caps() {
    assert_eq!(sanitize("\"Hello world.\""), "Hello world");
    assert_eq!(sanitize("  'A quoted title'  "), "A quoted title");
    assert_eq!(sanitize("First line\nSecond line"), "First line");

    let long = "x".repeat(120);
    let s = sanitize(&long);
    assert!(s.chars().count() <= MAX_TITLE_LEN);
}

#[test]
fn test_sanitize_empty_input() {
    assert_eq!(sanitize(""), "");
    assert_eq!(sanitize("   \n  "), "");
}

#[test]
fn test_build_title_messages_truncates_to_2000_chars() {
    let huge: String = "z".repeat(5000);
    let msgs = build_title_messages(&huge);
    assert_eq!(msgs.len(), 2);
    // First message is system, second is user.
    let user_msg = &msgs[1];
    assert_eq!(user_msg.role, bamboo_agent_core::Role::User);
    assert_eq!(user_msg.content.chars().count(), 2000);
}

#[test]
fn test_build_title_messages_short_input_passthrough() {
    let msgs = build_title_messages("Hi there");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[1].content, "Hi there");
    assert_eq!(msgs[0].role, bamboo_agent_core::Role::System);
}

#[test]
fn first_user_text_skips_textless_turn_so_a_later_message_can_retry() {
    let mut session = Session::new("retry-title", "model");
    session.add_message(Message::user("   "));
    session.add_message(Message::assistant("Image acknowledged", None));
    session.add_message(Message::user("Use this later text for the title"));

    assert_eq!(
        first_user_text(&session).as_deref(),
        Some("Use this later text for the title")
    );
}

#[test]
fn first_user_text_stays_empty_until_a_text_turn_exists() {
    let mut session = Session::new("pending-title", "model");
    session.add_message(Message::user("\n\t"));

    assert!(first_user_text(&session).is_none());
}

#[test]
fn title_provider_fallback_uses_default_provider_instance() {
    let mut config = bamboo_config::Config::default();
    let instance = serde_json::from_value(serde_json::json!({
        "provider_type": "openai",
        "enabled": true
    }))
    .unwrap();
    config.provider = "anthropic".to_string();
    config
        .provider_instances
        .insert("work-openai".to_string(), instance);
    config.default_provider_instance = Some("work-openai".to_string());
    let session = Session::new("title-provider-instance", "model");

    assert_eq!(
        title_provider_name(&config, &session),
        "work-openai",
        "title generation must route through the instance authority"
    );
}
