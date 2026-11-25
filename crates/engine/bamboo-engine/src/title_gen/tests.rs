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
            try_llm_title(
                &context,
                &session,
                build_title_messages("Synthetic title request")
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
        state.config.write().await.defaults.as_mut().unwrap().fast = Some(
            bamboo_domain::ProviderModelRef::new("memory-c", "background-c"),
        );
        try_llm_title(
            &context,
            &session,
            build_title_messages("Synthetic replacement request"),
        )
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
            build_title_messages("Synthetic invalid route")
        )
        .await
        .is_err());
        state.assert_no_requests();
    }
}

#[tokio::test]
async fn routing_contract_title_embedder_owned_removed_fast_keeps_sdk_route() {
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
            build_title_messages("Synthetic removed-account request"),
        )
        .await;
        // This provider map belongs to the embedding application, not Config.
        // Managed removal is separately tested with ProviderRegistry::from_config.
        assert_eq!(result.unwrap(), "Synthetic routing title", "flag {flag:?}");
        assert_eq!(
            state.requests("fast-b"),
            vec![("cheap-b".into(), "title_generation".into())]
        );
        assert!(state.requests("chat-a").is_empty());
        assert!(state.requests("memory-c").is_empty());
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
                    build_title_messages("Synthetic embedded request"),
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
                build_title_messages("Synthetic legacy request"),
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
            try_llm_title(
                &context,
                &session,
                build_title_messages("Synthetic managed title")
            )
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
        let rejected = try_llm_title(
            &context,
            &session,
            build_title_messages("Synthetic stale managed title"),
        )
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
            try_llm_title(
                &context,
                &session,
                build_title_messages("Synthetic instance title")
            )
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
        let rejected = try_llm_title(
            &context,
            &session,
            build_title_messages("Synthetic stale instance title"),
        )
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
            try_llm_title(
                &context,
                &session,
                build_title_messages("Synthetic legacy title")
            )
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
fn test_heuristic_title_caps_without_splitting_a_word() {
    // 60 characters lands inside "detai|ls"; the title must not keep the fragment.
    let input =
        "Wrangle the deployment pipeline for every microservice before the release window closes";
    let title = heuristic_title(input);
    assert!(title.chars().count() <= 60, "{title:?}");
    assert!(
        input.starts_with(&title),
        "title must be a prefix of the input: {title:?}"
    );
    // The cut falls on a word boundary, so the title ends a whole word.
    let next = input[title.len()..].chars().next();
    assert!(
        next.is_none_or(|c| c.is_whitespace()),
        "title ends mid-word: {title:?} then {next:?}"
    );
}

#[test]
fn test_heuristic_title_keeps_a_word_that_ends_exactly_at_the_limit() {
    // This sentence's 60th character is the space after "logs", so the whole word
    // fits inside the limit and must not be dropped by a second retreat.
    let input = "Investigate failed authentication request in production logs before deploying";
    assert_eq!(
        &input[..60],
        "Investigate failed authentication request in production logs"
    );
    assert!(input[60..].starts_with(' '));
    assert_eq!(
        heuristic_title(input),
        "Investigate failed authentication request in production logs"
    );
}

#[test]
fn test_heuristic_title_keeps_cjk_text_that_has_no_word_spaces() {
    // A CJK sentence has no whitespace to retreat to; it must keep the hard cut
    // instead of collapsing to an empty title.
    let input = "帮我把这个部署流水线在发布窗口关闭之前全部分析清楚并且给出改进建议";
    let title = heuristic_title(input);
    assert!(!title.is_empty());
    assert!(title.chars().count() <= 60, "{title:?}");
    assert!(input.starts_with(&title), "{title:?}");
}

#[test]
fn test_heuristic_title_drops_a_pasted_banner_joined_on_one_line() {
    // Reproduces the reported title: a selection over the banner joined its title
    // and <summary> label into one line, followed by the error body.
    let input = concat!(
        "Message sent, but generation was interrupted Technical details\n",
        "LLM error: initial response headers timed out\n",
        "\n",
        "\n",
        "Why does this keep happening? And here is my diagnostics bundle.\n"
    );
    let title = heuristic_title(input);
    assert_eq!(title, "Why does this keep happening");
}

#[test]
fn test_heuristic_title_drops_a_pasted_banner_on_separate_lines() {
    let input = concat!(
        "Message sent, but generation was interrupted\n",
        "Technical details\n",
        "LLM error: initial response headers timed out\n",
        "\n",
        "The retry policy never recovers on its own.\n"
    );
    let title = heuristic_title(input);
    assert_eq!(title, "The retry policy never recovers on its own");
}

#[test]
fn test_heuristic_title_drops_a_pasted_banner_without_details() {
    let input = "Message sent, but generation was interrupted\n\nHow do I stop this?\n";
    assert_eq!(heuristic_title(input), "How do I stop this");
}

#[test]
fn test_heuristic_title_drops_a_pasted_banner_in_chinese() {
    let input = concat!(
        "消息已发送，但生成中断 技术详情\n",
        "LLM error: initial response headers timed out\n",
        "\n",
        "老是有这个问题 是为什么?\n"
    );
    assert_eq!(heuristic_title(input), "老是有这个问题 是为什么");
}

#[test]
fn test_heuristic_title_falls_back_to_banner_copy_when_nothing_else_remains() {
    // Stripping everything would lose the title entirely, so the banner copy is
    // kept rather than emitting an empty session title.
    let input = "Message sent, but generation was interrupted\n";
    assert_eq!(
        heuristic_title(input),
        "Message sent, but generation was interrupted"
    );
}

#[test]
fn test_heuristic_title_leaves_ordinary_messages_untouched() {
    // Only a banner at the very start is stripped. A message that merely quotes
    // the banner copy keeps its own words, and the long line still caps normally.
    let input = "Why does the banner say \"Message sent, but generation was interrupted\"?";
    let title = heuristic_title(input);
    assert!(title.starts_with("Why does the banner say"), "{title:?}");

    // A banner phrase that is not the first line is not stripped. Only the strip
    // is asserted here: whitespace collapsing predates this fix and joins the
    // lines, so the exact title text is not this fix's contract.
    assert!(
        heuristic_title("Explain this\nMessage sent, but generation was interrupted")
            .starts_with("Explain this")
    );
    assert_eq!(
        heuristic_title("Refactor the auth module"),
        "Refactor the auth module"
    );
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
fn title_context_waits_for_evidence_and_excludes_runtime_records() {
    use bamboo_agent_core::MessagePhase;

    let mut session = Session::new("retry-title", "model");
    session.add_message(Message::user("https://example.test/37 看一下"));
    session.add_message(Message::assistant("收到", None));
    let mut hidden = Message::user("Private runtime instructions");
    hidden.metadata = Some(serde_json::json!({"hidden_from_ui": true}));
    session.add_message(hidden);
    let mut interrupted = Message::assistant("Private interrupted answer", None);
    interrupted.metadata =
        Some(serde_json::json!({"runtime_kind": "interrupted_assistant_output"}));
    session.add_message(interrupted);
    let mut commentary = Message::assistant("Provider commentary", None);
    commentary.phase = Some(MessagePhase::Commentary);
    session.add_message(commentary);
    session.add_message(Message::system("Private system prompt"));
    session.add_message(Message::tool_result("call-1", "Raw tool output"));
    assert!(TitleContext::from_session(&session).is_none());

    let mut answer = Message::assistant("这是 Lotus 的会话标题回退问题。", None);
    answer.phase = Some(MessagePhase::FinalAnswer);
    session.add_message(answer);
    let context = TitleContext::from_session(&session).expect("substantive reply");
    let messages = context.messages();
    let evidence: serde_json::Value = serde_json::from_str(&messages[1].content).unwrap();
    assert_eq!(
        evidence["conversation_excerpts"],
        serde_json::json!([{"role": "assistant", "text": "这是 Lotus 的会话标题回退问题。"}])
    );
    assert_eq!(
        context.parse(r#"{"title":"Lotus 会话标题回退"}"#),
        ModelTitle::Title("Lotus 会话标题回退".into())
    );
}

#[test]
fn title_evidence_fingerprint_ignores_repeated_turns_and_acknowledgements() {
    let mut session = Session::new("pending-title", "model");
    session.add_message(Message::user("我想了解这里的背景"));
    let first = TitleContext::from_session(&session).unwrap().fingerprint();
    session.add_message(Message::user("我想了解这里的背景。"));
    session.add_message(Message::assistant("好的", None));
    let repeated = TitleContext::from_session(&session).unwrap().fingerprint();
    assert_eq!(first, repeated);

    session.add_message(Message::assistant("主题是 Bamboo 的版本升级失败。", None));
    let clarified = TitleContext::from_session(&session).unwrap().fingerprint();
    assert_ne!(clarified, first);
    session.add_message(Message::user("我想了解这里的背景。"));
    assert_eq!(
        TitleContext::from_session(&session).unwrap().fingerprint(),
        clarified
    );
}

#[test]
fn title_model_rejects_a_copied_cjk_request_without_question_marks() {
    for input in [
        "你好，能麻烦你帮我看一下在两个工作目录间来回切换的时候，侧边栏为何会保留旧标签吗",
        "切换工作目录会保留旧标签吗",
        "请问切换目录后侧边栏还是保留了旧标签",
    ] {
        let mut session = Session::new("implicit-question", "model");
        session.add_message(Message::user(input));
        let context = TitleContext::from_session(&session).unwrap();
        assert_eq!(
            context.parse(&serde_json::json!({"title": input}).to_string()),
            ModelTitle::Invalid("input_echo")
        );
    }
}

#[test]
fn title_model_accepts_only_clear_json_decisions() {
    let mut session = Session::new("title-quality", "model");
    session.add_message(Message::user(
        "请你研究一下这个标题功能为什么经常复述用户的原话并且把回退方案写出来",
    ));
    let context = TitleContext::from_session(&session).unwrap();
    assert!(context.messages()[0].content.contains(r#"{"title":null}"#));
    assert_eq!(
        context.parse(r#"{"title":null}"#),
        ModelTitle::InsufficientContext
    );
    assert_eq!(
        context.parse(r#"{"title":"会话标题回退优化"}"#),
        ModelTitle::Title("会话标题回退优化".into())
    );
    for invalid in [
        "",
        "会话标题回退优化",
        "{}",
        r#"{"title":9}"#,
        r#"{"title":" "}"#,
        r#"{"title":"——"}"#,
        r#"{"title":"新会话"}"#,
        r#"{"title":"Title: Session titles"}"#,
        r#"{"title":"Topic\nAnother line"}"#,
        r#"{"title":"https://example.test"}"#,
        r#"{"title":"请你研究一下这个标题功能"}"#,
        r#"{"title":"请你研究一下这个标题功能为什么经常复述用户的原话并且把回退方案写出来"}"#,
        r#"{"title":null,"reason":"unknown"}"#,
    ] {
        assert!(
            matches!(context.parse(invalid), ModelTitle::Invalid(_)),
            "{invalid:?}"
        );
    }
}

#[test]
fn title_model_can_reuse_a_good_short_label() {
    let mut session = Session::new("named-topic", "model");
    session.add_message(Message::user("Bamboo Session Titles"));
    let context = TitleContext::from_session(&session).unwrap();
    assert_eq!(
        context.parse(r#"{"title":"Bamboo Session Titles"}"#),
        ModelTitle::Title("Bamboo Session Titles".into())
    );
}

#[test]
fn title_labels_keep_language_identifiers_and_issue_references() {
    let mut session = Session::new("language-title", "model");
    session.add_message(Message::user("C++"));
    let cpp = TitleContext::from_session(&session).unwrap();
    assert_eq!(
        cpp.parse(r#"{"title":"C++"}"#),
        ModelTitle::Title("C++".into())
    );
    session.messages.clear();
    session.add_message(Message::user("C#"));
    let csharp = TitleContext::from_session(&session).unwrap();
    assert_ne!(cpp.fingerprint(), csharp.fingerprint());
    assert_eq!(
        csharp.parse(r##"{"title":"#123 C# SDK"}"##),
        ModelTitle::Title("#123 C# SDK".into())
    );
}

#[test]
fn title_tracker_coalesces_new_evidence_while_preserving_the_active_slot() {
    let tracker = TitleGenerationTracker::default();
    let mut session = Session::new("s1", "model");
    assert!(tracker.acquire("s1", true));
    assert!(tracker.record_attempt(&session, 10));
    assert!(!tracker.acquire("s1", false));
    assert!(!tracker.acquire("s1", true));
    assert!(!tracker.acquire("s1", true));
    assert!(tracker.finish("s1"));
    assert!(!tracker.record_attempt(&session, 10));
    assert!(tracker.record_attempt(&session, 20));
    assert!(!tracker.finish("s1"));
    assert!(tracker.acquire("s1", false));
    assert!(!tracker.record_attempt(&session, 20));
    session.metadata.insert(
        bamboo_storage::v2::SIDE_CHAT_CLEAR_EPOCH_KEY.into(),
        "after-clear".into(),
    );
    assert!(tracker.record_attempt(&session, 20));
    tracker.release("s1");
    assert!(tracker.acquire("s1", true));
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
