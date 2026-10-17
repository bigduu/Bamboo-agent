use super::*;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::image_fallback::{describe_tool_images, resolve_vision_target};
use bamboo_agent_core::{ToolResult, ToolResultImage};
use serde_json::json;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn config_for(protocol: &str, endpoint: &str) -> Config {
    let mut config = Config::default();
    config.features.provider_model_ref = true;
    config.default_provider_instance = Some("main-account".into());
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat": {"provider":"main-account", "model":"same-name"},
            "fast": {"provider":"main-account", "model":"same-name"},
            "vision": {"provider":"vision-account", "model":"same-name"}
        }))
        .unwrap(),
    );
    for (id, kind, support) in [
        ("main-account", "openai", false),
        ("vision-account", protocol, true),
    ] {
        config.provider_instances.insert(
            id.into(),
            serde_json::from_value(json!({
                "provider_type": kind, "api_key":"mock-key-not-a-credential", "base_url":endpoint,
                "model":"same-name", "runtime_models":["same-name"],
                "model_capabilities":{"same-name":{"supports_vision":support}}
            }))
            .unwrap(),
        );
    }
    config
}

#[test]
fn vision_configuration_requires_an_explicit_exact_enabled_admitted_target() {
    let config = config_for("anthropic", "http://127.0.0.1:1");
    let (target, instance) =
        configured_target(&config, config.effective_default_provider()).unwrap();
    assert_eq!(target.provider, "vision-account");
    assert_eq!(instance.provider_type, "anthropic");
    for variant in 0..7 {
        let mut invalid = config.clone();
        match variant {
            0 => invalid.defaults.as_mut().unwrap().vision = None,
            1 => {
                invalid
                    .defaults
                    .as_mut()
                    .unwrap()
                    .vision
                    .as_mut()
                    .unwrap()
                    .provider = "unconfigured".into()
            }
            2 => {
                invalid
                    .provider_instances
                    .get_mut("vision-account")
                    .unwrap()
                    .enabled = false
            }
            3 => {
                invalid
                    .provider_instances
                    .get_mut("vision-account")
                    .unwrap()
                    .extra
                    .insert("runtime_models".into(), json!([]));
            }
            4 => {
                invalid
                    .provider_instances
                    .get_mut("vision-account")
                    .unwrap()
                    .extra
                    .insert(
                        "model_capabilities".into(),
                        json!({"same-name":{"supports_vision":false}}),
                    );
            }
            5 => {
                invalid
                    .defaults
                    .as_mut()
                    .unwrap()
                    .vision
                    .as_mut()
                    .unwrap()
                    .provider = "anthropic".into()
            }
            _ => {
                invalid
                    .defaults
                    .as_mut()
                    .unwrap()
                    .vision
                    .as_mut()
                    .unwrap()
                    .model = "not-admitted".into()
            }
        }
        assert!(
            configured_target(&invalid, invalid.effective_default_provider()).is_err(),
            "variant {variant} must not fall back to fast/chat or another account"
        );
    }
}

#[test]
fn vision_configuration_preserves_default_on_null_and_legacy_selections() {
    for capability in [
        json!({}),
        json!({"same-name":{}}),
        json!({"same-name":{"supports_vision":null}}),
    ] {
        let mut config = config_for("gemini", "http://127.0.0.1:1");
        config
            .provider_instances
            .get_mut("vision-account")
            .unwrap()
            .extra
            .insert("model_capabilities".into(), capability);
        assert!(configured_target(&config, config.effective_default_provider()).is_ok());
        // Legacy mode uses the active instance's explicitly selected vision_model.
        config.features.provider_model_ref = false;
        config.default_provider_instance = Some("vision-account".into());
        config
            .provider_instances
            .get_mut("vision-account")
            .unwrap()
            .vision_model = Some("same-name".into());
        assert_eq!(
            configured_target(&config, config.effective_default_provider())
                .unwrap()
                .0
                .provider,
            "vision-account"
        );
        config
            .provider_instances
            .get_mut("vision-account")
            .unwrap()
            .vision_model = None;
        assert!(configured_target(&config, config.effective_default_provider()).is_err());
    }
}

fn sse(protocol: &str) -> &'static str {
    match protocol {
        "openai" => concat!(
            "data: {\"id\":\"mock\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"same-name\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"mock visual description\"},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        ),
        "anthropic" => concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"mock\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"same-name\",\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"mock visual description\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
        "gemini" => "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"mock visual description\"}],\"role\":\"model\"},\"finishReason\":\"STOP\"}]}\n\n",
        _ => unreachable!(),
    }
}

fn image_result() -> ToolResult {
    ToolResult {
        success: true,
        images: vec![ToolResultImage {
            mime_type: "image/png".into(),
            data: "iVBORw0KGgo=".into(),
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn configured_vision_factory_uses_native_protocols_only_at_the_selected_mock_endpoint() {
    // These are local fake HTTP services; every credential/model is a fixture.
    // The main account has the same model name with Vision=false, proving that
    // selection cannot collapse the target onto the executing provider.
    for protocol in ["openai", "anthropic", "gemini"] {
        let main = MockServer::start().await;
        let vision = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(sse(protocol)),
            )
            .expect(1)
            .mount(&vision)
            .await;
        let mut config = config_for(protocol, &vision.uri());
        config
            .provider_instances
            .get_mut("main-account")
            .unwrap()
            .base_url = Some(main.uri());
        let config = Arc::new(RwLock::new(config));
        let dir = tempfile::tempdir().unwrap();
        let loop_config = AgentLoopConfig {
            vision_model_resolver: Some(configured_vision_resolver(
                config.clone(),
                Some(dir.path().to_owned()),
                "main-account".into(),
            )),
            ..Default::default()
        };
        let target = resolve_vision_target(&loop_config).await.unwrap();
        let mut result = image_result();
        describe_tool_images(&target, &mut result).await.unwrap();
        assert!(result.images.is_empty());
        assert!(result.result.contains("Vision model output"));
        assert!(result.result.contains("untrusted data"));
        assert!(result.result.contains("mock visual description"));
        assert!(!result.result.contains("iVBORw0KGgo="));
        assert!(main.received_requests().await.unwrap().is_empty());
        let requests = vision.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        match protocol {
            "openai" => {
                assert_eq!(requests[0].url.path(), "/chat/completions");
                assert_eq!(body["model"], "same-name");
                assert_eq!(body["messages"][1]["content"][1]["type"], "image_url");
            }
            "anthropic" => {
                assert_eq!(requests[0].url.path(), "/messages");
                assert_eq!(body["messages"][0]["content"][1]["type"], "image");
                assert_eq!(
                    body["messages"][0]["content"][1]["source"]["type"],
                    "base64"
                );
            }
            "gemini" => {
                assert_eq!(
                    requests[0].url.path(),
                    "/models/same-name:streamGenerateContent"
                );
                assert_eq!(
                    body["contents"][0]["parts"][1]["inlineData"]["mimeType"],
                    "image/png"
                );
            }
            _ => unreachable!(),
        }
        // Live config changes fail closed on the next call; no main-model or
        // provider reassignment and no request to another account is allowed.
        config
            .write()
            .await
            .provider_instances
            .get_mut("vision-account")
            .unwrap()
            .enabled = false;
        assert!(resolve_vision_target(&loop_config).await.is_err());
        assert_eq!(vision.received_requests().await.unwrap().len(), 1);
        assert_eq!(
            config.read().await.defaults.as_ref().unwrap().chat.provider,
            "main-account"
        );
    }
}

#[test]
fn missing_global_vision_role_can_use_the_active_instances_explicit_vision_model() {
    let mut config = config_for("gemini", "http://127.0.0.1:1");
    config.defaults.as_mut().unwrap().vision = None;
    let active = config.provider_instances.get_mut("main-account").unwrap();
    active.vision_model = Some("local-vision".into());
    active.extra.insert(
        "runtime_models".into(),
        json!(["same-name", "local-vision"]),
    );
    let (target, _) = configured_target(&config, config.effective_default_provider()).unwrap();
    assert_eq!(target.provider, "main-account");
    assert_eq!(target.model, "local-vision");
}

fn session_provider_config(default_endpoint: &str, active_endpoint: &str) -> Config {
    let mut config = config_for("openai", active_endpoint);
    config.defaults.as_mut().unwrap().vision = None;
    let default = config.provider_instances.get_mut("main-account").unwrap();
    default.base_url = Some(default_endpoint.into());
    default.vision_model = Some("same-name".into());
    default.extra.insert(
        "model_capabilities".into(),
        json!({"same-name":{"supports_vision":true}}),
    );
    let active = config.provider_instances.get_mut("vision-account").unwrap();
    active.model = Some("text-main".into());
    active.vision_model = Some("same-name".into());
    active
        .extra
        .insert("runtime_models".into(), json!(["text-main", "same-name"]));
    active.extra.insert(
        "model_capabilities".into(),
        json!({"text-main":{"supports_vision":false},"same-name":{"supports_vision":true}}),
    );
    config
}

#[test]
fn vision_no_global_role_uses_only_the_sessions_actual_provider_instance() {
    for model_ref_enabled in [true, false] {
        let mut config = session_provider_config("http://127.0.0.1:1", "http://127.0.0.1:2");
        config.features.provider_model_ref = model_ref_enabled;
        assert_eq!(config.effective_default_provider(), "main-account");
        let (target, _) = configured_target(&config, "vision-account").unwrap();
        assert_eq!(target.provider, "vision-account");
        assert_eq!(target.model, "same-name");
        for variant in 0..5 {
            let mut invalid = config.clone();
            let active = invalid
                .provider_instances
                .get_mut("vision-account")
                .unwrap();
            match variant {
                0 => active.vision_model = None,
                1 => active.enabled = false,
                2 => {
                    active.extra.insert(
                        "model_capabilities".into(),
                        json!({"same-name":{"supports_vision":false}}),
                    );
                }
                3 => {
                    active
                        .extra
                        .insert("runtime_models".into(), json!(["text-main"]));
                }
                _ => {}
            }
            let provider = if variant == 4 {
                "unconfigured"
            } else {
                "vision-account"
            };
            assert!(
                configured_target(&invalid, provider).is_err(),
                "variant {variant} must never select the valid process-default Vision account"
            );
        }
    }
}

#[tokio::test]
async fn vision_no_global_role_never_posts_to_the_process_default_account() {
    let default = MockServer::start().await;
    let active = MockServer::start().await;
    for endpoint in [&default, &active] {
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(sse("openai")),
            )
            .mount(endpoint)
            .await;
    }
    let config = Arc::new(RwLock::new(session_provider_config(
        &default.uri(),
        &active.uri(),
    )));
    let dir = tempfile::tempdir().unwrap();
    let loop_config = AgentLoopConfig {
        vision_model_resolver: Some(configured_vision_resolver(
            config.clone(),
            Some(dir.path().to_owned()),
            "vision-account".into(),
        )),
        ..Default::default()
    };
    let target = resolve_vision_target(&loop_config).await.unwrap();
    let mut result = image_result();
    describe_tool_images(&target, &mut result).await.unwrap();
    assert!(result.result.contains("mock visual description"));
    assert!(default.received_requests().await.unwrap().is_empty());
    assert_eq!(active.received_requests().await.unwrap().len(), 1);
    config
        .write()
        .await
        .provider_instances
        .get_mut("vision-account")
        .unwrap()
        .vision_model = None;
    assert!(resolve_vision_target(&loop_config).await.is_err());
    assert!(default.received_requests().await.unwrap().is_empty());
    assert_eq!(active.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        config.read().await.defaults.as_ref().unwrap().chat.provider,
        "main-account"
    );
}
