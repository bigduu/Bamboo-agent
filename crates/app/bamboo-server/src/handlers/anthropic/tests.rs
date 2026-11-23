use actix_web::http::StatusCode;
use serde_json::json;
use std::collections::HashMap;

use bamboo_llm::api::models::{
    ChatCompletionResponse, ChatMessage, Content, ResponseChoice, Role, Usage,
};
use bamboo_llm::providers::anthropic::api_types::{
    AnthropicContent, AnthropicMessage, AnthropicMessagesRequest, AnthropicRole,
    AnthropicToolChoice,
};
use bamboo_llm::providers::anthropic::conversion::AnthropicConversionError;

use super::conversion::{convert_messages_request, convert_messages_response};
use super::errors::map_conversion_error;

#[actix_web::test]
async fn messages_stream_forwards_late_cache_usage_and_records_metrics() {
    use actix_web::{test, web, App};
    use bamboo_llm::providers::AnthropicProvider;
    use bamboo_metrics::types::{ForwardMetricsFilter, ForwardStatus};
    use std::{sync::Arc, time::Duration};
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    let upstream = MockServer::start().await;
    let events = concat!(
        "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":9}}}\n\n",
        "event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
        "event: message_delta\ndata: {\"usage\":{\"output_tokens\":6,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":64000}}\n\n",
        "event: message_stop\ndata: {}\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(events, "text/event-stream"))
        .expect(1)
        .mount(&upstream)
        .await;
    let home = tempfile::tempdir().unwrap();
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
    config.features.provider_model_ref = false;
    config
        .anthropic_model_mapping
        .mappings
        .insert("sonnet".into(), "fixture-model".into());
    let provider =
        AnthropicProvider::new("fixture-key").with_base_url(format!("{}/v1", upstream.uri()));
    let state = web::Data::new(
        Box::pin(crate::app_state::AppState::new_with_provider(
            home.path().into(),
            config,
            Arc::new(provider),
        ))
        .await
        .unwrap(),
    );
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/anthropic/v1/messages", web::post().to(super::messages)),
    )
    .await;
    let request = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model":"claude-sonnet-fixture",
            "messages":[{"role":"user","content":"hi"}],
            "max_tokens":128,
            "stream":true
        }))
        .to_request();
    let response = test::call_service(&app, request).await;
    let status = response.status();
    let bytes = test::read_body(response).await;
    let sse = std::str::from_utf8(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{sse}");
    let delta = sse
        .split("\n\n")
        .find_map(|event| {
            let data = event.strip_prefix("event: message_delta\ndata: ")?;
            serde_json::from_str::<serde_json::Value>(data).ok()
        })
        .expect("terminal usage event");
    assert_eq!(
        delta["usage"],
        json!({
            "input_tokens":9,
            "output_tokens":6,
            "cache_creation_input_tokens":0,
            "cache_read_input_tokens":64000
        })
    );

    let metrics = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = state
                .metrics_service
                .forward_requests(ForwardMetricsFilter {
                    endpoint: Some("anthropic.messages".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            if let Some(record) = requests
                .into_iter()
                .find(|request| request.status == Some(ForwardStatus::Success))
            {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completed forward metrics");
    assert_eq!(metrics.token_details.cache_creation_input_tokens, Some(0));
    assert_eq!(metrics.token_details.cache_read_input_tokens, Some(64000));
    let usage = metrics.token_usage.unwrap();
    assert_eq!(
        (
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens
        ),
        (64009, 6, 64015)
    );
}

#[test]
fn convert_messages_request_should_convert_basic_user_text() {
    let request = AnthropicMessagesRequest {
        model: "claude-3-5-sonnet-latest".to_string(),
        messages: vec![AnthropicMessage {
            role: AnthropicRole::User,
            content: AnthropicContent::Text("hello".to_string()),
        }],
        system: None,
        max_tokens: Some(64),
        temperature: None,
        top_p: None,
        top_k: None,
        stop_sequences: None,
        stream: Some(false),
        tools: None,
        tool_choice: None,
        extra: HashMap::new(),
    };

    let converted = convert_messages_request(request).expect("request conversion should work");

    assert_eq!(converted.messages.len(), 1);
    assert_eq!(converted.parameters.get("max_tokens"), Some(&json!(64)));
    assert_eq!(
        converted.messages[0].content,
        Content::Text("hello".to_string())
    );
}

#[test]
fn convert_messages_response_should_map_tool_calls_to_tool_use_stop_reason() {
    let response = ChatCompletionResponse {
        id: "chatcmpl-1".to_string(),
        object: Some("chat.completion".to_string()),
        created: Some(1),
        model: Some("gpt-4o".to_string()),
        choices: vec![ResponseChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content: Content::Text("hi".to_string()),
                phase: None,
                tool_calls: None,
                tool_call_id: None,
            },
            finish_reason: Some("tool_calls".to_string()),
        }],
        usage: Some(Usage {
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
        }),
        system_fingerprint: None,
    };

    let converted = convert_messages_response(response, "claude-3-5-sonnet-latest")
        .expect("response conversion should work");

    assert_eq!(converted.stop_reason, "tool_use");
}

#[test]
fn convert_messages_request_should_preserve_provider_error_status_and_type() {
    let request = AnthropicMessagesRequest {
        model: "claude-3-5-sonnet-latest".to_string(),
        messages: vec![AnthropicMessage {
            role: AnthropicRole::User,
            content: AnthropicContent::Text("hello".to_string()),
        }],
        system: None,
        max_tokens: None,
        temperature: None,
        top_p: None,
        top_k: None,
        stop_sequences: None,
        stream: Some(false),
        tools: None,
        tool_choice: Some(AnthropicToolChoice::String("unsupported".to_string())),
        extra: HashMap::new(),
    };

    let error =
        convert_messages_request(request).expect_err("conversion should reject bad tool_choice");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.error_type, "invalid_request_error");
    assert!(error.message.contains("Unsupported tool_choice value"));
}

#[test]
fn map_conversion_error_should_fallback_to_bad_gateway_for_invalid_status_code() {
    let mapped = map_conversion_error(AnthropicConversionError::new(
        1000,
        "api_error",
        "invalid upstream status".to_string(),
    ));

    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
}
