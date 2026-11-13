use super::{AnthropicProvider, BodhiProvider, GeminiProvider, OpenAIProvider};
use crate::{provider_factory::create_provider_from_instance, Config, LLMProvider};
use bamboo_domain::Message;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn provider_factory_uses_the_same_header_precedence_for_every_http_format() {
    for (provider_type, target, endpoint, url, auth_name, auth_value) in [
        (
            "openai",
            "openai",
            "chat_completions",
            "/chat/completions",
            "authorization",
            "Bearer fixture-key",
        ),
        (
            "openai",
            "openai",
            "responses",
            "/responses",
            "authorization",
            "Bearer fixture-key",
        ),
        (
            "anthropic",
            "anthropic",
            "messages",
            "/messages",
            "x-api-key",
            "fixture-key",
        ),
        (
            "gemini",
            "gemini",
            "stream_generate_content",
            "/models/fixture-model:streamGenerateContent",
            "x-goog-api-key",
            "fixture-key",
        ),
        (
            "bodhi",
            "openai",
            "chat_completions",
            "/proxy/openai/v1/chat/completions",
            "authorization",
            "Bearer fixture-key",
        ),
        (
            "bodhi",
            "anthropic",
            "messages",
            "/proxy/anthropic/v1/messages",
            "authorization",
            "Bearer fixture-key",
        ),
        (
            "bodhi",
            "gemini",
            "stream_generate_content",
            "/proxy/gemini/v1beta/models/fixture-model:streamGenerateContent",
            "authorization",
            "Bearer fixture-key",
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(url))
            .respond_with(ResponseTemplate::new(400).set_body_string("fixture response"))
            .expect(1)
            .mount(&server)
            .await;
        let instance = serde_json::from_value(json!({
            "provider_type": provider_type,
            "target_provider": target,
            "api_key": "fixture-key",
            "base_url": server.uri(),
            "enabled": true,
            "responses_only_models": if endpoint == "responses" { vec!["fixture-model"] } else { vec![] },
            "request_overrides": {
                "common": { "headers": { "User-Agent": "Fixture/1.0", "X-Gateway-Trace": "common" } },
                "endpoints": { (endpoint): { "headers": { "x-gateway-trace": "endpoint" } } },
                "rules": [{ "model_pattern": "fixture-model", "endpoint": endpoint,
                    "scope": { "headers": { "user-agent": "Fixture/2.0" } } }]
            }
        })).unwrap();
        let provider =
            create_provider_from_instance(&Config::default(), &instance, std::env::temp_dir())
                .await
                .unwrap();
        let result = provider
            .chat_stream(&[Message::user("hello")], &[], Some(64), "fixture-model")
            .await;
        assert!(result.is_err(), "{provider_type} {endpoint}");
        let requests = server.received_requests().await.unwrap();
        let [request] = requests.as_slice() else {
            panic!("{provider_type} {endpoint} should send one request");
        };
        assert_eq!(request.url.path(), url, "{provider_type} {endpoint}");
        assert_eq!(request.headers["user-agent"], "Fixture/2.0");
        assert_eq!(request.headers["x-gateway-trace"], "endpoint");
        assert_eq!(request.headers[auth_name], auth_value);
    }
}

#[tokio::test]
async fn direct_provider_constructors_send_the_default_user_agent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .expect(4)
        .mount(&server)
        .await;
    let providers: [Box<dyn LLMProvider>; 4] = [
        Box::new(OpenAIProvider::new("fixture-key").with_base_url(server.uri())),
        Box::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri())),
        Box::new(GeminiProvider::new("fixture-key").with_base_url(server.uri())),
        Box::new(BodhiProvider::new("fixture-key").with_base_url(server.uri())),
    ];
    for provider in providers {
        provider.list_models().await.unwrap();
    }
    for request in server.received_requests().await.unwrap() {
        assert_eq!(
            request.headers["user-agent"],
            concat!("Bamboo/", env!("CARGO_PKG_VERSION"))
        );
    }
}
