use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use actix_web::{test, web, App};
use bamboo_llm::{providers::AnthropicProvider, Config};
use serde_json::{json, Value};
use wiremock::{matchers::path, Mock, MockServer, Request, ResponseTemplate};

use crate::{app_state::AppState, routes};

const EVENTS: &str = concat!(
    "event: message_start\ndata: {\"message\":{\"id\":\"msg_test\",\"usage\":{\"input_tokens\":8}}}\n\n",
    "event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1,\"cache_read_input_tokens\":12,\"cache_creation_input_tokens\":0}}\n\n",
    "event: message_stop\ndata: {}\n\n",
);

fn forwarding_request(url: &str, body: &Value, route: &str) -> test::TestRequest {
    test::TestRequest::post()
        .uri(url)
        .insert_header(("provider-route", route))
        .insert_header(("proxy-route-dye", "alternate-provider"))
        .append_header(("custom-tags", "first"))
        .append_header(("custom-tags", "second"))
        .insert_header(("x-litellm-session-id", "fixture-session-label"))
        .insert_header(("anthropic-beta", "caller-beta, common-beta"))
        .insert_header(("x-api-key", "local-key"))
        .insert_header(("authorization", "Bearer local-only"))
        .insert_header(("cookie", "local-only=1"))
        .insert_header(("x-bamboo-control", "local-only"))
        .insert_header(("connection", "keep-alive, connection-option"))
        .insert_header(("connection-option", "local-only"))
        .set_json(body)
}

#[actix_web::test]
async fn registered_compat_routes_forward_custom_headers_both_ways() {
    let upstream = MockServer::start().await;
    let other_origin = MockServer::start().await;
    let redirect_url = format!("{}/v1/messages", other_origin.uri());
    let retried = Arc::new(AtomicBool::new(false));
    let retry_state = retried.clone();
    Mock::given(path("/v1/messages"))
        .respond_with(move |request: &Request| {
            let route = request
                .headers
                .get("provider-route")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing");
            if route == "retry" && !retry_state.swap(true, Ordering::Relaxed) {
                return ResponseTemplate::new(503).insert_header("discarded-attempt", "first");
            }
            if route == "redirect" {
                return ResponseTemplate::new(307)
                    .insert_header("location", redirect_url.as_str())
                    .insert_header("provider-route-result", route);
            }
            ResponseTemplate::new(200)
                .set_body_raw(EVENTS, "text/event-stream")
                .insert_header("provider-route-result", route)
                .append_header("provider-traces", "first")
                .append_header("provider-traces", "second")
                .insert_header("x-litellm-model-id", "deployment-fixture")
                .insert_header("set-cookie", "private=1")
                .insert_header("llm_provider-authorization", "sensitive")
                .insert_header("bamboo-output-scope", "upstream")
                .insert_header("connection", "close, connection-option")
                .insert_header("connection-option", "upstream-only")
        })
        .mount(&upstream)
        .await;

    let home = tempfile::tempdir().unwrap();
    let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
    config.features.provider_model_ref = false;
    config
        .anthropic_model_mapping
        .mappings
        .insert("sonnet".into(), "fixture-model".into());
    config
        .gemini_model_mapping
        .mappings
        .insert("flash".into(), "fixture-model".into());
    let provider = AnthropicProvider::new("fixture-key")
        .with_base_url(format!("{}/v1", upstream.uri()))
        .with_request_overrides(Some(
            serde_json::from_value(json!({
                "common": {"headers": {"anthropic-beta": "provider-beta,common-beta"}}
            }))
            .unwrap(),
        ));
    let state = web::Data::new(
        Box::pin(AppState::new_with_provider(
            home.path().to_path_buf(),
            config,
            Arc::new(provider),
        ))
        .await
        .unwrap(),
    );
    let app = test::init_service(
        App::new()
            .app_data(state)
            .configure(routes::configure_routes)
            .wrap(crate::config::build_cors("127.0.0.1", 9562))
            .wrap(actix_web::middleware::from_fn(super::expose)),
    )
    .await;

    let messages = json!([{"role":"user","content":"hi"}]);
    let gemini = json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}]});
    let mut cases = Vec::new();
    for stream in [false, true] {
        cases.extend([
            ("/anthropic/v1/messages", json!({
                "model":"claude-sonnet-fixture","messages":messages,"max_tokens":128,"stream":stream
            })),
            ("/anthropic/v1/complete", json!({
                "model":"claude-sonnet-fixture","prompt":"\n\nHuman: hi\n\nAssistant:",
                "max_tokens_to_sample":128,"stream":stream
            })),
            ("/openai/v1/chat/completions", json!({
                "model":"fixture-model","messages":messages,"stream":stream
            })),
            ("/openai/v1/responses", json!({
                "model":"fixture-model","input":"hi","stream":stream
            })),
        ]);
    }
    cases.extend([
        (
            "/gemini/v1beta/models/gemini-flash:generateContent",
            gemini.clone(),
        ),
        (
            "/gemini/v1beta/models/gemini-flash:streamGenerateContent",
            gemini,
        ),
    ]);
    for (index, (uri, body)) in cases.iter().enumerate() {
        let route = format!("call-{index}");
        let response = test::call_service(
            &app,
            forwarding_request(uri, body, &route)
                .insert_header(("origin", "http://localhost:5173"))
                .to_request(),
        )
        .await;
        assert!(
            response.status().is_success(),
            "{uri}: {}",
            response.status()
        );
        let headers = response.headers();
        assert_eq!(
            headers.get("provider-route-result").unwrap(),
            route.as_str()
        );
        assert_eq!(
            headers.get("x-litellm-model-id").unwrap(),
            "deployment-fixture"
        );
        let trace_values: Vec<_> = headers
            .get_all("provider-traces")
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(trace_values, ["first", "second"]);
        for name in [
            "set-cookie",
            "llm_provider-authorization",
            "bamboo-output-scope",
            "connection-option",
        ] {
            assert!(!headers.contains_key(name), "{name} on {uri}");
        }
        let exposed: Vec<_> = headers
            .get_all("access-control-expose-headers")
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(',').map(str::trim))
            .collect();
        assert!(exposed.contains(&"provider-route-result"), "{uri}");
        let body = test::read_body(response).await;
        assert!(String::from_utf8_lossy(&body).contains("hello"), "{uri}");
    }

    let (uri, body) = &cases[0];
    let (retry_response, parallel_response) = tokio::join!(
        test::call_service(&app, forwarding_request(uri, body, "retry").to_request()),
        test::call_service(&app, forwarding_request(uri, body, "parallel").to_request()),
    );
    assert!(retried.load(Ordering::Relaxed));
    for (response, route) in [(retry_response, "retry"), (parallel_response, "parallel")] {
        assert!(response.status().is_success());
        assert_eq!(
            response.headers().get("provider-route-result").unwrap(),
            route
        );
        assert!(!response.headers().contains_key("discarded-attempt"));
        assert!(response
            .headers()
            .get_all("access-control-expose-headers")
            .all(|value| !value.to_str().unwrap().contains("provider-route-result")));
    }

    let response =
        test::call_service(&app, forwarding_request(uri, body, "redirect").to_request()).await;
    assert!(!response.status().is_success());
    assert_eq!(
        response.headers().get("provider-route-result").unwrap(),
        "redirect"
    );
    assert!(!response.headers().contains_key("location"));
    assert!(other_origin.received_requests().await.unwrap().is_empty());

    let requests = upstream.received_requests().await.unwrap();
    let forwarded: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path() == "/v1/messages")
        .collect();
    assert_eq!(forwarded.len(), cases.len() + 4);
    for request in forwarded {
        let headers = &request.headers;
        assert_eq!(
            headers.get("proxy-route-dye").unwrap(),
            "alternate-provider"
        );
        assert_eq!(headers.get("x-api-key").unwrap(), "fixture-key");
        assert_eq!(
            headers.get("x-litellm-session-id").unwrap(),
            "fixture-session-label"
        );
        assert_eq!(
            headers.get("anthropic-beta").unwrap(),
            "provider-beta,common-beta,caller-beta"
        );
        let tags: Vec<_> = headers
            .get_all("custom-tags")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(tags, ["first", "second"]);
        for name in [
            "authorization",
            "cookie",
            "x-bamboo-control",
            "connection-option",
        ] {
            assert!(!headers.contains_key(name), "{name} in upstream request");
        }
    }
}
