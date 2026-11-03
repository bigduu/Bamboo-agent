use super::*;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

const TIMEOUT: Duration = Duration::from_secs(10);

async fn profile(server: &MockServer, model: &str, context: u32, tokens: &[u32]) {
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "id": model, "owned_by": "llamacpp",
                "meta": {"n_vocab": 201088, "n_embd": 2880, "n_params": 20914790400u64}
            }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/props"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model_alias": model,
            "default_generation_settings": {"n_ctx": context}
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/tokenize"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tokens": tokens})))
        .mount(server)
        .await;
}

fn answer(prompt: &GptOssPrompt, tokens: &[u32]) -> Value {
    json!({
        "model": MODEL,
        "stop": true,
        "stop_type": "eos",
        "truncated": false,
        "tokens_evaluated": prompt.tokens().len(),
        "tokens_predicted": tokens.len(),
        "tokens": tokens,
    })
}

fn final_answer(text: &str) -> Vec<u32> {
    let mut tokens = PROBE_TOKENS[2..5].to_vec();
    tokens.extend(tokenizer().unwrap().encode_ordinary(text));
    tokens.push(RETURN);
    tokens
}

async fn completion(server: &MockServer, response: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path("/completion"))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn raw_completion_sends_the_preflighted_numeric_prompt_once() {
    let server = MockServer::start().await;
    profile(&server, MODEL, 4096, PROBE_TOKENS).await;
    assert_eq!(
        tokenizer().unwrap().encode_with_special_tokens(PROBE),
        PROBE_TOKENS
    );
    let prompt = GptOssPrompt::encode("Return a JSON object.", "restricted Work evidence").unwrap();
    let mut generated = tokenizer().unwrap().encode_with_special_tokens(
        "<|channel|>analysis<|message|>internal reasoning<|end|><|start|>assistant",
    );
    let final_json = r#"{"advice":"observe"}"#;
    generated.extend(final_answer(final_json));
    let reply = answer(&prompt, &generated);
    completion(&server, ResponseTemplate::new(200).set_body_json(&reply)).await;

    let client = GptOssLocalClient::new(*server.address()).unwrap();
    let prepared = client.prepare(&prompt, 128, TIMEOUT).await.unwrap();
    let probes = server.received_requests().await.unwrap();
    assert_eq!(probes.len(), 3);
    assert_eq!(probes[0].url.path(), "/v1/models");
    assert_eq!(probes[1].url.path(), "/props");
    assert_eq!(probes[2].url.path(), "/tokenize");
    assert_eq!(
        probes[2].body_json::<Value>().unwrap(),
        json!({"content": PROBE, "add_special": false, "parse_special": true})
    );

    let result = prepared.complete(TIMEOUT).await.unwrap();
    assert_eq!(result.final_text.as_deref(), Some(final_json));
    let usage = result.usage.known().unwrap();
    assert_eq!(usage.prompt_tokens, prompt.tokens().len() as u64);
    assert_eq!(usage.completion_tokens, generated.len() as u64);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    let payload = requests[3].body_json::<Value>().unwrap();
    assert_eq!(requests[3].url.path(), "/completion");
    assert_eq!(payload["model"], MODEL);
    assert_eq!(payload["prompt"], json!(prompt.tokens()));
    assert_eq!(payload["n_predict"], 128);
    assert_eq!(payload["n_cmpl"], 1);
    assert_eq!(payload["cache_prompt"], false);
    assert_eq!(payload["stream"], false);
    assert_eq!(payload["return_tokens"], true);
}

#[tokio::test]
async fn incompatible_or_nonlocal_profile_never_receives_the_prompt() {
    assert!(GptOssLocalClient::new("192.0.2.1:8123".parse().unwrap()).is_err());
    let prompt = GptOssPrompt::encode("Return JSON.", "Work evidence").unwrap();
    for (model, context, tokens) in [
        ("gpt-oss-120b", 4096, PROBE_TOKENS),
        (MODEL, 32, PROBE_TOKENS),
        (MODEL, 4096, &[0][..]),
    ] {
        let server = MockServer::start().await;
        profile(&server, model, context, tokens).await;
        let client = GptOssLocalClient::new(*server.address()).unwrap();
        assert!(matches!(
            client.prepare(&prompt, 64, TIMEOUT).await,
            Err(CompletionError::DeploymentMismatch)
        ));
        assert!(server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/completion"));
    }
}

#[tokio::test]
async fn advisory_output_and_accounting_are_validated_independently() {
    let prompt = GptOssPrompt::encode("Return JSON.", "Work evidence").unwrap();
    let normal = answer(&prompt, &final_answer("{}"));
    for (key, value, known, expected_count) in [
        ("tokens", json!("invalid frame"), true, None),
        (
            "tokens",
            json!([CHANNEL, 17_196, MESSAGE, 200_012]),
            true,
            None,
        ),
        ("tokens_predicted", Value::Null, false, None),
        ("tokens_evaluated", json!(1), false, None),
        ("tokens_predicted", json!(129), true, Some(129)),
        ("truncated", json!(true), true, None),
        ("stop_type", json!("limit"), true, None),
    ] {
        let server = MockServer::start().await;
        profile(&server, MODEL, 4096, PROBE_TOKENS).await;
        let mut reply = normal.clone();
        reply[key] = value;
        completion(&server, ResponseTemplate::new(200).set_body_json(reply)).await;
        let client = GptOssLocalClient::new(*server.address()).unwrap();
        let result = client
            .prepare(&prompt, 128, TIMEOUT)
            .await
            .unwrap()
            .complete(TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.usage.known().is_some(), known, "{key}");
        assert!(result.final_text.is_none(), "{key}");
        if let Some(count) = expected_count {
            assert_eq!(result.usage.output_tokens, Some(count));
        }
    }
}

#[tokio::test]
async fn a_malformed_counter_preserves_the_other_dimension_and_eos_requires_return() {
    let prompt = GptOssPrompt::encode("Return JSON.", "Work evidence").unwrap();
    let mut without_return = final_answer("{}");
    without_return.pop();
    let server = MockServer::start().await;
    profile(&server, MODEL, 4096, PROBE_TOKENS).await;
    let mut reply = answer(&prompt, &without_return);
    reply["tokens_evaluated"] = json!("malformed");
    reply["tokens_predicted"] = json!(129);
    completion(&server, ResponseTemplate::new(200).set_body_json(reply)).await;
    let client = GptOssLocalClient::new(*server.address()).unwrap();
    let result = client
        .prepare(&prompt, 128, TIMEOUT)
        .await
        .unwrap()
        .complete(TIMEOUT)
        .await
        .unwrap();
    assert_eq!(result.usage.input_tokens, None);
    assert_eq!(result.usage.output_tokens, Some(129));
    assert!(result.usage.known().is_none());
    assert!(result.final_text.is_none());
    assert!(decode_final(&without_return).is_none());
}

#[tokio::test]
async fn failed_completion_never_redirects_or_retries() {
    let prompt = GptOssPrompt::encode("Return JSON.", "Work evidence").unwrap();
    for (response, timeout) in [
        (
            ResponseTemplate::new(307).insert_header("location", "/elsewhere"),
            TIMEOUT,
        ),
        (ResponseTemplate::new(503), TIMEOUT),
        (
            ResponseTemplate::new(200).set_body_string("x".repeat(RESPONSE_BYTES + 1)),
            TIMEOUT,
        ),
        (
            ResponseTemplate::new(200).set_delay(Duration::from_secs(2)),
            Duration::from_millis(200),
        ),
    ] {
        let server = MockServer::start().await;
        profile(&server, MODEL, 4096, PROBE_TOKENS).await;
        completion(&server, response).await;
        let client = GptOssLocalClient::new(*server.address()).unwrap();
        assert!(client
            .prepare(&prompt, 64, TIMEOUT)
            .await
            .unwrap()
            .complete(timeout)
            .await
            .is_err());
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[3].url.path(), "/completion");
    }
}
