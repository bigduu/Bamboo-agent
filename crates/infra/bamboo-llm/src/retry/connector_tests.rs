use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

const RECOVERY: &str = "Authentication Error, Error in connector: Error querying the database: FATAL: the database system is not yet accepting connections\nDETAIL: Consistent recovery state has not been yet reached.";

fn recovery_body() -> String {
    serde_json::json!({"type":"error","error":{"type":"auth_error","message":RECOVERY}}).to_string()
}

fn config(attempts: u32) -> RetryConfig {
    RetryConfig {
        max_attempts: attempts,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    }
}

struct ConnectorResponder {
    failures: usize,
    hits: Arc<AtomicUsize>,
}

impl Respond for ConnectorResponder {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        if self.hits.fetch_add(1, Ordering::SeqCst) < self.failures {
            ResponseTemplate::new(401).set_body_string(recovery_body())
        } else {
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string("data: original stream\n\n")
        }
    }
}

async fn run(
    server: &MockServer,
    config: &RetryConfig,
    budget: &InitialRequestBudget,
) -> Result<InitialResponse, LLMError> {
    let client = reqwest::Client::new();
    send_anthropic_with_retry(config, budget, || {
        client
            .post(server.uri())
            .json(&serde_json::json!({"messages":[{"role":"user","content":"keep context"}]}))
    })
    .await
}

#[test]
fn connector_classification_requires_the_exact_structured_recovery_error() {
    assert!(is_connector_recovery(
        StatusCode::UNAUTHORIZED,
        &recovery_body()
    ));
    for body in [
        "not JSON".to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":"Invalid API key"}}).to_string(),
        serde_json::json!({"error":{"type":"permission_error","message":RECOVERY}}).to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":format!("{RECOVERY} Invalid API key")}}).to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":"temporary upstream failure"}}).to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":"database password authentication failed"}}).to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":format!("{RECOVERY} {}", "x".repeat(CONNECTOR_BODY_LIMIT))}}).to_string(),
        serde_json::json!({"message":RECOVERY}).to_string(),
        serde_json::json!({"error":{"type":"auth_error","message":RECOVERY,"code":"invalid_api_key"}}).to_string(),
        serde_json::json!({"code":"permission_denied","error":{"type":"auth_error","message":RECOVERY}}).to_string(),
    ] {
        assert!(!is_connector_recovery(StatusCode::UNAUTHORIZED, &body), "{body:.100}");
    }
    for status in [
        StatusCode::FORBIDDEN,
        StatusCode::BAD_REQUEST,
        StatusCode::OK,
    ] {
        assert!(!is_connector_recovery(status, &recovery_body()));
    }
}

#[tokio::test]
async fn connector_recovery_retries_only_the_same_request_and_keeps_success_body_unread() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(ConnectorResponder {
            failures: 2,
            hits: hits.clone(),
        })
        .mount(&server)
        .await;
    let cfg = config(4);
    let budget = InitialRequestBudget::new(&cfg);
    let observer = Arc::new(InitialHttpRetryObserver::default());
    let response = observe_initial_http_retries(observer.clone(), run(&server, &cfg, &budget))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_response().text().await.unwrap(),
        "data: original stream\n\n"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    assert!(observer.request_budget_managed());
    assert_eq!(observer.last().unwrap().status, 401);
    let requests = server.received_requests().await.unwrap();
    assert!(requests
        .iter()
        .all(|r| r.body == requests[0].body && r.headers == requests[0].headers));
}

#[tokio::test]
async fn persistent_connector_failure_has_a_hard_bound_and_secret_free_terminal_error() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(ConnectorResponder {
            failures: 99,
            hits: hits.clone(),
        })
        .mount(&server)
        .await;
    let cfg = config(99);
    let error = run(&server, &cfg, &InitialRequestBudget::new(&cfg))
        .await
        .unwrap_err();
    assert!(matches!(error, LLMError::RequestFailed(_)));
    let message = error.to_string();
    assert!(
        message.contains("HTTP 401")
            && message.contains("temporarily recovering")
            && message.contains("4 request(s)")
    );
    assert!(!message.contains("Please check your API key") && !message.contains(RECOVERY));
    assert_eq!(hits.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn auth_denials_and_oversized_or_ambiguous_errors_are_never_retried() {
    for (status, body) in [
        (
            401,
            r#"{"error":{"type":"authentication_error","message":"invalid API key"}}"#.to_string(),
        ),
        (
            401,
            r#"{"error":{"type":"auth_error","message":"temporary gateway error"}}"#.to_string(),
        ),
        (401, "broken JSON".to_string()),
        (401, "x".repeat(CONNECTOR_BODY_LIMIT + 1)),
        (403, recovery_body()),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body.clone()))
            .mount(&server)
            .await;
        let cfg = config(4);
        let response = run(&server, &cfg, &InitialRequestBudget::new(&cfg))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        let text = response.text().await.unwrap();
        if body.len() <= CONNECTOR_BODY_LIMIT {
            assert_eq!(text, body);
        } else {
            assert!(text.contains("size limit"));
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn request_budget_is_shared_with_compatibility_fallbacks() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("unsupported reasoning"))
        .mount(&server)
        .await;
    let cfg = config(2);
    let budget = InitialRequestBudget::new(&cfg);
    for _ in 0..2 {
        assert_eq!(
            run(&server, &cfg, &budget)
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "unsupported reasoning"
        );
    }
    assert!(matches!(
        run(&server, &cfg, &budget).await,
        Err(LLMError::RequestFailed(_))
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn retry_after_seconds_and_dates_beyond_budget_stop_instead_of_retrying_early() {
    let date = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc2822();
    for hint in ["1".to_string(), date] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(401)
                    .insert_header("Retry-After", hint)
                    .set_body_string(recovery_body()),
            )
            .mount(&server)
            .await;
        let cfg = config(4);
        let mut budget = InitialRequestBudget::new(&cfg);
        budget.deadline = Instant::now() + Duration::from_millis(50);
        let started = Instant::now();
        assert!(matches!(
            run(&server, &cfg, &budget).await,
            Err(LLMError::RequestFailed(_))
        ));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}

#[test]
fn retry_after_date_and_numeric_parsing_preserves_valid_hints() {
    for (hint, expected) in [
        ("0", Some(Duration::ZERO)),
        ("2", Some(Duration::from_secs(2))),
        ("invalid", None),
    ] {
        let response: Response = http::Response::builder()
            .status(429)
            .header("Retry-After", hint)
            .body("")
            .unwrap()
            .into();
        assert_eq!(parse_retry_after_with_date(&response), expected);
    }
    let hint = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc2822();
    let response: Response = http::Response::builder()
        .header("Retry-After", hint)
        .body("")
        .unwrap()
        .into();
    let delay = parse_retry_after_with_date(&response).unwrap();
    assert!(delay > Duration::from_secs(28) && delay <= Duration::from_secs(30));
}

#[tokio::test]
async fn elapsed_request_budget_bounds_waiting_for_response_headers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
        .mount(&server)
        .await;
    let cfg = config(4);
    let mut budget = InitialRequestBudget::new(&cfg);
    budget.deadline = Instant::now() + Duration::from_millis(30);
    let started = Instant::now();
    assert!(matches!(
        run(&server, &cfg, &budget).await,
        Err(LLMError::RequestFailed(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn dropping_a_backoff_cancels_the_inline_request_without_later_attempts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("Retry-After", "5")
                .set_body_string(recovery_body()),
        )
        .mount(&server)
        .await;
    let cfg = config(4);
    let budget = InitialRequestBudget::new(&cfg);
    let observer = Arc::new(InitialHttpRetryObserver::default());
    let mut future = Box::pin(observe_initial_http_retries(
        observer.clone(),
        run(&server, &cfg, &budget),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut future => panic!("retry sleep must remain pending"),
            _ = async { while observer.last().is_none() { tokio::task::yield_now().await; } } => {}
        }
    })
    .await
    .unwrap();
    drop(future);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

async fn incomplete_error_server(status: u16) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let read = socket.read(&mut request).await.unwrap();
        assert!(
            read > 0,
            "a fixture request must arrive before its response"
        );
        socket.write_all(format!("HTTP/1.1 {status} Error\r\nContent-Length: 65536\r\nContent-Type: application/json\r\n\r\n{{").as_bytes()).await.unwrap();
        while socket.read(&mut request).await.unwrap_or(0) != 0 {}
    });
    (format!("http://{address}/"), server)
}

#[tokio::test]
async fn slow_auth_and_compatibility_error_bodies_share_the_request_deadline() {
    for status in [400, 401, 403] {
        let (url, server) = incomplete_error_server(status).await;
        let client = reqwest::Client::new();
        let cfg = config(4);
        let mut budget = InitialRequestBudget::new(&cfg);
        budget.deadline = Instant::now() + Duration::from_millis(40);
        let error = send_anthropic_with_retry(&cfg, &budget, || client.post(&url))
            .await
            .unwrap_err();
        assert!(matches!(error, LLMError::RequestFailed(_)));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn dropping_during_error_body_read_closes_the_pending_request() {
    let (url, server) = incomplete_error_server(401).await;
    let client = reqwest::Client::new();
    let cfg = config(4);
    let budget = InitialRequestBudget::new(&cfg);
    let mut future = Box::pin(send_anthropic_with_retry(&cfg, &budget, || {
        client.post(&url)
    }));
    tokio::select! {
        _ = &mut future => panic!("incomplete response must stay pending"),
        _ = tokio::time::sleep(Duration::from_millis(40)) => {}
    }
    drop(future);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn disabled_connector_retries_issue_exactly_one_request() {
    for attempts in [0, 1] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string(recovery_body()))
            .mount(&server)
            .await;
        let cfg = config(attempts);
        let error = run(&server, &cfg, &InitialRequestBudget::new(&cfg))
            .await
            .unwrap_err();
        assert!(matches!(error, LLMError::RequestFailed(_)));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
