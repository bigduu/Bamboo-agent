use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fast(attempts: u32) -> RetryConfig {
    RetryConfig {
        max_attempts: attempts,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    }
}

async fn anthropic(
    server: &MockServer,
    config: &RetryConfig,
    budget: &InitialRequestBudget,
) -> Result<InitialResponse, LLMError> {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    send_anthropic_with_retry(config, budget, || {
        client.post(server.uri()).body("synthetic retained context")
    })
    .await
}

#[tokio::test]
async fn configured_count_above_four_is_honored_by_the_real_http_consumer() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(if count.fetch_add(1, Ordering::SeqCst) < 7 {
                503
            } else {
                200
            })
        })
        .mount(&server)
        .await;
    let config = fast(9);
    let response = anthropic(&server, &config, &InitialRequestBudget::new(&config))
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(hits.load(Ordering::SeqCst), 8);
    let requests = server.received_requests().await.unwrap();
    assert!(requests.iter().all(|r| r.body == requests[0].body));
}

#[tokio::test]
async fn explicit_continuous_mode_outlives_count_and_elapsed_budget_without_replaying_a_stream() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            if count.fetch_add(1, Ordering::SeqCst) < 7 {
                ResponseTemplate::new(503)
            } else {
                ResponseTemplate::new(200).set_body_string("original successful stream")
            }
        })
        .mount(&server)
        .await;
    with_upstream_retry(UpstreamRetry::UntilCancelled, async {
        let config = fast(1);
        let mut budget = InitialRequestBudget::new(&config);
        budget.deadline = Instant::now();
        let response = anthropic(&server, &config, &budget).await.unwrap();
        assert_eq!(
            response.into_response().text().await.unwrap(),
            "original successful stream"
        );
    })
    .await;
    assert_eq!(hits.load(Ordering::SeqCst), 8);
    let config = fast(1);
    assert!(
        !InitialRequestBudget::new(&config).continuous,
        "opt-in must not leak to the next invocation"
    );
}

#[tokio::test]
async fn continuous_mode_still_stops_for_auth_parameters_permissions_and_other_terminal_statuses() {
    for status in [400, 401, 403, 404, 422] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status).set_body_string("terminal error"))
            .expect(1)
            .mount(&server)
            .await;
        with_upstream_retry(UpstreamRetry::UntilCancelled, async {
            let config = fast(1);
            let response = anthropic(&server, &config, &InitialRequestBudget::new(&config))
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
        })
        .await;
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn continuous_headers_timeout_retries_only_the_initial_send() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            if count.fetch_add(1, Ordering::SeqCst) < 2 {
                ResponseTemplate::new(200).set_delay(Duration::from_secs(2))
            } else {
                ResponseTemplate::new(200).set_body_string("successful unread body")
            }
        })
        .mount(&server)
        .await;
    with_upstream_retry(UpstreamRetry::UntilCancelled, async {
        let config = fast(1);
        let mut budget = InitialRequestBudget::new(&config);
        budget.attempt_timeout = Duration::from_millis(30);
        let response = anthropic(&server, &config, &budget).await.unwrap();
        assert_eq!(
            response.into_response().text().await.unwrap(),
            "successful unread body"
        );
    })
    .await;
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

async fn wait_for_retry(observer: &InitialHttpRetryObserver) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while observer.last().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mock response must reach backoff");
}

#[tokio::test]
async fn continuous_retry_after_seconds_and_http_date_are_not_shortened_and_backoff_is_cancellable()
{
    for hint in [
        "120".to_owned(),
        (chrono::Utc::now() + chrono::Duration::seconds(120))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string(),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", hint))
            .mount(&server)
            .await;
        let observer = Arc::new(InitialHttpRetryObserver::default());
        let observed = observer.clone();
        let url = server.uri();
        let task = tokio::spawn(async move {
            with_upstream_retry(
                UpstreamRetry::UntilCancelled,
                observe_initial_http_retries(observed, async {
                    let client = reqwest::Client::builder().no_proxy().build().unwrap();
                    send_provider_request(&fast(1), "synthetic", || client.post(&url)).await
                }),
            )
            .await
        });
        wait_for_retry(&observer).await;
        assert!(observer.last().unwrap().delay >= Duration::from_secs(118));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn continuous_inflight_send_is_cancellable_and_records_no_second_request() {
    let server = MockServer::start().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let notified = entered.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            notified.notify_one();
            ResponseTemplate::new(200).set_delay(Duration::from_secs(10))
        })
        .mount(&server)
        .await;
    let url = server.uri();
    let task = tokio::spawn(async move {
        with_upstream_retry(UpstreamRetry::UntilCancelled, async {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            send_provider_request(&fast(1), "synthetic", || client.post(&url)).await
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_unrepresentable_server_delay_stops_instead_of_panicking_or_retrying_early() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", u64::MAX.to_string()))
        .expect(1)
        .mount(&server)
        .await;
    with_upstream_retry(UpstreamRetry::UntilCancelled, async {
        let config = fast(1);
        let error = anthropic(&server, &config, &InitialRequestBudget::new(&config))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("too large to schedule safely"));
    })
    .await;
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn continuous_mode_cannot_spend_a_work_physical_reservation_twice() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    with_upstream_retry(
        UpstreamRetry::UntilCancelled,
        WorkPhysicalRequest::default().scope(async {
            let config = fast(20);
            let budget = InitialRequestBudget::new(&config);
            assert!(!budget.continuous);
            assert_eq!(budget.max_attempts, 1);
            let first = anthropic(&server, &config, &budget).await.unwrap_err();
            assert!(first.to_string().contains("attempt limit reached"));
            let second = anthropic(&server, &config, &InitialRequestBudget::new(&config))
                .await
                .unwrap_err();
            assert!(second
                .to_string()
                .contains("new Host reservation is required"));
        }),
    )
    .await;
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn no_http_connect_failures_continue_until_cancelled_but_invalid_request_stops() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let builds = Arc::new(AtomicUsize::new(0));
    let count = builds.clone();
    let task = tokio::spawn(async move {
        with_upstream_retry(UpstreamRetry::UntilCancelled, async {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            send_provider_request(&fast(1), "controlled-connect", || {
                if count.fetch_add(1, Ordering::SeqCst) >= 6 {
                    signal.notify_one();
                }
                client.post(&url)
            })
            .await
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(builds.load(Ordering::SeqCst) >= 7);
    let count = AtomicUsize::new(0);
    with_upstream_retry(UpstreamRetry::UntilCancelled, async {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let error = send_provider_request(&fast(1), "controlled-invalid", || {
            count.fetch_add(1, Ordering::SeqCst);
            client.post("not a URL")
        })
        .await
        .unwrap_err();
        assert!(matches!(error, LLMError::Http(_)));
    })
    .await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
