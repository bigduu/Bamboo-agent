use super::*;
use bamboo_domain::{AdmissionCommit, LiveBootstrapProof};
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

struct FailThenOk {
    hits: Arc<AtomicUsize>,
    delay_first: bool,
    times: Arc<Mutex<Vec<Instant>>>,
}

impl Respond for FailThenOk {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        self.times.lock().unwrap().push(Instant::now());
        if self.hits.fetch_add(1, Ordering::SeqCst) == 0 {
            if self.delay_first {
                ResponseTemplate::new(200).set_delay(Duration::from_millis(250))
            } else {
                ResponseTemplate::new(503).insert_header("Retry-After", "1")
            }
        } else {
            ResponseTemplate::new(200).set_body_string("ok")
        }
    }
}

async fn run_real_managed_request(delay_first: bool) {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let times = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .respond_with(FailThenOk {
            hits: hits.clone(),
            delay_first,
            times: times.clone(),
        })
        .mount(&server)
        .await;
    let (observer, offer) = InitialHttpRetryObserver::with_live_bootstrap();
    let observed = observer.clone();
    let url = server.uri();
    let task = tokio::spawn(async move {
        let config = RetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        };
        let mut budget = InitialRequestBudget::new(&config);
        budget.attempt_timeout = Duration::from_millis(30);
        budget.deadline = Instant::now() + Duration::from_secs(3);
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        observe_initial_http_retries(observed, async {
            send_anthropic_with_retry(&config, &budget, || {
                client
                    .post(&url)
                    .json(&serde_json::json!({"synthetic":"request"}))
            })
            .await
        })
        .await
    });
    let offer = tokio::time::timeout(Duration::from_secs(2), offer)
        .await
        .unwrap()
        .expect("actual managed request must offer continuation");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(
        observer.terminal_diagnostic().is_none(),
        "waiting is not terminal"
    );
    let gate = offer.gate();
    let proof = LiveBootstrapProof::from_storage((), gate.clone());
    assert!(offer.matches_proof(&proof));
    assert!(matches!(
        gate.commit(|| offer.approve(proof)),
        Ok(AdmissionCommit::Committed(()))
    ));
    let result = tokio::time::timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    if !delay_first {
        let times = times.lock().unwrap();
        assert!(
            times[1].duration_since(times[0]) >= Duration::from_secs(1),
            "Retry-After was shortened"
        );
    }
}

#[tokio::test]
async fn actual_anthropic_response_headers_timeout_can_continue_same_request() {
    run_real_managed_request(true).await;
}

#[tokio::test]
async fn actual_anthropic_retry_after_waits_for_actor_gate_and_server_hint() {
    run_real_managed_request(false).await;
}
