use super::ExecuteRequest;
use crate::app_state::{AgentRunner, AgentStatus};
use bamboo_domain::reasoning::ReasoningEffort;

#[test]
fn test_agent_status_running_blocks_restart() {
    // Test that Running status should block restart.
    let status = AgentStatus::Running;
    assert!(matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_completed_allows_restart() {
    // Test that Completed status should allow restart.
    let status = AgentStatus::Completed;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_error_allows_restart() {
    // Test that Error status should allow restart.
    let status = AgentStatus::Error("test error".to_string());
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_cancelled_allows_restart() {
    // Test that Cancelled status should allow restart.
    let status = AgentStatus::Cancelled;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_runner_creation() {
    // Test that runners can be created and have proper initial state.
    let runner = AgentRunner::new();
    assert!(matches!(runner.status, AgentStatus::Pending));
    // Verify cancel token exists (can be cloned).
    let _token_clone = runner.cancel_token.clone();
}

// ========== SESSION-DRIVEN EXECUTE REQUEST TESTS ==========
// These tests ensure the design principle:
// "execute is session-driven; request.model is only a compatibility fallback"

#[test]
fn execute_request_model_type_is_optional() {
    let json = r#"{
            "model": "kimi-for-coding"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    let _model_str: Option<&str> = request.model.as_deref();
    assert_eq!(request.model.as_deref(), Some("kimi-for-coding"));
}

#[test]
fn execute_request_allows_missing_model() {
    let json = r#"{}"#;
    let result: Result<ExecuteRequest, _> = serde_json::from_str(json);
    assert!(
        result.is_ok(),
        "ExecuteRequest should deserialize without model field"
    );
    assert!(result.expect("request should deserialize").model.is_none());
}

#[test]
fn execute_request_empty_model_normalizes_to_compat_absent() {
    let request = ExecuteRequest {
        model: Some("   ".to_string()),
        provider: None,
        model_ref: None,
        skill_mode: None,
        reasoning_effort: None,
        client_sync: None,
        no_human_approver: false,
        run_budget: None,
        upstream_retry: None,
        live_bootstrap_resume: false,
    };

    let model = request.model.as_deref().unwrap_or("").trim();
    assert!(
        model.is_empty(),
        "Empty compatibility model should normalize to absent"
    );
}

#[test]
fn execute_request_with_valid_model_succeeds() {
    let json = r#"{
            "model": "gpt-4o-mini"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    assert_eq!(request.model.as_deref(), Some("gpt-4o-mini"));
}

#[test]
fn execute_request_accepts_reasoning_effort() {
    let json = r#"{
            "model": "gpt-4o-mini",
            "reasoning_effort": "xhigh"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    assert_eq!(request.reasoning_effort, Some(ReasoningEffort::Xhigh));
}

#[test]
fn execute_request_rejects_invalid_reasoning_effort() {
    let json = r#"{
            "model": "gpt-4o-mini",
            "reasoning_effort": "extreme"
        }"#;

    let result: Result<ExecuteRequest, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

// Test-only snapshots at the actual Server -> Engine spawn adapter. The map
// retains nothing for ordinary tests/runs and is never present in production.
type InputSnapshot = Option<Vec<(String, Option<bamboo_domain::SessionSkillRequest>)>>;
fn input_taps() -> &'static std::sync::Mutex<std::collections::HashMap<String, Vec<InputSnapshot>>>
{
    static TAPS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, Vec<InputSnapshot>>>,
    > = std::sync::OnceLock::new();
    TAPS.get_or_init(std::sync::Mutex::default)
}
pub(super) fn observe_inputs(
    id: &str,
    inputs: Option<&bamboo_engine::config::UntrustedExecutionInputs>,
) {
    let mut taps = input_taps().lock().unwrap();
    if let Some(entries) = taps.get_mut(id) {
        entries.push(inputs.map(|inputs| {
            inputs
                .observations()
                .iter()
                .map(|item| (item.input_id().into(), item.request().cloned()))
                .collect()
        }));
    }
}

// Test-only one-shot gate at the real HTTP selection/admission boundary.
// There is no callback, wait or map in production builds.
type AdmissionSelectionGate = (
    tokio::sync::oneshot::Sender<bool>,
    tokio::sync::oneshot::Receiver<()>,
);
fn admission_selection_gates(
) -> &'static std::sync::Mutex<std::collections::HashMap<(String, bool), AdmissionSelectionGate>> {
    static GATES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, bool), AdmissionSelectionGate>>,
    > = std::sync::OnceLock::new();
    GATES.get_or_init(std::sync::Mutex::default)
}
pub(super) async fn after_root_admission_selection(id: &str, reserved: bool) {
    let gate = {
        admission_selection_gates()
            .lock()
            .unwrap()
            .remove(&(id.to_owned(), false))
    };
    if let Some((selected, continue_admission)) = gate {
        let _ = selected.send(reserved);
        let _ = continue_admission.await;
    }
}

// Reachable only on the repaired HTTP ownership path. Baseline counterfactual
// runs target the four early-selection races, never this secondary-only gate.
impl crate::AppState {
    pub(crate) async fn before_http_root_reservation_retry_for_test(&self, id: &str) {
        let gate = {
            admission_selection_gates()
                .lock()
                .unwrap()
                .remove(&(id.to_owned(), true))
        };
        if let Some((selected, continue_reservation)) = gate {
            let _ = selected.send(false);
            let _ = continue_reservation.await;
        }
    }
}

mod execution_input_http {
    use super::*;
    use actix_web::{http::StatusCode, test, web};
    use async_trait::async_trait;
    use bamboo_domain::{SessionInboxPort, SessionMessageEnvelope, SessionMessageId};
    use bamboo_llm::{
        LLMChunk, LLMError, LLMProvider, LLMStream, ProviderModelRouter, ProviderRegistry,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    include!("live_retry_vertical_fixture.rs");

    struct RetryHttpProvider {
        url: String,
    }
    #[async_trait]
    impl LLMProvider for RetryHttpProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let config = bamboo_llm::retry::RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(5),
            };
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let response =
                bamboo_llm::retry::send_provider_request(&config, "controlled-http", || {
                    client.post(&self.url).json(messages)
                })
                .await?;
            if !response.status().is_success() {
                return Err(LLMError::RequestFailed("controlled request failed".into()));
            }
            Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::Token("controlled HTTP complete".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[actix_web::test]
    async fn execute_http_retry_mode_reaches_the_real_provider_and_stop_retires_exact_run() {
        use std::sync::atomic::AtomicUsize;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        for stop in [false, true] {
            let server = MockServer::start().await;
            let count = Arc::new(AtomicUsize::new(0));
            let hits = count.clone();
            Mock::given(method("POST"))
                .respond_with(move |_: &wiremock::Request| {
                    let n = hits.fetch_add(1, Ordering::SeqCst);
                    if stop {
                        ResponseTemplate::new(429).insert_header("Retry-After", "120")
                    } else {
                        ResponseTemplate::new(if n < 3 { 503 } else { 200 })
                    }
                })
                .mount(&server)
                .await;
            let (_home, state, _fault) =
                state_with_provider(None, Arc::new(RetryHttpProvider { url: server.uri() })).await;
            let id = format!("retry-http-{}", uuid::Uuid::new_v4());
            let session = owned_http_session(&state, &id);
            state.storage.save_session(&session).await.unwrap();
            queue_owned_root(&state, &id).await;
            let mut events = state.get_session_event_sender(&id).await.subscribe();
            let app = test::init_service(
                actix_web::App::new()
                    .app_data(state.clone())
                    .configure(crate::routes::configure_routes),
            )
            .await;
            let response = test::call_service(&app, test::TestRequest::post().uri(&format!("/api/v1/execute/{id}")).set_json(serde_json::json!({"model":"test-model","upstream_retry":{"mode":"until_cancelled"}})).to_request()).await;
            assert!(response.status().is_success());
            let started: serde_json::Value = test::read_body_json(response).await;
            let run_id = started["run_id"]
                .as_str()
                .expect("Execute run id")
                .to_owned();
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    if stop && count.load(Ordering::SeqCst) >= 1 {
                        break;
                    }
                    if !stop
                        && state
                            .agent_runners
                            .read()
                            .await
                            .get(&id)
                            .is_some_and(|r| matches!(r.status, AgentStatus::Completed))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("controlled actual execute consumer");
            if stop {
                let duplicate = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&format!("/api/v1/execute/{id}"))
                        .set_json(serde_json::json!({"upstream_retry":{"mode":"bounded"}}))
                        .to_request(),
                )
                .await;
                assert_eq!(duplicate.status(), StatusCode::OK);
                let duplicate: serde_json::Value = test::read_body_json(duplicate).await;
                assert_eq!(duplicate["status"], "already_running");
                let stale = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&format!("/api/v1/stop/{id}"))
                        .insert_header(("Bamboo-Run-Id", "an-earlier-run"))
                        .to_request(),
                )
                .await;
                assert_eq!(stale.status(), StatusCode::CONFLICT);
                let runners = state.agent_runners.read().await;
                let current = runners.get(&id).unwrap();
                assert_eq!(current.run_id, run_id);
                assert!(!current.cancel_token.is_cancelled());
                drop(runners);
                let response = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&format!("/api/v1/stop/{id}"))
                        .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK);
                let stopped: serde_json::Value = test::read_body_json(response).await;
                assert_eq!(stopped["run_id"], run_id);
                assert!(state
                    .agent_runners
                    .read()
                    .await
                    .get(&id)
                    .unwrap()
                    .cancel_token
                    .is_cancelled());
            }
            wait_for_owned_terminal_with_cancellation(&state, &id, &run_id, &mut events, stop)
                .await;
            assert_eq!(count.load(Ordering::SeqCst), if stop { 1 } else { 4 });
            let cold = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(
                cold.last_run_status().as_deref(),
                Some(if stop { "cancelled" } else { "completed" })
            );
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|m| m.content == "one owned Root input")
                    .count(),
                1
            );
            tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
                .await
                .unwrap();
        }
    }

    #[actix_web::test]
    async fn exact_stop_http_validates_run_identity_and_never_cancels_a_successor_or_legacy_token()
    {
        use crate::app_state::AgentRunner;
        use actix_web::http::header::HeaderValue;
        use tokio_util::sync::CancellationToken;

        let (_home, state, _fault) = state(None).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for (canonical, child) in [(false, false), (true, false), (false, true), (true, true)] {
            let id = format!("exact-stop-http-{}", uuid::Uuid::new_v4());
            let session = if child {
                let parent = owned_http_session(&state, &format!("parent-{id}"));
                state.storage.save_session(&parent).await.unwrap();
                bamboo_agent_core::Session::new_child_of(
                    &id,
                    &parent,
                    "test-model",
                    "controlled-stop-child",
                )
            } else {
                owned_http_session(&state, &id)
            };
            state.storage.save_session(&session).await.unwrap();
            let uri = if canonical {
                format!("/api/v1/sessions/{id}/stop")
            } else {
                format!("/api/v1/stop/{id}")
            };
            let mut owner = AgentRunner::new();
            owner.status = AgentStatus::Running;
            let run_id = owner.run_id.clone();
            let token = owner.cancel_token.clone();
            state.agent_runners.write().await.insert(id.clone(), owner);

            for header in [
                HeaderValue::from_static(""),
                HeaderValue::from_static("invalid run"),
                HeaderValue::from_str(&"x".repeat(129)).unwrap(),
                HeaderValue::from_bytes(&[0xff]).unwrap(),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&uri)
                        .insert_header(("Bamboo-Run-Id", header))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                assert!(!token.is_cancelled());
            }
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&uri)
                    .append_header(("Bamboo-Run-Id", run_id.as_str()))
                    .append_header(("Bamboo-Run-Id", run_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(!token.is_cancelled());

            // An exact stop cannot fall through to the compatibility table.
            let legacy_guard = state.cancel_tokens.write().await;
            for _ in 0..2 {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    test::call_service(
                        &app,
                        test::TestRequest::post()
                            .uri(&uri)
                            .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                            .to_request(),
                    ),
                )
                .await
                .expect("exact Stop must not wait for legacy bookkeeping");
                assert_eq!(response.status(), StatusCode::OK);
                let stopped: serde_json::Value = test::read_body_json(response).await;
                assert_eq!(
                    stopped,
                    serde_json::json!({"success":true,"session_id":id,"run_id":run_id})
                );
                assert!(token.is_cancelled());
            }
            drop(legacy_guard);

            let mut successor = AgentRunner::new();
            successor.status = AgentStatus::Pending;
            let successor_id = successor.run_id.clone();
            let successor_token = successor.cancel_token.clone();
            state
                .agent_runners
                .write()
                .await
                .insert(id.clone(), successor);
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&uri)
                    .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert!(!successor_token.is_cancelled());
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&uri)
                    .insert_header(("Bamboo-Run-Id", successor_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(successor_token.is_cancelled());

            for status in [
                AgentStatus::Completed,
                AgentStatus::Error("controlled".into()),
            ] {
                let mut terminal = AgentRunner::new();
                terminal.status = status;
                let terminal_id = terminal.run_id.clone();
                let terminal_token = terminal.cancel_token.clone();
                state
                    .agent_runners
                    .write()
                    .await
                    .insert(id.clone(), terminal);
                let response = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&uri)
                        .insert_header(("Bamboo-Run-Id", terminal_id.as_str()))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::CONFLICT);
                assert!(!terminal_token.is_cancelled());
            }

            state.agent_runners.write().await.remove(&id);
            let legacy = CancellationToken::new();
            state
                .cancel_tokens
                .write()
                .await
                .insert(id.clone(), legacy.clone());
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&uri)
                    .insert_header(("Bamboo-Run-Id", successor_id.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert!(!legacy.is_cancelled());
            let response =
                test::call_service(&app, test::TestRequest::post().uri(&uri).to_request()).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(legacy.is_cancelled());
        }
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn stop_http_requires_supervisor_owner_for_exact_and_headerless_requests() {
        use crate::app_state::AgentRunner;
        let (_home, state, _fault) = state(None).await;
        let supervisor = state
            .storage
            .get_or_create_default_supervisor("test-model")
            .await
            .unwrap();
        let id = supervisor.session_id;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for canonical in [false, true] {
            let uri = if canonical {
                format!("/api/v1/sessions/{id}/stop")
            } else {
                format!("/api/v1/stop/{id}")
            };
            for exact in [false, true] {
                let mut runner = AgentRunner::new();
                runner.status = AgentStatus::Running;
                let run_id = runner.run_id.clone();
                let token = runner.cancel_token.clone();
                state.agent_runners.write().await.insert(id.clone(), runner);
                for remote in [true, false] {
                    let mut request = test::TestRequest::post().uri(&uri).peer_addr(
                        if remote {
                            "192.0.2.1:12345"
                        } else {
                            "127.0.0.1:12345"
                        }
                        .parse()
                        .unwrap(),
                    );
                    if exact {
                        request = request.insert_header(("Bamboo-Run-Id", run_id.as_str()));
                    }
                    let response = test::call_service(&app, request.to_request()).await;
                    assert_eq!(
                        response.status(),
                        if remote {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::OK
                        },
                        "Supervisor Stop requires its verified owner for either request form",
                    );
                    assert_eq!(token.is_cancelled(), !remote);
                    let runners = state.agent_runners.read().await;
                    let current = runners.get(&id).unwrap();
                    assert_eq!(current.run_id, run_id);
                    assert!(if remote {
                        matches!(current.status, AgentStatus::Running)
                    } else {
                        matches!(current.status, AgentStatus::Cancelled)
                    });
                }
            }
        }
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    struct LocalProvider;
    #[async_trait]
    impl LLMProvider for LocalProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            if tools.iter().any(|tool| tool.function.name == "skills_list")
                && !messages.iter().any(|message| {
                    message.tool_call_id.as_deref() == Some("execution-input-existing-load")
                })
            {
                let page = messages
                    .iter()
                    .rev()
                    .find(|m| m.tool_call_id.as_deref() == Some("execution-input-list"));
                let (id, name, arguments) = if let Some(page) = page {
                    let page: serde_json::Value = serde_json::from_str(&page.content).unwrap();
                    let package = page["skills"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|skill| skill["name"] == "review")
                        .unwrap()["package"]
                        .as_str()
                        .unwrap();
                    (
                        "execution-input-existing-load",
                        "skills_read",
                        serde_json::json!({"package":package,"resource":"SKILL.md"}),
                    )
                } else {
                    (
                        "execution-input-list",
                        "skills_list",
                        serde_json::json!({"limit":20}),
                    )
                };
                return Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::ToolCalls(vec![
                        bamboo_agent_core::tools::ToolCall {
                            id: id.into(),
                            tool_type: "function".into(),
                            function: bamboo_agent_core::tools::FunctionCall {
                                name: name.into(),
                                arguments: arguments.to_string(),
                            },
                        },
                    ])),
                    Ok(LLMChunk::Done),
                ])));
            }
            // The actual Chat selection keeps its existing legacy activation
            // contract: load the selected workflow before producing an answer.
            // I-E neither bypasses that gate nor creates a new consumer.
            if let Some(schema) = tools.iter().find(|tool| {
                tool.function.name == "load_skill" || tool.function.name.ends_with("::load_skill")
            }) {
                if !messages.iter().any(|message| {
                    message.tool_call_id.as_deref() == Some("execution-input-existing-load")
                }) {
                    let call = bamboo_agent_core::tools::ToolCall {
                        id: "execution-input-existing-load".into(),
                        tool_type: "function".into(),
                        function: bamboo_agent_core::tools::FunctionCall {
                            name: schema.function.name.clone(),
                            arguments: serde_json::json!({"skill_id":"review"}).to_string(),
                        },
                    };
                    return Ok(Box::pin(futures::stream::iter(vec![
                        Ok(LLMChunk::ToolCalls(vec![call])),
                        Ok(LLMChunk::Done),
                    ])));
                }
            }
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(LLMChunk::Token("finished".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }
    struct FailOnceAck {
        inner: Arc<dyn SessionInboxPort>,
        fail: Arc<AtomicBool>,
        after_receipt: bool,
    }
    #[async_trait]
    impl SessionInboxPort for FailOnceAck {
        async fn deliver(
            &self,
            e: &SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            self.inner.deliver(e).await
        }
        async fn existing_staged_delivery(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<Option<bamboo_domain::SessionInboxReceipt>, bamboo_domain::SessionInboxError>
        {
            self.inner.existing_staged_delivery(envelope).await
        }
        async fn pending_child_outcomes(
            &self,
            target: &str,
            child: &str,
        ) -> Result<Vec<SessionMessageEnvelope>, bamboo_domain::SessionInboxError> {
            self.inner.pending_child_outcomes(target, child).await
        }
        async fn mark_activation_eligible(
            &self,
            id: &str,
            g: u64,
            p: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner.mark_activation_eligible(id, g, p).await
        }
        async fn mark_message_activation_eligible(
            &self,
            id: &str,
            receipt: &bamboo_domain::SessionInboxReceipt,
            policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner
                .mark_message_activation_eligible(id, receipt, policy)
                .await
        }
        async fn claim(
            &self,
            id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            self.inner.claim(id, limit).await
        }
        async fn was_admitted(
            &self,
            id: &str,
            input: &SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            self.inner.was_admitted(id, input).await
        }
        async fn ack(
            &self,
            id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            if self.fail.swap(false, Ordering::SeqCst) {
                if self.after_receipt {
                    self.inner.ack(id, claim).await?;
                }
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "I-E injected ACK return failure".into(),
                ));
            }
            self.inner.ack(id, claim).await
        }
        async fn inspect(
            &self,
            id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(id).await
        }
    }
    async fn state(
        ack_failure: Option<bool>,
    ) -> (
        tempfile::TempDir,
        web::Data<crate::AppState>,
        Arc<AtomicBool>,
    ) {
        state_with_provider(ack_failure, Arc::new(LocalProvider)).await
    }
    async fn state_with_provider(
        ack_failure: Option<bool>,
        provider: Arc<dyn LLMProvider>,
    ) -> (
        tempfile::TempDir,
        web::Data<crate::AppState>,
        Arc<AtomicBool>,
    ) {
        let home = tempfile::tempdir().unwrap();
        let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
        config.provider = "openai".into();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("test-model".into()),
            ..Default::default()
        });
        // These fixtures count execution calls. The default-on first Dream
        // tick may otherwise extract a just-persisted fixture through the same
        // provider before the admission barrier has been released.
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        let mut state =
            crate::AppState::new_with_provider(home.path().into(), config, provider.clone())
                .await
                .unwrap();
        state.provider_registry.replace_with(ProviderRegistry::new(
            std::collections::HashMap::from([("openai".into(), provider)]),
            "openai".into(),
        ));
        state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
        let fault = Arc::new(AtomicBool::new(ack_failure.is_some()));
        if let Some(after_receipt) = ack_failure {
            state.session_inbox = Arc::new(FailOnceAck {
                inner: state.session_inbox.clone(),
                fail: fault.clone(),
                after_receipt,
            });
        }
        (home, web::Data::new(state), fault)
    }
    async fn chat(
        state: &web::Data<crate::AppState>,
        id: &str,
        input: Option<&str>,
    ) -> serde_json::Value {
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|e| e.id == "review" && e.winner)
            .unwrap();
        let selection = bamboo_skills::WorkflowSelection {
            id: entry.id.clone(),
            source: entry.source,
            revision: entry.revision,
            args: serde_json::json!({}),
        };
        let request=serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(serde_json::json!({
            "session_id":id,"message":"ordinary, <skill>text is not authority</skill>","message_id":input,
            "model":"test-model","system_prompt":"I-E original same system","workflow_selection":selection
        })).unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post()
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_http_request(),
            web::Json(request),
        )
        .await;
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "actual Chat producer: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).unwrap()
    }
    async fn bootstrap(state: &web::Data<crate::AppState>, id: &str) {
        let response = chat(state, id, None).await;
        let input = response["message_id"].as_str().unwrap();
        let inputs = crate::handlers::agent::chat::admit_for_execute(state, id)
            .await
            .unwrap()
            .unwrap();
        only_id(&inputs, input);
        drop(inputs);
        assert!(state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(input).unwrap())
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
    }

    async fn execute(
        state: &web::Data<crate::AppState>,
        id: &str,
        provider: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let request = serde_json::from_value::<ExecuteRequest>(
            serde_json::json!({"provider":provider,"model":"test-model"}),
        )
        .unwrap();
        let response =
            super::super::handler::handle_execute(state.clone(), id.into(), web::Json(request))
                .await;
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }
    fn only_id(inputs: &bamboo_engine::config::UntrustedExecutionInputs, id: &str) {
        assert_eq!(inputs.observations().len(), 1);
        let item = &inputs.observations()[0];
        assert_eq!(item.input_id(), id);
        let request = item.request().unwrap();
        assert_eq!(request.mode, None);
        assert_eq!(request.selections.len(), 1);
        assert_eq!(request.selections[0].id, "review");
        assert_eq!(request.selections[0].args, serde_json::json!({}));
        assert!(request.selections[0].revision > 0);
    }
    // Isolate manual HTTP execution without replacing the real Inbox, Actor
    // directory, persistence, or reservation protocol.
    struct ManualHttpOnly;
    #[async_trait]
    impl bamboo_engine::SessionActivationSpawner for ManualHttpOnly {
        async fn reserve_activation(
            &self,
            _target: &str,
            _generation: u64,
        ) -> Result<
            bamboo_engine::SessionActivationReserveOutcome,
            bamboo_domain::SessionActivationError,
        > {
            Err(bamboo_domain::SessionActivationError::Internal(
                "automatic launch unavailable in manual HTTP fixture".into(),
            ))
        }
    }

    // Full HTTP/Root fixture for the continuous-retry recovery contract. The
    // completed tool exchange is synthetic; the provider is an in-process stub.
    #[derive(Default)]
    struct ResumeBootstrapProvider {
        calls: std::sync::atomic::AtomicUsize,
        observed: std::sync::Mutex<Vec<Vec<bamboo_agent_core::Message>>>,
    }
    #[async_trait]
    impl LLMProvider for ResumeBootstrapProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.observed.lock().unwrap().push(messages.to_vec());
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(LLMError::RequestFailed(
                    "synthetic initial headers timeout".into(),
                ))
            } else {
                Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::Token("synthetic recovered reply".into())),
                    Ok(LLMChunk::Done),
                ])))
            }
        }
    }

    fn completed_synthetic_tool_exchange() -> Vec<bamboo_agent_core::Message> {
        use bamboo_agent_core::tools::{FunctionCall, ToolCall};
        vec![
            bamboo_agent_core::Message::user("one original user message"),
            bamboo_agent_core::Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "already-completed-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "synthetic_effect".into(),
                        arguments: "{}".into(),
                    },
                }]),
            ),
            bamboo_agent_core::Message::tool_result("already-completed-call", "already completed"),
        ]
    }

    #[actix_web::test]
    async fn continuous_resume_contract_failed_bootstrap_after_tool_result() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-after-bootstrap";
        let mut session = owned_http_session(&state, id);
        session.messages = completed_synthetic_tool_exchange();
        // Prime a real execute from the existing complete tool exchange. Its
        // provider will fail during bootstrap and consume this old marker.
        session
            .metadata
            .insert("retry_resume_pending".into(), "true".into());
        state.storage.save_session(&session).await.unwrap();
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = first.status();
        let first: serde_json::Value = test::read_body_json(first).await;
        assert_eq!(first_status, StatusCode::ACCEPTED, "first execute: {first}");
        assert_eq!(first["status"], "started");
        let first_run = first["run_id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let terminal = state
                    .agent_runners
                    .read()
                    .await
                    .get(id)
                    .is_some_and(|runner| {
                        runner.run_id == first_run && matches!(runner.status, AgentStatus::Error(_))
                    });
                let persisted = state.storage.load_session(id).await.unwrap();
                if terminal
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                    && persisted
                        .as_ref()
                        .is_some_and(|s| s.last_run_status().as_deref() == Some("error"))
                {
                    break persisted.unwrap();
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual synthetic bootstrap failure durably retired");
        let before = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(before.last_run_status().as_deref(), Some("error"));
        assert_eq!(
            before
                .agent_runtime_state
                .as_ref()
                .map(|runtime| runtime.status),
            Some(bamboo_domain::AgentStatusState::Failed)
        );
        assert!(!before.metadata.contains_key("retry_resume_pending"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let cached_before = state.load_session(id).await.expect("cached session");
        let cached_snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&cached_before);
        let durable_snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&before);
        let backlog = state
            .session_inbox
            .inspect(id)
            .await
            .expect("fixture inbox");
        let runner_observation = {
            let runners = state.agent_runners.read().await;
            runners.get(id).map(|runner| {
                (
                    format!("{:?}", runner.status),
                    runner.root_reset_admission_reserved(),
                )
            })
        };
        let runtime_observation = before.agent_runtime_state.as_ref().map(|runtime| {
            (
                format!("{:?}", runtime.status),
                runtime.waiting_for_children.is_some(),
                runtime.waiting_for_bash.is_some(),
            )
        });
        eprintln!("synthetic pre-retry: cached={cached_snapshot:?}, durable={durable_snapshot:?}, backlog={backlog:?}, runner={runner_observation:?}, runtime={runtime_observation:?}, queued={}, injected={}, suspended={}",
            before.metadata.contains_key("chat.queued_ingress.v1"),
            before.has_pending_injected_messages(),
            before.metadata.contains_key("runtime.suspend_reason"));
        {
            let observed = provider.observed.lock().unwrap();
            assert_eq!(observed.len(), 1);
            assert!(
                observed[0].iter().any(|message| {
                    message.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|call| call.id == "already-completed-call")
                    })
                }),
                "the first real provider dispatch must include the completed call"
            );
            assert!(
                observed[0].iter().any(|message| {
                    message.tool_call_id.as_deref() == Some("already-completed-call")
                }),
                "the first real provider dispatch must include its recorded result"
            );
        }
        assert_eq!(
            before.messages.last().unwrap().role,
            bamboo_agent_core::Role::Tool
        );
        assert_eq!(
            before
                .messages
                .iter()
                .filter(|m| m.content == "one original user message")
                .count(),
            1
        );
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        let after_request_calls = provider.calls.load(Ordering::SeqCst);
        eprintln!("continuous retry after synthetic bootstrap failure: HTTP {status}, body={body}, provider_calls_after={after_request_calls}");
        // Count before checking the exact error code so a protocol mismatch
        // cannot hide an unexpected provider dispatch.
        assert_eq!(after_request_calls, 1, "a refused retry must not dispatch");
        // A durable failure and a finished tool result are not by themselves
        // authorization to restart a turn. Refuse explicitly until a scoped,
        // non-destructive recovery contract prepares this exact history.
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "ambiguous retry must refuse: {body}"
        );
        assert_eq!(body["error"]["code"], "retry_resume_not_prepared");
        assert!(body.get("run_id").is_none());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(after.last_run_status().as_deref(), Some("error"));
        assert_eq!(
            serde_json::to_value(&before.messages).unwrap(),
            serde_json::to_value(&after.messages).unwrap(),
            "the previous messages and all their fields must be retained"
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.content == "one original user message")
                .count(),
            1
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("already-completed-call"))
                .count(),
            1
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[actix_web::test]
    async fn continuous_resume_contract_failed_bootstrap_user_tail_requires_receipt() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let hook_file = home.path().join("synthetic-session-start-count");
        state.config.write().await.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
            enabled: true,
            session_start: vec![bamboo_config::LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![bamboo_config::LifecycleHookHandler::command(
                    format!("printf s >> '{}'", hook_file.display()),
                    bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                )],
            }],
            ..Default::default()
        };
        let id = "continuous-resume-after-user-bootstrap";
        state
            .storage
            .save_session(&owned_http_session(&state, id))
            .await
            .unwrap();
        queue_owned_root(&state, id).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = response.status();
        let first: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(
            first_status,
            StatusCode::ACCEPTED,
            "first execution: {first}"
        );
        let first_run = first["run_id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let terminal = state.agent_runners.read().await.get(id).is_some_and(|r| {
                    r.run_id == first_run && matches!(r.status, AgentStatus::Error(_))
                });
                let actor_failed = bamboo_domain::ActorDirectoryPort::inspect_actor(
                    state.session_store.as_ref(),
                    id,
                )
                .await
                .ok()
                .and_then(|a| a.activation)
                .is_some_and(|a| {
                    a.run_id == first_run
                        && a.status == bamboo_domain::ActorActivationStatus::Failed
                });
                let saved = state.storage.load_session(id).await.unwrap();
                if terminal
                    && actor_failed
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                    && saved
                        .as_ref()
                        .is_some_and(|s| s.last_run_status().as_deref() == Some("error"))
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first synthetic bootstrap error durably retired");
        let before = state.storage.load_session(id).await.unwrap().unwrap();
        let user = before.messages.last().unwrap();
        assert_eq!(user.role, bamboo_agent_core::Role::User);
        assert_eq!(user.content, "one owned Root input");
        let user_id = user.id.clone();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let hook_before = std::fs::read_to_string(&hook_file).unwrap();
        assert_eq!(hook_before, "s", "first SessionStart really ran once");

        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if let Some(run) = body.get("run_id").and_then(|v| v.as_str()) {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let terminal = state.agent_runners.read().await.get(id).is_some_and(|r| {
                        r.run_id == run
                            && !matches!(r.status, AgentStatus::Pending | AgentStatus::Running)
                    });
                    if terminal
                        && state
                            .session_activation_router
                            .current_run_id(id)
                            .await
                            .is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("second synthetic execution retired");
        }
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        let after_calls = provider.calls.load(Ordering::SeqCst);
        let hook_after = std::fs::read_to_string(&hook_file).unwrap();
        eprintln!("user-tail retry: HTTP {status}, body={body}, provider_calls_before=1, provider_calls_after={after_calls}, session_start_before={hook_before:?}, session_start_after={hook_after:?}, user_count={}",
            after.messages.iter().filter(|m| m.id == user_id).count());
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        assert_eq!(
            after_calls, 1,
            "unprepared recovery must not dispatch provider"
        );
        assert_eq!(
            hook_after, hook_before,
            "unprepared recovery must not re-run SessionStart"
        );
        assert_eq!(status, StatusCode::CONFLICT, "unprepared recovery: {body}");
        assert_eq!(body["error"]["code"], "retry_resume_not_prepared");
        assert!(body.get("run_id").is_none());
        assert_eq!(
            serde_json::to_value(&before.messages).unwrap(),
            serde_json::to_value(&after.messages).unwrap()
        );
        assert_eq!(after.messages.iter().filter(|m| m.id == user_id).count(), 1);
    }

    #[actix_web::test]
    async fn continuous_resume_contract_new_user_after_failure_starts_once() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-fresh-user-after-bootstrap";
        state
            .storage
            .save_session(&owned_http_session(&state, id))
            .await
            .unwrap();
        queue_owned_root(&state, id).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = response.status();
        let first: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(first_status, StatusCode::ACCEPTED, "first run: {first}");
        let first_run = first["run_id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let failed = state.agent_runners.read().await.get(id).is_some_and(|r| {
                    r.run_id == first_run && matches!(r.status, AgentStatus::Error(_))
                });
                let actor_failed = bamboo_domain::ActorDirectoryPort::inspect_actor(
                    state.session_store.as_ref(),
                    id,
                )
                .await
                .ok()
                .and_then(|a| a.activation)
                .is_some_and(|a| {
                    a.run_id == first_run
                        && a.status == bamboo_domain::ActorActivationStatus::Failed
                });
                if failed
                    && actor_failed
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first failed run retires");
        let before = state.storage.load_session(id).await.unwrap().unwrap();
        let first_user = before.messages.last().unwrap().id.clone();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({"session_id":id,"message":"a genuinely new user input", "model":"test-model"})
        ).unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        let chat_status = response.status();
        let chat_body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            chat_status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&chat_body)
        );
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::ACCEPTED, "new input must start: {body}");
        let new_run = body["run_id"].as_str().unwrap().to_owned();
        assert_ne!(new_run, first_run);
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let completed = state.agent_runners.read().await.get(id).is_some_and(|r| {
                    r.run_id == new_run && matches!(r.status, AgentStatus::Completed)
                });
                let actor_done = bamboo_domain::ActorDirectoryPort::inspect_actor(
                    state.session_store.as_ref(),
                    id,
                )
                .await
                .ok()
                .and_then(|a| a.activation)
                .is_some_and(|a| {
                    a.run_id == new_run
                        && a.status == bamboo_domain::ActorActivationStatus::Succeeded
                });
                if completed
                    && actor_done
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("new user run completes");
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            after.messages.iter().filter(|m| m.id == first_user).count(),
            1
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.content == "a genuinely new user input")
                .count(),
            1
        );
        assert!(after
            .messages
            .iter()
            .any(|m| m.content == "synthetic recovered reply"));
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    // Draft consumer regressions for inclusion inside
    // crates/app/bamboo-server/src/handlers/agent/execute/tests.rs::execution_input_http.
    // Prepared against HEAD dce5f96bf36473636f16996de105e3f7c133aad0 and
    // git diff SHA-256 671549aca4a7602f38a0f14eeba62d48d2ea11cc3b760c8698ee3a0902b70234.
    // NOT compiled or run. Do not include these cases in acceptance counts until
    // their fixture preconditions and results are observed on the actual source.

    async fn scratch_wait_failed_root(state: &web::Data<crate::AppState>, id: &str, run: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let runner_failed =
                    state.agent_runners.read().await.get(id).is_some_and(|r| {
                        r.run_id == run && matches!(r.status, AgentStatus::Error(_))
                    });
                let actor_failed = bamboo_domain::ActorDirectoryPort::inspect_actor(
                    state.session_store.as_ref(),
                    id,
                )
                .await
                .ok()
                .and_then(|a| a.activation)
                .is_some_and(|a| {
                    a.run_id == run && a.status == bamboo_domain::ActorActivationStatus::Failed
                });
                let failed_session = state
                    .storage
                    .load_session(id)
                    .await
                    .unwrap()
                    .is_some_and(|s| s.last_run_status().as_deref() == Some("error"));
                if runner_failed
                    && actor_failed
                    && failed_session
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the actual failed Root must be saved and its exact owner retired");
    }

    // A new User created by the actual direct/legacy Chat route has a pending
    // handoff even though runtime.json can still contain the previous failure.
    // A Child exercises the existing direct append path without forging a Root
    // owner or bypassing the normal Root queued-input authority.
    #[actix_web::test]
    async fn scratch_retry_direct_fresh_user_survives_old_failed_runtime() {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let parent = "scratch-direct-parent";
        let id = "scratch-direct-child";
        state
            .storage
            .save_session(&owned_http_session(&state, parent))
            .await
            .unwrap();
        let mut child = bamboo_agent_core::Session::new_child(id, parent, "test-model", "Child");
        child.title_generated = true;
        child.add_message(bamboo_agent_core::Message::user("old failed input"));
        child.set_last_run_status("error");
        child.set_last_run_error("synthetic previous failure");
        let mut runtime = bamboo_domain::AgentRuntimeState::new("older-run");
        runtime.status = bamboo_domain::AgentStatusState::Failed;
        child.agent_runtime_state = Some(runtime);
        state.storage.save_session(&child).await.unwrap();
        assert!(!state
            .session_store
            .root_actor_input_required(&child)
            .await
            .unwrap());
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({"session_id":id,"message":"direct fresh user","model":"test-model"}),
        )
        .unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        let chat_status = response.status();
        let chat_body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            chat_status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&chat_body)
        );
        let prepared = state.storage.load_session(id).await.unwrap().unwrap();
        let fresh = prepared.messages.last().unwrap();
        assert_eq!(fresh.content, "direct fresh user");
        let fresh_id = fresh.id.clone();
        assert_eq!(prepared.last_run_status().as_deref(), Some("pending"));
        assert_eq!(
            prepared.agent_runtime_state.as_ref().map(|s| s.status),
            Some(bamboo_domain::AgentStatusState::Failed)
        );
        assert!(!state
            .session_inbox
            .inspect(id)
            .await
            .unwrap()
            .activation_pending());
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model","upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if status == StatusCode::ACCEPTED {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    if provider.messages.lock().unwrap().iter().any(|messages| {
                        messages
                            .iter()
                            .any(|m| m.id == fresh_id && m.content == "direct fresh user")
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("actual provider saw the exact new User");
        }
        let calls = provider.messages.lock().unwrap().len();
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        eprintln!(
            "direct fresh: HTTP {status}, body={body}, provider_calls={calls}, fresh_count={}",
            after.messages.iter().filter(|m| m.id == fresh_id).count()
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "a genuine direct fresh User must start: {body}"
        );
        assert_eq!(
            after.messages.iter().filter(|m| m.id == fresh_id).count(),
            1
        );
    }

    // A fresh owned input admitted before a client-sync mismatch is still pending
    // execution. A corrected execute must not require posting Chat again.
    #[actix_web::test]
    async fn scratch_retry_owned_fresh_user_after_sync_mismatch() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "scratch-owned-sync-retry";
        state
            .storage
            .save_session(&owned_http_session(&state, id))
            .await
            .unwrap();
        queue_owned_root(&state, id).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = response.status();
        let first: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(first_status, StatusCode::ACCEPTED, "first: {first}");
        let first_run = first["run_id"].as_str().unwrap().to_owned();
        scratch_wait_failed_root(&state, id, &first_run).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({"session_id":id,"message":"fresh after failure","model":"test-model"}),
        )
        .unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        let chat_status = response.status();
        let chat_body: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(chat_status, StatusCode::CREATED, "{chat_body}");
        let fresh_id = chat_body["message_id"].as_str().unwrap().to_owned();
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(
                    serde_json::json!({"model":"test-model","upstream_retry":{"mode":"until_cancelled"},
                "client_sync":{"client_message_count":0,"client_has_pending_question":false}}),
                )
                .to_request(),
        )
        .await;
        let mismatch_status = response.status();
        let mismatch: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(mismatch_status, StatusCode::OK, "{mismatch}");
        assert_eq!(mismatch["sync"]["need_sync"], true);
        assert!(mismatch.get("run_id").is_none(), "resync is not a started generation");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let pending_run = {
            let runners = state.agent_runners.read().await;
            let runner = runners.get(id).expect("fresh owner retained for sync");
            assert!(matches!(runner.status, AgentStatus::Pending));
            assert_ne!(runner.run_id, first_run);
            runner.run_id.clone()
        };
        let admitted = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            admitted
                .messages
                .iter()
                .filter(|m| m.id == fresh_id)
                .count(),
            1
        );
        assert!(state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(&fresh_id).unwrap())
            .await
            .unwrap());
        let snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(&admitted);
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(
                    serde_json::json!({"model":"test-model","upstream_retry":{"mode":"until_cancelled"},
                "client_sync":{"client_message_count":snapshot.message_count,
                    "client_last_message_id":snapshot.last_message_id,
                    "client_has_pending_question":snapshot.has_pending_question,
                    "client_pending_question_tool_call_id":snapshot.pending_question_tool_call_id}}),
                )
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if status == StatusCode::ACCEPTED {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                while provider.calls.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("corrected execute must reach provider");
        }
        let calls = provider.calls.load(Ordering::SeqCst);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        eprintln!(
            "sync retry: HTTP {status}, body={body}, provider_calls={calls}, fresh_count={}",
            after.messages.iter().filter(|m| m.id == fresh_id).count()
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "corrected sync must start the same admitted User: {body}"
        );
        assert_eq!(calls, 2);
        assert_eq!(body["run_id"], pending_run, "the retained owner starts once");
        assert_eq!(
            after.messages.iter().filter(|m| m.id == fresh_id).count(),
            1
        );
    }

    #[derive(Default)]
    struct ScratchFailThenChildProvider {
        calls: std::sync::atomic::AtomicUsize,
        observed: std::sync::Mutex<Vec<Vec<bamboo_agent_core::Message>>>,
    }

    #[async_trait]
    impl LLMProvider for ScratchFailThenChildProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.observed.lock().unwrap().push(messages.to_vec());
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(LLMError::RequestFailed(
                    "synthetic initial headers timeout".into(),
                ))
            } else {
                Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::Token("owned Root finished".into())),
                    Ok(LLMChunk::Done),
                ])))
            }
        }
    }

    // Actual child execution and completion coordinator supply the sole eligible
    // ChildOutcome. If a 409 is chosen instead of processing it, its ACK and
    // activation must remain recoverable; an acknowledged-but-unrun outcome fails.
    #[actix_web::test]
    async fn scratch_retry_failed_user_and_child_outcome_keep_work() {
        let provider = Arc::new(ScratchFailThenChildProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "scratch-failed-parent";
        let child_id = "scratch-failed-parent-child";
        state
            .storage
            .save_session(&owned_http_session(&state, id))
            .await
            .unwrap();
        let mut child = bamboo_agent_core::Session::new_child(child_id, id, "test-model", "Child");
        child.title_generated = true;
        child.add_message(bamboo_agent_core::Message::user(
            "real synthetic child work",
        ));
        state.storage.save_session(&child).await.unwrap();
        queue_owned_root(&state, id).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = response.status();
        let first: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(first_status, StatusCode::ACCEPTED, "first: {first}");
        let run = first["run_id"].as_str().unwrap().to_owned();
        scratch_wait_failed_root(&state, id, &run).await;
        let failed = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            failed.messages.last().unwrap().role,
            bamboo_agent_core::Role::User
        );
        let original_id = failed.messages.last().unwrap().id.clone();
        let (outcome, _) = complete_http_race_child(&state, id, child_id).await;
        let outcome_id = outcome.id.clone();
        let before = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(before.pending, 1);
        assert!(before.activation_pending());
        assert!(!state
            .session_inbox
            .was_admitted(id, &outcome_id)
            .await
            .unwrap());
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "one failed parent + one real child"
        );
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model","upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if status == StatusCode::ACCEPTED {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    if provider
                        .observed
                        .lock()
                        .unwrap()
                        .iter()
                        .skip(2)
                        .any(|messages| messages.iter().any(|m| m.id == outcome_id.as_str()))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the new owner must present the real outcome");
        }
        let admitted = state
            .session_inbox
            .was_admitted(id, &outcome_id)
            .await
            .unwrap();
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        let calls = provider.calls.load(Ordering::SeqCst);
        eprintln!("child outcome: HTTP {status}, body={body}, admitted={admitted}, backlog={backlog:?}, provider_calls={calls}");
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        assert_eq!(
            saved
                .messages
                .iter()
                .filter(|m| m.id == original_id)
                .count(),
            1
        );
        if status == StatusCode::ACCEPTED {
            assert!(admitted);
            assert_eq!(
                saved
                    .messages
                    .iter()
                    .filter(|m| m.id == outcome_id.as_str())
                    .count(),
                1
            );
            assert_eq!(calls, 3);
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {body}");
            assert!(
                !admitted,
                "a refused retry must not ACK an outcome it never executes"
            );
            assert!(
                backlog.activation_pending(),
                "a refused retry must preserve the activation"
            );
            assert_eq!(calls, 2);
        }
    }

    // A real new Root Chat that changes the system prompt is delivered as the
    // `root_chat_turn_v1` RuntimeInstruction, not a source=user/UserInput envelope.
    // Its exact fresh input and prompt must reach the actual provider once.
    #[actix_web::test]
    async fn scratch_retry_new_root_chat_with_changed_system_prompt_starts_once() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "scratch-root-chat-prompt-after-failure";
        let user_text = "synthetic fresh prompt-carrying user";
        let prompt = "SYNTHETIC SYSTEM PROMPT VERSION TWO";
        state
            .storage
            .save_session(&owned_http_session(&state, id))
            .await
            .unwrap();
        queue_owned_root(&state, id).await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let first_response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        let first_status = first_response.status();
        let first: serde_json::Value = test::read_body_json(first_response).await;
        assert_eq!(first_status, StatusCode::ACCEPTED, "first run: {first}");
        let first_run = first["run_id"].as_str().unwrap().to_owned();
        scratch_wait_failed_root(&state, id, &first_run).await;
        let failed = state.storage.load_session(id).await.unwrap().unwrap();
        let old_id = failed.messages.last().unwrap().id.clone();
        assert_eq!(
            failed.messages.last().unwrap().role,
            bamboo_agent_core::Role::User
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({"session_id":id,"message":user_text,"model":"test-model", "system_prompt":prompt}),
        )
        .unwrap();
        let chat_response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        let chat_status = chat_response.status();
        let chat_body: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(chat_response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(chat_status, StatusCode::CREATED, "new Chat: {chat_body}");
        let fresh_id = chat_body["message_id"].as_str().unwrap().to_owned();
        assert_ne!(fresh_id, old_id);
        assert!(state
            .session_inbox
            .inspect(id)
            .await
            .unwrap()
            .activation_pending());

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if status == StatusCode::ACCEPTED {
            let run = body["run_id"].as_str().expect("started run id");
            assert_ne!(run, first_run);
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let completed = state.agent_runners.read().await.get(id).is_some_and(|r| {
                        r.run_id == run && matches!(r.status, AgentStatus::Completed)
                    });
                    let actor_done = bamboo_domain::ActorDirectoryPort::inspect_actor(
                        state.session_store.as_ref(),
                        id,
                    )
                    .await
                    .ok()
                    .and_then(|a| a.activation)
                    .is_some_and(|a| {
                        a.run_id == run
                            && a.status == bamboo_domain::ActorActivationStatus::Succeeded
                    });
                    if completed
                        && actor_done
                        && state
                            .session_activation_router
                            .current_run_id(id)
                            .await
                            .is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the new prompt-bearing Root run must complete and retire");
        }

        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        let matching: Vec<_> = saved.messages.iter().filter(|m| m.id == fresh_id).collect();
        let envelope = matching
            .first()
            .and_then(|message| message.metadata.as_ref())
            .and_then(|metadata| metadata.get("session_message"))
            .cloned()
            .and_then(|value| serde_json::from_value::<SessionMessageEnvelope>(value).ok());
        let calls = provider.calls.load(Ordering::SeqCst);
        let (provider_user_count, provider_saw_system) = {
            let observed = provider.observed.lock().unwrap();
            let next = observed.get(1);
            (
                next.map_or(0, |messages| {
                    messages
                        .iter()
                        .filter(|m| m.id == fresh_id && m.content == user_text)
                        .count()
                }),
                next.is_some_and(|messages| {
                    messages.iter().any(|m| {
                        m.role == bamboo_agent_core::Role::System && m.content.contains(prompt)
                    })
                }),
            )
        };
        let admitted = state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(&fresh_id).unwrap())
            .await
            .unwrap();
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        eprintln!("new Root prompt: HTTP {status}, body={body}, admitted={admitted}, backlog={backlog:?}, provider_calls={calls}, fresh_in_provider={provider_user_count}, system_seen={provider_saw_system}, fresh_in_session={}", matching.len());
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();

        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "new prompt-bearing Chat must start: {body}"
        );
        assert!(admitted);
        assert_eq!(matching.len(), 1);
        let message = matching[0];
        let envelope = envelope.expect("checkpointed new input retains the typed envelope");
        assert_eq!(envelope.root_chat_prompt().unwrap(), Some(prompt));
        assert!(bamboo_domain::is_matching_session_message(
            message, &envelope
        ));
        assert_eq!(saved.messages.iter().filter(|m| m.id == old_id).count(), 1);
        assert_eq!(calls, 2);
        assert_eq!(provider_user_count, 1);
        assert!(provider_saw_system);
    }

    #[actix_web::test]
    async fn scratch_retry_unknown_user_tail_requires_proven_handoff() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "scratch-unknown-user-tail";
        let mut session = owned_http_session(&state, id);
        session.add_message(bamboo_agent_core::Message::user(
            "old unproven synthetic input",
        ));
        assert!(session.last_run_status().is_none());
        assert!(session.agent_runtime_state.is_none());
        state.storage.save_session(&session).await.unwrap();
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let mismatch = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"},
                "client_sync":{"client_message_count":0,"client_has_pending_question":false}}))
            .to_request()).await;
        let mismatch_status = mismatch.status();
        let mismatch_body: serde_json::Value = test::read_body_json(mismatch).await;
        assert_eq!(mismatch_status, StatusCode::OK, "{mismatch_body}");
        assert_eq!(mismatch_body["sync"]["need_sync"], true);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        if let Some(run) = body.get("run_id").and_then(|value| value.as_str()) {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let terminal = state
                        .agent_runners
                        .read()
                        .await
                        .get(id)
                        .is_some_and(|runner| {
                            runner.run_id == run
                                && !matches!(
                                    runner.status,
                                    AgentStatus::Pending | AgentStatus::Running
                                )
                        });
                    if terminal
                        && state
                            .session_activation_router
                            .current_run_id(id)
                            .await
                            .is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("unwanted unknown-tail run must retire before counting");
        }
        let calls = provider.calls.load(Ordering::SeqCst);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        eprintln!(
            "unknown user tail: HTTP {status}, body={body}, provider_calls={calls}, user_count={}",
            after
                .messages
                .iter()
                .filter(|message| message.content == "old unproven synthetic input")
                .count()
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "unknown handoff must refuse: {body}"
        );
        assert_eq!(calls, 0, "unproven history cannot dispatch");
        assert!(body.get("run_id").is_none());
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|message| message.content == "old unproven synthetic input")
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn continuous_resume_contract_completed_answer_stays_completed() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-already-completed";
        let mut session = owned_http_session(&state, id);
        session
            .messages
            .push(bamboo_agent_core::Message::user("already answered"));
        session
            .messages
            .push(bamboo_agent_core::Message::assistant("finished", None));
        session.set_last_run_status("completed");
        state.storage.save_session(&session).await.unwrap();
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        eprintln!("continuous retry on already completed answer: HTTP {status}, body={body}");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "completed");
        assert!(body.get("run_id").is_none());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(after.messages.len(), 2);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_unresolved_and_interrupted_history_refused() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for kind in ["unresolved-tool", "interrupted-assistant"] {
            let id = format!("continuous-resume-{kind}");
            let mut session = owned_http_session(&state, &id);
            session.messages = completed_synthetic_tool_exchange();
            session.messages.pop();
            if kind == "interrupted-assistant" {
                session.messages.pop();
                let mut partial = bamboo_agent_core::Message::assistant("partial output", None);
                partial.metadata = Some(serde_json::json!({
                    "runtime_kind":"interrupted_assistant_output", "interrupted":true
                }));
                session.messages.push(partial);
            }
            session.set_last_run_status("error");
            session.set_last_run_error("synthetic ambiguous error");
            let original = serde_json::to_value(&session.messages).unwrap();
            state.storage.save_session(&session).await.unwrap();
            let response = test::call_service(&app, test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()).await;
            let status = response.status();
            let body: serde_json::Value = test::read_body_json(response).await;
            assert_eq!(status, StatusCode::CONFLICT, "{kind}: {body}");
            assert_eq!(body["error"]["code"], "retry_resume_not_prepared");
            assert!(body.get("run_id").is_none());
            let after = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(&after.messages).unwrap(),
                original,
                "{kind}"
            );
            assert_eq!(after.last_run_status().as_deref(), Some("error"));
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_cache_and_durable_disagreement_refused() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-stale-cache";
        let mut cached = owned_http_session(&state, id);
        cached
            .messages
            .push(bamboo_agent_core::Message::user("old question"));
        cached
            .messages
            .push(bamboo_agent_core::Message::assistant("old answer", None));
        cached.set_last_run_status("completed");
        state.save_and_cache_session(&mut cached).await;
        let mut durable = state.storage.load_session(id).await.unwrap().unwrap();
        durable
            .messages
            .push(bamboo_agent_core::Message::user("new durable input"));
        durable.set_last_run_status("pending");
        state.storage.save_session(&durable).await.unwrap();
        let cached_snapshot =
            bamboo_engine::session_app::types::ServerExecuteSnapshot::from_session(
                &state.load_session(id).await.unwrap(),
            );
        assert_eq!(cached_snapshot.message_count, 2);
        assert_eq!(
            state
                .storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .len(),
            3
        );
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({
                "model":"test-model",
                "upstream_retry":{"mode":"until_cancelled"},
                "client_sync":{
                    "client_message_count":cached_snapshot.message_count,
                    "client_last_message_id":cached_snapshot.last_message_id,
                    "client_has_pending_question":cached_snapshot.has_pending_question,
                    "client_pending_question_tool_call_id":cached_snapshot.pending_question_tool_call_id
                }
            }))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "retry_state_changed");
        assert!(body.get("run_id").is_none());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&after.messages).unwrap(),
            serde_json::to_value(&durable.messages).unwrap()
        );
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_existing_tool_tail_run_returns_its_exact_identity() {
        let provider = Arc::new(RunningProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            messages: std::sync::Mutex::new(Vec::new()),
        });
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-running-tool-tail";
        let mut session = owned_http_session(&state, id);
        session.messages = completed_synthetic_tool_exchange();
        session
            .metadata
            .insert("retry_resume_pending".into(), "true".into());
        state.storage.save_session(&session).await.unwrap();
        let mut events = state.get_session_event_sender(id).await.subscribe();
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model"}))
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first: serde_json::Value = test::read_body_json(first).await;
        let run_id = first["run_id"].as_str().unwrap().to_owned();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            provider.started.acquire(),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
        assert_eq!(
            state
                .session_activation_router
                .current_run_id(id)
                .await
                .as_deref(),
            Some(run_id.as_str())
        );
        let running = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            running.messages.last().unwrap().role,
            bamboo_agent_core::Role::Tool
        );
        assert!(!running.metadata.contains_key("retry_resume_pending"));
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "already_running");
        assert_eq!(body["run_id"], run_id);
        assert_eq!(body["sync"]["has_pending_user_message"], false);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let stopped = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/stop/{id}"))
                .insert_header(("Bamboo-Run-Id", run_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(stopped.status(), StatusCode::OK);
        wait_for_owned_terminal_with_cancellation(&state, id, &run_id, &mut events, true).await;
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_missing_session_and_legacy_status_fail_closed() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for missing in [false, true] {
            let id = if missing {
                "continuous-resume-deleted"
            } else {
                "continuous-resume-legacy-status"
            };
            let mut session = owned_http_session(&state, id);
            session
                .messages
                .push(bamboo_agent_core::Message::user("old question"));
            session
                .messages
                .push(bamboo_agent_core::Message::assistant("old answer", None));
            state.save_and_cache_session(&mut session).await;
            if missing {
                assert!(state.storage.delete_session(id).await.unwrap());
            }
            let response = test::call_service(&app, test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()).await;
            let status = response.status();
            let body: serde_json::Value = test::read_body_json(response).await;
            if missing {
                assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            } else {
                assert_eq!(status, StatusCode::CONFLICT, "{body}");
                assert_eq!(body["error"]["code"], "retry_resume_not_prepared");
            }
            assert!(body.get("run_id").is_none());
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_staged_inbox_without_activation_keeps_completion() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-staged-no-activation";
        let mut session = owned_http_session(&state, id);
        session
            .messages
            .push(bamboo_agent_core::Message::user("old question"));
        session
            .messages
            .push(bamboo_agent_core::Message::assistant("old answer", None));
        session.set_last_run_status("completed");
        state.storage.save_session(&session).await.unwrap();
        state
            .session_inbox
            .deliver(&SessionMessageEnvelope::user_input(id, "staged"))
            .await
            .unwrap();
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert!(backlog.pending > 0);
        assert!(!backlog.activation_pending());
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{id}"))
            .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
            .to_request()).await;
        let status = response.status();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "completed");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_default_and_bounded_responses_unchanged() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "continuous-resume-bounded-control";
        let mut session = owned_http_session(&state, id);
        session.messages = completed_synthetic_tool_exchange();
        session.set_last_run_status("error");
        session.set_last_run_error("synthetic previous error");
        let original = serde_json::to_value(&session.messages).unwrap();
        state.storage.save_session(&session).await.unwrap();
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for body in [
            serde_json::json!({"model":"test-model"}),
            serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"bounded"}}),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/execute/{id}"))
                    .set_json(body)
                    .to_request(),
            )
            .await;
            let status = response.status();
            let body: serde_json::Value = test::read_body_json(response).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["status"], "completed");
            assert!(body.get("run_id").is_none());
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(&after.messages).unwrap(), original);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn continuous_resume_contract_pending_slot_and_suspended_runtime_refused() {
        let provider = Arc::new(ResumeBootstrapProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let app = test::init_service(
            actix_web::App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for kind in [
            "pending-slot",
            "suspended-runtime",
            "completed-runtime-failed",
            "completed-runtime-cancelled",
            "completed-runner-failed",
            "completed-runner-cancelled",
        ] {
            let id = format!("continuous-resume-{kind}");
            let mut session = owned_http_session(&state, &id);
            session
                .messages
                .push(bamboo_agent_core::Message::user("old question"));
            session
                .messages
                .push(bamboo_agent_core::Message::assistant("old answer", None));
            session.set_last_run_status("completed");
            if kind.starts_with("suspended-") || kind.starts_with("completed-runtime-") {
                let mut runtime = bamboo_domain::AgentRuntimeState::new(&id);
                runtime.status = match kind {
                    "suspended-runtime" => bamboo_domain::AgentStatusState::Suspended,
                    "completed-runtime-failed" => bamboo_domain::AgentStatusState::Failed,
                    "completed-runtime-cancelled" => bamboo_domain::AgentStatusState::Cancelled,
                    _ => unreachable!(),
                };
                session.agent_runtime_state = Some(runtime);
            }
            state.storage.save_session(&session).await.unwrap();
            if kind == "pending-slot" || kind.starts_with("completed-runner-") {
                let mut runner = AgentRunner::new();
                runner.status = match kind {
                    "completed-runner-failed" => AgentStatus::Error("synthetic".into()),
                    "completed-runner-cancelled" => AgentStatus::Cancelled,
                    _ => AgentStatus::Pending,
                };
                state.agent_runners.write().await.insert(id.clone(), runner);
            }
            let response = test::call_service(&app, test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({"model":"test-model", "upstream_retry":{"mode":"until_cancelled"}}))
                .to_request()).await;
            let status = response.status();
            let body: serde_json::Value = test::read_body_json(response).await;
            assert_eq!(status, StatusCode::CONFLICT, "{kind}: {body}");
            assert_eq!(body["error"]["code"], "retry_state_changed");
            assert!(
                body.get("run_id").is_none(),
                "{kind} must not invent an executable run"
            );
            if kind == "pending-slot" || kind.starts_with("completed-runner-") {
                state.agent_runners.write().await.remove(&id);
            }
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .unwrap();
    }

    #[derive(Default)]
    struct OwnedRootProvider {
        messages: std::sync::Mutex<Vec<Vec<bamboo_agent_core::Message>>>,
    }
    #[async_trait]
    impl LLMProvider for OwnedRootProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.messages.lock().unwrap().push(messages.to_vec());
            Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::Token("owned Root finished".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    fn owned_http_session(
        state: &web::Data<crate::AppState>,
        id: &str,
    ) -> bamboo_agent_core::Session {
        // Establish this disposable fixture's canonical scope before its real
        // Root is reserved; a same-scope Chat must not introduce a workspace.
        let workspace = state.app_data_dir.join("owned-http-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let mut session = bamboo_agent_core::Session::new(id, "test-model");
        session.title_generated = true;
        session.set_workspace_path_meta(workspace.to_string_lossy());
        session.metadata.insert(
            bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
            "explicit".into(),
        );
        session
    }

    async fn reserve_owned_root(
        state: &web::Data<crate::AppState>,
        id: &str,
    ) -> bamboo_engine::execution::SessionExecutionReservation {
        use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
        let sender = state.get_session_event_sender(id).await;
        let mut reservation = match reserve_session_execution(
            &state.agent,
            &state.agent_runners,
            &state.session_event_senders,
            id,
            &sender,
        )
        .await
        {
            SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
            _ => panic!("fixture must reserve its real idle Root"),
        };
        let session = state.storage.load_session(id).await.unwrap().unwrap();
        reservation
            .bind_root_actor(&state.agent, &session)
            .await
            .unwrap();
        assert!(reservation.root_actor_writer().is_some());
        assert!(reservation.execution_persistence().is_some());
        reservation
    }

    async fn queue_owned_root(state: &web::Data<crate::AppState>, id: &str) {
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({"session_id":id,"message":"one owned Root input", "model":"test-model"}),
        ).unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let session = state.storage.load_session(id).await.unwrap().unwrap();
        // Prepare only this Chat-published fixture workspace through the real
        // SkillManager. Keep the original 15-second completion deadline.
        state
            .skill_manager
            .store_for_workspace(
                session
                    .workspace_path_meta()
                    .as_deref()
                    .map(std::path::Path::new),
            )
            .await
            .unwrap();
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        assert!(state
            .session_inbox
            .inspect(id)
            .await
            .unwrap()
            .activation_pending());
        assert!(!session
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
    }

    // Own every session-keyed test tap so a timeout/panic cannot contaminate
    // another test. The gate itself is removed before any await.
    struct SelectionProbe(String);
    impl Drop for SelectionProbe {
        fn drop(&mut self) {
            admission_selection_gates()
                .lock()
                .unwrap()
                .retain(|(id, _), _| id != &self.0);
            input_taps().lock().unwrap().remove(&self.0);
        }
    }
    fn selection_probe(
        id: &str,
    ) -> (
        SelectionProbe,
        tokio::sync::oneshot::Receiver<bool>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (selected_tx, selected_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        assert!(admission_selection_gates()
            .lock()
            .unwrap()
            .insert((id.into(), false), (selected_tx, continue_rx))
            .is_none());
        assert!(input_taps()
            .lock()
            .unwrap()
            .insert(id.into(), Vec::new())
            .is_none());
        (SelectionProbe(id.into()), selected_rx, continue_tx)
    }

    fn checked_selection_probe(
        id: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<bool>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (selected_tx, selected_rx) = tokio::sync::oneshot::channel();
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        assert!(admission_selection_gates()
            .lock()
            .unwrap()
            .insert((id.into(), true), (selected_tx, continue_rx))
            .is_none());
        (selected_rx, continue_tx)
    }

    async fn shutdown_rejected_race(state: &web::Data<crate::AppState>, ids: &[&str]) {
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .expect("rejected HTTP race fixture shutdown");
        for id in ids {
            assert!(
                !state
                    .agent_runners
                    .read()
                    .await
                    .get(*id)
                    .is_some_and(|runner| matches!(
                        runner.status,
                        AgentStatus::Pending | AgentStatus::Running
                    )),
                "rejected fixture must leave no live runner for {id}"
            );
            assert!(
                state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_none(),
                "rejected fixture must leave no router owner for {id}"
            );
        }
    }

    async fn wait_for_owned_terminal(
        state: &web::Data<crate::AppState>,
        id: &str,
        run: &str,
        events: &mut tokio::sync::broadcast::Receiver<bamboo_agent_core::AgentEvent>,
    ) {
        wait_for_owned_terminal_with_cancellation(state, id, run, events, false).await;
    }

    async fn wait_for_owned_terminal_with_cancellation(
        state: &web::Data<crate::AppState>,
        id: &str,
        run: &str,
        events: &mut tokio::sync::broadcast::Receiver<bamboo_agent_core::AgentEvent>,
        cancelled: bool,
    ) {
        let expected_actor = if cancelled {
            bamboo_domain::ActorActivationStatus::Cancelled
        } else {
            bamboo_domain::ActorActivationStatus::Succeeded
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if matches!(events.recv().await.unwrap(),
                    bamboo_agent_core::AgentEvent::SessionHistoryCommitted { session_id } if session_id == id)
                {
                    break;
                }
            }
            // The durable publication barrier intentionally precedes exact
            // runner/Actor retirement. Wait for that real terminal state too.
            loop {
                let terminal = {
                    let runners = state.agent_runners.read().await;
                    let runner = runners.get(id).unwrap();
                    assert_eq!(runner.run_id, run);
                    if matches!(runner.status, AgentStatus::Pending | AgentStatus::Running) {
                        false
                    } else {
                        assert!(if cancelled {
                            matches!(runner.status, AgentStatus::Cancelled)
                        } else {
                            matches!(runner.status, AgentStatus::Completed)
                        }, "{:?}", runner.status);
                        true
                    }
                };
                let actor = bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                    .await.unwrap().activation.unwrap();
                assert_eq!(actor.run_id, run);
                if terminal && actor.status == expected_actor
                    && state.session_activation_router.current_run_id(id).await.is_none()
                {
                    assert!(actor.finished_at.is_some());
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("owned Root durable publication and exact retirement within 15 seconds");
    }

    async fn run_owned_http_selection_race(initially_running: bool) {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = if initially_running {
            "owned-http-owner-retires-after-selection"
        } else {
            "owned-http-input-arrives-after-selection"
        };
        let mut session = owned_http_session(&state, id);
        state.storage.save_session(&session).await.unwrap();
        let mut owner = Some(reserve_owned_root(&state, id).await);
        let reservation = owner.as_ref().unwrap();
        let initial_run = reservation.run_id().to_owned();
        let upgrade = reservation
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(
                &mut session,
                state.session_inbox.clone(),
                Some(&initial_run),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(upgrade.merged, 0);
        assert!(upgrade.admission_error.is_none());
        let legacy = state.session_inbox.claim(id, 1).await.unwrap_err();
        assert!(
            legacy
                .to_string()
                .contains("owned Inbox claim API required"),
            "{legacy}"
        );
        if initially_running {
            // The first selector must see this exact bound owner as Busy.
            queue_owned_root(&state, id).await;
        } else {
            owner.take().unwrap().abandon().await;
            assert!(!state
                .session_inbox
                .inspect(id)
                .await
                .unwrap()
                .activation_pending());
        }
        let (_probe, selected_rx, continue_tx) = selection_probe(id);
        let mut events = state.get_session_event_sender(id).await.subscribe();
        let http = super::super::handler::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Path::from(id.to_owned()),
            web::Json(serde_json::from_value(serde_json::json!({"model":"test-model"})).unwrap()),
        );
        let transition = async {
            assert!(
                !selected_rx.await.unwrap(),
                "early selector must return None"
            );
            if initially_running {
                owner.take().unwrap().abandon().await;
            } else {
                // Real Chat publication happens only after the empty inspect.
                queue_owned_root(&state, id).await;
            }
            assert!(state
                .session_activation_router
                .current_run_id(id)
                .await
                .is_none());
            assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
            assert!(provider.messages.lock().unwrap().is_empty());
            continue_tx.send(()).unwrap();
        };
        let (response, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(http, transition)
        })
        .await
        .expect("oneshot gates establish the interleaving without sleeps");
        let status = response.status();
        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert!(
                String::from_utf8_lossy(&bytes).contains("SessionInbox claim unresolved"),
                "{body}"
            );
            assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
            assert!(provider.messages.lock().unwrap().is_empty());
        }
        if status != StatusCode::ACCEPTED {
            shutdown_rejected_race(&state, &[id]).await;
        }
        // This same assertion is the natural counterfactual RED on d21.
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "actual HTTP selection race: {body}"
        );
        let run = body["run_id"].as_str().unwrap();
        assert_ne!(run, initial_run);
        wait_for_owned_terminal(&state, id, run, &mut events).await;
        let stored = state.storage.load_session(id).await.unwrap().unwrap();
        let users: Vec<_> = stored
            .messages
            .iter()
            .filter(|m| m.role == bamboo_agent_core::Role::User)
            .collect();
        assert_eq!(users.len(), 1, "one durable typed User checkpoint");
        let input = &users[0].id;
        assert_eq!(users[0].content, "one owned Root input");
        let proof = &users[0].metadata.as_ref().unwrap()["session_message"];
        let envelope: SessionMessageEnvelope = serde_json::from_value(proof.clone()).unwrap();
        assert!(bamboo_domain::is_matching_session_message(
            users[0], &envelope
        ));
        assert_eq!(envelope.target_session_id, id);
        assert!(state
            .session_inbox
            .was_admitted(id, &envelope.id)
            .await
            .unwrap());
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        let taps = input_taps().lock().unwrap().remove(id).unwrap();
        assert_eq!(taps.len(), 1, "one actual Ready/spawn handoff");
        assert_eq!(taps[0].as_ref().unwrap().len(), 1);
        assert_eq!(taps[0].as_ref().unwrap()[0].0, *input);
        let calls = provider.messages.lock().unwrap();
        assert_eq!(calls.len(), 1, "one execution provider call");
        assert_eq!(calls[0].iter().filter(|m| m.id == *input).count(), 1);
        drop(calls);
        let actor =
            bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                .await
                .unwrap()
                .activation
                .unwrap();
        assert_eq!(actor.run_id, run);
        assert_eq!(
            actor.attempt, 2,
            "one upgrade and exactly one successor; no Ready re-reserve"
        );
        assert_eq!(stored.last_run_status().as_deref(), Some("completed"));
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .expect("HTTP race fixture shutdown");
    }

    #[actix_web::test]
    async fn owned_root_http_retired_owner_after_selection_stays_owned() {
        run_owned_http_selection_race(true).await;
    }

    #[actix_web::test]
    async fn owned_root_http_late_input_after_selection_stays_owned() {
        run_owned_http_selection_race(false).await;
    }

    #[actix_web::test]
    async fn owned_root_http_empty_selection_without_transition_does_not_acquire() {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "owned-http-no-selection-transition";
        let mut session = bamboo_agent_core::Session::new(id, "test-model");
        session.title_generated = true;
        state.storage.save_session(&session).await.unwrap();
        let owner = reserve_owned_root(&state, id).await;
        let initial_run = owner.run_id().to_owned();
        owner
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(
                &mut session,
                state.session_inbox.clone(),
                Some(&initial_run),
            )
            .await
            .unwrap()
            .unwrap();
        owner.abandon().await;
        let (_probe, selected_rx, continue_tx) = selection_probe(id);
        let http = execute(&state, id, None);
        let transition = async {
            assert!(!selected_rx.await.unwrap());
            assert!(!state
                .session_inbox
                .inspect(id)
                .await
                .unwrap()
                .activation_pending());
            continue_tx.send(()).unwrap();
        };
        let ((status, body), ()) =
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                tokio::join!(http, transition)
            })
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "completed");
        let actor =
            bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                .await
                .unwrap()
                .activation
                .unwrap();
        assert_eq!(actor.run_id, initial_run);
        assert_eq!(actor.attempt, 1);
        assert!(provider.messages.lock().unwrap().is_empty());
        assert!(input_taps().lock().unwrap().get(id).unwrap().is_empty());
        tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
            .await
            .expect("HTTP race fixture shutdown");
    }

    // Produce a genuine terminal source through the real child HTTP runner.
    // Its installed production completion coordinator publishes the only Root
    // input. No User-string stand-in, raw fence, receipt or source is fabricated.
    async fn complete_http_race_child(
        state: &web::Data<crate::AppState>,
        parent: &str,
        child: &str,
    ) -> (
        SessionMessageEnvelope,
        bamboo_engine::execution::ChildCompletion,
    ) {
        use bamboo_engine::execution::{ChildCompletion, ChildCompletionSource};
        use sha2::{Digest, Sha256};
        let (status, body) = execute(state, child, None).await;
        assert_eq!(status, StatusCode::ACCEPTED, "real Child execution: {body}");
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let outcomes = state
                    .session_inbox
                    .pending_child_outcomes(parent, child)
                    .await
                    .unwrap();
                if let Some(envelope) = outcomes.into_iter().next() {
                    if state
                        .session_inbox
                        .inspect(parent)
                        .await
                        .unwrap()
                        .activation_pending()
                    {
                        break envelope;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real Child terminal coordinator must publish eligible outcome");
        let saved = state.storage.load_session(child).await.unwrap().unwrap();
        let source: ChildCompletionSource =
            serde_json::from_str(&saved.metadata["runtime.child_completion_source_v1"]).unwrap();
        assert_eq!(source.activation_run_id, body["run_id"].as_str().unwrap());
        assert_eq!(source.child_session_id, child);
        assert_eq!(source.parent_session_id, parent);
        assert_eq!(source.status, "completed");
        assert_eq!(
            source.result_message_id.as_deref(),
            saved.messages.last().map(|m| m.id.as_str())
        );
        let assistant = saved.messages.last().unwrap();
        let sealed = source.last_assistant.as_ref().unwrap();
        assert_eq!(sealed.message_id, assistant.id);
        assert_eq!(
            sealed.message_sha256,
            hex::encode(Sha256::digest(serde_json::to_vec(assistant).unwrap()))
        );
        assert_eq!(
            envelope.kind,
            bamboo_domain::SessionMessageKind::ChildOutcome
        );
        assert_eq!(envelope.target_session_id, parent);
        let bamboo_domain::SessionMessageBody::ChildOutcome(outcome) = &envelope.body else {
            panic!("production coordinator must emit typed ChildOutcome");
        };
        assert_eq!(outcome.child_session_id, child);
        assert_eq!(outcome.status, "completed");
        assert_eq!(outcome.result.as_deref(), Some("owned Root finished"));
        assert_eq!(
            outcome.provider_message.as_ref().unwrap().metadata["child_completion_source"],
            serde_json::to_value(&source).unwrap()
        );
        let completion = ChildCompletion {
            parent_session_id: parent.into(),
            child_session_id: child.into(),
            status: "completed".into(),
            error: None,
            completed_at: saved.updated_at,
            source: Some(source),
        };
        (envelope, completion)
    }

    // Exercise the coordinator's real locked preparation without launching its
    // automatic Root. Dropping this unpublished launch must retire its exact
    // Actor/registry reservation before the gated HTTP request continues.
    async fn prepare_child_interrupt_for_http(
        state: &web::Data<crate::AppState>,
        id: &str,
        initial_run: &str,
        wait: &bamboo_domain::session::runtime_state::WaitingForChildrenState,
    ) -> String {
        use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort};
        use bamboo_engine::{SessionActivationReserveOutcome, SessionActivationSpawner};
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(backlog.pending, 1);
        assert_eq!(backlog.claimed, 0);
        assert!(backlog.activation_pending());
        assert!(backlog.interrupt_pending());
        assert_eq!(backlog.coordinator_generation, 0);
        assert!(state
            .session_activation_router
            .current_run_id(id)
            .await
            .is_none());
        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!saved.has_pending_question());
        assert_eq!(
            saved
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .waiting_for_children
                .as_ref(),
            Some(wait)
        );
        let retired = state
            .session_store
            .inspect_actor(id)
            .await
            .unwrap()
            .activation
            .unwrap();
        assert_eq!(retired.run_id, initial_run);
        assert_eq!(retired.attempt, 1);
        assert_eq!(retired.status, ActorActivationStatus::Cancelled);
        assert!(retired.finished_at.is_some());
        let launch = match state
            .child_completion_coordinator
            .reserve_activation(id, backlog.generation)
            .await
            .unwrap()
        {
            SessionActivationReserveOutcome::Reserved(launch) => launch,
            _ => panic!("eligible ChildOutcome must reach real coordinator preparation"),
        };
        let run = launch.run_id.clone();
        assert_ne!(run, initial_run);
        let token = {
            let runners = state.agent_runners.read().await;
            let runner = runners.get(id).unwrap();
            assert_eq!(runner.run_id, run);
            assert!(matches!(runner.status, AgentStatus::Running));
            runner.cancel_token.clone()
        };
        assert!(!token.is_cancelled());
        let prepared_actor = state
            .session_store
            .inspect_actor(id)
            .await
            .unwrap()
            .activation
            .unwrap();
        assert_eq!(prepared_actor.run_id, run);
        assert_eq!(prepared_actor.attempt, 2);
        assert_eq!(prepared_actor.status, ActorActivationStatus::Running);
        drop(launch);
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let actor = state
                    .session_store
                    .inspect_actor(id)
                    .await
                    .unwrap()
                    .activation
                    .unwrap();
                assert_eq!(actor.fence(), prepared_actor.fence());
                if actor.status == ActorActivationStatus::Cancelled
                    && actor.finished_at.is_some()
                    && !state.agent_runners.read().await.contains_key(id)
                    && state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("exact unpublished preparation rollback within 15 seconds");
        assert!(token.is_cancelled());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap(), backlog);
        let prepared = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(prepared.messages.is_empty());
        let runtime = prepared.agent_runtime_state.as_ref().unwrap();
        assert_eq!(
            runtime.status,
            bamboo_domain::session::runtime_state::AgentStatusState::Idle
        );
        assert!(runtime.suspension.is_none());
        assert_eq!(runtime.waiting_for_children.as_ref(), Some(wait));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&prepared.metadata["agent.runtime.state"])
                .unwrap(),
            serde_json::to_value(runtime).unwrap()
        );
        assert!(!prepared.metadata.contains_key("runtime.suspend_reason"));
        run
    }

    async fn cleanup_child_http_race(
        state: &web::Data<crate::AppState>,
        ids: &[&str],
    ) -> Result<(), String> {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            // AppState::shutdown stops Child/MCP runtimes, not Root tasks.
            // Cancel only this fixture's actual tokens before awaiting both.
            {
                let runners = state.agent_runners.read().await;
                for id in ids {
                    if let Some(runner) = runners.get(*id) {
                        if matches!(runner.status, AgentStatus::Pending | AgentStatus::Running) {
                            runner.cancel_token.cancel();
                        }
                    }
                }
            }
            state.shutdown().await;
            loop {
                let mut live = false;
                for id in ids {
                    live |= state
                        .agent_runners
                        .read()
                        .await
                        .get(*id)
                        .is_some_and(|runner| {
                            matches!(runner.status, AgentStatus::Pending | AgentStatus::Running)
                        });
                    live |= state
                        .session_activation_router
                        .current_run_id(id)
                        .await
                        .is_some();
                }
                // Only the parent is a native Root Actor. A panic before its
                // creation has no Actor to retire; later failures must await it.
                if state
                    .storage
                    .load_session(ids[0])
                    .await
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    let actor = bamboo_domain::ActorDirectoryPort::inspect_actor(
                        state.session_store.as_ref(),
                        ids[0],
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .activation;
                    live |= actor
                        .is_some_and(|actor| actor.status.is_live() || actor.finished_at.is_none());
                }
                if !live {
                    return Ok(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            format!("fixture cleanup left a live runner/router/Actor after 15 seconds: {ids:?}")
        })?
    }

    async fn run_owned_http_child_selection_race(initially_running: bool, collide_on_retry: bool) {
        use futures::FutureExt;
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        let id = if collide_on_retry {
            "owned-http-child-retry-collision"
        } else if initially_running {
            "owned-http-busy-child-race"
        } else {
            "owned-http-empty-child-race"
        };
        let result = std::panic::AssertUnwindSafe(run_owned_http_child_selection_race_body(
            state.clone(),
            provider,
            id,
            initially_running,
            collide_on_retry,
        ))
        .catch_unwind()
        .await;
        let cleanup =
            cleanup_child_http_race(&state, &[id, &format!("{id}-fast"), &format!("{id}-slow")])
                .await;
        if let Err(panic) = result {
            if let Err(error) = cleanup {
                eprintln!("Child HTTP fixture cleanup also failed: {error}");
            }
            std::panic::resume_unwind(panic);
        }
        cleanup.expect("Child HTTP fixture must finish cleanup before reporting its result");
        if let Some((status, body)) = result.unwrap() {
            assert_eq!(
                status,
                StatusCode::ACCEPTED,
                "sole ChildOutcome must make owned HTTP progress: {body}"
            );
        }
    }

    async fn run_owned_http_child_selection_race_body(
        state: web::Data<crate::AppState>,
        provider: Arc<OwnedRootProvider>,
        id: &str,
        initially_running: bool,
        collide_on_retry: bool,
    ) -> Option<(StatusCode, serde_json::Value)> {
        use bamboo_domain::session::runtime_state::{
            AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState,
            WaitingForChildrenState,
        };
        use bamboo_engine::execution::ChildCompletionHandler;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let fast = format!("{id}-fast");
        let slow = format!("{id}-slow");
        let mut parent = bamboo_agent_core::Session::new(id, "test-model");
        parent.title_generated = true;
        let wait = WaitingForChildrenState::for_children(
            vec![fast.clone(), slow.clone()],
            ChildWaitPolicy::All,
            chrono::Utc::now(),
        );
        let mut runtime = AgentRuntimeState::new("waiting-before-http-race");
        runtime.status = AgentStatusState::Suspended;
        runtime.waiting_for_children = Some(wait.clone());
        runtime.suspension = Some(SuspensionState {
            reason: "waiting_for_children".into(),
            suspended_at: chrono::Utc::now(),
            resumable: true,
            hook_point: Some("ChildCompletion".into()),
        });
        parent.agent_runtime_state = Some(runtime);
        parent.metadata.insert(
            "runtime.suspend_reason".into(),
            "waiting_for_children".into(),
        );
        state.storage.save_session(&parent).await.unwrap();
        let mut child = bamboo_agent_core::Session::new_child(&fast, id, "test-model", "Child");
        child.title_generated = true;
        child.add_message(bamboo_agent_core::Message::user(
            "complete the real race child",
        ));
        state.storage.save_session(&child).await.unwrap();
        let mut sibling = bamboo_agent_core::Session::new_child(&slow, id, "test-model", "Child");
        sibling.set_last_run_status("running");
        state.storage.save_session(&sibling).await.unwrap();
        state.skill_manager.store_for_workspace(None).await.unwrap();
        let mut owner = Some(reserve_owned_root(&state, id).await);
        let initial_run = owner.as_ref().unwrap().run_id().to_owned();
        let upgrade = owner
            .as_ref()
            .unwrap()
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(&mut parent, state.session_inbox.clone(), Some(&initial_run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(upgrade.merged, 0);
        assert!(upgrade.admission_error.is_none());
        let legacy = state.session_inbox.claim(id, 1).await.unwrap_err();
        assert!(legacy
            .to_string()
            .contains("owned Inbox claim API required"));
        let mut outcome = if initially_running {
            Some(complete_http_race_child(&state, id, &fast).await)
        } else {
            owner.take().unwrap().abandon().await;
            assert!(!state
                .session_inbox
                .inspect(id)
                .await
                .unwrap()
                .activation_pending());
            None
        };
        let (_probe, selected_rx, continue_tx) = selection_probe(id);
        let retry_gate = collide_on_retry.then(|| checked_selection_probe(id));
        let mut contender = None;
        let mut prepared_run = None;
        let mut events = state.get_session_event_sender(id).await.subscribe();
        let http = super::super::handler::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Path::from(id.to_owned()),
            web::Json(serde_json::from_value(serde_json::json!({"model":"test-model"})).unwrap()),
        );
        let transition = async {
            assert!(!selected_rx.await.unwrap());
            if initially_running {
                owner.take().unwrap().abandon().await;
            } else {
                outcome = Some(complete_http_race_child(&state, id, &fast).await);
            }
            assert!(state
                .session_activation_router
                .current_run_id(id)
                .await
                .is_none());
            let backlog = state.session_inbox.inspect(id).await.unwrap();
            assert_eq!(backlog.pending, 1);
            assert!(backlog.activation_pending());
            assert_eq!(
                backlog.coordinator_generation, 0,
                "partial All wait has no blanket prefix grant"
            );
            assert_eq!(
                provider.messages.lock().unwrap().len(),
                1,
                "only the actual Child has run"
            );
            let saved = state.storage.load_session(id).await.unwrap().unwrap();
            assert!(
                saved.messages.is_empty(),
                "sole pending work is the real ChildOutcome"
            );
            assert_eq!(
                saved
                    .agent_runtime_state
                    .as_ref()
                    .unwrap()
                    .waiting_for_children
                    .as_ref(),
                Some(&wait)
            );
            let sibling_before =
                serde_json::to_value(state.storage.load_session(&slow).await.unwrap().unwrap())
                    .unwrap();
            prepared_run =
                Some(prepare_child_interrupt_for_http(&state, id, &initial_run, &wait).await);
            let envelope = &outcome.as_ref().unwrap().0;
            assert!(!state
                .session_inbox
                .was_admitted(id, &envelope.id)
                .await
                .unwrap());
            let pending = state
                .session_inbox
                .pending_child_outcomes(id, &fast)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(pending).unwrap(),
                serde_json::to_value(vec![envelope]).unwrap()
            );
            assert_eq!(
                serde_json::to_value(state.storage.load_session(&slow).await.unwrap().unwrap())
                    .unwrap(),
                sibling_before
            );
            assert_eq!(
                provider.messages.lock().unwrap().len(),
                1,
                "unpublished preparation cannot call Root provider"
            );
            assert!(input_taps().lock().unwrap().get(id).unwrap().is_empty());
            continue_tx.send(()).unwrap();
            if let Some((checked_rx, continue_reserve)) = retry_gate {
                assert!(
                    !checked_rx.await.unwrap(),
                    "checked admission needs a missing owner"
                );
                // The reader must have released its guard: a genuine contender
                // now reserves/binds through the registry writer without deadlock.
                contender = Some(reserve_owned_root(&state, id).await);
                continue_reserve.send(()).unwrap();
            }
        };
        let (mut response, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(http, transition)
        })
        .await
        .expect("bounded real Child transition while Root HTTP is gated");
        let mut contender_run = None;
        if collide_on_retry {
            let status = response.status();
            let bytes = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "api_error");
            assert_eq!(
                body["error"]["message"],
                "Root admission changed before ownership; retry execute"
            );
            let contender = contender.take().unwrap();
            let writer = contender.root_actor_writer().unwrap();
            contender_run = Some(contender.run_id().to_owned());
            assert!(!contender.cancel_token().is_cancelled());
            let actor =
                bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                    .await
                    .unwrap()
                    .activation
                    .unwrap();
            assert_eq!(actor.fence(), writer.fence);
            assert_eq!(actor.attempt, 3);
            assert_ne!(actor.run_id, initial_run);
            assert_ne!(Some(actor.run_id.as_str()), prepared_run.as_deref());
            assert_eq!(actor.status, bamboo_domain::ActorActivationStatus::Running);
            assert_eq!(
                state.agent_runners.read().await.get(id).unwrap().run_id,
                contender.run_id()
            );
            assert_eq!(
                state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .as_deref(),
                Some(contender.run_id())
            );
            let backlog = state.session_inbox.inspect(id).await.unwrap();
            assert_eq!(backlog.pending, 1);
            assert_eq!(backlog.claimed, 0);
            assert_eq!(backlog.coordinator_generation, 0);
            let envelope = &outcome.as_ref().unwrap().0;
            assert!(!state
                .session_inbox
                .was_admitted(id, &envelope.id)
                .await
                .unwrap());
            assert!(state
                .storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .is_empty());
            assert_eq!(
                provider.messages.lock().unwrap().len(),
                1,
                "collision cannot call Root provider"
            );
            assert!(input_taps().lock().unwrap().get(id).unwrap().is_empty());
            contender.abandon().await;
            assert!(state
                .session_activation_router
                .current_run_id(id)
                .await
                .is_none());
            // A new real HTTP request, not a loop or unowned fallback in the
            // rejected request, must progress the exact same permanent input.
            response = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                super::super::handler::handler(
                    state.clone(),
                    test::TestRequest::post().to_http_request(),
                    web::Path::from(id.to_owned()),
                    web::Json(
                        serde_json::from_value(serde_json::json!({"model":"test-model"})).unwrap(),
                    ),
                ),
            )
            .await
            .expect("fresh owned HTTP retry after exact contender retirement");
        }
        let status = response.status();
        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if status != StatusCode::ACCEPTED {
            return Some((status, body));
        }
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "sole ChildOutcome must make owned HTTP progress: {body}"
        );
        let run = body["run_id"].as_str().unwrap();
        assert_ne!(run, initial_run);
        assert_ne!(Some(run), prepared_run.as_deref());
        assert_ne!(Some(run), contender_run.as_deref());
        wait_for_owned_terminal(&state, id, run, &mut events).await;
        let (envelope, completion) = outcome.unwrap();
        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        let matches: Vec<_> = saved
            .messages
            .iter()
            .filter(|m| m.id == envelope.id.as_str())
            .collect();
        assert_eq!(matches.len(), 1);
        assert!(bamboo_domain::is_matching_session_message(
            matches[0], &envelope
        ));
        let source = completion.source.as_ref().unwrap();
        let durable_source = &matches[0].metadata.as_ref().unwrap()["child_completion_source"];
        assert_eq!(*durable_source, serde_json::to_value(source).unwrap());
        assert!(state
            .session_inbox
            .was_admitted(id, &envelope.id)
            .await
            .unwrap());
        assert_eq!(
            saved
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .waiting_for_children
                .as_ref(),
            Some(&wait)
        );
        let taps = input_taps().lock().unwrap().remove(id).unwrap();
        assert_eq!(taps.len(), 1, "same reservation reaches one Ready/spawn");
        assert!(
            taps[0].is_none(),
            "Child outcome cannot manufacture current User input authority"
        );
        let calls = provider.messages.lock().unwrap();
        assert_eq!(
            calls.len(),
            2,
            "one Child plus one successor Root provider call"
        );
        assert_eq!(
            calls
                .iter()
                .filter(|messages| messages.iter().any(|m| m.id == envelope.id.as_str()))
                .count(),
            1
        );
        drop(calls);
        let actor =
            bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                .await
                .unwrap()
                .activation
                .unwrap();
        assert_eq!(actor.run_id, run);
        assert_eq!(actor.attempt, if collide_on_retry { 4 } else { 3 });
        // Production duplicate completion must retain its permanent receipt,
        // not release the unfinished sibling or create another reasoning turn.
        state
            .child_completion_coordinator
            .on_child_completed(completion)
            .await;
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        assert_eq!(backlog.coordinator_generation, 0);
        assert_eq!(provider.messages.lock().unwrap().len(), 2);
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            after
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .waiting_for_children
                .as_ref(),
            Some(&wait)
        );
        assert_eq!(
            after
                .messages
                .iter()
                .filter(|m| m.id == envelope.id.as_str())
                .count(),
            1
        );
        None
    }

    #[actix_web::test]
    async fn owned_root_http_retired_owner_child_outcome_stays_owned() {
        run_owned_http_child_selection_race(true, false).await;
    }

    #[actix_web::test]
    async fn owned_root_http_late_child_outcome_stays_owned() {
        run_owned_http_child_selection_race(false, false).await;
    }

    #[actix_web::test]
    async fn owned_root_http_secondary_collision_preserves_child_for_fresh_retry() {
        run_owned_http_child_selection_race(false, true).await;
    }

    #[actix_web::test]
    async fn owned_root_http_real_v3_admission_reaches_same_execution_once() {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "owned-root-http-real-v3";
        let mut session = bamboo_agent_core::Session::new(id, "test-model");
        // A previously titled Root isolates the single execution provider call.
        session.title_generated = true;
        state.storage.save_session(&session).await.unwrap();
        let reservation = reserve_owned_root(&state, id).await;
        let initial_run = reservation.run_id().to_owned();
        let upgraded = reservation
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(
                &mut session,
                state.session_inbox.clone(),
                Some(&initial_run),
            )
            .await
            .unwrap()
            .expect("real bound Root owned admission");
        assert_eq!(upgraded.merged, 0);
        assert!(upgraded.admission_error.is_none());
        reservation.abandon().await;
        {
            let runners = state.agent_runners.read().await;
            let retired = runners
                .get(id)
                .expect("exact reservation cleanup retains its terminal slot");
            assert_eq!(retired.run_id, initial_run);
            assert!(matches!(retired.status, AgentStatus::Cancelled));
        }
        assert!(state
            .session_activation_router
            .current_run_id(id)
            .await
            .is_none());
        let legacy = state.session_inbox.claim(id, 1).await.unwrap_err();
        assert!(
            legacy
                .to_string()
                .contains("owned Inbox claim API required"),
            "{legacy}"
        );
        queue_owned_root(&state, id).await;
        assert!(provider.messages.lock().unwrap().is_empty());
        input_taps().lock().unwrap().insert(id.into(), Vec::new());
        let response = super::super::handler::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Path::from(id.to_owned()),
            web::Json(serde_json::from_value(serde_json::json!({"model":"test-model"})).unwrap()),
        )
        .await;
        let status = response.status();
        let body: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "actual owned-v3 HTTP boundary: {body}"
        );
        let run = body["run_id"].as_str().unwrap();
        assert_ne!(run, initial_run);
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let runners = state.agent_runners.read().await;
                let runner = runners.get(id).expect("same reserved runner reaches spawn");
                assert_eq!(runner.run_id, run);
                if !matches!(runner.status, AgentStatus::Pending | AgentStatus::Running) {
                    assert!(
                        matches!(runner.status, AgentStatus::Completed),
                        "{:?}",
                        runner.status
                    );
                    break;
                }
                drop(runners);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("actual owned Root completes within unchanged deadline");
        let stored = state.storage.load_session(id).await.unwrap().unwrap();
        let users: Vec<_> = stored
            .messages
            .iter()
            .filter(|m| m.role == bamboo_agent_core::Role::User)
            .collect();
        assert_eq!(users.len(), 1, "one durable typed User checkpoint");
        let input = &users[0].id;
        assert_eq!(users[0].content, "one owned Root input");
        assert!(state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(input).unwrap())
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
        let taps = input_taps().lock().unwrap().remove(id).unwrap();
        assert_eq!(taps.len(), 1, "one actual Ready/spawn handoff");
        assert_eq!(taps[0].as_ref().unwrap().len(), 1);
        assert_eq!(taps[0].as_ref().unwrap()[0].0, *input);
        let calls = provider.messages.lock().unwrap();
        assert_eq!(calls.len(), 1, "one execution provider call");
        assert_eq!(calls[0].iter().filter(|m| m.id == *input).count(), 1);
        drop(calls);
        let activation =
            bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                .await
                .unwrap()
                .activation
                .unwrap();
        assert_eq!(
            activation.run_id, run,
            "admission and execution keep the same Root owner"
        );
        assert_eq!(
            activation.attempt, 2,
            "one initial upgrade, one HTTP reservation; Ready cannot reserve again"
        );
        let retry = state.admit_chat_for_execute(id).await.unwrap();
        assert!(retry.inputs.is_none());
        assert!(!retry.generate_title, "NoNew cannot manufacture title work");
    }

    #[actix_web::test]
    async fn owned_root_http_existing_reservation_keeps_input_pending() {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "owned-root-http-active-owner";
        let mut session = owned_http_session(&state, id);
        state.storage.save_session(&session).await.unwrap();
        let reservation = reserve_owned_root(&state, id).await;
        let owner = reservation.root_actor_writer().unwrap();
        let admission = reservation
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(
                &mut session,
                state.session_inbox.clone(),
                Some(reservation.run_id()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(admission.admission_error.is_none());
        queue_owned_root(&state, id).await;
        let response = super::super::handler::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Path::from(id.to_owned()),
            web::Json(serde_json::from_value(serde_json::json!({"model":"test-model"})).unwrap()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let activation =
            bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), id)
                .await
                .unwrap()
                .activation
                .unwrap();
        assert_eq!(activation.fence(), owner.fence);
        assert_eq!(
            activation.status,
            bamboo_domain::ActorActivationStatus::Running
        );
        assert_eq!(
            state
                .session_activation_router
                .current_run_id(id)
                .await
                .as_deref(),
            Some(reservation.run_id())
        );
        let runners = state.agent_runners.read().await;
        assert_eq!(runners.get(id).unwrap().run_id, reservation.run_id());
        assert!(matches!(
            runners.get(id).unwrap().status,
            AgentStatus::Running
        ));
        drop(runners);
        assert!(!reservation.cancel_token().is_cancelled());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        assert!(!state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
        assert!(provider.messages.lock().unwrap().is_empty());
        reservation.abandon().await;
    }

    #[actix_web::test]
    async fn owned_root_http_lost_bound_owner_fails_closed_before_admission() {
        let provider = Arc::new(OwnedRootProvider::default());
        let (_home, state, _) = state_with_provider(None, provider.clone()).await;
        state
            .session_activation_router
            .set_spawner(Arc::new(ManualHttpOnly))
            .await;
        let id = "owned-root-http-lost-owner";
        let mut session = owned_http_session(&state, id);
        state.storage.save_session(&session).await.unwrap();
        let mut reservation = reserve_owned_root(&state, id).await;
        let admission = reservation
            .execution_persistence()
            .unwrap()
            .admit_root_inbox(
                &mut session,
                state.session_inbox.clone(),
                Some(reservation.run_id()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(admission.admission_error.is_none());
        queue_owned_root(&state, id).await;
        let mismatched = state
            .admit_chat_for_execute_with_reservation("another-root", Some(&reservation))
            .await
            .err()
            .expect("an exact reservation cannot admit a different Root");
        assert_eq!(mismatched.status(), StatusCode::SERVICE_UNAVAILABLE);
        // End the real fence through its owning reservation. The previously
        // bound persistence is now stale; no watermark/fence bytes are forged.
        reservation
            .finish_root_actor(bamboo_domain::ActorActivationFinish::Cancelled)
            .await;
        let failure = state
            .admit_chat_for_execute_with_reservation(id, Some(&reservation))
            .await
            .err()
            .expect("lost Root owner must reject admission");
        assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        assert!(!state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
        assert!(provider.messages.lock().unwrap().is_empty());
        reservation.abandon().await;
        assert!(state
            .agent_runners
            .read()
            .await
            .get(id)
            .is_some_and(|runner| matches!(runner.status, AgentStatus::Cancelled)));
        assert!(state
            .session_activation_router
            .current_run_id(id)
            .await
            .is_none());
    }

    #[actix_web::test]
    async fn native_inbox_real_chat_receipt_checkpoint_ack_and_current_input_once() {
        let (_home, state, _fault) = state(None).await;
        let id = "native-inbox-real-consumer";
        let response = chat(&state, id, None).await;
        let input = response["message_id"]
            .as_str()
            .expect("Native Chat returns its actual Inbox receipt");
        assert!(response["ingress_seq"].as_u64().unwrap() > 0);
        let before = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!before
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
        assert!(!before.metadata.contains_key("chat.queued_ingress.v1"));
        assert!(!before.title_generated);
        assert!(state
            .session_inbox
            .inspect(id)
            .await
            .unwrap()
            .activation_pending());
        let inputs = crate::handlers::agent::chat::admit_for_execute(&state, id)
            .await
            .unwrap()
            .expect("real checked New admission supplies current data");
        only_id(&inputs, input);
        let admitted = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            admitted.messages.iter().filter(|m| m.id == input).count(),
            1
        );
        assert!(state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(input).unwrap())
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
        drop(inputs);
        assert!(
            crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .is_none(),
            "NoNew retry cannot mint current data from canonical history"
        );
    }

    #[actix_web::test]
    async fn native_ack_failure_recovers_only_scheduling_owner_without_data_or_title() {
        for after_receipt in [false, true] {
            let (_home, state, _fault) = state(Some(after_receipt)).await;
            let id = format!("native-ack-no-new-{after_receipt}");
            let mut original = bamboo_agent_core::Session::new(&id, "test-model");
            original.set_last_run_status("error");
            original.set_last_run_error("old failure");
            state.storage.save_session(&original).await.unwrap();
            let response = chat(&state, &id, None).await;
            let input = response["message_id"].as_str().unwrap();
            let mut feed = state.account_sink.subscribe();
            let failure = state.admit_chat_for_execute(&id).await.err().unwrap();
            assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
            let failed = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(failed.messages.iter().filter(|m| m.id == input).count(), 1);
            assert_eq!(failed.last_run_status().as_deref(), Some("error"));
            assert!(crate::handlers::agent::events::startup_work_id(&failed).is_none());
            let event = tokio::time::timeout(std::time::Duration::from_secs(2), feed.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(&event.event, bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. } if message_id == input)
            );
            let retry = state.admit_chat_for_execute(&id).await.unwrap();
            assert!(
                retry.inputs.is_none(),
                "ACK recovery/history cannot reconstruct the previous request or startup seal"
            );
            assert!(!retry.generate_title, "NoNew cannot invent a title effect");
            let recovered = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(
                crate::handlers::agent::events::startup_work_id(&recovered).as_deref(),
                Some(input)
            );
            assert_eq!(
                recovered.messages.iter().filter(|m| m.id == input).count(),
                1
            );
            assert!(state
                .session_inbox
                .was_admitted(&id, &SessionMessageId::parse(input).unwrap())
                .await
                .unwrap());
            assert_eq!(state.session_inbox.inspect(&id).await.unwrap().pending, 0);
            assert!(
                feed.try_recv().is_err(),
                "recovery publishes no duplicate canonical event"
            );
        }
    }

    #[actix_web::test]
    async fn execution_input_http_actual_queue_admission_returns_only_this_calls_new_user() {
        let (_home, state, _fault) = state(None).await;
        let id = "execution-input-http-admit";
        bootstrap(&state, id).await;
        chat(&state, id, Some("current-queued-a")).await;
        let inputs = crate::handlers::agent::chat::admit_for_execute(&state, id)
            .await
            .unwrap()
            .unwrap();
        only_id(&inputs, "current-queued-a");
        drop(inputs);
        assert!(
            crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .is_none(),
            "stored history cannot mint another observation"
        );
        chat(&state, id, Some("current-queued-a")).await;
        assert!(
            crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .is_none(),
            "same-ID retry/recovery is not new"
        );
        chat(&state, id, Some("current-queued-b")).await;
        only_id(
            &crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .unwrap(),
            "current-queued-b",
        );
        let stored = state.storage.load_session(id).await.unwrap().unwrap();
        for input in ["current-queued-a", "current-queued-b"] {
            assert_eq!(stored.messages.iter().filter(|m| m.id == input).count(), 1);
        }
    }
    #[actix_web::test]
    async fn execution_input_http_checked_queue_reaches_exact_ready_spawn_once() {
        for native in [false, true] {
            let (_home, state, _fault) = state(None).await;
            let id = "execution-input-http-ready";
            let input = if native {
                chat(&state, id, None).await["message_id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            } else {
                bootstrap(&state, id).await;
                chat(&state, id, Some("ready-current-user")).await;
                "ready-current-user".to_owned()
            };
            input_taps().lock().unwrap().insert(id.into(), Vec::new());
            let (status, body) = execute(&state, id, None).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
            assert_eq!(body["status"], "started", "{body}");
            let observed = input_taps().lock().unwrap().remove(id).unwrap();
            assert_eq!(observed.len(), 1, "one actual Ready/spawn handoff");
            let inputs = observed[0].as_ref().unwrap();
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].0, input);
            assert_eq!(inputs[0].1.as_ref().unwrap().selections[0].id, "review");
            let completion = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let terminal = state.agent_runners.read().await.get(id).is_some_and(|r| {
                        matches!(
                            r.status,
                            crate::app_state::AgentStatus::Completed
                                | crate::app_state::AgentStatus::Error(_)
                                | crate::app_state::AgentStatus::Cancelled
                        )
                    });
                    if terminal {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await;
            let runner_status = state
                .agent_runners
                .read()
                .await
                .get(id)
                .map(|runner| runner.status.clone());
            assert!(
                completion.is_ok()
                    && matches!(
                        runner_status.as_ref(),
                        Some(crate::app_state::AgentStatus::Completed)
                    ),
                "actual execution must finish successfully: {runner_status:?}"
            );
            let stored = state.storage.load_session(id).await.unwrap().unwrap();
            let loads = stored
                .messages
                .iter()
                .filter(|message| {
                    message.tool_call_id.as_deref() == Some("execution-input-existing-load")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                loads.len(),
                1,
                "one real workflow prerequisite through the admitted surface"
            );
            assert_eq!(loads[0].tool_success, Some(true));
            assert_eq!(stored.messages.iter().filter(|m| m.id == input).count(), 1);
            assert!(crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .is_none());
        }
    }
    #[actix_web::test]
    async fn execution_input_http_ack_failure_preserves_prefix_events_but_recovery_cannot_regrant_data(
    ) {
        for after_receipt in [false, true] {
            let (_home, state, fault) = state(Some(after_receipt)).await;
            fault.store(false, Ordering::SeqCst);
            let id = format!("execution-input-http-ack-{after_receipt}");
            bootstrap(&state, &id).await;
            fault.store(true, Ordering::SeqCst);
            chat(&state, &id, Some("ack-current-a")).await;
            let mut feed = state.account_sink.subscribe();
            let error = crate::handlers::agent::chat::admit_for_execute(&state, &id)
                .await
                .unwrap_err();
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
            let stored = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(
                stored
                    .messages
                    .iter()
                    .filter(|m| m.id == "ack-current-a")
                    .count(),
                1,
                "successful transcript prefix retained"
            );
            let change = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(&change.event,bamboo_agent_core::AgentEvent::MessageAppended{message_id,..} if message_id=="ack-current-a"),
                "checkpointed event survives ACK failure"
            );
            assert!(
                crate::handlers::agent::chat::admit_for_execute(&state, &id)
                    .await
                    .unwrap()
                    .is_none(),
                "checkpoint/receipt recovery is not fresh input data"
            );
            assert!(
                feed.try_recv().is_err(),
                "recovery does not duplicate prefix event"
            );
            chat(&state, &id, Some("ack-successor-b")).await;
            only_id(
                &crate::handlers::agent::chat::admit_for_execute(&state, &id)
                    .await
                    .unwrap()
                    .unwrap(),
                "ack-successor-b",
            );
        }
    }
    #[actix_web::test]
    async fn execution_input_http_startup_rejection_drops_current_data_and_successor_is_separate() {
        for (native, reason) in [
            (false, "provider"),
            (true, "provider"),
            (true, "model"),
            (true, "image"),
            (true, "sync"),
        ] {
            let (_home, state, _fault) = state(None).await;
            // A rejected early reservation has already activated this Root.
            // Keep its later Chat input for this fixture's manual HTTP owner.
            state
                .session_activation_router
                .set_spawner(Arc::new(ManualHttpOnly))
                .await;
            let id = format!("execution-input-http-reject-{native}-{reason}");
            if !native {
                bootstrap(&state, &id).await;
            }
            let receipt = chat(
                &state,
                &id,
                if native {
                    None
                } else {
                    Some("rejected-current-a")
                },
            )
            .await;
            let rejected = receipt["message_id"].as_str().unwrap().to_owned();
            if reason == "image" {
                let mut config = state.config.write().await;
                config.hooks.image_fallback.enabled = true;
                config.hooks.image_fallback.mode = "invalid-existing-mode".into();
            } else if reason == "model" {
                state
                    .config
                    .write()
                    .await
                    .providers_mut()
                    .openai
                    .as_mut()
                    .unwrap()
                    .model = None;
                let mut canonical = state.storage.load_session(&id).await.unwrap().unwrap();
                canonical.model.clear();
                state.save_and_cache_session(&mut canonical).await;
            }
            input_taps().lock().unwrap().insert(id.clone(), Vec::new());
            let request = serde_json::from_value::<ExecuteRequest>(serde_json::json!({
                "provider": (reason == "provider").then_some("unavailable-explicit-provider"),
                "client_sync": (reason == "sync").then(|| serde_json::json!({"client_message_count":0,"client_has_pending_question":false}))
            })).unwrap();
            let response = super::super::handler::handler(
                state.clone(),
                test::TestRequest::post().to_http_request(),
                web::Path::from(id.clone()),
                web::Json(request),
            )
            .await;
            let status = response.status();
            let body: serde_json::Value = serde_json::from_slice(
                &actix_web::body::to_bytes(response.into_body())
                    .await
                    .unwrap(),
            )
            .unwrap();
            if reason == "sync" {
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(body["sync"]["need_sync"], true);
            } else {
                assert!(
                    status.is_client_error() || status.is_server_error(),
                    "{reason}: {body}"
                );
            }
            assert!(
                input_taps().lock().unwrap().remove(&id).unwrap().is_empty(),
                "failed startup has no execution handoff"
            );
            // Both server-generated and explicit input IDs belong to this
            // current Root and require the same real reservation. Rejection
            // must retire that exact runner and Actor before a successor.
            let runners = state.agent_runners.read().await;
            let runner = runners
                .get(&id)
                .expect("rejected Root startup retains its exact terminal runner");
            assert!(matches!(runner.status, AgentStatus::Cancelled));
            assert!(runner.cancel_token.is_cancelled());
            let rejected_run = runner.run_id.clone();
            drop(runners);
            let actor =
                bamboo_domain::ActorDirectoryPort::inspect_actor(state.session_store.as_ref(), &id)
                    .await
                    .unwrap()
                    .activation
                    .expect("the rejected reservation had a real bound Root Actor");
            assert_eq!(actor.run_id, rejected_run);
            assert_eq!(
                actor.status,
                bamboo_domain::ActorActivationStatus::Cancelled
            );
            assert!(actor.finished_at.is_some());
            assert!(state
                .session_activation_router
                .current_run_id(&id)
                .await
                .is_none());
            assert!(state
                .session_inbox
                .was_admitted(&id, &SessionMessageId::parse(&rejected).unwrap())
                .await
                .unwrap());
            let no_new = state.admit_chat_for_execute(&id).await.unwrap();
            assert!(no_new.inputs.is_none());
            assert!(!no_new.generate_title);
            let successor = chat(
                &state,
                &id,
                if native {
                    None
                } else {
                    Some("rejection-successor-b")
                },
            )
            .await;
            let successor_reservation = state.reserve_root_chat_for_execute(&id).await.unwrap();
            let successor_admission = state
                .admit_chat_for_execute_with_reservation(&id, successor_reservation.as_ref())
                .await
                .unwrap();
            let stored = state.storage.load_session(&id).await.unwrap().unwrap();
            let user = stored
                .messages
                .iter()
                .rev()
                .find(|message| message.role == bamboo_agent_core::Role::User)
                .expect("successor has a canonical typed User checkpoint");
            let proof = &user.metadata.as_ref().unwrap()["session_message"];
            assert_eq!(proof["source"]["type"], "user");
            assert_eq!(proof["kind"], "user_input");
            assert_eq!(proof["target_session_id"], id);
            let envelope: SessionMessageEnvelope = serde_json::from_value(proof.clone()).unwrap();
            assert!(bamboo_domain::is_matching_session_message(user, &envelope));
            assert_ne!(
                user.id, rejected,
                "a successor cannot reuse rejected current data"
            );
            if let Some(receipt) = successor["message_id"].as_str() {
                assert_eq!(user.id, receipt);
            } else {
                assert!(
                    native,
                    "only activated Native Root Chat may omit its receipt"
                );
            }
            assert!(state
                .session_inbox
                .was_admitted(&id, &envelope.id)
                .await
                .unwrap());
            only_id(&successor_admission.inputs.unwrap(), &user.id);
            if let Some(reservation) = successor_reservation {
                reservation.abandon().await;
            }
        }
    }

    struct RunningProvider {
        calls: std::sync::atomic::AtomicUsize,
        started: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        messages: std::sync::Mutex<Vec<Vec<bamboo_agent_core::Message>>>,
    }
    #[async_trait]
    impl LLMProvider for RunningProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.messages.lock().unwrap().push(messages.to_vec());
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            LocalProvider.chat_stream(messages, tools, max, model).await
        }
    }

    #[actix_web::test]
    async fn native_running_owner_consumes_current_turn_at_next_real_provider_boundary_once() {
        let provider = Arc::new(RunningProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            messages: std::sync::Mutex::new(Vec::new()),
        });
        let (_home, state, _fault) = state_with_provider(None, provider.clone()).await;
        let mut parent = bamboo_agent_core::Session::new("native-running-parent", "test-model");
        state.save_and_cache_session(&mut parent).await;
        let id = "native-running-owned-child";
        let mut child =
            bamboo_agent_core::Session::new_child_of(id, &parent, "test-model", "Child");
        state.save_and_cache_session(&mut child).await;
        let mut feed = state.account_sink.subscribe();
        let initial = chat(&state, id, None).await;
        let first = initial["message_id"].as_str().unwrap().to_owned();
        let (status, started) = execute(&state, id, None).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{started}");
        let run = started["run_id"].as_str().unwrap().to_owned();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            provider.started.acquire(),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
        assert!(state
            .agent_runners
            .read()
            .await
            .get(id)
            .is_some_and(|r| r.run_id == run && matches!(r.status, AgentStatus::Running)));
        let text = "current Native while blocked 原样\nnext";
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(
            serde_json::json!({
                "session_id":id,"message":text,"model":"test-model"
            }),
        )
        .unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post()
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_http_request(),
            web::Json(request),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let receipt: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        let second = receipt["message_id"].as_str().unwrap().to_owned();
        assert!(receipt["ingress_seq"].as_u64().unwrap() > 0);
        let second_id = SessionMessageId::parse(second.clone()).unwrap();
        assert!(!state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == second));
        assert!(!state
            .session_inbox
            .was_admitted(id, &second_id)
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        let blocked = state.admit_chat_for_execute(id).await.unwrap();
        assert!(blocked.inputs.is_none());
        assert!(!blocked.generate_title);
        let (status, repeated) = execute(&state, id, None).await;
        assert_eq!(status, StatusCode::OK, "{repeated}");
        assert_eq!(repeated["status"], "already_running");
        if let Some(repeated_run) = repeated["run_id"].as_str() {
            assert_eq!(repeated_run, run);
        }
        assert!(state
            .agent_runners
            .read()
            .await
            .get(id)
            .is_some_and(|r| r.run_id == run));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        provider.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                if state.agent_runners.read().await.get(id).is_some_and(|r|
                    r.run_id == run && matches!(r.status, AgentStatus::Completed))
                    && state.session_inbox.was_admitted(id, &second_id).await.unwrap()
                    && provider.messages.lock().unwrap().iter().skip(1).any(|rows|
                        rows.iter().any(|m| m.id == second && m.content == text)) { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("same actual owner checkpoints/ACKs and presents Native to a next real provider request");
        let snapshots = provider.messages.lock().unwrap();
        assert!(!snapshots[0].iter().any(|m| m.id == second));
        assert!(snapshots.iter().skip(1).any(|rows| rows
            .iter()
            .filter(|m| m.id == second && m.content == text && m.content_parts.is_none())
            .count()
            == 1));
        drop(snapshots);
        let cold = state.storage.load_session(id).await.unwrap().unwrap();
        for input in [&first, &second] {
            assert_eq!(cold.messages.iter().filter(|m| &m.id == input).count(), 1);
        }
        let load = cold
            .messages
            .iter()
            .filter(|m| m.tool_call_id.as_deref() == Some("execution-input-existing-load"))
            .collect::<Vec<_>>();
        assert_eq!(load.len(), 1);
        assert_eq!(load[0].tool_success, Some(true));
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
        let mut events = std::collections::BTreeMap::<String, usize>::new();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !events.contains_key(&first) || !events.contains_key(&second) {
                let change = feed.recv().await.unwrap();
                if let bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. } =
                    &change.event
                {
                    if message_id == &first || message_id == &second {
                        *events.entry(message_id.clone()).or_default() += 1;
                    }
                }
            }
        })
        .await
        .expect("actual durable append events for both Native inputs");
        while let Ok(change) = feed.try_recv() {
            if let bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. } = &change.event
            {
                if message_id == &first || message_id == &second {
                    *events.entry(message_id.clone()).or_default() += 1;
                }
            }
        }
        assert_eq!(events.get(&first), Some(&1));
        assert_eq!(events.get(&second), Some(&1));
        let no_new = state.admit_chat_for_execute(id).await.unwrap();
        assert!(no_new.inputs.is_none());
        assert!(!no_new.generate_title);
    }

    #[actix_web::test]
    async fn native_pending_slot_and_reserved_owner_keep_input_until_original_release() {
        use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
        let (_home, state, _fault) = state(None).await;
        let id = "native-pending-reservation";
        state
            .storage
            .save_session(&bamboo_agent_core::Session::new(id, "test-model"))
            .await
            .unwrap();
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(serde_json::json!({"session_id":id,"message":"Native before the original owner releases","model":"test-model"})).unwrap();
        let response = crate::handlers::agent::chat::handler(
            state.clone(),
            test::TestRequest::post().to_http_request(),
            web::Json(request),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let receipt: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        let input = receipt["message_id"].as_str().unwrap();
        // Existing default Pending-slot control, as in the original Server fixtures.
        // This is not a production Pending activation or a running Runtime proof.
        state
            .agent_runners
            .write()
            .await
            .insert(id.into(), AgentRunner::new());
        let pending = state.admit_chat_for_execute(id).await.unwrap();
        assert!(pending.inputs.is_none());
        assert!(!pending.generate_title);
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        {
            let mut runners = state.agent_runners.write().await;
            let removed =
                bamboo_engine::execution::runner_lifecycle::remove_runner_entry(&mut runners, id)
                    .await
                    .unwrap();
            assert!(matches!(removed.status, AgentStatus::Pending));
        }
        let sender = state.get_session_event_sender(id).await;
        let reservation = match reserve_session_execution(
            &state.agent,
            &state.agent_runners,
            &state.session_event_senders,
            id,
            &sender,
        )
        .await
        {
            SessionExecutionReserveOutcome::Reserved(r) => r,
            _ => panic!("original idle reservation"),
        };
        let run = reservation.run_id().to_owned();
        assert!(state
            .agent_runners
            .read()
            .await
            .get(id)
            .is_some_and(|r| r.run_id == run && matches!(r.status, AgentStatus::Running)));
        let blocked = state.admit_chat_for_execute(id).await.unwrap();
        assert!(blocked.inputs.is_none());
        assert!(!blocked.generate_title);
        let (status, body) = execute(&state, id, None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // Original execute checks canonical pending work before reservation.
        // The input stays in Inbox, so this response does not finish the owner.
        assert_eq!(body["status"], "completed");
        assert!(state
            .agent_runners
            .read()
            .await
            .get(id)
            .is_some_and(|r| r.run_id == run && matches!(r.status, AgentStatus::Running)));
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 1);
        assert!(!state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == input));
        reservation.abandon().await;
        // Original abandonment may launch a legitimate successor before HTTP execute.
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !state
                .session_inbox
                .was_admitted(id, &SessionMessageId::parse(input).unwrap())
                .await
                .unwrap()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original successor checkpoints and ACKs Native once");
        let no_new = state.admit_chat_for_execute(id).await.unwrap();
        assert!(no_new.inputs.is_none());
        assert!(state
            .session_inbox
            .was_admitted(id, &SessionMessageId::parse(input).unwrap())
            .await
            .unwrap());
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
        assert_eq!(
            state
                .storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .iter()
                .filter(|m| m.id == input)
                .count(),
            1
        );
    }

    struct VisionStartupProvider {
        support: Option<bool>,
        received_image: Arc<AtomicBool>,
    }
    #[async_trait]
    impl LLMProvider for VisionStartupProvider {
        async fn vision_support_override(&self, model: &str) -> Option<bool> {
            if model == "test-model" {
                self.support
            } else {
                None
            }
        }
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            assert_eq!(model, "test-model");
            let has_images = messages.iter().any(|message| {
                message.content_parts.as_ref().is_some_and(|parts| {
                    parts
                        .iter()
                        .any(|part| matches!(part, bamboo_domain::MessagePart::ImageUrl { .. }))
                })
            });
            if self.support == Some(false) {
                assert!(!has_images, "non-Vision main gets no image payload");
                assert!(messages.iter().any(|message| {
                    message.content.contains("bamboo-image://")
                        && message.content.contains("not inspected")
                }));
            } else {
                assert!(has_images, "explicit Vision support receives native images");
            }
            self.received_image.store(true, Ordering::SeqCst);
            LocalProvider.chat_stream(messages, tools, max, model).await
        }
    }
    async fn assert_vision_startup(
        support: Option<bool>,
        model: &str,
        configured_vision: bool,
        followup: bool,
        expected: StatusCode,
    ) {
        let received = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(VisionStartupProvider {
            support,
            received_image: received.clone(),
        });
        let (_home, state, _fault) = state_with_provider(None, provider).await;
        let id = format!("vision-startup-{support:?}-{model}-{configured_vision}-{followup}");
        bootstrap(&state, &id).await;
        let mut session = state.storage.load_session(&id).await.unwrap().unwrap();
        session.model = model.into();
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new("openai", model));
        let message = session
            .messages
            .iter_mut()
            .rev()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::User))
            .unwrap();
        message.content_parts = Some(vec![bamboo_domain::MessagePart::ImageUrl {
            image_url: bamboo_domain::ImageUrlRef {
                url: "data:image/png;base64,iVBORw0KGgo=".into(),
                detail: None,
            },
        }]);
        let image_id = message.id.clone();
        let image_before = serde_json::to_vec(&*message).unwrap();
        if followup {
            session.add_message(bamboo_agent_core::Message::assistant("earlier turn", None));
            session.add_message(bamboo_agent_core::Message::user(
                "inspect the earlier retained image",
            ));
        }
        state.save_and_cache_session(&mut session).await;
        {
            let mut config = state.config.write().await;
            config.hooks.image_fallback.enabled = true;
            config.hooks.image_fallback.mode = "error".into();
            if configured_vision {
                config.features.provider_model_ref = true;
                config.defaults = Some(
                    serde_json::from_value(serde_json::json!({
                        "chat": {"provider": "openai", "model": "test-model"},
                        "fast": {"provider": "openai", "model": "test-model"},
                        "vision": {"provider": "unconstructed-vision", "model": "vision-fixture"}
                    }))
                    .unwrap(),
                );
                config.provider_instances.insert(
                    "unconstructed-vision".into(),
                    serde_json::from_value(serde_json::json!({
                        "provider_type": "not-a-provider", "enabled": true,
                        "model": "vision-fixture", "runtime_models": ["vision-fixture"],
                        "model_capabilities": {"vision-fixture": {"supports_vision": true}}
                    }))
                    .unwrap(),
                );
            }
        }
        let mut events = state.get_session_event_sender(&id).await.subscribe();
        let (status, body) = execute(&state, &id, None).await;
        assert_eq!(status, expected, "{support:?} {model}: {body}");
        if expected == StatusCode::ACCEPTED {
            let run = body["run_id"].as_str().expect("accepted owned Root run");
            wait_for_owned_terminal(&state, &id, run, &mut events).await;
            assert!(
                received.load(Ordering::SeqCst),
                "completed main provider received native image or retained reference"
            );
        } else {
            assert!(
                !received.load(Ordering::SeqCst),
                "rejected images are not dispatched"
            );
        }
        let saved = state.storage.load_session(&id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_vec(
                saved
                    .messages
                    .iter()
                    .find(|message| message.id == image_id)
                    .unwrap()
            )
            .unwrap(),
            image_before,
            "original durable image message is byte-identical",
        );
        if support == Some(false) && !configured_vision {
            let error = body["error"]["message"]
                .as_str()
                .expect("canonical error.message");
            assert!(error.contains("Original images"));
            assert!(error.contains("preserved"));
            assert!(error.contains("defaults.vision"));
            assert!(error.contains("vision_model"));
            assert!(error.contains("genuinely Vision-capable main"));
        }
        if expected == StatusCode::ACCEPTED {
            tokio::time::timeout(std::time::Duration::from_secs(15), state.shutdown())
                .await
                .expect("Vision HTTP fixture infrastructure shutdown");
        } else {
            shutdown_rejected_race(&state, &[&id]).await;
        }
    }

    #[actix_web::test]
    async fn vision_actual_execute_startup_uses_routed_model_before_legacy_error_fallback() {
        for (support, model, expected) in [
            (Some(true), "test-model", StatusCode::ACCEPTED),
            (Some(false), "test-model", StatusCode::BAD_REQUEST),
            (None, "test-model", StatusCode::BAD_REQUEST),
            (Some(true), "other-model", StatusCode::BAD_REQUEST),
        ] {
            assert_vision_startup(support, model, false, false, expected).await;
        }
    }

    #[actix_web::test]
    async fn vision_nonvision_new_upload_reaches_main_with_references() {
        assert_vision_startup(Some(false), "test-model", true, false, StatusCode::ACCEPTED).await;
    }

    #[actix_web::test]
    async fn vision_nonvision_history_followup_reaches_main_with_references() {
        assert_vision_startup(Some(false), "test-model", true, true, StatusCode::ACCEPTED).await;
        assert_vision_startup(
            Some(false),
            "test-model",
            false,
            true,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
}
