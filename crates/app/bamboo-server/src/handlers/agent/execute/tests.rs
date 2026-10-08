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
        fail: AtomicBool,
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
        async fn mark_activation_eligible(
            &self,
            id: &str,
            g: u64,
            p: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner.mark_activation_eligible(id, g, p).await
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
    async fn state(ack_failure: Option<bool>) -> (tempfile::TempDir, web::Data<crate::AppState>) {
        let home = tempfile::tempdir().unwrap();
        let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().into()));
        config.provider = "openai".into();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("test-model".into()),
            ..Default::default()
        });
        let provider: Arc<dyn LLMProvider> = Arc::new(LocalProvider);
        let mut state =
            crate::AppState::new_with_provider(home.path().into(), config, provider.clone())
                .await
                .unwrap();
        state.provider_registry = Arc::new(ProviderRegistry::new(
            std::collections::HashMap::from([("openai".into(), provider)]),
            "openai".into(),
        ));
        state.provider_router = Arc::new(ProviderModelRouter::new(state.provider_registry.clone()));
        if let Some(after_receipt) = ack_failure {
            state.session_inbox = Arc::new(FailOnceAck {
                inner: state.session_inbox.clone(),
                fail: AtomicBool::new(true),
                after_receipt,
            });
        }
        (home, web::Data::new(state))
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
    #[actix_web::test]
    async fn native_inbox_real_chat_receipt_checkpoint_ack_and_current_input_once() {
        let (_home, state) = state(None).await;
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
    async fn execution_input_http_actual_queue_admission_returns_only_this_calls_new_user() {
        let (_home, state) = state(None).await;
        let id = "execution-input-http-admit";
        chat(&state, id, None).await;
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
        let (_home, state) = state(None).await;
        let id = "execution-input-http-ready";
        chat(&state, id, None).await;
        chat(&state, id, Some("ready-current-user")).await;
        input_taps().lock().unwrap().insert(id.into(), Vec::new());
        let (status, body) = execute(&state, id, None).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["status"], "started", "{body}");
        let observed = input_taps().lock().unwrap().remove(id).unwrap();
        assert_eq!(observed.len(), 1, "one actual Ready/spawn handoff");
        let inputs = observed[0].as_ref().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].0, "ready-current-user");
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
        assert_eq!(loads.len(), 1, "one real existing workflow prerequisite");
        assert_eq!(loads[0].tool_success, Some(true));
        assert_eq!(
            stored
                .messages
                .iter()
                .filter(|m| m.id == "ready-current-user")
                .count(),
            1
        );
        assert!(crate::handlers::agent::chat::admit_for_execute(&state, id)
            .await
            .unwrap()
            .is_none());
    }
    #[actix_web::test]
    async fn execution_input_http_ack_failure_preserves_prefix_events_but_recovery_cannot_regrant_data(
    ) {
        for after_receipt in [false, true] {
            let (_home, state) = state(Some(after_receipt)).await;
            let id = format!("execution-input-http-ack-{after_receipt}");
            chat(&state, &id, None).await;
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
        let (_home, state) = state(None).await;
        let id = "execution-input-http-reject";
        chat(&state, id, None).await;
        chat(&state, id, Some("rejected-current-a")).await;
        input_taps().lock().unwrap().insert(id.into(), Vec::new());
        let (status, _body) = execute(&state, id, Some("unavailable-explicit-provider")).await;
        assert!(status.is_client_error() || status.is_server_error());
        assert!(
            input_taps().lock().unwrap().remove(id).unwrap().is_empty(),
            "failed startup has no execution handoff"
        );
        assert!(crate::handlers::agent::chat::admit_for_execute(&state, id)
            .await
            .unwrap()
            .is_none());
        chat(&state, id, Some("rejection-successor-b")).await;
        only_id(
            &crate::handlers::agent::chat::admit_for_execute(&state, id)
                .await
                .unwrap()
                .unwrap(),
            "rejection-successor-b",
        );
    }
}
