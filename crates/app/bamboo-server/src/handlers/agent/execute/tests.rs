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
        let mut state =
            crate::AppState::new_with_provider(home.path().into(), config, provider.clone())
                .await
                .unwrap();
        state.provider_registry = Arc::new(ProviderRegistry::new(
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
            assert_eq!(loads.len(), 1, "one real existing workflow prerequisite");
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
            assert!(!state.agent_runners.read().await.contains_key(&id));
            assert!(state
                .session_inbox
                .was_admitted(&id, &SessionMessageId::parse(rejected).unwrap())
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
            only_id(
                &crate::handlers::agent::chat::admit_for_execute(&state, &id)
                    .await
                    .unwrap()
                    .unwrap(),
                successor["message_id"].as_str().unwrap(),
            );
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
    async fn native_reserved_owner_and_pending_slot_keep_input_until_original_release() {
        use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
        let (_home, state, _fault) = state(None).await;
        let id = "native-pending-reservation";
        state
            .storage
            .save_session(&bamboo_agent_core::Session::new(id, "test-model"))
            .await
            .unwrap();
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
        let request = serde_json::from_value::<crate::handlers::agent::chat::ChatRequest>(serde_json::json!({"session_id":id,"message":"Native before reserved task starts","model":"test-model"})).unwrap();
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
        let admitted = state.admit_chat_for_execute(id).await.unwrap();
        assert_eq!(admitted.inputs.unwrap().observations()[0].input_id(), input);
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
}
