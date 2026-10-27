use std::sync::Arc;

use crate::config::GoldConfig;
use crate::runtime::gold_evaluation::GoldEvaluationResult;
use crate::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::ProviderModelRef;
use bamboo_llm::{LLMProvider, LLMRequestOptions};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app_context::AgentSessionContext;
use crate::session_app::provider_model::session_effective_model_ref;

use super::decision::{parse_gold_auto_answer_decision, GoldAutoAnswerDecision};
use super::prompt::{build_gold_auto_answer_messages, get_gold_auto_answer_tools};

pub(crate) struct GoldAuxiliaryTarget {
    pub(crate) provider: Arc<dyn LLMProvider>,
    pub(crate) model: String,
    pub(crate) timeout_context: crate::runtime::stream::handler::StreamTimeoutContext,
    pub(crate) configured_limit: usize,
}

pub(crate) async fn evaluate_gold_state_for_pending_question(
    state: &dyn AgentSessionContext,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<GoldEvaluationResult, String> {
    let target = resolve_gold_provider_and_model(state, session, gold_config).await?;
    let iteration = session
        .agent_runtime_state
        .as_ref()
        .map(|runtime| runtime.round.current_round)
        .unwrap_or(0);

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
    let session_sender = state.get_session_event_sender(session_id).await;
    let forwarder = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let _ = session_sender.send(event);
        }
    });

    let result = evaluate_gold_state_with_target(
        session_id,
        session,
        gold_config,
        target,
        &event_tx,
        iteration,
    )
    .await
    .map_err(|error| error.to_string());

    drop(event_tx);
    let _ = forwarder.await;
    result
}

pub(crate) async fn evaluate_gold_state_with_target(
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    target: GoldAuxiliaryTarget,
    event_tx: &mpsc::Sender<AgentEvent>,
    iteration: u32,
) -> Result<GoldEvaluationResult, bamboo_agent_core::AgentError> {
    let GoldAuxiliaryTarget {
        provider,
        model,
        timeout_context,
        configured_limit,
    } = target;
    let task_context = TaskLoopContext::from_session(session);
    let budget_provider = provider.clone();
    let budget_model = model.clone();
    let acquire_dispatch_guard = async move {
        crate::runtime::runner::auxiliary_budget::acquire(
            &budget_provider,
            &budget_model,
            configured_limit,
        )
        .await
    };

    crate::runtime::gold_evaluation::evaluate_gold_with_dispatch(
        session,
        task_context.as_ref(),
        gold_config,
        provider,
        &crate::runtime::gold_evaluation::GoldEvalFrame {
            event_tx,
            session_id,
            model: &model,
            timeout_context,
            reasoning_effort: session.reasoning_effort,
            checkpoint: bamboo_agent_core::GoldCheckpoint::Terminal,
            iteration,
        },
        acquire_dispatch_guard,
    )
    .await
}

pub(crate) async fn evaluate_gold_auto_answer_question(
    state: &dyn AgentSessionContext,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    state_evaluation: &GoldEvaluationResult,
) -> Result<GoldAutoAnswerDecision, String> {
    if session.pending_question.is_none() {
        return Ok(GoldAutoAnswerDecision::decline(
            "no pending question available",
        ));
    }

    let target = resolve_gold_provider_and_model(state, session, gold_config).await?;
    evaluate_gold_auto_answer_question_with_target(
        session_id,
        session,
        gold_config,
        state_evaluation,
        target,
    )
    .await
}

pub(crate) async fn evaluate_gold_auto_answer_question_with_target(
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    state_evaluation: &GoldEvaluationResult,
    target: GoldAuxiliaryTarget,
) -> Result<GoldAutoAnswerDecision, String> {
    let Some(pending) = session.pending_question.as_ref() else {
        return Ok(GoldAutoAnswerDecision::decline(
            "no pending question available",
        ));
    };
    let messages = build_gold_auto_answer_messages(session, pending, state_evaluation, gold_config);
    let tools = get_gold_auto_answer_tools();
    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort: session.reasoning_effort,
        parallel_tool_calls: None,
        required_tool: None,
        responses: None,
        request_purpose: Some("gold_auto_answer".to_string()),
        cache: None,
    };

    let cancel_token = CancellationToken::new();
    let _dispatch_guard = crate::runtime::runner::auxiliary_budget::acquire(
        &target.provider,
        &target.model,
        target.configured_limit,
    )
    .await;
    let timeout_context = target.timeout_context.begin_request();
    let stream = crate::runtime::stream::handler::await_stream_bootstrap(
        target.provider.chat_stream_with_options(
            &messages,
            &tools,
            Some(gold_config.max_output_tokens),
            &target.model,
            Some(&request_options),
        ),
        &cancel_token,
        session_id,
        &timeout_context,
    );
    let stream = stream
        .await
        .map_err(|error| format!("provider bootstrap failed: {error}"))?
        .map_err(|error| format!("provider call failed: {error}"))?;
    let stream_output = crate::runtime::stream::handler::consume_llm_stream_silent_with_context(
        stream,
        &cancel_token,
        session_id,
        &timeout_context,
    )
    .await
    .map_err(|error| format!("stream handling failed: {error}"))?;

    Ok(
        parse_gold_auto_answer_decision(&stream_output.tool_calls).unwrap_or_else(|| {
            GoldAutoAnswerDecision::decline(
                "Gold auto-answer returned no structured tool result; declining.",
            )
        }),
    )
}

async fn resolve_gold_provider_and_model(
    state: &dyn AgentSessionContext,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<GoldAuxiliaryTarget, String> {
    // Resolve fast model eagerly for the fallback chain.
    let config_snapshot = state.config().read().await.clone();
    let provider_name = gold_provider_name(&config_snapshot, session);
    let stream_timeout = config_snapshot.stream_timeout;
    let configured_limit = crate::runtime::config::normalize_auxiliary_evaluation_max_concurrency(
        config_snapshot
            .extra
            .get("auxiliary_evaluation_max_concurrency")
            .and_then(serde_json::Value::as_u64),
    );
    let fast_model_name = crate::model_config_helper::resolve_fast_model(
        &config_snapshot,
        &provider_name,
        state.provider_registry(),
    )
    .map(|resolved| resolved.model_name);

    // Prefer: gold_config.model_name → fast model → session model.
    let model = gold_config
        .model_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or(fast_model_name)
        .or_else(|| {
            let model = session.model.trim();
            if model.is_empty() || model == "unknown" {
                None
            } else {
                Some(model.to_string())
            }
        })
        .ok_or_else(|| "gold model name is required".to_string())?;

    if let Some(model_ref) = session_effective_model_ref(session) {
        let target = ProviderModelRef::new(model_ref.provider.clone(), model.clone());
        if let Some(provider) = state.get_provider_for_model_ref(&target) {
            return Ok(gold_auxiliary_target(
                provider,
                model,
                provider_name,
                stream_timeout,
                configured_limit,
            ));
        }
        if let Some(provider) = state.provider_registry().get(&model_ref.provider) {
            return Ok(gold_auxiliary_target(
                provider,
                model,
                provider_name,
                stream_timeout,
                configured_limit,
            ));
        }
        if let Some(provider) = state.get_provider_for_endpoint(&model_ref.provider).await {
            return Ok(gold_auxiliary_target(
                provider,
                model,
                provider_name,
                stream_timeout,
                configured_limit,
            ));
        }
    }

    if let Some(metadata_provider_name) = session
        .metadata
        .get("provider_name")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(provider) = state.provider_registry().get(metadata_provider_name) {
            return Ok(gold_auxiliary_target(
                provider,
                model,
                metadata_provider_name.to_string(),
                stream_timeout,
                configured_limit,
            ));
        }
        if let Some(provider) = state
            .get_provider_for_endpoint(metadata_provider_name)
            .await
        {
            return Ok(gold_auxiliary_target(
                provider,
                model,
                metadata_provider_name.to_string(),
                stream_timeout,
                configured_limit,
            ));
        }
    }

    if let Some(provider) = state.provider_registry().get_default() {
        return Ok(gold_auxiliary_target(
            provider,
            model,
            provider_name,
            stream_timeout,
            configured_limit,
        ));
    }

    Ok(gold_auxiliary_target(
        state.get_provider().await,
        model,
        provider_name,
        stream_timeout,
        configured_limit,
    ))
}

fn gold_provider_name(config: &bamboo_config::Config, session: &Session) -> String {
    session_effective_model_ref(session)
        .map(|r| r.provider.clone())
        .unwrap_or_else(|| config.effective_default_provider().to_string())
}

fn gold_auxiliary_target(
    provider: Arc<dyn LLMProvider>,
    model: String,
    provider_name: String,
    stream_timeout: bamboo_config::StreamTimeoutConfig,
    configured_limit: usize,
) -> GoldAuxiliaryTarget {
    let timeout_context = crate::runtime::stream::handler::StreamTimeoutContext::new(
        stream_timeout,
        Some(&provider_name),
        Some(&model),
    );
    GoldAuxiliaryTarget {
        provider,
        model,
        timeout_context,
        configured_limit,
    }
}

#[cfg(test)]
mod provider_selection_tests {
    use std::sync::{Arc, Mutex};

    use super::gold_provider_name;
    use super::{
        evaluate_gold_auto_answer_question_with_target, evaluate_gold_state_with_target,
        GoldAuxiliaryTarget,
    };
    use crate::runtime::config::GoldConfig;
    use crate::runtime::gold_evaluation::GoldEvaluationResult;
    use crate::runtime::stream::handler::StreamTimeoutContext;
    use bamboo_agent_core::{
        GoldCheckpoint, GoldConfidence, GoldDecision, Message, PendingQuestion,
        PendingQuestionSource, Session, ToolSchema,
    };
    use bamboo_domain::ReasoningEffort;
    use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream};

    #[tokio::test]
    async fn routing_contract_gold_pending_question_keeps_the_fast_provider_model_pair() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        for flag in [None, Some(false), Some(true)] {
            let state = RoutingContext::new(flag);
            let session = RoutingContext::session();
            super::evaluate_gold_state_for_pending_question(
                state.as_ref(),
                &session.id,
                &session,
                &GoldConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                state.requests("fast-b"),
                vec![("cheap-b".into(), "gold_evaluation".into())]
            );
            assert!(state.requests("chat-a").is_empty());
        }
    }

    #[tokio::test]
    async fn routing_contract_gold_invalid_fast_and_provider_failures_never_switch_accounts() {
        use crate::app_context::model_routing_test_support::{Failure, RoutingContext};
        for flag in [None, Some(false), Some(true)] {
            for invalid in 0..3 {
                let state = RoutingContext::new(flag);
                match invalid {
                    0 => {
                        state
                            .config
                            .write()
                            .await
                            .defaults
                            .as_mut()
                            .unwrap()
                            .fast
                            .as_mut()
                            .unwrap()
                            .provider = "missing-account".into()
                    }
                    1 => {
                        state
                            .config
                            .write()
                            .await
                            .provider_instances
                            .get_mut("fast-b")
                            .unwrap()
                            .enabled = false;
                        state.registry.remove("fast-b");
                    }
                    _ => state
                        .registry
                        .set_runtime_models(std::collections::HashMap::from([
                            (
                                "chat-a".into(),
                                vec!["main-a".into(), "legacy-fast-a".into()],
                            ),
                            ("fast-b".into(), Vec::new()),
                        ])),
                }
                let session = RoutingContext::session();
                assert!(super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &GoldConfig::default()
                )
                .await
                .is_err());
                state.assert_no_requests();
            }
            for failure in [Failure::Auth, Failure::Quota, Failure::Network] {
                let state = RoutingContext::new(flag);
                *state.providers["fast-b"].failure.lock().unwrap() = Some(failure);
                let session = RoutingContext::session();
                assert!(super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &GoldConfig::default()
                )
                .await
                .is_err());
                assert_eq!(
                    state.requests("fast-b"),
                    vec![("cheap-b".into(), "gold_evaluation".into())]
                );
                assert!(state.requests("chat-a").is_empty());
                assert!(state.requests("memory-c").is_empty());
            }
        }
    }

    #[derive(Default)]
    struct RecordingRejectingProvider {
        requests: Mutex<Vec<(String, Option<ReasoningEffort>)>>,
    }

    #[async_trait::async_trait]
    impl LLMProvider for RecordingRejectingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("Gold requests must carry request controls")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let options = options.expect("request controls");
            self.requests.lock().unwrap().push((
                options.request_purpose.clone().expect("request purpose"),
                options.reasoning_effort,
            ));
            Err(LLMError::Api(
                "fixture rejected selected effort".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn gold_evaluator_requests_preserve_selected_reasoning_effort() {
        for effort in [
            None,
            Some(ReasoningEffort::Disabled),
            Some(ReasoningEffort::Low),
            Some(ReasoningEffort::Medium),
            Some(ReasoningEffort::High),
            Some(ReasoningEffort::Xhigh),
            Some(ReasoningEffort::Max),
        ] {
            let mut session = Session::new("gold-effort-selection", "session-model");
            session.reasoning_effort = effort;
            session.pending_question = Some(PendingQuestion {
                tool_call_id: "pending-question".to_string(),
                tool_name: "conclusion_with_options".to_string(),
                question: "Continue?".to_string(),
                options: vec!["Continue".to_string()],
                allow_custom: false,
                source: PendingQuestionSource::PauseTool,
            });
            let provider = Arc::new(RecordingRejectingProvider::default());
            let target = || GoldAuxiliaryTarget {
                provider: provider.clone(),
                model: "gold-effort-model".to_string(),
                timeout_context: StreamTimeoutContext::default(),
                configured_limit: 1,
            };
            let config = GoldConfig::default();
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(2);
            let error = evaluate_gold_state_with_target(
                &session.id,
                &session,
                &config,
                target(),
                &event_tx,
                0,
            )
            .await
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("fixture rejected selected effort"));

            let state_evaluation = GoldEvaluationResult {
                checkpoint: GoldCheckpoint::Terminal,
                iteration: 0,
                decision: GoldDecision::Continue,
                confidence: GoldConfidence::High,
                reasoning: "fixture state evaluation".to_string(),
                missing_information: Vec::new(),
                next_action: None,
                prompt_tokens: 0,
                completion_tokens: 0,
            };
            let error = evaluate_gold_auto_answer_question_with_target(
                &session.id,
                &session,
                &config,
                &state_evaluation,
                target(),
            )
            .await
            .unwrap_err();
            assert!(error.contains("fixture rejected selected effort"));
            assert_eq!(
                provider.requests.lock().unwrap().as_slice(),
                &[
                    ("gold_evaluation".to_string(), effort),
                    ("gold_auto_answer".to_string(), effort)
                ],
            );
        }
    }

    #[test]
    fn gold_provider_fallback_uses_default_provider_instance() {
        let mut config = bamboo_config::Config::default();
        let instance = serde_json::from_value(serde_json::json!({
            "provider_type": "anthropic",
            "enabled": true
        }))
        .unwrap();
        config.provider = "openai".to_string();
        config
            .provider_instances
            .insert("gold-anthropic".to_string(), instance);
        config.default_provider_instance = Some("gold-anthropic".to_string());
        let session = Session::new("gold-provider-instance", "model");

        assert_eq!(gold_provider_name(&config, &session), "gold-anthropic");
    }
}
