use std::sync::Arc;

use crate::config::GoldConfig;
use crate::runtime::gold_evaluation::GoldEvaluationResult;
use crate::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::reasoning::ReasoningEffort;
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
        reasoning_effort: normalize_lightweight_reasoning_effort(session.reasoning_effort),
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
    let provider_name = session_effective_model_ref(session)
        .map(|r| r.provider.clone())
        .unwrap_or_else(|| config_snapshot.provider.clone());
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

fn normalize_lightweight_reasoning_effort(
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<ReasoningEffort> {
    reasoning_effort.map(|effort| match effort {
        ReasoningEffort::Xhigh | ReasoningEffort::Max => ReasoningEffort::High,
        other => other,
    })
}
