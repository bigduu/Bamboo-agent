use std::sync::Arc;

use actix_web::web::Data;
use bamboo_agent_core::{
    AgentEvent, FunctionSchema, GoldConfidence, GoldDecision, Message, PendingQuestion,
    PendingQuestionSource, Role, Session, ToolCall, ToolSchema,
};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::ProviderModelRef;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::runtime::gold_evaluation::{evaluate_gold, GoldEvaluationResult};
use bamboo_engine::runtime::stream::handler::consume_llm_stream_silent;
use bamboo_engine::TaskLoopContext;
use bamboo_infrastructure::{LLMProvider, LLMRequestOptions};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app_state::{resume_adapter::AppStateResumeRef, AppState};
use crate::events::publish_replayable_session_event;
use crate::model_config_helper::{
    resolve_background_model, resolve_fast_model, resolve_gold_config, resolve_provider_type,
    resolve_task_summary_model, GOLD_CONFIG_METADATA_KEY,
};
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::respond::{
    submit_pending_response_with_source, PlanModeTransition, ResponseSource,
};
use crate::session_app::resume::resume_session_execution;
use crate::session_app::types::{RespondInput, ResumeConfigSnapshot, ResumeOutcome};

const GOLD_AUTO_ANSWER_TOOL_NAME: &str = "report_gold_auto_answer";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GoldAutoAnswerOutcome {
    Skipped {
        reason: String,
    },
    Applied {
        answer: String,
        resume_outcome: ResumeOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GoldAutoAnswerDecision {
    apply: bool,
    answer: Option<String>,
    confidence: GoldConfidence,
    reasoning: String,
}

impl GoldAutoAnswerDecision {
    fn decline(reason: impl Into<String>) -> Self {
        Self {
            apply: false,
            answer: None,
            confidence: GoldConfidence::Low,
            reasoning: reason.into(),
        }
    }
}

pub(crate) async fn maybe_auto_answer_pending_question(
    state: Data<AppState>,
    session_id: &str,
    gold_config_override: Option<GoldConfig>,
) -> GoldAutoAnswerOutcome {
    let Some(session) = state.load_session_merged(session_id).await else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "session_not_found".to_string(),
        };
    };

    let config_snapshot = state.config.read().await.clone();
    let Some(gold_config) = gold_config_override.or_else(|| {
        resolve_gold_config(
            &config_snapshot,
            session
                .metadata
                .get(GOLD_CONFIG_METADATA_KEY)
                .map(String::as_str),
        )
    }) else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "gold_config_unavailable".to_string(),
        };
    };

    let Some(pending_question) = session.pending_question.as_ref() else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "no_pending_question".to_string(),
        };
    };

    if !gold_config.enabled {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "gold_disabled".to_string(),
        };
    }

    if !gold_config.auto_answer_enabled {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "gold_auto_answer_disabled".to_string(),
        };
    }

    if !session_is_awaiting_clarification(&session) {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "session_not_awaiting_clarification".to_string(),
        };
    }

    if !should_attempt_gold_auto_answer(pending_question) {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "pending_question_not_whitelisted".to_string(),
        };
    }

    let state_evaluation = match evaluate_gold_state_for_pending_question(
        state.get_ref(),
        session_id,
        &session,
        &gold_config,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(
                session_id = %session_id,
                error = %error,
                "Gold auto-answer skipped because Gold state evaluation failed"
            );
            return GoldAutoAnswerOutcome::Skipped {
                reason: format!("state_evaluation_failed:{error}"),
            };
        }
    };

    if !state_evaluation
        .confidence
        .meets(gold_config.min_auto_continue_confidence)
    {
        return GoldAutoAnswerOutcome::Skipped {
            reason: format!(
                "state_evaluation_confidence_{}",
                state_evaluation.confidence.as_str()
            ),
        };
    }

    if !matches!(
        state_evaluation.decision,
        GoldDecision::Continue | GoldDecision::NeedInput
    ) {
        return GoldAutoAnswerOutcome::Skipped {
            reason: format!(
                "state_evaluation_decision_{}",
                state_evaluation.decision.as_str()
            ),
        };
    }

    let answer_decision = match evaluate_gold_auto_answer_question(
        state.get_ref(),
        session_id,
        &session,
        &gold_config,
        &state_evaluation,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(
                session_id = %session_id,
                error = %error,
                "Gold auto-answer skipped because question evaluation failed"
            );
            return GoldAutoAnswerOutcome::Skipped {
                reason: format!("question_evaluation_failed:{error}"),
            };
        }
    };

    if !answer_decision.apply {
        return GoldAutoAnswerOutcome::Skipped {
            reason: format!("question_decision_declined:{}", answer_decision.reasoning),
        };
    }

    if !answer_decision
        .confidence
        .meets(gold_config.min_auto_continue_confidence)
    {
        return GoldAutoAnswerOutcome::Skipped {
            reason: format!(
                "question_decision_confidence_{}",
                answer_decision.confidence.as_str()
            ),
        };
    }

    let Some(raw_answer) = answer_decision.answer.as_deref() else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "question_decision_missing_answer".to_string(),
        };
    };

    let Some(answer) = canonicalize_pending_answer(pending_question, raw_answer) else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "question_decision_answer_not_canonical".to_string(),
        };
    };

    tracing::info!(
        session_id = %session_id,
        tool_name = %pending_question.tool_name,
        answer = %answer,
        reasoning = %answer_decision.reasoning,
        "Applying Gold auto-answer for pending clarification"
    );

    let respond_input = RespondInput {
        session_id: session_id.to_string(),
        user_response: answer.clone(),
        model: None,
        model_ref: None,
        provider: None,
        reasoning_effort: session.reasoning_effort,
    };

    let (updated_session, _submitted_answer, plan_mode_transition) =
        match submit_pending_response_with_source(
            state.as_ref(),
            respond_input,
            ResponseSource::Gold,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %error,
                    "Gold auto-answer skipped because submitting the response failed"
                );
                return GoldAutoAnswerOutcome::Skipped {
                    reason: format!("submit_pending_response_failed:{error}"),
                };
            }
        };

    if let Some(event) = plan_mode_transition_event(session_id, plan_mode_transition.as_ref()) {
        publish_replayable_session_event(state.get_ref(), session_id, event).await;
    }

    let resume_config =
        build_resume_config_snapshot(state.get_ref(), &updated_session, Some(gold_config.clone()))
            .await;
    let resume_outcome =
        resume_session_execution(&AppStateResumeRef(state.clone()), session_id, resume_config)
            .await;

    tracing::info!(
        session_id = %session_id,
        resume_status = %resume_outcome.status_str(),
        resume_run_id = %resume_outcome.run_id().map(String::as_str).unwrap_or_default(),
        "Gold auto-answer completed"
    );

    GoldAutoAnswerOutcome::Applied {
        answer,
        resume_outcome,
    }
}

fn should_attempt_gold_auto_answer(pending: &PendingQuestion) -> bool {
    if matches!(pending.source, PendingQuestionSource::Gold) {
        return false;
    }

    matches!(
        normalized_pending_tool_name(&pending.tool_name).as_str(),
        "ExitPlanMode" | "conclusion_with_options"
    )
}

fn session_is_awaiting_clarification(session: &Session) -> bool {
    if session
        .metadata
        .get("runtime.suspend_reason")
        .map(String::as_str)
        == Some("awaiting_clarification")
    {
        return true;
    }

    session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.suspension.as_ref())
        .map(|suspension| suspension.reason.as_str() == "awaiting_clarification")
        .unwrap_or(false)
}

fn normalized_pending_tool_name(tool_name: &str) -> String {
    let trimmed = tool_name.trim();
    if trimmed.eq_ignore_ascii_case("conclusion_with_options")
        || trimmed.eq_ignore_ascii_case("ConclusionWithOptions")
        || trimmed.eq_ignore_ascii_case("conclusionWithOptions")
    {
        return "conclusion_with_options".to_string();
    }
    if trimmed.eq_ignore_ascii_case("ExitPlanMode") {
        return "ExitPlanMode".to_string();
    }
    if trimmed.eq_ignore_ascii_case("request_permissions")
        || trimmed.eq_ignore_ascii_case("RequestPermissions")
    {
        return "request_permissions".to_string();
    }

    bamboo_tools::normalize_tool_ref(trimmed).unwrap_or_else(|| trimmed.to_string())
}

async fn evaluate_gold_state_for_pending_question(
    state: &AppState,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<GoldEvaluationResult, String> {
    let task_context = TaskLoopContext::from_session(session);
    let (provider, model) = resolve_gold_provider_and_model(state, session, gold_config).await?;
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

    let result = evaluate_gold(
        session,
        task_context.as_ref(),
        gold_config,
        provider,
        &event_tx,
        session_id,
        &model,
        session.reasoning_effort,
        bamboo_agent_core::GoldCheckpoint::Terminal,
        iteration,
    )
    .await
    .map_err(|error| error.to_string());

    drop(event_tx);
    let _ = forwarder.await;
    result
}

async fn evaluate_gold_auto_answer_question(
    state: &AppState,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    state_evaluation: &GoldEvaluationResult,
) -> Result<GoldAutoAnswerDecision, String> {
    let Some(pending) = session.pending_question.as_ref() else {
        return Ok(GoldAutoAnswerDecision::decline(
            "no pending question available",
        ));
    };

    let (provider, model) = resolve_gold_provider_and_model(state, session, gold_config).await?;
    let messages = build_gold_auto_answer_messages(session, pending, state_evaluation, gold_config);
    let tools = get_gold_auto_answer_tools();
    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort: normalize_lightweight_reasoning_effort(session.reasoning_effort),
        parallel_tool_calls: None,
        responses: None,
        request_purpose: Some("gold_auto_answer".to_string()),
    };

    let stream = provider
        .chat_stream_with_options(
            &messages,
            &tools,
            Some(gold_config.max_output_tokens),
            &model,
            Some(&request_options),
        )
        .await
        .map_err(|error| format!("provider call failed: {error}"))?;

    let stream_output = consume_llm_stream_silent(stream, &CancellationToken::new(), session_id)
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
    state: &AppState,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<(Arc<dyn LLMProvider>, String), String> {
    // Resolve fast model eagerly for the fallback chain.
    let config_snapshot = state.config.read().await.clone();
    let provider_name = session_effective_model_ref(session)
        .map(|r| r.provider.clone())
        .unwrap_or_else(|| config_snapshot.provider.clone());
    let fast_model_name = crate::model_config_helper::resolve_fast_model(
        &config_snapshot,
        &provider_name,
        &state.provider_registry,
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
        if let Ok(provider) = state.get_provider_for_model_ref(&target) {
            return Ok((provider, model));
        }
        if let Some(provider) = state.provider_registry.get(&model_ref.provider) {
            return Ok((provider, model));
        }
        if let Ok(provider) = state.get_provider_for_endpoint(&model_ref.provider).await {
            return Ok((provider, model));
        }
    }

    if let Some(provider_name) = session
        .metadata
        .get("provider_name")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(provider) = state.provider_registry.get(provider_name) {
            return Ok((provider, model));
        }
        if let Ok(provider) = state.get_provider_for_endpoint(provider_name).await {
            return Ok((provider, model));
        }
    }

    if let Some(provider) = state.provider_registry.get_default() {
        return Ok((provider, model));
    }

    Ok((state.get_provider().await, model))
}

fn gold_model_name(session: &Session, gold_config: &GoldConfig) -> Option<String> {
    gold_config
        .model_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            let model = session.model.trim();
            if model.is_empty() || model == "unknown" {
                None
            } else {
                Some(model.to_string())
            }
        })
}

fn normalize_lightweight_reasoning_effort(
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<ReasoningEffort> {
    reasoning_effort.map(|effort| match effort {
        ReasoningEffort::Xhigh | ReasoningEffort::Max => ReasoningEffort::High,
        other => other,
    })
}

fn build_gold_auto_answer_messages(
    session: &Session,
    pending: &PendingQuestion,
    state_evaluation: &GoldEvaluationResult,
    gold_config: &GoldConfig,
) -> Vec<Message> {
    let mut messages = Vec::new();

    let mut system_prompt = String::from(
        "You are a cautious Gold auto-answer evaluator. Decide whether the server should automatically answer the pending clarification for the agent.\n\nRules:\n1. This is Phase 2 low-risk auto-answer only.\n2. Only auto-answer if the answer is clearly derivable from the current session and task context.\n3. Never auto-answer permissions, credentials, secrets, external auth, or ambiguous high-risk questions.\n4. If options are provided, choose one of the provided options verbatim.\n5. If you are unsure, return apply=false.\n6. Use confidence=high only when the answer is clearly low-risk.\n7. Call report_gold_auto_answer exactly once."
    );

    if let Some(extra) = gold_config
        .evaluation_prompt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        system_prompt.push_str("\n\nAdditional instructions:\n");
        system_prompt.push_str(extra);
    }

    messages.push(Message::system(system_prompt));

    let task_summary = TaskLoopContext::from_session(session)
        .map(|context| context.format_for_prompt())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "## Current Task List\nNo task list available.".to_string());

    let options_summary = if pending.options.is_empty() {
        "none".to_string()
    } else {
        pending
            .options
            .iter()
            .map(|option| format!("- {option}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let recent_messages = format_recent_messages(session, 6);

    let user_prompt = format!(
        "## Pending Question\nquestion={}\ntool={}\nnormalized_tool={}\nsource={:?}\nallow_custom={}\n\n## Options\n{}\n\n## Gold State Evaluation\ndecision={}\nconfidence={}\nreasoning={}\n\n{}\n\n## Recent Conversation\n{}\n\n## Instruction\nReturn apply=true only if you can safely choose the answer right now. When options exist, answer with the exact option text.",
        pending.question,
        pending.tool_name,
        normalized_pending_tool_name(&pending.tool_name),
        pending.source,
        pending.allow_custom,
        options_summary,
        state_evaluation.decision.as_str(),
        state_evaluation.confidence.as_str(),
        state_evaluation.reasoning,
        task_summary,
        recent_messages,
    );

    messages.push(Message::user(user_prompt));
    messages
}

fn format_recent_messages(session: &Session, limit: usize) -> String {
    let start = session.messages.len().saturating_sub(limit);
    let mut lines = Vec::new();

    for message in session.messages.iter().skip(start) {
        let role = match message.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        };

        let mut content = message.content.trim().replace('\n', " ");
        if content.chars().count() > 240 {
            content = format!("{}…", content.chars().take(240).collect::<String>());
        }
        if content.is_empty() {
            content = "<empty>".to_string();
        }

        lines.push(format!("- [{role}] {content}"));
    }

    if lines.is_empty() {
        "- <no messages>".to_string()
    } else {
        lines.join("\n")
    }
}

fn get_gold_auto_answer_tools() -> Vec<ToolSchema> {
    vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: GOLD_AUTO_ANSWER_TOOL_NAME.to_string(),
            description: "Report whether Gold should auto-answer the pending clarification"
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "apply": {
                        "type": "boolean"
                    },
                    "answer": {
                        "type": "string",
                        "description": "The exact answer text to submit when apply=true. Use an empty string when apply=false."
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["low", "medium", "high"]
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Short concrete reasoning for the decision"
                    }
                },
                "required": ["apply", "answer", "confidence", "reasoning"],
                "additionalProperties": false
            }),
        },
    }]
}

fn parse_gold_auto_answer_decision(tool_calls: &[ToolCall]) -> Option<GoldAutoAnswerDecision> {
    for tool_call in tool_calls {
        if tool_call.function.name != GOLD_AUTO_ANSWER_TOOL_NAME {
            continue;
        }

        let Ok(args) = serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
        else {
            continue;
        };

        let apply = args
            .get("apply")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let answer = args
            .get("answer")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let confidence = match args.get("confidence").and_then(|value| value.as_str()) {
            Some("high") => GoldConfidence::High,
            Some("medium") => GoldConfidence::Medium,
            _ => GoldConfidence::Low,
        };
        let reasoning = args
            .get("reasoning")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("Gold auto-answer produced no reasoning")
            .to_string();

        return Some(GoldAutoAnswerDecision {
            apply,
            answer,
            confidence,
            reasoning,
        });
    }

    None
}

fn canonicalize_pending_answer(pending: &PendingQuestion, raw_answer: &str) -> Option<String> {
    if pending.options.is_empty() {
        return None;
    }

    canonicalize_option(raw_answer, &pending.options)
}

fn canonicalize_option(raw_answer: &str, options: &[String]) -> Option<String> {
    let trimmed_answer = raw_answer.trim();
    if trimmed_answer.is_empty() {
        return None;
    }

    if let Some(exact) = options
        .iter()
        .find(|option| option.trim() == trimmed_answer)
    {
        return Some(exact.clone());
    }

    let normalized_answer = normalize_answer_key(trimmed_answer);
    if normalized_answer.is_empty() {
        return None;
    }

    let matches = options
        .iter()
        .filter(|option| normalize_answer_key(option) == normalized_answer)
        .collect::<Vec<_>>();

    if matches.len() == 1 {
        Some(matches[0].clone())
    } else {
        None
    }
}

fn normalize_answer_key(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`'))
        .trim()
        .trim_end_matches(|ch: char| matches!(ch, '.' | '。' | '!' | '！' | '?' | '？'))
        .trim()
        .to_ascii_lowercase()
}

async fn build_resume_config_snapshot(
    state: &AppState,
    session: &Session,
    gold_config_override: Option<GoldConfig>,
) -> ResumeConfigSnapshot {
    let config_snapshot = state.config.read().await.clone();
    let resolved_provider_name = session_effective_model_ref(session)
        .map(|model_ref| model_ref.provider)
        .unwrap_or_else(|| config_snapshot.provider.clone());
    let resolved_provider_type = resolve_provider_type(
        &config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );
    let resolved_fast = resolve_fast_model(
        &config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );
    let resolved_background = resolve_background_model(
        &config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );
    let resolved_summarization = resolve_task_summary_model(
        &config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );

    ResumeConfigSnapshot {
        provider_name: resolved_provider_name,
        provider_type: resolved_provider_type,
        fast_model: resolved_fast.as_ref().map(|model| model.model_name.clone()),
        fast_model_ref: config_snapshot
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.fast.clone()),
        background_model: resolved_background
            .as_ref()
            .map(|model| model.model_name.clone()),
        background_model_ref: config_snapshot
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.memory_background.clone()),
        background_model_provider: resolved_background.map(|model| model.provider),
        summarization_model: resolved_summarization
            .as_ref()
            .map(|model| model.model_name.clone()),
        summarization_model_ref: config_snapshot
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.task_summary.clone()),
        summarization_model_provider: resolved_summarization.map(|model| model.provider),
        disabled_tools: config_snapshot.disabled_tool_names(),
        disabled_skill_ids: config_snapshot.disabled_skill_ids(),
        image_fallback: crate::handlers::agent::execute::image_fallback::resolve_image_fallback(
            &config_snapshot,
        )
        .ok()
        .flatten(),
        gold_config: gold_config_override.or_else(|| {
            resolve_gold_config(
                &config_snapshot,
                session
                    .metadata
                    .get(GOLD_CONFIG_METADATA_KEY)
                    .map(String::as_str),
            )
        }),
    }
}

fn plan_mode_transition_event(
    session_id: &str,
    transition: Option<&PlanModeTransition>,
) -> Option<AgentEvent> {
    transition.map(|transition| match transition {
        PlanModeTransition::Entered {
            reason,
            pre_permission_mode,
            entered_at,
            status,
            plan_file_path,
        } => AgentEvent::PlanModeEntered {
            session_id: session_id.to_string(),
            reason: reason.clone(),
            pre_permission_mode: pre_permission_mode.clone(),
            entered_at: *entered_at,
            status: *status,
            plan_file_path: plan_file_path.clone(),
        },
        PlanModeTransition::Exited {
            approved,
            restored_mode,
            plan,
        } => AgentEvent::PlanModeExited {
            session_id: session_id.to_string(),
            approved: *approved,
            restored_mode: restored_mode.clone(),
            plan: plan.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AgentStatus;
    use crate::session_app::execute::has_pending_clarification_resume;
    use actix_web::web::Data;
    use async_trait::async_trait;
    use bamboo_agent_core::FunctionCall;
    use bamboo_domain::session::runtime_state::{
        AgentRuntimeState, AgentStatusState, PlanModeState, PlanModeStatus, SuspensionState,
    };
    use bamboo_infrastructure::{
        Config, LLMChunk, LLMError, LLMStream, ProviderModelRouter, ProviderRegistry,
    };
    use futures::stream;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{broadcast, Semaphore};
    use tokio::time::{sleep, timeout, Duration};

    fn pending_question(tool_name: &str) -> PendingQuestion {
        PendingQuestion {
            tool_call_id: format!("call::{tool_name}"),
            tool_name: tool_name.to_string(),
            question: "Question?".to_string(),
            options: vec!["OK".to_string(), "Need changes".to_string()],
            allow_custom: true,
            source: PendingQuestionSource::PauseTool,
        }
    }

    fn auto_answer_call(arguments: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: GOLD_AUTO_ANSWER_TOOL_NAME.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedRequest {
        purpose: String,
        model: String,
    }

    struct ScriptedProvider {
        auto_answer: String,
        requests: Mutex<Vec<RecordedRequest>>,
        agent_loop_gate: Arc<Semaphore>,
    }

    impl ScriptedProvider {
        fn new(auto_answer: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                auto_answer: auto_answer.into(),
                requests: Mutex::new(Vec::new()),
                agent_loop_gate: Arc::new(Semaphore::new(0)),
            })
        }

        fn request_purposes(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("requests lock")
                .iter()
                .map(|request| request.purpose.clone())
                .collect()
        }

        fn release_agent_loop(&self) {
            self.agent_loop_gate.add_permits(1);
        }
    }

    #[async_trait]
    impl LLMProvider for ScriptedProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("chat_stream should not be called directly in this test")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let purpose = options
                .and_then(|value| value.request_purpose.clone())
                .unwrap_or_else(|| "unknown".to_string());
            self.requests
                .lock()
                .expect("requests lock")
                .push(RecordedRequest {
                    purpose: purpose.clone(),
                    model: model.to_string(),
                });

            match purpose.as_str() {
                "gold_evaluation" => Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![gold_evaluation_call(json!({
                        "decision": "continue",
                        "confidence": "high",
                        "reasoning": "The clarification can be answered safely and execution should continue."
                    }))])),
                    Ok(LLMChunk::Done),
                ]))),
                "gold_auto_answer" => Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::ToolCalls(vec![auto_answer_call(json!({
                        "apply": true,
                        "answer": self.auto_answer,
                        "confidence": "high",
                        "reasoning": "The answer is an exact low-risk option already supported by the session context."
                    }))])),
                    Ok(LLMChunk::Done),
                ]))),
                "agent_loop" => {
                    let gate = self.agent_loop_gate.clone();
                    Ok(Box::pin(async_stream::stream! {
                        let _permit = gate.acquire().await.expect("agent loop gate should stay open");
                        yield Ok::<LLMChunk, LLMError>(LLMChunk::Token("done".to_string()));
                        yield Ok::<LLMChunk, LLMError>(LLMChunk::Done);
                    }))
                }
                other => Err(LLMError::Api(format!(
                    "unexpected request_purpose in ScriptedProvider: {other}"
                ))),
            }
        }
    }

    fn gold_evaluation_call(arguments: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "gold-evaluation-call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "report_gold_evaluation".to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    fn test_gold_config() -> GoldConfig {
        GoldConfig {
            enabled: true,
            auto_answer_enabled: true,
            auto_continue_enabled: false,
            model_name: Some("test-model".to_string()),
            max_output_tokens: 256,
            max_auto_continuations: 3,
            ..GoldConfig::default()
        }
    }

    fn awaiting_clarification_state(run_id: &str) -> AgentRuntimeState {
        let mut runtime_state = AgentRuntimeState::new(run_id.to_string());
        runtime_state.status = AgentStatusState::Suspended;
        runtime_state.round.current_round = 3;
        runtime_state.round.last_round_id = Some("round-3".to_string());
        runtime_state.suspension = Some(SuspensionState {
            reason: "awaiting_clarification".to_string(),
            suspended_at: chrono::Utc::now(),
            resumable: true,
            hook_point: Some("AfterToolExecution".to_string()),
        });
        runtime_state
    }

    fn build_pending_session(
        session_id: &str,
        tool_call_id: &str,
        tool_name: &str,
        question: &str,
        options: &[&str],
        allow_custom: bool,
        tool_result_payload: serde_json::Value,
    ) -> Session {
        let mut session = Session::new(session_id, "test-model");
        session.add_message(Message::user(
            "Please continue once the clarification has been resolved.",
        ));
        session.add_message(Message::assistant(
            "I need a clarification before I can continue.",
            None,
        ));
        session.add_message(Message::tool_result_with_status(
            tool_call_id.to_string(),
            tool_result_payload.to_string(),
            true,
        ));
        session.set_pending_question_with_source(
            tool_call_id.to_string(),
            tool_name.to_string(),
            question.to_string(),
            options.iter().map(|option| option.to_string()).collect(),
            allow_custom,
            PendingQuestionSource::PauseTool,
        );
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "awaiting_clarification".to_string(),
        );
        session.agent_runtime_state =
            Some(awaiting_clarification_state("run-awaiting-clarification"));
        session
    }

    #[test]
    fn should_attempt_gold_auto_answer_allows_exit_plan_mode() {
        assert!(should_attempt_gold_auto_answer(&pending_question(
            "ExitPlanMode"
        )));
    }

    #[test]
    fn should_attempt_gold_auto_answer_allows_conclusion_with_options() {
        assert!(should_attempt_gold_auto_answer(&pending_question(
            "conclusion_with_options"
        )));
    }

    #[test]
    fn should_attempt_gold_auto_answer_rejects_request_permissions() {
        assert!(!should_attempt_gold_auto_answer(&pending_question(
            "request_permissions"
        )));
    }

    #[test]
    fn should_attempt_gold_auto_answer_rejects_gold_origin_questions() {
        let mut pending = pending_question("conclusion_with_options");
        pending.source = PendingQuestionSource::Gold;
        assert!(!should_attempt_gold_auto_answer(&pending));
    }

    #[test]
    fn parse_gold_auto_answer_decision_reads_structured_fields() {
        let parsed = parse_gold_auto_answer_decision(&[auto_answer_call(json!({
            "apply": true,
            "answer": "OK",
            "confidence": "high",
            "reasoning": "The option is explicitly supported"
        }))])
        .expect("decision should parse");

        assert!(parsed.apply);
        assert_eq!(parsed.answer.as_deref(), Some("OK"));
        assert_eq!(parsed.confidence, GoldConfidence::High);
        assert_eq!(parsed.reasoning, "The option is explicitly supported");
    }

    #[test]
    fn canonicalize_pending_answer_uses_existing_option_casing() {
        let pending = pending_question("conclusion_with_options");
        let answer = canonicalize_pending_answer(&pending, "ok.");
        assert_eq!(answer.as_deref(), Some("OK"));
    }

    #[test]
    fn canonicalize_pending_answer_rejects_free_text() {
        let pending = pending_question("conclusion_with_options");
        let answer = canonicalize_pending_answer(&pending, "Ship it");
        assert!(answer.is_none());
    }

    #[tokio::test]
    async fn gold_auto_answer_conclusion_with_options_full_loop() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        config.provider = String::new();
        config.features.provider_model_ref = true;
        let provider = ScriptedProvider::new("ok.");
        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state =
            AppState::new_with_provider(temp_dir.path().to_path_buf(), config, provider_trait)
                .await
                .expect("app state");
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        let state = Data::new(app_state);

        let session_id = "gold-auto-answer-conclusion-full-loop";
        let tool_call_id = "call-conclusion";
        let mut session = build_pending_session(
            session_id,
            tool_call_id,
            "conclusion_with_options",
            "Any other requests before I finish?",
            &["OK", "Need changes"],
            true,
            json!({
                "summary": "Core validation is complete and release is ready."
            }),
        );
        state.save_and_cache_session(&mut session).await;

        let outcome =
            maybe_auto_answer_pending_question(state.clone(), session_id, Some(test_gold_config()))
                .await;

        let run_id = match &outcome {
            GoldAutoAnswerOutcome::Applied {
                answer,
                resume_outcome,
            } => {
                assert_eq!(answer, "OK");
                match resume_outcome {
                    ResumeOutcome::Started { run_id } => run_id.clone(),
                    other => panic!("expected resume to start, got {other:?}"),
                }
            }
            other => panic!("expected Gold auto-answer to apply, got {other:?}"),
        };

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        let after_respond = state
            .load_session_merged(session_id)
            .await
            .expect("session should exist after respond");
        assert!(after_respond.pending_question.is_none());
        assert!(after_respond.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some(tool_call_id)
                && message.content == "Auto-selected response (gold): OK"
                && message.tool_success == Some(true)
        }));
        assert!(!has_pending_clarification_resume(&after_respond));

        {
            let runners = state.agent_runners.read().await;
            let runner = runners
                .get(session_id)
                .expect("runner should exist after auto-resume start");
            assert_eq!(runner.run_id, run_id);
            assert!(matches!(
                runner.status,
                AgentStatus::Running | AgentStatus::Completed
            ));
        }

        assert_eq!(
            provider.request_purposes(),
            vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
        );

        provider.release_agent_loop();
        sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn gold_auto_answer_exit_plan_mode_full_loop() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
        config.provider = String::new();
        config.features.provider_model_ref = true;
        let provider = ScriptedProvider::new("Approve (Default mode)");
        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state =
            AppState::new_with_provider(temp_dir.path().to_path_buf(), config, provider_trait)
                .await
                .expect("app state");
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        let state = Data::new(app_state);

        let session_id = "gold-auto-answer-exit-plan-mode-full-loop";
        let tool_call_id = "call-exit-plan-mode";
        let mut session = build_pending_session(
            session_id,
            tool_call_id,
            "ExitPlanMode",
            "Approve leaving plan mode?",
            &["Approve (Default mode)", "Stay in plan mode"],
            false,
            json!({ "plan": "1. Inspect the codebase\n2. Write the implementation plan" }),
        );
        if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
            runtime_state.plan_mode = Some(PlanModeState {
                entered_at: chrono::Utc::now(),
                pre_permission_mode: "default".to_string(),
                plan_file_path: Some("/tmp/test-plan.md".to_string()),
                status: PlanModeStatus::AwaitingApproval,
            });
        }
        state.save_and_cache_session(&mut session).await;
        let mut event_rx = state.get_session_event_sender(session_id).await.subscribe();

        let outcome =
            maybe_auto_answer_pending_question(state.clone(), session_id, Some(test_gold_config()))
                .await;
        let run_id = match &outcome {
            GoldAutoAnswerOutcome::Applied {
                answer,
                resume_outcome,
            } => {
                assert_eq!(answer, "Approve (Default mode)");
                match resume_outcome {
                    ResumeOutcome::Started { run_id } => run_id.clone(),
                    other => panic!("expected resume to start, got {other:?}"),
                }
            }
            other => panic!("expected Gold auto-answer to apply, got {other:?}"),
        };

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        timeout(Duration::from_secs(5), async {
            loop {
                match event_rx.recv().await {
                    Ok(AgentEvent::PlanModeExited {
                        session_id: event_session_id,
                        approved,
                        restored_mode,
                        plan,
                    }) if event_session_id == session_id => {
                        assert!(approved);
                        assert_eq!(restored_mode, "default");
                        assert_eq!(
                            plan.as_deref(),
                            Some("1. Inspect the codebase\n2. Write the implementation plan")
                        );
                        break;
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(error) => panic!("failed to receive session event: {error}"),
                }
            }
        })
        .await
        .expect("expected PlanModeExited event");

        let after_respond = state
            .load_session_merged(session_id)
            .await
            .expect("session should exist after respond");
        assert!(after_respond.pending_question.is_none());
        assert!(after_respond.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some(tool_call_id)
                && message.content == "Auto-selected response (gold): Approve (Default mode)"
                && message.tool_success == Some(true)
        }));
        assert!(!has_pending_clarification_resume(&after_respond));
        assert!(after_respond
            .agent_runtime_state
            .as_ref()
            .and_then(|runtime_state| runtime_state.plan_mode.as_ref())
            .is_none());

        {
            let runners = state.agent_runners.read().await;
            let runner = runners
                .get(session_id)
                .expect("runner should exist after auto-resume start");
            assert_eq!(runner.run_id, run_id);
            assert!(matches!(
                runner.status,
                AgentStatus::Running | AgentStatus::Completed
            ));
        }

        assert_eq!(
            provider.request_purposes(),
            vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
        );
        provider.release_agent_loop();
        sleep(Duration::from_millis(50)).await;
    }

    async fn wait_for_resume_activity(state: &AppState, session_id: &str) -> AgentStatus {
        timeout(Duration::from_secs(5), async {
            loop {
                let marker_consumed = state
                    .load_session_merged(session_id)
                    .await
                    .is_some_and(|session| !has_pending_clarification_resume(&session));
                let runner_status = {
                    let runners = state.agent_runners.read().await;
                    runners.get(session_id).map(|runner| runner.status.clone())
                };
                if marker_consumed {
                    if let Some(status @ AgentStatus::Running) = runner_status.clone() {
                        return status;
                    }
                    if let Some(status @ AgentStatus::Completed) = runner_status {
                        return status;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("resume activity should appear")
    }

    async fn wait_for_provider_purpose(provider: &ScriptedProvider, purpose: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if provider
                    .request_purposes()
                    .iter()
                    .any(|value| value == purpose)
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("provider purpose should appear");
    }

    #[test]
    fn session_is_awaiting_clarification_checks_metadata_reason() {
        let mut session = Session::new("session-1", "model");
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "awaiting_clarification".to_string(),
        );
        assert!(session_is_awaiting_clarification(&session));
    }
}
