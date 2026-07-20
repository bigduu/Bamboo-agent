use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionSchema, ToolCall, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, GoldCheckpoint, GoldConfidence, GoldDecision};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use bamboo_domain::ReasoningEffort;
use bamboo_llm::{LLMProvider, LLMRequestOptions};
use chrono::Utc;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::GoldConfig;
use crate::runtime::stream::handler::{
    consume_llm_stream_silent_with_context, StreamTimeoutContext,
};
use crate::runtime::task_context::TaskLoopContext;
use bamboo_metrics::TokenUsage as MetricsTokenUsage;

/// Evaluation-scoped frame bundling parameters that identify and configure a
/// single gold evaluation pass.  Passed into [`evaluate_gold`] to keep its
/// parameter count below the clippy threshold.
pub struct GoldEvalFrame<'a> {
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub session_id: &'a str,
    pub model: &'a str,
    pub timeout_context: StreamTimeoutContext,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub checkpoint: GoldCheckpoint,
    pub iteration: u32,
}

#[derive(Debug, Clone)]
pub struct GoldEvaluationResult {
    pub checkpoint: GoldCheckpoint,
    pub iteration: u32,
    pub decision: GoldDecision,
    pub confidence: GoldConfidence,
    pub reasoning: String,
    /// Concrete information still missing to achieve the goal, if any.
    pub missing_information: Vec<String>,
    /// The single most useful next action for the agent to take, if the
    /// evaluator can name one. Used to give the auto-continue nudge direction.
    pub next_action: Option<String>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AsyncGoldEvaluationRequest {
    pub(crate) session_id: String,
    pub(crate) round_number: usize,
    pub(crate) model_name: String,
    pub(crate) timeout_context: StreamTimeoutContext,
    pub(crate) reasoning_effort: Option<ReasoningEffort>,
    pub(crate) checkpoint: GoldCheckpoint,
    pub(crate) session_snapshot: Session,
    pub(crate) task_context_snapshot: Option<TaskLoopContext>,
    pub(crate) gold_config: GoldConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct AsyncGoldEvaluationResult {
    pub(crate) round_number: usize,
    pub(crate) model_name: String,
    pub(crate) evaluation_result: GoldEvaluationResult,
}

fn normalize_lightweight_reasoning_effort(
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<ReasoningEffort> {
    reasoning_effort.map(|effort| match effort {
        ReasoningEffort::Xhigh | ReasoningEffort::Max => ReasoningEffort::High,
        other => other,
    })
}

fn estimate_prompt_tokens(messages: &[Message]) -> u64 {
    let counter = TiktokenTokenCounter::default();
    u64::from(counter.count_messages(messages))
}

fn estimate_completion_tokens(content: &str, tool_calls: &[ToolCall]) -> u64 {
    let counter = TiktokenTokenCounter::default();
    let mut completion_surface = content.to_string();

    for call in tool_calls {
        if !completion_surface.is_empty() {
            completion_surface.push('\n');
        }
        completion_surface.push_str(&call.function.name);
        completion_surface.push('\n');
        completion_surface.push_str(&call.function.arguments);
    }

    u64::from(counter.count_text(&completion_surface))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_async_gold_evaluation_request(
    task_context: &Option<TaskLoopContext>,
    session: &Session,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    checkpoint: GoldCheckpoint,
    gold_config: &GoldConfig,
    timeout_context: StreamTimeoutContext,
) -> Result<Option<AsyncGoldEvaluationRequest>, AgentError> {
    if !gold_config.enabled {
        return Ok(None);
    }

    let model_name = gold_config
        .model_name
        .as_deref()
        .or(model_name)
        .ok_or_else(|| AgentError::LLM("gold evaluation model_name is required".to_string()))?;

    Ok(Some(AsyncGoldEvaluationRequest {
        session_id: session_id.to_string(),
        round_number,
        model_name: model_name.to_string(),
        timeout_context,
        reasoning_effort,
        checkpoint,
        session_snapshot: session.clone(),
        task_context_snapshot: task_context.clone(),
        gold_config: gold_config.clone(),
    }))
}

pub(crate) async fn execute_async_gold_evaluation(
    request: AsyncGoldEvaluationRequest,
    llm: Arc<dyn LLMProvider>,
    event_tx: mpsc::Sender<AgentEvent>,
) -> AsyncGoldEvaluationResult {
    let evaluation_result = match evaluate_gold(
        &request.session_snapshot,
        request.task_context_snapshot.as_ref(),
        &request.gold_config,
        llm,
        &GoldEvalFrame {
            event_tx: &event_tx,
            session_id: &request.session_id,
            model: &request.model_name,
            timeout_context: request.timeout_context.clone(),
            reasoning_effort: request.reasoning_effort,
            checkpoint: request.checkpoint,
            iteration: request.round_number as u32,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => GoldEvaluationResult {
            checkpoint: request.checkpoint,
            iteration: request.round_number as u32,
            decision: GoldDecision::Continue,
            confidence: GoldConfidence::Low,
            reasoning: format!("Gold evaluation failed: {error}"),
            missing_information: Vec::new(),
            next_action: None,
            prompt_tokens: 0,
            completion_tokens: 0,
        },
    };

    AsyncGoldEvaluationResult {
        round_number: request.round_number,
        model_name: request.model_name,
        evaluation_result,
    }
}

pub async fn evaluate_gold(
    session: &Session,
    task_context: Option<&TaskLoopContext>,
    config: &GoldConfig,
    llm: Arc<dyn LLMProvider>,
    frame: &GoldEvalFrame<'_>,
) -> Result<GoldEvaluationResult, AgentError> {
    // Bind frame fields as locals so the rest of the function body stays unchanged.
    let event_tx = frame.event_tx;
    let session_id = frame.session_id;
    let model = frame.model;
    let reasoning_effort = frame.reasoning_effort;
    let checkpoint = frame.checkpoint;
    let iteration = frame.iteration;

    let _ = event_tx
        .send(AgentEvent::GoldEvaluationStarted {
            session_id: session_id.to_string(),
            checkpoint,
            iteration,
        })
        .await;

    let messages = build_gold_messages(session, task_context, config, checkpoint);
    let prompt_tokens = estimate_prompt_tokens(&messages);
    let tools = get_gold_evaluation_tools();

    let request_reasoning_effort = normalize_lightweight_reasoning_effort(reasoning_effort);
    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort: request_reasoning_effort,
        parallel_tool_calls: None,
        responses: None,
        request_purpose: Some("gold_evaluation".to_string()),
        cache: None,
    };

    match llm
        .chat_stream_with_options(
            &messages,
            &tools,
            Some(config.max_output_tokens),
            model,
            Some(&request_options),
        )
        .await
    {
        Ok(stream) => {
            let stream_output = consume_llm_stream_silent_with_context(
                stream,
                &CancellationToken::new(),
                session_id,
                &frame.timeout_context,
            )
            .await?;

            let result = parse_gold_evaluation(
                &stream_output.content,
                &stream_output.tool_calls,
                checkpoint,
                iteration,
                prompt_tokens,
            );

            let _ = event_tx
                .send(AgentEvent::GoldEvaluationCompleted {
                    session_id: session_id.to_string(),
                    checkpoint: result.checkpoint,
                    iteration: result.iteration,
                    decision: result.decision,
                    confidence: result.confidence,
                    reasoning: result.reasoning.clone(),
                })
                .await;

            Ok(result)
        }
        Err(error) => Err(AgentError::LLM(error.to_string())),
    }
}

pub fn build_gold_messages(
    session: &Session,
    task_context: Option<&TaskLoopContext>,
    config: &GoldConfig,
    checkpoint: GoldCheckpoint,
) -> Vec<Message> {
    let mut messages = Vec::new();

    let mut system_prompt = String::from(
        "You are a gold progress evaluator. Judge whether the agent has already achieved the user's goal, should continue execution, needs user input, is blocked, or is exhausted.\n\nRules:\n1. This phase is observe-only: do not mutate state or invent actions.\n2. You must call report_gold_evaluation exactly once.\n3. Use achieved only when the user's actual goal is satisfied.\n4. Use continue when more agent work is still appropriate.\n5. Use need_input only when missing user input is the true next blocker.\n6. Use blocked only for a concrete blocking condition.\n7. Use exhausted for loops, budget exhaustion, or clear inability to make progress.\n8. Keep reasoning short, concrete, and evidence-based."
    );

    if let Some(extra) = config
        .evaluation_prompt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        system_prompt.push_str("\n\nAdditional instructions:\n");
        system_prompt.push_str(extra);
    }

    messages.push(Message::system(system_prompt));

    let task_summary = task_context
        .map(TaskLoopContext::format_for_prompt)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "## Current Task List\nNo task list available.".to_string());

    let pending_question_summary = session
        .pending_question
        .as_ref()
        .map(|question| {
            let options = if question.options.is_empty() {
                "none".to_string()
            } else {
                question.options.join(" | ")
            };
            let tool_name = if question.tool_name.trim().is_empty() {
                "unknown".to_string()
            } else {
                question.tool_name.clone()
            };
            format!(
                "question={} | options={} | tool={} | source={:?}",
                question.question, options, tool_name, question.source
            )
        })
        .unwrap_or_else(|| "none".to_string());

    let runtime_summary = session
        .agent_runtime_state
        .as_ref()
        .map(|state| {
            format!(
                "status={:?} | current_round={} | max_rounds={} | suspend_reason={} | waiting_for_children={}",
                state.status,
                state.round.current_round,
                state.round.max_rounds,
                state
                    .suspension
                    .as_ref()
                    .map(|s| s.reason.clone())
                    .unwrap_or_else(|| "none".to_string()),
                state.waiting_for_children.is_some()
            )
        })
        .unwrap_or_else(|| "runtime_state=none".to_string());

    let recent_messages = format_recent_messages(session, 6);

    let goal_section = config
        .effective_goal()
        .map(|goal| format!("## Goal\n{goal}"))
        .unwrap_or_else(|| {
            "## Goal\nNo explicit goal set. Judge against the user's request inferred from the conversation.".to_string()
        });

    let user_prompt = format!(
        "## Gold Checkpoint\ncheckpoint={}\n\n{}\n\n## Runtime\n{}\n\n## Pending Question\n{}\n\n{}\n\n## Recent Conversation\n{}\n\n## Instruction\nReport the best current Gold judgment for this checkpoint by measuring progress against the goal above. Remember: Phase 1 is observe-only, so only report decision/confidence/reasoning.",
        checkpoint.as_str(),
        goal_section,
        runtime_summary,
        pending_question_summary,
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

        lines.push(format!("- [{}] {}", role, content));
    }

    if lines.is_empty() {
        "- <no messages>".to_string()
    } else {
        lines.join("\n")
    }
}

pub fn get_gold_evaluation_tools() -> Vec<ToolSchema> {
    vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "report_gold_evaluation".to_string(),
            description: "Report the current Gold evaluation decision for the session".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "decision": {
                        "type": "string",
                        "enum": ["continue", "achieved", "blocked", "need_input", "exhausted"]
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["low", "medium", "high"]
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Short concrete reasoning for the decision"
                    },
                    "missing_information": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Concrete pieces of information still missing to achieve the goal. Empty when nothing is missing."
                    },
                    "next_action": {
                        "type": "string",
                        "description": "The single most useful next action the agent should take. Provide when decision is continue."
                    }
                },
                "required": ["decision", "confidence", "reasoning"],
                "additionalProperties": false
            }),
        },
    }]
}

pub fn parse_gold_evaluation(
    content: &str,
    tool_calls: &[ToolCall],
    checkpoint: GoldCheckpoint,
    iteration: u32,
    prompt_tokens: u64,
) -> GoldEvaluationResult {
    let completion_tokens = estimate_completion_tokens(content, tool_calls);
    let parsed = parse_gold_result_from_tool_calls(tool_calls);

    let parsed = parsed.unwrap_or_else(|| {
        let fallback_reasoning = content.trim().to_string();
        ParsedGoldResult {
            decision: GoldDecision::Continue,
            confidence: GoldConfidence::Low,
            reasoning: if fallback_reasoning.is_empty() {
                "Gold evaluation returned no structured result; defaulting to continue.".to_string()
            } else {
                fallback_reasoning
            },
            missing_information: Vec::new(),
            next_action: None,
        }
    });

    GoldEvaluationResult {
        checkpoint,
        iteration,
        decision: parsed.decision,
        confidence: parsed.confidence,
        reasoning: parsed.reasoning,
        missing_information: parsed.missing_information,
        next_action: parsed.next_action,
        prompt_tokens,
        completion_tokens,
    }
}

struct ParsedGoldResult {
    decision: GoldDecision,
    confidence: GoldConfidence,
    reasoning: String,
    missing_information: Vec<String>,
    next_action: Option<String>,
}

fn parse_gold_result_from_tool_calls(tool_calls: &[ToolCall]) -> Option<ParsedGoldResult> {
    for tool_call in tool_calls {
        if tool_call.function.name != "report_gold_evaluation" {
            continue;
        }

        let Ok(args) = serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
        else {
            continue;
        };

        let decision = match args.get("decision").and_then(|value| value.as_str()) {
            Some("continue") => GoldDecision::Continue,
            Some("achieved") => GoldDecision::Achieved,
            Some("blocked") => GoldDecision::Blocked,
            Some("need_input") => GoldDecision::NeedInput,
            Some("exhausted") => GoldDecision::Exhausted,
            _ => continue,
        };

        let confidence = match args.get("confidence").and_then(|value| value.as_str()) {
            Some("low") => GoldConfidence::Low,
            Some("medium") => GoldConfidence::Medium,
            Some("high") => GoldConfidence::High,
            _ => GoldConfidence::Low,
        };

        let reasoning = args
            .get("reasoning")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("Gold evaluation produced no reasoning")
            .to_string();

        let missing_information = args
            .get("missing_information")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let next_action = args
            .get("next_action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        return Some(ParsedGoldResult {
            decision,
            confidence,
            reasoning,
            missing_information,
            next_action,
        });
    }

    None
}

pub(crate) fn apply_gold_evaluation_result(
    session: &mut Session,
    result: &GoldEvaluationResult,
) -> MetricsTokenUsage {
    let evaluation_count = session
        .metadata
        .get("gold.evaluation_count")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1);

    let summary = json!({
        "checkpoint": result.checkpoint.as_str(),
        "iteration": result.iteration,
        "decision": result.decision.as_str(),
        "confidence": result.confidence.as_str(),
        "reasoning": result.reasoning,
        "recorded_at": Utc::now().to_rfc3339(),
    });

    session
        .metadata
        .insert("gold.last_evaluation".to_string(), summary.to_string());
    session.metadata.insert(
        "gold.last_decision".to_string(),
        result.decision.as_str().to_string(),
    );
    session.metadata.insert(
        "gold.last_confidence".to_string(),
        result.confidence.as_str().to_string(),
    );
    session
        .metadata
        .insert("gold.last_reasoning".to_string(), result.reasoning.clone());
    session.metadata.insert(
        "gold.last_checkpoint".to_string(),
        result.checkpoint.as_str().to_string(),
    );
    session.metadata.insert(
        "gold.last_iteration".to_string(),
        result.iteration.to_string(),
    );
    session.metadata.insert(
        "gold.evaluation_count".to_string(),
        evaluation_count.to_string(),
    );
    session.updated_at = Utc::now();

    let mut usage = MetricsTokenUsage {
        prompt_tokens: result.prompt_tokens,
        completion_tokens: result.completion_tokens,
        ..Default::default()
    };
    usage.recompute_total();
    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;

    fn report_call(arguments: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "report_gold_evaluation".to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn parse_gold_result_from_tool_calls_reads_structured_fields() {
        let parsed = parse_gold_result_from_tool_calls(&[report_call(json!({
            "decision": "blocked",
            "confidence": "high",
            "reasoning": "Missing credentials",
            "missing_information": ["API key", "  ", "Database URL"],
            "next_action": "  Ask the user for the API key  "
        }))])
        .expect("gold result should parse");

        assert_eq!(parsed.decision, GoldDecision::Blocked);
        assert_eq!(parsed.confidence, GoldConfidence::High);
        assert_eq!(parsed.reasoning, "Missing credentials");
        assert_eq!(
            parsed.missing_information,
            vec!["API key".to_string(), "Database URL".to_string()]
        );
        assert_eq!(
            parsed.next_action.as_deref(),
            Some("Ask the user for the API key")
        );
    }

    #[test]
    fn parse_gold_result_from_tool_calls_ignores_other_tools() {
        let parsed = parse_gold_result_from_tool_calls(&[ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "other_tool".to_string(),
                arguments: "{}".to_string(),
            },
        }]);

        assert!(parsed.is_none());
    }

    #[test]
    fn apply_gold_evaluation_result_updates_metadata_keys() {
        let mut session = Session::new("session-1", "model");
        let result = GoldEvaluationResult {
            checkpoint: GoldCheckpoint::PostRound,
            iteration: 2,
            decision: GoldDecision::Achieved,
            confidence: GoldConfidence::Medium,
            reasoning: "Goal satisfied".to_string(),
            missing_information: Vec::new(),
            next_action: None,
            prompt_tokens: 10,
            completion_tokens: 5,
        };

        let usage = apply_gold_evaluation_result(&mut session, &result);

        assert_eq!(
            session
                .metadata
                .get("gold.last_decision")
                .map(String::as_str),
            Some("achieved")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.last_confidence")
                .map(String::as_str),
            Some("medium")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.last_checkpoint")
                .map(String::as_str),
            Some("post_round")
        );
        assert_eq!(
            session
                .metadata
                .get("gold.evaluation_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }
}
