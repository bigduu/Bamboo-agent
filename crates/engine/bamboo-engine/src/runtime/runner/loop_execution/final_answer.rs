//! Accounting and outcome handling for the opt-in final-answer check.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bamboo_agent_core::{AgentError, Message, Session};
use bamboo_domain::session::runtime_state::AgentRuntimeState;
use bamboo_llm::LLMProvider;
use bamboo_metrics::{MetricsCollector, RoundStatus, TokenUsage};
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::final_evidence::{
    evaluate_final_evidence, FinalEvidenceFrame, FinalEvidenceVerdict,
};
use crate::runtime::stream::handler::StreamTimeoutContext;

pub(super) struct FinalAnswerCheck {
    pub revised: bool,
    pub usage: TokenUsage,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn check(
    session: &mut Session,
    runtime: &mut AgentRuntimeState,
    answer: &mut Message,
    config: &AgentLoopConfig,
    llm: Arc<dyn LLMProvider>,
    model: &str,
    cancel_token: &CancellationToken,
    metrics: Option<&MetricsCollector>,
    round_id: &str,
    main_usage: TokenUsage,
) -> Result<FinalAnswerCheck, AgentError> {
    let used_tokens = runtime
        .round
        .total_prompt_tokens
        .saturating_add(runtime.round.total_completion_tokens)
        .saturating_add(main_usage.total_tokens);
    let timeout = StreamTimeoutContext::new(config.stream_timeout, None, Some(model));
    let frame = FinalEvidenceFrame {
        model,
        reasoning_effort: config.reasoning_effort,
        timeout_context: &timeout,
        cancel_token,
        max_output_tokens: 8192,
        token_budget: config
            .run_budget
            .max_total_tokens
            .map(|limit| limit.saturating_sub(used_tokens)),
        auxiliary_max_concurrency: config.auxiliary_evaluation_max_concurrency,
    };
    let evaluation_id = format!("{round_id}:final_evidence_check");
    let dispatched = AtomicBool::new(false);
    let result = evaluate_final_evidence(session, &answer.content, llm, &frame, || {
        dispatched.store(true, Ordering::Release);
        if let Some(metrics) = metrics {
            metrics.round_started(
                evaluation_id.clone(),
                session.id.clone(),
                model.to_string(),
                chrono::Utc::now(),
            );
        }
    })
    .await;
    let (usage, error) = match &result {
        Ok(evaluation) => (
            evaluation.usage,
            evaluation.error.map(|error| error.to_string()),
        ),
        Err(failure) => (failure.usage, Some(failure.error.to_string())),
    };
    // The main round is committed separately. Auxiliary spend contributes once
    // to the same run budget and once to its own metrics row.
    let mut cumulative = TokenUsage {
        prompt_tokens: runtime.round.total_prompt_tokens,
        completion_tokens: runtime.round.total_completion_tokens,
        total_tokens: 0,
    }
    .clamped_for_durable_metrics();
    cumulative.add_assign_durable(usage);
    runtime.round.total_prompt_tokens = cumulative.prompt_tokens;
    runtime.round.total_completion_tokens = cumulative.completion_tokens;
    if dispatched.load(Ordering::Acquire) {
        if let Some(metrics) = metrics {
            metrics.round_completed(
                evaluation_id,
                chrono::Utc::now(),
                if matches!(&result, Err(failure) if matches!(failure.error.as_ref(), AgentError::Cancelled))
                {
                    RoundStatus::Cancelled
                } else if error.is_some() {
                    RoundStatus::Error
                } else {
                    RoundStatus::Success
                },
                usage,
                0,
                0,
                error.clone(),
            );
        }
    }
    if let Some(error) = error {
        let status = match &result {
            Err(failure) if matches!(failure.error.as_ref(), AgentError::Cancelled) => "cancelled",
            Err(failure) if matches!(failure.error.as_ref(), AgentError::Budget(_)) => {
                "skipped_budget"
            }
            _ => "failed",
        };
        record_status(session, status, &[]);
        return Err(match result {
            Err(failure) => *failure.error,
            Ok(_) => AgentError::LLM(format!("final evidence check failed: {error}")),
        });
    }
    let evaluation = match result {
        Ok(evaluation) => evaluation,
        Err(failure) => return Err(*failure.error),
    };
    let revised = evaluation.verdict == Some(FinalEvidenceVerdict::Revise);
    let status = if evaluation.is_skipped() {
        "skipped_no_evidence"
    } else if revised {
        "revised"
    } else {
        "supported"
    };
    record_status(session, status, &evaluation.evidence_ids);
    if revised {
        answer.content = evaluation
            .corrected_answer
            .expect("a validated revision contains an answer");
        answer.reasoning = None;
        answer.reasoning_signature = None;
    }
    if cancel_token.is_cancelled() {
        record_status(session, "cancelled", &[]);
        return Err(AgentError::Cancelled);
    }
    Ok(FinalAnswerCheck { revised, usage })
}

fn record_status(session: &mut Session, status: &str, evidence_ids: &[String]) {
    session.metadata.insert(
        "runtime.final_evidence_check".to_string(),
        serde_json::json!({"status": status, "evidence_ids": evidence_ids}).to_string(),
    );
}
