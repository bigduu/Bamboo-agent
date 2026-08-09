use crate::config::GoldConfig;
use bamboo_agent_core::GoldDecision;

use crate::app_context::AgentSessionContext;
use crate::events::publish_replayable_session_event;
use crate::model_config_helper::{resolve_gold_config, GOLD_CONFIG_METADATA_KEY};
use crate::session_app::repository::SessionAccess;
use crate::session_app::respond::{submit_pending_response_with_source, ResponseSource};
use crate::session_app::resume::{resume_session_execution, ResumeExecutionPort};
use crate::session_app::types::{RespondInput, ResumeOutcome};

mod decision;
mod evaluation;
mod prompt;
mod resume;

#[cfg(test)]
mod tests;

use decision::{
    canonicalize_pending_answer, session_is_awaiting_clarification, should_attempt_gold_auto_answer,
};
use evaluation::{evaluate_gold_auto_answer_question, evaluate_gold_state_for_pending_question};
#[cfg(test)]
pub(crate) use evaluation::{
    evaluate_gold_auto_answer_question_with_target, evaluate_gold_state_with_target,
    GoldAuxiliaryTarget,
};
use resume::{build_resume_config_snapshot, plan_mode_transition_event};

const GOLD_AUTO_ANSWER_TOOL_NAME: &str = "report_gold_auto_answer";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoldAutoAnswerOutcome {
    Skipped {
        reason: String,
    },
    Applied {
        answer: String,
        resume_outcome: ResumeOutcome,
    },
}

/// Attempt a Gold auto-answer for a session's pending clarification.
///
/// `state` supplies session/provider/event context (and session persistence
/// via [`SessionAccess`]); `resume_port` is the server-side adapter that knows
/// how to actually spawn a resumed agent execution.
pub async fn maybe_auto_answer_pending_question<S>(
    state: &S,
    resume_port: &dyn ResumeExecutionPort,
    session_id: &str,
    gold_config_override: Option<GoldConfig>,
) -> GoldAutoAnswerOutcome
where
    S: AgentSessionContext + SessionAccess,
{
    let Some(session) = state.load_session_merged(session_id).await else {
        return GoldAutoAnswerOutcome::Skipped {
            reason: "session_not_found".to_string(),
        };
    };

    let config_snapshot = state.config().read().await.clone();
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

    let state_evaluation =
        match evaluate_gold_state_for_pending_question(state, session_id, &session, &gold_config)
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
        state,
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

    // Gold (eval) auto-answers do not record permission grants; eval sessions
    // should run with a permissive posture (e.g. BypassPermissions) so they never
    // pause for approval in the first place.
    let (updated_session, _submitted_answer, plan_mode_transition, _permission_grants) =
        match submit_pending_response_with_source(state, respond_input, ResponseSource::Gold).await
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
        publish_replayable_session_event(state, session_id, event).await;
    }

    let resume_config =
        build_resume_config_snapshot(state, &updated_session, Some(gold_config.clone())).await;
    let resume_outcome = resume_session_execution(resume_port, session_id, resume_config).await;

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
