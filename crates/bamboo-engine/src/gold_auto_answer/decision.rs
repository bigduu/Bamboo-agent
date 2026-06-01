use bamboo_agent_core::{GoldConfidence, PendingQuestion, PendingQuestionSource, Session, ToolCall};

use super::GOLD_AUTO_ANSWER_TOOL_NAME;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoldAutoAnswerDecision {
    pub(crate) apply: bool,
    pub(crate) answer: Option<String>,
    pub(crate) confidence: GoldConfidence,
    pub(crate) reasoning: String,
}

impl GoldAutoAnswerDecision {
    pub(crate) fn decline(reason: impl Into<String>) -> Self {
        Self {
            apply: false,
            answer: None,
            confidence: GoldConfidence::Low,
            reasoning: reason.into(),
        }
    }
}

pub(crate) fn should_attempt_gold_auto_answer(pending: &PendingQuestion) -> bool {
    if matches!(pending.source, PendingQuestionSource::Gold) {
        return false;
    }

    matches!(
        normalized_pending_tool_name(&pending.tool_name).as_str(),
        "ExitPlanMode" | "conclusion_with_options"
    )
}

pub(crate) fn session_is_awaiting_clarification(session: &Session) -> bool {
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

pub(crate) fn normalized_pending_tool_name(tool_name: &str) -> String {
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

pub(crate) fn parse_gold_auto_answer_decision(
    tool_calls: &[ToolCall],
) -> Option<GoldAutoAnswerDecision> {
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

pub(crate) fn canonicalize_pending_answer(
    pending: &PendingQuestion,
    raw_answer: &str,
) -> Option<String> {
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
        .trim_matches(['"', '\'', '`'])
        .trim()
        .trim_end_matches(['.', '。', '!', '！', '?', '？'])
        .trim()
        .to_ascii_lowercase()
}
