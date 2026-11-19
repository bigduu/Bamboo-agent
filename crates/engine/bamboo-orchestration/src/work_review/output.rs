//! Interpret output only after the original usage has been settled. These
//! diagnostics never retain provider text/errors or imply delivery acceptance.
use super::AdvisoryWorkReview;
use serde::Serialize;

const MAX_ADVISORY_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewFinishReason {
    Completed,
    OutputLimit,
    ContextLimit,
    Safety,
    Tools,
    Other,
    #[default]
    Unavailable,
}

impl ReviewFinishReason {
    pub fn from_provider_code(code: &str) -> Self {
        match code {
            "stop" | "STOP" | "end_turn" | "stop_sequence" | "completed" => Self::Completed,
            "length" | "max_tokens" | "MAX_TOKENS" | "max_output_tokens" => Self::OutputLimit,
            "model_context_window_exceeded" => Self::ContextLimit,
            "tool_calls" | "function_call" | "tool_use" | "UNEXPECTED_TOOL_CALL" => Self::Tools,
            "content_filter" | "refusal" | "SAFETY" | "RECITATION" | "BLOCKLIST"
            | "PROHIBITED_CONTENT" | "SPII" | "IMAGE_SAFETY" => Self::Safety,
            "unavailable" => Self::Unavailable,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WorkReviewOutputObservations {
    pub reasoning_seen: bool,
    pub text_bytes: usize,
    pub unexpected_tools: bool,
    pub text_overflow: bool,
    pub finish_reason: ReviewFinishReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InconclusiveReason {
    UsageUnknown,
    UsageOverCap,
    ReasoningOnly,
    EmptyText,
    UnexpectedTools,
    TextTooLarge,
    InvalidAdvisory,
    EmptySummary,
}

#[derive(Debug, Serialize)]
pub struct InconclusiveWorkReview {
    pub reason: InconclusiveReason,
    pub finish_reason: ReviewFinishReason,
    pub reasoning_seen: bool,
    pub text_bytes: usize,
}

pub(super) fn parse_advisory(text: &str) -> Result<AdvisoryWorkReview, InconclusiveReason> {
    if text.len() > MAX_ADVISORY_BYTES {
        return Err(InconclusiveReason::TextTooLarge);
    }
    if text.trim().is_empty() {
        return Err(InconclusiveReason::EmptyText);
    }
    let review: AdvisoryWorkReview =
        serde_json::from_str(text).map_err(|_| InconclusiveReason::InvalidAdvisory)?;
    if review.summary.trim().is_empty() {
        return Err(InconclusiveReason::EmptySummary);
    }
    Ok(review)
}

pub(super) fn inconclusive(
    known_usage: bool,
    over_cap: bool,
    text: Option<&str>,
    observed: WorkReviewOutputObservations,
) -> InconclusiveWorkReview {
    use InconclusiveReason::*;
    let reason = if over_cap {
        UsageOverCap
    } else if !known_usage {
        UsageUnknown
    } else if observed.unexpected_tools {
        UnexpectedTools
    } else if observed.text_overflow || text.is_some_and(|text| text.len() > MAX_ADVISORY_BYTES) {
        TextTooLarge
    } else if text.is_none_or(|text| text.trim().is_empty()) {
        if observed.reasoning_seen {
            ReasoningOnly
        } else {
            EmptyText
        }
    } else {
        parse_advisory(text.unwrap())
            .err()
            .unwrap_or(InvalidAdvisory)
    };
    InconclusiveWorkReview {
        reason,
        finish_reason: observed.finish_reason,
        reasoning_seen: observed.reasoning_seen,
        text_bytes: observed.text_bytes.max(text.map_or(0, str::len)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_advisory_bytes_schema_and_summary_are_independent_of_token_budget() {
        assert_eq!(
            parse_advisory(r#"{"summary":"Confirmed progress."}"#)
                .unwrap()
                .summary,
            "Confirmed progress."
        );
        for invalid in [
            "{",
            "```json\n{}\n```",
            r#"{"summary":"ok","extra":1}"#,
            r#"{"summary":3}"#,
        ] {
            assert!(matches!(
                parse_advisory(invalid),
                Err(InconclusiveReason::InvalidAdvisory)
            ));
        }
        assert!(matches!(
            parse_advisory(r#"{"summary":"  "}"#),
            Err(InconclusiveReason::EmptySummary)
        ));
        let compact = serde_json::json!({"summary":"界"}).to_string();
        let boundary = serde_json::json!({"summary":format!("界{}", "x".repeat(MAX_ADVISORY_BYTES - compact.len()))}).to_string();
        assert_eq!(boundary.len(), MAX_ADVISORY_BYTES);
        assert!(parse_advisory(&boundary).is_ok());
        let escaped = format!("{{\"summary\":\"{}\"}}", "\\u754c".repeat(700));
        assert!(escaped.len() > MAX_ADVISORY_BYTES);
        assert!(matches!(
            parse_advisory(&escaped),
            Err(InconclusiveReason::TextTooLarge)
        ));
    }

    #[test]
    fn invalid_text_does_not_infer_truncation_from_usage_or_provider_strings() {
        for (code, expected) in [
            ("length", ReviewFinishReason::OutputLimit),
            ("stop", ReviewFinishReason::Completed),
            ("private unrecognized reason", ReviewFinishReason::Other),
            ("unavailable", ReviewFinishReason::Unavailable),
        ] {
            let output = inconclusive(
                true,
                false,
                Some("{\"summary\":"),
                WorkReviewOutputObservations {
                    finish_reason: ReviewFinishReason::from_provider_code(code),
                    ..Default::default()
                },
            );
            assert_eq!(output.reason, InconclusiveReason::InvalidAdvisory);
            assert_eq!(output.finish_reason, expected);
            assert!(!serde_json::to_string(&output).unwrap().contains("private"));
        }
    }

    #[test]
    fn accounting_and_specific_output_failures_take_precedence_over_empty_text() {
        let reasoning = WorkReviewOutputObservations {
            reasoning_seen: true,
            ..Default::default()
        };
        assert_eq!(
            inconclusive(true, false, None, reasoning).reason,
            InconclusiveReason::ReasoningOnly
        );
        assert_eq!(
            inconclusive(false, false, None, reasoning).reason,
            InconclusiveReason::UsageUnknown
        );
        assert_eq!(
            inconclusive(true, true, None, reasoning).reason,
            InconclusiveReason::UsageOverCap
        );
        assert_eq!(
            inconclusive(
                true,
                false,
                None,
                WorkReviewOutputObservations {
                    unexpected_tools: true,
                    ..reasoning
                }
            )
            .reason,
            InconclusiveReason::UnexpectedTools
        );
        assert_eq!(
            inconclusive(
                true,
                false,
                None,
                WorkReviewOutputObservations {
                    text_overflow: true,
                    ..reasoning
                }
            )
            .reason,
            InconclusiveReason::TextTooLarge
        );
        assert!(parse_advisory(r#"{"summary":"Usable answer after reasoning."}"#).is_ok());
    }
}
