//! Bounded model projection; canonical goals and receipts remain intact.

use bamboo_agent_core::tools::ToolResult;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use serde_json::{json, Value};

pub(crate) fn fits(result: &ToolResult, cap: u32) -> bool {
    cap == 0 || TiktokenTokenCounter::default().count_text(&result.result) <= cap
}

pub(crate) fn budget_failure(cap: u32) -> ToolResult {
    let mut failure = ToolResult::text(
        false,
        json!({"error":"output_budget_too_small"}).to_string(),
    );
    if !fits(&failure, cap) {
        failure.result.clear();
    }
    failure
}

pub(crate) fn bound_result(mut result: ToolResult, cap: u32) -> ToolResult {
    if fits(&result, cap) {
        return result;
    }
    // A mutation receipt is indivisible: never trim its protocol fields. This
    // fallback alone cannot undo a mutation; accept_command must preflight fit.
    let Ok(mut body) = serde_json::from_str::<Value>(&result.result) else {
        return budget_failure(cap);
    };
    if !result.success || body.get("can_start").is_none() {
        return budget_failure(cap);
    }
    let Some(status) = body.get_mut("loop").and_then(Value::as_object_mut) else {
        return budget_failure(cap);
    };
    for field in ["objective", "pending_objective"] {
        if let Some(text) = status.get(field).and_then(Value::as_str) {
            let bytes = text.len();
            status.remove(field);
            status.insert(format!("{field}_omitted_bytes"), json!(bytes));
        }
    }
    result.result = body.to_string();
    if fits(&result, cap) {
        result
    } else {
        budget_failure(cap)
    }
}

/// Keep original protocol fields and success unchanged. This renderer never
/// checkpoints: caller replaces the existing paired result and checkpoints it
/// again BEFORE ToolComplete when the returned text has changed.
pub(crate) fn append_feedback(result: &mut ToolResult, feedback: Vec<String>, cap: u32) {
    let feedback = feedback
        .into_iter()
        .filter_map(|text| {
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_owned())
        })
        .collect::<Vec<_>>();
    if feedback.is_empty() || !fits(result, cap) {
        return;
    }
    let Ok(Value::Object(original)) = serde_json::from_str::<Value>(&result.result) else {
        // Includes the truthful empty failure used when cap cannot fit JSON.
        return;
    };
    if original.contains_key("hook_feedback")
        || original.contains_key("hook_feedback_omitted_bytes")
    {
        return;
    }
    let feedback = feedback.join("\n");
    let success = result.success;
    let candidate = |prefix: &str| -> Option<String> {
        let mut body = original.clone();
        body.insert(
            "hook_feedback".to_string(),
            Value::String(prefix.to_string()),
        );
        if prefix.len() < feedback.len() {
            body.insert(
                "hook_feedback_omitted_bytes".to_string(),
                json!(feedback.len() - prefix.len()),
            );
        }
        let text = Value::Object(body).to_string();
        let proposed = ToolResult::text(success, &text);
        fits(&proposed, cap).then_some(text)
    };
    if let Some(text) = candidate(&feedback) {
        result.result = text;
        return;
    }
    let Some(mut best) = candidate("") else {
        // Even omission metadata does not fit. Preserve the already-capped
        // original rather than appending an over-budget marker or fake ACK.
        return;
    };
    let counter = TiktokenTokenCounter::default();
    let (mut low, mut high) = (0, counter.count_text(&feedback).min(cap));
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        let prefix = counter.truncate_to_token_prefix(&feedback, middle);
        if let Some(text) = candidate(&prefix) {
            best = text;
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    // BPE token counts need not be perfectly monotone with added prefix bytes;
    // only candidates whose FINAL serialized JSON was counted can become best.
    if fits(&ToolResult::text(result.success, &best), cap) {
        result.result = best;
    }
}
