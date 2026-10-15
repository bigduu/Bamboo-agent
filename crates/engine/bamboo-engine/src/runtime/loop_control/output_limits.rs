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

/// Project optional native feedback into the existing per-run hook context.
/// The immutable result plus this complete correlated block and its framework
/// separator share the exact output cap captured at dispatch.
pub(crate) fn feedback_context(
    result: &ToolResult,
    tool_call_id: &str,
    feedback: Vec<String>,
    cap: u32,
) -> Option<String> {
    let feedback = feedback
        .into_iter()
        .filter_map(|text| {
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_owned())
        })
        .collect::<Vec<_>>();
    if feedback.is_empty() || !fits(result, cap) {
        return None;
    }
    let feedback = feedback.join("\n");
    let counter = TiktokenTokenCounter::default();
    let base_tokens = counter.count_text(&result.result);
    // build_agent_hook_context_block uses this exact inter-context separator.
    let separator_tokens = counter.count_text("\n\n---\n\n");
    let candidate = |prefix: &str| -> Option<String> {
        let mut body = json!({"tool_call_id": tool_call_id, "hook_feedback": prefix});
        if prefix.len() < feedback.len() {
            body["hook_feedback_omitted_bytes"] = json!(feedback.len() - prefix.len());
        }
        let context = body.to_string();
        let tokens = base_tokens
            .saturating_add(separator_tokens)
            .saturating_add(counter.count_text(&context));
        (cap == 0 || tokens <= cap).then_some(context)
    };
    if let Some(context) = candidate(&feedback) {
        return Some(context);
    }
    let mut best = candidate("")?;
    let (mut low, mut high) = (0, counter.count_text(&feedback).min(cap));
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        let prefix = counter.truncate_to_token_prefix(&feedback, middle);
        if let Some(context) = candidate(&prefix) {
            best = context;
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    // Only a final serialized candidate whose complete token count was checked
    // may be retained; no monotonic BPE-size assumption can admit excess text.
    Some(best)
}
