use super::{
    retrieval_window_preflight, AgentError, AgentLoopConfig, LLMProvider, ModelContextResetReason,
    Role, Session, TokenBudget, TokenCounter, ToolSchema,
};
use bamboo_compression::MessageSegmenter;
use bamboo_config::ContextManagementStrategy;
use bamboo_domain::session::{
    ToolOutputPreview, MAX_TOOL_OUTPUT_PREVIEWS, MAX_TOOL_OUTPUT_PREVIEW_BYTES,
};
use bamboo_domain::{canonical_tool_name, sha256_hex, SessionKind};
use std::collections::HashMap;
use std::sync::Arc;

pub(in crate::runtime::runner::round_lifecycle) fn enabled(
    session: &Session,
    config: &AgentLoopConfig,
) -> bool {
    config.context_management.prune_old_tool_outputs
        && config.context_management.strategy == ContextManagementStrategy::Summary
        && config.persistence.is_some()
        && session.kind == SessionKind::Root
        && session.authority_identity.is_ordinary()
        && session.supervisor_management.is_none()
        && !session.root_orchestration_only
        && session.provider_transcript.groups().is_empty()
}

/// Stage a single prefix boundary and measure it through the real request projection.
/// A candidate that cannot leave enough working room is discarded before summarizing.
#[allow(clippy::too_many_arguments)]
pub(super) async fn try_apply(
    session: &mut Session,
    config: &AgentLoopConfig,
    model: &str,
    tool_schemas: &[ToolSchema],
    llm: &Arc<dyn LLMProvider>,
    budget: &TokenBudget,
    counter: &dyn TokenCounter,
    before: u32,
) -> Result<bool, AgentError> {
    let trigger = budget.compression_thresholds().trigger_tokens;
    let target = trigger.saturating_sub((trigger / 10).clamp(1, 8192));
    let candidates = candidates(session, budget.prompt_cache_min_tool_output_chars, counter);
    if candidates.is_empty() {
        return Ok(false);
    }

    let mut staged = session.clone();
    staged.reset_model_context_epoch(ModelContextResetReason::ExplicitHistoryRewrite);
    staged.metadata.remove("responses.previous_response_id");
    let state = staged
        .model_context_state
        .as_mut()
        .expect("reset initialized ledger");
    let available = MAX_TOOL_OUTPUT_PREVIEWS.saturating_sub(state.tool_output_previews.len());
    let mut remaining = candidates.into_iter().take(available).peekable();
    let mut saved = 0u32;
    let mut applied = 0usize;
    let mut required = before.saturating_sub(target);
    // The first batch is sized by the local estimate. One additional projection
    // can cover errors due to a new ledger baseline or provider tool rendering.
    for _ in 0..2 {
        let state = staged
            .model_context_state
            .as_mut()
            .expect("reset initialized ledger");
        while applied == 0 || saved < required {
            let Some((id, preview, tokens)) = remaining.next() else {
                break;
            };
            state.tool_output_previews.insert(id, preview);
            saved = saved.saturating_add(tokens);
            applied += 1;
        }
        if applied == 0 {
            return Ok(false);
        }
        let after =
            retrieval_window_preflight(&staged, config, model, tool_schemas, llm, budget, counter)
                .await?;
        if !after.history_slice_callable
            || after.ledger_rendered_bytes > bamboo_domain::MAX_MODEL_CONTEXT_RENDERED_BYTES
        {
            return Ok(false);
        }
        if after.active_tokens <= target {
            tracing::info!(
                session_id = %session.id,
                pruned_tool_outputs = applied,
                before_tokens = before,
                after_tokens = after.active_tokens,
                target_tokens = target,
                "staged tool-output pruning at the request boundary"
            );
            *session = staged;
            return Ok(true);
        }
        if remaining.peek().is_none() {
            break;
        }
        required = saved.saturating_add(after.active_tokens.saturating_sub(target));
    }
    Ok(false)
}

fn candidates(
    session: &Session,
    min_chars: u32,
    counter: &dyn TokenCounter,
) -> Vec<(String, ToolOutputPreview, u32)> {
    let mut occurrences = HashMap::new();
    let mut message_ids = HashMap::new();
    for message in &session.messages {
        *message_ids.entry(message.id.as_str()).or_insert(0usize) += 1;
        for call in message.tool_calls.iter().flatten() {
            occurrences
                .entry(call.id.as_str())
                .or_insert((0usize, 0usize))
                .0 += 1;
        }
        if let Some(id) = &message.tool_call_id {
            occurrences.entry(id.as_str()).or_insert((0, 0)).1 += 1;
        }
    }
    let artifacts = bamboo_storage::search_index::session_history_search_artifact_ids(session);
    let current = session
        .model_context_state
        .as_ref()
        .map(|state| &state.tool_output_previews);
    let segments = MessageSegmenter::new().segment(
        session
            .messages
            .iter()
            .filter(|message| !message.compressed)
            .cloned()
            .collect(),
    );
    // Count complete batches before checking whether their tool type is
    // eligible. Recent reads or writes still protect their place in the tail.
    let older_batches = segments
        .iter()
        .rev()
        .filter(|segment| {
            segment.messages.first().is_some_and(|message| {
                message.role == Role::Assistant
                    && message.tool_calls.as_ref().is_some_and(|calls| {
                        !calls.is_empty()
                            && segment.messages.len() == calls.len() + 1
                            && segment.get_missing_results().is_empty()
                    })
            })
        })
        .skip(2);

    let mut result = Vec::new();
    for batch in older_batches {
        if batch.messages.iter().any(|message| {
            message.never_compress
                || bamboo_domain::session::history::is_hidden_from_ui(message)
                || artifacts.contains(&message.id)
        }) {
            continue;
        }
        let calls = batch.messages[0].tool_calls.as_deref().unwrap_or_default();
        if calls
            .iter()
            .any(|call| occurrences.get(call.id.as_str()) != Some(&(1, 1)))
        {
            continue;
        }
        for message in batch.messages[1..].iter().rev() {
            if message.role != Role::Tool
                || message
                    .content_parts
                    .as_ref()
                    .is_some_and(|parts| !parts.is_empty())
                || message.id.is_empty()
                || message.id.len() > 80
                || !message
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                || message_ids.get(message.id.as_str()) != Some(&1)
                || current
                    .and_then(|previews| previews.get(&message.id))
                    .is_some_and(|preview| preview.matches(message))
                || message.content.chars().take(min_chars as usize).count() < min_chars as usize
            {
                continue;
            }
            let Some(call) = calls
                .iter()
                .find(|call| Some(call.id.as_str()) == message.tool_call_id.as_deref())
            else {
                continue;
            };
            let tool = canonical_tool_name(&call.function.name);
            let Some(status) = prunable_result_status(
                &tool,
                &call.function.arguments,
                &message.content,
                message.tool_success,
            ) else {
                continue;
            };
            let head: String = message.content.chars().take(210).collect();
            let tail: String = message
                .content
                .chars()
                .rev()
                .take(140)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            let rendered_text = format!(
                "[Earlier {tool} output shortened ({status}). Read the saved output with session_history_current, action read_slice, message_id {}.]\n{head}\n…\n{tail}",
                message.id,
            );
            let saved = counter
                .count_text(&message.content)
                .saturating_sub(counter.count_text(&rendered_text));
            if saved > 0 && rendered_text.len() <= MAX_TOOL_OUTPUT_PREVIEW_BYTES {
                result.push((
                    message.id.clone(),
                    ToolOutputPreview {
                        source_sha256: sha256_hex(message.content.as_bytes()),
                        rendered_text,
                    },
                    saved,
                ));
            }
        }
        if result.len() >= MAX_TOOL_OUTPUT_PREVIEWS {
            result.truncate(MAX_TOOL_OUTPUT_PREVIEWS);
            break;
        }
    }
    result
}

fn prunable_result_status(
    tool: &str,
    arguments: &str,
    output: &str,
    success: Option<bool>,
) -> Option<String> {
    let tool_status = match success {
        Some(true) => "succeeded",
        Some(false) => "failed",
        None => "status unspecified",
    };
    if matches!(tool, "Grep" | "Glob" | "WebSearch") {
        return Some(tool_status.into());
    }
    let argument = match tool {
        "Bash" => "command",
        "exec_command" => "cmd",
        _ => return None,
    };
    let args = serde_json::from_str::<serde_json::Value>(arguments).ok()?;
    let command = args.get(argument).and_then(serde_json::Value::as_str)?;
    if command.contains(['\r', '\n', ';', '|', '&', '>', '<', '`', '$']) {
        return None;
    }
    let mut words = command.split_ascii_whitespace();
    let eligible = matches!(
        (words.next(), words.next(), words.next()),
        (
            Some("cargo"),
            Some("test" | "nextest" | "check" | "clippy" | "build"),
            _
        ) | (
            Some("npm" | "pnpm" | "yarn" | "bun" | "go" | "dotnet" | "make"),
            Some("test" | "build"),
            _
        ) | (
            Some("npm" | "pnpm" | "yarn" | "bun"),
            Some("run"),
            Some("test" | "build")
        ) | (
            Some("pytest" | "jest" | "vitest" | "tsc" | "rg" | "grep"),
            _,
            _
        ) | (Some("python" | "python3"), Some("-m"), Some("pytest"))
            | (Some("npx"), Some("vitest" | "jest" | "tsc"), _)
            | (Some("git"), Some("grep"), _)
    );
    if !eligible {
        return None;
    }

    // A tool response can carry a live subprocess handle. Preserve it. A
    // completed shell tool can also report a nonzero exit with tool_success
    // set to true, so retain the exit code ahead of the tool transport status.
    let output_fields = if output.trim_start().starts_with('{') {
        serde_json::from_str::<serde_json::Value>(output).ok()
    } else {
        None
    };
    if output_fields.as_ref().is_some_and(|fields| {
        fields
            .get("session_id")
            .is_some_and(|value| !value.is_null())
    }) {
        return None;
    }
    let exit_code = output_fields
        .as_ref()
        .and_then(|fields| fields.get("exit_code"))
        .and_then(serde_json::Value::as_i64);
    match exit_code {
        Some(code) => Some(format!("exit code {code}")),
        None if tool == "exec_command" => None,
        None => Some(tool_status.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::prunable_result_status;

    #[test]
    fn only_selects_requested_tools_and_preserves_terminal_status() {
        let inspect = |tool, args, output| prunable_result_status(tool, args, output, Some(true));
        assert_eq!(
            inspect(
                "exec_command",
                r#"{"cmd":"cargo test"}"#,
                r#"{"exit_code":101}"#
            )
            .as_deref(),
            Some("exit code 101"),
        );
        assert_eq!(
            inspect("Bash", r#"{"command":"npm run build"}"#, "log").as_deref(),
            Some("succeeded"),
        );
        assert!(inspect("Grep", "{}", "result").is_some());
        assert!(inspect("Read", "{}", "source").is_none());
        assert!(inspect("Bash", r#"{"command":"echo cargo test"}"#, "text").is_none());
        assert!(inspect("Bash", r#"{"command":"cargo test; cat file"}"#, "text").is_none());
        assert!(inspect(
            "exec_command",
            r#"{"cmd":"cargo test"}"#,
            r#"{"session_id":"running"}"#
        )
        .is_none());
    }
}
