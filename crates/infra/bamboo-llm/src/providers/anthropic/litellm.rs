//! Serialize Bamboo's planned Messages request with LiteLLM's wire types.

mod capabilities;
mod claude_reasoning;
mod claude_tool_ids;

pub(super) use claude_reasoning::thinking_replay_enabled;
pub(super) use claude_tool_ids::normalize_claude_tool_ids;

use bamboo_domain::ReasoningEffort;
use bamboo_litellm_llms_types::{
    formats::messages::{
        ContentBlock, Message, MessageContent, MessagesOptionalParams, MessagesRequest,
        OutputConfig, SystemPrompt,
    },
    recognized::Recognized,
};
use serde_json::Value;

use super::PreparedAnthropicContent;
use crate::provider::{LLMError, Result};

pub(super) fn messages_request(
    content: PreparedAnthropicContent,
    model: &str,
    max_tokens: u32,
    stream: bool,
    effort: Option<ReasoningEffort>,
) -> Result<MessagesRequest> {
    let messages = content
        .messages
        .into_iter()
        .map(|turn| {
            Ok(Message {
                role: turn.role.into(),
                content: MessageContent::Blocks(content_blocks(turn.content)?),
                extra: Default::default(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let system = content
        .system
        .map(content_blocks)
        .transpose()?
        .map(SystemPrompt::Blocks);

    // Final budget validation runs after output caps have been applied.
    let (thinking, level) = claude_reasoning::controls(model, effort)?;
    let output_config = level.map(|effort| {
        Recognized::Known(OutputConfig {
            effort: Some(Recognized::Known(effort)),
            ..Default::default()
        })
    });

    Ok(MessagesRequest {
        model: model.into(),
        messages,
        params: MessagesOptionalParams {
            max_tokens: Some(max_tokens.into()),
            stream: Some(stream),
            system,
            // Preserve Bamboo's custom tool declarations and any native fields.
            tools: Some(
                content
                    .tools
                    .into_iter()
                    .map(Recognized::Unrecognized)
                    .collect(),
            ),
            tool_choice: content.tool_choice,
            thinking: thinking.map(Recognized::Known),
            output_config,
            ..Default::default()
        },
    })
}

/// Preserve a generated budget when the model and reasoning controls still
/// match what the adapter prepared.
pub(super) fn retained_thinking_budget(
    body: &Value,
    model: &str,
    prepared_budget: Option<u64>,
) -> Option<u64> {
    prepared_budget.filter(|budget| {
        body["model"].as_str() == Some(model)
            && body.pointer("/thinking/type").and_then(Value::as_str) == Some("enabled")
            && body
                .pointer("/thinking/budget_tokens")
                .and_then(Value::as_u64)
                == Some(*budget)
            && body.pointer("/output_config/effort").is_none()
            && body.get("thinking_budget").is_none()
    })
}

pub(super) fn clamp_thinking_budget(body: &mut Value, budget: u64) -> Result<()> {
    let max_tokens = body["max_tokens"].as_u64().ok_or_else(|| {
        LLMError::Api("Claude Messages legacy thinking requires a numeric max_tokens".into())
    })?;
    body["thinking"]["budget_tokens"] =
        claude_reasoning::fit_legacy_budget(budget, max_tokens)?.into();
    Ok(())
}

fn content_blocks(blocks: Vec<Value>) -> Result<Vec<ContentBlock>> {
    blocks
        .into_iter()
        .map(|block| {
            let Value::Object(extra) = block else {
                return Err(LLMError::Api(
                    "Messages content blocks must be objects".into(),
                ));
            };
            // Provider-owned history can contain unknown keys or explicit nulls.
            // The flattened extension map retains them through serialization.
            Ok(ContentBlock {
                extra,
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
