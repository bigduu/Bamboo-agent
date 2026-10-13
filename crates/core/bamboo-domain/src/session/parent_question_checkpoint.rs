//! Bounded Worker observation submitted to the Host before a Child question
//! may become visible. Identity and Run authority are deliberately absent.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{
    Message, ParentQuestion, PendingQuestion, PendingQuestionSource, Role, SessionMessageId,
};

pub const PARENT_QUESTION_CHECKPOINT_ACTION: &str = "__host_parent_question_checkpoint_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentQuestionCheckpointV1 {
    pub version: u32,
    pub prefix_message_count: usize,
    pub prefix_digest: SessionMessageId,
    pub suffix: Vec<Message>,
    pub pending: PendingQuestion,
    pub tool_call_id: String,
    pub tool_result_message_id: String,
    pub question_digest: SessionMessageId,
}

impl ParentQuestionCheckpointV1 {
    pub const MAX_SUFFIX_MESSAGES: usize = 32;
    pub const MAX_SUFFIX_BYTES: usize = 48 * 1024;

    /// Validate only the payload shape. The Host must additionally compare the
    /// exact prefix, active Run, Child birth and Project under canonical state.
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.version != 1
            || self.suffix.is_empty()
            || self.suffix.len() > Self::MAX_SUFFIX_MESSAGES
            || serde_json::to_vec(&self.suffix)
                .map_or(true, |bytes| bytes.len() > Self::MAX_SUFFIX_BYTES)
            || self.pending.source != PendingQuestionSource::AgenticClarification
            || self.tool_call_id.is_empty()
            || self.tool_call_id.len() > 128
            || self.pending.tool_call_id != self.tool_call_id
            || self.pending.question.trim().is_empty()
            || self.pending.question.len() > super::PARENT_QUESTION_TEXT_MAX_BYTES
            || self.pending.options.len() > 16
            || self
                .pending
                .options
                .iter()
                .any(|option| option.trim().is_empty())
            || SessionMessageId::parse(self.tool_result_message_id.clone()).is_err()
            || self.question_digest
                != ParentQuestion::question_digest(&self.pending, &self.tool_result_message_id)
        {
            return Err("unsupported Child question checkpoint");
        }
        let mut ids = HashSet::with_capacity(self.suffix.len());
        for message in &self.suffix {
            if SessionMessageId::parse(message.id.clone()).is_err()
                || !ids.insert(message.id.as_str())
                || !matches!(message.role, Role::Assistant | Role::Tool)
                || message
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.get("session_message").is_some())
            {
                return Err("unsupported Child question transcript");
            }
        }
        let paired_index = self
            .suffix
            .iter()
            .rposition(|message| {
                message.role == Role::Tool
                    && message.id == self.tool_result_message_id
                    && message.tool_call_id.as_deref() == Some(self.tool_call_id.as_str())
            })
            .ok_or("Child question tool result missing")?;
        let paired = &self.suffix[paired_index];
        if paired.content != format!("Clarification needed: {}", self.pending.question)
            || self.suffix[paired_index + 1..].iter().any(|message| {
                message.role == Role::Tool
                    && message.tool_call_id.as_deref() == Some(self.tool_call_id.as_str())
            })
            || !self.suffix[..paired_index].iter().any(|message| {
                message.role == Role::Assistant
                    && message.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|call| {
                            call.id == self.tool_call_id
                                && call.function.name == self.pending.tool_name
                        })
                    })
            })
        {
            return Err("Child question tool occurrence mismatch");
        }
        Ok(())
    }
}
