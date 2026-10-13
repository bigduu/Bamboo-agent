//! Model-facing direct-parent reply seam. The server owns every authority
//! check; a tool caller can only name a canonical request and choose one of
//! its bounded options.

use async_trait::async_trait;
use bamboo_domain::{ParentRequestOption, PARENT_ANSWER_MAX_BYTES};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentRequestReplyState {
    Recorded,
    AlreadyResolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParentRequestReplyReceipt {
    pub decision: ParentRequestOption,
    pub state: ParentRequestReplyState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentQuestionReplyReceipt {
    pub answer: String,
    pub state: ParentRequestReplyState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentRequestMessageReceipt {
    Permission(ParentRequestReplyReceipt),
    Clarification(ParentQuestionReplyReceipt),
}

pub fn validate_parent_answer_input(answer: &str) -> Result<(), &'static str> {
    if answer.trim().is_empty() || answer.len() > PARENT_ANSWER_MAX_BYTES || answer.contains('\0') {
        return Err("ParentQuestion answer must be non-empty bounded text");
    }
    Ok(())
}

#[async_trait]
pub trait ParentRequestReplyPort: Send + Sync {
    /// Resolve only after the Host has inspected the canonical request kind.
    /// Existing permission-only implementations remain closed for messages.
    async fn resolve_message(
        &self,
        _caller_session_id: &str,
        _request_id: &str,
        _message: &str,
    ) -> Result<ParentRequestMessageReceipt, String> {
        Err("Direct-parent message authority is unavailable".into())
    }

    async fn resolve(
        &self,
        caller_session_id: &str,
        request_id: &str,
        decision: ParentRequestOption,
    ) -> Result<ParentRequestReplyReceipt, String>;

    /// Only a canonical, still-pending direct-parent ParentQuestion may answer.
    /// The default stays closed until a Host resolver implements its CAS path.
    async fn resolve_answer(
        &self,
        _caller_session_id: &str,
        _request_id: &str,
        _answer: &str,
    ) -> Result<ParentQuestionReplyReceipt, String> {
        Err("Direct-parent clarification authority is unavailable".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answer_input_is_bounded_without_inferencing_permission_options() {
        assert!(validate_parent_answer_input("A").is_ok());
        assert!(validate_parent_answer_input("approve_once").is_ok());
        assert!(validate_parent_answer_input("deny").is_ok());
        assert!(validate_parent_answer_input(" \n ").is_err());
        assert!(validate_parent_answer_input("a\0b").is_err());
        assert!(validate_parent_answer_input(&"x".repeat(PARENT_ANSWER_MAX_BYTES + 1)).is_err());
    }
}
