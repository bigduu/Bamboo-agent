//! One canonical conversation generation, independent of roles and tool aliases.
//!
//! These checks never adopt a newer generation into an older transcript. A
//! mismatch requires a complete canonical reload or refusal. Only the future
//! fenced reset transaction may publish a new ledger; normal writers preserve it.

use sha2::{Digest, Sha256};

use super::{ConversationResetState, Session, SessionKind};

/// Host-local data captured from the original caller. This is not serializable,
/// is not a grant and is never refreshed from a new Actor lease. Identity and
/// caller authority remain independently checked by their existing boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationGenerationObservation(Option<ConversationResetState>);

impl ConversationGenerationObservation {
    /// Check the whole ledger against a validated canonical Session. No context
    /// is adopted, even when the observed revision number happens to match.
    pub fn check_canonical(&self, canonical: &Session) -> Result<(), ConversationGenerationError> {
        canonical.validate_conversation_state()?;
        if self.0 != canonical.conversation_reset {
            return Err(ConversationGenerationError::StaleGeneration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConversationGenerationError {
    #[error("Session conversation reset state is invalid or unsupported")]
    InvalidState,
    #[error("Session conversation generation belongs to a different lifetime")]
    LifetimeChanged,
    #[error("Session conversation generation changed; reload the complete canonical Session")]
    StaleGeneration,
}

impl Session {
    /// Capture data only; final use validates canonical state and authority.
    pub fn conversation_generation_observation(&self) -> ConversationGenerationObservation {
        ConversationGenerationObservation(self.conversation_reset.clone())
    }

    /// Observation only, never an authority grant. Invalid ledgers are rejected
    /// by `validate_conversation_state` and final canonical writer checks.
    pub fn conversation_revision(&self) -> u64 {
        self.conversation_reset
            .as_ref()
            .map_or(0, |state| state.conversation_revision)
    }

    pub fn validate_conversation_state(&self) -> Result<(), ConversationGenerationError> {
        let Some(state) = self.conversation_reset.as_ref() else {
            return Ok(());
        };
        if self.kind != SessionKind::Root
            || self.parent_session_id.is_some()
            || self.parent_created_at.is_some()
            || self.spawn_depth != 0
            || (!self.root_session_id.is_empty() && self.root_session_id != self.id)
            // The zero state is represented only by absence. Do not migrate an
            // old Session or introduce a second serialized spelling by saving.
            || state.conversation_revision == 0
            || !state.valid_for(&self.root_mode_birth_token())
        {
            return Err(ConversationGenerationError::InvalidState);
        }
        Ok(())
    }

    /// Preserve the exact canonical ledger, including receipts at the same
    /// revision. A copied revision number alone cannot authorize stale context.
    /// Original zero/zero Sessions retain their existing lifetime validation at
    /// their own caller/storage boundary; this method adds no new zero-state
    /// rebind or identity semantics.
    pub fn check_conversation_generation(
        &self,
        canonical: &Session,
    ) -> Result<(), ConversationGenerationError> {
        self.validate_conversation_state()?;
        canonical.validate_conversation_state()?;
        if self.conversation_reset.is_none() && canonical.conversation_reset.is_none() {
            return Ok(());
        }
        if self.id != canonical.id
            || self.created_at != canonical.created_at
            || self.kind != canonical.kind
            || self.authority_identity != canonical.authority_identity
        {
            return Err(ConversationGenerationError::LifetimeChanged);
        }
        if self.conversation_reset != canonical.conversation_reset {
            return Err(ConversationGenerationError::StaleGeneration);
        }
        Ok(())
    }

    /// Bounded proof projection of the complete canonical ledger. The inline
    /// state stays authoritative; a hash alone is not a replacement ledger.
    pub fn conversation_reset_sha256(&self) -> Result<Option<String>, ConversationGenerationError> {
        self.validate_conversation_state()?;
        let Some(state) = self.conversation_reset.as_ref() else {
            return Ok(None);
        };
        let bytes =
            serde_json::to_vec(state).map_err(|_| ConversationGenerationError::InvalidState)?;
        let mut digest = Sha256::new();
        digest.update(b"bamboo.conversation-reset.ledger.v1\0");
        digest.update(bytes);
        Ok(Some(hex::encode(digest.finalize())))
    }
}

#[cfg(test)]
#[path = "conversation_generation_tests.rs"]
mod tests;
