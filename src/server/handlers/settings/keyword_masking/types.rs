use crate::core::keyword_masking::KeywordEntry;
use serde::{Deserialize, Serialize};

/// Response for keyword masking configuration.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct KeywordMaskingResponse {
    pub(super) entries: Vec<KeywordEntry>,
}

impl KeywordMaskingResponse {
    pub(super) fn new(entries: Vec<KeywordEntry>) -> Self {
        Self { entries }
    }
}

/// Validation error for keyword entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ValidationError {
    pub(super) index: usize,
    pub(super) message: String,
}

pub(super) fn map_validation_errors(errors: Vec<(usize, String)>) -> Vec<ValidationError> {
    errors
        .into_iter()
        .map(|(idx, msg)| ValidationError {
            index: idx,
            message: msg,
        })
        .collect()
}
