//! Versioned, provider-reported model-call usage for read-only observers.
//!
//! An absent value is unknown, not zero. Input is the provider's total input;
//! cache read and creation are subsets, while cache write is separate provider
//! metadata. Reasoning is a subset of output. Do not add these fields together.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelTokenUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCallStatus {
    Success,
    Error,
    Cancelled,
}

/// One completion of one dispatched model call, not a cumulative session total.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCallUsage {
    pub schema_version: u8,
    pub event_id: String,
    pub call_id: String,
    /// Safe session IDs are unchanged. Other nonempty IDs are represented by
    /// a `~`-prefixed, process-local reference that changes after restart;
    /// it is not a persisted ID or an authorization credential.
    pub session_id: String,
    pub run_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub status: ModelCallStatus,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub usage: Option<ModelTokenUsage>,
}
