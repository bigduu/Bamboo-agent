//! Shared ceilings for the patrol model settings and runtime policy.

pub const MAX_INPUT_TOKENS: u32 = 32_768;
/// Registered providers can reserve up to the canonical Work input ceiling.
pub const MAX_PROVIDER_INPUT_TOKENS: u32 = 1_048_576;
pub const MAX_OUTPUT_TOKENS: u32 = 8_192;
pub const MAX_EVIDENCE_BYTES: usize = 131_072;
pub const MAX_EVIDENCE_REFS: usize = 32;
