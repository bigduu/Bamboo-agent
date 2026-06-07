//! Provider-neutral multimodal content part for messages.
//!
//! This type replaces the LLM-layer `ContentPart` in the domain to avoid
//! polluting session types with provider-specific infrastructure details.
//! The serde wire format is byte-for-byte identical to `ContentPart`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessagePart {
    Text { text: String },
    ImageUrl { image_url: ImageUrlRef },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageUrlRef {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
