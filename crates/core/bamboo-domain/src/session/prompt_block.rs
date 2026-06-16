//! Structured prompt content blocks.
//!
//! The content-block model that replaces concatenated prompt strings. Each
//! logical piece of the prompt (base identity, core directives, env snapshot,
//! and the relocated/dynamic contexts) is a discrete [`PromptBlock`] carrying
//! its own raw text — NO HTML-comment markers, NO `key: value` preamble — plus
//! the metadata needed to place it (stability/priority) and to anchor a cache
//! breakpoint on it ([`PromptBlock::cache_anchor`]).
//!
//! This is the transport for the prompt-assembly refactor: the provider system
//! field becomes an array of these blocks, and each injected/dynamic context
//! message's content becomes an array of these blocks, so we keep structured
//! data end-to-end instead of building a big string and re-parsing it by marker.
//!
//! Step 1 (this file) is purely additive: the types exist but are not yet wired
//! into the assembly path. Reuses the existing [`ContextBlockType`],
//! [`ContextBlockPriority`], and [`ContextBlockStability`] enums.

use serde::{Deserialize, Serialize};

use crate::session::context_block::{
    ContextBlockPriority, ContextBlockStability, ContextBlockType,
};

/// Anthropic-style cache breakpoint marker carried on a structured block.
///
/// Mirrors the provider wire shape (`{"type":"ephemeral"}` optionally with
/// `{"ttl":"1h"}`). Kept provider-neutral in the domain; the LLM layer renders
/// it into each provider's native format (Anthropic honors it; OpenAI ignores).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheControl {
    #[serde(rename = "type", default = "default_cache_type")]
    pub cache_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
}

fn default_cache_type() -> String {
    "ephemeral".to_string()
}

impl Default for CacheControl {
    fn default() -> Self {
        Self {
            cache_type: default_cache_type(),
            ttl: None,
        }
    }
}

impl CacheControl {
    /// Standard ephemeral (default ~5min provider TTL) breakpoint.
    pub fn ephemeral() -> Self {
        Self::default()
    }

    /// Ephemeral breakpoint with an explicit TTL, e.g. `"1h"` for the extended
    /// cache TTL.
    pub fn ephemeral_with_ttl(ttl: impl Into<String>) -> Self {
        Self {
            cache_type: default_cache_type(),
            ttl: Some(ttl.into()),
        }
    }
}

/// One discrete, structured piece of the prompt.
///
/// Holds RAW text only (no markers / no preamble). Placement is decided by
/// [`stability`](Self::stability) (which lane it rides) and cache breakpoints by
/// [`cache_anchor`](Self::cache_anchor) (whether this block ends a cacheable
/// prefix). The [`id`](Self::id) is a stable, deterministic identifier for the
/// logical piece (used for breakpoint resolution and observability).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptBlock {
    /// Stable, deterministic id for the logical piece (e.g. `"base"`,
    /// `"core_directives"`, `"env"`). Used to resolve cache breakpoints and for
    /// observability — NOT rendered into the prompt text.
    pub id: String,
    /// Which logical kind of content this is.
    pub kind: ContextBlockType,
    /// The raw body text — no markers, no `key: value` preamble.
    pub text: String,
    /// How stable this block is across rounds/sessions — drives lane placement.
    pub stability: ContextBlockStability,
    /// Relative priority cue (preserved from the existing context-block model).
    pub priority: ContextBlockPriority,
    /// Whether a cache breakpoint should be anchored on this block (this block
    /// ends a cacheable prefix). Subject to the provider breakpoint budget.
    #[serde(default)]
    pub cache_anchor: bool,
    /// Optional structured metadata for observability / round-tripping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl PromptBlock {
    /// Build a block with the given id/kind/text and default placement
    /// (`SessionStable`, `Medium` priority, no cache anchor).
    pub fn new(id: impl Into<String>, kind: ContextBlockType, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind,
            text: text.into(),
            stability: ContextBlockStability::SessionStable,
            priority: ContextBlockPriority::Medium,
            cache_anchor: false,
            metadata: None,
        }
    }

    pub fn with_stability(mut self, stability: ContextBlockStability) -> Self {
        self.stability = stability;
        self
    }

    pub fn with_priority(mut self, priority: ContextBlockPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Mark this block as the end of a cacheable prefix (a breakpoint candidate).
    pub fn as_cache_anchor(mut self) -> Self {
        self.cache_anchor = true;
        self
    }

    pub fn with_metadata(mut self, metadata: Option<serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// True when the block carries no meaningful content.
    pub fn is_empty(&self) -> bool {
        self.text.trim().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_block_builder_defaults_and_overrides() {
        let b = PromptBlock::new("base", ContextBlockType::Base, "You are Bodhi.")
            .with_stability(ContextBlockStability::Stable)
            .with_priority(ContextBlockPriority::High)
            .as_cache_anchor();
        assert_eq!(b.id, "base");
        assert_eq!(b.kind, ContextBlockType::Base);
        assert_eq!(b.stability, ContextBlockStability::Stable);
        assert!(b.cache_anchor);
        assert!(!b.is_empty());
    }

    #[test]
    fn prompt_block_serde_round_trip() {
        let b =
            PromptBlock::new("env", ContextBlockType::EnvSnapshot, "os=darwin").as_cache_anchor();
        let json = serde_json::to_string(&b).unwrap();
        let back: PromptBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
    }

    #[test]
    fn cache_control_wire_shape() {
        let cc = CacheControl::ephemeral_with_ttl("1h");
        let json = serde_json::to_value(&cc).unwrap();
        assert_eq!(json["type"], "ephemeral");
        assert_eq!(json["ttl"], "1h");
        // default ephemeral omits ttl
        let plain = serde_json::to_value(CacheControl::ephemeral()).unwrap();
        assert_eq!(plain["type"], "ephemeral");
        assert!(plain.get("ttl").is_none());
    }
}
