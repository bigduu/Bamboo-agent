//! `PromptIR` — bamboo's rich, provider-AGNOSTIC canonical request.
//!
//! The engine emits ONE `PromptIR` per round; each provider adapter renders it
//! into its own wire format by calling the lowering methods ([`PromptIR::system_field`],
//! [`PromptIR::flatten`], [`PromptIR::body_chat`], [`PromptIR::responses_input`],
//! [`PromptIR::continuation_delta`]) rather than reading fields ad hoc. This is
//! what lets bamboo own prompt assembly while providers stay pure adapters —
//! INCLUDING the stateful Responses continuation, which an adapter derives from
//! the addressable message runs + the [`Continuation`] boundary instead of the
//! engine pre-baking a delta.
//!
//! Key invariant — the "two-position SystemRemainder" trap: a history `System`
//! message not folded into the system field sits BETWEEN dynamic context and the
//! conversation in the chat/Responses views, but FIRST in a continuation delta.
//! [`PromptIR::body_chat`] and [`PromptIR::continuation_delta`] are therefore
//! INDEPENDENTLY-written concatenations over the same `segments` — never factor
//! them through a shared helper, or one path silently drifts (see the locking
//! tests, which fail loudly if the two positions are ever unified).

use bamboo_domain::{Message, PromptBlock};

use crate::cache::PromptCachePlan;
use crate::provider::PromptLanes;

/// The provider-agnostic canonical request the engine emits once per round.
/// Supersedes `PromptLanes` as the provider entry point.
#[derive(Debug, Clone, Default)]
pub struct PromptIR {
    /// Byte-authoritative system text (non-block providers + flatten). Empty →
    /// fall back to joining `system_blocks` with `"\n\n"`. Same semantics as
    /// `PromptLanes::stable_instructions`, so the auto-prefix cache keys on the
    /// exact same bytes.
    pub system_text: String,
    /// Parallel structured system field for block-native providers (Anthropic).
    pub system_blocks: Vec<PromptBlock>,
    /// Ordered, homogeneous message runs — one per structural [`SegmentRole`].
    /// The lowering methods (not the Vec order) impose the canonical
    /// concatenation order per view.
    pub segments: Vec<Segment>,
    /// Cross-cutting cache plan — the SOLE authority for breakpoint placement.
    pub cache: PromptCachePlan,
    /// `Some` → stateful Responses continuation (send only the delta with
    /// `previous_response_id` set). `None` → a full request.
    pub continuation: Option<Continuation>,
}

/// A contiguous run of messages sharing one structural role. Grouping into runs
/// (rather than tagging each `Message`) keeps the persisted `Message` type
/// untouched and makes each run directly addressable for delta projection.
#[derive(Debug, Clone)]
pub struct Segment {
    pub role: SegmentRole,
    pub messages: Vec<Message>,
}

impl Segment {
    pub fn new(role: SegmentRole, messages: Vec<Message>) -> Self {
        Self { role, messages }
    }
}

/// The structural role of a message run — drives cache-prefix placement and
/// continuation-delta projection. This is NOT content semantics (those live in
/// [`PromptBlock`]'s kind).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentRole {
    /// Tool guide, MCP guidance, workspace, env, skills. Session-stable cacheable
    /// prefix; OMITTED from a continuation delta (the stored turn already has it).
    StablePrefix,
    /// Conversation summary / per-round dynamic context. After the cache
    /// breakpoint, before history. INCLUDED in a continuation delta.
    DynamicContext,
    /// History `System` messages not folded into the system field (the
    /// "remainder"; usually empty). Sits BETWEEN DynamicContext and Conversation
    /// in the chat/Responses views; sits FIRST in a continuation delta.
    SystemRemainder,
    /// Real user / assistant / tool history (no system, no volatile).
    Conversation,
    /// Per-round volatile tail (recalled memory, task list, goal, plan). Never
    /// cached; always last; always resent (incl. in a delta).
    VolatileTail,
}

/// Stateful Responses-API continuation directive.
#[derive(Debug, Clone)]
pub struct Continuation {
    pub previous_response_id: String,
    /// Id of the last assistant message the stored turn ends at; the delta is the
    /// `Conversation` run strictly after it. `None` → no assistant boundary → the
    /// whole `Conversation` run is new (mirrors the legacy `continuation_messages`
    /// fallback).
    pub last_committed_assistant_id: Option<String>,
}

impl PromptIR {
    /// The messages of a given run, or `&[]` when absent.
    pub fn run(&self, role: SegmentRole) -> &[Message] {
        self.segments
            .iter()
            .find(|segment| segment.role == role)
            .map(|segment| segment.messages.as_slice())
            .unwrap_or(&[])
    }

    /// Byte-authoritative system text: `system_text` when non-empty, else the
    /// non-empty `system_blocks` joined by `"\n\n"`. Identical to
    /// `PromptLanes::system_text`.
    pub fn system_field(&self) -> String {
        if !self.system_text.is_empty() {
            self.system_text.clone()
        } else {
            self.system_blocks
                .iter()
                .map(|block| block.text.as_str())
                .filter(|text| !text.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n\n")
        }
    }

    /// Body in canonical chat order, WITHOUT the system message:
    /// `StablePrefix ++ DynamicContext ++ SystemRemainder ++ Conversation ++ VolatileTail`.
    /// SystemRemainder sits between DynamicContext and Conversation — the exact
    /// legacy order (`envelope.conversation_messages = remainder ++ conversation`).
    pub fn body_chat(&self) -> Vec<Message> {
        let mut out = Vec::new();
        out.extend_from_slice(self.run(SegmentRole::StablePrefix));
        out.extend_from_slice(self.run(SegmentRole::DynamicContext));
        out.extend_from_slice(self.run(SegmentRole::SystemRemainder));
        out.extend_from_slice(self.run(SegmentRole::Conversation));
        out.extend_from_slice(self.run(SegmentRole::VolatileTail));
        out
    }

    /// Flat message list for chat / non-block providers: `[system?] ++ body_chat`.
    /// Byte-identical to `PromptLanes::flatten()`.
    pub fn flatten(&self) -> Vec<Message> {
        let mut out = Vec::new();
        let system = self.system_field();
        if !system.trim().is_empty() {
            out.push(Message::system(system.trim().to_string()));
        }
        out.extend(self.body_chat());
        out
    }

    /// Reconstruct the legacy [`PromptLanes`] from the IR — the `SystemRemainder`
    /// and `VolatileTail` runs are merged back into the conversation lane, exactly
    /// as the engine built the lanes before the IR. A transitional bridge so a
    /// provider that still consumes lanes (its `chat_stream_lanes` override)
    /// renders byte-identically during the migration. `to_lanes().flatten()`
    /// equals [`flatten`](Self::flatten).
    pub fn to_lanes(&self) -> PromptLanes {
        let mut conversation_messages = Vec::new();
        conversation_messages.extend_from_slice(self.run(SegmentRole::SystemRemainder));
        conversation_messages.extend_from_slice(self.run(SegmentRole::Conversation));
        conversation_messages.extend_from_slice(self.run(SegmentRole::VolatileTail));
        PromptLanes {
            stable_instructions: self.system_text.clone(),
            system_blocks: self.system_blocks.clone(),
            stable_prefix_messages: self.run(SegmentRole::StablePrefix).to_vec(),
            dynamic_context_messages: self.run(SegmentRole::DynamicContext).to_vec(),
            conversation_messages,
        }
    }

    /// Responses-API input view (system rides `instructions`, not the array):
    /// the same body as [`body_chat`](Self::body_chat) — today's Responses input
    /// also includes the SystemRemainder.
    pub fn responses_input(&self) -> Vec<Message> {
        self.body_chat()
    }

    /// Continuation delta — only meaningful when `continuation` is `Some`. Order:
    /// `SystemRemainder ++ DynamicContext ++ conversation_tail ++ VolatileTail`
    /// (SystemRemainder FIRST — deliberately different from
    /// [`body_chat`](Self::body_chat)). Written independently so the two
    /// positions can never be unified by accident.
    pub fn continuation_delta(&self) -> Vec<Message> {
        let mut out = Vec::new();
        out.extend_from_slice(self.run(SegmentRole::SystemRemainder));
        out.extend_from_slice(self.run(SegmentRole::DynamicContext));
        out.extend_from_slice(self.conversation_tail());
        out.extend_from_slice(self.run(SegmentRole::VolatileTail));
        out
    }

    /// New conversation since the last committed assistant turn (the delta of the
    /// `Conversation` run). Reproduces the legacy `rposition(Role::Assistant)`
    /// semantics, pinned by id so the engine commits the boundary instead of
    /// re-scanning.
    fn conversation_tail(&self) -> &[Message] {
        let conversation = self.run(SegmentRole::Conversation);
        match self
            .continuation
            .as_ref()
            .and_then(|continuation| continuation.last_committed_assistant_id.as_deref())
        {
            Some(id) => match conversation.iter().rposition(|message| message.id == id) {
                // New turns exist after the boundary → send exactly those.
                Some(index) if index + 1 < conversation.len() => &conversation[index + 1..],
                // Nothing new after the boundary, OR the boundary churned away
                // (compression / id wipe) → fail open to the whole conversation,
                // matching the legacy `continuation_messages` None fallback.
                _ => conversation,
            },
            None => conversation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::PromptLanes;
    use bamboo_domain::Role;

    fn shape(messages: &[Message]) -> Vec<(Role, String)> {
        messages
            .iter()
            .map(|message| (message.role.clone(), message.content.clone()))
            .collect()
    }

    /// Fixture with the exact combination that exposes the two-position
    /// SystemRemainder trap: a non-empty SystemRemainder, a non-empty
    /// VolatileTail, and a multi-turn Conversation ending in an Assistant turn.
    fn fixture_ir() -> PromptIR {
        PromptIR {
            system_text: "SYSTEM".to_string(),
            segments: vec![
                Segment::new(SegmentRole::StablePrefix, vec![Message::user("PREFIX")]),
                Segment::new(SegmentRole::DynamicContext, vec![Message::user("DYNAMIC")]),
                Segment::new(
                    SegmentRole::SystemRemainder,
                    vec![Message::system("REMAINDER")],
                ),
                Segment::new(
                    SegmentRole::Conversation,
                    vec![
                        Message::user("u1"),
                        Message::assistant("a1", None),
                        Message::user("u2"),
                    ],
                ),
                Segment::new(SegmentRole::VolatileTail, vec![Message::user("VOLATILE")]),
            ],
            ..PromptIR::default()
        }
    }

    #[test]
    fn flatten_byte_matches_legacy_lanes_with_remainder_and_volatile() {
        let ir = fixture_ir();
        // Legacy lanes merge remainder ++ conversation ++ volatile into the single
        // conversation lane, exactly as build_request_envelope does today.
        let conversation_messages = vec![
            Message::system("REMAINDER"),
            Message::user("u1"),
            Message::assistant("a1", None),
            Message::user("u2"),
            Message::user("VOLATILE"),
        ];
        let lanes = PromptLanes {
            stable_instructions: "SYSTEM".to_string(),
            stable_prefix_messages: vec![Message::user("PREFIX")],
            dynamic_context_messages: vec![Message::user("DYNAMIC")],
            conversation_messages,
            ..PromptLanes::default()
        };
        // Byte-identical effective flat list (compare role+content; the synthesized
        // system message gets a fresh id in each flatten()).
        assert_eq!(shape(&ir.flatten()), shape(&lanes.flatten()));
        assert_eq!(ir.system_field(), lanes.system_text());
    }

    #[test]
    fn chat_and_delta_place_system_remainder_in_opposite_positions() {
        let chat = shape(&fixture_ir().flatten());
        let dynamic = chat.iter().position(|(_, c)| c == "DYNAMIC").unwrap();
        let remainder = chat.iter().position(|(_, c)| c == "REMAINDER").unwrap();
        let conversation = chat.iter().position(|(_, c)| c == "u1").unwrap();
        assert!(
            dynamic < remainder && remainder < conversation,
            "chat: remainder sits between dynamic context and the conversation"
        );

        let continued = PromptIR {
            continuation: Some(Continuation {
                previous_response_id: "resp_prev".to_string(),
                last_committed_assistant_id: None,
            }),
            ..fixture_ir()
        };
        let delta = shape(&continued.continuation_delta());
        assert_eq!(delta[0].1, "REMAINDER", "delta: remainder is FIRST");
    }

    #[test]
    fn continuation_delta_slices_conversation_after_committed_assistant() {
        let conversation = vec![
            Message::user("u1"),
            Message::assistant("a1", None),
            Message::user("u2"),
        ];
        let assistant_id = conversation[1].id.clone();
        let ir = PromptIR {
            segments: vec![Segment::new(SegmentRole::Conversation, conversation)],
            continuation: Some(Continuation {
                previous_response_id: "resp".to_string(),
                last_committed_assistant_id: Some(assistant_id),
            }),
            ..PromptIR::default()
        };
        assert_eq!(
            shape(&ir.continuation_delta()),
            vec![(Role::User, "u2".to_string())],
            "delta is the conversation strictly after the committed assistant turn"
        );
    }

    #[test]
    fn continuation_delta_falls_open_when_boundary_is_last_message() {
        // Boundary = the last message (nothing new after it). Legacy
        // continuation_messages returns None here → whole conversation; the IR
        // must match (NOT an empty delta).
        let conversation = vec![Message::user("u1"), Message::assistant("a1", None)];
        let assistant_id = conversation[1].id.clone();
        let ir = PromptIR {
            segments: vec![Segment::new(SegmentRole::Conversation, conversation)],
            continuation: Some(Continuation {
                previous_response_id: "resp".to_string(),
                last_committed_assistant_id: Some(assistant_id),
            }),
            ..PromptIR::default()
        };
        assert_eq!(ir.continuation_delta().len(), 2, "nothing new → whole conv");
    }

    #[test]
    fn continuation_delta_falls_open_when_boundary_id_missing() {
        let conversation = vec![Message::user("u1"), Message::assistant("a1", None)];
        let ir = PromptIR {
            segments: vec![Segment::new(SegmentRole::Conversation, conversation)],
            continuation: Some(Continuation {
                previous_response_id: "resp".to_string(),
                last_committed_assistant_id: Some("nonexistent-id".to_string()),
            }),
            ..PromptIR::default()
        };
        // id not found → fail open to the whole conversation (no-assistant fallback).
        assert_eq!(ir.continuation_delta().len(), 2);
    }

    #[test]
    fn responses_input_equals_body_chat() {
        let ir = fixture_ir();
        assert_eq!(shape(&ir.responses_input()), shape(&ir.body_chat()));
    }

    #[test]
    fn to_lanes_round_trips_flatten() {
        // The transitional bridge must reproduce the exact flat bytes, so a
        // provider routed through the reconstructed lanes is byte-identical.
        let ir = fixture_ir();
        assert_eq!(shape(&ir.to_lanes().flatten()), shape(&ir.flatten()));
    }
}
