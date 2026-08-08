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

/// The single, provider-agnostic canonical request the engine emits once per
/// round. Every provider renders its wire from this via `chat_stream_ir`.
#[derive(Debug, Clone, Default)]
pub struct PromptIR {
    /// Byte-authoritative system text (non-block providers + flatten). Empty →
    /// fall back to joining `system_blocks` with `"\n\n"`. The engine assembles
    /// the exact wire string here, so an auto-prefix cache keys on the exact same
    /// bytes regardless of how `system_blocks` is structured.
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
    /// One chronological provider-visible transcript containing real
    /// conversation messages interleaved with durable model-context events.
    /// When present it replaces the legacy DynamicContext/SystemRemainder/
    /// Conversation/VolatileTail projection for normal request lowering.
    ModelTranscript,
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
///
/// WIRE BEHAVIOR — the two wire families lower this DIFFERENTLY, and the
/// difference is deliberate + byte-faithful to the pre-IR engine:
/// - OpenAI/Copilot **Responses** path: sends the FULL [`PromptIR::responses_input`]
///   as `input` together with `previous_response_id` (NOT the delta). This mirrors
///   the legacy `responses_input_messages` exactly; `store=false` is used, so the
///   request is effectively stateless and `previous_response_id` rides along for
///   reasoning continuity rather than to elide history.
/// - Chat-Completions path: sends [`PromptIR::continuation_delta`] (the delta after
///   `last_committed_assistant_id`). No Responses-only provider takes this path
///   today, so the delta lowering is currently exercised only by tests.
///
/// (Whether `store=false` + `previous_response_id` is the ideal Responses contract
/// is a PRE-EXISTING question, untouched by the IR rewrite — see the PR discussion.)
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
    /// non-empty `system_blocks` joined by `"\n\n"`.
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
        if self
            .segments
            .iter()
            .any(|segment| segment.role == SegmentRole::ModelTranscript)
        {
            out.extend_from_slice(self.run(SegmentRole::ModelTranscript));
            return out;
        }
        out.extend_from_slice(self.run(SegmentRole::DynamicContext));
        out.extend_from_slice(self.run(SegmentRole::SystemRemainder));
        out.extend_from_slice(self.run(SegmentRole::Conversation));
        out.extend_from_slice(self.run(SegmentRole::VolatileTail));
        out
    }

    /// Flat message list for chat / non-block providers: `[system?] ++ body_chat`.
    pub fn flatten(&self) -> Vec<Message> {
        let mut out = Vec::new();
        let system = self.system_field();
        if !system.trim().is_empty() {
            out.push(Message::system(system.trim().to_string()));
        }
        out.extend(self.body_chat());
        out
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
        if self
            .segments
            .iter()
            .any(|segment| segment.role == SegmentRole::ModelTranscript)
        {
            return self.model_transcript_tail().to_vec();
        }
        let mut out = Vec::new();
        out.extend_from_slice(self.run(SegmentRole::SystemRemainder));
        out.extend_from_slice(self.run(SegmentRole::DynamicContext));
        out.extend_from_slice(self.conversation_tail());
        out.extend_from_slice(self.run(SegmentRole::VolatileTail));
        out
    }

    /// Lower to the OpenAI/Copilot Responses-API request options: the input array
    /// (the stable system rides top-level `instructions`, NOT the array), the
    /// trimmed `instructions`, and the stateful `previous_response_id` — merged onto
    /// `base`, which carries the engine's request POLICY (store / verbosity /
    /// reasoning summary / include list). This is the adapter seam: a Responses
    /// provider derives its wire view from the canonical IR here instead of the
    /// engine pre-baking it. `instructions` is trimmed to stay byte-identical to the
    /// wire `build_responses_body` produces (which trims) regardless of how the
    /// system field was assembled.
    pub fn responses_request_options(
        &self,
        base: Option<&crate::provider::ResponsesRequestOptions>,
    ) -> crate::provider::ResponsesRequestOptions {
        let mut options = base.cloned().unwrap_or_default();
        options.input_messages = Some(self.responses_input());
        let system = self.system_field();
        let trimmed = system.trim();
        options.instructions = (!trimmed.is_empty()).then(|| trimmed.to_string());
        if let Some(continuation) = self.continuation.as_ref() {
            options.previous_response_id = Some(continuation.previous_response_id.clone());
        }
        options
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

    fn model_transcript_tail(&self) -> &[Message] {
        let transcript = self.run(SegmentRole::ModelTranscript);
        match self
            .continuation
            .as_ref()
            .and_then(|continuation| continuation.last_committed_assistant_id.as_deref())
        {
            Some(id) => match transcript.iter().rposition(|message| message.id == id) {
                Some(index) if index + 1 < transcript.len() => &transcript[index + 1..],
                _ => transcript,
            },
            None => transcript,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn flatten_orders_runs_with_remainder_and_volatile() {
        let ir = fixture_ir();
        // [system] ++ StablePrefix ++ DynamicContext ++ SystemRemainder ++ Conversation ++ VolatileTail
        let expected: Vec<(Role, String)> = vec![
            (Role::System, "SYSTEM".to_string()),
            (Role::User, "PREFIX".to_string()),
            (Role::User, "DYNAMIC".to_string()),
            (Role::System, "REMAINDER".to_string()),
            (Role::User, "u1".to_string()),
            (Role::Assistant, "a1".to_string()),
            (Role::User, "u2".to_string()),
            (Role::User, "VOLATILE".to_string()),
        ];
        assert_eq!(shape(&ir.flatten()), expected);
        assert_eq!(ir.system_field(), "SYSTEM");
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
    fn responses_request_options_derives_input_instructions_and_continuation() {
        use crate::provider::ResponsesRequestOptions;
        // Base carries the engine's request POLICY only.
        let base = ResponsesRequestOptions {
            store: Some(false),
            text_verbosity: Some("high".to_string()),
            prompt_cache_key: Some("session-affinity-hash".to_string()),
            ..Default::default()
        };
        let ir = PromptIR {
            continuation: Some(Continuation {
                previous_response_id: "resp_prev".to_string(),
                last_committed_assistant_id: None,
            }),
            ..fixture_ir()
        };

        let options = ir.responses_request_options(Some(&base));
        // Policy preserved.
        assert_eq!(options.store, Some(false));
        assert_eq!(options.text_verbosity.as_deref(), Some("high"));
        assert_eq!(
            options.prompt_cache_key.as_deref(),
            Some("session-affinity-hash")
        );
        // Prompt wire view derived from the IR.
        assert_eq!(options.instructions.as_deref(), Some("SYSTEM"));
        assert_eq!(
            shape(options.input_messages.as_deref().unwrap()),
            shape(&ir.responses_input()),
            "input_messages is the full responses_input view (system rides instructions)"
        );
        assert_eq!(options.previous_response_id.as_deref(), Some("resp_prev"));
    }

    #[test]
    fn responses_request_options_omits_instructions_when_system_blank() {
        let ir = PromptIR {
            system_text: "   ".to_string(),
            ..PromptIR::default()
        };
        let options = ir.responses_request_options(None);
        assert!(
            options.instructions.is_none(),
            "blank system field → no instructions (matches legacy None)"
        );
        assert!(options.previous_response_id.is_none());
    }

    #[test]
    fn system_field_prefers_authoritative_text_else_joins_blocks() {
        use bamboo_domain::{ContextBlockType, PromptBlock};
        let blocks = vec![
            PromptBlock::new("base", ContextBlockType::Base, "base"),
            // an empty block is skipped in the fallback join
            PromptBlock::new("skill", ContextBlockType::SkillContext, "   "),
            PromptBlock::new("env", ContextBlockType::EnvSnapshot, "env"),
        ];
        // Authoritative: `system_text` wins even when `system_blocks` is present, so
        // the non-block providers' wire bytes never change as the blocks evolve.
        let authoritative = PromptIR {
            system_text: "AUTHORITATIVE".to_string(),
            system_blocks: blocks.clone(),
            ..PromptIR::default()
        };
        assert_eq!(authoritative.system_field(), "AUTHORITATIVE");
        // Fallback: empty `system_text` → join the non-empty blocks by "\n\n".
        let fallback = PromptIR {
            system_text: String::new(),
            system_blocks: blocks,
            ..PromptIR::default()
        };
        assert_eq!(fallback.system_field(), "base\n\nenv");
    }

    #[test]
    fn dynamic_context_and_remainder_swap_between_chat_and_delta() {
        // The two runs are ORDERED OPPOSITELY across the boundary: chat puts
        // DynamicContext before SystemRemainder; the delta puts SystemRemainder
        // first. Locking the swap guards against a future "unify" that would
        // silently desync the stored-turn layout from the delta layout (review
        // finding #2).
        let ir = PromptIR {
            continuation: Some(Continuation {
                previous_response_id: "r".to_string(),
                last_committed_assistant_id: None,
            }),
            ..fixture_ir()
        };

        let chat = shape(&ir.body_chat());
        let dynamic_chat = chat.iter().position(|(_, c)| c == "DYNAMIC").unwrap();
        let remainder_chat = chat.iter().position(|(_, c)| c == "REMAINDER").unwrap();
        assert!(
            dynamic_chat < remainder_chat,
            "chat: dynamic context precedes the system remainder"
        );

        let delta = shape(&ir.continuation_delta());
        let dynamic_delta = delta.iter().position(|(_, c)| c == "DYNAMIC").unwrap();
        let remainder_delta = delta.iter().position(|(_, c)| c == "REMAINDER").unwrap();
        assert!(
            remainder_delta < dynamic_delta,
            "delta: the system remainder precedes dynamic context (swapped vs chat)"
        );
    }

    #[test]
    fn model_transcript_is_chronological_and_supersedes_legacy_mutable_lanes() {
        let transcript = vec![
            Message::user("context-v1"),
            Message::user("u1"),
            Message::assistant("a1", None),
            Message::tool_result("call-1", "tool-output"),
            Message::user("context-v2"),
        ];
        let assistant_id = transcript[2].id.clone();
        let ir = PromptIR {
            system_text: "SYSTEM".to_string(),
            segments: vec![
                Segment::new(SegmentRole::StablePrefix, vec![Message::user("GUIDE")]),
                Segment::new(SegmentRole::ModelTranscript, transcript.clone()),
                // These legacy lanes remain supported when ModelTranscript is
                // absent, but must never be duplicated into the new engine path.
                Segment::new(
                    SegmentRole::DynamicContext,
                    vec![Message::user("OLD-DYNAMIC")],
                ),
                Segment::new(
                    SegmentRole::Conversation,
                    vec![Message::user("OLD-CONVERSATION")],
                ),
                Segment::new(SegmentRole::VolatileTail, vec![Message::user("OLD-TAIL")]),
            ],
            continuation: Some(Continuation {
                previous_response_id: "resp".to_string(),
                last_committed_assistant_id: Some(assistant_id),
            }),
            ..PromptIR::default()
        };

        let mut expected_body = vec![Message::user("GUIDE")];
        expected_body.extend(transcript.clone());
        assert_eq!(shape(&ir.body_chat()), shape(&expected_body));
        assert_eq!(shape(&ir.responses_input()), shape(&expected_body));
        assert_eq!(
            shape(&ir.continuation_delta()),
            shape(&transcript[3..]),
            "continuation slices the single chronological transcript"
        );
        assert!(ir
            .body_chat()
            .iter()
            .all(|message| !message.content.starts_with("OLD-")));
    }
}
